// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{proposal_digest, ConsensusMessage, Header, Proposal};
use crate::primary::{Height, PrimaryMessage, PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// Timer resolution for retrying sync requests.
const TIMER_RESOLUTION: u64 = 1_000;

/// Commands sent to the waiter.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header, bool),
    SyncProposals(Vec<Proposal>, ConsensusMessage, Header),
    SyncParent(Digest, Header),
    SyncHeader(Digest),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Height,
    /// Delay between sync-request retries.
    sync_retry_delay: u64,
    /// Number of nodes contacted when retrying a sync request.
    sync_retry_nodes: usize,

    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<WaiterMessage>,
    /// Loops back to the core headers for which we got all parents and batches.
    tx_core: Sender<Header>,
    /// Returns commit messages to the committer for reprocessing.
    tx_consensus_loopback: Sender<(ConsensusMessage, Header)>,

    /// Network driver allowing to send messages.
    network: SimpleSender,

    /// Certificate requests and their retry timestamps.
    parent_requests: HashMap<Digest, (Height, u128)>,
    /// Requests for special parents.
    header_requests: HashMap<Digest, (Height, u128)>,
    /// Batch requests and their heights.
    batch_requests: HashMap<Digest, Height>,
    /// Pending items and cancellation handles.
    pending: HashMap<Digest, (Height, Sender<()>)>,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Height,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
        tx_consensus_loopback: Sender<(ConsensusMessage, Header)>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                consensus_round,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_synchronizer,
                tx_core,
                tx_consensus_loopback,
                network: SimpleSender::new()
                    .with_metrics(metrics)
                    .with_batching(batch),
                parent_requests: HashMap::new(),
                header_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Waits for storage entries or cancellation.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Header,
        mut handler: Receiver<()>,
    ) -> DagResult<Option<Header>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn proposal_waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: (ConsensusMessage, Header),
        mut handler: Receiver<()>,
    ) -> DagResult<Option<(ConsensusMessage, Header)>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();
        let mut proposal_waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncBatches(missing, header, force_sync) => {
                            debug!("Synching the payload of {}", header);
                            let header_id = header.id.clone();
                            let round = header.height;
                            let author = header.author;

                            // Deduplicate requests for the same header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Wait for all batches.
                            let wait_for = missing
                                .iter()
                                .map(|(digest, worker_id)| {
                                    let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                                    (key.to_vec(), self.store.clone())
                                })
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (round, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            if force_sync {
                                let mut requires_sync = HashMap::new();
                                for (digest, worker_id) in missing.into_iter() {
                                    self.batch_requests.entry(digest.clone()).or_insert_with(|| {
                                        requires_sync.entry(worker_id).or_insert_with(Vec::new).push(digest);
                                        round
                                    });
                                }
                                for (worker_id, digests) in requires_sync {
                                    let address = self.committee
                                        .worker(&self.name, &worker_id)
                                        .expect("Author of valid header is not in the committee")
                                        .primary_to_worker;
                                    debug!("Sent syncbatches message for height {}", round);
                                    let message = PrimaryWorkerMessage::Synchronize(digests, author);
                                    let bytes = bincode::serialize(&message)
                                        .expect("Failed to serialize batch sync request");
                                    self.network.send_typed(address, Bytes::from(bytes), "Synchronize").await;
                                }
                            }
                        }

                        WaiterMessage::SyncHeader(missing) => {
                            debug!("Syncing on header with digest {}", missing);

                            let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                            let mut requires_sync = Vec::new();
                            self.header_requests.entry(missing.clone()).or_insert_with(|| {
                                requires_sync.push(missing);
                                (0, now)
                            });

                            if !requires_sync.is_empty() {
                                let addresses = self.committee
                                .others_primaries(&self.name)
                                .iter()
                                .map(|(_, x)| x.primary_to_primary)
                                .collect();

                                let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.lucky_broadcast_typed(
                                    addresses,
                                    Bytes::from(bytes),
                                    self.sync_retry_nodes,
                                    "HeadersRequest",
                                ).await;
                            }
                        }

                        WaiterMessage::SyncParent(missing, header) => {
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let height = header.height();
                            let author = header.author;

                            // Deduplicate requests for the same header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Wait for the missing parent.
                            let wait_for = vec![(missing.to_vec(), self.store.clone())];
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (height, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Contact the header author first.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            self.parent_requests.entry(missing.clone()).or_insert_with(|| {
                                requires_sync.push(missing);
                                (height, now)
                            });
                            if !requires_sync.is_empty() {
                                let address = self.committee
                                    .primary(&author)
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.send_typed(address, Bytes::from(bytes), "HeadersRequest").await;
                            }
                        }


                        WaiterMessage::SyncProposals(missing, consensus_message, header) => {
                            let height = header.height();
                            let author = header.author;
                            let id = proposal_digest(&consensus_message);

                            // Deduplicate requests for the same proposal.
                            if self.pending.contains_key(&id) {
                                continue;
                            }

                            // Wait for all referenced headers.
                            let wait_for = missing
                                .iter()
                                .map(|x| (x.header_digest.to_vec(), self.store.clone()))
                                .collect();
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(id, (height, tx_cancel));
                            let fut = Self::proposal_waiter(wait_for, (consensus_message, header), rx_cancel);
                            proposal_waiting.push(fut);

                            // Contact the header author first.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            for missing in missing {
                                self.parent_requests.entry(missing.header_digest.clone()).or_insert_with(|| {
                                    requires_sync.push(missing.header_digest);
                                    (missing.height, now)
                                });
                            }
                            if !requires_sync.is_empty() {
                                let address = self.committee
                                    .primary(&author)
                                    .expect("Author of valid header not in the committee")
                                    .primary_to_primary;
                                let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.send_typed(address, Bytes::from(bytes), "HeadersRequest").await;
                            }
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        debug!("Finished synching {:?}", header);
                        let _ = self.pending.remove(&header.id);
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
                        let _ = self.parent_requests.remove(&header.parent_cert.header_digest);

                        self.tx_core.send(header).await.expect("Failed to send header");
                    },
                    Ok(None) => {},
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                Some(result) = proposal_waiting.next() => match result {
                    Ok(Some(deliver)) => {
                        let id = proposal_digest(&deliver.0);
                        let _ = self.pending.remove(&id);
                        for x in deliver.1.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }

                        let possibly_missing = match &deliver.0 {
                            ConsensusMessage::Prepare {view: _, slot: _, tc: _, qc_ticket: _, proposals} => proposals,
                            ConsensusMessage::Confirm {view: _, slot: _, qc: _, proposals} => proposals,
                            ConsensusMessage::Commit {view: _, slot: _, qc: _, proposals} => proposals,
                        };
                        for (_, prop) in possibly_missing.iter() {
                            let _ = self.parent_requests.remove(&prop.header_digest);
                        }

                        self.tx_consensus_loopback.send(deliver).await.expect("Failed to send header");
                    },
                    Ok(None) => {},
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                () = &mut timer => {
                    // Broadcast requests whose targeted retry timed out.
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    // Header requests use a separate retry path.
                    let mut retry = Vec::new();
                    for (digest, (_, timestamp)) in &self.parent_requests {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting retry sync for parent header {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    let addresses = self.committee.others_primaries(&self.name).iter().map(|(_, x)| x.primary_to_primary).collect();
                    let message = PrimaryMessage::HeadersRequest(retry, self.name);
                    let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                    self.network.lucky_broadcast_typed(addresses, Bytes::from(bytes), self.sync_retry_nodes, "HeadersRequest").await;

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let mut gc_round = round - self.gc_depth;

                for (r, handler) in self.pending.values() {
                    if r <= &gc_round {
                        let _ = handler.send(()).await;
                    }
                }
                self.pending.retain(|_, (r, _)| r > &mut gc_round);
                self.batch_requests.retain(|_, r| r > &mut gc_round);
                self.parent_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.header_requests.retain(|_, (r, _)| r > &mut gc_round);
            }
        }
    }
}
