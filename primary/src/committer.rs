use crate::messages::ConsensusMessage;
#[cfg(feature = "benchmark")]
use crate::primary::PrimaryWorkerMessage;
use crate::primary::{Slot, CHANNEL_CAPACITY};
use crate::synchronizer::Synchronizer;
use crate::{Certificate, Header, Height};
#[cfg(feature = "benchmark")]
use bytes::Bytes;
use config::Committee;
#[cfg(feature = "benchmark")]
use config::WorkerId;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::Hash as _;
use crypto::PublicKey;
use log::{debug, info};
use metrics::Metrics;
use network::BatchConfig;
#[cfg(feature = "benchmark")]
use network::SimpleSender;
use std::borrow::BorrowMut;
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "benchmark")]
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// The state that needs to be persisted for crash-recovery.
struct State {
    // Keeps the last committed height for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_executed_heights: HashMap<PublicKey, Height>,
    // Log containing slots and committed certificates
    log: HashMap<Slot, ConsensusMessage>,
    // The last executed slot
    last_executed_slot: Slot,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_executed_heights: genesis.keys().map(|x| (*x, 0)).collect(),
            log: HashMap::new(),
            last_executed_slot: 0,
        }
    }
}

pub struct Committer {
    rx_mempool: Receiver<Certificate>,
    rx_deliver: Receiver<Certificate>,
    rx_commit_message: Receiver<ConsensusMessage>,
    tx_output: Sender<Header>,
    synchronizer: Synchronizer,
    genesis: Vec<Certificate>,
    /// Our own local workers' `primary_to_worker` addresses, keyed by id (benchmark-only:
    /// used to notify a worker of the batches it just saw committed, PHASE2-SPEC.md #5).
    #[cfg(feature = "benchmark")]
    worker_addresses: HashMap<WorkerId, SocketAddr>,
    #[cfg(feature = "benchmark")]
    network: SimpleSender,
}

impl Committer {
    // clippy::too_many_arguments: this is a `::spawn` constructor for an audited,
    // long-lived task -- every parameter is a distinct wired channel/dependency with
    // no natural sub-grouping; bundling them into a params struct would only move the
    // same argument list one level of indirection away and churn every call site
    // (same call as `Core::spawn`/`VantageCore::spawn`'s existing allows).
    #[allow(clippy::too_many_arguments)]
    // `name`/`metrics` are only read under `#[cfg(feature = "benchmark")]` below
    // (worker-notification wiring), so they're unused on the default build;
    // `store`/`gc_depth`/`rx_commit` are genuinely unused in every build (kept, not
    // removed, to avoid touching the one call site in primary.rs for parameters with
    // no correctness weight either way -- same reasoning as
    // `Synchronizer::tx_certificate_waiter`).
    #[allow(unused_variables)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        gc_depth: Height,
        rx_mempool: Receiver<Certificate>,
        rx_commit: Receiver<Certificate>,
        rx_commit_message: Receiver<ConsensusMessage>,
        tx_output: Sender<Header>,
        synchronizer: Synchronizer,
        // METRICS-DASHBOARD-SPEC.md §1: appended last, same convention as `Core::spawn`.
        metrics: Arc<Metrics>,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
    ) {
        let (_tx_deliver, rx_deliver) = channel(CHANNEL_CAPACITY);

        let genesis = Certificate::genesis(&committee);

        //special blocks from round >1 can also have genesis as parent!!! ==> Solution: Write genesis to store
        //Alternatively, just store genesis digests and compare against
        //let genesis_digests = genesis.clone().iter().map(|x| x.digest()).collect();

        #[cfg(feature = "benchmark")]
        let worker_addresses: HashMap<WorkerId, SocketAddr> = committee
            .our_workers_by_id(&name)
            .expect("Our public key or worker id is not in the committee")
            .into_iter()
            .map(|(id, address)| (id, address.primary_to_worker))
            .collect();

        tokio::spawn(async move {
            Self {
                rx_mempool,
                rx_deliver,
                rx_commit_message,
                tx_output,
                synchronizer,
                genesis,
                #[cfg(feature = "benchmark")]
                worker_addresses,
                #[cfg(feature = "benchmark")]
                network: SimpleSender::new()
                    .with_metrics(metrics)
                    .with_batching(batch),
            }
            .run()
            .await;
        });
    }

    async fn process_commit_message(
        &mut self,
        state: &mut State,
        commit_message: ConsensusMessage,
    ) {
        if let ConsensusMessage::Commit {
            slot,
            view: _,
            qc: _,
            proposals: _,
        } = commit_message.clone()
        {
            if slot <= state.last_executed_slot {
                debug!("Already committed slot {}", slot);
                return;
            }

            // Store the commit message if all proposals are ready to be processed
            state.log.insert(slot, commit_message);

            while state.log.contains_key(&(state.last_executed_slot + 1)) {
                let current_commit_message =
                    state.log.get(&(state.last_executed_slot + 1)).unwrap();
                debug!(
                    "Currently executing slot {:?}",
                    state.last_executed_slot + 1
                );
                if let ConsensusMessage::Commit {
                    slot: _,
                    view: _,
                    qc: _,
                    proposals,
                } = current_commit_message
                {
                    for (pk, proposal) in proposals {
                        let stop_height = *state.last_executed_heights.get(pk).unwrap();
                        // Don't execute proposals which are too old
                        if proposal.height <= stop_height {
                            debug!("skipping this proposal because it's too old");
                            continue;
                        }

                        let headers = self
                            .synchronizer
                            .get_all_headers_for_proposal(proposal.clone(), stop_height)
                            .await
                            .expect("should have ancestors by now");

                        // Update last executed height for the lane
                        if proposal.height > stop_height {
                            state.last_executed_heights.insert(*pk, proposal.height);
                        }

                        // Commit all of the headers
                        for header in headers {
                            info!("Committed {}", header);
                            #[cfg(feature = "benchmark")]
                            {
                                for digest in header.payload.keys() {
                                    // NOTE: This log entry is used to compute performance.
                                    info!("Committed {} -> {:?}", header, digest);
                                }

                                // Commit instant (PHASE2-SPEC.md #5, amended): taken
                                // once per header, right at the "Committed" log site,
                                // and carried in the notification itself so the
                                // worker's latency measurement is submission -> this
                                // exact instant -- not submission -> whenever the
                                // worker's queue got around to the notification.
                                let commit_millis = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Failed to measure time")
                                    .as_millis()
                                    as u64;

                                // Notify our own local worker(s), grouped by
                                // WorkerId, of the batches just committed so they
                                // can extract real transaction latency
                                // (PHASE2-SPEC.md #5). Routed to *our* worker with
                                // the same id as the header author's -- batches are
                                // gossiped worker-to-worker by matching id, so our
                                // local worker likely holds a replica even for a
                                // remote author's batch; a store miss is fine
                                // (worker-side `latency_misses`, never blocks).
                                let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
                                for (digest, worker_id) in header.payload.iter() {
                                    by_worker
                                        .entry(*worker_id)
                                        .or_default()
                                        .push(digest.clone());
                                }
                                for (worker_id, digests) in by_worker {
                                    if let Some(address) = self.worker_addresses.get(&worker_id) {
                                        let bytes =
                                            bincode::serialize(&PrimaryWorkerMessage::Committed(
                                                commit_millis,
                                                digests,
                                            ))
                                            .expect("Failed to serialize committed message");
                                        self.network
                                            .send_typed(*address, Bytes::from(bytes), "Committed")
                                            .await;
                                    }
                                }
                            }
                            debug!("Finished Commit");
                            // Output the block to the top-level application.
                            if let Err(e) = self.tx_output.send(header.clone()).await {
                                debug!("Failed to send block through the output channel: {}", e);
                            }
                            debug!("Finish upcall");
                        }
                    }
                    state.last_executed_slot += 1;
                }
            }
        };
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        loop {
            tokio::select! {
                Some(_) = self.rx_mempool.recv() => {},
                Some(commit_message) = self.rx_commit_message.recv() => {
                    self.process_commit_message(state.borrow_mut(), commit_message).await;
                },
                Some(_) = self.rx_deliver.recv() => {}

            }
        }
    }
}
