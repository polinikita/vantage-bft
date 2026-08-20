// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{proposal_digest, ConsensusMessage, Header, Proposal};
use crate::primary::{Height, PrimaryMessage, PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Blake3Hasher, Digest, Hash as _, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use metrics::Metrics;
use network::{BatchConfig, ChannelAuth, SimpleSender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// Timer resolution for retrying sync requests.
const TIMER_RESOLUTION: u64 = 1_000;

type BatchRequestGroups = HashMap<WorkerId, Vec<Digest>>;
type OptimisticTipSources = HashMap<Digest, (PublicKey, Height)>;
type CertifiedLaneSources = HashMap<PublicKey, (Vec<PublicKey>, Height)>;

fn record_certified_lane_sources(
    known: &mut CertifiedLaneSources,
    lane: PublicKey,
    mut sources: Vec<PublicKey>,
    tip_height: Height,
) {
    sources.sort_unstable();
    sources.dedup();
    known
        .entry(lane)
        .and_modify(|(existing, known_height)| {
            existing.append(&mut sources);
            existing.sort_unstable();
            existing.dedup();
            *known_height = (*known_height).max(tip_height);
        })
        .or_insert((sources, tip_height));
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BatchSyncSource {
    Author(PublicKey),
    OptimisticLeader(PublicKey),
    ProofSources(Vec<PublicKey>),
}

#[derive(Clone, Debug)]
struct PendingProposalRequest {
    proposal: Proposal,
    stop_height: Height,
    sources: Vec<PublicKey>,
    requested_at: u128,
}

fn proposal_request_needs_update(
    pending: Option<&PendingProposalRequest>,
    proposal: &Proposal,
    stop_height: Height,
    sources: &[PublicKey],
) -> bool {
    pending.is_none_or(|pending| {
        pending.proposal != *proposal
            || pending.stop_height > stop_height
            || pending.sources != sources
    })
}

#[derive(Clone, Debug)]
struct PendingPayload {
    author: PublicKey,
    height: Height,
    missing: HashMap<Digest, WorkerId>,
}

type PendingPayloads = HashMap<Digest, PendingPayload>;

fn register_batch_requests(
    tracked: &mut HashMap<Digest, (Height, BatchSyncSource)>,
    missing: &HashMap<Digest, WorkerId>,
    height: Height,
    source: BatchSyncSource,
) -> BatchRequestGroups {
    let mut groups = HashMap::new();
    for (digest, worker_id) in missing {
        if tracked
            .get(digest)
            .is_some_and(|(_, previous_source)| *previous_source == source)
        {
            continue;
        }
        tracked.insert(digest.clone(), (height, source.clone()));
        groups
            .entry(*worker_id)
            .or_insert_with(Vec::new)
            .push(digest.clone());
    }
    groups
}

fn proposal_waiter_id(message: &ConsensusMessage) -> Digest {
    let phase = message.digest();
    let proposals = proposal_digest(message);
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"autobahn-proposal-waiter");
    hasher.update(&phase.0);
    hasher.update(&proposals.0);
    Digest(hasher.finalize().into())
}

fn register_optimistic_tip_sources(
    sources: &mut OptimisticTipSources,
    missing: &[(PublicKey, Proposal)],
    proposal_leader: PublicKey,
) {
    for (_, proposal) in missing {
        sources.insert(
            proposal.header_digest.clone(),
            (proposal_leader, proposal.height),
        );
    }
}

fn merge_batch_request_groups(destination: &mut BatchRequestGroups, source: BatchRequestGroups) {
    for (worker, mut digests) in source {
        destination.entry(worker).or_default().append(&mut digests);
    }
}

/// Commands sent to the waiter.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header, bool),
    /// Certified suffixes and their exclusive executed-height bound.
    SyncCertified(Vec<(PublicKey, Proposal, Height)>),
    /// Missing optimistic tips, the elected leader, and an optional Prepare
    /// that must resume once the tips themselves are present.
    SyncOptimistic(
        Vec<(PublicKey, Proposal)>,
        PublicKey,
        Option<(ConsensusMessage, Header)>,
    ),
    /// Repairs an implicitly certified winning tip asynchronously from the
    /// replicas named by its TC or PrepareQC evidence.
    SyncImplicit(Vec<(PublicKey, Proposal)>, Vec<PublicKey>),
    /// A Commit resumes only after all currently missing suffix roots arrive.
    WaitForCommit(Vec<Digest>, ConsensusMessage, Header),
    SyncParent(Digest, Header, Height),
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
    metrics: Arc<Metrics>,

    /// Requests for special parents.
    header_requests: HashMap<Digest, (Height, u128)>,
    /// Batch requests, their heights, and their current target. A Prepare
    /// leader may supersede an earlier request to the Byzantine lane author.
    batch_requests: HashMap<Digest, (Height, BatchSyncSource)>,
    /// Missing payloads retained by lane so a Prepare can request the whole
    /// unavailable prefix concurrently rather than walking one block per RTT.
    pending_payloads: PendingPayloads,
    /// Prepare leader that can relay each missing optimistic tip. The lane
    /// author itself is not a valid liveness dependency when it is Byzantine.
    optimistic_tip_sources: OptimisticTipSources,
    /// Exact TC/PrepareQC witnesses for implicitly certified optimistic tips.
    implicit_tip_sources: HashMap<Digest, (Vec<PublicKey>, Height)>,
    /// PoA voters and covered tip height for each certified lane suffix.
    certified_lane_sources: CertifiedLaneSources,
    /// Whole-suffix requests and their proof-derived retry sources.
    proposal_requests: HashMap<Digest, PendingProposalRequest>,
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
        auth: Option<Arc<ChannelAuth>>,
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
                    .with_queue_role("header_waiter")
                    .with_metrics(metrics.clone())
                    .with_batching(batch)
                    .with_channel_auth(auth),
                metrics,
                header_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending_payloads: HashMap::new(),
                optimistic_tip_sources: HashMap::new(),
                implicit_tip_sources: HashMap::new(),
                certified_lane_sources: HashMap::new(),
                proposal_requests: HashMap::new(),
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
        started: Instant,
    ) -> DagResult<Option<((ConsensusMessage, Header), Duration)>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some((deliver, started.elapsed()))).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn send_batch_requests(
        &mut self,
        target: PublicKey,
        requests: BatchRequestGroups,
        optimistic_leader_repair: bool,
    ) {
        for (worker_id, digests) in requests {
            let address = self
                .committee
                .worker(&self.name, &worker_id)
                .expect("Author of valid header is not in the committee")
                .primary_to_worker;
            debug!(
                "Requesting {} missing batch(es) from {} {}",
                digests.len(),
                if optimistic_leader_repair {
                    "optimistic proposal leader"
                } else {
                    "header author"
                },
                target
            );
            let message = if optimistic_leader_repair {
                PrimaryWorkerMessage::SynchronizeOptimistic(digests, target)
            } else {
                PrimaryWorkerMessage::SynchronizeAuthor(digests, target)
            };
            let bytes =
                bincode::serialize(&message).expect("Failed to serialize batch sync request");
            self.network
                .send_typed(address, Bytes::from(bytes), message.type_name())
                .await;
        }
    }

    async fn send_proof_batch_requests(
        &mut self,
        mut sources: Vec<PublicKey>,
        requests: BatchRequestGroups,
    ) {
        sources.sort_unstable();
        sources.dedup();
        if sources.is_empty() {
            return;
        }
        for (worker_id, digests) in requests {
            let address = self
                .committee
                .worker(&self.name, &worker_id)
                .expect("Local worker is not in the committee")
                .primary_to_worker;
            let message = PrimaryWorkerMessage::SynchronizeProofSources(digests, sources.clone());
            let bytes =
                bincode::serialize(&message).expect("Failed to serialize proof-source batch sync");
            self.network
                .send_typed(address, Bytes::from(bytes), message.type_name())
                .await;
        }
    }

    fn proposal_sources(proposal: &Proposal) -> Vec<PublicKey> {
        let mut sources: Vec<_> = proposal
            .poa
            .as_ref()
            .map(|poa| poa.votes.iter().map(|(author, _)| *author).collect())
            .unwrap_or_default();
        sources.sort_unstable();
        sources.dedup();
        sources
    }

    async fn send_proposal_suffix_request(
        &mut self,
        proposal: Proposal,
        stop_height: Height,
        mut sources: Vec<PublicKey>,
    ) {
        sources.sort_unstable();
        sources.dedup();
        let message =
            PrimaryMessage::ProposalHeadersRequest(proposal.clone(), stop_height, self.name);
        let bytes =
            bincode::serialize(&message).expect("Failed to serialize proposal suffix request");
        for source in &sources {
            if *source == self.name {
                continue;
            }
            let address = self
                .committee
                .primary(source)
                .expect("Proof source is not in the committee")
                .primary_to_primary;
            self.network
                .send_typed(
                    address,
                    Bytes::from(bytes.clone()),
                    "ProposalHeadersRequest",
                )
                .await;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();
        self.proposal_requests.insert(
            proposal.header_digest.clone(),
            PendingProposalRequest {
                proposal,
                stop_height,
                sources,
                requested_at: now,
            },
        );
    }

    /// Starts a request, or retargets an existing request when stronger
    /// protocol evidence names a different source set. An already pending
    /// request that covers a longer suffix is sufficient.
    async fn ensure_proposal_suffix_request(
        &mut self,
        proposal: Proposal,
        stop_height: Height,
        mut sources: Vec<PublicKey>,
    ) {
        sources.sort_unstable();
        sources.dedup();
        let needs_request = proposal_request_needs_update(
            self.proposal_requests.get(&proposal.header_digest),
            &proposal,
            stop_height,
            &sources,
        );
        if needs_request {
            self.send_proposal_suffix_request(proposal, stop_height, sources)
                .await;
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

                            self.pending_payloads
                                .entry(header_id.clone())
                                .and_modify(|pending| {
                                    pending.missing.extend(missing.clone());
                                })
                                .or_insert_with(|| PendingPayload {
                                    author,
                                    height: round,
                                    missing: missing.clone(),
                                });

                            let optimistic_source = self
                                .optimistic_tip_sources
                                .get(&header_id)
                                .copied();
                            let implicit_sources = self
                                .implicit_tip_sources
                                .get(&header_id)
                                .cloned()
                                .map(|(sources, _)| sources);
                            let certified_sources = self
                                .certified_lane_sources
                                .get(&author)
                                .cloned()
                                .filter(|(_, tip_height)| round <= *tip_height)
                                .map(|(sources, _)| sources);

                            // A served copy must be allowed to upgrade an
                            // existing direct-header wait into a fetch before
                            // the waiter itself is deduplicated below.
                            if force_sync
                                || optimistic_source.is_some()
                                || implicit_sources.is_some()
                                || certified_sources.is_some()
                            {
                                // A new-view possession proof supersedes the
                                // original leader-only obligation. This avoids
                                // pinning the new leader to a stale source.
                                if let Some(sources) = implicit_sources {
                                    let source = BatchSyncSource::ProofSources(sources.clone());
                                    let requests = register_batch_requests(
                                        &mut self.batch_requests,
                                        &missing,
                                        round,
                                        source,
                                    );
                                    if !requests.is_empty() {
                                        self.send_proof_batch_requests(sources, requests).await;
                                    }
                                } else if let Some((proposal_leader, _)) = optimistic_source {
                                    let requests = register_batch_requests(
                                        &mut self.batch_requests,
                                        &missing,
                                        round,
                                        BatchSyncSource::OptimisticLeader(proposal_leader),
                                    );
                                    if !requests.is_empty() {
                                        self.send_batch_requests(proposal_leader, requests, true).await;
                                    }
                                } else if let Some(sources) = certified_sources {
                                    let source = BatchSyncSource::ProofSources(sources.clone());
                                    let requests = register_batch_requests(
                                        &mut self.batch_requests,
                                        &missing,
                                        round,
                                        source,
                                    );
                                    if !requests.is_empty() {
                                        self.send_proof_batch_requests(sources, requests).await;
                                    }
                                } else {
                                    let requests = register_batch_requests(
                                        &mut self.batch_requests,
                                        &missing,
                                        round,
                                        BatchSyncSource::Author(author),
                                    );
                                    if !requests.is_empty() {
                                        self.send_batch_requests(author, requests, false).await;
                                    }
                                }
                            }

                            // A served copy upgrades an existing direct wait to
                            // an immediate fetch above, but must not install a
                            // second waiter for the same header.
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
                            let fut = Self::waiter(wait_for, header.clone(), rx_cancel);
                            waiting.push(fut);
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

                        WaiterMessage::SyncParent(missing, header, stop_height) => {
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
                            let fut = Self::waiter(wait_for, header.clone(), rx_cancel);
                            waiting.push(fut);

                            // A car vote implies possession of the parent chain.
                            // Recover it only from the voters named by the parent
                            // PoA, never from timer-selected random peers.
                            let parent = Proposal::certified(header.parent_cert.clone());
                            let sources = Self::proposal_sources(&parent);
                            record_certified_lane_sources(
                                &mut self.certified_lane_sources,
                                author,
                                sources.clone(),
                                parent.height,
                            );
                            self.ensure_proposal_suffix_request(parent, stop_height, sources)
                                .await;
                        }
                        WaiterMessage::SyncCertified(repairs) => {
                            for (lane, proposal, stop_height) in repairs {
                                let sources = Self::proposal_sources(&proposal);
                                record_certified_lane_sources(
                                    &mut self.certified_lane_sources,
                                    lane,
                                    sources.clone(),
                                    proposal.height,
                                );

                                // A header may have arrived before its cut proof.
                                // Upgrade any such payload waits to the exact PoA
                                // voter set immediately.
                                let mut requests = HashMap::new();
                                let source = BatchSyncSource::ProofSources(sources.clone());
                                for pending in self.pending_payloads.values() {
                                    if pending.author == lane
                                        && pending.height > stop_height
                                        && pending.height <= proposal.height
                                    {
                                        merge_batch_request_groups(
                                            &mut requests,
                                            register_batch_requests(
                                                &mut self.batch_requests,
                                                &pending.missing,
                                                pending.height,
                                                source.clone(),
                                            ),
                                        );
                                    }
                                }
                                if !requests.is_empty() {
                                    self.send_proof_batch_requests(sources.clone(), requests)
                                        .await;
                                }

                                self.ensure_proposal_suffix_request(
                                    proposal,
                                    stop_height,
                                    sources,
                                )
                                .await;
                            }
                        }

                        WaiterMessage::SyncOptimistic(missing, proposal_leader, resume) => {
                            register_optimistic_tip_sources(
                                &mut self.optimistic_tip_sources,
                                &missing,
                                proposal_leader,
                            );

                            let mut requests = HashMap::new();
                            for (_, proposal) in &missing {
                                if let Some(pending) = self.pending_payloads.get(&proposal.header_digest) {
                                    merge_batch_request_groups(
                                        &mut requests,
                                        register_batch_requests(
                                            &mut self.batch_requests,
                                            &pending.missing,
                                            pending.height,
                                            BatchSyncSource::OptimisticLeader(proposal_leader),
                                        ),
                                    );
                                }
                            }
                            if !requests.is_empty() {
                                self.send_batch_requests(proposal_leader, requests, true).await;
                            }

                            for (_, proposal) in &missing {
                                self.ensure_proposal_suffix_request(
                                    proposal.clone(),
                                    proposal.height.saturating_sub(1),
                                    vec![proposal_leader],
                                )
                                .await;
                            }

                            if let Some((consensus_message, header)) = resume {
                                let id = proposal_waiter_id(&consensus_message);
                                if !self.pending.contains_key(&id) {
                                    self.metrics.autobahn_prepare_sync_events_total.inc();
                                    self.metrics
                                        .autobahn_prepare_missing_headers_total
                                        .inc_by(missing.len() as u64);
                                    let wait_for = missing
                                        .iter()
                                        .map(|(_, proposal)| {
                                            (proposal.header_digest.to_vec(), self.store.clone())
                                        })
                                        .collect();
                                    let height = missing
                                        .iter()
                                        .map(|(_, proposal)| proposal.height)
                                        .max()
                                        .unwrap_or_default();
                                    let (tx_cancel, rx_cancel) = channel(1);
                                    self.pending.insert(id, (height, tx_cancel));
                                    proposal_waiting.push(Self::proposal_waiter(
                                        wait_for,
                                        (consensus_message, header),
                                        rx_cancel,
                                        Instant::now(),
                                    ));
                                }
                            }
                        }

                        WaiterMessage::SyncImplicit(missing, mut sources) => {
                            sources.sort_unstable();
                            sources.dedup();
                            let mut requests = HashMap::new();
                            for (_, proposal) in &missing {
                                // TC/PrepareQC evidence supersedes a fresh
                                // leader-only fetch and keeps repair asynchronous.
                                self.optimistic_tip_sources
                                    .remove(&proposal.header_digest);
                                self.implicit_tip_sources.insert(
                                    proposal.header_digest.clone(),
                                    (sources.clone(), proposal.height),
                                );
                                if let Some(pending) = self.pending_payloads.get(&proposal.header_digest) {
                                    merge_batch_request_groups(
                                        &mut requests,
                                        register_batch_requests(
                                            &mut self.batch_requests,
                                            &pending.missing,
                                            pending.height,
                                            BatchSyncSource::ProofSources(sources.clone()),
                                        ),
                                    );
                                }
                                self.ensure_proposal_suffix_request(
                                    proposal.clone(),
                                    proposal.height.saturating_sub(1),
                                    sources.clone(),
                                )
                                .await;
                            }
                            if !requests.is_empty() {
                                self.send_proof_batch_requests(sources, requests).await;
                            }
                        }

                        WaiterMessage::WaitForCommit(missing, consensus_message, header) => {
                            let id = proposal_waiter_id(&consensus_message);
                            if !self.pending.contains_key(&id) {
                                let wait_for = missing
                                    .into_iter()
                                    .map(|digest| (digest.to_vec(), self.store.clone()))
                                    .collect();
                                // Direct consensus requests use a synthetic
                                // carrier header at height zero. Retain the wait
                                // according to the committed cut itself instead.
                                let height = match &consensus_message {
                                    ConsensusMessage::Prepare { proposals, .. }
                                    | ConsensusMessage::Confirm { proposals, .. }
                                    | ConsensusMessage::Commit { proposals, .. } => proposals
                                        .values()
                                        .map(|proposal| proposal.height)
                                        .max()
                                        .unwrap_or_else(|| header.height()),
                                };
                                let (tx_cancel, rx_cancel) = channel(1);
                                self.pending.insert(id, (height, tx_cancel));
                                proposal_waiting.push(Self::proposal_waiter(
                                    wait_for,
                                    (consensus_message, header),
                                    rx_cancel,
                                    Instant::now(),
                                ));
                            }
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        debug!("Finished synching {:?}", header);
                        let _ = self.pending.remove(&header.id);
                        let _ = self.pending_payloads.remove(&header.id);
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
                        let _ = self.proposal_requests.remove(&header.id);

                        self.tx_core.send(header).await.expect("Failed to send header");
                    },
                    Ok(None) => {},
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                Some(result) = proposal_waiting.next() => match result {
                    Ok(Some((deliver, elapsed))) => {
                        if matches!(&deliver.0, ConsensusMessage::Prepare { .. }) {
                            self.metrics.autobahn_prepare_sync_completed_total.inc();
                            self.metrics
                                .autobahn_prepare_sync_wait_micros_total
                                .inc_by(elapsed.as_micros().min(u64::MAX as u128) as u64);
                        }
                        let id = proposal_waiter_id(&deliver.0);
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
                            let _ = self.proposal_requests.remove(&prop.header_digest);
                            let _ = self.optimistic_tip_sources.remove(&prop.header_digest);
                            let _ = self.implicit_tip_sources.remove(&prop.header_digest);
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

                    // Retry only the leader or proof voters selected by the
                    // protocol evidence. No arbitrary peer is introduced.
                    let retry: Vec<_> = self.proposal_requests
                        .iter()
                        .filter(|(_, request)| {
                            request.requested_at + (self.sync_retry_delay as u128) < now
                        })
                        .map(|(digest, request)| (digest.clone(), request.clone()))
                        .collect();
                    for (digest, request) in retry {
                        if self.store.read(digest.to_vec()).await.ok().flatten().is_some() {
                            self.proposal_requests.remove(&digest);
                            continue;
                        }
                        self.send_proposal_suffix_request(
                            request.proposal,
                            request.stop_height,
                            request.sources,
                        ).await;
                    }

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
                self.batch_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.pending_payloads
                    .retain(|_, pending| pending.height > gc_round);
                self.optimistic_tip_sources
                    .retain(|_, (_, r)| r > &mut gc_round);
                self.implicit_tip_sources
                    .retain(|_, (_, r)| r > &mut gc_round);
                self.certified_lane_sources
                    .retain(|_, (_, r)| r > &mut gc_round);
                self.proposal_requests
                    .retain(|_, request| request.proposal.height > gc_round);
                self.header_requests.retain(|_, (r, _)| r > &mut gc_round);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certified_forks_retain_all_proof_derived_sources() {
        let keys = crate::common::keys();
        let lane = keys[0].0;
        let mut known = CertifiedLaneSources::new();

        record_certified_lane_sources(&mut known, lane, vec![keys[1].0, keys[2].0], 7);
        record_certified_lane_sources(&mut known, lane, vec![keys[2].0, keys[3].0], 7);

        let (sources, height) = known.get(&lane).unwrap();
        assert_eq!(*height, 7);
        assert_eq!(sources.len(), 3);
        assert!(sources.contains(&keys[1].0));
        assert!(sources.contains(&keys[2].0));
        assert!(sources.contains(&keys[3].0));
    }

    #[test]
    fn batch_requests_group_by_worker_and_deduplicate() {
        let first_target = crate::common::keys()[0].0;
        let replacement_target = crate::common::keys()[1].0;
        let a = Digest([1; 32]);
        let b = Digest([2; 32]);
        let c = Digest([3; 32]);
        let missing = HashMap::from([(a.clone(), 0), (b.clone(), 0), (c.clone(), 1)]);
        let first_source = BatchSyncSource::Author(first_target);
        let replacement_source = BatchSyncSource::OptimisticLeader(replacement_target);
        let mut tracked = HashMap::from([(a.clone(), (4, first_source.clone()))]);

        let mut groups = register_batch_requests(&mut tracked, &missing, 7, first_source.clone());
        let mut worker_zero = groups.remove(&0).unwrap();
        worker_zero.sort();
        assert_eq!(worker_zero, vec![b]);
        assert_eq!(groups.remove(&1), Some(vec![c]));
        assert!(groups.is_empty());
        assert!(register_batch_requests(&mut tracked, &missing, 8, first_source).is_empty());

        let replacement =
            register_batch_requests(&mut tracked, &missing, 8, replacement_source.clone());
        assert_eq!(replacement.values().map(Vec::len).sum::<usize>(), 3);
        assert_eq!(tracked.get(&a), Some(&(8, replacement_source)));

        let third_target = crate::common::keys()[2].0;
        let first_proof = BatchSyncSource::ProofSources(vec![first_target, replacement_target]);
        let second_proof = BatchSyncSource::ProofSources(vec![first_target, third_target]);
        let mut proof_tracked = HashMap::new();
        assert_eq!(
            register_batch_requests(&mut proof_tracked, &missing, 8, first_proof)
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            3
        );
        assert_eq!(
            register_batch_requests(&mut proof_tracked, &missing, 8, second_proof)
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn implicit_evidence_retargets_a_pending_leader_only_header_request() {
        let keys = crate::common::keys();
        let old_leader = keys[0].0;
        let proof_sources = vec![keys[1].0, keys[2].0];
        let proposal = Proposal {
            header_digest: Digest([17; 32]),
            height: 9,
            poa: None,
            ..Default::default()
        };
        let pending = PendingProposalRequest {
            proposal: proposal.clone(),
            stop_height: 8,
            sources: vec![old_leader],
            requested_at: 0,
        };

        assert!(proposal_request_needs_update(
            Some(&pending),
            &proposal,
            8,
            &proof_sources,
        ));
    }

    #[test]
    fn proposal_waiters_do_not_merge_prepare_and_commit() {
        let proposals = HashMap::new();
        let prepare = ConsensusMessage::Prepare {
            slot: 4,
            view: 2,
            tc: None,
            qc_ticket: None,
            proposals: proposals.clone(),
        };
        let commit = ConsensusMessage::Commit {
            slot: 4,
            view: 2,
            qc: Default::default(),
            proposals,
        };

        assert_ne!(proposal_waiter_id(&prepare), proposal_waiter_id(&commit));
    }

    #[test]
    fn prepare_leader_is_recorded_for_every_missing_optimistic_tip() {
        let leader = crate::common::keys()[0].0;
        let first = Proposal {
            header_digest: Digest([7; 32]),
            height: 11,
            poa: None,
            ..Default::default()
        };
        let second = Proposal {
            header_digest: Digest([8; 32]),
            height: 13,
            poa: None,
            ..Default::default()
        };
        let mut sources = HashMap::new();

        register_optimistic_tip_sources(
            &mut sources,
            &[(leader, first.clone()), (leader, second.clone())],
            leader,
        );

        assert_eq!(sources.get(&first.header_digest), Some(&(leader, 11)));
        assert_eq!(sources.get(&second.header_digest), Some(&(leader, 13)));
    }

    #[test]
    fn optimistic_leader_obligation_is_tip_only() {
        let leader = crate::common::keys()[0].0;
        let child = Digest([7; 32]);
        let parent = Digest([8; 32]);
        let sources = HashMap::from([(child.clone(), (leader, 11))]);

        assert_eq!(sources.get(&child), Some(&(leader, 11)));
        assert!(!sources.contains_key(&parent));
    }

    #[test]
    fn certified_repair_sources_are_exactly_the_poa_voters() {
        let header = crate::common::header();
        let mut certificate = crate::common::certificate(&header);
        certificate.votes.truncate(2);
        let expected: Vec<_> = certificate
            .votes
            .iter()
            .map(|(author, _)| *author)
            .collect();

        let proposal = Proposal::certified(certificate);
        let mut sources = HeaderWaiter::proposal_sources(&proposal);
        let mut expected = expected;
        sources.sort_unstable();
        expected.sort_unstable();
        assert_eq!(sources, expected);
    }
}
