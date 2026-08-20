// Copyright(C) Facebook, Inc. and its affiliates.
#[cfg(feature = "benchmark")]
use crate::transaction_counts_toward_goodput;
use crate::worker::{Round, WorkerMessage, CHANNEL_CAPACITY};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{debug, error};
use metrics::Metrics;
use network::{BatchConfig, ChannelAuth, SimpleSender};
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
#[cfg(feature = "benchmark")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Sync retry timer resolution, in milliseconds.
const TIMER_RESOLUTION: u64 = 1_000;

/// Deferred benchmark metric retention, in milliseconds.
#[cfg(feature = "benchmark")]
const BENCHMARK_METRICS_RETENTION_MILLIS: u64 = 10 * 60 * 1_000;

/// A committed digest whose batch is not yet in the local store.
#[cfg(feature = "benchmark")]
struct DeferredMiss {
    digest: Digest,
    commit_millis: u64,
    cancel: Receiver<()>,
}

/// Latency totals accumulated before they are flushed to shared counters.
#[cfg(feature = "benchmark")]
#[derive(Default)]
struct BatchLatencyTotals {
    tx_count: u64,
    uncounted_tx_count: u64,
    tx_bytes: u64,
    committed_squared_micros: u64,
    materialised_squared_micros: u64,
    committed_latency_counts: BTreeMap<Duration, usize>,
    materialised_latency_counts: BTreeMap<Duration, usize>,
    #[cfg(feature = "pipeline-tracing")]
    commit_to_materialised_latency_counts: BTreeMap<Duration, usize>,
}

#[cfg(feature = "benchmark")]
impl BatchLatencyTotals {
    fn accumulate(&mut self, other: &Self) {
        self.tx_count += other.tx_count;
        self.uncounted_tx_count += other.uncounted_tx_count;
        self.tx_bytes += other.tx_bytes;
        self.committed_squared_micros = self
            .committed_squared_micros
            .saturating_add(other.committed_squared_micros);
        self.materialised_squared_micros = self
            .materialised_squared_micros
            .saturating_add(other.materialised_squared_micros);
        for (latency, count) in &other.committed_latency_counts {
            *self.committed_latency_counts.entry(*latency).or_default() += *count;
        }
        for (latency, count) in &other.materialised_latency_counts {
            *self
                .materialised_latency_counts
                .entry(*latency)
                .or_default() += *count;
        }
        #[cfg(feature = "pipeline-tracing")]
        for (latency, count) in &other.commit_to_materialised_latency_counts {
            *self
                .commit_to_materialised_latency_counts
                .entry(*latency)
                .or_default() += *count;
        }
    }
}

/// Result of reading and decoding one committed batch.
#[cfg(feature = "benchmark")]
enum BatchReadOutcome {
    Hit(BatchLatencyTotals),
    Miss,
    Error,
}

/// Current epoch time in milliseconds, matching commit and submission timestamps.
#[cfg(feature = "benchmark")]
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis() as u64
}

pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// Persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// Delay between sync retries, in milliseconds.
    sync_retry_delay: u64,
    /// Number of random peers used to materialize committed data.
    sync_retry_nodes: usize,
    /// Commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// Sends requests to other workers.
    network: SimpleSender,
    /// Primary round used for cleanup.
    round: Round,
    /// Digests awaiting their batches, with cleanup round, request time,
    /// exact protocol-derived repair policy, and escalation attempt.
    pending: HashMap<Digest, (Round, Sender<()>, u128, SyncPolicy, u32)>,
}

/// Deterministic escalation ladder over proof-holder targets: attempt 0
/// contacts one holder, attempt 1 the sync-retry quorum, later attempts all
/// of them. The rotation varies with the attempt so a crashed pick does not
/// stall two rounds.
fn staged_proof_targets(
    sources: &[PublicKey],
    attempt: u32,
    retry_nodes: usize,
    seed: &Digest,
) -> Vec<PublicKey> {
    let width = match attempt {
        0 => 1,
        1 => retry_nodes.max(1),
        _ => sources.len(),
    };
    if width >= sources.len() {
        return sources.to_vec();
    }
    let seed = u64::from_le_bytes(seed.0[..8].try_into().expect("digest holds eight bytes"));
    let start = (seed as usize).wrapping_add(attempt as usize) % sources.len();
    (0..width)
        .map(|i| sources[(start + i) % sources.len()])
        .collect()
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SyncPolicy {
    /// Before commit, the lane author is the only source justified by a
    /// directly received header.
    Author(PublicKey),
    OptimisticLeader(PublicKey),
    ProofSources(Vec<PublicKey>),
    /// Once consensus commits a digest, materialization may use arbitrary
    /// peers without affecting the availability decision.
    Committed,
}

/// Processes benchmark commit metrics without blocking batch synchronization.
#[cfg(feature = "benchmark")]
pub(crate) struct CommitObserver {
    store: Store,
    rx_committed: Receiver<(u64, Vec<Digest>)>,
    metrics: Arc<Metrics>,
    /// Batch digests already counted in the benchmark metrics.
    observed_commits: HashSet<Digest>,
    /// Commit-time index for pruning `observed_commits`.
    observed_commits_order: BTreeSet<(u64, Digest)>,
    /// Committed digests missing from local storage, keyed by commit time.
    pending_misses: BTreeMap<(u64, Digest), Sender<()>>,
}

impl Synchronizer {
    // clippy::too_many_arguments: see primary/src/committer.rs's identical justification.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
        latency_map: HashMap<SocketAddr, Duration>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
        auth: Option<Arc<ChannelAuth>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                id,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new()
                    .with_queue_role("worker_sync")
                    .with_latency(latency_map)
                    .with_metrics(metrics.clone())
                    .with_batching(batch)
                    .with_channel_auth(auth),
                round: Round::default(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Wait for a batch to become available and return its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn synchronize(
        &mut self,
        digests: Vec<Digest>,
        mut policy: SyncPolicy,
        tx_waiter: &Sender<Result<Option<Digest>, StoreError>>,
    ) {
        if let SyncPolicy::ProofSources(sources) = &mut policy {
            sources.sort_unstable();
            sources.dedup();
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();

        let candidates: Vec<Digest> = digests;

        let present = if candidates.is_empty() {
            Vec::new()
        } else {
            self.store
                .read_many(candidates.iter().map(|d| d.to_vec()).collect())
                .await
        };

        let mut missing = Vec::new();
        for (digest, found) in candidates.into_iter().zip(present) {
            if found.is_some() {
                continue;
            }

            if let Some((_, _, timestamp, previous_policy, attempt)) = self.pending.get_mut(&digest)
            {
                // Commit is a one-way boundary: it may widen an existing
                // pre-commit request, but a late header must never narrow a
                // committed materialization request again.
                if *previous_policy != policy && !matches!(previous_policy, SyncPolicy::Committed) {
                    *timestamp = now;
                    *previous_policy = policy.clone();
                    *attempt = 0;
                    missing.push(digest);
                }
                continue;
            }

            missing.push(digest.clone());
            debug!("Requesting sync for batch {}", digest);

            let (tx_cancel, rx_cancel) = channel(1);
            let store = self.store.clone();
            let tx_result = tx_waiter.clone();
            let deliver = digest.clone();
            let missing_key = digest.clone();
            tokio::spawn(async move {
                let result = Self::waiter(missing_key, store, deliver, rx_cancel).await;
                let _ = tx_result.send(result).await;
            });
            self.pending
                .insert(digest, (self.round, tx_cancel, now, policy.clone(), 0));
        }

        if missing.is_empty() {
            return;
        }
        self.send_requests(missing, &policy, 0).await;
    }

    async fn send_requests(&mut self, missing: Vec<Digest>, policy: &SyncPolicy, attempt: u32) {
        let missing_count = missing.len();
        if matches!(policy, SyncPolicy::Committed) {
            let addresses = self
                .committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(_, address)| address.worker_to_worker)
                .collect();
            let message = WorkerMessage::CommittedBatchRequest(missing, self.name);
            let serialized =
                bincode::serialize(&message).expect("Failed to serialize committed batch request");
            debug!(
                "Requesting {} committed batch(es) from {} random peer(s)",
                missing_count, self.sync_retry_nodes
            );
            self.network
                .lucky_broadcast_typed(
                    addresses,
                    Bytes::from(serialized),
                    self.sync_retry_nodes,
                    "CommittedBatchRequest",
                )
                .await;
            return;
        }

        let (targets, message, kind) = match policy {
            SyncPolicy::Author(author) => (
                vec![*author],
                WorkerMessage::BatchRequest(missing, self.name),
                "BatchRequest",
            ),
            SyncPolicy::OptimisticLeader(leader) => (
                vec![*leader],
                WorkerMessage::OptimisticBatchRequest(missing, self.name),
                "OptimisticBatchRequest",
            ),
            SyncPolicy::ProofSources(sources) => {
                let seed = missing.first().cloned().unwrap_or_default();
                (
                    staged_proof_targets(sources, attempt, self.sync_retry_nodes, &seed),
                    WorkerMessage::BatchRequest(missing, self.name),
                    "ProofSourceBatchRequest",
                )
            }
            SyncPolicy::Committed => unreachable!("handled above"),
        };
        debug!(
            "Sending {} {} message(s) to {} protocol-derived target(s)",
            missing_count,
            kind,
            targets.len()
        );
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
        for target in targets {
            if target == self.name {
                continue;
            }
            let address = match self.committee.worker(&target, &self.id) {
                Ok(address) => address.worker_to_worker,
                Err(e) => {
                    error!("The primary asked us to sync with an unknown node: {}", e);
                    continue;
                }
            };
            self.network
                .send_typed(address, Bytes::from(serialized.clone()), kind)
                .await;
        }
    }

    /// Main loop for primary synchronization requests.
    ///
    /// Waiters run in separate tasks so store operations and waiter sends cannot block
    /// this loop. Results return through a bounded channel.
    async fn run(&mut self) {
        let (tx_waiter, mut rx_waiter) =
            channel::<Result<Option<Digest>, StoreError>>(CHANNEL_CAPACITY);

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_message.recv() => match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        self.synchronize(digests, SyncPolicy::Author(target), &tx_waiter).await;
                    },
                    PrimaryWorkerMessage::SynchronizeOptimistic(digests, target) => {
                        self.synchronize(
                            digests,
                            SyncPolicy::OptimisticLeader(target),
                            &tx_waiter,
                        ).await;
                    },
                    PrimaryWorkerMessage::SynchronizeProofSources(digests, sources) => {
                        self.synchronize(
                            digests,
                            SyncPolicy::ProofSources(sources),
                            &tx_waiter,
                        ).await;
                    },
                    PrimaryWorkerMessage::SynchronizeAuthor(digests, author) => {
                        self.synchronize(
                            digests,
                            SyncPolicy::Author(author),
                            &tx_waiter,
                        ).await;
                    },
                    PrimaryWorkerMessage::Cleanup(round) => {
                        self.round = round;

                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for (r, handler, _, _, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        self.pending.retain(|_, (r, _, _, _, _)| r > &mut gc_round);
                    }
                    PrimaryWorkerMessage::Committed(_, digests) => {
                        self.synchronize(digests, SyncPolicy::Committed, &tx_waiter).await;
                    }
                },

                Some(result) = rx_waiter.recv() => match result {
                    Ok(Some(digest)) => {
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {},
                    Err(e) => error!("{}", e)
                },

                () = &mut timer => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp, _, _)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    for digest in &retry {
                        if let Some((_, _, timestamp, _, attempt)) = self.pending.get_mut(digest) {
                            *timestamp = now;
                            *attempt = attempt.saturating_add(1);
                        }
                    }
                    if !retry.is_empty() {
                        let mut by_policy: HashMap<(SyncPolicy, u32), Vec<Digest>> = HashMap::new();
                        for digest in retry {
                            if let Some((_, _, _, policy, attempt)) = self.pending.get(&digest) {
                                by_policy
                                    .entry((policy.clone(), *attempt))
                                    .or_default()
                                    .push(digest);
                            }
                        }
                        for ((policy, attempt), digests) in by_policy {
                            self.send_requests(digests, &policy, attempt).await;
                        }
                    }

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}

#[cfg(feature = "benchmark")]
impl CommitObserver {
    pub(crate) fn spawn(
        store: Store,
        rx_committed: Receiver<(u64, Vec<Digest>)>,
        metrics: Arc<Metrics>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                rx_committed,
                metrics,
                observed_commits: HashSet::new(),
                observed_commits_order: BTreeSet::new(),
                pending_misses: BTreeMap::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let (tx_waiter, mut rx_waiter) = channel::<Option<(Digest, u64)>>(CHANNEL_CAPACITY);

        loop {
            tokio::select! {
                Some((commit_millis, digests)) = self.rx_committed.recv() => {
                    for miss in self.observe_committed(commit_millis, digests).await {
                        let store = self.store.clone();
                        let tx_result = tx_waiter.clone();
                        tokio::spawn(async move {
                            let resolved = Self::metrics_waiter(
                                miss.digest,
                                miss.commit_millis,
                                store,
                                miss.cancel,
                            )
                            .await;
                            let _ = tx_result.send(resolved).await;
                        });
                    }
                }

                Some(resolved) = rx_waiter.recv() => {
                    if let Some((digest, commit_millis)) = resolved {
                        self.finish_deferred_retry(digest, commit_millis).await;
                    }
                }
            }
        }
    }

    /// Record committed transaction metrics and defer missing batches until they arrive.
    /// The committed latency uses the primary's commit time; materialised latency uses
    /// the local read time. Both are collected only while metrics are active.
    #[cfg(feature = "benchmark")]
    async fn observe_committed(
        &mut self,
        commit_millis: u64,
        digests: Vec<Digest>,
    ) -> Vec<DeferredMiss> {
        self.prune_stale(commit_millis).await;

        if !self.metrics.metrics_active.load(Ordering::Relaxed) {
            return Vec::new();
        }

        let materialised_now_millis = now_millis();

        let mut totals = BatchLatencyTotals::default();
        let mut deferred = Vec::new();
        let mut unique = HashSet::new();
        let candidates: Vec<Digest> = digests
            .into_iter()
            .filter(|digest| {
                !self.observed_commits.contains(digest) && unique.insert(digest.clone())
            })
            .collect();
        let present = self
            .store
            .read_many(candidates.iter().map(|digest| digest.to_vec()).collect())
            .await;

        for (digest, bytes) in candidates.into_iter().zip(present) {
            match bytes.map_or(BatchReadOutcome::Miss, |bytes| {
                self.observe_batch_bytes(&digest, &bytes, commit_millis, materialised_now_millis)
            }) {
                BatchReadOutcome::Hit(batch_totals) => {
                    self.mark_observed(digest, commit_millis);
                    totals.accumulate(&batch_totals);
                }
                BatchReadOutcome::Miss => {
                    self.metrics.latency_misses.inc();
                    let (cancel_tx, cancel_rx) = channel(1);
                    self.pending_misses
                        .insert((commit_millis, digest.clone()), cancel_tx);
                    deferred.push(DeferredMiss {
                        digest,
                        commit_millis,
                        cancel: cancel_rx,
                    });
                }
                BatchReadOutcome::Error => {}
            }
        }

        self.flush_totals(&totals);
        deferred
    }

    /// Record metrics after a deferred batch arrives in the store.
    #[cfg(feature = "benchmark")]
    async fn finish_deferred_retry(&mut self, digest: Digest, commit_millis: u64) {
        self.pending_misses.remove(&(commit_millis, digest.clone()));

        if self.observed_commits.contains(&digest) {
            return;
        }
        if !self.metrics.metrics_active.load(Ordering::Relaxed) {
            return;
        }

        let materialised_now_millis = now_millis();
        match self
            .read_and_observe_batch(&digest, commit_millis, materialised_now_millis)
            .await
        {
            BatchReadOutcome::Hit(totals) => {
                self.mark_observed(digest, commit_millis);
                self.metrics.latency_misses_resolved.inc();
                self.flush_totals(&totals);
            }
            BatchReadOutcome::Miss => {
                log::warn!(
                    "Deferred batch {} still missing immediately after its store \
                     write notification fired; dropping (will not retry again)",
                    digest
                );
            }
            BatchReadOutcome::Error => {}
        }
    }

    /// Prune old benchmark metric state and cancel its waiters.
    #[cfg(feature = "benchmark")]
    async fn prune_stale(&mut self, now_millis: u64) {
        let floor = now_millis.saturating_sub(BENCHMARK_METRICS_RETENTION_MILLIS);

        let kept = self
            .observed_commits_order
            .split_off(&(floor, Digest::default()));
        let stale = std::mem::replace(&mut self.observed_commits_order, kept);
        for (_, digest) in stale {
            self.observed_commits.remove(&digest);
        }

        let kept = self.pending_misses.split_off(&(floor, Digest::default()));
        let stale = std::mem::replace(&mut self.pending_misses, kept);
        for (_, cancel_tx) in stale {
            let _ = cancel_tx.send(()).await;
        }
    }

    /// Mark a digest as counted and index it by commit time.
    #[cfg(feature = "benchmark")]
    fn mark_observed(&mut self, digest: Digest, commit_millis: u64) {
        self.observed_commits.insert(digest.clone());
        self.observed_commits_order.insert((commit_millis, digest));
    }

    /// Read a batch, record its transaction latency, and return aggregate counters.
    #[cfg(feature = "benchmark")]
    async fn read_and_observe_batch(
        &mut self,
        digest: &Digest,
        commit_millis: u64,
        materialised_now_millis: u64,
    ) -> BatchReadOutcome {
        let bytes = match self.store.read(digest.to_vec()).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return BatchReadOutcome::Miss,
            Err(e) => {
                error!("{}", e);
                return BatchReadOutcome::Error;
            }
        };

        self.observe_batch_bytes(digest, &bytes, commit_millis, materialised_now_millis)
    }

    #[cfg(feature = "benchmark")]
    fn observe_batch_bytes(
        &self,
        digest: &Digest,
        bytes: &[u8],
        commit_millis: u64,
        materialised_now_millis: u64,
    ) -> BatchReadOutcome {
        let message: WorkerMessage = match bincode::deserialize(bytes) {
            Ok(message) => message,
            Err(e) => {
                error!("Failed to deserialize committed batch {}: {}", digest, e);
                return BatchReadOutcome::Error;
            }
        };
        let WorkerMessage::Batch(transactions) = message else {
            return BatchReadOutcome::Hit(BatchLatencyTotals::default());
        };

        let mut totals = BatchLatencyTotals::default();
        for tx in transactions {
            // Format: marker, big-endian ID, little-endian submission timestamp.
            if tx.len() < 17 {
                continue;
            }
            let submitted_millis = u64::from_le_bytes(tx[9..17].try_into().unwrap());
            if !self.metrics.counts_toward_metrics(submitted_millis) {
                continue;
            }
            if !transaction_counts_toward_goodput(&tx) {
                totals.uncounted_tx_count += 1;
                continue;
            }
            let committed_latency =
                Duration::from_millis(commit_millis.saturating_sub(submitted_millis));
            let materialised_latency =
                Duration::from_millis(materialised_now_millis.saturating_sub(submitted_millis));
            #[cfg(feature = "pipeline-tracing")]
            let commit_to_materialised_latency =
                Duration::from_millis(materialised_now_millis.saturating_sub(commit_millis));

            *totals
                .committed_latency_counts
                .entry(committed_latency)
                .or_default() += 1;
            *totals
                .materialised_latency_counts
                .entry(materialised_latency)
                .or_default() += 1;
            #[cfg(feature = "pipeline-tracing")]
            {
                *totals
                    .commit_to_materialised_latency_counts
                    .entry(commit_to_materialised_latency)
                    .or_default() += 1;
            }

            let committed_micros = committed_latency.as_micros() as u64;
            let materialised_micros = materialised_latency.as_micros() as u64;
            totals.committed_squared_micros = totals
                .committed_squared_micros
                .saturating_add(committed_micros.saturating_mul(committed_micros));
            totals.materialised_squared_micros = totals
                .materialised_squared_micros
                .saturating_add(materialised_micros.saturating_mul(materialised_micros));
            totals.tx_count += 1;
            totals.tx_bytes += tx.len() as u64;
        }
        BatchReadOutcome::Hit(totals)
    }

    /// Flush accumulated transaction totals into shared counters.
    #[cfg(feature = "benchmark")]
    fn flush_totals(&self, totals: &BatchLatencyTotals) {
        if totals.tx_count == 0 && totals.uncounted_tx_count == 0 {
            return;
        }
        for (latency, count) in &totals.committed_latency_counts {
            self.metrics
                .transaction_committed_latency
                .observe_n(*latency, *count);
        }
        for (latency, count) in &totals.materialised_latency_counts {
            self.metrics
                .transaction_materialised_latency
                .observe_n(*latency, *count);
        }
        #[cfg(feature = "pipeline-tracing")]
        for (latency, count) in &totals.commit_to_materialised_latency_counts {
            self.metrics
                .pipeline
                .transaction_commit_to_materialised_latency
                .observe_n(*latency, *count);
        }
        self.metrics
            .transaction_committed_latency_squared_micros
            .inc_by(totals.committed_squared_micros);
        self.metrics
            .transaction_materialised_latency_squared_micros
            .inc_by(totals.materialised_squared_micros);
        self.metrics.committed_transactions.inc_by(totals.tx_count);
        self.metrics
            .committed_uncounted_transactions
            .inc_by(totals.uncounted_tx_count);
        self.metrics.committed_bytes.inc_by(totals.tx_bytes);
    }

    /// Wait for a deferred batch or cancellation.
    #[cfg(feature = "benchmark")]
    async fn metrics_waiter(
        digest: Digest,
        commit_millis: u64,
        mut store: Store,
        mut cancel: Receiver<()>,
    ) -> Option<(Digest, u64)> {
        tokio::select! {
            result = store.notify_read(digest.to_vec()) => {
                result.ok().map(|_| (digest, commit_millis))
            }
            _ = cancel.recv() => None,
        }
    }
}
