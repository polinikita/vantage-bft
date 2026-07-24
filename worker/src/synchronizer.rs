// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PairwiseKeys, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use primary::PrimaryWorkerMessage;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom as _;
use rand::SeedableRng as _;
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    /// Loosely keep track of the primary's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,
    /// Starfish-parity real transaction latency (PHASE2-SPEC.md #5). Always present
    /// (the metrics server and its registered gauge shape are always on), but only
    /// observed into under the `benchmark` feature -- genuinely unused (not dead
    /// code to delete) on the default build, hence the feature-scoped allow.
    #[cfg_attr(not(feature = "benchmark"), allow(dead_code))]
    metrics: Arc<Metrics>,
    /// Batch digests already accounted for in `metrics`, so a `Committed` notification
    /// for the same digest (should one ever arrive twice) is not double-counted.
    /// Benchmark-only; unbounded for the run's duration, which is fine at benchmark
    /// batch-count scale (a run is seconds to minutes, not committed-batches-forever).
    #[cfg(feature = "benchmark")]
    observed_commits: HashSet<Digest>,
    /// SECURITY (Fable audit): `Parameters::authenticate_channels`. `None` is
    /// byte-identical to pre-MAC behavior.
    channel_auth: Option<Arc<PairwiseKeys>>,
    /// Only consulted when `channel_auth` is `Some`, to shuffle+truncate the retry
    /// broadcast's own `(PublicKey, SocketAddr)` pairs ourselves (needed so each
    /// destination gets its own per-destination tag -- `SimpleSender::
    /// lucky_broadcast_typed`'s internal shuffle only ever sees plain `SocketAddr`s,
    /// with no way to carry a distinct tag per address). A no-op source of randomness
    /// otherwise (this is a best-effort "pick some random peers to retry against"
    /// selection, not a determinism-sensitive one).
    rng: SmallRng,
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
        // Fable audit item 4 (WAN latency injection): this authority's own
        // per-destination artificial latency map (same contract/construction as
        // `BatchMaker::spawn`'s -- see its doc comment). Applied to worker-to-worker
        // sync requests, previously undelayed even under a WAN-shaped run.
        latency_map: HashMap<SocketAddr, Duration>,
        metrics: Arc<Metrics>,
        // METRICS-DASHBOARD-SPEC.md §8: appended last, same convention as `metrics`.
        compress_network: bool,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
        // SECURITY (Fable audit): appended last, same convention as every other
        // MAC-consuming `::spawn`.
        channel_auth: Option<Arc<PairwiseKeys>>,
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
                    .with_latency(latency_map)
                    .with_metrics(metrics.clone())
                    .with_compression(compress_network)
                    .with_batching(batch),
                round: Round::default(),
                pending: HashMap::new(),
                metrics,
                #[cfg(feature = "benchmark")]
                observed_commits: HashSet::new(),
                channel_auth,
                rng: SmallRng::from_entropy(),
            }
            .run()
            .await;
        });
    }

    /// SECURITY (Fable audit): appends `dest`'s tag (byte-identical, unappended, when
    /// `channel_auth` is off) to `payload`, keyed `k_{self.name, dest}`.
    fn tagged(&self, dest: &PublicKey, payload: Vec<u8>) -> Bytes {
        match &self.channel_auth {
            None => Bytes::from(payload),
            Some(auth) => {
                let tag = auth
                    .tag_for(dest, &payload)
                    .expect("dest is a committee member");
                let mut tagged = payload;
                tagged.extend_from_slice(&tag);
                Bytes::from(tagged)
            }
        }
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
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

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            // Ensure we do not send twice the same sync request.
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // Check if we received the batch in the meantime.
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {
                                    // The batch arrived in the meantime: no need to request it.
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(digest, (self.round, tx_cancel, now));
                        }

                        // Send sync request to a single node. If this fails, we will send it
                        // to other nodes when a timer times out.
                        let address = match self.committee.worker(&target, &self.id) {
                            Ok(address) => address.worker_to_worker,
                            Err(e) => {
                                error!("The primary asked us to sync with an unknown node: {}", e);
                                continue;
                            }
                        };
                        let message = WorkerMessage::BatchRequest(missing, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        let tagged = self.tagged(&target, serialized);
                        self.network.send_typed(address, tagged, "BatchRequest").await;
                    },
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // Keep track of the primary's round number.
                        self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for (r, handler, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        self.pending.retain(|_, (r, _, _)| r > &mut gc_round);
                    }
                    // Benchmark-only: extracting/observing per-tx timestamps on every
                    // committed batch is pure overhead outside instrumented runs --
                    // both parameters genuinely go unused on the default build, hence
                    // the feature-scoped allow (not dead code to delete).
                    #[cfg_attr(not(feature = "benchmark"), allow(unused_variables))]
                    PrimaryWorkerMessage::Committed(commit_millis, digests) => {
                        // Starfish-parity real transaction latency (PHASE2-SPEC.md #5).
                        #[cfg(feature = "benchmark")]
                        self.observe_committed(commit_millis, digests).await;
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                        // The sync request for this batch has been canceled.
                    },
                    Err(e) => error!("{}", e)
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to a bunch of other nodes (selected at random).
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        match self.channel_auth.clone() {
                            None => {
                                let addresses = self.committee
                                    .others_workers(&self.name, &self.id)
                                    .iter().map(|(_, address)| address.worker_to_worker)
                                    .collect();
                                self.network
                                    .lucky_broadcast_typed(addresses, Bytes::from(serialized), self.sync_retry_nodes, "BatchRequest")
                                    .await;
                            }
                            // SECURITY (Fable audit): each destination needs its own
                            // per-destination tag (`k_{self.name, dest}`), so this
                            // can't reuse `lucky_broadcast_typed`'s single-`Bytes`-
                            // shared-across-addresses convenience -- shuffle+truncate
                            // ourselves, then unicast each with its own tag appended.
                            Some(auth) => {
                                let mut peers: Vec<(PublicKey, SocketAddr)> = self.committee
                                    .others_workers(&self.name, &self.id)
                                    .iter().map(|(pk, address)| (*pk, address.worker_to_worker))
                                    .collect();
                                peers.shuffle(&mut self.rng);
                                peers.truncate(self.sync_retry_nodes);
                                for (peer, addr) in peers {
                                    let tag = auth.tag_for(&peer, &serialized).expect("peer is a committee member");
                                    let mut tagged = serialized.clone();
                                    tagged.extend_from_slice(&tag);
                                    self.network.send_typed(addr, Bytes::from(tagged), "BatchRequest").await;
                                }
                            }
                        }
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }

    /// Starfish-parity real transaction latency (PHASE2-SPEC.md #5, amended): for each
    /// batch the primary just told us was committed, read it from our local store (a
    /// miss -- possible even for our own batches under GC, and expected for a remote
    /// author's batch we never received a worker-to-worker gossip copy of -- is
    /// skipped, counted, never blocked on), then observe every transaction's
    /// (`commit_millis` - embedded submission timestamp) into the latency histogram.
    /// `commit_millis` is the primary's own instant, taken once at its "Committed"
    /// log site and carried in the notification -- not `SystemTime::now()` read here,
    /// which would additionally include the primary->worker notification hop and this
    /// task's own queueing delay under load (exactly the bias that made the first
    /// version of this metric run measurably hotter than the legacy sample metric).
    #[cfg(feature = "benchmark")]
    async fn observe_committed(&mut self, commit_millis: u64, digests: Vec<Digest>) {
        // Accumulated locally and flushed once at the end of the call (one atomic
        // `inc_by` per counter instead of one per transaction) -- at 240k tx/s that's
        // the difference between a handful of atomic ops per `Committed` message and
        // one per transaction. Only the histogram observation is inherently
        // per-transaction (each has its own latency value); it's already a lock-free
        // channel push, not an atomic increment, so batching it wouldn't help.
        let mut squared_micros_sum: u64 = 0;
        let mut tx_count: u64 = 0;
        let mut tx_bytes: u64 = 0;

        for digest in digests {
            // Dedup: a digest we've already accounted for is a no-op.
            if !self.observed_commits.insert(digest.clone()) {
                continue;
            }

            let bytes = match self.store.read(digest.to_vec()).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => {
                    self.metrics.latency_misses.inc();
                    continue;
                }
                Err(e) => {
                    error!("{}", e);
                    continue;
                }
            };

            let message: WorkerMessage = match bincode::deserialize(&bytes) {
                Ok(message) => message,
                Err(e) => {
                    error!("Failed to deserialize committed batch {}: {}", digest, e);
                    continue;
                }
            };
            let WorkerMessage::Batch(transactions) = message else {
                continue;
            };

            for tx in transactions {
                // §4 wire format: [1 B marker][8 B id, BE][8 B submission timestamp, LE].
                // A transaction shorter than the header (should not happen once every
                // client is on the Phase-2 format) is skipped rather than indexed into.
                if tx.len() < 17 {
                    continue;
                }
                let submitted_millis = u64::from_le_bytes(tx[9..17].try_into().unwrap());
                // saturating_sub: tolerate any clock skew between client and node
                // instead of panicking (NTP-grade sync is assumed, not enforced).
                let latency = Duration::from_millis(commit_millis.saturating_sub(submitted_millis));

                self.metrics.transaction_committed_latency.observe(latency);
                let latency_micros = latency.as_micros() as u64;
                squared_micros_sum = squared_micros_sum
                    .saturating_add(latency_micros.saturating_mul(latency_micros));
                tx_count += 1;
                tx_bytes += tx.len() as u64;
            }
        }

        if tx_count > 0 {
            self.metrics
                .transaction_committed_latency_squared_micros
                .inc_by(squared_micros_sum);
            self.metrics.committed_transactions.inc_by(tx_count);
            self.metrics.committed_bytes.inc_by(tx_bytes);
        }
    }
}
