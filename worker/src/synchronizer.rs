// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
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

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

/// See `Synchronizer::prune_stale`'s doc comment. No spec gives an exact figure for
/// how long a commit-metrics deferral should be retried before being treated as a
/// permanent loss; this is generous relative to the default `sync_retry_delay` (the
/// worker-to-worker `BatchRequest` retry cadence a miss is expected to resolve
/// within, 5s) and typical sync latency, so a legitimately in-flight sync is never
/// pruned out from under itself, while still bounding memory over a long-running
/// validator. Flagged as an open question in this change's own report rather than
/// asserted as the "correct" figure -- pick a different value if a given run's sync/
/// GC timing calls for one.
#[cfg(feature = "benchmark")]
const BENCHMARK_METRICS_RETENTION_MILLIS: u64 = 10 * 60 * 1_000;

/// clippy::type_complexity: named alias for `metrics_waiting`'s element type in
/// `run` (mirrors `primary::core::SlotViewTimerFuture`'s identical justification).
/// Declared unconditionally, matching `metrics_waiting` itself (see `run`'s doc
/// comment on that variable for why it stays unconditional).
type MetricsRetryFuture = Pin<Box<dyn Future<Output = Option<(Digest, u64)>> + Send>>;

/// A digest whose batch missed the local store at commit time, deferred rather than
/// dropped by `Synchronizer::observe_committed` -- returned to its caller (`run`),
/// which starts a `Synchronizer::metrics_waiter` retry for it. Carries the cancel
/// handle `prune_stale` uses to stop that wait early if the entry goes stale first.
#[cfg(feature = "benchmark")]
struct DeferredMiss {
    digest: Digest,
    commit_millis: u64,
    cancel: Receiver<()>,
}

/// Per-batch (or, in `observe_committed`'s hot loop, accumulated across every batch
/// in one `Committed` notification) latency totals, folded into the shared counters
/// by `Synchronizer::flush_totals` -- see `Synchronizer::read_and_observe_batch`'s
/// doc comment for why this is split out.
#[cfg(feature = "benchmark")]
#[derive(Default)]
struct BatchLatencyTotals {
    tx_count: u64,
    tx_bytes: u64,
    committed_squared_micros: u64,
    materialised_squared_micros: u64,
}

#[cfg(feature = "benchmark")]
impl BatchLatencyTotals {
    fn accumulate(&mut self, other: &Self) {
        self.tx_count += other.tx_count;
        self.tx_bytes += other.tx_bytes;
        self.committed_squared_micros = self
            .committed_squared_micros
            .saturating_add(other.committed_squared_micros);
        self.materialised_squared_micros = self
            .materialised_squared_micros
            .saturating_add(other.materialised_squared_micros);
    }
}

/// Outcome of a single digest's store lookup + deserialize, for
/// `Synchronizer::observe_committed`/`Synchronizer::finish_deferred_retry` to react
/// to differently: `Miss` defers and retries; `Error` (a store error, or bytes that
/// don't deserialize -- both already logged at the point of failure) drops for good,
/// matching this code's pre-existing treatment of that case.
#[cfg(feature = "benchmark")]
enum BatchReadOutcome {
    Hit(BatchLatencyTotals),
    Miss,
    Error,
}

/// `SystemTime`-since-epoch milliseconds -- the same clock/units `commit_millis`
/// (stamped once at the primary's own "Committed" log site) and the client-embedded
/// per-transaction submission timestamp both use, so subtracting across them is
/// meaningful.
#[cfg(feature = "benchmark")]
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Failed to measure time")
        .as_millis() as u64
}

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
    /// Batch digests already counted into `metrics` (after a genuinely successful
    /// read+deserialize -- see `observe_committed`'s doc comment), so a `Committed`
    /// notification for the same digest (should one ever arrive twice) is not
    /// double-counted. Bounded by `observed_commits_order` below, which carries the
    /// age information this plain set doesn't.
    #[cfg(feature = "benchmark")]
    observed_commits: HashSet<Digest>,
    /// Age index for `observed_commits`, keyed `(commit_millis, digest)` -- the
    /// commit instant the digest was actually counted at -- so it can be pruned by
    /// `split_off` at a wall-clock floor instead of a `retain` scan of
    /// `observed_commits` itself (project convention for anything keyed by a
    /// monotonically increasing quantity; mirrors e.g.
    /// `vantage::control::ControlLog`'s `delivered_set`/`pending_fetch`, same
    /// `(u64, Digest)` key shape for the same reason). `prune_stale` evicts both
    /// together: it `split_off`s this one, then removes exactly the digests that
    /// came back from `observed_commits` -- an O(pruned) targeted removal, never a
    /// scan of the live set.
    #[cfg(feature = "benchmark")]
    observed_commits_order: BTreeSet<(u64, Digest)>,
    /// Digests the primary told us were committed but that missed our local store on
    /// first read, deferred rather than dropped (see `observe_committed`'s doc
    /// comment: this is the measurement-bug fix). Maps `(commit_millis, digest)` --
    /// the ORIGINAL commit instant, preserved so a later retry still measures true
    /// commit -> materialise latency, not lookup-time latency -- to the cancel
    /// handle for that digest's `metrics_waiter` wait (see `run`). Same
    /// `(u64, Digest)`-keyed, `split_off`-pruned shape as `observed_commits_order`
    /// above, for the identical reason: bounded by wall-clock age, never scanned.
    #[cfg(feature = "benchmark")]
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
        // Fable audit item 4 (WAN latency injection): this authority's own
        // per-destination artificial latency map (same contract/construction as
        // `BatchMaker::spawn`'s -- see its doc comment). Applied to worker-to-worker
        // sync requests, previously undelayed even under a WAN-shaped run.
        latency_map: HashMap<SocketAddr, Duration>,
        metrics: Arc<Metrics>,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
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
                    .with_batching(batch),
                round: Round::default(),
                pending: HashMap::new(),
                metrics,
                #[cfg(feature = "benchmark")]
                observed_commits: HashSet::new(),
                #[cfg(feature = "benchmark")]
                observed_commits_order: BTreeSet::new(),
                #[cfg(feature = "benchmark")]
                pending_misses: BTreeMap::new(),
            }
            .run()
            .await;
        });
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
        // Starfish-parity real transaction latency (PHASE2-SPEC.md #5, amended):
        // metrics-only retry waiters for deferred misses (see `observe_committed`'s
        // doc comment) -- structurally the same "wait for `store.notify_read` or be
        // canceled" shape as `waiting` above, just for a different consumer
        // (`finish_deferred_retry` instead of `self.pending.remove`). Declared with
        // an explicit boxed-future element type (rather than relying on inference
        // from a push site) so this stays well-typed even on a non-`benchmark`
        // build, where nothing ever pushes into it. An always-empty
        // `FuturesUnordered` here does not busy-loop the `select!` below: per
        // `tokio::select!`'s documented lifecycle, a branch whose future resolves to
        // a value that doesn't match its pattern is disabled for that ONE call, not
        // retried -- the surrounding loop only iterates again once some OTHER,
        // genuinely pending branch (a real message, a real timer) wakes it.
        let mut metrics_waiting: FuturesUnordered<MetricsRetryFuture> = FuturesUnordered::new();

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
                        self.network.send_typed(address, Bytes::from(serialized), "BatchRequest").await;
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
                        for miss in self.observe_committed(commit_millis, digests).await {
                            metrics_waiting.push(Box::pin(Self::metrics_waiter(
                                miss.digest,
                                miss.commit_millis,
                                self.store.clone(),
                                miss.cancel,
                            )));
                        }
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

                // A deferred commit-metrics miss either resolved (its batch landed in
                // the store) or was canceled (pruned as stale by `prune_stale`) --
                // see `observe_committed`'s doc comment and `metrics_waiter`.
                Some(resolved) = metrics_waiting.next() => {
                    #[cfg(feature = "benchmark")]
                    if let Some((digest, commit_millis)) = resolved {
                        self.finish_deferred_retry(digest, commit_millis).await;
                    }
                    #[cfg(not(feature = "benchmark"))]
                    let _ = resolved;
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
                    // REFRESH the timestamps of everything just retried, or this is not a
                    // retry timer -- it is a re-broadcast-everything-forever timer. The
                    // timestamp is set once at `pending.insert` and was never updated here,
                    // so a digest that went one `sync_retry_delay` without arriving was
                    // re-broadcast on EVERY subsequent tick for as long as it stayed
                    // pending: unbounded request amplification, growing with the size of the
                    // backlog, aimed at a worker that is by definition already behind.
                    // Measured on the 2026-08-08 n=50 @200k netem run alongside the
                    // primary-side twin in `vantage::payload::sync_batches`.
                    for digest in &retry {
                        if let Some((_, _, timestamp)) = self.pending.get_mut(digest) {
                            *timestamp = now;
                        }
                    }
                    if !retry.is_empty() {
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        let addresses = self.committee
                            .others_workers(&self.name, &self.id)
                            .iter().map(|(_, address)| address.worker_to_worker)
                            .collect();
                        self.network
                            .lucky_broadcast_typed(addresses, Bytes::from(serialized), self.sync_retry_nodes, "BatchRequest")
                            .await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }

    /// Starfish-parity real transaction latency (PHASE2-SPEC.md #5, amended): for each
    /// batch the primary just told us was committed, read it from our local store and
    /// observe every contained transaction's TWO latency series --
    /// `transaction_committed_latency` (`commit_millis`, the primary's own instant
    /// taken once at its "Committed" log site and carried in the notification, minus
    /// the embedded submission timestamp -- never `SystemTime::now()` read here,
    /// which would additionally include the primary->worker notification hop and
    /// this task's own queueing delay under load) and `transaction_materialised_
    /// latency` (this call's own "now" minus the same submission timestamp; see that
    /// field's doc comment on `Metrics` for the starfish-comparable semantics this
    /// adds). The two are nearly identical for an immediate hit; for a digest that
    /// missed and was later resolved by `finish_deferred_retry`, `commit_millis`
    /// stays the ORIGINAL instant while the materialised series uses the LATER retry
    /// instant -- the gap between the two series is exactly the payload-availability
    /// cost a miss represents.
    ///
    /// Fixes a measurement bug: a miss (possible even for our own batches under GC,
    /// and EXPECTED for a remote author's batch that hasn't yet arrived via
    /// worker-to-worker gossip -- the normal case the primary commits ahead of) used
    /// to be dropped, via an `observed_commits.insert` that ran BEFORE the store read
    /// -- so a digest that missed was permanently marked "observed" and could never
    /// be counted even once it actually arrived, silently undercounting
    /// `committed_transactions`/`committed_bytes` and both latency histograms. Fixed
    /// by deferring instead of dropping: `observed_commits` is only inserted into
    /// after a genuinely successful read+deserialize (`mark_observed`, called from
    /// either this function's hot loop or `finish_deferred_retry`'s later
    /// resolution -- never from a failed attempt of any kind), and a miss is
    /// recorded in `pending_misses` (keyed by this digest's ORIGINAL `commit_millis`)
    /// instead. This function returns the newly-deferred misses so its caller
    /// (`run`) can start an event-driven `metrics_waiter` retry for each -- backed by
    /// `store.notify_read`, which resolves exactly when the batch lands, whichever of
    /// `Processor`'s two spawned instances (own or others' batches; both write
    /// through the same `Store`) performs that write. Never polled.
    ///
    /// Gated on `Metrics::metrics_active` (perf-audit addendum, starfish parity:
    /// `RealCommitHandler::transaction_observer`'s identical early return) -- late
    /// commits during warmup/wind-down would otherwise skew TPS, the latency
    /// distribution, and the bandwidth-efficiency denominator, exactly as they would
    /// for starfish. `prune_stale` runs unconditionally, BEFORE the gate: bounding
    /// memory is not a rate metric and must keep working even while inactive (e.g. a
    /// deployment that never activates metrics at all).
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

        // Captured once per call (not once per digest/transaction), same discipline
        // as `commit_millis` itself -- see this function's own doc comment.
        let materialised_now_millis = now_millis();

        // Accumulated locally and flushed once at the end of the call (one atomic
        // `inc_by` per counter instead of one per transaction) -- at 240k tx/s that's
        // the difference between a handful of atomic ops per `Committed` message and
        // one per transaction. Only the histogram observations are inherently
        // per-transaction (each has its own latency value); they're already a
        // lock-free channel push, not an atomic increment, so batching them
        // wouldn't help -- see `read_and_observe_batch`.
        let mut totals = BatchLatencyTotals::default();
        let mut deferred = Vec::new();

        for digest in digests {
            // Dedup: a digest we've already counted is a no-op.
            if self.observed_commits.contains(&digest) {
                continue;
            }

            match self
                .read_and_observe_batch(&digest, commit_millis, materialised_now_millis)
                .await
            {
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
                BatchReadOutcome::Error => {
                    // Already logged inside `read_and_observe_batch`.
                }
            }
        }

        self.flush_totals(&totals);
        deferred
    }

    /// Resolution side of a deferred miss (see `observe_committed`'s doc comment):
    /// called once `metrics_waiter` confirms `digest`'s batch landed in the store.
    /// `commit_millis` is the ORIGINAL instant preserved in `pending_misses` and
    /// carried by the waiter future, never this call's own time.
    #[cfg(feature = "benchmark")]
    async fn finish_deferred_retry(&mut self, digest: Digest, commit_millis: u64) {
        // Bookkeeping cleanup happens regardless of the dedup/active-window outcome
        // below -- an entry that resolved is no longer pending, full stop.
        self.pending_misses.remove(&(commit_millis, digest.clone()));

        // Defensive dedup (see `observed_commits`'s doc comment): a no-op unless the
        // same digest was somehow committed twice.
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
                // `metrics_waiter` only resolves `Some` after `store.notify_read`
                // confirms the write happened -- immediately re-reading the same key
                // through the same single-threaded store actor (`store::Store`'s
                // internal command queue is strictly FIFO: `NotifyRead`'s pending
                // senders are drained synchronously inside the SAME `Write` command
                // that unblocks them, before the actor moves on to whatever is queued
                // after our subsequent `Read`) should always see it. Not re-deferred:
                // a second `notify_read` on the same key could only resolve on a
                // SECOND write to it, which nothing in this codebase ever does --
                // re-deferring would risk a wait that never resolves rather than
                // self-heal.
                log::warn!(
                    "Deferred batch {} still missing immediately after its store \
                     write notification fired; dropping (will not retry again)",
                    digest
                );
            }
            BatchReadOutcome::Error => {}
        }
    }

    /// Wall-clock GC for the two benchmark-only bookkeeping structures this fix adds
    /// (`observed_commits`/`observed_commits_order`, and `pending_misses`), run on
    /// every `Committed` notification -- never a background poll. Evicts entries
    /// older than `now_millis - BENCHMARK_METRICS_RETENTION_MILLIS` via `split_off`
    /// at that floor (never a `retain` scan -- see `observed_commits_order`'s doc
    /// comment). A pruned pending-miss's waiter is explicitly canceled (mirrors
    /// `PrimaryWorkerMessage::Cleanup`'s identical cancellation of stale `pending`
    /// sync-waiters just above in this file) rather than left to wait on a
    /// `notify_read` that may now never resolve.
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

    /// Marks `digest` as counted, keyed by the commit instant it was actually
    /// counted at -- see `observed_commits_order`'s doc comment for why both
    /// structures are always updated together.
    #[cfg(feature = "benchmark")]
    fn mark_observed(&mut self, digest: Digest, commit_millis: u64) {
        self.observed_commits.insert(digest.clone());
        self.observed_commits_order.insert((commit_millis, digest));
    }

    /// Reads+deserializes `digest`'s batch and computes every contained
    /// transaction's committed/materialised latency contribution, WITHOUT touching
    /// the shared counters itself -- the caller decides how to accumulate/flush
    /// those (batched across a whole `Committed` message in `observe_committed`'s
    /// hot loop, or immediately for `finish_deferred_retry`'s single resolved
    /// digest). The histogram observations themselves (already a lock-free channel
    /// push) happen here either way, since batching those wouldn't help.
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

        let message: WorkerMessage = match bincode::deserialize(&bytes) {
            Ok(message) => message,
            Err(e) => {
                error!("Failed to deserialize committed batch {}: {}", digest, e);
                return BatchReadOutcome::Error;
            }
        };
        let WorkerMessage::Batch(transactions) = message else {
            // Genuinely deserialized, just not the variant expected at this key --
            // still a "successful read+deserialize" (see `observe_committed`'s doc
            // comment), so the caller still marks it observed; nothing to add to the
            // totals.
            return BatchReadOutcome::Hit(BatchLatencyTotals::default());
        };

        let mut totals = BatchLatencyTotals::default();
        for tx in transactions {
            // §4 wire format: [1 B marker][8 B id, BE][8 B submission timestamp, LE].
            // A transaction shorter than the header (should not happen once every
            // client is on the Phase-2 format) is skipped rather than indexed into.
            if tx.len() < 17 {
                continue;
            }
            let submitted_millis = u64::from_le_bytes(tx[9..17].try_into().unwrap());
            // Metrics-active window (see `Metrics::active_from_millis`): a transaction
            // SUBMITTED before the window opened is skipped outright -- it contributes
            // to neither latency series nor the committed counters. Gating on the
            // submission instant rather than the commit instant is the point: the
            // startup transient is exactly the population that was submitted while the
            // committee was still forming, and those are the observations that pinned
            // p99 near 3.5s. A no-op (`from == 0`) unless the harness set the window.
            if !self.metrics.counts_toward_metrics(submitted_millis) {
                continue;
            }
            // saturating_sub: tolerate any clock skew between client and node
            // instead of panicking (NTP-grade sync is assumed, not enforced).
            let committed_latency =
                Duration::from_millis(commit_millis.saturating_sub(submitted_millis));
            let materialised_latency =
                Duration::from_millis(materialised_now_millis.saturating_sub(submitted_millis));

            self.metrics
                .transaction_committed_latency
                .observe(committed_latency);
            self.metrics
                .transaction_materialised_latency
                .observe(materialised_latency);

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

    /// Flushes one batch's (or one call's accumulated multi-batch) totals into the
    /// shared counters -- see `read_and_observe_batch`'s doc comment for why this is
    /// split out from it. A no-op when `totals.tx_count == 0` (nothing observed),
    /// matching the pre-existing `if tx_count > 0` guard this replaced.
    #[cfg(feature = "benchmark")]
    fn flush_totals(&self, totals: &BatchLatencyTotals) {
        if totals.tx_count == 0 {
            return;
        }
        self.metrics
            .transaction_committed_latency_squared_micros
            .inc_by(totals.committed_squared_micros);
        self.metrics
            .transaction_materialised_latency_squared_micros
            .inc_by(totals.materialised_squared_micros);
        self.metrics.committed_transactions.inc_by(totals.tx_count);
        self.metrics.committed_bytes.inc_by(totals.tx_bytes);
    }

    /// Mirrors `waiter` (this file's existing primary-sync retry-wait) for a
    /// deferred commit-metrics miss: waits for `digest`'s batch to land in the
    /// store, or for its own cancellation (the entry was pruned as stale by
    /// `prune_stale`), carrying `digest` and its ORIGINAL `commit_millis` forward on
    /// success so `finish_deferred_retry` needs no separate index to recover them.
    /// `None` on cancellation, exactly like `waiter`'s own `Ok(None)`.
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
