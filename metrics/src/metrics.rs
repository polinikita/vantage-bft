// Ported (minimally) from starfish (`~/code/starfish/crates/starfish-core/src/metrics.rs`,
// Apache-2.0) for Starfish-parity real transaction latency (PHASE2-SPEC.md #5).
//
// Only the transaction-commit-latency slice Phase 2 needs is ported -- starfish's
// `Metrics` also carries dozens of DAG/BLS/shard-reconstruction fields with no Autobahn
// or Vantage equivalent yet; Phase 3+ (the Vantage core) extends this same struct with
// whatever counters it needs rather than starting a parallel one.
//
// Deviation from starfish: gauge reporting uses `std::sync::Mutex` instead of
// `parking_lot::Mutex` (avoids adding a dependency for a single low-contention lock:
// the only reader/writer is the 10s reporter task; producers never touch it, they only
// push onto `HistogramSender`'s channel) and `log::info!`/`log::error!` instead of
// `tracing` (this workspace's existing logging stack throughout primary/worker/node).

use std::{
    ops::AddAssign,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
    time::Duration,
};

/// Process-wide panic tally, owned by `Metrics::install_panic_hook`'s singleton hook
/// rather than by any one `Metrics` instance -- `node local-benchmark` runs N nodes,
/// and therefore N registries, inside one process and one hook.
static PROCESS_PANICS: AtomicU64 = AtomicU64::new(0);

/// How often `spawn_queue_sampler` reads every probe. 10 Hz: fast enough that a queue
/// which fills and drains inside one publish interval still registers in the peak, cheap
/// enough to be irrelevant (a handful of atomic loads per tick).
const QUEUE_SAMPLE_INTERVAL_MS: u64 = 100;

/// How often `spawn_queue_sampler` publishes, in sample ticks. 1 s, matching the other
/// progress gauges, so a dashboard reads them all on one cadence.
const QUEUE_PUBLISH_EVERY: u32 = 10;

/// Occupancy reader for one bounded channel, type-erased so probes over channels of
/// different message types can be sampled by one task.
///
/// Occupancy is `Sender::max_capacity() - Sender::capacity()`, which needs no counter on
/// the send path and no change to any producer -- these channels sit on the transaction hot
/// path. NOTE it counts permits HELD, not messages queued; the two diverge exactly in the
/// deadlock case, which is why `StoreProbe` also carries a drain counter.
pub struct QueueProbe {
    pub stage: &'static str,
    /// Returns `(depth, capacity)`.
    pub occupancy: Box<dyn Fn() -> (usize, usize) + Send + Sync>,
}

/// The store actor's own channel plus the two liveness readings that disambiguate a full
/// one. Separate from `QueueProbe` because those readings have no analogue for a plain
/// channel.
pub struct StoreProbe {
    /// Returns `(depth, capacity)` of the actor's command channel.
    pub occupancy: Box<dyn Fn() -> (usize, usize) + Send + Sync>,
    /// Epoch-ms stamp of the actor's last completed loop iteration.
    pub heartbeat_millis: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Monotonic count of commands the actor has dequeued.
    pub commands_drained: Box<dyn Fn() -> u64 + Send + Sync>,
}

/// Publish bounded-queue occupancy and store-actor liveness until the process exits.
///
/// Lives here, taking closures rather than a `store::Store`, so BOTH the primary and the
/// worker can use one implementation without `metrics` gaining a `store` dependency. The
/// primary needs it too: it has its own store and its own per-key `notify_read` waiters,
/// and until this was shared its store was entirely unobserved -- the same class of blind
/// spot that made the worker wedge take a day to find. Pass an empty `probes` for a process
/// with no pipeline channels of its own.
pub fn spawn_queue_sampler(probes: Vec<QueueProbe>, store: StoreProbe, metrics: Arc<Metrics>) {
    // Write-once: bounds never change, and publishing them lets a dashboard show occupancy
    // as a fraction without hard-coding each channel's constant.
    for p in &probes {
        let (_, capacity) = (p.occupancy)();
        metrics
            .worker_queue_capacity
            .with_label_values(&[p.stage])
            .set(capacity as i64);
    }
    let (_, store_capacity) = (store.occupancy)();
    metrics
        .worker_queue_capacity
        .with_label_values(&["store"])
        .set(store_capacity as i64);

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(QUEUE_SAMPLE_INTERVAL_MS));
        let mut peaks: std::collections::HashMap<&'static str, usize> =
            std::collections::HashMap::new();
        let mut age_peak: u64 = 0;
        // The store counter is monotonic and process-local; the Prometheus counter is
        // advanced by the DELTA so it keeps counter semantics across restarts of neither.
        let mut drained_reported: u64 = 0;
        let mut ticks: u32 = 0;
        loop {
            ticker.tick().await;
            ticks += 1;

            let mut latest: Vec<(&'static str, usize)> = Vec::with_capacity(probes.len() + 1);
            for p in &probes {
                let (depth, _) = (p.occupancy)();
                latest.push((p.stage, depth));
            }
            let (store_depth, _) = (store.occupancy)();
            latest.push(("store", store_depth));
            for (stage, depth) in &latest {
                let slot = peaks.entry(stage).or_insert(0);
                *slot = (*slot).max(*depth);
            }
            // Age, not the raw stamp, so the READER's clock defines "now" -- the actor's
            // own clock could be the thing that is stuck. Saturating: a stamp from the
            // future (clock step) reads 0 rather than wrapping.
            let age = now_millis().saturating_sub((store.heartbeat_millis)());
            age_peak = age_peak.max(age);

            if !ticks.is_multiple_of(QUEUE_PUBLISH_EVERY) {
                continue;
            }
            for (stage, depth) in &latest {
                metrics
                    .worker_queue_depth
                    .with_label_values(&[stage])
                    .set(*depth as i64);
            }
            for (stage, peak) in peaks.iter_mut() {
                metrics
                    .worker_queue_peak
                    .with_label_values(&[stage])
                    .set(*peak as i64);
                *peak = 0;
            }
            metrics
                .store_actor_heartbeat_age_ms
                .set(age.min(i64::MAX as u64) as i64);
            metrics
                .store_actor_heartbeat_age_ms_peak
                .set(age_peak.min(i64::MAX as u64) as i64);
            age_peak = 0;

            let drained = (store.commands_drained)();
            metrics
                .store_commands_drained_total
                .inc_by(drained.saturating_sub(drained_reported));
            drained_reported = drained;
        }
    });
}

/// Wall-clock epoch milliseconds; see `spawn_queue_sampler`'s use of it.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

use prometheus::{
    core::{Collector, Desc},
    proto::MetricFamily,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, Gauge, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use tokio::time::Instant;

/// Publishes `metrics_active_seconds`: how long this node's metrics-active window has
/// been open, or 0 while it is closed / not configured.
///
/// This is the starfish `benchmark_duration` idea (`crates/orchestrator/src/
/// measurements.rs`): the VALIDATOR owns the clock that rates are divided by, so the
/// harness never has to infer the window from its own wall clock. Dividing a committed
/// delta by an orchestrator-chosen scrape interval understates TPS whenever the window
/// opened partway through that interval -- the counter only accumulated for part of it.
/// With this series the harness divides by `Δmetrics_active_seconds` instead, which is
/// exactly the in-window time, so a warmup can no longer distort the rate and
/// `--warmup` no longer has to be configured relative to the window budget.
///
/// A `Collector` rather than a periodically-ticked gauge on purpose: `collect` runs on
/// every `/metrics` scrape, so the value is exact AT SCRAPE TIME. The 10s
/// `MetricReporter` tick would otherwise quantise the denominator by up to 10s, ~8% of
/// a 120s measurement window.
struct ActiveWindowCollector {
    active_from_millis: Arc<AtomicU64>,
    gauge: Gauge,
}

impl ActiveWindowCollector {
    fn new(active_from_millis: Arc<AtomicU64>) -> Self {
        let gauge = Gauge::with_opts(Opts::new(
            "metrics_active_seconds",
            "Seconds this node's metrics-active window has been open (0 = closed)",
        ))
        .expect("static metric opts are valid");
        Self {
            active_from_millis,
            gauge,
        }
    }
}

impl Collector for ActiveWindowCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let from = self
            .active_from_millis
            .load(std::sync::atomic::Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // 0 when ungated (from == 0) or before the window opens: the harness reads that
        // as "no node-side clock" and falls back to its wall-clock window, which is
        // byte-identical to the behaviour before this series existed.
        let seconds = if from == 0 || now <= from {
            0.0
        } else {
            (now - from) as f64 / 1000.0
        };
        self.gauge.set(seconds);
        self.gauge.collect()
    }
}

/// METRICS-DASHBOARD-SPEC.md §3: starfish-style (`metrics.rs:1325-1376`) Drop-guard
/// busy-time timer, ported minimally (only the `IntCounterVec`-labeled, owned variant
/// -- `VantageCore` is a single long-lived task with no borrow-lifetime constraints
/// that would need the borrowed `UtilizationTimer<'a>` starfish also defines). Adds
/// its elapsed wall time (microseconds) to the labeled counter when dropped, whether
/// via normal fall-through or an early `?`/`return` -- so a section's busy time is
/// counted even on its error paths, same as starfish's own guarantee.
pub struct UtilizationTimer {
    metric: IntCounter,
    start: Instant,
}

impl Drop for UtilizationTimer {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as u64);
    }
}

impl UtilizationTimer {
    /// Fable perf-audit item 7: construct directly from an already-resolved counter
    /// handle. A caller that repeatedly times the SAME fixed label (e.g.
    /// `VantageCore`'s handful of named sections) can resolve the `IntCounter` once
    /// via `IntCounterVec::with_label_values` and reuse this constructor on every
    /// subsequent call, skipping the vec's internal lookup entirely from then on.
    /// Identical timing semantics to `UtilizationTimerVecExt::utilization_timer`
    /// (same `Drop` impl, same elapsed-time accounting) -- this is purely an
    /// alternate constructor, not a behavior change.
    pub fn from_counter(metric: IntCounter) -> Self {
        Self {
            metric,
            start: Instant::now(),
        }
    }
}

pub trait UtilizationTimerVecExt {
    /// Start a timer for `label`; the accumulated busy time is committed to the
    /// counter when the returned guard is dropped.
    fn utilization_timer(&self, label: &str) -> UtilizationTimer;
}

impl UtilizationTimerVecExt for IntCounterVec {
    fn utilization_timer(&self, label: &str) -> UtilizationTimer {
        UtilizationTimer {
            metric: self.with_label_values(&[label]),
            start: Instant::now(),
        }
    }
}

use crate::stat::{histogram, DivUsize, HistogramSender, PreciseHistogram};

/// Phase-2 metrics: the real (submission-to-commit) transaction latency distribution,
/// plus the counters needed to interpret it (how many transactions it covers, how many
/// commit-side store lookups missed and were skipped rather than blocking on).
/// Phase 3+ extends this struct in place.
#[derive(Clone)]
pub struct Metrics {
    /// Channel-fed observation point for a single transaction's (commit time - embedded
    /// submission timestamp). Hot path: `observe` is a lock-free unbounded-channel push.
    pub transaction_committed_latency: HistogramSender<Duration>,
    /// Running sum of (latency in microseconds)^2 across all observed transactions --
    /// paired with the histogram's `sum`/`count` gauges this gives the harness an exact
    /// global stddev: `sqrt(squared_sum/count - (sum/count)^2)`.
    pub transaction_committed_latency_squared_micros: IntCounter,
    /// Perf-audit fix (measurement bug): submit -> ordered ∧ MATERIALISED latency --
    /// `commit_millis` is when the primary ordered the batch, but a worker that
    /// didn't yet hold the payload only learns the transactions' contents (and can
    /// only observe them into this histogram) later, once the batch actually lands
    /// locally via `SyncBatches`/worker-to-worker gossip. Observed at that later
    /// instant minus the same embedded submission timestamp, from the exact same
    /// loop iteration as `transaction_committed_latency` above -- so every
    /// transaction contributes to both. Starfish-comparable: starfish's own
    /// `transaction_committed_latency` is observed only once the block's
    /// transactions are locally available in the first place (see
    /// `RealCommitHandler::transaction_observer`), i.e. it measures this same
    /// submit -> ordered ∧ materialised quantity, not submit -> ordered alone. For
    /// an immediate hit the two histograms are nearly identical; for a deferred miss
    /// (see `latency_misses`) the gap between them is exactly the payload-
    /// availability cost.
    pub transaction_materialised_latency: HistogramSender<Duration>,
    /// Mirrors `transaction_committed_latency_squared_micros` exactly (same batched
    /// accumulate-then-`inc_by` treatment), for `transaction_materialised_latency`.
    pub transaction_materialised_latency_squared_micros: IntCounter,
    /// Total transactions whose latency was successfully observed.
    pub committed_transactions: IntCounter,
    /// Total bytes of transactions whose latency was successfully observed.
    pub committed_bytes: IntCounter,
    /// Commit-time batch lookups that missed the local store and were DEFERRED for
    /// retry, not dropped (perf-audit fix -- see
    /// `worker::synchronizer::Synchronizer::observe_committed`'s doc comment for the
    /// bug this replaced: a miss used to permanently mark the digest "observed",
    /// silently undercounting `committed_transactions`/`committed_bytes` and the
    /// latency histograms whenever the payload arrived after the commit
    /// notification, which is the normal case for a remote author's batch). Every
    /// deferral increments this counter exactly once, whether or not it later
    /// resolves; `latency_misses_resolved` below tells the two apart.
    pub latency_misses: IntCounter,
    /// Deferred misses (`latency_misses`) that later resolved when their batch
    /// landed in the store and were counted. `latency_misses -
    /// latency_misses_resolved` is the number still pending retry (mid-run) or
    /// permanently unresolved (after the run has ended and any legitimately
    /// in-flight sync has had time to finish).
    pub latency_misses_resolved: IntCounter,

    // --- Phase 3 (PHASE3-SPEC.md §6.4): vantage data-plane counters. Always
    // registered (same pattern as the rest of this struct); only observed into on the
    // `Protocol::Vantage` path, so they simply stay zero on the two Autobahn paths.
    /// Blocks this node published (self-authored, including our own).
    pub vantage_blocks_published: IntCounter,
    /// Blocks this node received (direct publish or relayed) and cached.
    pub vantage_blocks_received: IntCounter,
    /// N3 ack CONFIRMATIONS this node produced -- incremented in `LaneManager::
    /// on_direct_pub_confirmed`, which is deliberately unaware of
    /// `Parameters::ack_watermarks`. Under `--ack-watermarks` the per-block
    /// `VantageAck` wire broadcast is suppressed, so this keeps counting while
    /// nothing is sent: it is a confirmation count, NOT a wire-send count. The
    /// watermark front-end that replaces those sends is `vantage_avail_sent`.
    pub vantage_acks_sent: IntCounter,
    /// Unsigned acks this node counted, first-hand (N4) -- WIRE-sourced only, so
    /// this is structurally 0 under `--ack-watermarks` (see `vantage_acks_sent`);
    /// the watermark equivalent is `vantage_avail_credited_refs`.
    pub vantage_acks_received: IntCounter,
    /// `request(h)` messages this node sent (N6/D2).
    pub vantage_repairs_requested: IntCounter,
    /// `serve(h, b)` messages this node sent (N7).
    pub vantage_repairs_served: IntCounter,
    /// Cumulative bincode-encoded size of every block this node has retained (N8).
    pub vantage_retained_bytes: IntCounter,
    /// Fable-audit fix: inbound wire messages dropped by `VantageCore::dispatch_inbound`
    /// because their declared sender is not a committee member. Always zero on the
    /// honest-only path; a nonzero value means a Byzantine node is forging sender keys.
    /// Reused as-is by `SimpleItCore::dispatch_inbound`'s own instance of the identical
    /// gate (`vantage::wire::sender_is_member`) -- the two protocols are mutually
    /// exclusive per node/run, so there is no ambiguity in practice about which
    /// assembly a nonzero reading came from.
    pub vantage_rejected_nonmember_total: IntCounter,
    /// Bulk recovery messages dropped because the bulk inbound queue was full
    /// (`vantage::node::VantageReceiverHandler::tx_bulk`). Every message counted here
    /// is re-requestable -- served payload or a fetch/resume request -- so a nonzero
    /// reading means the core is behind on recovery traffic, NOT that consensus data
    /// was lost. Sustained growth is the signal that resume serving needs throttling;
    /// zero is the healthy steady state.
    pub vantage_bulk_inbound_dropped_total: IntCounter,
    /// Optional ack-watermark front-end (`Parameters::ack_watermarks`): periodic
    /// per-lane availability broadcasts this node sent (one per period with a
    /// nonempty flush -- see `vantage::lanes::LaneManager::take_avail_flush` --, not
    /// one per author). Always zero when the flag is off (`VantageCore`/
    /// `SimpleItCore::run` never even schedule the tick in that case).
    pub vantage_avail_sent: IntCounter,
    /// `VantageAvail` messages this node received and routed to `LaneManager::
    /// resolve_watermark`. Always zero when the flag is off (no peer ever sends one).
    pub vantage_avail_received: IntCounter,
    /// `BlockRef`s actually credited into the shared `AckAggregator` via the
    /// ack-watermark front-end (immediate resolution via `resolve_watermark` plus
    /// later backfill via `retry_pending_avail`, combined) -- the watermark analogue
    /// of `vantage_acks_received`. Always zero when the flag is off.
    pub vantage_avail_credited_refs: IntCounter,
    /// `SimpleItCore`'s effect-execution loop received a `vantage::Effect` variant
    /// that `LaneManager`/`Repairer` can never actually construct (every
    /// AGB/pacemaker/control-log/anchor/cursor-output variant -- see that loop's own
    /// doc comment for the full list). Always zero: the match arm that increments this
    /// also fires a `debug_assert!(false, ..)`, so any nonzero reading in a debug build
    /// is also an immediate panic during development; in a release build it is a
    /// silent, observable drop instead of a validator crash.
    pub simpleit_unexpected_effect_total: IntCounter,
    /// `SimpleItCore`'s commit-materialisation queue depth: committed rounds
    /// (`CutEffect::Commit`) whose cut has not yet been fully emitted because at
    /// least one author's block chain isn't locally verified all the way from its
    /// watermark yet (a correct node can commit a round it hasn't voted on -- `2f+1`
    /// peers acking a tip is not the same as holding it -- so this is expected to be
    /// briefly nonzero under normal repair latency, not just under a fault). A
    /// sustained/growing value means the drain is stuck: repair for some author's
    /// lane is not making progress.
    pub simpleit_commit_queue_len: IntGauge,

    // --- Phase 6 (PHASE6-SPEC.md §9 gate amendment): per-view seal-route breakdown.
    /// How each view got sealed/ordered, one label `"route"`, incremented exactly once
    /// per view at the try-seal arbiter's FIRST-acceptance point (the submission that
    /// wins is the route -- later compatible submissions for the same view never
    /// count again). Routes: `fast_full` (all-n unanimous fast seal), `direct_full`
    /// (grade-1 ready quorum), `direct_core` (grade-0 ready quorum), `anchor_full`,
    /// `anchor_core`, `anchor_skip` (the apply-anchor adapter's three outcomes),
    /// `vote_skip` (the grounded post-ready skip-vote quorum, signature-free.tex's
    /// par:skip-seal). Different nodes can legitimately show different route
    /// distributions for the same view (e.g. one node fast-seals while another only
    /// reaches the anchor).
    pub vantage_seals: IntCounterVec,

    // --- signature-free.tex's "Grounded post-ready skip" (par:skip-seal).
    /// `SKIP-VOTE(u)` statements this node broadcast (one per target it voted on --
    /// at most once per target, ever).
    pub vantage_skip_votes_sent: IntCounter,
    /// `SKIP-VOTE(u)` statements this node counted first-hand from a peer (self-votes
    /// are not counted here -- see `vantage_skip_votes_sent`).
    pub vantage_skip_votes_received: IntCounter,

    // --- signature-free.tex §8.3 "Digest-named AGB statements" (`Parameters::
    // digest_statements`). Always zero when the flag is off (no `VantageEchoDigest`/
    // `VantageReadyDigest` is ever sent, so no digest statement is ever buffered, so
    // no fetch is ever issued; nothing this counts is reachable without the flag).
    /// `VantageBodyFetch` messages this node sent (one per target statement author,
    /// per outstanding (view, digest) pair, per retry attempt).
    pub vantage_body_fetches_sent: IntCounter,
    /// `VantageBodyServe` messages this node sent, answering a peer's
    /// `VantageBodyFetch` with its own held, fixed `ViewProposal`.
    pub vantage_bodies_served: IntCounter,

    // --- Mechanism A (sender-side lane resume, ack-census-gap-triggered -- see
    // `vantage::resume`'s own module doc comment). Shared by both Vantage and
    // Simple-IT (same `LaneManager`/`Wire` data plane). Always registered; expected
    // to stay at/near zero on a fault-free run (a persistent gap never forms) and to
    // recover dissemination after a windowed `--withhold` fault closes.
    /// `VantageLaneResume` requests this node sent (one per (lane author, gap
    /// height) the requester-side trigger actually fired for, after its two-tick
    /// persistence check and backoff -- not one per tick).
    pub vantage_lane_resume_requests_sent: IntCounter,
    /// Own blocks this node served, as unicast `Header(_, false)`, answering a
    /// peer's `VantageLaneResume` -- counted per block actually served, not per
    /// request received.
    pub vantage_lane_resume_blocks_served: IntCounter,
    /// Fable perf audit (loop-starvation fix): resume traffic (`VantageLaneResume`
    /// requests, and `Header(_, false)` resume-batch entries) `Wire::enqueue_resume`
    /// failed to `try_send` onto the dedicated resume-sender task's channel --
    /// either it was full (the task fell behind draining a backed-up destination)
    /// or closed (the task panicked). Expected to stay at/near zero: Mechanism A's
    /// own end-to-end retry (`vantage::resume::ResumeTrigger`'s backoff,
    /// `vantage::resume::ResumeServe`'s dedup) recovers every drop this counts, so a
    /// nonzero rate here is a liveness signal (the channel is genuinely saturated),
    /// never a correctness bug on its own.
    pub vantage_lane_resume_send_drops: IntCounter,

    // --- reconnect-replay plan §10 (server-authoritative floor, v3): a SEPARATE
    // mechanism from Mechanism A above (see `vantage::resume`'s own module doc
    // comment) -- resumes one-shot AGB/consensus broadcasts lost to a volatile
    // session death, rather than lane content. `network_messages_sent_total`/
    // `network_bytes_sent_total`/`network_frames_sent_total` (labeled `"Replay"`/
    // `"VantageResumeHello"`/`"VantageReplayDone"`) already cover raw send volume
    // generically -- these four cover what those generic counters structurally
    // cannot: WHY a send happened, or that one was dropped before ever reaching the
    // wire. Always registered; expected to stay at/near zero on a fault-free run.
    /// `Wire::enqueue_replay`'s `try_send` onto the resume-sender task's channel
    /// failed (full or closed) -- audit-3 A2's own Err arm: `pending_low` is left
    /// untouched on this path, recovered by the next nudge/tick re-ask.
    pub vantage_replay_enqueue_drops_total: IntCounter,
    /// `VantageReplayDone` sends where `outbox_floor` truncated the requested span
    /// below what was actually asked for -- a recovered-with-gap signal (the
    /// requester's episode still closes/continues correctly; this just means some
    /// of what it asked for is permanently gone).
    pub vantage_replay_done_clamped_total: IntCounter,
    /// Server-side nudge Hellos sent (`pending_low[X]` set, `X` not in-flight,
    /// backoff elapsed since the last serve-or-nudge toward `X` -- audit-3 A3). The
    /// backstop for a lost Hello/Done/reconnect-event: closes the asymmetric case
    /// where the peer that's missing data never itself asks for it.
    pub vantage_replay_pending_low_nudges_total: IntCounter,
    /// In-flight-replay-stream entries found stale (older than `replay_episode_max_
    /// ms`, audit-3 A6) and evicted -- expected near-zero; a sustained nonzero rate
    /// means the resume task is falling behind draining what it already enqueued
    /// (strict `Message`-priority scheduling does not guarantee replay throughput).
    pub vantage_replay_inflight_ttl_expired_total: IntCounter,

    // --- Phase 7 prep (PHASE7-PREP-NOTES.md, Finding A diagnosis): always-on progress
    // gauges, one flat value each (no labels), sampled once/sec by `VantageCore` itself
    // (metrics-only addition -- no protocol-semantic effect, same "always registered,
    // only observed into on the vantage path" pattern as the Phase-3 counters above).
    // Together with `vantage_seals` these let `local-benchmark --timeline` reconstruct,
    // second by second, where a stalled run's bottleneck actually sits: the WISH
    // pacemaker's own entry frontier vs. the responsive-proposal frontier vs. the output
    // cursor vs. the resolution control-log's round/log-consumption position.
    /// `Pacemaker::entered_view` -- the largest view this party has formally entered (W5).
    pub vantage_entered_view: IntGauge,
    /// `Pacemaker::own_watermark` -- this party's own current wish.
    pub vantage_own_watermark: IntGauge,
    /// `Pacemaker::entry_target` -- the entry target `advance_entry_target` last set.
    pub vantage_entry_target: IntGauge,
    /// `Pacemaker::omega_q` -- the (2f+1)-party wish statistic driving entry.
    pub vantage_omega_q: IntGauge,
    /// `Frontier::a_i` -- the responsive proposal frontier.
    pub vantage_frontier_a_i: IntGauge,
    /// `Cursor::next_view` -- the lowest view the output cursor has not yet advanced past.
    pub vantage_cursor_next_view: IntGauge,
    /// `ControlLog::curr_round` -- the resolution pipeline's current Simple-IT round.
    pub vantage_control_round: IntGauge,
    /// `ControlLog::delivered_log.len()` -- total (view, digest) pairs RB-delivered into
    /// the control log so far.
    pub vantage_control_delivered_len: IntGauge,
    /// `ControlLog::consume_pos` -- how far `pump_log` has consumed that delivered log
    /// (a persistent gap between this and `delivered_len` means every subsequent anchor
    /// is blocked on a still-missing `B_w`, not on delivery itself).
    pub vantage_control_consume_pos: IntGauge,
    /// `AgbEngine::pending_gate.len()` -- views active+fixed but not yet echoed, i.e.
    /// the population `recheck_all`'s budgeted scan rotates over. Near-zero on a
    /// healthy node; growth tracking the node's view gap is the n=100 straggler
    /// death-spiral signature (2026-08-08 investigation) this gauge exists to confirm.
    /// MEASURED 0 on healthy AND straggling nodes alike, which is what eliminated
    /// `recheck_all` as the n=100 cause and pointed at `Repairer` instead.
    pub vantage_pending_gate_len: IntGauge,
    /// `DigestStatements::pending_fetch.len()` -- outstanding AGB body fetches. Was
    /// observable only through a `#[cfg(test)]` accessor, which made the 2026-08-08
    /// body-fetch storm impossible to characterise from a scrape: the fetch COUNT alone
    /// cannot separate "many pending pairs, few targets each" from the reverse. Bounded
    /// by `agb::MAX_PENDING_FETCH`; sitting at that ceiling means resolution has stalled.
    pub vantage_pending_body_fetch_len: IntGauge,
    /// Body-fetch pairs dropped at `ensure_fetch` because `pending_fetch` was at its
    /// ceiling. Nonzero means the node is accumulating views it cannot resolve; each
    /// eviction is free (the pair is re-created from the retained statement on the next
    /// arrival) and prevents quadratic retry growth.
    pub vantage_body_fetch_evicted_total: IntCounter,
    /// `Repairer::pending_settle.len()` -- authorized-but-unsettled refs. This is the
    /// `P` in `on_block_available`'s O(P) re-sweep-per-cached-block, and it has no GC,
    /// so a node that cannot obtain a block keeps its refs here forever. The n=100
    /// analysis inferred P >= 1,920 indirectly (from `repairs_requested / (n-1)`);
    /// this measures it directly.
    pub vantage_pending_settle_len: IntGauge,
    /// Total `Repairer::settle` calls -- the actual work `on_block_available`'s sweep
    /// generates, which no counter previously exposed. `settle_calls / blocks_received`
    /// is the sweep amplification: ~1 is healthy, ~P means the sweep is the bottleneck
    /// (the n=100 straggler estimate was ~91.6 MILLION calls).
    pub vantage_repair_settle_calls_total: IntCounter,
    /// Times `settle` entered its missing-block branch, i.e. how often the peer
    /// fan-out was CONSIDERED. Compare against `vantage_repairs_requested`, which only
    /// counts requests actually emitted: before the 2026-08-08 gate the ratio was the
    /// pure-waste multiplier (every miss re-ran an n-1 loop that emitted nothing), and
    /// it was invisible because only new `(peer, digest)` pairs ticked a counter. With
    /// the gate the two should track each other at ~1/(n-1).
    pub vantage_repair_fanout_loops_total: IntCounter,
    /// Current adaptive per-tick ceiling on repair-request emission. AIMD on bulk-inbound
    /// drops: halves on a dropping tick, doubles on a clean one. A node in recovery with no
    /// congestion should climb to the maximum -- a fixed ceiling was a measured regression
    /// (node 96 deferred 9,224 requests while reporting zero drops).
    pub vantage_repair_emit_ceiling: IntGauge,
    /// Legacy/control counter for emit-ceiling halvings caused by core-queue pressure.
    /// Registered but deliberately never incremented in the queue-backoff ablation, so it
    /// stays a stable zero while preserving dashboard and time-series compatibility with the
    /// baseline arm. A nonzero value therefore proves the wrong binary is running.
    pub vantage_repair_ceiling_halved_by_queue: IntCounter,
    /// Emit-ceiling halvings caused by NEW bulk-inbound drops since the previous tick; the
    /// only active halving cause in the queue-backoff ablation.
    pub vantage_repair_ceiling_halved_by_drops: IntCounter,
    /// Ticks on which `adapt_recovery_ceiling` ran and RAISED (or held at max) the ceiling.
    /// The denominator: if the ceiling is pinned at the floor, this distinguishes "the
    /// controller keeps choosing to halve" from "the controller stopped running and the
    /// gauge is stale", which are opposite bugs with opposite fixes.
    pub vantage_repair_ceiling_raised: IntCounter,
    /// In-flight repair slots reclaimed by `ASK_TIMEOUT_TICKS` because a round went
    /// unanswered. Nonzero means asks are being lost -- most likely the receiver's bulk
    /// queue `try_send`-dropping `HeadersRequest`, which N6 makes unrecoverable without
    /// this reclaim. Expected at/near zero on a healthy run; a rising value is the signal
    /// that repair requests are being burned.
    pub vantage_repair_asks_reclaimed_total: IntCounter,
    /// Availability credits skipped because the ref had already reached the terminal
    /// `Quorum` threshold, so the credit could not change any output.
    ///
    /// This is the ack fan-in's waste, and it dominated core time at n=100: 190,292
    /// credited refs/s per node, 96.3 per avail message (one watermark entry per author),
    /// 48.1s of a 122.6s window at 2.06us each = 39% of one core against 49% total
    /// `inbound_dispatch`. All n senders credit the same block; only the first 2f+1 matter.
    /// Compare against `vantage_avail_credited_refs` for the fraction eliminated.
    pub vantage_avail_credit_skipped_total: IntCounter,
    /// Repair requests outstanding (emitted, digest not yet in hand). Capped by
    /// `RECOVERY_IN_FLIGHT_MAX`. This is what bounds INBOUND: an answer only arrives for
    /// something we asked for. A rate limit alone does not -- the 2026-08-07 run had 3,420
    /// digests asked of ~49 peers each, ~167k invited answers, which pinned
    /// `core_queue_length` at its 1000-slot cap while `bulk_inbound_dropped` stayed 0.
    pub vantage_repair_in_flight: IntGauge,
    /// Cached blocks evicted because every peer confirmed holding them. Zero means eviction
    /// is not acting -- check `vantage_block_cache_evict_blocked` before concluding the cache
    /// is simply small.
    pub vantage_block_cache_evicted_total: IntCounter,
    /// Lanes whose eviction floor is unknown because some peer has not reported on that lane,
    /// so their blocks are pinned in memory. Nonzero is safe but means memory keeps growing
    /// for those lanes: a lagging peer pins its lane rather than authorising an unsafe drop.
    pub vantage_block_cache_evict_blocked: IntGauge,
    /// Times a repair request was deferred to the next tick because this tick's recovery
    /// allowance (`RECOVERY_EMIT_PER_TICK`) was exhausted. Zero on a healthy node. Nonzero
    /// means a node is recovering at the ceiling -- which is the intended behaviour, not an
    /// error: the per-mechanism caps bound each mechanism, and this bounds their sum.
    pub vantage_repair_budget_deferred_total: IntCounter,
    /// `BlockCache` entries held. The cache has NO eviction ("every block this node has
    /// ever obtained"), so this is monotone and is the leading suspect for the residual
    /// ~2.8 MB/s/node RSS growth that `local-dryrun/rss-growth.sh` measures. Divide that
    /// MB/s by this series' growth to get bytes-per-block.
    pub vantage_block_cache_len: IntGauge,
    /// `AckAggregator::senders` size: refs whose first-hand ack set is still being
    /// accumulated. Retired at `Quorum`, so this tracks refs still BELOW quorum and must
    /// NOT grow with every block ever seen -- that growth was the dominant memory leak at
    /// n=100 (13.43 MB/s per node, 1.19 -> 2.73 GiB over a 123s window, ~7 min to OOM on an
    /// 8 GiB box). One `HashSet<PublicKey>` of ~97 entries is ~4.2 KB, held per block
    /// forever.
    pub vantage_ack_senders_tracked: IntGauge,
    /// `AckAggregator::emitted` size: refs retired after reaching a threshold. ~73 B each
    /// against `senders`' ~4.2 KB, so retirement is a ~59x cut -- but this map is still
    /// unbounded (~0.8 GB/hour at n=100), and this is the series that shows it.
    pub vantage_ack_refs_retired: IntGauge,
    /// Digests whose peer fan-out is started but not yet complete (`Repairer::fanout`).
    /// The n=100 recovery fix stages coverage instead of asking all n-1 peers at once, so
    /// this is the outstanding-repair backlog: healthy nodes sit near zero, and a node in
    /// recovery shows the real size of its gap (the 2026-08-07 run's stalled nodes were
    /// missing 6,328-51,851 distinct digests, which no counter exposed at the time).
    pub vantage_repair_fanout_pending: IntGauge,
    /// Fan-out rounds beyond the first, i.e. how often the first `FANOUT_FIRST` peers
    /// failed to answer and coverage had to widen. Near-zero means the bounded first round
    /// is sufficient in practice; large means peers are not holding what we ask for.
    pub vantage_repair_fanout_escalations_total: IntCounter,

    // --- Metrics/dashboard expansion (METRICS-DASHBOARD-SPEC.md §1): wire-layer
    // counters, hooked in the `network` crate itself so every protocol (Autobahn
    // Optimistic/Seamless and Vantage) and every direction is covered by construction,
    // not by remembering to instrument each call site separately. Untyped totals
    // mirror starfish's own hook (`network.rs:614-691`) and include the 4-byte
    // length-delimited-codec frame prefix; the typed vectors go beyond starfish
    // (serialized length is already in hand at the same call sites) and are labeled
    // with the wire variant name of every `PrimaryMessage`/`PrimaryWorkerMessage`/
    // `WorkerPrimaryMessage`/`WorkerMessage` variant. No per-peer labels (starfish
    // parity -- committee size is small).
    /// Total bytes physically written to the wire across every outbound connection
    /// this node's senders (`ReliableSender`/`SimpleSender`) own, length prefix
    /// included. Zero-cost when no sender attaches a metrics handle (`with_metrics`
    /// is never called, e.g. in any test harness that doesn't wire it up).
    pub bytes_sent_total: IntCounter,
    /// Total bytes physically read off the wire by every inbound connection this
    /// node's `network::Receiver`s own, length prefix included.
    pub bytes_received_total: IntCounter,
    /// Wire messages sent, by `type` (the wire variant name), counted at the
    /// send/broadcast call site where the variant is known -- once per physical
    /// unicast transmission (a broadcast to n peers increments this n times, same
    /// convention as `bytes_sent_total`).
    pub network_messages_sent_total: IntCounterVec,
    /// Wire messages received, by `type`, counted at receiver dispatch post-deserialize.
    pub network_messages_received_total: IntCounterVec,
    /// Serialized (pre-frame-prefix) bytes sent, by `type`.
    pub network_bytes_sent_total: IntCounterVec,
    /// Serialized (pre-frame-prefix) bytes received, by `type`.
    pub network_bytes_received_total: IntCounterVec,
    /// Physical wire frames sent (length-prefix-delimited units), across every
    /// connection this node's senders own -- NOT per-type. When transport-level
    /// batching (`Parameters::batch_messages`) is off, every logical message is its
    /// own frame, so this equals the sum of `network_messages_sent_total` across
    /// types. When batching is on, several coalesced logical messages can share one
    /// frame, so `network_messages_sent_total` (sum) / `network_frames_sent_total`
    /// reads directly as the coalescing ratio.
    pub network_frames_sent_total: IntCounter,
    /// Volatile sends shed at enqueue because the destination's outbound queue depth
    /// reached `ReliableSender`'s volatile soft cap -- each shed message's filing key
    /// is min-merged into the drop map exactly like a session-death discard, so the
    /// reconnect-replay nudge/Hello path recovers it (`n=100 straggler fix
    /// 2026-08-08: a connected-but-slow peer now earns replay episodes without a
    /// session death). Sustained growth against one peer means that peer cannot keep
    /// up with organic broadcast volume; zero is the healthy steady state.
    pub network_volatile_shed_total: IntCounter,
    /// Currently-open inbound TCP connections, labeled by listener role. The
    /// Prometheus target already separates primary and worker processes; this label
    /// separates the listeners inside a process.
    pub network_connections: IntGaugeVec,
    /// `SimpleSender` frames discarded while waiting out a connect backoff, i.e.
    /// addressed to a peer we have not managed to connect to yet. Best-effort sends
    /// are allowed to vanish, but they must not vanish SILENTLY: this is the only
    /// signal distinguishing "the link is down and we are shedding" from "the link is
    /// fine and the peer is ignoring us". Sustained growth against one peer means that
    /// peer has been unreachable for a while.
    pub network_connect_wait_discarded_total: IntCounter,

    // --- METRICS-DASHBOARD-SPEC.md §2: goodput / pipeline counters (worker ingress).
    /// Transactions the worker's `BatchMaker` received from a client, before batching
    /// (the numerator for submission-side throughput; `committed_transactions` above
    /// remains the sequenced-goodput denominator, unchanged).
    pub submitted_transactions: IntCounter,
    /// Bytes of transactions the worker's `BatchMaker` received from a client.
    pub submitted_transactions_bytes: IntCounter,

    // --- METRICS-DASHBOARD-SPEC.md §3: consensus quality / utilization.
    /// Vantage block serialized size at publish time (self-authored blocks only),
    /// reported via the same `HistogramReporter` pattern as
    /// `transaction_committed_latency`.
    pub proposed_block_size_bytes: HistogramSender<usize>,
    /// Starfish parity: the proposed header/block's METADATA alone, serialized in
    /// isolation from any payload -- at n=50 this is the metric that distinguishes
    /// O(n^2) from O(n^3) metadata growth across protocols (a header whose own size
    /// is O(n), e.g. an embedded vote list, times n headers/round times n peers/
    /// broadcast). Self-authored proposals only, observed at the same publish call
    /// site as `proposed_block_size_bytes` -- see `primary::core::Core::
    /// process_own_header` (Autobahn, both optimistic and seamless) and
    /// `vantage::wire::Wire::broadcast_message` (Vantage and Simple-IT, which reuses
    /// Vantage's data plane verbatim). Identical value to `proposed_block_size_bytes`
    /// on the two Vantage-family protocols today (their wire `Header` never carries
    /// inline transactions, only payload digests -- see `proposed_transaction_size_
    /// bytes`'s doc comment), computed as a separate serialization anyway so the two
    /// metrics stay independently correct if that ever changes.
    pub proposed_header_size_bytes: HistogramSender<usize>,
    /// Starfish parity, adapted to this codebase's Narwhal-style architecture: this
    /// repo's headers/proposals never carry transaction bytes inline (only batch
    /// digests -- `primary::messages::Header::payload: BTreeMap<Digest, WorkerId>`),
    /// unlike starfish's monolithic blocks, which do. The closest analogue of
    /// starfish's per-block transaction-payload size is therefore observed one layer
    /// down, at the point this node's own worker seals a batch of transactions for
    /// inclusion (`worker::batch_maker::BatchMaker::seal`) -- own batches only,
    /// matching every other `proposed_*` metric's self-authored-only scope. Lives on
    /// the WORKER's own registry (a distinct scrape target from
    /// `proposed_header_size_bytes`, which is primary-side), same split as
    /// `submitted_transactions`/`committed_transactions`.
    pub proposed_transaction_size_bytes: HistogramSender<usize>,
    /// Starfish-style (`metrics.rs:1325-1376`) busy-time accounting around
    /// `VantageCore`'s own major sections, in accumulated microseconds, labeled by
    /// `proc` (section name). A `Drop`-guard timer (see `stat::UtilizationTimer`)
    /// adds its elapsed wall time to this counter when it goes out of scope, whether
    /// via normal fall-through or an early return/`?`.
    pub utilization_timer: IntCounterVec,
    /// Fable perf audit (measurement gap): the WAITING subset of `utilization_timer`,
    /// same `proc` labeling, same microsecond units, same `Drop`-guard mechanism.
    /// `utilization_timer` records WALL time, so a section blocked on an `.await`
    /// (notably the store actor's FIFO) is indistinguishable from one burning CPU.
    /// Scopes opened against THIS counter mark regions whose cost is known to be
    /// waiting rather than computing, so the dashboard can read
    /// `utilization - wait ~= CPU` per section and tell a CPU-bound core apart from
    /// one starved by a downstream queue. Deliberately NOT a partition of
    /// `utilization_timer`'s labels: a wait scope nested inside a utilization scope
    /// contributes to both (that is the point), and `proc` values need not match.
    ///
    /// Two caveats the dashboard has to respect. (a) The `store_probe` scope lives in
    /// `LaneManager`, which Simple-IT also uses, whereas every `utilization_timer`
    /// scope is in `vantage::node` -- so on a Simple-IT run this counter advances while
    /// `utilization_timer` stays flat, and any panel computing `utilization - wait`
    /// must filter to `protocol_info{protocol="vantage"}` or it renders negative.
    /// (b) `utilization_timer`'s `avail_flush` / `resume_tick` / `inbound_dispatch` /
    /// `effect_execution` sections also await the network with no matching wait scope,
    /// so `utilization - wait` is an UPPER bound on CPU, not an equality.
    pub core_wait_timer: IntCounterVec,
    /// `VantageCore`'s own inbound-message channel depth, sampled the same way as
    /// the Finding-A progress gauges (once/sec, in `VantageCore::run`'s own select
    /// loop) -- `rx_vantage.len()` (a `tokio::sync::mpsc::Receiver` exposes this
    /// cheaply without contorting the channel type). `0` on the two Autobahn paths.
    pub core_queue_length: IntGauge,
    /// Fable perf audit (measurement gap): the PEAK `rx_vantage.len()` observed since
    /// the previous 1 Hz sample, reset each time it is published. `core_queue_length`
    /// is sampled once/sec FROM the busy core thread, so it can only ever be read at
    /// an instant when that thread is between select branches -- systematically
    /// missing the sub-second bursts that matter (a core that is CPU-bound shows a
    /// growing peak even while the instantaneous sample keeps returning ~0).
    pub core_queue_peak: IntGauge,

    // --- Worker-process observability (2026-08-08 wedge post-mortem).
    //
    // Under n=50 @ 200k tx/s with real netem queues, 5-8/50 nodes stopped committing
    // while their PRIMARIES stayed healthy -- cursor advancing, seal mix normal. The
    // worker's `network_bytes_received{Batch}` delta was exactly 0 on those nodes,
    // worker CPU 10s against 74s healthy, and the primary was sending its own worker
    // ~600 `Synchronize`/s against ~11/s. Everything touching the worker's `Store`
    // froze together and nothing else did.
    //
    // Diagnosing that took a raw-scrape review and four refuted hypotheses, for one
    // structural reason: between `submitted_transactions` and
    // `committed_transactions` -- two counters in two different processes -- the
    // worker published NOTHING about its own internal pipeline. Every bounded channel
    // between the two was invisible, so a wedge in any of them looked identical from
    // the outside to a slow primary. These three families close that gap.
    /// Occupancy of each bounded worker channel, sampled at 10 Hz by
    /// `Worker::spawn`'s sampler task and published once a second. Labels are the
    /// pipeline stages the channel feeds: `synchronizer` (primary -> worker
    /// `Synchronize`, the path that carried the 600/s flood), `batch_maker` (client
    /// transactions), `processor_own` (our sealed batches), `processor_peer` (batches
    /// from other workers -- the path that read a flat zero), `helper` (peer batch
    /// requests), `primary_connector` (worker -> primary digests), and `store` (the
    /// store actor's own command channel).
    ///
    /// A wedge shows as one or more of these pinned at `worker_queue_capacity`; which
    /// ones are pinned localises it to a stage without a debugger. Absent on the
    /// primary process, which has no worker channels.
    pub worker_queue_depth: IntGaugeVec,
    /// Peak `worker_queue_depth` over the second preceding each publish, reset on
    /// publish -- the same instantaneous-vs-burst argument as `core_queue_peak`. A
    /// channel that is momentarily full 5 times a second reads ~0 on every 1 Hz
    /// instantaneous sample.
    pub worker_queue_peak: IntGaugeVec,
    /// The bound each labeled channel was constructed with, so a dashboard plots
    /// occupancy as a fraction without hard-coding `CHANNEL_CAPACITY` (1000) or the
    /// store actor's own 100. Write-once at boot; same label set as the two above.
    pub worker_queue_capacity: IntGaugeVec,
    /// Milliseconds since the store actor last COMPLETED a `select!` iteration (see
    /// `store::Store::heartbeat_millis`). Steady state is under
    /// `FLUSH_INTERVAL_MS` (50) because the flush ticker fires unconditionally, so
    /// this is load-independent and a large value is always a real stall.
    ///
    /// This is the reading that separates the two live hypotheses for the wedge, and
    /// it does so remotely, from a scrape, with no thread dump: a **blocked** actor
    /// (RocksDB write stall, cold `db.get`) shows a growing age with the channel
    /// pinned full, whereas a **dead** actor (task panic) shows the same growing age
    /// alongside a nonzero `process_panics_total`.
    pub store_actor_heartbeat_age_ms: IntGauge,
    /// Peak `store_actor_heartbeat_age_ms` over the second preceding each publish, reset
    /// on publish. The instantaneous gauge above can only report the age at the moment of
    /// a scrape, so a stall that started and ended between two scrapes -- a RocksDB write
    /// stall, a cold `db.get` on a compacting LSM -- is invisible to it.
    pub store_actor_heartbeat_age_ms_peak: IntGauge,
    /// Commands the store actor has DEQUEUED (see `store::Store::commands_drained`).
    ///
    /// The discriminator that `store_actor_heartbeat_age_ms` cannot provide on its own,
    /// and the metric that would have prevented misreading the 2026-08-08 wedge as
    /// saturation. `worker_queue_depth{queue="store"}` counts permits HELD, not messages
    /// queued, so a full reading has two opposite causes:
    ///
    ///   full + this ADVANCING -> real saturation; the actor is the bottleneck.
    ///   full + this FLAT + heartbeat fresh -> the queue is EMPTY and every permit sits in
    ///       a `send()` future that will never be polled again. Actor idle, senders
    ///       deadlocked against it. This is what actually happened.
    pub store_commands_drained_total: IntCounter,
    /// Headers whose payload is still incomplete (`PayloadIo::pending_payload`). Unbounded
    /// by design while a worker is not materialising, so its growth IS the symptom.
    pub vantage_pending_payload_headers: IntGauge,
    /// Total outstanding `(digest, worker_id)` keys across those headers -- the quantity
    /// that actually scales with the backlog, since one header can miss many batches.
    pub vantage_pending_payload_keys: IntGauge,
    /// Size of `PayloadIo::last_synchronize`. Was insert-only (one entry per distinct
    /// `(digest, worker_id)` ever synced, never removed) and so grew without bound for the
    /// life of the process; now pruned, and this is how that stays true.
    pub vantage_last_synchronize_len: IntGauge,
    /// Nodes visited by the three O(gap) prefix walks, by `family`:
    /// `chain` (`verified_prefix_through_genesis`), `direct` (`direct_prefix_ok`),
    /// `settle` (`Repairer::settle`'s descend).
    ///
    /// All three memoize SUCCESS only, so a cached suffix above a MISSING block is
    /// re-walked in full on every call -- and all three are called per inbound message
    /// (`recheck_all`) or per publish (`refresh_author`). This family exists to test that
    /// as the cause of the 2026-08-08 n=100 straggler tail, where 10/100 nodes ran their
    /// core at 97% busy on FEWER messages and FEWER settle calls than healthy nodes
    /// (~99 us/message against ~39 us), which no volume-based explanation fits.
    ///
    /// Read as a ratio to `blocks_received`: bounded (order 1x) when every walk
    /// short-circuits on a memo, orders of magnitude larger once a hole forces full
    /// re-walks. A straggler whose walk-step rate is NOT elevated refutes the hypothesis.
    pub vantage_walk_steps_total: IntCounterVec,
    /// Body-fetch pairs given up on after `MAX_FETCH_ATTEMPTS` rather than asked again.
    ///
    /// Abandoning is safe and re-creatable (see `MAX_FETCH_ATTEMPTS`), so a healthy rate
    /// here is not an error -- it is the mechanism working. What it bounds: a stalled node
    /// sent 433,656 body fetches in 120s against 53 on a healthy peer, at a network-wide
    /// answer rate of 7.8%, each send costing ~50us on the single consensus core.
    pub vantage_body_fetch_abandoned_total: IntCounter,
    /// Panics observed by this process's panic hook (`install_panic_hook`).
    ///
    /// tokio silently absorbs a panicking task: the panic travels in the `JoinHandle`,
    /// and every task in this codebase is spawned fire-and-forget, so a dead subsystem
    /// leaves the process alive, the metrics server answering, and every OTHER counter
    /// still advancing. That is precisely what a wedged worker looked like from
    /// outside. A nonzero value here turns "we cannot tell whether it parked or died"
    /// into an answer, from a scrape.
    ///
    /// A GAUGE rather than a counter, deliberately: the hook is a process-global
    /// singleton but `Metrics` is not (`node local-benchmark` builds one per in-process
    /// node), so the value published is the process-wide running total written straight
    /// from the hook. A counter's `inc_by` contract cannot express "mirror a global
    /// that other registries also observe" without per-registry reconciliation state.
    pub process_panics: IntGauge,

    // --- METRICS-DASHBOARD-SPEC.md §8 addenda.
    /// Write-once at boot: which protocol this node is running (starfish pattern --
    /// `consensus_protocol_info`). Always exactly one label value set to `1`.
    pub protocol_info: IntGaugeVec,
    /// Write-once (where known -- see `set_transaction_mode_info`'s doc): which
    /// client transaction-payload mode this run uses.
    pub transaction_mode_info: IntGaugeVec,

    // --- Perf-audit addendum: metrics-active window (starfish parity,
    // `metrics.rs`'s own `metrics_active`/`transactions_generator.rs`'s early
    // return in `RealCommitHandler::transaction_observer`).
    /// True iff commit-time observations should feed the rate-relevant counters
    /// (the two transaction-latency histograms and their squared-micros
    /// accumulators, `committed_transactions`/`committed_bytes`, and
    /// `latency_misses`/`latency_misses_resolved`) -- gated in
    /// `worker::synchronizer::Synchronizer::observe_committed`/
    /// `finish_deferred_retry`, mirroring starfish's identical early return in
    /// `RealCommitHandler::transaction_observer`. Outside the active window (a
    /// warmup before load generation starts, or a wind-down after it stops), a late
    /// commit would otherwise skew TPS, the latency distribution, and the
    /// bandwidth-efficiency denominator -- exactly starfish's own rationale.
    /// Defaults to `true` (active): unlike starfish, nothing in this codebase's
    /// benchmark harness currently flips this (see METRICS-NOTES.md/this change's
    /// own report for what the equivalent hook would be), so every existing run's
    /// numbers are unaffected until something does. `Arc`-wrapped (matching
    /// starfish's own type exactly) so every clone of `Metrics` -- there is
    /// currently one long-lived instance per primary/worker, but the struct is
    /// `Clone` -- observes the same flag.
    pub metrics_active: Arc<AtomicBool>,
    /// Absolute wall-clock instant (epoch milliseconds) from which an observation
    /// counts; `0` means "no gate" (every observation counts, the pre-existing
    /// behaviour). Set write-once at boot from `config::Parameters::
    /// metrics_active_at_ms` -- see that field for why the window is an absolute
    /// instant rather than a per-node uptime offset.
    ///
    /// This is the FINE-GRAINED companion to `metrics_active` above: that flag gates
    /// a whole `observe_committed` call, whereas this compares each transaction's own
    /// embedded submission timestamp, so a transaction submitted during warmup is
    /// excluded even though it commits inside the active window. Exactness matters
    /// here: the startup transient is precisely the population whose SUBMISSION was
    /// early, and it is those transactions whose multi-second latencies dominated
    /// p99 (measured 2026-08-06: a fixed ~7.4s submission window contributed ~5% of
    /// all observations at every offered rate, pinning p99 near 3.5s while p95 stayed
    /// under 600ms).
    pub active_from_millis: Arc<AtomicU64>,
}

/// Owns the receiving half of the latency histogram and periodically drains + publishes
/// it as labeled gauges. `Metrics` (the sender-side handles) is `Clone`+`Send`+`Sync` and
/// can be shared freely; `MetricReporter` is not meant to be touched outside its own
/// background task other than via `start`.
pub struct MetricReporter {
    transaction_committed_latency: Mutex<HistogramReporter<Duration>>,
    transaction_materialised_latency: Mutex<HistogramReporter<Duration>>,
    proposed_block_size_bytes: Mutex<HistogramReporter<usize>>,
    proposed_header_size_bytes: Mutex<HistogramReporter<usize>>,
    proposed_transaction_size_bytes: Mutex<HistogramReporter<usize>>,
}

/// Publishes a `PreciseHistogram<T>` as a `name{v="..."}` gauge vector: exact count, sum,
/// max and the p25/p50/p75/p90/p95/p99 quantiles. This preserves starfish's exposition
/// shape and adds p95 for the benchmark dashboard.
pub struct HistogramReporter<T> {
    histogram: PreciseHistogram<T>,
    gauge: IntGaugeVec,
}

pub trait AsPrometheusMetric {
    fn as_prometheus_metric(&self) -> i64;
}

impl AsPrometheusMetric for Duration {
    fn as_prometheus_metric(&self) -> i64 {
        self.as_micros() as i64
    }
}

impl AsPrometheusMetric for usize {
    fn as_prometheus_metric(&self) -> i64 {
        *self as i64
    }
}

impl<T: Ord + AddAssign + DivUsize + Copy + Default + AsPrometheusMetric> HistogramReporter<T> {
    pub fn new_in_registry(
        histogram: PreciseHistogram<T>,
        registry: &Registry,
        name: &str,
    ) -> Self {
        let gauge = register_int_gauge_vec_with_registry!(name, name, &["v"], registry).unwrap();
        Self { histogram, gauge }
    }

    /// Publish the current exact quantiles. A no-op (leaves the gauge unset) until the
    /// first observation arrives, so an idle `Metrics` (e.g. primary's in Phase 2, which
    /// registers this same shape but never observes into it) simply omits the metric
    /// from its scrape output rather than reporting a misleading zero.
    pub fn report(&mut self) {
        let Some([p25, p50, p75, p90, p95, p99]) =
            self.histogram.pcts([250, 500, 750, 900, 950, 990])
        else {
            return;
        };
        let Some(max) = self.histogram.max() else {
            return;
        };
        self.gauge
            .with_label_values(&["p25"])
            .set(p25.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p50"])
            .set(p50.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p75"])
            .set(p75.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p90"])
            .set(p90.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p95"])
            .set(p95.as_prometheus_metric());
        self.gauge
            .with_label_values(&["p99"])
            .set(p99.as_prometheus_metric());
        self.gauge
            .with_label_values(&["max"])
            .set(max.as_prometheus_metric());
        self.gauge
            .with_label_values(&["sum"])
            .set(self.histogram.total_sum().as_prometheus_metric());
        self.gauge
            .with_label_values(&["count"])
            .set(self.histogram.total_count() as i64);
    }

    pub fn receive_all(&mut self) {
        self.histogram.receive_all();
    }
}

impl Metrics {
    /// Registers this phase's metrics into `registry` and returns the (sender-side,
    /// reporter-side) pair. Both primary and worker call this on their own registry;
    /// only worker's copy is ever observed into in Phase 2 (see PHASE2-NOTES.md).
    pub fn new(registry: &Registry) -> (Arc<Self>, Arc<MetricReporter>) {
        // The node-side active-window clock the harness divides rates by. Created here
        // so the collector and `Metrics::active_from_millis` share ONE Arc: arming the
        // window via `set_active_from_millis` is then immediately visible to every
        // subsequent scrape. Registration failure is non-fatal -- a duplicate registry
        // (only tests build several) must not take a validator down over one series.
        let active_from_millis = Arc::new(AtomicU64::new(0));
        if let Err(e) = registry.register(Box::new(ActiveWindowCollector::new(
            active_from_millis.clone(),
        ))) {
            log::warn!("could not register metrics_active_seconds: {e}");
        }
        let (transaction_committed_latency_hist, transaction_committed_latency) = histogram();
        let (transaction_materialised_latency_hist, transaction_materialised_latency) = histogram();
        let (proposed_block_size_bytes_hist, proposed_block_size_bytes) = histogram();
        let (proposed_header_size_bytes_hist, proposed_header_size_bytes) = histogram();
        let (proposed_transaction_size_bytes_hist, proposed_transaction_size_bytes) = histogram();

        let reporter = MetricReporter {
            transaction_committed_latency: Mutex::new(HistogramReporter::new_in_registry(
                transaction_committed_latency_hist,
                registry,
                "transaction_committed_latency",
            )),
            transaction_materialised_latency: Mutex::new(HistogramReporter::new_in_registry(
                transaction_materialised_latency_hist,
                registry,
                "transaction_materialised_latency",
            )),
            proposed_block_size_bytes: Mutex::new(HistogramReporter::new_in_registry(
                proposed_block_size_bytes_hist,
                registry,
                "proposed_block_size_bytes",
            )),
            proposed_header_size_bytes: Mutex::new(HistogramReporter::new_in_registry(
                proposed_header_size_bytes_hist,
                registry,
                "proposed_header_size_bytes",
            )),
            proposed_transaction_size_bytes: Mutex::new(HistogramReporter::new_in_registry(
                proposed_transaction_size_bytes_hist,
                registry,
                "proposed_transaction_size_bytes",
            )),
        };

        let metrics = Self {
            transaction_committed_latency,
            transaction_committed_latency_squared_micros: register_int_counter_with_registry!(
                "transaction_committed_latency_squared_micros",
                "Sum of (transaction commit latency in microseconds)^2, for exact stddev",
                registry,
            )
            .unwrap(),
            transaction_materialised_latency,
            transaction_materialised_latency_squared_micros: register_int_counter_with_registry!(
                "transaction_materialised_latency_squared_micros",
                "Sum of (transaction materialised latency in microseconds)^2, for exact stddev",
                registry,
            )
            .unwrap(),
            committed_transactions: register_int_counter_with_registry!(
                "committed_transactions",
                "Total committed transactions whose latency was observed",
                registry,
            )
            .unwrap(),
            committed_bytes: register_int_counter_with_registry!(
                "committed_bytes",
                "Total bytes of committed transactions whose latency was observed",
                registry,
            )
            .unwrap(),
            latency_misses: register_int_counter_with_registry!(
                "latency_misses",
                "Commit-time batch lookups that missed the local store and were deferred for retry",
                registry,
            )
            .unwrap(),
            latency_misses_resolved: register_int_counter_with_registry!(
                "latency_misses_resolved",
                "Deferred commit-time misses (latency_misses) that later resolved and were counted",
                registry,
            )
            .unwrap(),
            vantage_blocks_published: register_int_counter_with_registry!(
                "vantage_blocks_published",
                "Vantage blocks this node published",
                registry,
            )
            .unwrap(),
            vantage_blocks_received: register_int_counter_with_registry!(
                "vantage_blocks_received",
                "Vantage blocks this node received and cached",
                registry,
            )
            .unwrap(),
            vantage_acks_sent: register_int_counter_with_registry!(
                "vantage_acks_sent",
                "Vantage acks this node broadcast",
                registry,
            )
            .unwrap(),
            vantage_acks_received: register_int_counter_with_registry!(
                "vantage_acks_received",
                "Vantage acks this node counted first-hand",
                registry,
            )
            .unwrap(),
            vantage_repairs_requested: register_int_counter_with_registry!(
                "vantage_repairs_requested",
                "Vantage request(h) messages this node sent",
                registry,
            )
            .unwrap(),
            vantage_repairs_served: register_int_counter_with_registry!(
                "vantage_repairs_served",
                "Vantage serve(h, b) messages this node sent",
                registry,
            )
            .unwrap(),
            vantage_retained_bytes: register_int_counter_with_registry!(
                "vantage_retained_bytes",
                "Cumulative bincode-encoded size of every vantage block this node retained",
                registry,
            )
            .unwrap(),
            vantage_rejected_nonmember_total: register_int_counter_with_registry!(
                "vantage_rejected_nonmember_total",
                "Inbound vantage wire messages dropped for a non-committee-member declared sender",
                registry,
            )
            .unwrap(),
            vantage_bulk_inbound_dropped_total: register_int_counter_with_registry!(
                "vantage_bulk_inbound_dropped_total",
                "Bulk recovery messages dropped because the bulk inbound queue was full",
                registry,
            )
            .unwrap(),
            vantage_avail_sent: register_int_counter_with_registry!(
                "vantage_avail_sent",
                "Ack-watermark broadcasts this node sent",
                registry,
            )
            .unwrap(),
            vantage_avail_received: register_int_counter_with_registry!(
                "vantage_avail_received",
                "Ack-watermark broadcasts this node received",
                registry,
            )
            .unwrap(),
            vantage_avail_credited_refs: register_int_counter_with_registry!(
                "vantage_avail_credited_refs",
                "BlockRefs credited into the shared AckAggregator via the ack-watermark front-end",
                registry,
            )
            .unwrap(),
            simpleit_unexpected_effect_total: register_int_counter_with_registry!(
                "simpleit_unexpected_effect_total",
                "SimpleItCore received a vantage::Effect variant lm/rep can never produce",
                registry,
            )
            .unwrap(),
            simpleit_commit_queue_len: register_int_gauge_with_registry!(
                "simpleit_commit_queue_len",
                "SimpleItCore: committed rounds queued pending full materialisation",
                registry,
            )
            .unwrap(),
            vantage_seals: register_int_counter_vec_with_registry!(
                "vantage_seals",
                "Vantage views sealed, by route (fast_full/direct_full/direct_core/anchor_full/anchor_core/anchor_skip/vote_skip)",
                &["route"],
                registry,
            )
            .unwrap(),
            vantage_skip_votes_sent: register_int_counter_with_registry!(
                "vantage_skip_votes_sent",
                "Grounded SKIP-VOTE(u) statements this node broadcast",
                registry,
            )
            .unwrap(),
            vantage_skip_votes_received: register_int_counter_with_registry!(
                "vantage_skip_votes_received",
                "Grounded SKIP-VOTE(u) statements this node counted first-hand from a peer",
                registry,
            )
            .unwrap(),
            vantage_body_fetches_sent: register_int_counter_with_registry!(
                "vantage_body_fetches_sent",
                "Digest-named AGB statements (signature-free.tex sec.8.3): VantageBodyFetch messages this node sent",
                registry,
            )
            .unwrap(),
            vantage_bodies_served: register_int_counter_with_registry!(
                "vantage_bodies_served",
                "Digest-named AGB statements (signature-free.tex sec.8.3): VantageBodyServe messages this node sent",
                registry,
            )
            .unwrap(),
            vantage_lane_resume_requests_sent: register_int_counter_with_registry!(
                "vantage_lane_resume_requests_sent",
                "Mechanism A (sender-side lane resume): VantageLaneResume requests this node sent",
                registry,
            )
            .unwrap(),
            vantage_lane_resume_blocks_served: register_int_counter_with_registry!(
                "vantage_lane_resume_blocks_served",
                "Mechanism A (sender-side lane resume): own blocks this node served answering a VantageLaneResume request",
                registry,
            )
            .unwrap(),
            vantage_lane_resume_send_drops: register_int_counter_with_registry!(
                "vantage_lane_resume_send_drops",
                "Mechanism A (sender-side lane resume): resume messages dropped because the dedicated resume-sender task's channel was full or closed",
                registry,
            )
            .unwrap(),
            vantage_replay_enqueue_drops_total: register_int_counter_with_registry!(
                "vantage_replay_enqueue_drops_total",
                "Reconnect replay: enqueue_replay try_send drops (Full/Closed) onto the resume-sender task",
                registry,
            )
            .unwrap(),
            vantage_replay_done_clamped_total: register_int_counter_with_registry!(
                "vantage_replay_done_clamped_total",
                "Reconnect replay: VantageReplayDone sends truncated below the requested span by outbox_floor",
                registry,
            )
            .unwrap(),
            vantage_replay_pending_low_nudges_total: register_int_counter_with_registry!(
                "vantage_replay_pending_low_nudges_total",
                "Reconnect replay: server-side nudge Hellos sent for a set pending_low with no recent serve",
                registry,
            )
            .unwrap(),
            vantage_replay_inflight_ttl_expired_total: register_int_counter_with_registry!(
                "vantage_replay_inflight_ttl_expired_total",
                "Reconnect replay: stale in-flight-replay-stream entries evicted past replay_episode_max_ms",
                registry,
            )
            .unwrap(),
            vantage_entered_view: register_int_gauge_with_registry!(
                "vantage_entered_view",
                "Pacemaker: largest view formally entered (W5)",
                registry,
            )
            .unwrap(),
            vantage_own_watermark: register_int_gauge_with_registry!(
                "vantage_own_watermark",
                "Pacemaker: this party's own current wish",
                registry,
            )
            .unwrap(),
            vantage_entry_target: register_int_gauge_with_registry!(
                "vantage_entry_target",
                "Pacemaker: current entry target",
                registry,
            )
            .unwrap(),
            vantage_omega_q: register_int_gauge_with_registry!(
                "vantage_omega_q",
                "Pacemaker: (2f+1)-party wish statistic omega_q",
                registry,
            )
            .unwrap(),
            vantage_frontier_a_i: register_int_gauge_with_registry!(
                "vantage_frontier_a_i",
                "Frontier: responsive proposal frontier a_i",
                registry,
            )
            .unwrap(),
            vantage_cursor_next_view: register_int_gauge_with_registry!(
                "vantage_cursor_next_view",
                "Cursor: lowest view not yet advanced past",
                registry,
            )
            .unwrap(),
            vantage_control_round: register_int_gauge_with_registry!(
                "vantage_control_round",
                "ControlLog: current Simple-IT resolution round",
                registry,
            )
            .unwrap(),
            vantage_control_delivered_len: register_int_gauge_with_registry!(
                "vantage_control_delivered_len",
                "ControlLog: total (view,digest) pairs RB-delivered into the control log",
                registry,
            )
            .unwrap(),
            vantage_control_consume_pos: register_int_gauge_with_registry!(
                "vantage_control_consume_pos",
                "ControlLog: consumption position of the delivered log (pump_log)",
                registry,
            )
            .unwrap(),
            vantage_pending_gate_len: register_int_gauge_with_registry!(
                "vantage_pending_gate_len",
                "AgbEngine: views active+fixed but not yet echoed (recheck_all's scan population)",
                registry,
            )
            .unwrap(),
            vantage_pending_body_fetch_len: register_int_gauge_with_registry!(
                "vantage_pending_body_fetch_len",
                "DigestStatements: outstanding AGB body fetches (capped by MAX_PENDING_FETCH)",
                registry,
            )
            .unwrap(),
            vantage_body_fetch_evicted_total: register_int_counter_with_registry!(
                "vantage_body_fetch_evicted_total",
                "Body-fetch pairs dropped because pending_fetch hit its ceiling",
                registry,
            )
            .unwrap(),
            vantage_pending_settle_len: register_int_gauge_with_registry!(
                "vantage_pending_settle_len",
                "Repairer: authorized-but-unsettled refs (on_block_available's sweep population)",
                registry,
            )
            .unwrap(),
            vantage_repair_settle_calls_total: register_int_counter_with_registry!(
                "vantage_repair_settle_calls_total",
                "Repairer::settle calls; divide by blocks_received for the sweep amplification",
                registry,
            )
            .unwrap(),
            vantage_repair_fanout_loops_total: register_int_counter_with_registry!(
                "vantage_repair_fanout_loops_total",
                "Times settle reached the missing-block branch (fan-out considered, not \
                 necessarily emitted); compare with vantage_repairs_requested",
                registry,
            )
            .unwrap(),
            vantage_repair_emit_ceiling: register_int_gauge_with_registry!(
                "vantage_repair_emit_ceiling",
                "Adaptive per-tick ceiling on repair-request emission (AIMD on bulk drops)",
                registry,
            )
            .unwrap(),
            vantage_repair_ceiling_halved_by_queue: register_int_counter_with_registry!(
                "vantage_repair_ceiling_halved_by_queue",
                "Legacy core-queue emit-ceiling halvings (zero in queue-backoff ablation)",
                registry,
            )
            .unwrap(),
            vantage_repair_ceiling_halved_by_drops: register_int_counter_with_registry!(
                "vantage_repair_ceiling_halved_by_drops",
                "Repair emit-ceiling halvings caused by new bulk-inbound drops",
                registry,
            )
            .unwrap(),
            vantage_repair_ceiling_raised: register_int_counter_with_registry!(
                "vantage_repair_ceiling_raised",
                "Ticks on which the repair emit ceiling was raised or held at maximum",
                registry,
            )
            .unwrap(),
            vantage_repair_asks_reclaimed_total: register_int_counter_with_registry!(
                "vantage_repair_asks_reclaimed_total",
                "In-flight repair slots reclaimed after an unanswered round timed out",
                registry,
            )
            .unwrap(),
            vantage_avail_credit_skipped_total: register_int_counter_with_registry!(
                "vantage_avail_credit_skipped_total",
                "Availability credits skipped because the ref was already at quorum (the \
                 ack fan-in's waste; compare with vantage_avail_credited_refs)",
                registry,
            )
            .unwrap(),
            vantage_repair_in_flight: register_int_gauge_with_registry!(
                "vantage_repair_in_flight",
                "Repair requests outstanding (the window that bounds inbound answers)",
                registry,
            )
            .unwrap(),
            vantage_block_cache_evicted_total: register_int_counter_with_registry!(
                "vantage_block_cache_evicted_total",
                "Cached blocks evicted because every peer confirmed holding them",
                registry,
            )
            .unwrap(),
            vantage_block_cache_evict_blocked: register_int_gauge_with_registry!(
                "vantage_block_cache_evict_blocked",
                "Lanes pinned in memory because some peer has not reported on them (safe, \
                 but memory keeps growing for those lanes)",
                registry,
            )
            .unwrap(),
            vantage_repair_budget_deferred_total: register_int_counter_with_registry!(
                "vantage_repair_budget_deferred_total",
                "Repair requests deferred to the next tick because this tick's recovery \
                 allowance was spent (bounds the SUM of the recovery mechanisms)",
                registry,
            )
            .unwrap(),
            vantage_block_cache_len: register_int_gauge_with_registry!(
                "vantage_block_cache_len",
                "BlockCache entries held (no eviction exists, so monotone)",
                registry,
            )
            .unwrap(),
            vantage_ack_senders_tracked: register_int_gauge_with_registry!(
                "vantage_ack_senders_tracked",
                "AckAggregator: refs still accumulating first-hand acks (retired at \
                 quorum; must not grow with every block ever seen)",
                registry,
            )
            .unwrap(),
            vantage_ack_refs_retired: register_int_gauge_with_registry!(
                "vantage_ack_refs_retired",
                "AckAggregator: refs retired after reaching a threshold (still unbounded)",
                registry,
            )
            .unwrap(),
            vantage_repair_fanout_pending: register_int_gauge_with_registry!(
                "vantage_repair_fanout_pending",
                "Repairer: digests whose peer fan-out is started but not yet complete \
                 (the outstanding-repair backlog, i.e. the size of this node's gap)",
                registry,
            )
            .unwrap(),
            vantage_repair_fanout_escalations_total: register_int_counter_with_registry!(
                "vantage_repair_fanout_escalations_total",
                "Repairer: fan-out rounds beyond the first (coverage had to widen because \
                 the bounded first round went unanswered)",
                registry,
            )
            .unwrap(),
            bytes_sent_total: register_int_counter_with_registry!(
                "bytes_sent_total",
                "Total bytes physically written to the wire (length prefix included)",
                registry,
            )
            .unwrap(),
            bytes_received_total: register_int_counter_with_registry!(
                "bytes_received_total",
                "Total bytes physically read from the wire (length prefix included)",
                registry,
            )
            .unwrap(),
            network_messages_sent_total: register_int_counter_vec_with_registry!(
                "network_messages_sent_total",
                "Wire messages sent, by type",
                &["type"],
                registry,
            )
            .unwrap(),
            network_messages_received_total: register_int_counter_vec_with_registry!(
                "network_messages_received_total",
                "Wire messages received, by type",
                &["type"],
                registry,
            )
            .unwrap(),
            network_bytes_sent_total: register_int_counter_vec_with_registry!(
                "network_bytes_sent_total",
                "Serialized bytes sent, by type (no frame prefix)",
                &["type"],
                registry,
            )
            .unwrap(),
            network_bytes_received_total: register_int_counter_vec_with_registry!(
                "network_bytes_received_total",
                "Serialized bytes received, by type (no frame prefix)",
                &["type"],
                registry,
            )
            .unwrap(),
            network_frames_sent_total: register_int_counter_with_registry!(
                "network_frames_sent_total",
                "Physical wire frames sent (bundles count once); compare against \
                 network_messages_sent_total for the batching coalescing ratio",
                registry,
            )
            .unwrap(),
            network_volatile_shed_total: register_int_counter_with_registry!(
                "network_volatile_shed_total",
                "Volatile sends shed at enqueue (outbound queue depth reached the soft \
                 cap); every shed key is min-merged into the drop map for replay",
                registry,
            )
            .unwrap(),
            network_connections: register_int_gauge_vec_with_registry!(
                "network_connections",
                "Currently-open inbound TCP connections by listener role",
                &["listener"],
                registry,
            )
            .unwrap(),
            network_connect_wait_discarded_total: register_int_counter_with_registry!(
                "network_connect_wait_discarded_total",
                "SimpleSender frames discarded while waiting out a connect backoff",
                registry,
            )
            .unwrap(),
            submitted_transactions: register_int_counter_with_registry!(
                "submitted_transactions",
                "Total transactions received by the worker's BatchMaker from a client",
                registry,
            )
            .unwrap(),
            submitted_transactions_bytes: register_int_counter_with_registry!(
                "submitted_transactions_bytes",
                "Total bytes of transactions received by the worker's BatchMaker from a client",
                registry,
            )
            .unwrap(),
            proposed_block_size_bytes,
            proposed_header_size_bytes,
            proposed_transaction_size_bytes,
            utilization_timer: register_int_counter_vec_with_registry!(
                "utilization_timer",
                "VantageCore busy time in microseconds, by proc (section name)",
                &["proc"],
                registry,
            )
            .unwrap(),
            core_wait_timer: register_int_counter_vec_with_registry!(
                "core_wait_timer",
                "Consensus-core time blocked on downstream I/O in microseconds, by proc",
                &["proc"],
                registry,
            )
            .unwrap(),
            core_queue_length: register_int_gauge_with_registry!(
                "core_queue_length",
                "VantageCore's own inbound-message channel depth",
                registry,
            )
            .unwrap(),
            core_queue_peak: register_int_gauge_with_registry!(
                "core_queue_peak",
                "VantageCore inbound-channel depth: peak since the previous sample",
                registry,
            )
            .unwrap(),
            worker_queue_depth: register_int_gauge_vec_with_registry!(
                "worker_queue_depth",
                "Occupancy of each bounded worker pipeline channel, by stage",
                &["queue"],
                registry,
            )
            .unwrap(),
            worker_queue_peak: register_int_gauge_vec_with_registry!(
                "worker_queue_peak",
                "Worker channel occupancy: peak since the previous publish, by stage",
                &["queue"],
                registry,
            )
            .unwrap(),
            worker_queue_capacity: register_int_gauge_vec_with_registry!(
                "worker_queue_capacity",
                "Bound each worker pipeline channel was constructed with, by stage",
                &["queue"],
                registry,
            )
            .unwrap(),
            store_actor_heartbeat_age_ms: register_int_gauge_with_registry!(
                "store_actor_heartbeat_age_ms",
                "Milliseconds since the store actor last completed a loop iteration",
                registry,
            )
            .unwrap(),
            store_actor_heartbeat_age_ms_peak: register_int_gauge_with_registry!(
                "store_actor_heartbeat_age_ms_peak",
                "Store-actor staleness: peak since the previous publish",
                registry,
            )
            .unwrap(),
            store_commands_drained_total: register_int_counter_with_registry!(
                "store_commands_drained_total",
                "Commands dequeued by the store actor (flat while depth is full = deadlock)",
                registry,
            )
            .unwrap(),
            vantage_pending_payload_headers: register_int_gauge_with_registry!(
                "vantage_pending_payload_headers",
                "Headers whose payload is still incomplete",
                registry,
            )
            .unwrap(),
            vantage_pending_payload_keys: register_int_gauge_with_registry!(
                "vantage_pending_payload_keys",
                "Outstanding (batch digest, worker) keys across all incomplete headers",
                registry,
            )
            .unwrap(),
            vantage_last_synchronize_len: register_int_gauge_with_registry!(
                "vantage_last_synchronize_len",
                "Size of the per-key Synchronize rate-limit map",
                registry,
            )
            .unwrap(),
            vantage_walk_steps_total: register_int_counter_vec_with_registry!(
                "vantage_walk_steps_total",
                "Nodes visited by the O(gap) prefix walks, by family",
                &["family"],
                registry,
            )
            .unwrap(),
            vantage_body_fetch_abandoned_total: register_int_counter_with_registry!(
                "vantage_body_fetch_abandoned_total",
                "Body-fetch pairs abandoned after the attempt cap rather than re-asked",
                registry,
            )
            .unwrap(),
            process_panics: register_int_gauge_with_registry!(
                "process_panics",
                "Panics observed by this process's panic hook (tokio absorbs task panics)",
                registry,
            )
            .unwrap(),
            protocol_info: register_int_gauge_vec_with_registry!(
                "protocol_info",
                "Write-once at boot: which protocol this node is running (value always 1)",
                &["protocol"],
                registry,
            )
            .unwrap(),
            transaction_mode_info: register_int_gauge_vec_with_registry!(
                "transaction_mode_info",
                "Write-once: which client transaction-payload mode this run uses (value always 1)",
                &["mode"],
                registry,
            )
            .unwrap(),
            // Perf-audit addendum: defaults active (starfish parity for "preserves
            // current behaviour when nothing sets it") -- see this field's own doc
            // comment for what would need to set it false.
            metrics_active: Arc::new(AtomicBool::new(true)),
            active_from_millis,
        };

        (Arc::new(metrics), Arc::new(reporter))
    }

    /// METRICS-DASHBOARD-SPEC.md §8: write-once at boot (`Primary::spawn`/
    /// `Worker::spawn`, both always know `parameters.protocol`).
    pub fn set_protocol_info(&self, protocol: &str) {
        self.protocol_info.with_label_values(&[protocol]).set(1);
    }

    /// Make a task panic visible instead of silent: publish it to `process_panics` and
    /// log it at `error` with payload, location, thread and backtrace.
    ///
    /// Installed once per process (`Once`), chaining to whatever hook was in place so
    /// the default "thread panicked at ..." message is not lost. Both `Primary::spawn`
    /// and `Worker::spawn` call this, and whichever runs first owns the gauge -- see
    /// `process_panics`' doc for why that is the right trade rather than a limitation.
    ///
    /// Why this exists at all: nothing in this codebase awaits a `JoinHandle`, so tokio
    /// has nowhere to report a panicking task and the panic is dropped on the floor. A
    /// subsystem can die while the process stays up, the metrics server keeps serving,
    /// and every unrelated counter keeps advancing -- indistinguishable from a healthy
    /// node under load until you diff two scrapes and notice one delta is exactly zero.
    /// That cost most of a day on 2026-08-08.
    pub fn install_panic_hook(metrics: Arc<Self>) {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(move || {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let count = PROCESS_PANICS.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                metrics.process_panics.set(count as i64);
                // `PanicHookInfo::payload` is `&dyn Any`; the two shapes `panic!` ever
                // produces are `&str` (literal) and `String` (formatted).
                let payload = info
                    .payload()
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| info.payload().downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "<non-string panic payload>".to_string());
                let location = info
                    .location()
                    .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
                    .unwrap_or_else(|| "<unknown location>".to_string());
                log::error!(
                    "PANIC #{} in thread {:?} at {}: {}\n{}",
                    count,
                    std::thread::current().name().unwrap_or("<unnamed>"),
                    location,
                    payload,
                    std::backtrace::Backtrace::force_capture(),
                );
                previous(info);
            }));
        });
    }

    /// Process-wide panic count, for callers that want the number without owning the
    /// registry the hook happened to bind to (see `install_panic_hook`).
    pub fn process_panic_count() -> u64 {
        PROCESS_PANICS.load(AtomicOrdering::Relaxed)
    }

    /// METRICS-DASHBOARD-SPEC.md §8: write-once, where the caller knows the client's
    /// tx-generation mode. `node local-benchmark` (the in-process vehicle) has it in
    /// scope directly. The standalone `node run primary`/`node run worker` path (what
    /// `fab remote` and docker-bench exec) has no direct view of the separate
    /// `benchmark_client` process's `--mode`, so it now reads the harness-supplied
    /// `Parameters::tx_mode` instead -- set by the generators (docker-bench `gen.py`),
    /// unset (`None`) for library/production callers, in which case this is not
    /// called and the gauge family stays absent (not a misleading zero).
    pub fn set_transaction_mode_info(&self, mode: &str) {
        self.transaction_mode_info.with_label_values(&[mode]).set(1);
    }

    /// Write-once at boot (same discipline as `set_protocol_info` above): open the
    /// metrics-active window at an absolute epoch-millisecond instant. `None` leaves
    /// the gate disabled, which is byte-identical to the behaviour before
    /// `active_from_millis` existed.
    pub fn set_active_from_millis(&self, at_millis: Option<u64>) {
        if let Some(at) = at_millis {
            self.active_from_millis
                .store(at, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// True iff a transaction submitted at `submitted_millis` falls inside the
    /// metrics-active window. Cheap and allocation-free -- called once per committed
    /// transaction on the hot path (240k+ tx/s), hence `Relaxed`: the value is
    /// written once at boot and never changes, so no ordering is needed.
    pub fn counts_toward_metrics(&self, submitted_millis: u64) -> bool {
        let from = self
            .active_from_millis
            .load(std::sync::atomic::Ordering::Relaxed);
        from == 0 || submitted_millis >= from
    }
}

impl MetricReporter {
    /// Spawn the periodic reporter task on the caller's (already-running) tokio runtime.
    pub fn start(self: Arc<Self>) {
        tokio::spawn(self.run());
    }

    async fn run(self: Arc<Self>) {
        const REPORT_INTERVAL: Duration = Duration::from_secs(10);
        let mut deadline = Instant::now();
        loop {
            deadline += REPORT_INTERVAL;
            tokio::time::sleep_until(deadline).await;
            self.force_report();
        }
    }

    /// Drain the histogram and publish its current gauges immediately, instead of
    /// waiting for the next periodic tick (up to `REPORT_INTERVAL` stale). Used by
    /// `local-benchmark` (PHASE2-SPEC.md §8), which reads the registry in-process at
    /// the exact end of the run and would otherwise miss up to 10s of the tail.
    ///
    /// Cumulative over the whole run (warm-up included), starfish-style: draining
    /// without clearing means reported quantiles reflect every observation so far,
    /// not just the last window (PHASE2-SPEC.md #5's semantics note).
    pub fn force_report(&self) {
        let mut latency = self.transaction_committed_latency.lock().unwrap();
        latency.receive_all();
        latency.report();

        let mut materialised_latency = self.transaction_materialised_latency.lock().unwrap();
        materialised_latency.receive_all();
        materialised_latency.report();

        let mut block_size = self.proposed_block_size_bytes.lock().unwrap();
        block_size.receive_all();
        block_size.report();

        let mut header_size = self.proposed_header_size_bytes.lock().unwrap();
        header_size.receive_all();
        header_size.report();

        let mut tx_size = self.proposed_transaction_size_bytes.lock().unwrap();
        tx_size.receive_all();
        tx_size.report();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_seconds(registry: &Registry) -> Option<f64> {
        registry
            .gather()
            .into_iter()
            .find(|f| f.get_name() == "metrics_active_seconds")
            .map(|f| f.get_metric()[0].get_gauge().get_value())
    }

    #[test]
    fn active_seconds_is_zero_until_the_window_is_armed() {
        // Ungated runs must expose 0, which the harness reads as "no node-side clock"
        // and falls back to its own wall-clock window -- identical to the behaviour
        // before this series existed.
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        assert_eq!(active_seconds(&registry), Some(0.0));
        metrics.set_active_from_millis(None);
        assert_eq!(active_seconds(&registry), Some(0.0));
    }

    #[test]
    fn active_seconds_is_computed_at_scrape_time_not_on_a_tick() {
        // The whole point of the Collector: no MetricReporter tick runs in this test, so
        // a periodically-published gauge would stay at 0. Arming the window 5s in the
        // past must therefore read ~5s on the very next gather.
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        metrics.set_active_from_millis(Some(now - 5_000));
        let seconds = active_seconds(&registry).expect("series is registered");
        assert!(
            (4.5..6.0).contains(&seconds),
            "expected ~5s of open window, got {seconds}"
        );
        // A window that opens in the FUTURE is still closed, hence 0 -- never negative,
        // which would poison a rate denominator.
        metrics.set_active_from_millis(Some(now + 60_000));
        assert_eq!(active_seconds(&registry), Some(0.0));
    }

    #[test]
    fn metrics_active_window_is_disabled_by_default() {
        // Every parameters file predating `metrics_active_at_ms` must behave exactly
        // as before: no gate, every observation counts, however old its timestamp.
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        assert!(metrics.counts_toward_metrics(0));
        assert!(metrics.counts_toward_metrics(1_770_000_000_000));
        metrics.set_active_from_millis(None);
        assert!(metrics.counts_toward_metrics(0));
    }

    #[test]
    fn metrics_active_window_excludes_transactions_submitted_before_it_opens() {
        // The gate keys off the transaction's own SUBMISSION instant, not its commit
        // instant: the startup transient is precisely the population that was
        // submitted while the committee was still forming, and a transaction
        // submitted then still commits (late) inside the active window.
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        let window_open = 1_770_000_012_500;
        metrics.set_active_from_millis(Some(window_open));

        assert!(!metrics.counts_toward_metrics(window_open - 1));
        assert!(!metrics.counts_toward_metrics(0));
        assert!(metrics.counts_toward_metrics(window_open));
        assert!(metrics.counts_toward_metrics(window_open + 1));
    }

    #[test]
    fn histogram_reporter_exports_p95() {
        let registry = Registry::new();
        let (histogram, sender) = histogram();
        let mut reporter = HistogramReporter::new_in_registry(histogram, &registry, "latency");

        for value in 1..=100 {
            sender.observe(value);
        }
        reporter.receive_all();
        reporter.report();

        let p95 = registry
            .gather()
            .into_iter()
            .find(|family| family.get_name() == "latency")
            .and_then(|family| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.get_name() == "v" && label.get_value() == "p95")
                    })
                    .map(|metric| metric.get_gauge().get_value() as usize)
            });

        assert_eq!(p95, Some(96));
    }

    /// The hook counts a panic and publishes it, and installing it twice is a no-op.
    ///
    /// The point of the whole mechanism is that a panicking tokio task is otherwise
    /// invisible (nothing awaits a `JoinHandle`), so "the hook is wired" has to be an
    /// assertion rather than an assumption -- a `Once` used wrongly fails silently and
    /// would leave the metric at a permanently reassuring 0.
    ///
    /// `catch_unwind` keeps the panic from failing the test; the chained default hook
    /// writes to stderr, which cargo captures and shows only on failure. The
    /// `Backtrace::force_capture()` in the hook is inside `log::error!`'s argument list
    /// and no logger is installed in tests, so the macro's level check short-circuits
    /// before evaluating it -- no backtrace is captured or printed here.
    #[test]
    fn panic_hook_counts_and_publishes() {
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        Metrics::install_panic_hook(metrics.clone());
        // Second call must not chain a second hook (which would double-count).
        Metrics::install_panic_hook(metrics.clone());

        let before = Metrics::process_panic_count();
        let caught = std::panic::catch_unwind(|| panic!("deliberate: panic_hook test"));
        assert!(caught.is_err(), "the closure was supposed to panic");
        let after = Metrics::process_panic_count();

        // Exactly one, not two: proves the second `install_panic_hook` was inert. A
        // delta rather than an absolute, because the tally is process-wide and other
        // tests share this binary.
        assert_eq!(after - before, 1, "hook counted {} panics", after - before);
        // The gauge tracks the same global. `>=` because another test in this binary may
        // have panicked and bumped it between the two reads above.
        assert!(metrics.process_panics.get() >= 1);
    }
}
