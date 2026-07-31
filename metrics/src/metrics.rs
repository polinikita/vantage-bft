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
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};

use prometheus::{
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use tokio::time::Instant;

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
    /// Unsigned acks this node broadcast (N3).
    pub vantage_acks_sent: IntCounter,
    /// Unsigned acks this node counted, first-hand (N4).
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
    /// Symmetric pairwise-MAC authenticated channels (`Parameters::
    /// authenticate_channels`): inbound frames dropped because the trailing MAC tag
    /// didn't verify against the message's declared/positionally-derived sender (a
    /// forged/tampered/replayed-under-a-different-key frame), across every handler
    /// (Vantage primary, Autobahn worker-facing primary, worker<->worker,
    /// worker<->primary) this node runs. Always zero on the honest-only path and
    /// whenever the flag is off; a nonzero value means a Byzantine node attempted
    /// message impersonation on an authenticated channel.
    pub authenticated_channel_rejected_total: IntCounter,

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
    /// `VantageCore`'s own inbound-message channel depth, sampled the same way as
    /// the Finding-A progress gauges (once/sec, in `VantageCore::run`'s own select
    /// loop) -- `rx_vantage.len()` (a `tokio::sync::mpsc::Receiver` exposes this
    /// cheaply without contorting the channel type). `0` on the two Autobahn paths.
    pub core_queue_length: IntGauge,

    // --- METRICS-DASHBOARD-SPEC.md §8 addenda.
    /// Write-once at boot: which protocol this node is running (starfish pattern --
    /// `consensus_protocol_info`). Always exactly one label value set to `1`.
    pub protocol_info: IntGaugeVec,
    /// Write-once (where known -- see `set_transaction_mode_info`'s doc): which
    /// client transaction-payload mode this run uses.
    pub transaction_mode_info: IntGaugeVec,
    /// Starfish's own counter, reinstated (§1 had omitted it as N/A without
    /// compression): sum of pre-compression serialized sizes, incremented only when
    /// `compress_network` is on (mirrors starfish's own conditional exactly -- when
    /// compression is off this would just duplicate `bytes_sent_total`).
    pub bytes_uncompressed_sent_total: IntCounter,

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
/// max and the p25/p50/p75/p90/p99 quantiles -- exactly starfish's exposition shape, so
/// the harness parses it with the same plain regex starfish's own orchestrator uses.
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
        let Some([p25, p50, p75, p90, p99]) = self.histogram.pcts([250, 500, 750, 900, 990]) else {
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
            authenticated_channel_rejected_total: register_int_counter_with_registry!(
                "authenticated_channel_rejected_total",
                "Inbound frames dropped for failing symmetric-pairwise-MAC verification \
                 (authenticate_channels)",
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
            core_queue_length: register_int_gauge_with_registry!(
                "core_queue_length",
                "VantageCore's own inbound-message channel depth",
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
            bytes_uncompressed_sent_total: register_int_counter_with_registry!(
                "bytes_uncompressed_sent_total",
                "Sum of pre-compression serialized sizes (only incremented when compress_network is on)",
                registry,
            )
            .unwrap(),
            // Perf-audit addendum: defaults active (starfish parity for "preserves
            // current behaviour when nothing sets it") -- see this field's own doc
            // comment for what would need to set it false.
            metrics_active: Arc::new(AtomicBool::new(true)),
        };

        (Arc::new(metrics), Arc::new(reporter))
    }

    /// METRICS-DASHBOARD-SPEC.md §8: write-once at boot (`Primary::spawn`/
    /// `Worker::spawn`, both always know `parameters.protocol`).
    pub fn set_protocol_info(&self, protocol: &str) {
        self.protocol_info.with_label_values(&[protocol]).set(1);
    }

    /// METRICS-DASHBOARD-SPEC.md §8: write-once, where the caller knows the client's
    /// tx-generation mode. Only `node local-benchmark` (the in-process vehicle) has
    /// this in scope at registry-construction time -- the standalone `node run
    /// primary`/`node run worker` path (what `fab remote` execs) has no channel
    /// carrying the separate `benchmark_client` process's `--mode` into a primary/
    /// worker's own registry, so this simply isn't called there and the gauge family
    /// stays absent (not a misleading zero) on that path. Documented scope decision,
    /// METRICS-NOTES.md.
    pub fn set_transaction_mode_info(&self, mode: &str) {
        self.transaction_mode_info.with_label_values(&[mode]).set(1);
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
