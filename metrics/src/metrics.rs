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

use std::{ops::AddAssign, sync::Arc, sync::Mutex, time::Duration};

use prometheus::{
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry,
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
    /// Total transactions whose latency was successfully observed.
    pub committed_transactions: IntCounter,
    /// Total bytes of transactions whose latency was successfully observed.
    pub committed_bytes: IntCounter,
    /// Commit-time batch lookups that missed the local store and were skipped (never
    /// blocked on) -- see PHASE2-SPEC.md #5's worker metrics task.
    pub latency_misses: IntCounter,

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
    pub vantage_rejected_nonmember_total: IntCounter,

    // --- Phase 6 (PHASE6-SPEC.md §9 gate amendment): per-view seal-route breakdown.
    /// How each view got sealed/ordered, one label `"route"`, incremented exactly once
    /// per view at the try-seal arbiter's FIRST-acceptance point (the submission that
    /// wins is the route -- later compatible submissions for the same view never
    /// count again). Routes: `fast_full` (all-n unanimous fast seal), `direct_full`
    /// (grade-1 ready quorum), `direct_core` (grade-0 ready quorum), `anchor_full`,
    /// `anchor_core`, `anchor_skip` (the apply-anchor adapter's three outcomes).
    /// Different nodes can legitimately show different route distributions for the
    /// same view (e.g. one node fast-seals while another only reaches the anchor).
    pub vantage_seals: IntCounterVec,

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
}

/// Owns the receiving half of the latency histogram and periodically drains + publishes
/// it as labeled gauges. `Metrics` (the sender-side handles) is `Clone`+`Send`+`Sync` and
/// can be shared freely; `MetricReporter` is not meant to be touched outside its own
/// background task other than via `start`.
pub struct MetricReporter {
    transaction_committed_latency: Mutex<HistogramReporter<Duration>>,
    proposed_block_size_bytes: Mutex<HistogramReporter<usize>>,
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
    pub fn new_in_registry(histogram: PreciseHistogram<T>, registry: &Registry, name: &str) -> Self {
        let gauge = register_int_gauge_vec_with_registry!(name, name, &["v"], registry).unwrap();
        Self { histogram, gauge }
    }

    /// Publish the current exact quantiles. A no-op (leaves the gauge unset) until the
    /// first observation arrives, so an idle `Metrics` (e.g. primary's in Phase 2, which
    /// registers this same shape but never observes into it) simply omits the metric
    /// from its scrape output rather than reporting a misleading zero.
    pub fn report(&mut self) {
        let Some([p25, p50, p75, p90, p99]) = self.histogram.pcts([250, 500, 750, 900, 990])
        else {
            return;
        };
        let Some(max) = self.histogram.max() else {
            return;
        };
        self.gauge.with_label_values(&["p25"]).set(p25.as_prometheus_metric());
        self.gauge.with_label_values(&["p50"]).set(p50.as_prometheus_metric());
        self.gauge.with_label_values(&["p75"]).set(p75.as_prometheus_metric());
        self.gauge.with_label_values(&["p90"]).set(p90.as_prometheus_metric());
        self.gauge.with_label_values(&["p99"]).set(p99.as_prometheus_metric());
        self.gauge.with_label_values(&["max"]).set(max.as_prometheus_metric());
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
        let (proposed_block_size_bytes_hist, proposed_block_size_bytes) = histogram();

        let reporter = MetricReporter {
            transaction_committed_latency: Mutex::new(HistogramReporter::new_in_registry(
                transaction_committed_latency_hist,
                registry,
                "transaction_committed_latency",
            )),
            proposed_block_size_bytes: Mutex::new(HistogramReporter::new_in_registry(
                proposed_block_size_bytes_hist,
                registry,
                "proposed_block_size_bytes",
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
                "Commit-time batch lookups that missed the local store and were skipped",
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
            vantage_seals: register_int_counter_vec_with_registry!(
                "vantage_seals",
                "Vantage views sealed, by route (fast_full/direct_full/direct_core/anchor_full/anchor_core/anchor_skip)",
                &["route"],
                registry,
            )
            .unwrap(),
            vantage_entered_view: register_int_gauge_with_registry!(
                "vantage_entered_view",
                "Pacemaker: largest view formally entered (W5)",
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

        let mut block_size = self.proposed_block_size_bytes.lock().unwrap();
        block_size.receive_all();
        block_size.report();
    }
}
