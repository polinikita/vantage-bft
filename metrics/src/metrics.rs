// Transaction latency, protocol, queue, and utilization metrics.

use std::{
    ops::AddAssign,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
    time::Duration,
};

/// Process-wide panic tally owned by the singleton panic hook.
static PROCESS_PANICS: AtomicU64 = AtomicU64::new(0);

/// Queue sampling interval. The sampler runs at 10 Hz.
const QUEUE_SAMPLE_INTERVAL_MS: u64 = 100;

/// Number of samples between queue publications.
const QUEUE_PUBLISH_EVERY: u32 = 10;

/// Type-erased occupancy reader for one bounded channel. Occupancy counts held permits,
/// not queued messages.
pub struct QueueProbe {
    pub stage: &'static str,
    /// Returns `(depth, capacity)`.
    pub occupancy: Box<dyn Fn() -> (usize, usize) + Send + Sync>,
}

/// Store actor channel occupancy and liveness readers.
pub struct StoreProbe {
    /// Returns `(depth, capacity)` of the actor's command channel.
    pub occupancy: Box<dyn Fn() -> (usize, usize) + Send + Sync>,
    /// Epoch-ms stamp of the actor's last completed loop iteration.
    pub heartbeat_millis: Box<dyn Fn() -> u64 + Send + Sync>,
    /// Monotonic count of commands the actor has dequeued.
    pub commands_drained: Box<dyn Fn() -> u64 + Send + Sync>,
}

/// Publishes bounded-queue occupancy and store-actor liveness until process exit.
pub fn spawn_queue_sampler(probes: Vec<QueueProbe>, store: StoreProbe, metrics: Arc<Metrics>) {
    // Queue capacities are fixed after construction.
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
        // Publish only the new portion of the monotonic store counter.
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
            // Saturate future timestamps instead of allowing unsigned underflow.
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

/// Current wall-clock time in epoch milliseconds.
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
/// The metrics-active duration in seconds. The collector computes it at scrape time.
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
        // Zero means no active window or a window that has not opened.
        let seconds = if from == 0 || now <= from {
            0.0
        } else {
            (now - from) as f64 / 1000.0
        };
        self.gauge.set(seconds);
        self.gauge.collect()
    }
}

/// Drop guard that records elapsed section time in microseconds.
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
    /// Constructs a timer from an already-resolved counter.
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

/// Transaction latency and protocol metrics.
#[derive(Clone)]
pub struct Metrics {
    /// Channel-fed observation point for a single transaction's (commit time - embedded
    /// submission timestamp). Hot path: `observe` is a lock-free unbounded-channel push.
    pub transaction_committed_latency: HistogramSender<Duration>,
    /// Running sum of (latency in microseconds)^2 across all observed transactions --
    /// paired with the histogram's `sum`/`count` gauges this gives the harness an exact
    /// global stddev: `sqrt(squared_sum/count - (sum/count)^2)`.
    pub transaction_committed_latency_squared_micros: IntCounter,
    /// Submit-to-materialized latency. It ends when the worker has the batch locally.
    pub transaction_materialised_latency: HistogramSender<Duration>,
    /// Mirrors `transaction_committed_latency_squared_micros` exactly (same batched
    /// accumulate-then-`inc_by` treatment), for `transaction_materialised_latency`.
    pub transaction_materialised_latency_squared_micros: IntCounter,
    /// Total transactions whose latency was successfully observed.
    pub committed_transactions: IntCounter,
    /// Total bytes of transactions whose latency was successfully observed.
    pub committed_bytes: IntCounter,
    /// Commit-time batch lookups deferred for retry because the payload is absent.
    /// Each deferral increments this counter once.
    pub latency_misses: IntCounter,
    /// Deferred misses later resolved and counted. The difference from
    /// `latency_misses` is unresolved or pending work.
    pub latency_misses_resolved: IntCounter,

    // --- Vantage data-plane counters.
    /// Blocks this node published (self-authored, including our own).
    pub vantage_blocks_published: IntCounter,
    /// Own blocks that reached committed output.
    pub vantage_own_blocks_committed_total: IntCounter,
    /// Own proposals broadcast.
    pub vantage_own_proposals_made_total: IntCounter,
    /// Proposer turns that reached a terminal outcome locally.
    pub vantage_own_proposer_turns_total: IntCounter,
    /// Proposer turns that sealed with a non-Skip outcome.
    pub vantage_own_proposals_committed_total: IntCounter,
    /// Committed blocks by author, as observed by this node. One series per committee
    /// member.
    pub vantage_committed_by_author: IntCounterVec,
    /// Set to 1 after state sync completes recovery.
    pub vantage_sequence_sync_recovered: IntGauge,
    /// Payload entries in own committed blocks.
    pub vantage_own_payload_committed_total: IntCounter,
    /// Blocks this node received (direct publish or relayed) and cached.
    pub vantage_blocks_received: IntCounter,
    /// Direct-publish confirmations produced by this node. This is not a wire-send
    /// count; `vantage_avail_sent` counts watermark broadcasts.
    pub vantage_acks_sent: IntCounter,
    /// Wire acknowledgments counted first-hand. Watermark credits are counted by
    /// `vantage_avail_credited_refs`.
    pub vantage_acks_received: IntCounter,
    /// `request(h)` messages this node sent (N6/D2).
    pub vantage_repairs_requested: IntCounter,
    /// `serve(h, b)` messages this node sent (N7).
    pub vantage_repairs_served: IntCounter,
    /// Cumulative bincode-encoded size of every block this node has retained (N8).
    pub vantage_retained_bytes: IntCounter,
    /// Inbound messages dropped because the declared sender is not a committee member.
    pub vantage_rejected_nonmember_total: IntCounter,
    /// Re-requestable recovery messages dropped because the bulk inbound queue was full.
    pub vantage_bulk_inbound_dropped_total: IntCounter,
    /// Per-lane availability watermark broadcasts sent by this node.
    pub vantage_avail_sent: IntCounter,
    /// `VantageAvail` messages received and routed to `LaneManager::resolve_watermark`.
    pub vantage_avail_received: IntCounter,
    /// Block references credited through the availability-watermark path.
    pub vantage_avail_credited_refs: IntCounter,
    /// Unexpected effect variants received by the Simple-IT effect loop.
    pub simpleit_unexpected_effect_total: IntCounter,
    /// Committed Simple-IT rounds waiting for complete local block-chain verification.
    pub simpleit_commit_queue_len: IntGauge,

    // --- Per-view seal-route breakdown.
    /// View seal route, labeled by `route`, counted at first acceptance.
    pub vantage_seals: IntCounterVec,

    // --- Grounded post-ready skip.
    /// `SKIP-VOTE(u)` statements broadcast by this node.
    pub vantage_skip_votes_sent: IntCounter,
    /// `SKIP-VOTE(u)` statements counted first-hand from peers.
    pub vantage_skip_votes_received: IntCounter,

    // --- Digest-named AGB statements.
    /// `VantageBodyFetch` messages this node sent (one per target statement author,
    /// per outstanding (view, digest) pair, per retry attempt).
    pub vantage_body_fetches_sent: IntCounter,
    /// `VantageBodyServe` messages this node sent, answering a peer's
    /// `VantageBodyFetch` with its own held, fixed `ViewProposal`.
    pub vantage_bodies_served: IntCounter,

    // --- Lane resume.
    /// `VantageLaneResume` requests sent by this node.
    pub vantage_lane_resume_requests_sent: IntCounter,
    /// Own blocks served in response to `VantageLaneResume`.
    pub vantage_lane_resume_blocks_served: IntCounter,
    /// Resume messages rejected by the dedicated resume-sender queue.
    pub vantage_lane_resume_send_drops: IntCounter,

    // --- Reconnect replay.
    /// `Wire::enqueue_replay`'s `try_send` onto the resume-sender task's channel
    /// failed (full or closed). `pending_low` is left
    /// untouched on this path, recovered by the next nudge/tick re-ask.
    pub vantage_replay_enqueue_drops_total: IntCounter,
    /// `VantageReplayDone` sends where `outbox_floor` truncated the requested span.
    pub vantage_replay_done_clamped_total: IntCounter,
    /// Server-side nudge Hellos sent for pending replay data.
    pub vantage_replay_pending_low_nudges_total: IntCounter,
    /// In-flight replay streams evicted after their TTL expired.
    pub vantage_replay_inflight_ttl_expired_total: IntCounter,

    // --- Progress gauges.
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
    /// Entries dropped because an author's lane contradicted delivered output.
    pub vantage_cursor_forked_entries_dropped: IntGauge,
    /// `ControlLog::curr_round` -- the resolution pipeline's current Simple-IT round.
    pub vantage_control_round: IntGauge,
    /// `ControlLog::delivered_log.len()` -- total (view, digest) pairs RB-delivered into
    /// the control log so far.
    pub vantage_control_delivered_len: IntGauge,
    /// `ControlLog::consume_pos` -- how far `pump_log` has consumed that delivered log
    /// (a persistent gap between this and `delivered_len` means every subsequent anchor
    /// is blocked on a still-missing `B_w`, not on delivery itself).
    pub vantage_control_consume_pos: IntGauge,
    /// Active and fixed views waiting for an echo.
    pub vantage_pending_gate_len: IntGauge,
    /// Outstanding AGB body fetches. Bounded by `agb::MAX_PENDING_FETCH`.
    pub vantage_pending_body_fetch_len: IntGauge,
    /// Body-fetch pairs dropped when the pending-fetch limit was reached.
    pub vantage_body_fetch_evicted_total: IntCounter,
    /// Authorized but unsettled repair references.
    pub vantage_pending_settle_len: IntGauge,
    /// Total `Repairer::settle` calls.
    pub vantage_repair_settle_calls_total: IntCounter,
    /// Times `settle` entered its missing-block branch.
    pub vantage_repair_fanout_loops_total: IntCounter,
    /// Adaptive per-tick repair-request ceiling. It decreases on bulk-inbound drops and
    /// increases on clean ticks.
    pub vantage_repair_emit_ceiling: IntGauge,
    /// Repair-ceiling halvings caused by core-queue pressure.
    pub vantage_repair_ceiling_halved_by_queue: IntCounter,
    /// Repair-ceiling halvings caused by new bulk-inbound drops.
    pub vantage_repair_ceiling_halved_by_drops: IntCounter,
    /// Ticks on which `adapt_recovery_ceiling` increased or held the ceiling.
    pub vantage_repair_ceiling_raised: IntCounter,
    /// In-flight repair slots reclaimed after an unanswered request round timed out.
    pub vantage_repair_asks_reclaimed_total: IntCounter,
    /// Availability credits skipped because the reference already reached quorum.
    pub vantage_avail_credit_skipped_total: IntCounter,
    /// Repair requests outstanding. Capped by `RECOVERY_IN_FLIGHT_MAX`.
    pub vantage_repair_in_flight: IntGauge,
    /// Cached blocks evicted after every peer confirmed holding them.
    pub vantage_block_cache_evicted_total: IntCounter,
    /// Lanes pinned because a peer has not reported an eviction floor.
    pub vantage_block_cache_evict_blocked: IntGauge,
    /// Repair requests deferred because the per-tick recovery allowance was exhausted.
    pub vantage_repair_budget_deferred_total: IntCounter,
    /// Number of entries held by `BlockCache`.
    pub vantage_block_cache_len: IntGauge,
    /// References whose first-hand acknowledgment set is below quorum.
    pub vantage_ack_senders_tracked: IntGauge,
    /// References retired after reaching an acknowledgment threshold.
    pub vantage_ack_refs_retired: IntGauge,
    /// Digests with an active but incomplete repair fan-out.
    pub vantage_repair_fanout_pending: IntGauge,
    /// Fan-out rounds beyond the first.
    pub vantage_repair_fanout_escalations_total: IntCounter,

    // --- Wire-layer counters.
    /// Total bytes written across outbound connections, including length prefixes.
    pub bytes_sent_total: IntCounter,
    /// Total bytes read across inbound connections, including length prefixes.
    pub bytes_received_total: IntCounter,
    /// Wire messages sent by type, counted per physical unicast transmission.
    pub network_messages_sent_total: IntCounterVec,
    /// Wire messages received, by `type`, counted at receiver dispatch post-deserialize.
    pub network_messages_received_total: IntCounterVec,
    /// Serialized (pre-frame-prefix) bytes sent, by `type`.
    pub network_bytes_sent_total: IntCounterVec,
    /// Serialized (pre-frame-prefix) bytes received, by `type`.
    pub network_bytes_received_total: IntCounterVec,
    /// Physical wire frames sent across outbound connections.
    pub network_frames_sent_total: IntCounter,
    /// Volatile sends shed after the destination queue reaches its soft cap.
    pub network_volatile_shed_total: IntCounter,
    /// Currently-open inbound TCP connections, labeled by listener role. The
    /// Prometheus target already separates primary and worker processes; this label
    /// separates the listeners inside a process.
    pub network_connections: IntGaugeVec,
    // --- Sequence-chain metrics.
    /// Highest view covered by the local sequence chain.
    pub vantage_sequence_head_view: IntGauge,
    /// Highest checkpoint boundary this node has passed.
    pub vantage_sequence_boundary_view: IntGauge,
    /// Current checkpoint head, labeled by session and head digest.
    pub vantage_sequence_boundary_head: IntGaugeVec,
    /// Sequence records committed to the local chain.
    pub vantage_sequence_records_total: IntCounter,
    /// Block digests folded into per-view output deltas.
    pub vantage_sequence_delta_digests_total: IntCounter,
    /// Records refused because the cursor finalized a view out of order.
    pub vantage_sequence_record_rejected_total: IntCounter,

    // --- Announcement counters.
    /// Announcements this node broadcast.
    pub vantage_sequence_announced_total: IntCounter,
    /// First-hand announcements that counted toward some (view, head).
    pub vantage_sequence_announce_counted_total: IntCounter,
    /// Announcements refused for sender, duplication, equivocation, or range errors.
    pub vantage_sequence_announce_ignored_total: IntCounter,
    /// Targets that reached f+1 matching first-hand announcements.
    pub vantage_sequence_certified_total: IntCounter,
    /// Highest certified checkpoint view.
    pub vantage_sequence_certified_view: IntGauge,
    /// Distinct senders caught announcing two different heads for one view.
    pub vantage_sequence_equivocators: IntGauge,
    /// Transfers whose targets were downloaded and verified against the certified head.
    pub vantage_sequence_sync_verified_total: IntCounter,
    /// Highest target view fully verified.
    pub vantage_sequence_sync_verified_view: IntGauge,
    /// Transfers abandoned because all matching announcers were dropped.
    pub vantage_sequence_sync_exhausted_total: IntCounter,
    pub vantage_sequence_sync_started_total: IntCounter,
    /// Target view of the active transfer.
    pub vantage_sequence_sync_target_view: IntGauge,
    /// Requests that hit the deadline and failed over to another source.
    pub vantage_sequence_sync_timeouts_total: IntCounter,
    /// Response chunks accepted.
    pub vantage_sequence_sync_chunks_total: IntCounter,
    /// Response chunks that failed verification.
    pub vantage_sequence_sync_invalid_total: IntCounter,
    /// State-sync frames dropped because the dedicated egress was full.
    pub vantage_sequence_sync_dropped_total: IntCounter,
    /// State-sync frames dropped because the dedicated inbound queue was full.
    pub vantage_sequence_sync_inbound_dropped_total: IntCounter,
    /// State-sync frames served to peers.
    pub vantage_sequence_sync_served_total: IntCounter,
    /// Responses received without an active transfer.
    pub vantage_sequence_sync_unsolicited_total: IntCounter,
    /// Verified targets whose head matched local execution at the same view.
    pub vantage_sequence_verify_match_total: IntCounter,
    /// Installed targets whose head matched the local self-check.
    pub vantage_sequence_install_selfcheck_match_total: IntCounter,
    /// Verified targets whose head disagreed with local execution. Must remain zero.
    pub vantage_sequence_verify_mismatch_total: IntCounter,

    // --- Installation staging.
    /// Verified targets accepted for staging.
    pub vantage_sequence_install_staged_total: IntCounter,
    /// Verified targets refused because output was not contiguous above the local head.
    pub vantage_sequence_install_rejected_total: IntCounter,
    /// Views in the target being staged.
    pub vantage_sequence_install_views: IntGauge,
    /// Staged views whose whole verified delta is now in the local block cache.
    pub vantage_sequence_install_views_ready: IntGauge,
    /// Staged views in the fetch window that are waiting on blocks.
    pub vantage_sequence_install_views_in_flight: IntGauge,
    /// Verified output digests whose headers or payloads are not yet deliverable.
    pub vantage_sequence_install_blocks_awaited: IntGauge,
    /// Header digests currently requested from checkpoint sources.
    pub vantage_sequence_install_header_requests_in_flight: IntGauge,
    /// Header digest requests sent, including timeout retries.
    pub vantage_sequence_install_headers_requested_total: IntCounter,
    /// Requested state-sync headers accepted after `BlockOK` validation.
    pub vantage_sequence_install_headers_received_total: IntCounter,
    /// Targets whose views are locally held and ready to install.
    pub vantage_sequence_install_ready_total: IntCounter,
    /// Views applied to the cursor from verified checkpoint state.
    pub vantage_sequence_install_views_applied_total: IntCounter,
    /// Installs refused by `Cursor::install`; the cursor remains unchanged.
    pub vantage_sequence_install_failed_total: IntCounter,
    /// Install passes that exhausted the digest budget while a view remained open.
    pub vantage_sequence_install_partial_views_total: IntCounter,
    /// Targets applied in full.
    pub vantage_sequence_install_completed_total: IntCounter,
    /// Highest view installed from verified checkpoint state.
    pub vantage_sequence_install_completed_view: IntGauge,
    /// Consensus, control, and service messages discarded while sequence recovery is active.
    pub vantage_sequence_install_obsolete_inbound_dropped_total: IntCounter,

    /// `SimpleSender` frames discarded while waiting for a connection.
    pub network_connect_wait_discarded_total: IntCounter,

    // --- Goodput and pipeline counters (worker ingress).
    /// Transactions received by the worker before batching.
    pub submitted_transactions: IntCounter,
    /// Bytes of transactions the worker's `BatchMaker` received from a client.
    pub submitted_transactions_bytes: IntCounter,

    // --- Consensus quality and utilization.
    /// Vantage block serialized size at publish time (self-authored blocks only),
    /// reported via the same `HistogramReporter` pattern as
    /// `transaction_committed_latency`.
    pub proposed_block_size_bytes: HistogramSender<usize>,
    /// Serialized metadata size of self-authored proposals, excluding payloads.
    pub proposed_header_size_bytes: HistogramSender<usize>,
    /// Serialized size of transactions in self-authored worker batches.
    pub proposed_transaction_size_bytes: HistogramSender<usize>,
    /// Accumulated section time in microseconds, labeled by `proc`.
    pub utilization_timer: IntCounterVec,
    /// Time spent waiting in instrumented sections, in microseconds. This overlaps
    /// `utilization_timer`; it is not a partition. Uninstrumented waits remain in the
    /// utilization total.
    pub core_wait_timer: IntCounterVec,
    /// `VantageCore` inbound-message channel depth, sampled once per second.
    pub core_queue_length: IntGauge,
    /// Peak inbound-message channel depth since the previous publication.
    pub core_queue_peak: IntGauge,

    // --- Worker-process observability.
    /// Current occupancy of each bounded worker channel. Labels identify the pipeline
    /// stage: `synchronizer`, `batch_maker`, `processor_own`, `processor_peer`,
    /// `helper`, `primary_connector`, and `store`.
    pub worker_queue_depth: IntGaugeVec,
    /// Peak queue occupancy since the previous publication.
    pub worker_queue_peak: IntGaugeVec,
    /// Capacity of each labeled worker channel.
    pub worker_queue_capacity: IntGaugeVec,
    /// Milliseconds since the store actor completed a `select!` iteration.
    pub store_actor_heartbeat_age_ms: IntGauge,
    /// Peak store actor heartbeat age since the previous publication.
    pub store_actor_heartbeat_age_ms_peak: IntGauge,
    /// Commands dequeued by the store actor.
    pub store_commands_drained_total: IntCounter,
    /// Headers with incomplete payloads.
    pub vantage_pending_payload_headers: IntGauge,
    /// Outstanding `(digest, worker_id)` keys across pending headers.
    pub vantage_pending_payload_keys: IntGauge,
    /// Size of `PayloadIo::last_synchronize`.
    pub vantage_last_synchronize_len: IntGauge,
    /// Nodes visited by prefix walks, labeled by `family`.
    pub vantage_walk_steps_total: IntCounterVec,
    /// Failed prefix walks by family and failure branch.
    pub vantage_walk_failures_total: IntCounterVec,
    /// Fresh repair campaigns after a full-coverage campaign went unanswered.
    pub vantage_repair_refetch_campaigns_total: IntCounter,
    /// Body-fetch pairs abandoned after `MAX_FETCH_ATTEMPTS`.
    pub vantage_body_fetch_abandoned_total: IntCounter,
    /// Panics observed by this process's panic hook.
    pub process_panics: IntGauge,

    // --- Protocol and workload labels.
    /// Protocol label written once at boot. One label value is set to `1`.
    pub protocol_info: IntGaugeVec,
    /// Client transaction-payload mode label, when known.
    pub transaction_mode_info: IntGaugeVec,

    // --- Metrics-active window.
    /// Whether commit-time observations contribute to rate and latency metrics.
    /// The value is shared by all clones.
    pub metrics_active: Arc<AtomicBool>,
    /// Epoch-millisecond start of the metrics-active window. Zero disables the gate.
    /// Set from `config::Parameters::metrics_active_at_ms`.
    pub active_from_millis: Arc<AtomicU64>,
}

/// Owns latency histogram receivers and publishes labeled gauges.
pub struct MetricReporter {
    transaction_committed_latency: Mutex<HistogramReporter<Duration>>,
    transaction_materialised_latency: Mutex<HistogramReporter<Duration>>,
    proposed_block_size_bytes: Mutex<HistogramReporter<usize>>,
    proposed_header_size_bytes: Mutex<HistogramReporter<usize>>,
    proposed_transaction_size_bytes: Mutex<HistogramReporter<usize>>,
}

/// Publishes exact histogram count, sum, maximum, and percentiles as gauges.
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
    /// first observation arrives, so an idle `Metrics` (for example, a primary that
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
    /// Registers these metrics into `registry` and returns the (sender-side,
    /// reporter-side) pair. Both primary and worker call this on their own registry;
    /// the worker's copy is observed by the reporter.
    pub fn new(registry: &Registry) -> (Arc<Self>, Arc<MetricReporter>) {
        // Share the active-window clock with the scrape-time collector.
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
            vantage_own_blocks_committed_total: register_int_counter_with_registry!(
                "vantage_own_blocks_committed_total",
                "Blocks authored by this node that reached committed output",
                registry,
            )
            .unwrap(),
            vantage_own_proposals_made_total: register_int_counter_with_registry!(
                "vantage_own_proposals_made_total",
                "Own consensus proposals broadcast",
                registry,
            )
            .unwrap(),
            vantage_own_proposer_turns_total: register_int_counter_with_registry!(
                "vantage_own_proposer_turns_total",
                "Views where this node was proposer and the view reached a terminal outcome",
                registry,
            )
            .unwrap(),
            vantage_own_proposals_committed_total: register_int_counter_with_registry!(
                "vantage_own_proposals_committed_total",
                "Own proposer turns that sealed with a committed outcome rather than Skip",
                registry,
            )
            .unwrap(),
            vantage_committed_by_author: register_int_counter_vec_with_registry!(
                "vantage_committed_by_author",
                "Committed blocks by authoring node, as observed by this node",
                &["author"],
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_recovered: register_int_gauge_with_registry!(
                "vantage_sequence_sync_recovered",
                "1 once state sync has latched off after recovery",
                registry,
            )
            .unwrap(),
            vantage_own_payload_committed_total: register_int_counter_with_registry!(
                "vantage_own_payload_committed_total",
                "Payload entries in blocks authored by this node that reached committed output",
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
            vantage_cursor_forked_entries_dropped: register_int_gauge_with_registry!(
                "vantage_cursor_forked_entries_dropped",
                "Cursor: manifest entries dropped because their ancestry contradicts \
                 delivered output (a forked lane -- restart that lost its frontier, or \
                 a Byzantine author)",
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
            vantage_sequence_head_view: register_int_gauge_with_registry!(
                "vantage_sequence_head_view",
                "Highest view covered by the local sequence chain",
                registry,
            )
            .unwrap(),
            vantage_sequence_boundary_view: register_int_gauge_with_registry!(
                "vantage_sequence_boundary_view",
                "Highest checkpoint boundary this node has passed",
                registry,
            )
            .unwrap(),
            vantage_sequence_boundary_head: register_int_gauge_vec_with_registry!(
                "vantage_sequence_boundary_head",
                "Checkpoint boundary head (hex label), valued by its boundary view",
                &["sid", "head"],
                registry,
            )
            .unwrap(),
            vantage_sequence_announced_total: register_int_counter_with_registry!(
                "vantage_sequence_announced_total",
                "Checkpoint announcements broadcast by this node",
                registry,
            )
            .unwrap(),
            vantage_sequence_announce_counted_total: register_int_counter_with_registry!(
                "vantage_sequence_announce_counted_total",
                "First-hand checkpoint announcements that counted",
                registry,
            )
            .unwrap(),
            vantage_sequence_announce_ignored_total: register_int_counter_with_registry!(
                "vantage_sequence_announce_ignored_total",
                "Checkpoint announcements refused",
                registry,
            )
            .unwrap(),
            vantage_sequence_certified_total: register_int_counter_with_registry!(
                "vantage_sequence_certified_total",
                "Checkpoint targets that reached f+1 matching announcements",
                registry,
            )
            .unwrap(),
            vantage_sequence_certified_view: register_int_gauge_with_registry!(
                "vantage_sequence_certified_view",
                "Highest certified checkpoint view",
                registry,
            )
            .unwrap(),
            vantage_sequence_equivocators: register_int_gauge_with_registry!(
                "vantage_sequence_equivocators",
                "Distinct senders caught announcing two heads for one view",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_verified_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_verified_total",
                "Transfers fully downloaded and verified",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_verified_view: register_int_gauge_with_registry!(
                "vantage_sequence_sync_verified_view",
                "Highest fully verified target view",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_exhausted_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_exhausted_total",
                "Transfers abandoned with no usable source left",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_started_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_started_total",
                "State-sync transfers started",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_target_view: register_int_gauge_with_registry!(
                "vantage_sequence_sync_target_view",
                "Target view of the active transfer",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_timeouts_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_timeouts_total",
                "Requests that timed out and failed over",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_chunks_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_chunks_total",
                "State-sync response chunks accepted",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_invalid_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_invalid_total",
                "State-sync response chunks that failed verification",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_dropped_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_dropped_total",
                "State-sync frames dropped because the bounded egress was full",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_inbound_dropped_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_inbound_dropped_total",
                "State-sync frames dropped because the dedicated inbound sequence queue was full",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_served_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_served_total",
                "State-sync frames served to peers",
                registry,
            )
            .unwrap(),
            vantage_sequence_sync_unsolicited_total: register_int_counter_with_registry!(
                "vantage_sequence_sync_unsolicited_total",
                "State-sync responses arriving with no active transfer",
                registry,
            )
            .unwrap(),
            vantage_sequence_verify_match_total: register_int_counter_with_registry!(
                "vantage_sequence_verify_match_total",
                "Verified checkpoint heads that matched local execution at the same view",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_selfcheck_match_total: register_int_counter_with_registry!(
                "vantage_sequence_install_selfcheck_match_total",
                "Head comparisons over an INSTALLED target -- self-consistency, not independent",
                registry,
            )
            .unwrap(),
            vantage_sequence_verify_mismatch_total: register_int_counter_with_registry!(
                "vantage_sequence_verify_mismatch_total",
                "Verified checkpoint heads that DISAGREED with local execution -- must stay 0",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_staged_total: register_int_counter_with_registry!(
                "vantage_sequence_install_staged_total",
                "Verified targets accepted for install staging",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_rejected_total: register_int_counter_with_registry!(
                "vantage_sequence_install_rejected_total",
                "Verified targets refused as non-contiguous above the local head",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_views: register_int_gauge_with_registry!(
                "vantage_sequence_install_views",
                "Views in the target being staged for install",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_views_ready: register_int_gauge_with_registry!(
                "vantage_sequence_install_views_ready",
                "Staged views whose whole verified delta is locally held",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_views_in_flight: register_int_gauge_with_registry!(
                "vantage_sequence_install_views_in_flight",
                "Staged views admitted to the fetch window and still awaiting blocks",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_blocks_awaited: register_int_gauge_with_registry!(
                "vantage_sequence_install_blocks_awaited",
                "Verified output digests in the staged install not yet deliverable",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_header_requests_in_flight:
                register_int_gauge_with_registry!(
                    "vantage_sequence_install_header_requests_in_flight",
                    "Verified-output headers currently requested from checkpoint sources",
                    registry,
                )
                .unwrap(),
            vantage_sequence_install_headers_requested_total:
                register_int_counter_with_registry!(
                    "vantage_sequence_install_headers_requested_total",
                    "Header digest asks sent by the batched sequence-install fast path",
                    registry,
                )
                .unwrap(),
            vantage_sequence_install_headers_received_total:
                register_int_counter_with_registry!(
                    "vantage_sequence_install_headers_received_total",
                    "Requested sequence-install headers accepted after validation",
                    registry,
                )
                .unwrap(),
            vantage_sequence_install_ready_total: register_int_counter_with_registry!(
                "vantage_sequence_install_ready_total",
                "Targets whose every view became locally held",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_views_applied_total: register_int_counter_with_registry!(
                "vantage_sequence_install_views_applied_total",
                "Views applied to the cursor from verified checkpoint state",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_failed_total: register_int_counter_with_registry!(
                "vantage_sequence_install_failed_total",
                "Installs refused by the cursor -- cursor unchanged, target abandoned",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_partial_views_total: register_int_counter_with_registry!(
                "vantage_sequence_install_partial_views_total",
                "Install passes that exhausted the digest budget mid-view",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_completed_total: register_int_counter_with_registry!(
                "vantage_sequence_install_completed_total",
                "Verified targets applied to the cursor in full",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_completed_view: register_int_gauge_with_registry!(
                "vantage_sequence_install_completed_view",
                "Highest view installed from verified checkpoint state",
                registry,
            )
            .unwrap(),
            vantage_sequence_install_obsolete_inbound_dropped_total:
                register_int_counter_with_registry!(
                    "vantage_sequence_install_obsolete_inbound_dropped_total",
                    "Consensus/control/service messages discarded while a sequence install makes them stale",
                    registry,
                )
                .unwrap(),
            vantage_sequence_records_total: register_int_counter_with_registry!(
                "vantage_sequence_records_total",
                "Sequence records committed to the local chain",
                registry,
            )
            .unwrap(),
            vantage_sequence_delta_digests_total: register_int_counter_with_registry!(
                "vantage_sequence_delta_digests_total",
                "Block digests folded into per-view output deltas",
                registry,
            )
            .unwrap(),
            vantage_sequence_record_rejected_total: register_int_counter_with_registry!(
                "vantage_sequence_record_rejected_total",
                "Sequence records refused because a view was finalized out of order",
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
            vantage_walk_failures_total: register_int_counter_vec_with_registry!(
                "vantage_walk_failures_total",
                "Failed prefix walks by family and failure branch",
                &["family", "branch"],
                registry,
            )
            .unwrap(),
            vantage_repair_refetch_campaigns_total: register_int_counter_with_registry!(
                "vantage_repair_refetch_campaigns_total",
                "Fresh repair campaigns for digests whose full coverage went unanswered",
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
            // Metrics are active until a start time is configured.
            metrics_active: Arc::new(AtomicBool::new(true)),
            active_from_millis,
        };

        (Arc::new(metrics), Arc::new(reporter))
    }

    /// Writes the protocol label once at boot (`Primary::spawn`/
    /// `Worker::spawn`, both always know `parameters.protocol`).
    pub fn set_protocol_info(&self, protocol: &str) {
        self.protocol_info.with_label_values(&[protocol]).set(1);
    }

    /// Installs a process-wide panic hook that records and logs task panics.
    pub fn install_panic_hook(metrics: Arc<Self>) {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(move || {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let count = PROCESS_PANICS.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                metrics.process_panics.set(count as i64);
                // Handle string and formatted panic payloads.
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

    /// Writes the workload label when the transaction mode is known.
    pub fn set_transaction_mode_info(&self, mode: &str) {
        self.transaction_mode_info.with_label_values(&[mode]).set(1);
    }

    /// Opens the metrics-active window at an epoch-millisecond instant. `None` leaves
    /// the gate disabled.
    pub fn set_active_from_millis(&self, at_millis: Option<u64>) {
        if let Some(at) = at_millis {
            self.active_from_millis
                .store(at, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Returns whether a transaction submitted at `submitted_millis` is in the active
    /// metrics window.
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

    /// Drains histogram receivers and publishes current gauges immediately.
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
        // An unconfigured window reports zero.
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        assert_eq!(active_seconds(&registry), Some(0.0));
        metrics.set_active_from_millis(None);
        assert_eq!(active_seconds(&registry), Some(0.0));
    }

    #[test]
    fn active_seconds_is_computed_at_scrape_time_not_on_a_tick() {
        // Scraping computes the duration without waiting for the reporter tick.
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
        // A future window is closed and reports zero.
        metrics.set_active_from_millis(Some(now + 60_000));
        assert_eq!(active_seconds(&registry), Some(0.0));
    }

    #[test]
    fn metrics_active_window_is_disabled_by_default() {
        // With no start time, every observation counts.
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        assert!(metrics.counts_toward_metrics(0));
        assert!(metrics.counts_toward_metrics(1_770_000_000_000));
        metrics.set_active_from_millis(None);
        assert!(metrics.counts_toward_metrics(0));
    }

    #[test]
    fn metrics_active_window_excludes_transactions_submitted_before_it_opens() {
        // The gate uses submission time rather than commit time.
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

    /// The hook records a panic and remains idempotent when installed twice.
    #[test]
    fn panic_hook_counts_and_publishes() {
        let registry = Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        Metrics::install_panic_hook(metrics.clone());
        // Installing twice must not chain a second hook.
        Metrics::install_panic_hook(metrics.clone());

        let before = Metrics::process_panic_count();
        let caught = std::panic::catch_unwind(|| panic!("deliberate: panic_hook test"));
        assert!(caught.is_err(), "the closure was supposed to panic");
        let after = Metrics::process_panic_count();

        // Compare a delta because the tally is process-wide.
        assert_eq!(after - before, 1, "hook counted {} panics", after - before);
        // The gauge mirrors the process-wide tally.
        assert!(metrics.process_panics.get() >= 1);
    }
}
