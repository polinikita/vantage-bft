// Mutable protocol state has one owner; the block cache is shared.

use crate::messages::{Ack, Header};
use crate::primary::{Height, PrimaryMessage, View, CHANNEL_CAPACITY};
use crate::vantage::agb::{
    AgbEngine, DigestStatements, EchoDigest, EchoOut, ProposalOut, ReadyDigest, ReadyOut,
    TimerKind, ViewProposal,
};
use crate::vantage::block::{self, BlockRef};
use crate::vantage::control::{ControlLog, ControlProposal, Round};

/// Maximum retained certified checkpoint candidates.
const SEQUENCE_CANDIDATE_WINDOWS: usize = 32;

/// Maximum checkpoint boundaries in one announcement.
const SEQUENCE_ANNOUNCE_BOUNDARIES: usize = 8;

/// Maximum views in one delta-range request.
const SEQUENCE_DELTA_RANGE_VIEWS: usize = 256;

/// Maximum digests in one sequence header request.
const SEQUENCE_BLOCK_REQUEST_BATCH: usize = 256;

/// Maximum headers in one sequence response.
const SEQUENCE_BLOCK_SERVE_BATCH: usize = 64;

/// Maximum unique sequence header requests in flight.
const SEQUENCE_BLOCK_MAX_IN_FLIGHT: usize = 2_048;

/// Request-window refill threshold.
const SEQUENCE_BLOCK_REFILL_AT: usize = SEQUENCE_BLOCK_MAX_IN_FLIGHT / 2;

/// Sequence installation period in milliseconds.
const SEQUENCE_INSTALL_DRIVE_PERIOD_MS: u64 = 100;

/// Views retained above the live intake floor for in-flight consensus evidence.
const SEQUENCE_LIVE_INTAKE_MARGIN: crate::primary::View = 16;

use crate::vantage::cursor::Cursor;
use crate::vantage::frontier::Frontier;
use crate::vantage::install::{RebaseOutcome, SequenceInstall};
use crate::vantage::lanes::{
    aggregate_received_ack, AckAggregator, AckAvailability, AvailEntry, BlockCache, LaneManager,
    SharedAckAggregator, SharedBlocks,
};
use crate::vantage::outbox::Outbox;
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::payload::{append_missing_payload_sync, block_was_cached, PayloadIo};
use crate::vantage::repair::Repairer;
use crate::vantage::resolve::Resolver;
use crate::vantage::resume::{
    in_flight_state, InFlightEntry, InFlightState, NudgeMemo, ReplayEpisodes, ResumeServe,
    ResumeTrigger, ServeBudget,
};
use crate::vantage::sequence::{
    genesis_head, head_hex, CheckpointCollector, SequenceAnnouncement, SequenceDeltaChunk,
    SequenceDeltaRangeChunk, SequenceDeltaRangeRequest, SequenceDeltaRequest, SequenceOutcome,
    SequenceOutcomeRequest, SequenceOutcomeServe, SequenceRecordChunk, SequenceRequest,
    SequenceStore, SequenceTransfer, SequenceUnavailable, SequenceWant, TransferState,
    SEQUENCE_VERSION,
};
use crate::vantage::wire::{self, Wire};

enum SequenceResponse {
    Records(SequenceRecordChunk),
    Outcome(SequenceOutcomeServe),
    Delta(SequenceDeltaChunk),
    DeltaRange(SequenceDeltaRangeChunk),
    Unavailable(SequenceUnavailable),
}

#[derive(Clone, Copy)]
struct SequenceBlockRequestState {
    requested_at: Instant,
    source_cursor: usize,
}
use crate::vantage::Effect;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use metrics::{Metrics, UtilizationTimer};
use network::{BatchConfig, DirtyMap, MessageHandler, ReliableSender, SimpleSender, Writer};
use parking_lot::Mutex;
use prometheus::IntCounter;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Capacity of the droppable service-request queue.
const BULK_CHANNEL_CAPACITY: usize = 2048;

/// Messages delivered to the Vantage core.
#[derive(Debug, Clone)]
pub enum Inbound {
    /// The key is copied from the header author because the frame has no sender field.
    Publish(PublicKey, Header),

    Serve(Header),
    HeadersRequest(Vec<Digest>, PublicKey),

    /// Aggregated availability derived from network acknowledgments.
    AckAvailability(AckAvailability),

    /// Direct acknowledgment input used by tests and local injection.
    Ack(Ack),

    /// The key is the declared sender and must be a committee member.
    Avail(Vec<AvailEntry>, PublicKey),

    /// The sender is derived from the proposal view because the frame has no sender field.
    Propose(ProposalOut),

    Echo(EchoOut),

    /// The final view is the sender's wish watermark.
    EchoSkip(View, PublicKey, View),

    Ready(ReadyOut),

    /// The final view is the sender's wish watermark.
    NoReady(View, PublicKey, View),

    Wish(View, PublicKey),

    CompReport(View, Digest, PublicKey),

    /// The sender is derived from the control round because the frame has no sender field.
    ControlInit(ControlProposal, Option<ProposalOut>),
    ControlEcho(PublicKey, ControlProposal),
    ControlReady(PublicKey, ControlProposal),
    ControlCommit(PublicKey, Round),
    ControlTimeoutVote(PublicKey, Round),
    ControlTimeoutAccept(PublicKey, Round),
    ControlFetch(View, Digest, PublicKey),

    ControlServe(View, ProposalOut),

    /// Skip votes do not carry a wish watermark.
    SkipVote(View, PublicKey),

    /// Digest statements are accepted regardless of the local emission setting.
    EchoDigest(EchoDigest),
    ReadyDigest(ReadyDigest),

    BodyFetch(View, Digest, PublicKey),

    BodyServe(View, ViewProposal),

    /// Contains the lane author, first requested height, and declared requester.
    LaneResume(PublicKey, Height, PublicKey),

    /// The floor is advisory; the receiver's drop state is authoritative.
    ResumeHello(View, PublicKey),

    /// Contains the end key, completion flag, clamp flag, and declared sender.
    ReplayDone(View, bool, bool, PublicKey),

    // Sequence messages carry a declared sender.
    SequenceAnnounce(SequenceAnnouncement, PublicKey),
    SequenceAnnounceBatch(Vec<SequenceAnnouncement>, PublicKey),
    SequenceRequest(SequenceRequest, PublicKey),
    SequenceRecords(SequenceRecordChunk, PublicKey),
    SequenceDeltaRequest(SequenceDeltaRequest, PublicKey),
    SequenceDelta(SequenceDeltaChunk, PublicKey),
    SequenceDeltaRangeRequest(SequenceDeltaRangeRequest, PublicKey),
    SequenceDeltaRange(SequenceDeltaRangeChunk, PublicKey),
    SequenceOutcomeRequest(SequenceOutcomeRequest, PublicKey),
    SequenceOutcome(SequenceOutcomeServe, PublicKey),
    SequenceUnavailable(SequenceUnavailable, PublicKey),
    SequenceHeadersRequest(Vec<Digest>, PublicKey),
    SequenceHeaders(Vec<Header>, PublicKey),
}

impl Inbound {
    /// Returns whether this is a droppable request for local service.
    fn is_bulk(&self) -> bool {
        matches!(
            self,
            Inbound::HeadersRequest(_, _)
                | Inbound::ControlFetch(_, _, _)
                | Inbound::BodyFetch(_, _, _)
                | Inbound::LaneResume(_, _, _)
                | Inbound::ResumeHello(_, _)
        )
    }

    fn is_sequence_sync(&self) -> bool {
        matches!(
            self,
            Inbound::SequenceAnnounce(_, _)
                | Inbound::SequenceAnnounceBatch(_, _)
                | Inbound::SequenceRequest(_, _)
                | Inbound::SequenceRecords(_, _)
                | Inbound::SequenceDeltaRequest(_, _)
                | Inbound::SequenceDelta(_, _)
                | Inbound::SequenceDeltaRangeRequest(_, _)
                | Inbound::SequenceDeltaRange(_, _)
                | Inbound::SequenceOutcomeRequest(_, _)
                | Inbound::SequenceOutcome(_, _)
                | Inbound::SequenceUnavailable(_, _)
                | Inbound::SequenceHeadersRequest(_, _)
                | Inbound::SequenceHeaders(_, _)
        )
    }

    /// Returns whether this input is required while stale view traffic is shed.
    fn keep_during_large_sequence_sync(&self) -> bool {
        matches!(
            self,
            // Wishes remain required because installation does not advance the AGB view.
            Inbound::Wish(_, _)
                | Inbound::SequenceAnnounce(_, _)
                | Inbound::SequenceAnnounceBatch(_, _)
                | Inbound::SequenceRecords(_, _)
                | Inbound::SequenceDelta(_, _)
                | Inbound::SequenceDeltaRange(_, _)
                | Inbound::SequenceOutcome(_, _)
                | Inbound::SequenceUnavailable(_, _)
                | Inbound::SequenceHeaders(_, _)
        )
    }

    /// Returns the view of input that a verified installation can replace.
    fn install_obsolete_view(&self) -> Option<View> {
        match self {
            Inbound::Propose(p) => Some(p.view()),
            Inbound::Echo(e) => Some(e.proposal_view()),
            Inbound::EchoSkip(view, _, _) => Some(*view),
            Inbound::Ready(r) => Some(r.proposal_view()),
            Inbound::NoReady(view, _, _) => Some(*view),
            Inbound::Wish(view, _) => Some(*view),
            Inbound::CompReport(view, _, _) => Some(*view),
            Inbound::ControlInit(proposal, _) => proposal.value.as_ref().map(|(view, _)| *view),
            Inbound::ControlEcho(_, proposal) => proposal.value.as_ref().map(|(view, _)| *view),
            Inbound::ControlReady(_, proposal) => proposal.value.as_ref().map(|(view, _)| *view),
            Inbound::ControlFetch(view, _, _) => Some(*view),
            Inbound::ControlServe(view, _) => Some(*view),
            Inbound::SkipVote(view, _) => Some(*view),
            Inbound::EchoDigest(msg) => Some(msg.view),
            Inbound::ReadyDigest(msg) => Some(msg.view),
            Inbound::BodyFetch(view, _, _) => Some(*view),
            Inbound::BodyServe(view, _) => Some(*view),
            _ => None,
        }
    }
}

fn ingress_replaces_inbound(
    inbound: &Inbound,
    large_gap_drop: bool,
    install_drop_through: View,
) -> bool {
    (large_gap_drop && !inbound.keep_during_large_sequence_sync())
        || (install_drop_through > 0
            && inbound
                .install_obsolete_view()
                .is_some_and(|view| view <= install_drop_through))
}

fn author_label(author: &PublicKey) -> String {
    author.to_string()
}

/// Receives Vantage messages from the primary-to-primary transport.
#[derive(Clone)]
pub struct VantageReceiverHandler {
    pub tx: Sender<Inbound>,
    pub(crate) codec: wire::VantageWireCodec,

    /// Full bulk queues drop requests that the requester can retry.
    pub tx_bulk: Sender<Inbound>,

    /// Sequence traffic bypasses the ordinary consensus queue.
    pub tx_sequence: Sender<Inbound>,

    /// Set while stale view-scoped traffic must be rejected before enqueueing.
    pub sequence_large_gap_drop: Arc<AtomicBool>,

    /// Highest view replaced by the active install.
    pub sequence_install_drop_through: Arc<AtomicU64>,
    pub ack_aggregator: SharedAckAggregator,

    /// Metrics may be absent in directly constructed test handlers.
    pub metrics: Option<Arc<Metrics>>,
}

impl VantageReceiverHandler {
    fn recovery_replaces(&self, inbound: &Inbound) -> bool {
        ingress_replaces_inbound(
            inbound,
            self.sequence_large_gap_drop.load(Ordering::Relaxed),
            self.sequence_install_drop_through.load(Ordering::Relaxed),
        )
    }
}

#[async_trait]
impl MessageHandler for VantageReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        let message = self.codec.deserialize(&serialized)?;

        if let Some(metrics) = &self.metrics {
            crate::primary::record_typed_received(metrics, message.type_name(), serialized.len());
        }
        let inbound = match message {
            PrimaryMessage::Header(h, false) => Inbound::Publish(h.author, h),
            PrimaryMessage::Header(h, true) => Inbound::Serve(h),
            PrimaryMessage::HeadersRequest(digests, requestor) => {
                Inbound::HeadersRequest(digests, requestor)
            }
            PrimaryMessage::VantageAck(a) => {
                let Some(availability) =
                    aggregate_received_ack(&self.ack_aggregator, self.metrics.as_deref(), &a)
                else {
                    return Ok(());
                };
                Inbound::AckAvailability(availability)
            }
            PrimaryMessage::VantageAvail(entries, sender) => Inbound::Avail(entries, sender),
            PrimaryMessage::VantagePropose(p) => Inbound::Propose(ProposalOut::Single(p)),
            PrimaryMessage::VantageEcho(e) => Inbound::Echo(EchoOut::Single(e)),
            PrimaryMessage::VantageEchoSkip(v, s, w) => Inbound::EchoSkip(v, s, w),
            PrimaryMessage::VantageReady(r) => Inbound::Ready(ReadyOut::Single(r)),
            PrimaryMessage::VantageNoReady(v, s, w) => Inbound::NoReady(v, s, w),
            PrimaryMessage::VantageWish(v, s) => Inbound::Wish(v, s),
            PrimaryMessage::CompReport(v, d, s) => Inbound::CompReport(v, d, s),
            PrimaryMessage::ControlInit(p, b) => {
                Inbound::ControlInit(p, b.map(ProposalOut::Single))
            }
            PrimaryMessage::ControlEcho(p, s) => Inbound::ControlEcho(s, p),
            PrimaryMessage::ControlReady(p, s) => Inbound::ControlReady(s, p),
            PrimaryMessage::ControlCommit(r, s) => Inbound::ControlCommit(s, r),
            PrimaryMessage::ControlTimeoutVote(r, s) => Inbound::ControlTimeoutVote(s, r),
            PrimaryMessage::ControlTimeoutAccept(r, s) => Inbound::ControlTimeoutAccept(s, r),
            PrimaryMessage::ControlFetch(v, d, s) => Inbound::ControlFetch(v, d, s),
            PrimaryMessage::ControlServe(v, p) => Inbound::ControlServe(v, ProposalOut::Single(p)),

            PrimaryMessage::VantageProposeBatch(p) => Inbound::Propose(ProposalOut::Batch(p)),
            PrimaryMessage::VantageEchoBatch(e) => Inbound::Echo(EchoOut::Batch(e)),
            PrimaryMessage::VantageReadyBatch(r) => Inbound::Ready(ReadyOut::Batch(r)),
            PrimaryMessage::ControlInitBatch(p, b) => {
                Inbound::ControlInit(p, b.map(ProposalOut::Batch))
            }
            PrimaryMessage::ControlServeBatch(v, p) => {
                Inbound::ControlServe(v, ProposalOut::Batch(p))
            }
            PrimaryMessage::VantageSkipVote(v, s) => Inbound::SkipVote(v, s),

            PrimaryMessage::VantageEchoDigest(d) => Inbound::EchoDigest(d),
            PrimaryMessage::VantageReadyDigest(d) => Inbound::ReadyDigest(d),
            PrimaryMessage::VantageBodyFetch(v, d, r) => Inbound::BodyFetch(v, d, r),
            PrimaryMessage::VantageBodyServe(v, p) => Inbound::BodyServe(v, p),

            PrimaryMessage::VantageLaneResume(author, from, requester) => {
                Inbound::LaneResume(author, from, requester)
            }

            PrimaryMessage::VantageResumeHello(floor, sender) => {
                Inbound::ResumeHello(floor, sender)
            }
            PrimaryMessage::VantageReplayDone(end_key, complete, clamped, sender) => {
                Inbound::ReplayDone(end_key, complete, clamped, sender)
            }

            PrimaryMessage::VantageSequenceAnnounce(a) => {
                let claimed = a.sender;
                Inbound::SequenceAnnounce(a, claimed)
            }
            PrimaryMessage::VantageSequenceAnnounceBatch(announcements, sender) => {
                Inbound::SequenceAnnounceBatch(announcements, sender)
            }
            PrimaryMessage::VantageSequenceRequest(r) => {
                let claimed = r.requester;
                Inbound::SequenceRequest(r, claimed)
            }
            PrimaryMessage::VantageSequenceRecords(c) => {
                let claimed = c.sender;
                Inbound::SequenceRecords(c, claimed)
            }
            PrimaryMessage::VantageSequenceDeltaRequest(r) => {
                let claimed = r.requester;
                Inbound::SequenceDeltaRequest(r, claimed)
            }
            PrimaryMessage::VantageSequenceDelta(c) => {
                let claimed = c.sender;
                Inbound::SequenceDelta(c, claimed)
            }
            PrimaryMessage::VantageSequenceDeltaRangeRequest(r) => {
                let claimed = r.requester;
                Inbound::SequenceDeltaRangeRequest(r, claimed)
            }
            PrimaryMessage::VantageSequenceDeltaRange(c) => {
                let claimed = c.sender;
                Inbound::SequenceDeltaRange(c, claimed)
            }
            PrimaryMessage::VantageSequenceOutcomeRequest(r) => {
                let claimed = r.requester;
                Inbound::SequenceOutcomeRequest(r, claimed)
            }
            PrimaryMessage::VantageSequenceOutcome(o) => {
                let claimed = o.sender;
                Inbound::SequenceOutcome(o, claimed)
            }
            PrimaryMessage::VantageSequenceUnavailable(u) => {
                let claimed = u.sender;
                Inbound::SequenceUnavailable(u, claimed)
            }
            PrimaryMessage::VantageSequenceHeadersRequest(digests, requester) => {
                Inbound::SequenceHeadersRequest(digests, requester)
            }
            PrimaryMessage::VantageSequenceHeaders(headers, sender) => {
                Inbound::SequenceHeaders(headers, sender)
            }

            _ => return Ok(()),
        };
        if self.recovery_replaces(&inbound) {
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_sequence_install_obsolete_inbound_dropped_total
                    .inc();
            }
            return Ok(());
        }

        // Sequence and bulk queues are best effort; retries recover drops.
        if inbound.is_sequence_sync() {
            if self.tx_sequence.try_send(inbound).is_err() {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_sync_inbound_dropped_total.inc();
                }
            }
            return Ok(());
        }
        if inbound.is_bulk() {
            if self.tx_bulk.try_send(inbound).is_err() {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_bulk_inbound_dropped_total.inc();
                }
            }
            return Ok(());
        }

        // Recheck after reservation because the drop policy may change.
        let permit = self
            .tx
            .reserve()
            .await
            .expect("Failed to reserve vantage message slot");
        if self.recovery_replaces(&inbound) {
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_sequence_install_obsolete_inbound_dropped_total
                    .inc();
            }
            return Ok(());
        }
        permit.send(inbound);
        Ok(())
    }
}

pub struct VantageCore {
    name: PublicKey,

    /// Only committee members may contribute wire messages to quorum counts.
    members: HashSet<PublicKey>,
    lm: LaneManager,
    ack_aggregator: SharedAckAggregator,
    rep: Repairer,
    agb: AgbEngine,
    frontier: Frontier,
    cursor: Cursor,

    sequence: Option<SequenceStore>,

    /// Collects matching checkpoint heads from at least `f + 1` declared senders.
    sequence_sync: Option<CheckpointCollector>,
    sequence_chunk_records: usize,
    sequence_chunk_outcomes: usize,
    sequence_chunk_outcome_items: usize,
    sequence_chunk_digests: usize,
    sequence_sync_min_gap_views: View,
    sequence_sync_shed_gap_views: View,
    sequence_sync_rearm_gap_views: View,
    sequence_large_gap_drop: Arc<AtomicBool>,
    sequence_install_drop_through: Arc<AtomicU64>,

    /// Remains active until the installed cursor crosses the live intake floor.
    sequence_sync_recovery_active: bool,

    /// Prevents ordinary recovery jitter from restarting state sync.
    sequence_sync_recovered: bool,

    /// Lowest view after which consensus intake has remained uninterrupted.
    sequence_live_intake_floor: View,

    sequence_shed_was_active: bool,

    /// Defers recovery completion until a staged installation finishes.
    sequence_latch_pending: bool,
    sequence_announce_period_ms: u64,
    sequence_announce_repeat_ms: u64,

    /// At most one sequence transfer may be active.
    sequence_transfer: Option<SequenceTransfer>,
    sequence_transfer_seq: u64,

    /// Missing verified-output headers requested from certified checkpoint sources.
    sequence_block_requests: HashMap<Digest, SequenceBlockRequestState>,

    /// Certified sources retained for the active installation target.
    sequence_install_sources: Vec<PublicKey>,

    /// Highest fully verified remote target awaiting local comparison.
    sequence_verified_target: Option<(View, Digest)>,

    /// At most one verified target may be staged for installation.
    sequence_install: Option<SequenceInstall>,
    sequence_install_window_views: usize,
    sequence_install_settle_ceiling: usize,
    sequence_install_enabled: bool,
    sequence_install_views_per_tick: usize,
    sequence_install_digests_per_tick: usize,

    /// Prevents repeated readiness logs while a target remains staged.
    sequence_install_ready_logged: bool,

    /// Records whether the pending target comparison includes installed views.
    sequence_target_installed: bool,

    sequence_request_at: Option<Instant>,

    /// Suppresses duplicate requests until the transfer's requested item changes.
    sequence_last_want: Option<SequenceWant>,
    sequence_request_timeout_ms: u64,
    sequence_max_sources: usize,

    last_announced: Option<(View, Instant)>,
    pacemaker: Pacemaker,
    resolver: Resolver,
    control: ControlLog,

    /// Translates digest statements into the by-value AGB representation.
    digest_stmts: DigestStatements,

    wire: Wire,

    header_size: usize,
    max_header_delay: u64,
    digests: Vec<(Digest, WorkerId)>,
    payload_size: usize,

    /// Suppresses network acknowledgments but never local self-acknowledgment.
    ack_watermarks: bool,

    /// Acknowledgment watermark period in milliseconds.
    ack_watermark_period_ms: u64,

    /// Controls compact statement emission; reception is always enabled.
    digest_statements: bool,

    /// Enables positional availability claims and disables periodic availability frames.
    echo_avail_claims: bool,

    /// Tracks persistent requester-side lane gaps and request backoff.
    resume_trigger: ResumeTrigger,

    /// Deduplicates author-side lane-resume service.
    resume_serve: ResumeServe,

    /// Lane-resume check period in milliseconds.
    resume_check_period_ms: u64,

    /// Shared request and service backoff in milliseconds.
    resume_backoff_ms: u64,

    /// Maximum own-lane blocks served per resume request.
    resume_batch: u64,

    /// Enables replay of volatile protocol messages independently of lane resume.
    reconnect_replay: bool,

    /// Stores bounded volatile broadcast history for reconnect replay.
    outbox: Outbox,

    /// Authoritative lowest possibly missing filing key for each peer.
    pending_low: HashMap<PublicKey, View>,

    replay_episodes: ReplayEpisodes,

    serve_budget: ServeBudget,

    nudge_memo: NudgeMemo,

    /// Replay retention window in views.
    replay_history_views: View,

    /// Maximum replay bytes served per peer in one backoff window.
    replay_serve_max_bytes: usize,

    /// Replay episode and in-flight stream lifetime in milliseconds.
    replay_episode_max_ms: u64,

    /// AGB timers ordered by earliest deadline.
    timers: BinaryHeap<Reverse<(Instant, View, TimerKind)>>,

    /// Control timers ordered by earliest deadline.
    control_timers: BinaryHeap<Reverse<(Instant, Round)>>,

    payload: PayloadIo,

    /// Internal-state retention window in views.
    gc_window: View,
    last_gc_floor: View,

    metrics: Option<Arc<Metrics>>,

    ut_inbound_dispatch: Option<IntCounter>,
    ut_payload_sync: Option<IntCounter>,
    ut_timer_firing: Option<IntCounter>,
    ut_effect_execution: Option<IntCounter>,

    ut_avail_flush: Option<IntCounter>,
    ut_resume_tick: Option<IntCounter>,
    ut_metrics_tick: Option<IntCounter>,

    walk_steps_published: (u64, u64, u64),

    walk_fails_published: ([u64; 3], [u64; 3]),
    ut_header_seal: Option<IntCounter>,

    queue_len_peak: usize,

    /// Coalesces idempotent AGB gate rechecks until the effect queue drains.
    recheck_pending: bool,
}

type BuildOutput = (
    VantageCore,
    Receiver<Inbound>,
    Receiver<Inbound>,
    Receiver<Inbound>,
    Receiver<(Digest, Digest, WorkerId)>,
    Sender<Inbound>,
    Sender<Inbound>,
    Sender<Inbound>,
    SharedAckAggregator,
    Arc<AtomicBool>,
    Arc<AtomicU64>,
    Receiver<SocketAddr>,
);

pub type VantageSpawnOutput = (
    Sender<Inbound>,
    Sender<Inbound>,
    Sender<Inbound>,
    SharedAckAggregator,
    Arc<AtomicBool>,
    Arc<AtomicU64>,
);

impl VantageCore {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        metrics: Option<Arc<Metrics>>,
        rx_our_digests: Receiver<(Digest, WorkerId)>,
        tx_output: Sender<Header>,
    ) -> VantageSpawnOutput {
        let (
            core,
            rx_vantage,
            rx_bulk,
            rx_sequence,
            rx_payload_ready,
            tx_vantage,
            tx_bulk,
            tx_sequence,
            ack_aggregator,
            sequence_large_gap_drop,
            sequence_install_drop_through,
            reconnect_rx,
        ) = Self::build(name, committee, parameters, store, metrics, tx_output);
        tokio::spawn(core.run(
            rx_vantage,
            rx_bulk,
            rx_sequence,
            rx_our_digests,
            rx_payload_ready,
            reconnect_rx,
        ));
        (
            tx_vantage,
            tx_bulk,
            tx_sequence,
            ack_aggregator,
            sequence_large_gap_drop,
            sequence_install_drop_through,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        metrics: Option<Arc<Metrics>>,
        tx_output: Sender<Header>,
    ) -> BuildOutput {
        let (tx_vantage, rx_vantage) = channel(CHANNEL_CAPACITY);

        let (tx_bulk, rx_bulk) = channel(BULK_CHANNEL_CAPACITY);
        let sequence_capacity = parameters.sequence_sync_inbound_capacity.max(1);
        let (tx_sequence, rx_sequence) = channel(sequence_capacity);
        let (tx_payload_ready, rx_payload_ready) = channel(CHANNEL_CAPACITY);
        let sequence_large_gap_drop = Arc::new(AtomicBool::new(false));
        let sequence_install_drop_through = Arc::new(AtomicU64::new(0));

        // Capture membership before constructing protocol state.
        let members: HashSet<PublicKey> = committee.authorities.keys().cloned().collect();

        let sid = block::session_id(&committee);
        let genesis = block::genesis_digest(&sid);
        let blocks: SharedBlocks = Arc::new(Mutex::new(BlockCache::new()));
        let ack_aggregator: SharedAckAggregator =
            Arc::new(Mutex::new(AckAggregator::new(committee.clone())));

        let mut lm = LaneManager::with_shared_blocks(
            name,
            committee.clone(),
            parameters.max_block_payload,
            store.clone(),
            blocks.clone(),
        );
        let mut rep = Repairer::new(
            name,
            committee.clone(),
            sid.clone(),
            genesis.clone(),
            parameters.max_block_payload,
            blocks.clone(),
        );
        let mut agb = AgbEngine::new(name, committee.clone(), sid.clone(), parameters.delta_ms);
        let mut digest_stmts = DigestStatements::new(parameters.delta_ms);
        let core_metrics = metrics.clone();
        if let Some(m) = metrics {
            lm = lm.with_metrics(m.clone());
            rep = rep.with_metrics(m.clone());
            agb = agb.with_metrics(m.clone());
            digest_stmts = digest_stmts.with_metrics(m);
        }
        let frontier = Frontier::new(name, committee.clone());
        let cursor = Cursor::new(
            committee.clone(),
            sid.clone(),
            genesis,
            parameters.max_block_payload,
            blocks,
        );
        let pacemaker = Pacemaker::new(name, &committee);
        let resolver = Resolver::new(committee.size(), parameters.delta_ms);

        let sequence = parameters.sequence_checkpoints.then(|| {
            SequenceStore::new(sid.clone(), parameters.sequence_checkpoint_interval_views)
        });

        let sequence_sync = parameters.sequence_checkpoints.then(|| {
            let n = committee.size();
            let f = n.saturating_sub(1) / 3;

            // Certification still requires matching members.
            CheckpointCollector::new(f + 1, SEQUENCE_CANDIDATE_WINDOWS, View::MAX)
        });
        let control = ControlLog::new(name, committee.clone(), sid, parameters.delta_ms);

        let other_primaries: Vec<(PublicKey, SocketAddr)> = committee
            .others_primaries(&name)
            .into_iter()
            .map(|(pk, addr)| (pk, addr.primary_to_primary))
            .collect();

        let other_primary_addrs: Vec<SocketAddr> =
            other_primaries.iter().map(|(_, a)| *a).collect();

        let addr_to_peer: HashMap<SocketAddr, PublicKey> = other_primaries
            .iter()
            .map(|(pk, addr)| (*addr, *pk))
            .collect();

        let dirty_map: DirtyMap = Arc::new(Mutex::new(HashMap::new()));
        let in_flight: wire::InFlightMap = Arc::new(Mutex::new(HashMap::new()));

        let (reconnect_tx, reconnect_rx) = channel(committee.size().max(1));

        let withheld_header_dests: wire::WithheldHeaderDests =
            config::withheld_destinations(&committee, &name, parameters.withhold_senders).map(
                |blocked| {
                    let full: Vec<(PublicKey, SocketAddr)> = other_primaries
                        .iter()
                        .filter(|(pk, _)| !blocked.contains(pk))
                        .copied()
                        .collect();
                    let addrs: Vec<SocketAddr> = full.iter().map(|(_, a)| *a).collect();
                    (addrs, full)
                },
            );

        let worker_addresses: HashMap<WorkerId, SocketAddr> = committee
            .our_workers_by_id(&name)
            .expect("Our public key is not in the committee")
            .into_iter()
            .map(|(id, addr)| (id, addr.primary_to_worker))
            .collect();

        let latency_map = parameters
            .latency_table
            .as_deref()
            .map(|table| committee.latency_map(&name, table))
            .unwrap_or_default();

        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        let wire_codec = wire::VantageWireCodec::new(&committee, parameters.vantage_compact_ids)
            .unwrap_or_else(|error| panic!("invalid Vantage wire configuration: {error}"));

        let resume_senders = wire::spawn_resume_sender(
            latency_map.clone(),
            batch,
            core_metrics.clone(),
            in_flight.clone(),
            wire_codec.clone(),
            parameters.replay_chunk_bytes,
            parameters.replay_chunk_interval_ms,
            parameters.replay_serve_max_bytes,
            parameters.retry_backoff_max_ms,
        );

        let core = Self {
            name,
            members,
            lm,
            ack_aggregator: ack_aggregator.clone(),
            rep,
            agb,
            frontier,
            cursor,
            sequence,
            sequence_sync,
            sequence_chunk_records: parameters.sequence_sync_chunk_records,
            sequence_chunk_outcomes: parameters.sequence_sync_chunk_outcomes,
            sequence_chunk_outcome_items: parameters.sequence_sync_chunk_outcome_items,
            sequence_chunk_digests: parameters.sequence_sync_chunk_digests,
            sequence_sync_min_gap_views: parameters.sequence_sync_min_gap_views,
            sequence_sync_shed_gap_views: parameters.sequence_sync_shed_gap_views,
            sequence_sync_rearm_gap_views: parameters.sequence_sync_rearm_gap_views,
            sequence_large_gap_drop: sequence_large_gap_drop.clone(),
            sequence_install_drop_through: sequence_install_drop_through.clone(),
            sequence_sync_recovery_active: false,
            sequence_sync_recovered: false,
            sequence_live_intake_floor: 0,
            sequence_shed_was_active: false,
            sequence_latch_pending: false,
            sequence_announce_period_ms: parameters.sequence_announce_period_ms,
            sequence_announce_repeat_ms: parameters.sequence_announce_repeat_ms,
            sequence_transfer: None,
            sequence_transfer_seq: 0,
            sequence_block_requests: HashMap::new(),
            sequence_install_sources: Vec::new(),
            sequence_verified_target: None,
            sequence_install: None,
            sequence_install_window_views: parameters.sequence_install_window_views,
            sequence_install_settle_ceiling: parameters.sequence_install_settle_ceiling,
            sequence_install_enabled: parameters.sequence_install_enabled,
            sequence_install_views_per_tick: parameters.sequence_install_views_per_tick,
            sequence_install_digests_per_tick: parameters.sequence_install_digests_per_tick,
            sequence_install_ready_logged: false,
            sequence_target_installed: false,
            sequence_request_at: None,
            sequence_last_want: None,
            sequence_request_timeout_ms: parameters.sequence_sync_request_timeout_ms,
            sequence_max_sources: parameters.sequence_sync_max_sources,
            last_announced: None,
            pacemaker,
            resolver,
            control,
            digest_stmts,
            wire: Wire {
                codec: wire_codec,
                network: {
                    let mut s = ReliableSender::new()
                        .with_latency(latency_map.clone())
                        .with_batching(batch)
                        .with_retry_backoff_max_ms(parameters.retry_backoff_max_ms);

                    if parameters.reconnect_replay {
                        s = s
                            .with_reconnect_events(reconnect_tx)
                            .with_drop_map(dirty_map.clone())
                            .with_volatile_soft_cap(parameters.volatile_soft_cap);
                    }
                    if let Some(m) = &core_metrics {
                        s = s.with_metrics(m.clone());
                    }
                    s
                },
                worker_network: {
                    let mut s = SimpleSender::new()
                        .with_latency(latency_map)
                        .with_batching(batch);
                    if let Some(m) = &core_metrics {
                        s = s.with_metrics(m.clone());
                    }
                    s
                },
                resume_lane_tx: resume_senders.lane,
                replay_tx: resume_senders.replay,
                sequence_tx: resume_senders.sequence,
                replay_generation: resume_senders.generation,
                cancel_handlers: Vec::new(),
                last_prune_len: 0,
                other_primaries,
                other_primary_addrs,
                worker_addresses,
                withheld_header_dests,
                withhold_window: parameters.withhold_window.clone(),
                metrics: core_metrics.clone(),
                addr_to_peer,
                dirty_map,
                in_flight,
            },
            header_size: parameters.header_size,
            max_header_delay: parameters.max_header_delay,
            digests: Vec::new(),
            payload_size: 0,
            ack_watermarks: parameters.ack_watermarks,
            ack_watermark_period_ms: parameters.ack_watermark_period_ms,
            digest_statements: parameters.digest_statements,
            echo_avail_claims: parameters.ack_watermarks && parameters.echo_avail_claims,
            resume_trigger: ResumeTrigger::with_max_concurrent(parameters.resume_max_concurrent),
            resume_serve: ResumeServe::new(),
            resume_check_period_ms: parameters.resume_check_period_ms,
            resume_backoff_ms: parameters.resume_backoff_ms,
            resume_batch: parameters.resume_batch,
            reconnect_replay: parameters.reconnect_replay,
            outbox: Outbox::new(parameters.outbox_max_bytes),
            pending_low: HashMap::new(),
            replay_episodes: ReplayEpisodes::new(),
            serve_budget: ServeBudget::new(),
            nudge_memo: NudgeMemo::new(),

            // Keep the current broadcast when the window is zero.
            replay_history_views: parameters.replay_history_views.max(1),
            replay_serve_max_bytes: parameters.replay_serve_max_bytes,
            replay_episode_max_ms: parameters.replay_episode_max_ms,
            timers: BinaryHeap::new(),
            control_timers: BinaryHeap::new(),
            payload: PayloadIo::new(store, tx_payload_ready, tx_output, core_metrics.clone()),

            // Keep state for the current view when the window is zero.
            gc_window: parameters.vantage_gc_window_views.max(1),
            last_gc_floor: 1,
            metrics: core_metrics,
            ut_inbound_dispatch: None,
            ut_payload_sync: None,
            ut_timer_firing: None,
            ut_effect_execution: None,
            ut_avail_flush: None,
            ut_resume_tick: None,
            ut_metrics_tick: None,
            walk_steps_published: (0, 0, 0),
            walk_fails_published: ([0; 3], [0; 3]),
            ut_header_seal: None,
            queue_len_peak: 0,
            recheck_pending: false,
        };
        (
            core,
            rx_vantage,
            rx_bulk,
            rx_sequence,
            rx_payload_ready,
            tx_vantage,
            tx_bulk,
            tx_sequence,
            ack_aggregator,
            sequence_large_gap_drop,
            sequence_install_drop_through,
            reconnect_rx,
        )
    }

    async fn run(
        mut self,
        mut rx_vantage: Receiver<Inbound>,
        mut rx_bulk: Receiver<Inbound>,
        mut rx_sequence: Receiver<Inbound>,
        mut rx_our_digests: Receiver<(Digest, WorkerId)>,
        mut rx_payload_ready: Receiver<(Digest, Digest, WorkerId)>,
        mut reconnect_rx: Receiver<SocketAddr>,
    ) {
        let boot = Instant::now();

        // Restore the lane frontier before publishing.
        self.lm.restore_own_frontier().await;

        let mut effects = Vec::new();
        // Republish an anchor whose initial send may not have completed.
        if let Some(anchor) = self.lm.take_seeded_anchor() {
            effects.push(Effect::BroadcastPublish(anchor));
        }

        effects.extend(self.enter_view_effects(1, boot));
        effects.extend(self.pacemaker.genesis());

        effects.extend(self.control.genesis());
        self.execute(effects, boot).await;

        let header_timer = tokio::time::sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(header_timer);

        let mut metrics_tick = tokio::time::interval(Duration::from_secs(1));
        metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut avail_tick = if self.ack_watermarks && !self.echo_avail_claims {
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.ack_watermark_period_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            Some(interval)
        } else {
            None
        };

        let mut resume_tick =
            tokio::time::interval(Duration::from_millis(self.resume_check_period_ms));
        resume_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut announce_tick = self.sequence.as_ref().map(|_| {
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.sequence_announce_period_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });
        let mut sequence_install_tick = self.sequence.as_ref().map(|_| {
            let mut interval =
                tokio::time::interval(Duration::from_millis(SEQUENCE_INSTALL_DRIVE_PERIOD_MS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval
        });

        loop {
            self.refresh_sequence_large_gap_drop();

            self.wire.maybe_prune_cancel_handlers();

            let next_deadline = self.timers.peek().map(|Reverse((d, _, _))| *d);
            let agb_sleep = async {
                match next_deadline {
                    Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(agb_sleep);

            let next_control_deadline = self.control_timers.peek().map(|Reverse((d, _))| *d);
            let control_sleep = async {
                match next_control_deadline {
                    Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(control_sleep);

            self.queue_len_peak = self.queue_len_peak.max(rx_vantage.len());

            // Check timers even while a queue is ready.
            tokio::select! {
                Some(inbound) = rx_vantage.recv() => {
                    self.dispatch_and_execute(inbound).await;
                }

                Some(inbound) = rx_bulk.recv() => {
                    self.dispatch_and_execute(inbound).await;
                }

                Some(inbound) = rx_sequence.recv() => {
                    self.dispatch_and_execute(inbound).await;
                }

                Some((header_digest, digest, worker_id)) = rx_payload_ready.recv() => {

                    self.on_payload_ready(header_digest, digest, worker_id).await;
                }

                Some((digest, worker_id)) = rx_our_digests.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                    if self.payload_size >= self.header_size {
                        self.seal_own_header(Instant::now()).await;
                        header_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_header_delay));
                    }
                }

                () = &mut header_timer => {
                    self.seal_own_header(Instant::now()).await;
                    header_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_header_delay));
                }

                () = &mut agb_sleep, if next_deadline.is_some() => {
                    let now = Instant::now();
                    let timer_firing_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_timer_firing, "timer_firing");
                    let effects = self.fire_agb_timers(now);
                    drop(timer_firing_timer);
                    self.execute(effects, now).await;
                }

                () = &mut control_sleep, if next_control_deadline.is_some() => {
                    let now = Instant::now();
                    let timer_firing_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_timer_firing, "timer_firing");
                    let effects = self.fire_control_timers(now);
                    drop(timer_firing_timer);
                    self.execute(effects, now).await;
                }

                () = async {
                    match avail_tick.as_mut() {
                        Some(t) => { t.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                }, if avail_tick.is_some() => {
                    let _avail_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_avail_flush, "avail_flush");
                    if let Some(entries) = self.lm.take_avail_flush() {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_avail_sent.inc();
                        }
                        self.wire
                            .broadcast_message(PrimaryMessage::VantageAvail(entries, self.name))
                            .await;
                    }
                }

                () = async {
                    match announce_tick.as_mut() {
                        Some(t) => { t.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                }, if announce_tick.is_some() => {
                    self.announce_checkpoint();
                    self.drive_sequence_sync();
                }

                () = async {
                    match sequence_install_tick.as_mut() {
                        Some(t) => { t.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                }, if sequence_install_tick.is_some() => {
                    // Installation has independent view, digest, and in-flight budgets.
                    let now = Instant::now();
                    let install_effects = self.drive_sequence_install(now).await;
                    if !install_effects.is_empty() {
                        self.execute(install_effects, now).await;
                    }
                }

                _ = resume_tick.tick() => {
                    let now = Instant::now();
                    let _resume_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_resume_tick, "resume_tick");

                    // Refresh drop state before replay and nudge checks.
                    if self.reconnect_replay {
                        self.sweep_dirty_map();
                    }

                    let authors: Vec<PublicKey> =
                        self.wire.other_primaries.iter().map(|(pk, _)| *pk).collect();
                    let episode_backoff = Duration::from_millis(self.resume_backoff_ms);
                    let episode_max_age = Duration::from_millis(self.replay_episode_max_ms);
                    for author in authors {
                        // Lane resume is independent of reconnect replay.
                        self.try_resume_request(author, now);

                        self.resume_tick_replay_effects(author, now, episode_backoff, episode_max_age).await;
                    }
                }

                // Reconnect events prompt replay; periodic ticks retry it.
                Some(addr) = reconnect_rx.recv() => {
                    let now = Instant::now();
                    let resolved = self.wire.addr_to_peer.get(&addr).copied();
                    let peer_index = resolved.and_then(|peer| {
                        self.wire.other_primaries.iter().position(|(pk, _)| *pk == peer)
                    });
                    log::debug!(
                        "vantage node: reconnect event received: addr={} peer_index={}",
                        addr,
                        peer_index.map_or_else(|| "unmapped".to_string(), |i| i.to_string())
                    );
                    if let Some(peer) = resolved {

                        if self.reconnect_replay {
                            self.replay_episodes.open(peer, now);
                            self.send_resume_hello(peer, now, "event").await;
                        }
                        self.try_resume_request(peer, now);
                    }
                }

                _ = metrics_tick.tick() => {
                    let metrics_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_metrics_tick, "metrics_tick");

                    self.wire.prune_cancel_handlers();
                    self.collect_internal_garbage();

                    let retry_now = Instant::now();
                    let mut retry_effects = self.digest_stmts.retry_fetches(retry_now);

                    self.rep.observe_core_queue(rx_vantage.len());
                    retry_effects.extend(self.rep.retry_requests());

                    for r in self.lm.take_missing_parents(16) {
                        log::debug!(
                            "vantage repair: prefix walk reported a missing block \
                             author={} height={}; authorizing repair",
                            r.0,
                            r.1
                        );
                        retry_effects.extend(self.rep.authorize(r));
                    }

                    drop(metrics_timer);
                    self.execute(retry_effects, retry_now).await;
                    let _metrics_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_metrics_tick, "metrics_tick");
                    self.sample_metrics();

                    let queue_len = rx_vantage.len();
                    self.queue_len_peak = self.queue_len_peak.max(queue_len);
                    if let Some(metrics) = &self.metrics {
                        metrics.core_queue_length.set(queue_len as i64);
                        metrics.core_queue_peak.set(self.queue_len_peak as i64);
                    }
                    self.queue_len_peak = 0;
                }
            }
        }
    }

    async fn seal_own_header(&mut self, now: Instant) {
        if self.sequence_sync_recovery_active
            || self.sequence_install.is_some()
            || self.large_sequence_sync_target().is_some()
        {
            self.digests.clear();
            self.payload_size = 0;
            return;
        }
        let seal_timer =
            Self::cached_utilization_timer(&self.metrics, &mut self.ut_header_seal, "header_seal");
        let payload = self.digests.drain(..).collect();
        self.payload_size = 0;
        let (_, effects) = self.lm.publish_own(payload).await;

        drop(seal_timer);
        self.execute(effects, now).await;
    }

    async fn dispatch_and_execute(&mut self, inbound: Inbound) {
        let now = Instant::now();
        let dispatch_timer = Self::cached_utilization_timer(
            &self.metrics,
            &mut self.ut_inbound_dispatch,
            "inbound_dispatch",
        );
        let effects = self.dispatch_inbound(inbound, now).await;
        drop(dispatch_timer);
        self.execute(effects, now).await;
    }

    /// Marks a header payload-ready only when its final missing batch arrives.
    async fn on_payload_ready(
        &mut self,
        header_digest: Digest,
        digest: Digest,
        worker_id: WorkerId,
    ) {
        let now = Instant::now();
        let payload_sync_timer = Self::cached_utilization_timer(
            &self.metrics,
            &mut self.ut_payload_sync,
            "payload_sync",
        );
        let mut resolved = false;
        if let Some(set) = self.payload.pending_payload.get_mut(&header_digest) {
            set.remove(&(digest, worker_id));
            resolved = set.is_empty();
        }

        self.payload.publish_sizes();
        if resolved {
            self.payload.pending_payload.remove(&header_digest);

            let author = self.lm.author_of(&header_digest);
            let before = author.map(|a| self.lm.own_direct_frontier(&a));

            let mut effects = self.lm.set_payload_ready(&header_digest);
            effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
            drop(payload_sync_timer);
            self.execute(effects, now).await;
            let _payload_sync_timer = Self::cached_utilization_timer(
                &self.metrics,
                &mut self.ut_payload_sync,
                "payload_sync",
            );
            if let (Some(author), Some(before)) = (author, before) {
                if self.lm.own_direct_frontier(&author) > before {
                    self.try_resume_request(author, now);
                }
            }
        }
    }

    /// Continues an established lane gap at receipt pace and applies shared backoff.
    fn try_resume_request(&mut self, author: PublicKey, now: Instant) {
        let frontier = self.lm.own_direct_frontier(&author);
        let avail = self.lm.avail_high(&author);
        let backoff = Duration::from_millis(self.resume_backoff_ms);
        if let Some(from) =
            self.resume_trigger
                .check(author, frontier, avail, now, backoff, self.resume_batch)
        {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_lane_resume_requests_sent.inc();
            }
            self.wire.enqueue_resume(
                author,
                PrimaryMessage::VantageLaneResume(author, from, self.name),
            );
        }
    }

    async fn resume_tick_replay_effects(
        &mut self,
        author: PublicKey,
        now: Instant,
        backoff: Duration,
        max_age: Duration,
    ) {
        if !self.reconnect_replay {
            return;
        }
        if self.replay_episodes.tick(author, now, backoff, max_age) {
            self.send_resume_hello(author, now, "tick").await;
        }
        self.maybe_nudge(author, now, backoff).await;
    }

    /// Records replayable broadcasts; uses durable sends when replay is disabled.
    async fn broadcast_recorded(&mut self, message: PrimaryMessage) {
        if !self.reconnect_replay {
            self.wire.broadcast_message(message).await;
            return;
        }
        let msg_type = message.type_name();
        let bytes = Bytes::from(self.wire.serialize_message(&message));
        let key = self.pacemaker.own_watermark();
        self.outbox.record(key, bytes.clone());
        self.wire.broadcast_volatile(bytes, msg_type, key).await;
    }

    /// Drains transport drop reports and minimum-merges them into authoritative peer floors.
    fn sweep_dirty_map(&mut self) {
        let drained: HashMap<SocketAddr, u64> = {
            let mut guard = self.wire.dirty_map.lock();
            std::mem::take(&mut *guard)
        };
        for (addr, key) in drained {
            let Some(&peer) = self.wire.addr_to_peer.get(&addr) else {
                continue;
            };
            self.pending_low
                .entry(peer)
                .and_modify(|existing| *existing = (*existing).min(key))
                .or_insert(key);
        }
    }

    /// Returns whether a replay stream is live and removes expired markers.
    fn check_in_flight(&mut self, peer: PublicKey, now: Instant) -> bool {
        let ttl = Duration::from_millis(self.replay_episode_max_ms);
        let state = {
            let guard = self.wire.in_flight.lock();
            in_flight_state(&guard, &peer, now, ttl)
        };
        match state {
            InFlightState::InFlight => true,
            InFlightState::Absent => false,
            InFlightState::Expired(generation) => {
                wire::remove_in_flight_generation(&self.wire.in_flight, peer, generation);
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_replay_inflight_ttl_expired_total.inc();
                }
                false
            }
        }
    }

    /// Sends a bounded replay request through the durable unicast path.
    async fn send_resume_hello(&mut self, peer: PublicKey, now: Instant, trigger: &'static str) {
        let floor_hint = self.pacemaker.omega_of(peer);
        log::debug!(
            "vantage node: resume hello sent: peer={} floor_hint={} trigger={}",
            peer,
            floor_hint,
            trigger
        );
        self.wire
            .send_message(
                peer,
                PrimaryMessage::VantageResumeHello(floor_hint, self.name),
            )
            .await;
        self.replay_episodes.record_hello_sent(peer, now);
    }

    /// Sends a self-superseding replay nudge through the volatile path.
    async fn send_nudge_hello(&mut self, peer: PublicKey, now: Instant) {
        let floor_hint = self.pacemaker.omega_of(peer);
        let key = self.pacemaker.own_watermark();
        log::debug!(
            "vantage node: resume hello sent: peer={} floor_hint={} trigger=nudge",
            peer,
            floor_hint
        );
        self.wire
            .send_volatile(
                peer,
                PrimaryMessage::VantageResumeHello(floor_hint, self.name),
                key,
            )
            .await;
        self.replay_episodes.record_hello_sent(peer, now);
        if let Some(metrics) = &self.metrics {
            metrics.vantage_replay_pending_low_nudges_total.inc();
        }
    }

    /// Sends replay nudges only to peers with a known gap, no live stream, and expired cooldown.
    async fn maybe_nudge(&mut self, peer: PublicKey, now: Instant, backoff: Duration) {
        if !self.pending_low.contains_key(&peer) {
            return;
        }
        if self.check_in_flight(peer, now) {
            return;
        }
        if !self.nudge_memo.due(peer, now, backoff) {
            return;
        }
        self.send_nudge_hello(peer, now).await;
        self.nudge_memo.record(peer, now);
    }

    /// Serves from the minimum of the peer hint and the locally recorded drop floor.
    async fn on_resume_hello(&mut self, hello_floor: View, sender: PublicKey, now: Instant) {
        log::debug!(
            "vantage node: resume hello received: sender={} floor={}",
            sender,
            hello_floor
        );
        let backoff = Duration::from_millis(self.resume_backoff_ms);

        // Reciprocation and drop-map draining must precede the authoritative floor read.
        if self.replay_episodes.on_hello_received(sender, now, backoff) {
            self.send_resume_hello(sender, now, "reciprocal").await;
        }

        self.sweep_dirty_map();

        if self.check_in_flight(sender, now) {
            log::debug!(
                "vantage node: resume serve suppressed: sender={} gate=in-flight",
                sender
            );
            return;
        }

        let pending = self.pending_low.get(&sender).copied();
        let raw_from = match pending {
            Some(p) => hello_floor.min(p),
            None => hello_floor,
        };
        let outbox_floor = self.outbox.floor();
        let served_from = raw_from.max(outbox_floor);
        let clamped = served_from > raw_from;

        let remaining =
            self.serve_budget
                .remaining(sender, now, backoff, self.replay_serve_max_bytes);
        let has_backlog = self.outbox.slice_from(served_from).next().is_some();
        if remaining == 0 && has_backlog {
            log::debug!(
                "vantage node: resume serve suppressed: sender={} served_from={} gate=budget",
                sender,
                served_from
            );
            return;
        }

        let (msgs, end_key, complete) = self.outbox.take_budgeted_slice(served_from, remaining);
        let msg_count = msgs.len();
        let served_bytes: usize = msgs.iter().map(Bytes::len).sum();

        let done = PrimaryMessage::VantageReplayDone(end_key, complete, clamped, self.name);

        // Insert before enqueue so immediate completion can remove this generation.
        let generation = self.wire.next_replay_generation();
        self.wire.in_flight.lock().insert(
            sender,
            InFlightEntry {
                started: now,
                generation,
            },
        );
        let enqueued = self.wire.enqueue_replay(sender, generation, msgs, done);

        if !enqueued {
            // Failed admission leaves the peer floor unchanged.
            wire::remove_in_flight_generation(&self.wire.in_flight, sender, generation);
            log::debug!(
                "vantage node: resume serve suppressed: sender={} served_from={} gate=enqueue-failed",
                sender,
                served_from
            );
            return;
        }
        self.serve_budget.record(sender, served_bytes, now, backoff);
        log::debug!(
            "vantage node: resume serve decision: sender={} served_from={} msgs={} end_key={} complete={} clamped={}",
            sender,
            served_from,
            msg_count,
            end_key,
            complete,
            clamped
        );
        if complete {
            self.pending_low.remove(&sender);
        } else {
            // Partial service advances the floor monotonically.
            self.pending_low
                .entry(sender)
                .and_modify(|existing| *existing = (*existing).max(end_key))
                .or_insert(end_key);
        }
        self.nudge_memo.record(sender, now);
        if clamped {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_replay_done_clamped_total.inc();
            }
        }
    }

    /// Closes complete replay episodes and immediately continues incomplete ones.
    async fn on_replay_done(
        &mut self,
        end_key: View,
        complete: bool,
        clamped: bool,
        sender: PublicKey,
        now: Instant,
    ) {
        log::debug!(
            "vantage node: resume done received: sender={} end_key={} complete={}",
            sender,
            end_key,
            complete
        );
        if clamped {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_replay_done_clamped_total.inc();
            }
        }
        if complete {
            self.replay_episodes.close(&sender);
        } else {
            self.replay_episodes.open(sender, now);
            self.send_resume_hello(sender, now, "event").await;
        }
    }

    /// Fires due AGB timers in deadline order and discards already-satisfied timers.
    fn fire_agb_timers(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        while let Some(&Reverse((d, view, kind))) = self.timers.peek() {
            if d > now {
                break;
            }
            self.timers.pop();
            let moot = match kind {
                TimerKind::EchoFallback | TimerKind::EchoAbsolute => self.agb.echo_sent(view),
                TimerKind::ReadyAbsolute => self.agb.ready_sent(view),
            };
            if moot {
                continue;
            }
            match kind {
                TimerKind::EchoFallback => effects.extend(self.agb.on_echo_fallback_timer(
                    view,
                    &mut self.lm,
                    &mut self.rep,
                )),
                TimerKind::EchoAbsolute => {
                    effects.extend(self.agb.on_echo_absolute_timer(view, &mut self.rep))
                }
                TimerKind::ReadyAbsolute => effects.extend(self.agb.on_ready_timer(view)),
            }
        }
        effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
        effects
    }

    /// Fires due control timers in deadline order and discards obsolete rounds.
    fn fire_control_timers(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        while let Some(&Reverse((d, round))) = self.control_timers.peek() {
            if d > now {
                break;
            }
            self.control_timers.pop();
            if round != self.control.curr_round() || self.control.voted() {
                continue;
            }
            effects.extend(self.control.on_control_round_timer(round));
        }
        effects
    }

    fn sample_metrics(&mut self) {
        if self.metrics.is_some() {
            let (chain_direct, fails) = {
                let blocks = self.lm.blocks_handle();
                let blocks = blocks.lock();
                (blocks.walk_steps(), blocks.walk_failures())
            };
            let now = (chain_direct.0, chain_direct.1, self.rep.walk_steps_settle());
            let prev = self.walk_steps_published;
            let prev_fails = self.walk_fails_published;
            if let Some(metrics) = &self.metrics {
                for (family, cur, was) in [
                    ("chain", now.0, prev.0),
                    ("direct", now.1, prev.1),
                    ("settle", now.2, prev.2),
                ] {
                    metrics
                        .vantage_walk_steps_total
                        .with_label_values(&[family])
                        .inc_by(cur.saturating_sub(was));
                }
                for (family, cur, was) in [
                    ("chain", fails.0, prev_fails.0),
                    ("direct", fails.1, prev_fails.1),
                ] {
                    for (i, branch) in ["missing", "pinned", "gate"].iter().enumerate() {
                        metrics
                            .vantage_walk_failures_total
                            .with_label_values(&[family, branch])
                            .inc_by(cur[i].saturating_sub(was[i]));
                    }
                }
            }
            self.walk_steps_published = now;
            self.walk_fails_published = fails;
        }
        let Some(metrics) = &self.metrics else { return };
        metrics
            .vantage_entered_view
            .set(self.pacemaker.entered_view() as i64);
        metrics
            .vantage_own_watermark
            .set(self.pacemaker.own_watermark() as i64);
        metrics
            .vantage_entry_target
            .set(self.pacemaker.entry_target() as i64);
        metrics.vantage_omega_q.set(self.pacemaker.omega_q() as i64);
        metrics.vantage_frontier_a_i.set(self.frontier.a_i() as i64);
        metrics
            .vantage_pending_gate_len
            .set(self.agb.pending_gate_len() as i64);
        metrics
            .vantage_pending_settle_len
            .set(self.rep.pending_settle_len() as i64);
        metrics
            .vantage_pending_body_fetch_len
            .set(self.digest_stmts.pending_fetch_len() as i64);
        metrics
            .vantage_block_cache_len
            .set(self.lm.block_cache_len() as i64);

        {
            let aggregator = self.ack_aggregator.lock();
            metrics
                .vantage_ack_senders_tracked
                .set(aggregator.senders_tracked() as i64);
            metrics
                .vantage_ack_refs_retired
                .set(aggregator.refs_retired() as i64);
        }
        metrics
            .vantage_cursor_next_view
            .set(self.cursor.next_view() as i64);
        metrics
            .vantage_cursor_forked_entries_dropped
            .set(self.cursor.forked_dropped() as i64);

        if let Some(store) = &self.sequence {
            metrics
                .vantage_sequence_head_view
                .set(store.head_view() as i64);
        }
        if let Some(install) = &self.sequence_install {
            let blocks = self.rep.blocks();
            metrics
                .vantage_sequence_install_blocks_awaited
                .set(install.blocks_awaited(&blocks) as i64);
        } else {
            metrics.vantage_sequence_install_blocks_awaited.set(0);
        }
        metrics
            .vantage_control_round
            .set(self.control.curr_round() as i64);
        metrics
            .vantage_control_delivered_len
            .set(self.control.delivered_log_len() as i64);
        metrics
            .vantage_control_consume_pos
            .set(self.control.consume_pos() as i64);

        log::debug!(
            "vantage node: timers.len()={} control_timers.len()={} cancel_handlers.len()={}",
            self.timers.len(),
            self.control_timers.len(),
            self.wire.cancel_handlers.len()
        );
    }

    fn collect_internal_garbage(&mut self) {
        // Outbox retention is independent of the resolver GC floor.
        let outbox_floor = self
            .pacemaker
            .own_watermark()
            .saturating_sub(self.replay_history_views)
            .max(1);
        self.outbox.prune_below(outbox_floor);

        // Advance floors below retained history.
        for pending in self.pending_low.values_mut() {
            if *pending < outbox_floor {
                *pending = outbox_floor;
            }
        }

        let floor = self.resolver.gc_floor(self.gc_window);
        if floor <= self.last_gc_floor {
            return;
        }

        let serve_floor = floor
            .saturating_sub(
                self.gc_window
                    .saturating_mul(ControlLog::SERVE_MARGIN_WINDOWS),
            )
            .max(1);
        self.agb.gc_below(floor);
        self.digest_stmts.gc_below(floor);
        self.frontier.gc_below(floor);

        self.control.gc_below(floor, serve_floor);
        self.resolver.gc_below(floor);
        self.timers.retain(|Reverse((_, view, _))| *view >= floor);
        self.last_gc_floor = floor;
        log::debug!(
            "vantage node: internal GC floor advanced to {} (serve floor {}, resolved_watermark={}, gc_window={})",
            floor,
            serve_floor,
            self.resolver.resolved_watermark(),
            self.gc_window
        );
    }

    /// Proposes eligible owned views in increasing order through the early-wish bound.
    fn try_propose_effects(&mut self, now: Instant) -> Vec<Effect> {
        if self.sequence_sync_recovery_active
            || self.sequence_install.is_some()
            || self.large_sequence_sync_target().is_some()
        {
            return Vec::new();
        }
        let mut effects = Vec::new();
        let bound = std::cmp::max(self.frontier.a_i() + 1, self.pacemaker.omega_plus());
        let mut view = self.frontier.a_i() + 1;
        while view <= bound {
            if self.agb.proposer(view) != self.name || self.frontier.already_proposed(view) {
                view += 1;
                continue;
            }

            let entries: Vec<crate::vantage::ResolutionEntry> = {
                let agb = &self.agb;
                let control = &self.control;
                let resolved = |u: View| agb.is_sealed(u) || control.is_anchor_resolved(u);
                self.resolver.decide_prefix(agb, view, now, resolved)
            };
            let proposal = match entries.len() {
                0 => self
                    .frontier
                    .propose_view(view, &self.lm, None)
                    .map(ProposalOut::Single),
                1 => self
                    .frontier
                    .propose_view(view, &self.lm, entries.into_iter().next())
                    .map(ProposalOut::Single),
                _ => self
                    .frontier
                    .propose_view_batch(view, &self.lm, entries)
                    .map(ProposalOut::Batch),
            };
            if let Some(proposal) = proposal {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_own_proposals_made_total.inc();
                }
                effects.push(Effect::BroadcastPropose(proposal.clone()));
                effects.extend(match proposal {
                    ProposalOut::Single(p) => {
                        self.agb
                            .on_propose(self.name, p, now, &mut self.lm, &mut self.rep)
                    }
                    ProposalOut::Batch(p) => {
                        self.agb
                            .on_propose_batch(self.name, p, now, &mut self.lm, &mut self.rep)
                    }
                });
            }
            view += 1;
        }
        effects
    }

    /// Records one terminal view in order and compares an awaited verified target once.
    fn record_sequence(&mut self, view: View, outcome: &SequenceOutcome, output_delta: &[Digest]) {
        self.resolver.note_resolved_through(view);

        if self.agb.proposer(view) == self.name {
            log::debug!(
                "vantage sequence: own proposer turn view={} outcome={}",
                view,
                if matches!(outcome, SequenceOutcome::Skip) {
                    "skip"
                } else {
                    "committed"
                }
            );
            if let Some(metrics) = &self.metrics {
                metrics.vantage_own_proposer_turns_total.inc();
                if matches!(outcome, SequenceOutcome::Skip) {
                    if self.frontier.already_proposed(view) {
                        metrics.vantage_own_proposals_skipped_total.inc();
                    }
                } else {
                    metrics.vantage_own_proposals_committed_total.inc();
                }
            }
        }

        let sid_label = head_hex(self.agb.sid());

        let awaited = match &self.sequence_verified_target {
            Some((target_view, head)) if *target_view == view => Some(head.clone()),
            _ => None,
        };
        let Some(store) = self.sequence.as_mut() else {
            return;
        };
        match store.record(view, outcome, output_delta) {
            Ok(head) => {
                let local_head = head.clone();
                let boundary = store.latest_boundary().map(|(v, h)| (v, h.clone()));
                if let Some(metrics) = &self.metrics {
                    metrics
                        .vantage_sequence_delta_digests_total
                        .inc_by(output_delta.len() as u64);
                    metrics.vantage_sequence_records_total.inc();
                }

                if let Some((boundary_view, head)) = boundary {
                    if boundary_view == view {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_sequence_boundary_view.set(view as i64);

                            metrics.vantage_sequence_boundary_head.reset();
                            metrics
                                .vantage_sequence_boundary_head
                                .with_label_values(&[&sid_label, &head_hex(&head)])
                                .set(view as i64);
                        }
                        log::debug!(
                            "vantage sequence checkpoint: view={view} head={}",
                            head_hex(&head)
                        );
                    }
                }
                if let Some(expected) = awaited {
                    self.sequence_verified_target = None;

                    self.sequence_install = None;
                    let matched = expected == local_head;
                    let installed = std::mem::take(&mut self.sequence_target_installed);
                    if let Some(metrics) = &self.metrics {
                        if !matched {
                            metrics.vantage_sequence_verify_mismatch_total.inc();
                        } else if installed {
                            metrics.vantage_sequence_install_selfcheck_match_total.inc();
                        } else {
                            metrics.vantage_sequence_verify_match_total.inc();
                        }
                    }
                    if matched {
                        let basis = if installed {
                            "installed -- self-consistency only"
                        } else {
                            "independently executed locally"
                        };
                        log::debug!(
                            "vantage sequence sync: MATCH at view={view} head={} ({basis})",
                            head_hex(&local_head)
                        );
                    } else {
                        log::error!(
                            "vantage sequence sync: MISMATCH at view={view}: \
                             verified={} local={}",
                            head_hex(&expected),
                            head_hex(&local_head)
                        );
                    }
                }
            }
            Err(e) => {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_record_rejected_total.inc();
                }
                log::error!("vantage sequence: {e}");
            }
        }
    }

    /// Advances the single active transfer without replacing it with newer announcements.
    fn drive_sequence_sync(&mut self) {
        if self.sequence_sync.is_none() {
            return;
        }
        let local_view = self.sequence.as_ref().map(|s| s.head_view()).unwrap_or(0);
        let local_head = self
            .sequence
            .as_ref()
            .map(|s| s.head().clone())
            .unwrap_or_else(|| genesis_head(self.agb.sid()));

        match self.sequence_transfer.as_ref().map(|t| t.state()) {
            Some(TransferState::Verified) => {
                let transfer = self.sequence_transfer.as_ref().expect("present");
                let staged: Vec<(View, SequenceOutcome, Vec<Digest>)> = transfer
                    .verified_output()
                    .map(|o| {
                        o.into_iter()
                            .map(|(v, outcome, delta)| (v, outcome.clone(), delta.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                let heads = transfer.verified_heads().unwrap_or_default();
                let sources = transfer.next_sources(usize::MAX);
                let views = staged.len();
                let (view, head) = transfer.target();
                let (view, head) = (view, head.clone());
                self.sequence_verified_target = Some((view, head.clone()));

                let install = SequenceInstall::new(
                    local_view,
                    view,
                    head,
                    staged,
                    heads,
                    self.sequence_install_window_views,
                    self.sequence_install_settle_ceiling,
                );
                if install.is_contiguous() {
                    if let Some(metrics) = &self.metrics {
                        metrics.vantage_sequence_install_staged_total.inc();
                        metrics
                            .vantage_sequence_install_views
                            .set(install.views_total() as i64);
                    }

                    let tips = install.lane_tips();
                    for (author, height) in tips {
                        for peer in &sources {
                            self.rep.note_holder(*peer, author, height);
                        }
                    }
                    self.sequence_block_requests.clear();
                    self.sequence_install_sources = sources;
                    self.sequence_install = Some(install);
                    self.sequence_install_ready_logged = false;
                    self.sequence_target_installed = false;
                } else {
                    self.sequence_install_sources.clear();
                    if let Some(metrics) = &self.metrics {
                        metrics.vantage_sequence_install_rejected_total.inc();
                    }
                    log::error!(
                        "vantage sequence install: verified target view={view} is not \
                         contiguous above local view={local_view}; refusing to stage"
                    );
                }
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_sync_verified_total.inc();
                    metrics.vantage_sequence_sync_verified_view.set(view as i64);
                }
                let install_mode = if self.sequence_install_enabled {
                    "staged for install"
                } else {
                    "install disabled; awaiting local execution"
                };
                log::info!(
                    "vantage sequence sync: VERIFIED target view={view} ({views} views); \
                     {install_mode}"
                );
                self.sequence_transfer = None;
                self.sequence_request_at = None;
                self.sequence_last_want = None;
            }
            Some(TransferState::Exhausted) => {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_sync_exhausted_total.inc();
                }
                self.sequence_transfer = None;
                self.sequence_request_at = None;
                self.sequence_last_want = None;
            }
            _ => {}
        }

        if self
            .sequence_transfer
            .as_ref()
            .map(|t| t.target().0 <= local_view)
            .unwrap_or(false)
        {
            let target = self.sequence_transfer.as_ref().expect("present").target().0;
            log::debug!(
                "vantage sequence sync: ordinary cursor passed target view={target}; aborting"
            );
            self.sequence_transfer = None;
            self.sequence_request_at = None;
            self.sequence_last_want = None;
        }

        if self.sequence_transfer.is_none() {
            let Some(collector) = self.sequence_sync.as_ref() else {
                return;
            };
            let verified_view = self
                .sequence_verified_target
                .as_ref()
                .map(|(view, _)| *view)
                .unwrap_or(0);

            if verified_view > local_view {
                return;
            }
            let Some((view, head)) = collector.certified_head(local_view) else {
                return;
            };
            let gap = view.saturating_sub(local_view);

            if self.sequence_sync_recovered {
                if gap < self.sequence_sync_rearm_gap_views {
                    return;
                }
                log::info!(
                    "vantage sequence sync: re-arming after a {gap}-view gap (>= {})",
                    self.sequence_sync_rearm_gap_views
                );
                self.sequence_sync_recovered = false;

                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_sync_recovered.set(0);
                }
            }
            if !self.sequence_sync_recovery_active && gap < self.sequence_sync_min_gap_views {
                return;
            }
            let sources = collector.announcers(view, &head);
            if sources.is_empty() {
                return;
            }
            self.sequence_transfer_seq += 1;
            let id = self.sequence_transfer_seq;
            self.sequence_transfer = Some(SequenceTransfer::new(
                self.agb.sid().clone(),
                id,
                local_view,
                local_head,
                view,
                head,
                sources,
            ));
            self.sequence_request_at = None;
            self.sequence_last_want = None;
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_sync_started_total.inc();
                metrics.vantage_sequence_sync_target_view.set(view as i64);
            }
        }

        let now = Instant::now();
        let timed_out = self
            .sequence_request_at
            .map(|at| {
                now.duration_since(at) >= Duration::from_millis(self.sequence_request_timeout_ms)
            })
            .unwrap_or(true);
        if !timed_out {
            return;
        }
        if self.sequence_request_at.is_some() {
            if let Some(t) = self.sequence_transfer.as_mut() {
                t.rotate();
            }
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_sync_timeouts_total.inc();
            }
        }
        self.emit_sequence_requests();
    }

    /// Admits and applies verified views under independent validation and installation budgets.
    async fn drive_sequence_install(&mut self, now: Instant) -> Vec<Effect> {
        if self.sequence_install.is_none() {
            self.sequence_block_requests.clear();
            self.sequence_install_sources.clear();
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_install_views.set(0);
                metrics.vantage_sequence_install_views_ready.set(0);
                metrics.vantage_sequence_install_views_in_flight.set(0);
                metrics.vantage_sequence_install_blocks_awaited.set(0);
                metrics
                    .vantage_sequence_install_header_requests_in_flight
                    .set(0);
            }
            return Vec::new();
        }
        let blocks = self.rep.blocks();
        let retry_headers = self
            .sequence_install
            .as_ref()
            .map(|install| install.payload_retry_headers(&blocks, 64))
            .unwrap_or_default();
        let mut effects = Vec::new();
        for header in retry_headers {
            let missing = self.lm.missing_payload(&header).await;
            if missing.is_empty() {
                effects.extend(self.lm.set_payload_ready(&header.id));
            } else {
                self.payload
                    .sync_batches(&mut self.wire, header.author, header.id.clone(), missing)
                    .await;
            }
        }

        if !self.rebase_sequence_install() {
            return Vec::new();
        }
        let validation_budget = self.sequence_install_digests_per_tick.max(1);
        let install = self.sequence_install.as_mut().expect("present");
        let examined = install.refresh_budgeted(&blocks, validation_budget);
        let refs = install.admit(self.rep.pending_settle_len());

        let (complete, total, in_flight) = (
            install.views_complete(),
            install.views_total(),
            install.views_in_flight(),
        );
        let staged_ready = complete == total;
        let target = install.target().0;

        for r in refs {
            effects.extend(self.rep.authorize(r));
        }
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_sequence_install_views_ready
                .set(complete as i64);
            metrics
                .vantage_sequence_install_views_in_flight
                .set(in_flight as i64);
        }
        if staged_ready && !self.sequence_install_ready_logged {
            self.sequence_install_ready_logged = true;
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_install_ready_total.inc();
            }
            log::debug!(
                "vantage sequence install: all {total} views of target view={target} are \
                 locally held"
            );
        }
        effects.extend(self.apply_sequence_install(validation_budget - examined, now));
        self.drive_sequence_block_fetch(now).await;
        effects
    }

    /// Fetches missing verified headers in bounded batches from certified sources.
    async fn drive_sequence_block_fetch(&mut self, now: Instant) {
        if self.sequence_install.is_none() {
            self.sequence_block_requests.clear();
            return;
        }
        let sources = self.sequence_install_sources.clone();
        if sources.is_empty() {
            return;
        }

        let blocks = self.rep.blocks();
        let missing = self
            .sequence_install
            .as_ref()
            .expect("checked")
            .missing_digests(&blocks, SEQUENCE_BLOCK_MAX_IN_FLIGHT);
        let missing_set: HashSet<Digest> = missing.iter().cloned().collect();
        self.sequence_block_requests
            .retain(|digest, _| missing_set.contains(digest));

        let timeout = Duration::from_millis(self.sequence_request_timeout_ms);
        let mut by_source: HashMap<PublicKey, Vec<Digest>> = HashMap::new();
        let mut scheduled = 0usize;

        // Rotate timed-out requests even when the in-flight window is full.
        for digest in &missing {
            if scheduled >= SEQUENCE_BLOCK_MAX_IN_FLIGHT {
                break;
            }
            let Some(state) = self.sequence_block_requests.get_mut(digest) else {
                continue;
            };
            if now.duration_since(state.requested_at) < timeout {
                continue;
            }
            state.source_cursor = (state.source_cursor + 1) % sources.len();
            state.requested_at = now;
            by_source
                .entry(sources[state.source_cursor])
                .or_default()
                .push(digest.clone());
            scheduled += 1;
        }

        // Refill after half the request window drains to preserve batching.
        if self.sequence_block_requests.len() <= SEQUENCE_BLOCK_REFILL_AT {
            let room =
                SEQUENCE_BLOCK_MAX_IN_FLIGHT.saturating_sub(self.sequence_block_requests.len());
            let mut added = 0usize;
            for digest in missing {
                if added >= room {
                    break;
                }
                if self.sequence_block_requests.contains_key(&digest) {
                    continue;
                }
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&digest.0[..8]);
                let source_cursor = (u64::from_le_bytes(prefix) as usize) % sources.len();
                self.sequence_block_requests.insert(
                    digest.clone(),
                    SequenceBlockRequestState {
                        requested_at: now,
                        source_cursor,
                    },
                );
                self.rep.expect_sequence_digest(digest.clone());
                by_source
                    .entry(sources[source_cursor])
                    .or_default()
                    .push(digest);
                scheduled += 1;
                added += 1;
            }
        }

        for (source, digests) in by_source {
            for chunk in digests.chunks(SEQUENCE_BLOCK_REQUEST_BATCH) {
                self.send_sequence(
                    &source,
                    PrimaryMessage::VantageSequenceHeadersRequest(chunk.to_vec(), self.name),
                );
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_sequence_install_headers_requested_total
                .inc_by(scheduled as u64);
            metrics
                .vantage_sequence_install_header_requests_in_flight
                .set(self.sequence_block_requests.len() as i64);
        }
    }

    /// Aligns the staged suffix with the current cursor and rejects a divergent base.
    fn rebase_sequence_install(&mut self) -> bool {
        let local_view = self.cursor.next_view().saturating_sub(1);
        let local_head = self
            .sequence
            .as_ref()
            .map(|s| s.head().clone())
            .unwrap_or_else(|| genesis_head(self.agb.sid()));
        let Some(install) = self.sequence_install.as_mut() else {
            return false;
        };
        match install.rebase(local_view, &local_head) {
            RebaseOutcome::Continue => true,
            RebaseOutcome::Overtaken => {
                log::debug!(
                    "vantage sequence install: ordinary execution reached view={local_view}; \
                     target retired without installing"
                );
                self.sequence_install = None;
                let _ = self.cursor.abort_install();
                false
            }
            RebaseOutcome::Diverged {
                view,
                expected,
                local,
            } => {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_install_failed_total.inc();
                    metrics.vantage_sequence_verify_mismatch_total.inc();
                }
                log::error!(
                    "vantage sequence install: MISMATCH at rebase view={view}: \
                     verified={} local={}; abandoning target",
                    head_hex(&expected),
                    head_hex(&local)
                );
                self.sequence_install = None;
                self.sequence_verified_target = None;
                let _ = self.cursor.abort_install();
                false
            }
        }
    }

    /// Applies complete staged views and aborts the target on any refusal.
    fn apply_sequence_install(&mut self, digest_budget: usize, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        if !self.sequence_install_enabled || digest_budget == 0 {
            return effects;
        }
        let mut applied = 0usize;

        // The digest budget bounds work when one view contains a large lane suffix.
        let mut digests_left = digest_budget;
        while applied < self.sequence_install_views_per_tick && digests_left > 0 {
            let Some(install) = self.sequence_install.as_ref() else {
                break;
            };
            let Some(view) = install.installable() else {
                break;
            };
            let Some((outcome, delta)) = install.view_output(view) else {
                break;
            };
            let outcome = outcome.clone();
            match self
                .cursor
                .install_budgeted(view, outcome, delta, digests_left)
            {
                Ok((fx, finalized, examined)) => {
                    effects.extend(fx);
                    digests_left = digests_left.saturating_sub(examined);

                    self.sequence_target_installed = true;
                    if !finalized {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_sequence_install_partial_views_total.inc();
                        }
                        break;
                    }
                    let blocks = self.rep.blocks();
                    self.sequence_install
                        .as_mut()
                        .expect("present")
                        .mark_installed(view, &blocks);
                    applied += 1;
                }
                Err(e) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.vantage_sequence_install_failed_total.inc();
                    }
                    log::error!("vantage sequence install: {e}; abandoning target");
                    self.sequence_install = None;
                    self.sequence_verified_target = None;
                    effects.extend(self.cursor.abort_install());
                    return effects;
                }
            }
        }

        // Deferred ordinary inputs resume after the bounded installation batch.
        if self.cursor.next_view()
            > self
                .sequence_install
                .as_ref()
                .map(|i| i.target().0)
                .unwrap_or(View::MAX)
        {
            effects.extend(self.cursor.retry());
        }
        if applied > 0 {
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_sequence_install_views_applied_total
                    .inc_by(applied as u64);
            }
            let done = self
                .sequence_install
                .as_ref()
                .map(|i| i.is_done())
                .unwrap_or(false);
            if done {
                let install = self.sequence_install.as_ref().expect("present");
                let target = install.target().0;
                let own_anchor = install.installed_lane_tip(&self.name);
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_install_completed_total.inc();
                    metrics
                        .vantage_sequence_install_completed_view
                        .set(target as i64);
                }

                self.resolver.note_resolved_through(target);

                if let Some(anchor) = own_anchor {
                    effects.push(Effect::RecoverOwnLane(anchor));
                }

                // Enter only the first live view; installed history must not replay effects.
                let next_live = target.saturating_add(1);
                self.pacemaker.fast_forward_installed_entry(next_live);
                effects.extend(self.enter_view_effects(next_live, now));

                log::info!("vantage sequence install: applied through view={target}");
            }
        }
        effects
    }

    /// Requests the same transfer item from each selected source concurrently.
    fn emit_sequence_requests(&mut self) {
        let Some(transfer) = self.sequence_transfer.as_ref() else {
            return;
        };
        let Some(want) = transfer.want() else {
            return;
        };
        let (target_view, target_head) = transfer.target();
        let (target_head, id) = (target_head.clone(), transfer.transfer_id());
        let sources = transfer.next_sources(self.sequence_max_sources);
        let records_cap = self.sequence_chunk_records as u32;
        let outcomes_cap = self.sequence_chunk_outcomes as u32;
        let outcome_items_cap = self.sequence_chunk_outcome_items as u32;
        let digests_cap = self.sequence_chunk_digests as u32;
        let me = self.name;
        for peer in sources {
            let message = match &want {
                SequenceWant::Records { from_view } => {
                    PrimaryMessage::VantageSequenceRequest(SequenceRequest {
                        version: SEQUENCE_VERSION,
                        transfer_id: id,
                        target_view,
                        target_head: target_head.clone(),
                        from_view: *from_view,
                        max_records: records_cap,
                        requester: me,
                    })
                }
                SequenceWant::Outcomes { from_view } => {
                    PrimaryMessage::VantageSequenceOutcomeRequest(SequenceOutcomeRequest {
                        version: SEQUENCE_VERSION,
                        transfer_id: id,
                        target_head: target_head.clone(),
                        target_view,
                        from_view: *from_view,
                        max_views: outcomes_cap,
                        max_items: outcome_items_cap,
                        requester: me,
                    })
                }
                SequenceWant::Deltas {
                    from_view,
                    start_index,
                } => PrimaryMessage::VantageSequenceDeltaRangeRequest(SequenceDeltaRangeRequest {
                    version: SEQUENCE_VERSION,
                    transfer_id: id,
                    target_head: target_head.clone(),
                    target_view,
                    from_view: *from_view,
                    start_index: *start_index,
                    max_views: SEQUENCE_DELTA_RANGE_VIEWS as u32,
                    max_items: digests_cap,
                    requester: me,
                }),
            };
            self.send_sequence(&peer, message);
        }
        self.sequence_request_at = Some(Instant::now());
        self.sequence_last_want = Some(want);
    }

    /// Accepts selected-source responses for the active request.
    fn on_sequence_response(&mut self, response: SequenceResponse, from: &PublicKey) {
        let Some(transfer) = self.sequence_transfer.as_mut() else {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_sync_unsolicited_total.inc();
            }
            return;
        };
        let result = match &response {
            SequenceResponse::Records(chunk) => transfer.on_records(chunk, from),
            SequenceResponse::Outcome(serve) => transfer.on_outcomes(serve, from),
            SequenceResponse::Delta(chunk) => transfer.on_delta(chunk, from),
            SequenceResponse::DeltaRange(chunk) => transfer.on_delta_range(chunk, from),
            SequenceResponse::Unavailable(u) => {
                transfer.on_unavailable(u, from);
                Ok(())
            }
        };
        if let Some(metrics) = &self.metrics {
            match &result {
                Ok(()) => metrics.vantage_sequence_sync_chunks_total.inc(),
                Err(_) => metrics.vantage_sequence_sync_invalid_total.inc(),
            }
        }
        if let Err(e) = result {
            log::debug!("vantage sequence sync: invalid chunk from a source: {e:?}");
        }

        let want = self.sequence_transfer.as_ref().and_then(|t| t.want());
        if want != self.sequence_last_want {
            self.sequence_request_at = None;
            self.drive_sequence_sync();
        }
    }

    /// Repeats bounded, serveable checkpoint suffixes so late nodes can certify them.
    fn announce_checkpoint(&mut self) {
        let Some(store) = self.sequence.as_ref() else {
            return;
        };
        let Some((view, _)) = store.latest_boundary() else {
            return;
        };
        let now = Instant::now();
        let repeat = Duration::from_millis(self.sequence_announce_repeat_ms);
        let due = match self.last_announced {
            Some((last_view, at)) => last_view != view || now.duration_since(at) >= repeat,
            None => true,
        };
        if !due {
            return;
        }
        self.last_announced = Some((view, now));
        let serve_floor = store.serve_floor();
        let announcements: Vec<_> = store
            .recent_boundaries(SEQUENCE_ANNOUNCE_BOUNDARIES)
            .into_iter()
            .map(|(view, head)| SequenceAnnouncement {
                version: SEQUENCE_VERSION,
                view,
                head,
                serve_floor,
                sender: self.name,
            })
            .collect();
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_sequence_announced_total
                .inc_by(announcements.len() as u64);
        }

        let peers: Vec<_> = self
            .members
            .iter()
            .copied()
            .filter(|peer| peer != &self.name)
            .collect();
        for peer in peers {
            self.send_sequence(
                &peer,
                PrimaryMessage::VantageSequenceAnnounceBatch(announcements.clone(), self.name),
            );
        }
    }

    /// Counts first-hand checkpoint announcements without changing live consensus state.
    fn on_sequence_announce(&mut self, announcement: &SequenceAnnouncement, sender: &PublicKey) {
        let Some(collector) = self.sequence_sync.as_mut() else {
            return;
        };
        let is_member = self.members.contains(sender);
        let local = self.cursor.next_view();
        let outcome = collector.on_announcement(announcement, sender, is_member, local);
        if let Some(metrics) = &self.metrics {
            match outcome {
                crate::vantage::sequence::AnnouncementOutcome::Counted { newly_certified } => {
                    metrics.vantage_sequence_announce_counted_total.inc();
                    if newly_certified {
                        metrics.vantage_sequence_certified_total.inc();
                        metrics
                            .vantage_sequence_certified_view
                            .set(announcement.view as i64);
                    }
                }
                crate::vantage::sequence::AnnouncementOutcome::Ignored(_) => {
                    metrics.vantage_sequence_announce_ignored_total.inc();
                }
            }
            metrics
                .vantage_sequence_equivocators
                .set(collector.equivocator_count() as i64);
        }
    }

    /// Returns an explicit unavailable response instead of clamping below the serve floor.
    fn serve_sequence_records(&mut self, request: &SequenceRequest, to: &PublicKey) {
        let Some(store) = &self.sequence else {
            return;
        };
        let floor = store.serve_floor();
        let max = self
            .sequence_chunk_records
            .min(request.max_records as usize);
        let records = if request.from_view < floor {
            Vec::new()
        } else {
            store.records_from(request.from_view, max)
        };
        let message = if records.is_empty() {
            PrimaryMessage::VantageSequenceUnavailable(SequenceUnavailable {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                serve_floor: floor,
                sender: self.name,
            })
        } else {
            PrimaryMessage::VantageSequenceRecords(SequenceRecordChunk {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                records,
                serve_floor: floor,
                sender: self.name,
            })
        };
        self.send_sequence(to, message);
    }

    fn serve_sequence_delta(&mut self, request: &SequenceDeltaRequest, to: &PublicKey) {
        let Some(store) = &self.sequence else {
            return;
        };
        let floor = store.serve_floor();
        let max = self.sequence_chunk_digests.min(request.max_items as usize);
        let message = match store.delta_chunk(request.view, request.start_index, max) {
            Some((items, complete)) => PrimaryMessage::VantageSequenceDelta(SequenceDeltaChunk {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                view: request.view,
                start_index: request.start_index,
                items,
                complete,
                sender: self.name,
            }),
            None => PrimaryMessage::VantageSequenceUnavailable(SequenceUnavailable {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                serve_floor: floor,
                sender: self.name,
            }),
        };
        self.send_sequence(to, message);
    }

    fn serve_sequence_delta_range(&mut self, request: &SequenceDeltaRangeRequest, to: &PublicKey) {
        let Some(store) = &self.sequence else {
            return;
        };
        let floor = store.serve_floor();
        let max_views = (request.max_views as usize).max(1);
        let max_items = self
            .sequence_chunk_digests
            .min(request.max_items as usize)
            .max(1);
        let entries = if request.from_view < floor {
            Vec::new()
        } else {
            store.delta_entries_from(
                request.from_view,
                request.start_index,
                request.target_view,
                max_views,
                max_items,
            )
        };
        let message = if entries.is_empty() {
            PrimaryMessage::VantageSequenceUnavailable(SequenceUnavailable {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                serve_floor: floor,
                sender: self.name,
            })
        } else {
            PrimaryMessage::VantageSequenceDeltaRange(SequenceDeltaRangeChunk {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                entries,
                sender: self.name,
            })
        };
        self.send_sequence(to, message);
    }

    fn serve_sequence_outcome(&mut self, request: &SequenceOutcomeRequest, to: &PublicKey) {
        let Some(store) = &self.sequence else {
            return;
        };
        let floor = store.serve_floor();
        let max_views = self
            .sequence_chunk_outcomes
            .min(request.max_views as usize)
            .max(1);
        let max_items = self
            .sequence_chunk_outcome_items
            .min(request.max_items as usize)
            .max(1);
        let outcomes = if request.from_view < floor {
            Vec::new()
        } else {
            store.outcomes_from(request.from_view, request.target_view, max_views, max_items)
        };
        let message = if outcomes.is_empty() {
            PrimaryMessage::VantageSequenceUnavailable(SequenceUnavailable {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                serve_floor: floor,
                sender: self.name,
            })
        } else {
            PrimaryMessage::VantageSequenceOutcome(SequenceOutcomeServe {
                version: SEQUENCE_VERSION,
                transfer_id: request.transfer_id,
                target_head: request.target_head.clone(),
                outcomes,
                sender: self.name,
            })
        };
        self.send_sequence(to, message);
    }

    fn verified_sequence_headers(&self, digests: &[Digest]) -> Vec<Header> {
        let blocks = self.rep.blocks();
        {
            let cache = blocks.lock();
            digests
                .iter()
                .filter_map(|digest| {
                    self.sequence
                        .as_ref()
                        .and_then(|store| store.retained_header(digest))
                        .cloned()
                        .or_else(|| {
                            cache.get(digest).and_then(|entry| {
                                entry.block_ok_verified.then(|| entry.block.clone())
                            })
                        })
                })
                .collect()
        }
    }

    /// Serves verified headers retained by the sequence or live block cache.
    fn serve_sequence_headers(&mut self, digests: &[Digest], to: &PublicKey) {
        let headers = self.verified_sequence_headers(digests);
        for chunk in headers.chunks(SEQUENCE_BLOCK_SERVE_BATCH) {
            self.send_sequence(
                to,
                PrimaryMessage::VantageSequenceHeaders(chunk.to_vec(), self.name),
            );
        }
    }

    /// Sends one best-effort frame on the dedicated sequence transport without replay recording.
    fn send_sequence(&mut self, to: &PublicKey, message: PrimaryMessage) {
        let sent = self.wire.try_send_sequence(to, message);
        if let Some(metrics) = &self.metrics {
            if sent {
                metrics.vantage_sequence_sync_served_total.inc();
            } else {
                metrics.vantage_sequence_sync_dropped_total.inc();
            }
        }
    }

    /// Enters AGB before Frontier, then activates every view exposed by the new floor.
    fn enter_view_effects(&mut self, view: View, now: Instant) -> Vec<Effect> {
        let mut effects = self.agb.enter(view, now, &mut self.lm, &mut self.rep);
        let activated = self.frontier.enter(view);
        for v in activated {
            effects.extend(self.agb.activate(v, &mut self.lm, &mut self.rep));
        }
        effects.extend(self.try_propose_effects(now));
        effects
    }

    /// Schedules an AGB recheck even when availability processing emits no effects.
    fn on_ack_availability(&mut self, availability: AckAvailability, _now: Instant) -> Vec<Effect> {
        self.recheck_pending = true;
        self.lm.process_ack_availability(availability)
    }

    fn record_local_ack(&mut self, ack: &Ack, now: Instant) -> Vec<Effect> {
        let availability = {
            let mut aggregator = self.ack_aggregator.lock();
            aggregator
                .record_ack(self.name, ack.reference())
                .availability
        };
        availability
            .map(|availability| self.on_ack_availability(availability, now))
            .unwrap_or_default()
    }

    fn record_injected_ack(&mut self, ack: Ack, now: Instant) -> Vec<Effect> {
        aggregate_received_ack(&self.ack_aggregator, self.metrics.as_deref(), &ack)
            .map(|availability| self.on_ack_availability(availability, now))
            .unwrap_or_default()
    }

    /// Credits watermark references through the same aggregator as individual acknowledgments.
    fn credit_refs(&mut self, sender: PublicKey, refs: Vec<BlockRef>, now: Instant) -> Vec<Effect> {
        let results = {
            let mut aggregator = self.ack_aggregator.lock();
            refs.iter()
                .cloned()
                .map(|r| aggregator.record_ack(sender, r))
                .collect::<Vec<_>>()
        };
        let mut effects = Vec::new();
        for (r, result) in refs.into_iter().zip(results) {
            self.rep.note_holder(sender, r.0, r.1);
            if !result.accepted {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_rejected_nonmember_total.inc();
                }
                continue;
            }
            if let Some(metrics) = &self.metrics {
                metrics.vantage_avail_credited_refs.inc();
            }
            if let Some(availability) = result.availability {
                effects.extend(self.on_ack_availability(availability, now));
            }
        }
        effects
    }

    /// Rejects nonmembers before any declared sender can affect quorum state.
    async fn dispatch_inbound(&mut self, inbound: Inbound, now: Instant) -> Vec<Effect> {
        if !wire::sender_is_member(&inbound, &self.members) {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_rejected_nonmember_total.inc();
            }
            return Vec::new();
        }
        if self.install_replaces_inbound(&inbound) {
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_sequence_install_obsolete_inbound_dropped_total
                    .inc();
            }
            return Vec::new();
        }
        match inbound {
            Inbound::Publish(sender, header) => {
                let author = header.author;
                let before = self.lm.own_direct_frontier(&author);
                let effects = self.lm.process_publish(sender, header).await;

                if self.lm.own_direct_frontier(&author) > before {
                    self.try_resume_request(author, now);
                }
                effects
            }
            Inbound::Serve(header) => {
                let digest = header.id.clone();
                let effects = self.serve_effects(header).await;
                let accepted = block_was_cached(&effects, &digest);
                if accepted && self.sequence_block_requests.remove(&digest).is_some() {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .vantage_sequence_install_headers_received_total
                            .inc();
                        metrics
                            .vantage_sequence_install_header_requests_in_flight
                            .set(self.sequence_block_requests.len() as i64);
                    }

                    if self.sequence_block_requests.len() <= SEQUENCE_BLOCK_REFILL_AT {
                        self.drive_sequence_block_fetch(now).await;
                    }
                }
                effects
            }
            Inbound::HeadersRequest(digests, requestor) => {
                let mut effects = Vec::new();
                for d in digests {
                    effects.extend(self.rep.on_request(requestor, d));
                }
                effects
            }
            Inbound::AckAvailability(availability) => self.on_ack_availability(availability, now),
            Inbound::Ack(ack) => self.record_injected_ack(ack, now),
            Inbound::Avail(entries, sender) => {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_avail_received.inc();
                }
                let refs = self.lm.resolve_watermark(sender, &entries);
                self.credit_refs(sender, refs, now)
            }
            Inbound::Propose(proposal) => {
                // Sender-less proposals are attributed to the designated proposer for the view.
                let claimed_sender = self.agb.proposer(proposal.view());
                match proposal {
                    ProposalOut::Single(p) => {
                        self.agb
                            .on_propose(claimed_sender, p, now, &mut self.lm, &mut self.rep)
                    }
                    ProposalOut::Batch(p) => self.agb.on_propose_batch(
                        claimed_sender,
                        p,
                        now,
                        &mut self.lm,
                        &mut self.rep,
                    ),
                }
            }

            Inbound::Echo(echo) => {
                // Apply the piggybacked wish before processing the consensus statement.
                let mut effects = self.pacemaker.on_wish(echo.sender(), echo.wish());
                effects.extend(match echo {
                    EchoOut::Single(e) => self.agb.on_echo(e, &mut self.rep),
                    EchoOut::Batch(e) => self.agb.on_echo_batch(e, &mut self.rep),
                });
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));

                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::EchoSkip(view, sender, wish) => {
                let mut effects = self.pacemaker.on_wish(sender, wish);
                effects.extend(self.agb.on_echo_skip(view, sender));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::Ready(ready) => {
                let mut effects = self.pacemaker.on_wish(ready.sender(), ready.wish());
                effects.extend(match ready {
                    ReadyOut::Single(r) => self.agb.on_ready(r, &mut self.rep),
                    ReadyOut::Batch(r) => self.agb.on_ready_batch(r, &mut self.rep),
                });
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::NoReady(view, sender, wish) => {
                let mut effects = self.pacemaker.on_wish(sender, wish);
                effects.extend(self.agb.on_noready(view, sender));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }

            Inbound::Wish(view, sender) => {
                let mut effects = self.pacemaker.on_wish(sender, view);
                effects.extend(self.try_propose_effects(now));
                effects
            }

            Inbound::CompReport(view, digest, sender) => {
                self.control.on_comp_report(view, digest, sender)
            }
            Inbound::ControlInit(proposal, b_w) => {
                // Sender-less control proposals are attributed to the designated round leader.
                let claimed_sender = self.control.control_leader(proposal.round);
                self.control.on_control_init(claimed_sender, proposal, b_w)
            }
            Inbound::ControlEcho(sender, proposal) => {
                self.control.on_control_echo(sender, proposal)
            }
            Inbound::ControlReady(sender, proposal) => {
                self.control.on_control_ready(sender, proposal)
            }
            Inbound::ControlCommit(sender, round) => self.control.on_control_commit(sender, round),
            Inbound::ControlTimeoutVote(sender, round) => {
                self.control.on_control_timeout_vote(sender, round)
            }
            Inbound::ControlTimeoutAccept(sender, round) => {
                self.control.on_control_timeout_accept(sender, round)
            }
            Inbound::ControlFetch(view, digest, requester) => {
                self.control.on_control_fetch(requester, view, digest)
            }
            Inbound::ControlServe(view, proposal) => self.control.on_control_serve(view, proposal),

            Inbound::SkipVote(view, sender) => {
                // A skip vote only adds an exclusion and cannot unblock a pending gate.
                self.agb.on_skip_vote(view, sender)
            }

            Inbound::EchoDigest(msg) => {
                let mut effects = self.pacemaker.on_wish(msg.sender, msg.wish);
                effects.extend(self.digest_stmts.on_echo_digest(
                    msg,
                    now,
                    &mut self.agb,
                    &mut self.rep,
                ));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::ReadyDigest(msg) => {
                let mut effects = self.pacemaker.on_wish(msg.sender, msg.wish);
                effects.extend(self.digest_stmts.on_ready_digest(
                    msg,
                    now,
                    &mut self.agb,
                    &mut self.rep,
                ));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::BodyFetch(view, digest, requester) => self
                .digest_stmts
                .on_body_fetch(requester, view, digest, &self.agb),
            Inbound::BodyServe(view, proposal) => {
                let mut effects =
                    self.digest_stmts
                        .on_body_serve(view, proposal, &mut self.agb, &mut self.rep);
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }

            Inbound::LaneResume(author, from, requester) => {
                // A node serves only its own lane and clamps requests to retained history.
                if author != self.name {
                    return Vec::new();
                }

                let floor = self.lm.earliest_authored_height(&author);
                let from = from.max(floor);
                let tip = self.lm.own_tip_height();
                if from > tip {
                    return Vec::new();
                }
                let backoff = Duration::from_millis(self.resume_backoff_ms);
                if !self
                    .resume_serve
                    .should_serve(requester, from, now, backoff)
                {
                    return Vec::new();
                }
                let to = (from + self.resume_batch - 1).min(tip);
                let mut effects = Vec::with_capacity((to - from + 1) as usize);
                for height in from..=to {
                    if let Some(header) = self.lm.author_block_at(&author, height) {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_lane_resume_blocks_served.inc();
                        }
                        effects.push(Effect::ResumeServeTo(requester, header));
                    }
                }
                effects
            }

            Inbound::ResumeHello(floor, sender) => {
                if self.reconnect_replay {
                    self.on_resume_hello(floor, sender, now).await;
                } else {
                    log::debug!(
                        "vantage node: resume hello ignored: reconnect replay disabled, sender={}",
                        sender
                    );
                }
                Vec::new()
            }

            Inbound::SequenceAnnounce(announcement, sender) => {
                self.on_sequence_announce(&announcement, &sender);
                Vec::new()
            }
            Inbound::SequenceAnnounceBatch(announcements, sender) => {
                for announcement in announcements.into_iter().take(SEQUENCE_ANNOUNCE_BOUNDARIES) {
                    self.on_sequence_announce(&announcement, &sender);
                }
                Vec::new()
            }
            Inbound::SequenceRequest(request, sender) => {
                self.serve_sequence_records(&request, &sender);
                Vec::new()
            }
            Inbound::SequenceDeltaRequest(request, sender) => {
                self.serve_sequence_delta(&request, &sender);
                Vec::new()
            }
            Inbound::SequenceDeltaRangeRequest(request, sender) => {
                self.serve_sequence_delta_range(&request, &sender);
                Vec::new()
            }
            Inbound::SequenceOutcomeRequest(request, sender) => {
                self.serve_sequence_outcome(&request, &sender);
                Vec::new()
            }
            Inbound::SequenceHeadersRequest(digests, sender) => {
                self.serve_sequence_headers(&digests, &sender);
                Vec::new()
            }

            Inbound::SequenceRecords(chunk, sender) => {
                self.on_sequence_response(SequenceResponse::Records(chunk), &sender);
                Vec::new()
            }
            Inbound::SequenceDelta(chunk, sender) => {
                self.on_sequence_response(SequenceResponse::Delta(chunk), &sender);
                Vec::new()
            }
            Inbound::SequenceDeltaRange(chunk, sender) => {
                self.on_sequence_response(SequenceResponse::DeltaRange(chunk), &sender);
                Vec::new()
            }
            Inbound::SequenceOutcome(serve, sender) => {
                self.on_sequence_response(SequenceResponse::Outcome(serve), &sender);
                Vec::new()
            }
            Inbound::SequenceHeaders(headers, _) => {
                let mut effects = Vec::new();
                let mut accepted = 0u64;
                for header in headers.into_iter().take(SEQUENCE_BLOCK_SERVE_BATCH) {
                    let digest = header.id.clone();

                    // Ignore unsolicited headers outside the active bounded request window.
                    if !self.sequence_block_requests.contains_key(&digest) {
                        continue;
                    }
                    let header_effects = self.sequence_serve_effects(header).await;
                    let valid = block_was_cached(&header_effects, &digest);
                    effects.extend(header_effects);
                    if valid && self.sequence_block_requests.remove(&digest).is_some() {
                        accepted += 1;
                    }
                }
                if accepted > 0 {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .vantage_sequence_install_headers_received_total
                            .inc_by(accepted);
                        metrics
                            .vantage_sequence_install_header_requests_in_flight
                            .set(self.sequence_block_requests.len() as i64);
                    }
                    if self.sequence_block_requests.len() <= SEQUENCE_BLOCK_REFILL_AT {
                        self.drive_sequence_block_fetch(now).await;
                    }
                }
                effects
            }
            Inbound::SequenceUnavailable(u, sender) => {
                self.on_sequence_response(SequenceResponse::Unavailable(u), &sender);
                Vec::new()
            }
            Inbound::ReplayDone(end_key, complete, clamped, sender) => {
                if self.reconnect_replay {
                    self.on_replay_done(end_key, complete, clamped, sender, now)
                        .await;
                } else {
                    log::debug!(
                        "vantage node: replay done ignored: reconnect replay disabled, sender={}",
                        sender
                    );
                }
                Vec::new()
            }
        }
    }

    /// Returns the highest certified, transferring, verified, or staged target.
    fn highest_sequence_sync_target(&self) -> Option<View> {
        if !self.sequence_install_enabled {
            return None;
        }
        let local = self
            .sequence
            .as_ref()
            .map(|store| store.head_view())
            .unwrap_or(0);
        [
            self.sequence_sync
                .as_ref()
                .and_then(|collector| collector.certified_head(local))
                .map(|(view, _)| view),
            self.sequence_transfer
                .as_ref()
                .map(|transfer| transfer.target().0),
            self.sequence_verified_target
                .as_ref()
                .map(|(view, _)| *view),
            self.sequence_install
                .as_ref()
                .map(|install| install.target().0),
        ]
        .into_iter()
        .flatten()
        .max()
    }

    /// Returns a target that requires ingress shedding, unless recovery is latched off.
    fn large_sequence_sync_target(&self) -> Option<View> {
        if self.sequence_sync_recovered {
            return None;
        }
        let local = self
            .sequence
            .as_ref()
            .map(|store| store.head_view())
            .unwrap_or(0);
        let target = self.highest_sequence_sync_target()?;

        (target.saturating_sub(local) >= self.sequence_sync_shed_gap_views).then_some(target)
    }

    /// Updates independent shedding, recovery, and recovered-latch state.
    fn refresh_sequence_large_gap_drop(&mut self) {
        let local = self
            .sequence
            .as_ref()
            .map(|store| store.head_view())
            .unwrap_or(0);
        let target = self.highest_sequence_sync_target();

        let was_recovering = self.sequence_sync_recovery_active;
        let shed_active = self.large_sequence_sync_target().is_some();
        let shed_released = self.sequence_shed_was_active && !shed_active;
        if shed_released {
            let intake_edge = self.frontier.a_i() + 1;
            let covered_edge = target.unwrap_or(0).max(intake_edge);
            let floor = covered_edge.saturating_add(SEQUENCE_LIVE_INTAKE_MARGIN);
            if floor > self.sequence_live_intake_floor {
                self.sequence_live_intake_floor = floor;
                log::debug!(
                    "vantage sequence sync: shed released; live-intake floor set to view={floor}"
                );
            }
        }
        self.sequence_shed_was_active = shed_active;

        self.sequence_sync_recovery_active = self.sequence_install_enabled
            && (local < self.sequence_live_intake_floor
                || target.is_some_and(|target| {
                    target.saturating_sub(local) >= self.sequence_sync_min_gap_views
                }));

        // Leaving recovery marks completion pending; an active installation defers it.
        if was_recovering && !self.sequence_sync_recovery_active {
            self.sequence_latch_pending = true;
        }
        if self.sequence_sync_recovery_active {
            self.sequence_latch_pending = false;
        }
        if self.sequence_latch_pending
            && !self.sequence_sync_recovered
            && self.sequence_install.is_none()
        {
            self.sequence_latch_pending = false;
            self.sequence_sync_recovered = true;
            log::info!(
                "vantage sequence sync: RECOVERED at view={local} (live-intake floor {}); \
                 state sync off until a gap of {} views or more",
                self.sequence_live_intake_floor,
                self.sequence_sync_rearm_gap_views
            );
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_sync_recovered.set(1);
            }
        }
        // Stop fetching near the target so live participation can complete recovery.
        if !self.sequence_sync_recovery_active && self.sequence_transfer.is_some() {
            log::debug!(
                "vantage sequence sync: within {} views of the target; stopping transfer \
                 and recovering the tail by ordinary participation",
                self.sequence_sync_min_gap_views
            );
            self.sequence_transfer = None;
            self.sequence_request_at = None;
            self.sequence_last_want = None;
        }
        self.sequence_large_gap_drop.store(
            self.large_sequence_sync_target().is_some(),
            Ordering::Relaxed,
        );
        let install_drop_through =
            if self.sequence_install_enabled && self.sequence_sync_recovery_active {
                self.sequence_install
                    .as_ref()
                    .map(|install| install.target().0)
                    .unwrap_or(0)
            } else {
                0
            };
        self.sequence_install_drop_through
            .store(install_drop_through, Ordering::Relaxed);
    }

    /// Drops view-scoped traffic replaced by active recovery.
    fn install_replaces_inbound(&self, inbound: &Inbound) -> bool {
        if !self.sequence_install_enabled {
            return false;
        }

        if self.large_sequence_sync_target().is_some() {
            return !inbound.keep_during_large_sequence_sync();
        }

        if !self.sequence_sync_recovery_active {
            return false;
        }

        let Some(target) = self
            .sequence_install
            .as_ref()
            .map(|install| install.target().0)
        else {
            return false;
        };
        inbound
            .install_obsolete_view()
            .is_some_and(|view| view <= target)
    }

    async fn serve_effects(&mut self, header: Header) -> Vec<Effect> {
        let mut effects = self.rep.on_serve(header.clone());
        append_missing_payload_sync(&mut self.lm, &header, &mut effects).await;
        effects
    }

    async fn sequence_serve_effects(&mut self, header: Header) -> Vec<Effect> {
        let mut effects = self.rep.on_sequence_serve(header.clone());
        append_missing_payload_sync(&mut self.lm, &header, &mut effects).await;
        effects
    }

    /// Drains initial and transitively produced effects without recursive futures.
    async fn execute(&mut self, initial: Vec<Effect>, now: Instant) {
        let _timer = Self::cached_utilization_timer(
            &self.metrics,
            &mut self.ut_effect_execution,
            "effect_execution",
        );
        let mut queue: VecDeque<Effect> = initial.into();

        // Run rechecks after queued lane and repair effects drain. Do not mutate AGB gate
        // state before this point.
        loop {
            while let Some(effect) = queue.pop_front() {
                match effect {
                    Effect::BroadcastPublish(header) => {
                        self.wire
                            .broadcast_message(PrimaryMessage::Header(header, false))
                            .await
                    }

                    Effect::AvailClaimed(sender, resolved) => {
                        let refs = self.lm.note_claim(sender, &resolved);
                        queue.extend(self.credit_refs(sender, refs, now));
                    }
                    Effect::BroadcastAck(ack) => {
                        // Local self-acknowledgment is required even when wire acks are suppressed.
                        queue.extend(self.record_local_ack(&ack, now));
                        if !self.ack_watermarks {
                            self.broadcast_recorded(PrimaryMessage::VantageAck(ack))
                                .await
                        }
                    }
                    Effect::SyncBatches(author, header_digest, missing) => {
                        self.payload
                            .sync_batches(&mut self.wire, author, header_digest, missing)
                            .await;
                    }
                    Effect::RequestTo(peer, digest) => {
                        self.wire
                            .send_message(
                                peer,
                                PrimaryMessage::HeadersRequest(vec![digest], self.name),
                            )
                            .await;
                    }
                    Effect::ServeTo(peer, header) => {
                        self.wire
                            .send_message(peer, PrimaryMessage::Header(header, true))
                            .await
                    }
                    Effect::BlockCached(digest) => {
                        for (sender, r) in self.lm.retry_pending_avail(&digest) {
                            queue.extend(self.credit_refs(sender, vec![r], now));
                        }
                        queue.extend(self.rep.on_block_available(digest));

                        self.recheck_pending = true;
                        queue.extend(self.cursor.retry());
                    }

                    Effect::BroadcastPropose(p) => match p {
                        ProposalOut::Single(p) => {
                            self.broadcast_recorded(PrimaryMessage::VantagePropose(p))
                                .await
                        }
                        ProposalOut::Batch(p) => {
                            self.broadcast_recorded(PrimaryMessage::VantageProposeBatch(p))
                                .await
                        }
                    },

                    Effect::BroadcastEcho(mut e) => {
                        // Stamp mutable protocol metadata at the serialization boundary.
                        e.set_wish(self.pacemaker.own_watermark());

                        if self.echo_avail_claims {
                            if let EchoOut::Single(inner) = &mut e {
                                inner.avail = Some(self.lm.build_avail_claim(&inner.proposal));
                            }
                        }
                        match e {
                            EchoOut::Single(e) if self.digest_statements => {
                                let msg = e.to_digest(self.agb.sid());
                                self.broadcast_recorded(PrimaryMessage::VantageEchoDigest(msg))
                                    .await
                            }
                            EchoOut::Single(e) => {
                                self.broadcast_recorded(PrimaryMessage::VantageEcho(e))
                                    .await
                            }
                            EchoOut::Batch(e) => {
                                self.broadcast_recorded(PrimaryMessage::VantageEchoBatch(e))
                                    .await
                            }
                        }
                    }
                    Effect::BroadcastEchoSkip(view) => {
                        let wish = self.pacemaker.own_watermark();
                        self.broadcast_recorded(PrimaryMessage::VantageEchoSkip(
                            view, self.name, wish,
                        ))
                        .await;
                    }
                    Effect::BroadcastReady(mut r) => {
                        r.set_wish(self.pacemaker.own_watermark());
                        match r {
                            ReadyOut::Single(r) if self.digest_statements => {
                                let msg = r.to_digest(self.agb.sid());
                                self.broadcast_recorded(PrimaryMessage::VantageReadyDigest(msg))
                                    .await
                            }
                            ReadyOut::Single(r) => {
                                self.broadcast_recorded(PrimaryMessage::VantageReady(r))
                                    .await
                            }
                            ReadyOut::Batch(r) => {
                                self.broadcast_recorded(PrimaryMessage::VantageReadyBatch(r))
                                    .await
                            }
                        }
                    }
                    Effect::BroadcastNoReady(view) => {
                        let wish = self.pacemaker.own_watermark();
                        self.broadcast_recorded(PrimaryMessage::VantageNoReady(
                            view, self.name, wish,
                        ))
                        .await;
                    }

                    Effect::BroadcastSkipVote(view) => {
                        self.broadcast_recorded(PrimaryMessage::VantageSkipVote(view, self.name))
                            .await;
                    }
                    Effect::Fixed(view, well_formed) => {
                        let activated = self.frontier.record_fixed(view, well_formed);
                        for v in activated {
                            queue.extend(self.agb.activate(v, &mut self.lm, &mut self.rep));
                        }
                        queue.extend(self.try_propose_effects(now));

                        queue.extend(self.digest_stmts.on_local_fixed(
                            view,
                            &mut self.agb,
                            &mut self.rep,
                        ));
                    }
                    Effect::Completed(view, c, t) => {
                        queue.extend(self.cursor.on_completed(view, c, t));
                    }
                    Effect::Sealed(view, outcome) => {
                        queue.extend(self.cursor.on_sealed(view, outcome));
                    }
                    Effect::ArmTimer(view, kind, deadline) => {
                        self.timers.push(Reverse((deadline, view, kind)));
                    }
                    Effect::NotifyCommitted(commit_millis, by_worker, headers) => {
                        if let Some(sequence) = self.sequence.as_mut() {
                            sequence.retain_verified_headers(headers.iter().cloned());
                        }
                        if let Some(metrics) = &self.metrics {
                            let mut own = 0u64;
                            let mut own_payload = 0u64;
                            for header in headers.iter() {
                                metrics
                                    .vantage_committed_by_author
                                    .with_label_values(&[&author_label(&header.author)])
                                    .inc();
                                if header.author != self.name {
                                    continue;
                                }
                                own += 1;
                                own_payload += header.payload.len() as u64;
                            }
                            if own > 0 {
                                metrics.vantage_own_blocks_committed_total.inc_by(own);

                                metrics
                                    .vantage_own_payload_committed_total
                                    .inc_by(own_payload);
                            }
                        }
                        self.payload
                            .notify_committed(&mut self.wire, commit_millis, by_worker, headers)
                            .await;
                    }
                    Effect::BroadcastWish(view) => {
                        self.broadcast_recorded(PrimaryMessage::VantageWish(view, self.name))
                            .await
                    }
                    Effect::Enter(view) => {
                        queue.extend(self.enter_view_effects(view, now));
                    }
                    Effect::RaiseWish(target) => {
                        queue.extend(self.pacemaker.raise_own_wish(target));
                    }
                    Effect::SequenceFinalized {
                        view,
                        outcome,
                        output_delta,
                    } => {
                        self.record_sequence(view, &outcome, &output_delta);
                    }
                    Effect::RecoverOwnLane(header) => {
                        self.lm.recover_own_frontier(header).await;
                    }

                    Effect::CompletionReportable(view, proposal) => {
                        for entry in proposal.entries() {
                            self.resolver.note_carrier_report(entry.target_view(), now);
                        }
                        queue.extend(self.control.on_completion_reportable(view, proposal));
                    }

                    Effect::BroadcastCompReport(view, digest) => {
                        self.broadcast_recorded(PrimaryMessage::CompReport(
                            view, digest, self.name,
                        ))
                        .await;
                    }

                    Effect::BroadcastControlInit(proposal, b_w) => match b_w {
                        None => {
                            self.broadcast_recorded(PrimaryMessage::ControlInit(proposal, None))
                                .await
                        }
                        Some(ProposalOut::Single(p)) => {
                            self.broadcast_recorded(PrimaryMessage::ControlInit(proposal, Some(p)))
                                .await
                        }
                        Some(ProposalOut::Batch(p)) => {
                            self.broadcast_recorded(PrimaryMessage::ControlInitBatch(
                                proposal,
                                Some(p),
                            ))
                            .await
                        }
                    },
                    Effect::BroadcastControlEcho(proposal) => {
                        self.broadcast_recorded(PrimaryMessage::ControlEcho(proposal, self.name))
                            .await;
                    }
                    Effect::BroadcastControlReady(proposal) => {
                        self.broadcast_recorded(PrimaryMessage::ControlReady(proposal, self.name))
                            .await;
                    }
                    Effect::BroadcastControlCommit(round) => {
                        self.broadcast_recorded(PrimaryMessage::ControlCommit(round, self.name))
                            .await;
                    }
                    Effect::BroadcastControlTimeoutVote(round) => {
                        self.broadcast_recorded(PrimaryMessage::ControlTimeoutVote(
                            round, self.name,
                        ))
                        .await;
                    }
                    Effect::BroadcastControlTimeoutAccept(round) => {
                        self.broadcast_recorded(PrimaryMessage::ControlTimeoutAccept(
                            round, self.name,
                        ))
                        .await;
                    }
                    Effect::ControlFetchTo(peer, view, digest) => {
                        self.wire
                            .send_message(
                                peer,
                                PrimaryMessage::ControlFetch(view, digest, self.name),
                            )
                            .await;
                    }

                    Effect::ControlServeTo(peer, view, proposal) => match proposal {
                        ProposalOut::Single(p) => {
                            self.wire
                                .send_message(peer, PrimaryMessage::ControlServe(view, p))
                                .await
                        }
                        ProposalOut::Batch(p) => {
                            self.wire
                                .send_message(peer, PrimaryMessage::ControlServeBatch(view, p))
                                .await
                        }
                    },
                    Effect::ArmControlTimer(round, deadline) => {
                        self.control_timers.push(Reverse((deadline, round)));
                    }

                    Effect::ApplyAnchor(view, outcome, refs) => {
                        for r in refs {
                            queue.extend(self.rep.authorize(r));
                        }
                        queue.extend(self.agb.submit_anchor(view, outcome));
                    }

                    Effect::BodyFetchTo(peer, view, digest) => {
                        self.wire
                            .send_message(
                                peer,
                                PrimaryMessage::VantageBodyFetch(view, digest, self.name),
                            )
                            .await;
                    }
                    Effect::BodyServeTo(peer, view, proposal) => {
                        self.wire
                            .send_message(peer, PrimaryMessage::VantageBodyServe(view, proposal))
                            .await;
                    }

                    Effect::ResumeServeTo(requester, header) => {
                        // Lane-resume service uses a nonblocking handoff to its sender task.
                        self.wire.enqueue_resume_header(requester, header);
                    }
                }
            }
            if !self.recheck_pending {
                break;
            }
            self.recheck_pending = false;
            let rechecked = self.agb.recheck_all(&mut self.lm, &mut self.rep);
            if rechecked.is_empty() {
                break;
            }
            queue.extend(rechecked);
        }
    }

    fn cached_utilization_timer(
        metrics: &Option<Arc<Metrics>>,
        cache: &mut Option<IntCounter>,
        label: &str,
    ) -> Option<UtilizationTimer> {
        let metrics = metrics.as_ref()?;
        let counter = cache
            .get_or_insert_with(|| metrics.utilization_timer.with_label_values(&[label]))
            .clone();
        Some(UtilizationTimer::from_counter(counter))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn bulk_class_is_requests_of_us_not_responses_to_us() {
        use crypto::PublicKey;
        let k = PublicKey::default();
        let d = Digest::default();

        for inbound in [
            Inbound::HeadersRequest(vec![d.clone()], k),
            Inbound::ControlFetch(1, d.clone(), k),
            Inbound::BodyFetch(1, d.clone(), k),
            Inbound::LaneResume(k, 1, k),
            Inbound::ResumeHello(1, k),
        ] {
            assert!(
                inbound.is_bulk(),
                "a request of us must be droppable: {inbound:?}"
            );
        }

        for inbound in [
            Inbound::Serve(Header::default()),
            Inbound::ReplayDone(1, true, false, k),
        ] {
            assert!(
                !inbound.is_bulk(),
                "a response to our own request must not be droppable: {inbound:?}"
            );
        }

        for inbound in [
            Inbound::Publish(k, Header::default()),
            Inbound::EchoSkip(1, k, 1),
            Inbound::NoReady(1, k, 1),
            Inbound::Wish(1, k),
            Inbound::CompReport(1, d.clone(), k),
            Inbound::ControlCommit(k, 1),
            Inbound::ControlTimeoutVote(k, 1),
            Inbound::ControlTimeoutAccept(k, 1),
            Inbound::SkipVote(1, k),
        ] {
            assert!(!inbound.is_bulk(), "must stay consensus-class: {inbound:?}");
        }
    }

    use super::*;
    use crate::vantage::agb::{Echo, Ready, ReadyGrade, ViewProposal};
    use crate::vantage::control::ControlProposal;
    use crypto::{generate_keypair, Hash as _};
    use rand::rngs::StdRng;
    use rand::SeedableRng as _;
    use std::collections::BTreeMap;
    use store::Store;

    fn test_core(idx: usize, path_suffix: &str) -> VantageCore {
        let (name, _) = crate::common::keys()[idx];
        let committee = crate::common::committee();
        let path = format!(".db_test_vantage_membership_gate_{}", path_suffix);
        let _ = std::fs::remove_dir_all(&path);
        let store = Store::new(&path).expect("store opens");
        let registry = prometheus::Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        let (tx_output, _rx_output) = channel(1);
        let (
            core,
            _rx_vantage,
            _rx_bulk,
            _rx_sequence,
            _rx_payload_ready,
            _tx_vantage,
            _tx_bulk,
            _tx_sequence,
            _ack_aggregator,
            _sequence_large_gap_drop,
            _sequence_install_drop_through,
            _reconnect_rx,
        ) = VantageCore::build(
            name,
            committee,
            Parameters::default(),
            store,
            Some(metrics),
            tx_output,
        );
        core
    }

    fn fabricated_key() -> PublicKey {
        let mut rng = StdRng::from_seed([7; 32]);
        let (pk, _sk) = generate_keypair(&mut rng);
        pk
    }

    fn dummy_proposal_at(view: View) -> ViewProposal {
        ViewProposal {
            view,
            c: Vec::new(),
            t: Vec::new(),
            m: None,
        }
    }

    fn dummy_proposal() -> ViewProposal {
        dummy_proposal_at(1)
    }

    fn rejected_count(core: &VantageCore) -> u64 {
        core.metrics
            .as_ref()
            .expect("test core has metrics")
            .vantage_rejected_nonmember_total
            .get()
    }

    async fn mark_payload(store: &mut Store, digest: &Digest, worker_id: WorkerId) {
        let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
        store.write(key, Vec::new()).await;
    }

    fn served_payload_header(core: &VantageCore, author: PublicKey) -> (Header, Digest, Digest) {
        let missing = Digest([0x11; 32]);
        let present = Digest([0x22; 32]);
        let mut payload = BTreeMap::new();
        payload.insert(missing.clone(), 0);
        payload.insert(present.clone(), 0);
        (
            Header::new_vantage(
                author,
                1,
                payload,
                core.lm.genesis().clone(),
                core.lm.sid().clone(),
            ),
            missing,
            present,
        )
    }

    fn sync_effect(effects: &[Effect]) -> Option<&Effect> {
        effects
            .iter()
            .find(|effect| matches!(effect, Effect::SyncBatches(..)))
    }

    #[tokio::test]
    async fn terminal_sequence_progress_advances_the_resolver_floor() {
        let mut core = test_core(0, "terminal_resolver_floor");

        assert_eq!(core.resolver.resolved_watermark(), 1);
        core.record_sequence(1, &SequenceOutcome::Skip, &[]);
        assert_eq!(core.resolver.resolved_watermark(), 2);
    }

    #[tokio::test]
    async fn committed_headers_are_retained_for_sequence_service() {
        let mut core = test_core(0, "sequence_retained_headers");
        let author = crate::common::keys()[1].0;
        let (header, _, _) = served_payload_header(&core, author);
        core.rep
            .authorize((author, header.height, header.id.clone()));

        let effects = core
            .dispatch_inbound(Inbound::Serve(header.clone()), Instant::now())
            .await;
        assert!(effects
            .iter()
            .any(|effect| matches!(effect, Effect::BlockCached(d) if d == &header.id)));
        core.execute(
            vec![Effect::NotifyCommitted(0, Vec::new(), vec![header.clone()])],
            Instant::now(),
        )
        .await;

        let blocks = core.rep.blocks();
        assert_eq!(
            blocks.lock().evict_author_below(&author, header.height + 1),
            1
        );
        assert!(!blocks.lock().contains(&header.id));

        assert_eq!(
            core.sequence
                .as_ref()
                .and_then(|store| store.retained_header(&header.id)),
            Some(&header)
        );
        assert_eq!(
            core.verified_sequence_headers(std::slice::from_ref(&header.id)),
            vec![header]
        );
    }

    #[tokio::test]
    async fn large_sequence_sync_drops_consensus_parking_until_gap_is_small() {
        let mut core = test_core(0, "large_sequence_drop");
        let (member, _) = crate::common::keys()[1];
        let target = 100;
        core.sequence_sync_min_gap_views = 50;
        core.sequence_sync_shed_gap_views = 50;
        core.sequence_install = Some(SequenceInstall::new(
            0,
            target,
            Digest::default(),
            Vec::new(),
            Vec::new(),
            8,
            4096,
        ));

        let future_echo = Inbound::Echo(EchoOut::Single(Echo {
            proposal: dummy_proposal_at(target + 50),
            grade: 0,
            sender: member,
            wish: 0,
            origin: None,
            avail: None,
        }));
        assert!(
            core.install_replaces_inbound(&future_echo),
            "large-gap install must not park future-view AGB traffic"
        );
        assert!(ingress_replaces_inbound(&future_echo, true, 0));

        let control = Inbound::ControlEcho(
            member,
            ControlProposal {
                round: 1,
                parent: 0,
                value: None,
            },
        );
        assert!(
            core.install_replaces_inbound(&control),
            "large-gap install must not park viewless control-round traffic"
        );

        let sequence_request = Inbound::SequenceRequest(
            SequenceRequest {
                version: SEQUENCE_VERSION,
                transfer_id: 1,
                target_view: target,
                target_head: Digest::default(),
                from_view: 1,
                max_records: 64,
                requester: member,
            },
            member,
        );
        assert!(
            core.install_replaces_inbound(&sequence_request),
            "a syncing node should not serve other peers' state-sync requests"
        );

        let (header, _, _) = served_payload_header(&core, member);
        assert!(
            core.install_replaces_inbound(&Inbound::Serve(header.clone())),
            "ordinary repair replies are obsolete once committed headers use the dedicated sequence path"
        );
        assert!(
            !core.install_replaces_inbound(&Inbound::SequenceHeaders(vec![header.clone()], member)),
            "dedicated committed-header responses must remain admissible"
        );
        assert!(
            core.install_replaces_inbound(&Inbound::Publish(member, header)),
            "live publishes are ordinary dissemination and should not fill the queue \
             during a large sequence sync"
        );

        let announcement = Inbound::SequenceAnnounce(
            SequenceAnnouncement {
                version: SEQUENCE_VERSION,
                view: target + 20,
                head: Digest::default(),
                serve_floor: 1,
                sender: member,
            },
            member,
        );
        assert!(
            !core.install_replaces_inbound(&announcement),
            "checkpoint announcements must still be accepted for the next sticky transfer"
        );

        core.sequence_sync_shed_gap_views = 200;

        core.sequence_sync_recovery_active = true;
        assert!(
            !core.install_replaces_inbound(&future_echo),
            "once the active gap is below the SHED threshold, future traffic can park"
        );

        let covered_echo = Inbound::Echo(EchoOut::Single(Echo {
            proposal: dummy_proposal_at(target),
            grade: 0,
            sender: member,
            wish: 0,
            origin: None,
            avail: None,
        }));
        assert!(
            core.install_replaces_inbound(&covered_echo),
            "the staged target still replaces messages for views it covers"
        );
        core.refresh_sequence_large_gap_drop();
        assert_eq!(
            core.sequence_install_drop_through.load(Ordering::Relaxed),
            target
        );
        assert!(ingress_replaces_inbound(&covered_echo, false, target));
        assert!(!ingress_replaces_inbound(&future_echo, false, target));
        assert!(!ingress_replaces_inbound(&announcement, false, target));
    }

    #[tokio::test]
    async fn recovered_node_does_not_resync_on_ordinary_jitter() {
        let mut core = test_core(0, "sequence_sync_recovered_latch");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 1_000;
        core.sequence_sync_rearm_gap_views = 400;
        let keys = crate::common::keys();

        let certify = |core: &mut VantageCore, view: View, tag: u8| {
            let head = Digest([tag; 32]);
            for (sender, _) in keys.iter().skip(1).take(3) {
                core.on_sequence_announce(
                    &SequenceAnnouncement {
                        version: SEQUENCE_VERSION,
                        view,
                        head: head.clone(),
                        serve_floor: 1,
                        sender: *sender,
                    },
                    sender,
                );
            }
        };

        core.refresh_sequence_large_gap_drop();
        assert!(
            !core.sequence_sync_recovered,
            "a node that has never recovered must be able to sync"
        );

        certify(&mut core, 500, 0x11);
        core.refresh_sequence_large_gap_drop();
        assert!(
            core.sequence_sync_recovery_active,
            "a 500-view gap must engage recovery"
        );
        assert!(!core.sequence_sync_recovered);

        for view in 1..=450 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(
            !core.sequence_sync_recovery_active,
            "the gap is now inside the sync gate"
        );
        assert!(
            core.sequence_sync_recovered,
            "leaving recovery must latch recovered"
        );

        let jitter_head = Digest([0x44; 32]);
        for (sender, _) in keys.iter().skip(1).take(8) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 650,
                    head: jitter_head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        core.drive_sequence_sync();
        assert!(
            core.sequence_transfer.is_none(),
            "ordinary jitter above the sync gate must NOT restart state sync"
        );
        assert!(core.sequence_sync_recovered);

        let outage_head = Digest([0x55; 32]);
        for (sender, _) in keys.iter().skip(1).take(8) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 1_000,
                    head: outage_head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        core.drive_sequence_sync();
        assert!(
            !core.sequence_sync_recovered,
            "a gap large enough to be an outage must re-arm state sync"
        );
    }

    #[tokio::test]
    async fn latch_waits_for_the_live_intake_floor() {
        let mut core = test_core(0, "sequence_sync_live_floor");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
        core.sequence_sync_rearm_gap_views = 800;
        let keys = crate::common::keys();

        let head = Digest([0x21; 32]);
        for (sender, _) in keys.iter().skip(1).take(3) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 500,
                    head: head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        core.refresh_sequence_large_gap_drop();
        assert!(core.sequence_sync_recovery_active);
        assert!(core.sequence_large_gap_drop.load(Ordering::Relaxed));

        core.frontier.enter(520);

        for view in 1..=450 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(!core.sequence_large_gap_drop.load(Ordering::Relaxed));
        assert_eq!(
            core.sequence_live_intake_floor,
            520 + SEQUENCE_LIVE_INTAKE_MARGIN,
            "the shed off-edge must stamp the floor from the entry frontier"
        );
        assert!(
            core.sequence_sync_recovery_active,
            "recovery must keep installing until the head crosses the live-intake floor"
        );
        assert!(
            !core.sequence_sync_recovered,
            "latching here strands the cursor"
        );

        for view in 451..=520 + SEQUENCE_LIVE_INTAKE_MARGIN {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(!core.sequence_sync_recovery_active);
        assert!(core.sequence_sync_recovered);
    }

    #[tokio::test]
    async fn live_intake_floor_does_not_chase_active_recovery_work() {
        let mut core = test_core(0, "sequence_sync_fixed_live_floor");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
        core.sequence_sync_rearm_gap_views = 800;
        let keys = crate::common::keys();

        let head = Digest([0x31; 32]);
        for (sender, _) in keys.iter().skip(1).take(3) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 500,
                    head: head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        core.refresh_sequence_large_gap_drop();
        core.frontier.enter(520);

        for view in 1..=450 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert_eq!(
            core.sequence_live_intake_floor,
            520 + SEQUENCE_LIVE_INTAKE_MARGIN
        );

        core.sequence_install = Some(SequenceInstall::new(
            450,
            500,
            head,
            Vec::new(),
            Vec::new(),
            8,
            4096,
        ));
        core.frontier.enter(700);
        for view in 451..=520 + SEQUENCE_LIVE_INTAKE_MARGIN {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert_eq!(
            core.sequence_live_intake_floor,
            520 + SEQUENCE_LIVE_INTAKE_MARGIN,
            "live traffic received after shedding stops uses the ordinary path"
        );
        assert!(!core.sequence_sync_recovery_active);
        assert!(!core.sequence_sync_recovered);

        core.sequence_install = None;
        core.frontier.enter(800);
        core.refresh_sequence_large_gap_drop();
        assert_eq!(
            core.sequence_live_intake_floor,
            520 + SEQUENCE_LIVE_INTAKE_MARGIN
        );
        assert!(!core.sequence_sync_recovery_active);
        assert!(core.sequence_sync_recovered);
    }

    #[tokio::test]
    async fn shed_release_floor_covers_sync_target_ahead_of_entry_frontier() {
        let mut core = test_core(0, "sequence_sync_floor_target");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
        core.sequence_sync_rearm_gap_views = 800;
        let keys = crate::common::keys();

        let head = Digest([0x61; 32]);
        for (sender, _) in keys.iter().skip(1).take(3) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 500,
                    head: head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        core.refresh_sequence_large_gap_drop();
        assert!(core.sequence_large_gap_drop.load(Ordering::Relaxed));

        core.frontier.enter(420);

        for view in 1..=250 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(!core.sequence_large_gap_drop.load(Ordering::Relaxed));
        assert_eq!(
            core.sequence_live_intake_floor,
            500 + SEQUENCE_LIVE_INTAKE_MARGIN
        );
        assert!(
            core.sequence_sync_recovery_active,
            "recovery must not latch while the shed-dropped target suffix is uncovered"
        );
        assert!(!core.sequence_sync_recovered);
    }

    #[tokio::test]
    async fn latch_deferred_by_a_staged_install_still_fires() {
        let mut core = test_core(0, "sequence_sync_latch_deferred");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 1_000;
        core.sequence_sync_rearm_gap_views = 800;
        let keys = crate::common::keys();

        let head = Digest([0x41; 32]);
        for (sender, _) in keys.iter().skip(1).take(3) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 500,
                    head: head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        core.refresh_sequence_large_gap_drop();
        assert!(core.sequence_sync_recovery_active);

        for view in 1..=450 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.sequence_install = Some(SequenceInstall::new(
            0,
            500,
            Digest::default(),
            Vec::new(),
            Vec::new(),
            8,
            4096,
        ));
        core.refresh_sequence_large_gap_drop();
        assert!(!core.sequence_sync_recovery_active);
        assert!(
            !core.sequence_sync_recovered,
            "the latch must wait for the staged install to drain"
        );

        core.sequence_install = None;
        core.refresh_sequence_large_gap_drop();
        assert!(core.sequence_sync_recovered, "the deferred latch must fire");
    }

    #[tokio::test]
    async fn completed_install_waits_to_propose_until_recovery_finishes() {
        let mut core = test_core(0, "sequence_install_enters_live_view");
        let next_live = (2..=32)
            .find(|view| core.agb.proposer(*view) == core.name)
            .expect("round-robin proposer repeats within the search window");
        let target = next_live - 1;
        core.sequence_install_views_per_tick = target as usize + 1;
        core.sequence_install_digests_per_tick = target as usize + 1;
        let staged = (1..=target)
            .map(|view| (view, SequenceOutcome::Skip, Vec::new()))
            .collect();
        let heads = (1..=target)
            .map(|view| (view, Digest([view as u8; 32])))
            .collect();
        core.sequence_install = Some(SequenceInstall::new(
            0,
            target,
            Digest([0x77; 32]),
            staged,
            heads,
            target as usize + 1,
            4096,
        ));

        let effects = core.apply_sequence_install(target as usize + 1, Instant::now());

        assert_eq!(
            core.cursor.next_view(),
            next_live,
            "the install should advance the output cursor through target"
        );
        assert_eq!(
            core.pacemaker.entered_view(),
            next_live,
            "the pacemaker must not think historical entries are still missing"
        );
        assert_eq!(core.pacemaker.own_watermark(), next_live);
        assert_eq!(
            core.frontier.a_i(),
            target,
            "frontier entry must be floored to the installed target"
        );
        assert!(core.frontier.is_active(next_live));
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                Effect::BroadcastPropose(proposal) if proposal.view() == next_live
            )),
            "a staged install must not emit a proposal"
        );

        core.sequence_install = None;
        core.sequence_sync_recovery_active = false;
        let effects = core.try_propose_effects(Instant::now());
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::BroadcastPropose(proposal) if proposal.view() == next_live
        )));
        core.record_sequence(next_live, &SequenceOutcome::Skip, &[]);
        assert_eq!(
            core.metrics
                .as_ref()
                .expect("test core has metrics")
                .vantage_own_proposals_skipped_total
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn every_completed_install_reconciles_the_own_lane() {
        let mut core = test_core(0, "sequence_install_reconciles_own_lane");
        let header = Header::new_vantage(
            core.name,
            1,
            BTreeMap::new(),
            core.lm.genesis().clone(),
            core.lm.sid().clone(),
        );
        core.lm.process_publish(core.name, header.clone()).await;

        let r = (header.author, header.height, header.id.clone());
        let mut install = SequenceInstall::new(
            0,
            1,
            Digest([0x81; 32]),
            vec![(
                1,
                SequenceOutcome::Core { c: vec![r] },
                vec![header.id.clone()],
            )],
            vec![(1, Digest([0x81; 32]))],
            8,
            4096,
        );
        install.admit(0);
        install.refresh(&core.rep.blocks());
        core.sequence_install = Some(install);
        core.sequence_sync_recovery_active = false;

        let effects = core.apply_sequence_install(8, Instant::now());

        assert!(effects
            .iter()
            .any(|effect| matches!(effect, Effect::RecoverOwnLane(anchor) if anchor == &header)));
    }

    #[tokio::test]
    async fn recovered_node_never_sheds() {
        let mut core = test_core(0, "sequence_sync_no_shed_latched");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
        core.sequence_sync_rearm_gap_views = 800;
        let keys = crate::common::keys();

        let head = Digest([0x31; 32]);
        for (sender, _) in keys.iter().skip(1).take(3) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 400,
                    head: head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        assert!(
            core.large_sequence_sync_target().is_some(),
            "an unlatched node with a 400-view gap sheds"
        );

        core.sequence_sync_recovered = true;
        assert!(
            core.large_sequence_sync_target().is_none(),
            "a latched node must never shed -- the gap is below the re-arm threshold, \
             so it recovers this range by ordinary participation"
        );
        core.refresh_sequence_large_gap_drop();
        assert!(!core.sequence_large_gap_drop.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn sequence_sync_thresholds_are_independent_controls() {
        let mut core = test_core(0, "sequence_sync_thresholds");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
        core.sequence_install = Some(SequenceInstall::new(
            0,
            400,
            Digest::default(),
            Vec::new(),
            Vec::new(),
            8,
            4096,
        ));

        core.refresh_sequence_large_gap_drop();
        assert!(core.sequence_sync_recovery_active);
        assert!(core.sequence_large_gap_drop.load(Ordering::Relaxed));

        for view in 1..=150 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(
            core.sequence_sync_recovery_active,
            "sync continues while the gap exceeds the sync threshold"
        );
        assert!(
            !core.sequence_large_gap_drop.load(Ordering::Relaxed),
            "below the shed threshold the node must stop dropping consensus traffic"
        );

        core.sequence_transfer = Some(SequenceTransfer::new(
            core.agb.sid().clone(),
            9,
            0,
            core.sequence.as_ref().unwrap().head().clone(),
            400,
            Digest([0x33; 32]),
            vec![crate::common::keys()[1].0],
        ));
        for view in 151..=350 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(
            core.sequence_sync_recovery_active,
            "state sync must continue below the sync threshold until the live-intake \
             floor is crossed"
        );
        assert!(!core.sequence_large_gap_drop.load(Ordering::Relaxed));

        let floor = core.sequence_live_intake_floor;
        for view in 351..=floor {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        core.refresh_sequence_large_gap_drop();
        assert!(
            !core.sequence_sync_recovery_active,
            "state sync must stop after the shed-covered floor is crossed"
        );
        assert!(!core.sequence_large_gap_drop.load(Ordering::Relaxed));
        assert!(
            core.sequence_transfer.is_none(),
            "the in-flight transfer must be released to the tail"
        );
        assert!(
            core.sequence_install.is_some(),
            "a staged install still drains -- it applies already-verified state"
        );
    }

    #[tokio::test]
    async fn newer_checkpoint_does_not_discard_active_or_staged_progress() {
        let mut core = test_core(0, "sticky_sequence_target");
        core.sequence_sync_shed_gap_views = 50;
        let keys = crate::common::keys();
        let first_source = keys[1].0;
        let newer_head = Digest([0x22; 32]);
        let local_head = core.sequence.as_ref().unwrap().head().clone();
        core.sequence_transfer = Some(SequenceTransfer::new(
            core.agb.sid().clone(),
            7,
            0,
            local_head,
            100,
            Digest([0x11; 32]),
            vec![first_source],
        ));

        for (sender, _) in keys.iter().skip(1).take(2) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 200,
                    head: newer_head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        assert_eq!(
            core.sequence_sync
                .as_ref()
                .unwrap()
                .certified_head(0)
                .map(|(view, _)| view),
            Some(200)
        );
        for view in 1..=70 {
            core.sequence
                .as_mut()
                .unwrap()
                .record(view, &SequenceOutcome::Skip, &[])
                .unwrap();
        }
        assert_eq!(
            core.large_sequence_sync_target(),
            Some(200),
            "shedding must follow the newest certified fleet gap while target 100 stays sticky"
        );

        core.drive_sequence_sync();
        assert_eq!(
            core.sequence_transfer.as_ref().unwrap().target().0,
            100,
            "a newer announcement must not reset downloaded transfer progress"
        );

        core.sequence_transfer = None;
        core.sequence_request_at = None;
        core.sequence_last_want = None;
        core.sequence_verified_target = Some((100, Digest([0x11; 32])));
        core.sequence_install = Some(SequenceInstall::new(
            0,
            100,
            Digest([0x11; 32]),
            Vec::new(),
            Vec::new(),
            8,
            4096,
        ));
        core.drive_sequence_sync();
        assert_eq!(
            core.sequence_install.as_ref().unwrap().target().0,
            100,
            "a newer announcement must not reset staged install progress"
        );
    }

    #[tokio::test]
    async fn sequence_install_batches_committed_header_digests_to_checkpoint_sources() {
        let mut core = test_core(0, "sequence_block_batch");
        let keys = crate::common::keys();
        let sid = core.agb.sid().clone();
        let genesis = core.lm.genesis().clone();
        let first =
            Header::new_vantage(keys[1].0, 1, BTreeMap::new(), genesis.clone(), sid.clone());
        let second = Header::new_vantage(keys[2].0, 1, BTreeMap::new(), genesis, sid);
        let target_head = Digest([0x44; 32]);
        for (sender, _) in keys.iter().skip(1).take(2) {
            core.on_sequence_announce(
                &SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view: 1,
                    head: target_head.clone(),
                    serve_floor: 1,
                    sender: *sender,
                },
                sender,
            );
        }
        let outcome = SequenceOutcome::Core {
            c: vec![
                (first.author, first.height, first.id.clone()),
                (second.author, second.height, second.id.clone()),
            ],
        };
        core.sequence_install_sources = core
            .sequence_sync
            .as_ref()
            .unwrap()
            .announcers(1, &target_head);
        let mut install = SequenceInstall::new(
            0,
            1,
            target_head.clone(),
            vec![(1, outcome, vec![first.id.clone(), second.id.clone()])],
            Vec::new(),
            8,
            4096,
        );
        assert_eq!(install.admit(0).len(), 2);
        core.sequence_install = Some(install);

        let now = Instant::now();
        core.drive_sequence_block_fetch(now).await;
        assert_eq!(
            core.sequence_block_requests.len(),
            2,
            "both missing delta digests are batched without waiting for parent walks"
        );

        // A long installation can outlive the collector's candidate window.
        for view in 2..=34 {
            for (sender, _) in keys.iter().skip(1).take(2) {
                let announcement = SequenceAnnouncement {
                    version: SEQUENCE_VERSION,
                    view,
                    head: Digest([view as u8; 32]),
                    serve_floor: 1,
                    sender: *sender,
                };
                core.sequence_sync.as_mut().unwrap().on_announcement(
                    &announcement,
                    sender,
                    true,
                    0,
                );
            }
        }
        assert!(
            core.sequence_sync
                .as_ref()
                .unwrap()
                .announcers(1, &target_head)
                .is_empty(),
            "the collector should evict the old target"
        );
        let requested = core
            .metrics
            .as_ref()
            .unwrap()
            .vantage_sequence_install_headers_requested_total
            .get();
        core.drive_sequence_block_fetch(
            now + Duration::from_millis(core.sequence_request_timeout_ms + 1),
        )
        .await;
        assert!(
            core.metrics
                .as_ref()
                .unwrap()
                .vantage_sequence_install_headers_requested_total
                .get()
                > requested,
            "the retained sources should keep timed-out requests moving"
        );

        let effects = core
            .dispatch_inbound(
                Inbound::SequenceHeaders(vec![first.clone()], keys[1].0),
                Instant::now(),
            )
            .await;
        assert!(effects
            .iter()
            .any(|effect| matches!(effect, Effect::BlockCached(d) if d == &first.id)));
        assert_eq!(core.sequence_block_requests.len(), 1);
        assert!(core.rep.blocks().lock().contains(&first.id));
    }

    #[tokio::test]
    async fn accepted_served_header_syncs_only_missing_payloads() {
        let mut core = test_core(0, "serve_missing");
        let author = crate::common::keys()[1].0;
        let (header, missing, present) = served_payload_header(&core, author);
        let mut store = core.lm.store_for_test();
        mark_payload(&mut store, &present, 0).await;
        core.rep.authorize((author, 1, header.id.clone()));

        let effects = core
            .dispatch_inbound(Inbound::Serve(header.clone()), Instant::now())
            .await;
        assert!(matches!(
            sync_effect(&effects),
            Some(Effect::SyncBatches(a, h, entries))
                if *a == author && h == &header.id && entries == &vec![(missing, 0)]
        ));
        assert!(
            !core
                .lm
                .blocks_handle()
                .lock()
                .get(&header.id)
                .unwrap()
                .payload_ok
        );
    }

    #[tokio::test]
    async fn duplicate_accepted_serve_merges_pending_payload_without_duplicate_waiters() {
        let mut core = test_core(0, "serve_duplicate");
        let author = crate::common::keys()[1].0;
        let (header, missing, first_arrival) = served_payload_header(&core, author);
        let mut store = core.lm.store_for_test();
        let (payload_tx, mut payload_rx) = channel(4);
        core.payload.tx_payload_ready = payload_tx;
        core.wire.worker_addresses.clear();
        core.rep.authorize((author, 1, header.id.clone()));

        let effects = core
            .dispatch_inbound(Inbound::Serve(header.clone()), Instant::now())
            .await;
        core.execute(effects, Instant::now()).await;

        mark_payload(&mut store, &first_arrival, 0).await;
        let arrived = tokio::time::timeout(Duration::from_secs(1), payload_rx.recv())
            .await
            .expect("first payload waiter resolves")
            .expect("payload-ready channel stays open");
        assert_eq!(arrived, (header.id.clone(), first_arrival.clone(), 0));

        let effects = core
            .dispatch_inbound(Inbound::Serve(header.clone()), Instant::now())
            .await;
        assert!(matches!(
            sync_effect(&effects),
            Some(Effect::SyncBatches(a, h, entries))
                if *a == author
                    && h == &header.id
                    && entries == &vec![(missing.clone(), 0)]
        ));
        core.execute(effects, Instant::now()).await;

        {
            let pending = core
                .payload
                .pending_payload
                .get(&header.id)
                .expect("header remains pending");
            assert_eq!(pending.len(), 2);
            assert!(pending.contains(&(first_arrival, 0)));
            assert!(pending.contains(&(missing.clone(), 0)));
        }

        mark_payload(&mut store, &missing, 0).await;
        let arrived = tokio::time::timeout(Duration::from_secs(1), payload_rx.recv())
            .await
            .expect("last payload waiter resolves")
            .expect("payload-ready channel stays open");
        assert_eq!(arrived, (header.id, missing, 0));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), payload_rx.recv())
                .await
                .is_err(),
            "a duplicate serve must not spawn a second waiter for the same payload"
        );
    }

    #[tokio::test]
    async fn rejected_served_header_emits_no_payload_sync() {
        let mut core = test_core(0, "serve_rejected");
        let author = crate::common::keys()[1].0;
        let mut header = Header::new_vantage(
            author,
            1,
            BTreeMap::new(),
            core.lm.genesis().clone(),
            core.lm.sid().clone(),
        );
        header.id = Digest([0xff; 32]);
        core.rep.authorize((author, 1, header.id.clone()));

        let effects = core
            .dispatch_inbound(Inbound::Serve(header), Instant::now())
            .await;
        assert!(effects.is_empty());
    }

    #[tokio::test]
    async fn fully_present_served_header_emits_no_payload_sync() {
        let mut core = test_core(0, "serve_present");
        let author = crate::common::keys()[1].0;
        let (mut header, _missing, present) = served_payload_header(&core, author);
        header.payload.retain(|digest, _| digest == &present);
        header.id = header.digest();
        let mut store = core.lm.store_for_test();
        mark_payload(&mut store, &present, 0).await;
        core.rep.authorize((author, 1, header.id.clone()));

        let effects = core
            .dispatch_inbound(Inbound::Serve(header.clone()), Instant::now())
            .await;
        assert!(sync_effect(&effects).is_none());
        assert!(
            !core
                .lm
                .blocks_handle()
                .lock()
                .get(&header.id)
                .unwrap()
                .payload_ok
        );
    }

    #[tokio::test]
    async fn dispatch_drops_echo_from_nonmember_sender() {
        let mut core = test_core(0, "echo_nonmember");
        let fake = fabricated_key();
        assert!(!core.members.contains(&fake));

        let echo = Echo {
            proposal: dummy_proposal(),
            grade: 0,
            sender: fake,
            wish: 0,
            origin: None,
            avail: None,
        };
        let effects = core
            .dispatch_inbound(Inbound::Echo(EchoOut::Single(echo)), Instant::now())
            .await;

        assert!(
            effects.is_empty(),
            "a fabricated non-member Echo must be dropped with no effects"
        );
        assert_eq!(rejected_count(&core), 1);
    }

    #[tokio::test]
    async fn dispatch_drops_ready_from_nonmember_sender() {
        let mut core = test_core(0, "ready_nonmember");
        let fake = fabricated_key();

        let ready = Ready {
            proposal: dummy_proposal(),
            grade: ReadyGrade::Zero,
            sender: fake,
            wish: 0,
        };
        let effects = core
            .dispatch_inbound(Inbound::Ready(ReadyOut::Single(ready)), Instant::now())
            .await;

        assert!(
            effects.is_empty(),
            "a fabricated non-member Ready must be dropped with no effects"
        );
        assert_eq!(rejected_count(&core), 1);
    }

    #[tokio::test]
    async fn dispatch_drops_control_echo_from_nonmember_sender() {
        let mut core = test_core(0, "control_echo_nonmember");
        let fake = fabricated_key();
        let proposal = ControlProposal {
            round: 1,
            parent: 0,
            value: None,
        };

        let effects = core
            .dispatch_inbound(Inbound::ControlEcho(fake, proposal), Instant::now())
            .await;

        assert!(
            effects.is_empty(),
            "a fabricated non-member ControlEcho must be dropped with no effects"
        );
        assert_eq!(rejected_count(&core), 1);
    }

    #[tokio::test]
    async fn dispatch_accepts_echo_from_committee_member() {
        let mut core = test_core(0, "echo_member");
        let (member, _) = crate::common::keys()[1];
        assert!(core.members.contains(&member));

        let echo = Echo {
            proposal: dummy_proposal(),
            grade: 0,
            sender: member,
            wish: 0,
            origin: None,
            avail: None,
        };
        let effects = core
            .dispatch_inbound(Inbound::Echo(EchoOut::Single(echo)), Instant::now())
            .await;

        assert_eq!(rejected_count(&core), 0);
        let _ = effects;
    }

    #[tokio::test]
    async fn dispatch_delivers_ack_over_tcp_once_threshold_reached() {
        use futures::SinkExt as _;
        use tokio_util::codec::{Framed, LengthDelimitedCodec};

        let committee = crate::common::committee();
        let ack_aggregator = Arc::new(Mutex::new(AckAggregator::new(committee.clone())));
        let (sender, _) = crate::common::keys()[1];
        let (pre_sender, _) = crate::common::keys()[2];
        let reference = (sender, 7, Digest::default());
        ack_aggregator
            .lock()
            .record_ack(pre_sender, reference.clone());
        let (tx_vantage, mut rx_vantage) = channel(4);

        let (tx_bulk, _rx_bulk) = channel(4);
        let (tx_sequence, _rx_sequence) = channel(4);
        let codec = wire::VantageWireCodec::new(&committee, true).unwrap();
        let handler = VantageReceiverHandler {
            tx: tx_vantage,
            codec: codec.clone(),
            tx_bulk,
            tx_sequence,
            sequence_large_gap_drop: Arc::new(AtomicBool::new(false)),
            sequence_install_drop_through: Arc::new(AtomicU64::new(0)),
            ack_aggregator,
            metrics: None,
        };

        let address: SocketAddr = "127.0.0.1:14510".parse().unwrap();
        network::Receiver::spawn(address, handler);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let ack = Ack::new(reference.0, reference.1, reference.2.clone(), sender);
        let payload = codec.serialize(&PrimaryMessage::VantageAck(ack)).unwrap();

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        transport.send(Bytes::from(payload)).await.unwrap();

        let received = tokio::time::timeout(Duration::from_millis(500), rx_vantage.recv()).await;
        assert!(
            matches!(received, Ok(Some(Inbound::AckAvailability(_)))),
            "must deliver the ACK once it advances availability"
        );
    }

    #[tokio::test]
    async fn dispatch_lane_resume_ignores_foreign_lane() {
        let mut core = test_core(0, "lane_resume_foreign");
        let (other_author, _) = crate::common::keys()[1];
        let (requester, _) = crate::common::keys()[2];
        assert_ne!(other_author, core.name);

        let effects = core
            .dispatch_inbound(
                Inbound::LaneResume(other_author, 1, requester),
                Instant::now(),
            )
            .await;

        assert!(
            effects.is_empty(),
            "a LaneResume naming a lane this party doesn't own must be ignored"
        );
    }

    #[tokio::test]
    async fn dispatch_lane_resume_clamps_below_floor() {
        let mut core = test_core(0, "lane_resume_clamp");
        let (requester, _) = crate::common::keys()[1];
        let author = core.name;

        for _ in 0..3 {
            core.lm.publish_own(std::collections::BTreeMap::new()).await;
        }
        assert_eq!(core.lm.own_tip_height(), 3);

        let effects = core
            .dispatch_inbound(Inbound::LaneResume(author, 0, requester), Instant::now())
            .await;

        let served: Vec<(PublicKey, u64)> = effects
            .iter()
            .map(|e| match e {
                Effect::ResumeServeTo(r, h) => (*r, h.height),
                other => panic!("expected ResumeServeTo, got {:?}", other),
            })
            .collect();
        assert_eq!(
            served,
            vec![(requester, 1), (requester, 2), (requester, 3)],
            "from=0 must clamp up to height 1, then serve through our own tip (3)"
        );
    }

    #[tokio::test]
    async fn dispatch_lane_resume_serves_one_batch_not_the_whole_tip() {
        let mut core = test_core(0, "lane_resume_batch");
        let (requester, _) = crate::common::keys()[1];
        let author = core.name;
        assert_eq!(core.resume_batch, 64);

        for _ in 0..100 {
            core.lm.publish_own(std::collections::BTreeMap::new()).await;
        }
        assert_eq!(core.lm.own_tip_height(), 100);

        let effects = core
            .dispatch_inbound(Inbound::LaneResume(author, 1, requester), Instant::now())
            .await;

        let served_heights: Vec<u64> = effects
            .iter()
            .map(|e| match e {
                Effect::ResumeServeTo(_, h) => h.height,
                other => panic!("expected ResumeServeTo, got {:?}", other),
            })
            .collect();
        assert_eq!(
            served_heights,
            (1..=64).collect::<Vec<u64>>(),
            "must serve exactly one resume_batch-sized span (1..=64), not loop through height 100"
        );
    }

    #[tokio::test]
    async fn dispatch_lane_resume_from_beyond_tip_serves_nothing() {
        let mut core = test_core(0, "lane_resume_beyond_tip");
        let (requester, _) = crate::common::keys()[1];
        let author = core.name;

        core.lm.publish_own(std::collections::BTreeMap::new()).await;
        assert_eq!(core.lm.own_tip_height(), 1);

        let effects = core
            .dispatch_inbound(Inbound::LaneResume(author, 5, requester), Instant::now())
            .await;

        assert!(effects.is_empty());
    }

    #[tokio::test]
    async fn dispatch_lane_resume_dedups_identical_repeat_request() {
        let mut core = test_core(0, "lane_resume_dedup");
        let (requester, _) = crate::common::keys()[1];
        let author = core.name;

        core.lm.publish_own(std::collections::BTreeMap::new()).await;
        let now = Instant::now();

        let first = core
            .dispatch_inbound(Inbound::LaneResume(author, 1, requester), now)
            .await;
        assert_eq!(first.len(), 1, "first request must be served");

        let second = core
            .dispatch_inbound(Inbound::LaneResume(author, 1, requester), now)
            .await;
        assert!(
            second.is_empty(),
            "an identical repeat within resume_backoff_ms must be suppressed"
        );
    }

    #[tokio::test]
    async fn dispatch_publish_continues_established_episode_without_a_third_tick() {
        let mut core = test_core(0, "lane_resume_receipt_continuation");
        core.resume_batch = 1;
        let (author, _) = crate::common::keys()[1];
        let (other_sender, _) = crate::common::keys()[2];

        let reference = (author, 5u64, Digest::default());
        let first_ack = {
            let mut agg = core.ack_aggregator.lock();
            agg.record_ack(author, reference.clone())
        };
        assert!(first_ack.availability.is_none());
        let second_ack = {
            let mut agg = core.ack_aggregator.lock();
            agg.record_ack(other_sender, reference.clone())
        };
        let availability = second_ack
            .availability
            .expect("second distinct acker crosses validity (f+1=2)");
        let now = Instant::now();
        core.on_ack_availability(availability, now);
        assert_eq!(core.lm.avail_high(&author), 5);
        assert_eq!(core.lm.own_direct_frontier(&author), 0);

        core.try_resume_request(author, now);
        assert_eq!(
            core.metrics
                .as_ref()
                .unwrap()
                .vantage_lane_resume_requests_sent
                .get(),
            0,
            "first observation must not fire"
        );
        let t1 = now + Duration::from_millis(core.resume_check_period_ms);
        core.try_resume_request(author, t1);
        let sent_after_establish = core
            .metrics
            .as_ref()
            .unwrap()
            .vantage_lane_resume_requests_sent
            .get();
        assert_eq!(
            sent_after_establish, 1,
            "second consecutive tick establishes the episode and fires the first request"
        );

        let header = Header::new_vantage(
            author,
            1,
            std::collections::BTreeMap::new(),
            core.lm.genesis().clone(),
            core.lm.sid().clone(),
        );
        let t2 = t1 + Duration::from_millis(50);
        core.dispatch_inbound(Inbound::Publish(author, header), t2)
            .await;

        assert_eq!(
            core.lm.own_direct_frontier(&author),
            1,
            "the publish must have advanced frontier(author) to 1"
        );
        assert_eq!(
            core.metrics
                .as_ref()
                .unwrap()
                .vantage_lane_resume_requests_sent
                .get(),
            sent_after_establish + 1,
            "receipt of the publish must fire the NEXT request immediately -- no \
             third tick was ever run in this test"
        );
    }

    fn addr_of(core: &VantageCore, peer: PublicKey) -> SocketAddr {
        core.wire
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
            .expect("peer must be an other-primary of this test committee")
    }

    fn intercept_resume_channel(core: &mut VantageCore) -> Receiver<wire::ReplaySend> {
        let (tx, rx) = wire::ReplaySender::channel(usize::MAX, core.wire.codec.clone());
        core.wire.replay_tx = tx;
        rx
    }

    fn break_resume_channel(core: &mut VantageCore) {
        let (tx, rx) = wire::ReplaySender::channel(usize::MAX, core.wire.codec.clone());
        drop(rx);
        core.wire.replay_tx = tx;
    }

    fn enqueue_drops(core: &VantageCore) -> u64 {
        core.metrics
            .as_ref()
            .unwrap()
            .vantage_replay_enqueue_drops_total
            .get()
    }

    fn done_clamped(core: &VantageCore) -> u64 {
        core.metrics
            .as_ref()
            .unwrap()
            .vantage_replay_done_clamped_total
            .get()
    }

    fn nudges_sent(core: &VantageCore) -> u64 {
        core.metrics
            .as_ref()
            .unwrap()
            .vantage_replay_pending_low_nudges_total
            .get()
    }

    fn ttl_expired(core: &VantageCore) -> u64 {
        core.metrics
            .as_ref()
            .unwrap()
            .vantage_replay_inflight_ttl_expired_total
            .get()
    }

    #[tokio::test]
    async fn resume_hello_serves_from_pending_low_regardless_of_inflated_hello_floor() {
        let mut core = test_core(0, "reconnect_b1");
        let (peer, _) = crate::common::keys()[1];

        core.outbox.record(5, Bytes::from_static(b"five"));
        core.outbox.record(10, Bytes::from_static(b"ten"));
        core.outbox.record(100, Bytes::from_static(b"hundred"));

        let peer_addr = addr_of(&core, peer);
        core.wire.dirty_map.lock().insert(peer_addr, 5);

        let mut rx = intercept_resume_channel(&mut core);

        let inflated_floor: View = 100;
        core.dispatch_inbound(Inbound::ResumeHello(inflated_floor, peer), Instant::now())
            .await;

        let sent = rx.try_recv().expect("a Replay must have been enqueued");
        let wire::ReplaySend {
            peer: p,
            msgs,
            done,
            ..
        } = sent;
        assert_eq!(p, peer);
        assert_eq!(
            msgs,
            vec![
                Bytes::from_static(b"five"),
                Bytes::from_static(b"ten"),
                Bytes::from_static(b"hundred"),
            ],
            "the dropped suffix from view 5 must be served in full, regardless of \
             the inflated Hello floor"
        );
        match done {
            PrimaryMessage::VantageReplayDone(end_key, complete, clamped, sender) => {
                assert_eq!(end_key, 101);
                assert!(complete);
                assert!(!clamped);
                assert_eq!(sender, core.name);
            }
            other => panic!("expected VantageReplayDone, got {:?}", other),
        }
        assert!(
            !core.pending_low.contains_key(&peer),
            "a complete serve must clear pending_low"
        );
    }

    #[tokio::test]
    async fn resume_hello_from_a_caught_up_peer_serves_nothing_and_reports_complete() {
        let mut core = test_core(0, "reconnect_caught_up");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        let mut rx = intercept_resume_channel(&mut core);

        core.dispatch_inbound(Inbound::ResumeHello(6, peer), Instant::now())
            .await;

        let wire::ReplaySend { msgs, done, .. } = rx
            .try_recv()
            .expect("a Done-only Replay must still be enqueued");
        assert!(msgs.is_empty());
        match done {
            PrimaryMessage::VantageReplayDone(end_key, complete, clamped, _) => {
                assert_eq!(end_key, 6);
                assert!(complete);
                assert!(!clamped);
            }
            other => panic!("expected VantageReplayDone, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn resume_hello_clamps_to_outbox_floor_and_reports_clamped() {
        let mut core = test_core(0, "reconnect_clamped");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));
        core.outbox.record(50, Bytes::from_static(b"fifty"));
        core.outbox.prune_below(20);

        let mut rx = intercept_resume_channel(&mut core);
        let clamped_metric_before = done_clamped(&core);
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), Instant::now())
            .await;

        let wire::ReplaySend { msgs, done, .. } = rx.try_recv().unwrap();
        assert_eq!(msgs, vec![Bytes::from_static(b"fifty")]);
        match done {
            PrimaryMessage::VantageReplayDone(end_key, complete, clamped, _) => {
                assert_eq!(end_key, 51);
                assert!(complete);
                assert!(
                    clamped,
                    "the requested floor (5) was below outbox_floor (20)"
                );
            }
            other => panic!("expected VantageReplayDone, got {:?}", other),
        }
        assert_eq!(clamped_metric_before + 1, done_clamped(&core));
    }

    #[tokio::test]
    async fn resume_hello_while_in_flight_is_ignored() {
        let mut core = test_core(0, "reconnect_in_flight");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        let mut rx = intercept_resume_channel(&mut core);
        let now = Instant::now();
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), now)
            .await;
        assert!(rx.try_recv().is_ok(), "the first Hello must be served");
        assert!(core.wire.in_flight.lock().contains_key(&peer));

        core.dispatch_inbound(
            Inbound::ResumeHello(5, peer),
            now + Duration::from_millis(10),
        )
        .await;
        assert!(
            rx.try_recv().is_err(),
            "a concurrent Hello while a superset stream is in flight must be ignored, not re-served"
        );
    }

    #[tokio::test]
    async fn resume_hello_served_again_after_in_flight_ttl_expires() {
        let mut core = test_core(0, "reconnect_ttl_expiry");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));
        assert_eq!(core.replay_episode_max_ms, 60_000);

        let mut rx = intercept_resume_channel(&mut core);
        let now = Instant::now();
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), now)
            .await;
        assert!(rx.try_recv().is_ok());

        let before = ttl_expired(&core);
        let later = now + Duration::from_millis(60_001);
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), later)
            .await;
        assert_eq!(
            before + 1,
            ttl_expired(&core),
            "the stale entry must be counted once"
        );
        assert!(
            rx.try_recv().is_ok(),
            "past the TTL, the entry is stale (not genuinely in flight) -- the Hello must be served"
        );
    }

    #[tokio::test]
    async fn try_send_failure_leaves_pending_low_unchanged_and_next_ask_recovers() {
        let mut core = test_core(0, "reconnect_a2");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));
        core.replay_serve_max_bytes = 4;
        let peer_addr = addr_of(&core, peer);
        core.wire.dirty_map.lock().insert(peer_addr, 5);

        break_resume_channel(&mut core);
        let drops_before = enqueue_drops(&core);
        let now = Instant::now();
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), now)
            .await;

        assert_eq!(drops_before + 1, enqueue_drops(&core));
        assert_eq!(
            core.pending_low.get(&peer).copied(),
            Some(5),
            "a failed enqueue must leave pending_low exactly as the dirty-map sweep set it"
        );
        assert!(
            !core.wire.in_flight.lock().contains_key(&peer),
            "a failed enqueue must not mark the stream in-flight either"
        );
        let backoff = Duration::from_millis(core.resume_backoff_ms);
        assert_eq!(
            core.serve_budget.remaining(peer, now, backoff, 4),
            4,
            "failed admission must leave the complete serve budget available"
        );

        let mut rx = intercept_resume_channel(&mut core);
        core.dispatch_inbound(
            Inbound::ResumeHello(5, peer),
            now + Duration::from_millis(10),
        )
        .await;
        let wire::ReplaySend { done, .. } = rx.try_recv().expect("the retried ask must be served");
        assert!(matches!(
            done,
            PrimaryMessage::VantageReplayDone(_, true, false, _)
        ));
        assert!(!core.pending_low.contains_key(&peer));
        assert_eq!(
            core.serve_budget
                .remaining(peer, now + Duration::from_millis(10), backoff, 4),
            0,
            "the successful retry must consume the admitted payload bytes"
        );
    }

    #[tokio::test]
    async fn resume_hello_in_flight_marker_is_present_for_a_task_side_remove_to_find() {
        let mut core = test_core(0, "reconnect_in_flight_inserted");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        let mut rx = intercept_resume_channel(&mut core);
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), Instant::now())
            .await;

        assert!(rx.try_recv().is_ok(), "the ask must have been enqueued");
        assert!(
            core.wire.in_flight.lock().contains_key(&peer),
            "the in-flight marker must be present once a Replay has been enqueued"
        );

        assert!(
            core.wire.in_flight.lock().remove(&peer).is_some(),
            "a task-side remove must always find the entry -- insert-before-\
             enqueue closes the window where it could race an insert that \
             hasn't happened yet"
        );
    }

    #[tokio::test]
    async fn resume_hello_enqueue_failure_leaves_no_stranded_in_flight_entry() {
        let mut core = test_core(0, "reconnect_in_flight_rollback");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        break_resume_channel(&mut core);
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), Instant::now())
            .await;

        assert!(
            !core.wire.in_flight.lock().contains_key(&peer),
            "a failed enqueue must never strand an in-flight entry"
        );
    }

    #[tokio::test]
    async fn replay_done_incomplete_reopens_episode_and_sends_hello_immediately() {
        let mut core = test_core(0, "reconnect_done_continuation");
        let (author, _) = crate::common::keys()[1];
        assert!(!core.replay_episodes.is_open(&author));

        core.dispatch_inbound(
            Inbound::ReplayDone(42, false, false, author),
            Instant::now(),
        )
        .await;

        assert!(
            core.replay_episodes.is_open(&author),
            "an incomplete Done must (re)open our own episode toward the author"
        );
    }

    #[tokio::test]
    async fn replay_done_complete_closes_the_episode() {
        let mut core = test_core(0, "reconnect_done_complete");
        let (author, _) = crate::common::keys()[1];
        core.replay_episodes.open(author, Instant::now());
        assert!(core.replay_episodes.is_open(&author));

        core.dispatch_inbound(Inbound::ReplayDone(42, true, false, author), Instant::now())
            .await;

        assert!(!core.replay_episodes.is_open(&author));
    }

    #[tokio::test]
    async fn replay_done_clamped_bumps_the_metric() {
        let mut core = test_core(0, "reconnect_done_clamped_metric");
        let (author, _) = crate::common::keys()[1];
        let before = done_clamped(&core);

        core.dispatch_inbound(Inbound::ReplayDone(42, true, true, author), Instant::now())
            .await;

        assert_eq!(before + 1, done_clamped(&core));
    }

    #[tokio::test]
    async fn nudge_fires_after_partial_serve_despite_a_forged_complete_done() {
        let mut core = test_core(0, "reconnect_a3_nudge");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));
        core.outbox.record(10, Bytes::from_static(b"ten"));

        let mut rx = intercept_resume_channel(&mut core);
        let now = Instant::now();

        core.replay_serve_max_bytes = 1;
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), now)
            .await;
        let wire::ReplaySend { done, .. } = rx.try_recv().unwrap();
        assert!(matches!(
            done,
            PrimaryMessage::VantageReplayDone(6, false, false, _)
        ));
        assert_eq!(core.pending_low.get(&peer).copied(), Some(6));

        core.dispatch_inbound(Inbound::ReplayDone(999, true, false, peer), now)
            .await;
        assert_eq!(core.pending_low.get(&peer).copied(), Some(6));

        let nudges_before = nudges_sent(&core);
        core.maybe_nudge(
            peer,
            now + Duration::from_millis(10),
            Duration::from_millis(4_000),
        )
        .await;
        assert_eq!(
            nudges_before,
            nudges_sent(&core),
            "too soon since the serve itself"
        );

        let later = now + Duration::from_millis(65_000);
        core.maybe_nudge(peer, later, Duration::from_millis(4_000))
            .await;
        assert_eq!(
            nudges_before + 1,
            nudges_sent(&core),
            "the nudge must fire after backoff while the peer gap remains"
        );
    }

    #[tokio::test]
    async fn maybe_nudge_is_a_no_op_when_pending_low_is_unset() {
        let mut core = test_core(0, "reconnect_nudge_noop");
        let (peer, _) = crate::common::keys()[1];
        let before = nudges_sent(&core);

        core.maybe_nudge(peer, Instant::now(), Duration::from_millis(4_000))
            .await;

        assert_eq!(
            before,
            nudges_sent(&core),
            "no pending_low entry -- nothing to nudge"
        );
    }

    #[tokio::test]
    async fn broadcast_recorded_with_replay_disabled_skips_outbox_and_goes_durable() {
        let mut core = test_core(0, "broadcast_recorded_replay_disabled");
        core.reconnect_replay = false;
        assert!(core.wire.cancel_handlers.is_empty());
        let other_primaries = core.wire.other_primaries.len();
        assert!(
            other_primaries > 0,
            "test committee must have other primaries"
        );

        core.broadcast_recorded(PrimaryMessage::VantageWish(7, core.name))
            .await;

        assert!(
            core.outbox.slice_from(0).next().is_none(),
            "the outbox must stay empty when replay is disabled"
        );
        assert_eq!(
            core.wire.cancel_handlers.len(),
            other_primaries,
            "a durable broadcast allocates one cancel handler per other primary -- \
             a volatile send would allocate none at all"
        );
    }

    #[tokio::test]
    async fn resume_hello_is_a_no_op_when_replay_is_disabled() {
        let mut core = test_core(0, "resume_hello_replay_disabled");
        core.reconnect_replay = false;
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        let mut rx = intercept_resume_channel(&mut core);
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), Instant::now())
            .await;

        assert!(
            rx.try_recv().is_err(),
            "replay must not be enqueued while replay is disabled"
        );
        assert!(
            !core.pending_low.contains_key(&peer),
            "pending_low must not change"
        );
        assert!(
            !core.wire.in_flight.lock().contains_key(&peer),
            "the in-flight map must not change"
        );
    }

    #[tokio::test]
    async fn replay_done_is_a_no_op_when_replay_is_disabled() {
        let mut core = test_core(0, "replay_done_replay_disabled");
        core.reconnect_replay = false;
        let (author, _) = crate::common::keys()[1];

        core.dispatch_inbound(
            Inbound::ReplayDone(42, false, false, author),
            Instant::now(),
        )
        .await;

        assert!(
            !core.replay_episodes.is_open(&author),
            "an incomplete Done must not open an episode while disabled"
        );
    }

    #[tokio::test]
    async fn resume_tick_replay_effects_are_inert_when_disabled() {
        let mut core = test_core(0, "resume_tick_replay_disabled");
        core.reconnect_replay = false;
        let (author, _) = crate::common::keys()[1];
        core.pending_low.insert(author, 5);
        let now = Instant::now();
        let backoff = Duration::from_millis(core.resume_backoff_ms);
        let max_age = Duration::from_millis(core.replay_episode_max_ms);
        let nudges_before = nudges_sent(&core);

        core.resume_tick_replay_effects(author, now, backoff, max_age)
            .await;

        assert!(
            !core.replay_episodes.is_open(&author),
            "no episode must open while disabled"
        );
        assert_eq!(
            nudges_before,
            nudges_sent(&core),
            "no nudge must fire while disabled"
        );
    }
}
