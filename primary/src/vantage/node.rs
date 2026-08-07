// PHASE4-SPEC.md §1 -- the single spawned `VantageCore` task: owns `LaneManager` +
// `Repairer` + `AgbEngine` + `Frontier` + `Cursor` and executes their returned
// `Effect`s. One owning loop avoids shared locks entirely (the components are
// synchronous/effect-returning state machines); the shared `BlockCache` mutex stays as
// the one piece of genuinely shared state (§3.3's cross-notification hook).

use crate::messages::{Ack, Header};
use crate::primary::{Height, PrimaryMessage, View, CHANNEL_CAPACITY};
use crate::vantage::agb::{
    AgbEngine, DigestStatements, EchoDigest, EchoOut, ProposalOut, ReadyDigest, ReadyOut,
    TimerKind, ViewProposal,
};
use crate::vantage::block::{self, BlockRef};
use crate::vantage::control::{ControlLog, ControlProposal, Round};
use crate::vantage::cursor::Cursor;
use crate::vantage::frontier::Frontier;
use crate::vantage::lanes::{
    AckAggregator, AckAvailability, AvailEntry, BlockCache, LaneManager, SharedAckAggregator,
    SharedBlocks,
};
use crate::vantage::outbox::Outbox;
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::payload::PayloadIo;
use crate::vantage::repair::Repairer;
use crate::vantage::resolve::Resolver;
use crate::vantage::resume::{
    in_flight_state, InFlightEntry, InFlightState, NudgeMemo, ReplayEpisodes, ResumeServe,
    ResumeTrigger, ServeBudget,
};
use crate::vantage::wire::{self, Wire};
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
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Inbound messages routed to `VantageCore`, either from the network
/// (`VantageReceiverHandler`) or from `PrimaryReceiverHandler`'s `HeadersRequest` arm
/// (shared wire variant with Autobahn).
///
/// `Clone` (PHASE6-SPEC.md §8): the Byzantine test suite's `harness::deliver_only_to`
/// needs to hand the identical constructed message to several distinct node indices
/// (e.g. an equivocating leader's two different proposals each going to a disjoint
/// subset) -- every constituent field type is already `Clone`, so this is a free,
/// behavior-neutral derive (production code never clones an `Inbound`).
/// Capacity of the SERVICE-REQUEST queue (`VantageReceiverHandler::tx_bulk`).
///
/// Sized for the request flood a single node can face: at n=100, up to 99 peers may be
/// asking it to serve a lane/body/header at once, repeatedly, and each entry is small
/// (`LaneResume` is two keys and a height). Generous headroom is therefore cheap here --
/// what must stay bounded is not this queue's length but the WORK it induces, which
/// `Parameters::resume_max_concurrent` caps on the requester side.
///
/// The first version of this split sized it at 128 and routed served payload here too.
/// The n=100 run of 2026-08-07 showed why that was wrong: lagging nodes dropped 90
/// messages/second (53k median, 130k peak) and received only 6% of blocks and 4% of
/// avail, because incoming SERVE RESPONSES -- the very data they needed to catch up --
/// queued behind, and were dropped alongside, the flood of requests the 78 healthy peers
/// were making OF them. Head-of-line blocking inside the recovery queue itself. See
/// `Inbound::is_bulk` for the corrected axis.
const BULK_CHANNEL_CAPACITY: usize = 2048;

#[derive(Debug, Clone)]
pub enum Inbound {
    /// `Header(h, false)`: publish path. Provenance is claimed-by-author (D4 ruling,
    /// PHASE3-NOTES.md §5/§11) -- there is no channel identity to compare `h.author`
    /// against, so the dispatcher always passes `h.author` itself as the trusted
    /// sender.
    Publish(PublicKey, Header),
    /// `Header(h, true)`: serve path.
    Serve(Header),
    HeadersRequest(Vec<Digest>, PublicKey),
    /// Production network ACKs are accumulated by `AckAggregator` before reaching the
    /// core. This compact mark is the only ACK-derived fact the hot protocol path needs.
    AckAvailability(AckAvailability),
    /// Test/direct-injection compatibility path. `VantageReceiverHandler` does not emit
    /// this in production.
    Ack(Ack),
    /// Optional ack-watermark front-end (`Parameters::ack_watermarks`) -- see
    /// `LaneManager::resolve_watermark`. `sender` is the broadcasting party's declared
    /// identity, the same D4-trust/MAC-binding model as `Ack`'s own `sender` field.
    Avail(Vec<AvailEntry>, PublicKey),
    /// `VantagePropose`/`VantageProposeBatch` carry no sender field on the wire (§2)
    /// -- see `VantageCore::dispatch_inbound` for how the trusted sender is derived.
    /// PHASE7: `ProposalOut` -- `Single` normalizes `VantagePropose`, `Batch`
    /// normalizes `VantageProposeBatch`.
    Propose(ProposalOut),
    /// PHASE7: `EchoOut` -- `Single` normalizes `VantageEcho`, `Batch` normalizes
    /// `VantageEchoBatch`.
    Echo(EchoOut),
    /// PHASE5-SPEC.md §2: trailing field is the piggybacked wish watermark (D5-2).
    EchoSkip(View, PublicKey, View),
    /// PHASE7: `ReadyOut` -- `Single` normalizes `VantageReady`, `Batch` normalizes
    /// `VantageReadyBatch`.
    Ready(ReadyOut),
    /// PHASE5-SPEC.md §2: trailing field is the piggybacked wish watermark (D5-2).
    NoReady(View, PublicKey, View),
    /// PHASE5-SPEC.md §2: a standalone `VantageWish` (W2 amplification).
    Wish(View, PublicKey),
    /// PHASE6-SPEC.md §5.
    CompReport(View, Digest, PublicKey),
    /// `ControlInit`/`ControlInitBatch` carry no sender field on the wire (same D4
    /// class as `Propose`) -- the trusted sender is derived as this round's control
    /// leader by `VantageCore::dispatch_inbound`. PHASE7: `Option<ProposalOut>`,
    /// mirroring `Propose`'s generalization.
    ControlInit(ControlProposal, Option<ProposalOut>),
    ControlEcho(PublicKey, ControlProposal),
    ControlReady(PublicKey, ControlProposal),
    ControlCommit(PublicKey, Round),
    ControlTimeoutVote(PublicKey, Round),
    ControlTimeoutAccept(PublicKey, Round),
    ControlFetch(View, Digest, PublicKey),
    /// PHASE7: `ProposalOut` -- `Single` normalizes `ControlServe`, `Batch`
    /// normalizes `ControlServeBatch`.
    ControlServe(View, ProposalOut),
    /// signature-free.tex's "Grounded post-ready skip" (par:skip-seal): a
    /// `VantageSkipVote`. Carries no wish watermark (unlike `EchoSkip`/`NoReady`
    /// above) -- the paper never requires one, and this is a rare, one-shot-per-target
    /// crash-fallback statement, not a frequent response worth piggybacking on.
    SkipVote(View, PublicKey),
    /// signature-free.tex §8.3 "Digest-named AGB statements" (`Parameters::
    /// digest_statements`) -- reception is unconditional regardless of this party's
    /// OWN flag setting (see `vantage::agb::DigestStatements`'s own module doc
    /// comment). `VantageEchoDigest`/`VantageReadyDigest` carry their own `sender`/
    /// `wish` fields directly (mirroring `Echo`/`Ready` themselves), unlike
    /// `Propose`/`ControlInit` above.
    EchoDigest(EchoDigest),
    ReadyDigest(ReadyDigest),
    /// A peer's `VantageBodyFetch(view, digest, requester)`.
    BodyFetch(View, Digest, PublicKey),
    /// A peer's `VantageBodyServe(view, proposal)` answer.
    BodyServe(View, ViewProposal),
    /// Mechanism A (sender-side lane resume, `vantage::resume`): a peer's
    /// `VantageLaneResume(author, from, requester)` -- `author` is the LANE this
    /// message is about (checked against `self.name` in `dispatch_inbound`, not
    /// trusted merely because it decoded), `from` is the requested resume-from
    /// height, and `requester` is who's asking (a real, declared, MAC/membership-
    /// checked sender -- same D4 class as `HeadersRequest`'s own `requestor` field).
    LaneResume(PublicKey, Height, PublicKey),
    /// reconnect-replay plan §7: a peer's `VantageResumeHello(floor hint, sender)`
    /// -- a SEPARATE mechanism from `LaneResume` above (see `vantage::resume`'s own
    /// module doc comment); `sender` is a real, declared, membership-checked
    /// sender, same D4 class as `LaneResume`'s own `requester` field.
    ResumeHello(View, PublicKey),
    /// A peer's `VantageReplayDone(end_key, complete, clamped, sender)`.
    ReplayDone(View, bool, bool, PublicKey),
}

impl Inbound {
    /// Is this a SERVICE REQUEST from a peer -- work this node may decline -- rather
    /// than data this node needs?
    ///
    /// The axis is "who is owed something", NOT "is it recovery traffic":
    ///
    ///   bulk (droppable): requests OTHERS make OF US. `LaneResume`, `HeadersRequest`,
    ///     `BodyFetch`, `ControlFetch`, `ResumeHello`. Declining one costs the REQUESTER
    ///     a retry on its next tick; it costs us nothing, and a node too busy to serve
    ///     is exactly a node that should be declining.
    ///
    ///   consensus-class: RESPONSES to requests WE made -- `Serve`, `BodyServe`,
    ///     `ControlServe`, `ReplayDone` -- plus all AGB/control/availability traffic.
    ///     A dropped response is data we asked for and are blocked on.
    ///
    /// The first version of this split used "is it recovery traffic" instead, which put
    /// served payload in the droppable queue. The n=100 run of 2026-08-07 showed the
    /// consequence: lagging nodes dropped 90 messages/second (53k median, 130k peak) and
    /// received only 6% of blocks and 4% of avail, because their incoming serve
    /// RESPONSES sat behind -- and were dropped alongside -- the flood of requests the 78
    /// healthy peers were making of them. The node least able to spare capacity was the
    /// one throwing away its own catch-up data. Splitting on "requests of us" vs
    /// "responses to us" makes the droppable class exactly the one whose loss is free.
    ///
    /// In-flight serve responses stay bounded without a small queue:
    /// `Parameters::resume_max_concurrent` (8) x `resume_batch` (64) caps them at ~512,
    /// inside `CHANNEL_CAPACITY` (1000).
    ///
    /// `Publish` is deliberately consensus-class: it is the ORGANIC car-delivery path --
    /// and also how a resume batch is delivered -- i.e. exactly the traffic whose
    /// starvation collapsed the n=100 run.
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
}

/// Network receiver handler for the Vantage assembly's `primary_to_primary` port.
/// Deliberately a distinct type from Autobahn's `PrimaryReceiverHandler` (which stays
/// byte-identical, untouched) -- the two assemblies never share a handler.
#[derive(Clone)]
pub struct VantageReceiverHandler {
    pub tx: Sender<Inbound>,
    /// Second, SEPARATE inbound queue for bulk recovery traffic (`Inbound::is_bulk`).
    ///
    /// Vantage's n=100 collapse of 2026-08-07: lane resume ignited on ~every lane
    /// (122,736 blocks re-served per node; ZERO at n=50) and the served payload shared
    /// this one 1000-slot channel with AGB echoes, acks and control messages. The queue
    /// pinned at capacity on 100/100 nodes, `tx.send().await` below then blocked the
    /// network receiver task -- which stops it reading frames at all -- and organic
    /// block delivery fell to ~5% of published. More holes produced more resume, which
    /// is what made the collapse self-sustaining.
    ///
    /// Splitting the queues means recovery traffic can never consume the budget
    /// consensus messages need: a resume backlog now fills only its own (smaller)
    /// channel, and bulk enqueues are `try_send` -- dropped rather than allowed to
    /// stall the reader, which is sound because every message routed here is
    /// re-requestable by construction (a resume/serve/fetch response).
    pub tx_bulk: Sender<Inbound>,
    pub ack_aggregator: SharedAckAggregator,
    /// METRICS-DASHBOARD-SPEC.md §1: `None` only in tests that construct this handler
    /// directly without wiring metrics (matches `VantageCore`'s own optional-handle
    /// convention) -- production (`Primary::spawn`) always passes `Some`.
    pub metrics: Option<Arc<Metrics>>,
}

#[async_trait]
impl MessageHandler for VantageReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // The ack is now sent by `network::Receiver` itself, once per received FRAME
        // rather than once per `dispatch` call -- required for batching (several
        // logical messages can share one frame, and only one ack may be sent per
        // frame). See `Receiver::acks`'s doc comment.

        let message: PrimaryMessage = bincode::deserialize(&serialized)?;

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
                let result = {
                    let mut aggregator = self.ack_aggregator.lock();
                    aggregator.record_ack(a.sender, a.reference())
                };
                if !result.accepted {
                    if let Some(metrics) = &self.metrics {
                        metrics.vantage_rejected_nonmember_total.inc();
                    }
                    return Ok(());
                }
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_acks_received.inc();
                }
                let Some(availability) = result.availability else {
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
            // PHASE7: the vector-`M` counterparts -- normalized into the SAME
            // `Inbound` variants above via `ProposalOut`/`EchoOut`/`ReadyOut`'s
            // `Batch` case, so `dispatch_inbound` needs no new arms, only a shape
            // dispatch at each existing one.
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
            // signature-free.tex §8.3 "Digest-named AGB statements": routed
            // unconditionally regardless of this party's OWN `digest_statements`
            // setting -- see `vantage::agb::DigestStatements`'s own module doc
            // comment for why reception is never flag-gated.
            PrimaryMessage::VantageEchoDigest(d) => Inbound::EchoDigest(d),
            PrimaryMessage::VantageReadyDigest(d) => Inbound::ReadyDigest(d),
            PrimaryMessage::VantageBodyFetch(v, d, r) => Inbound::BodyFetch(v, d, r),
            PrimaryMessage::VantageBodyServe(v, p) => Inbound::BodyServe(v, p),
            // Mechanism A (`vantage::resume`).
            PrimaryMessage::VantageLaneResume(author, from, requester) => {
                Inbound::LaneResume(author, from, requester)
            }
            // reconnect-replay plan §7 (a separate mechanism from `VantageLaneResume`
            // above -- see `vantage::resume`'s own module doc comment).
            PrimaryMessage::VantageResumeHello(floor, sender) => {
                Inbound::ResumeHello(floor, sender)
            }
            PrimaryMessage::VantageReplayDone(end_key, complete, clamped, sender) => {
                Inbound::ReplayDone(end_key, complete, clamped, sender)
            }
            // Autobahn-only variants never reach the Vantage assembly's port; ignore
            // rather than panic (defense in depth against a misrouted message).
            _ => return Ok(()),
        };
        // Bulk recovery traffic goes to its own queue, non-blocking: a full bulk
        // channel drops the message rather than stalling this receiver task, and the
        // requester re-asks on its next resume tick. Consensus traffic keeps the
        // original awaiting send on its own channel, so nothing about its delivery
        // guarantees changes -- it simply no longer queues behind re-served payload.
        if inbound.is_bulk() {
            if self.tx_bulk.try_send(inbound).is_err() {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_bulk_inbound_dropped_total.inc();
                }
            }
            return Ok(());
        }
        self.tx
            .send(inbound)
            .await
            .expect("Failed to send vantage message");
        Ok(())
    }
}

pub struct VantageCore {
    name: PublicKey,
    /// SECURITY (Fable audit): the trusted committee-membership set, populated once at
    /// `spawn`/`build` time from `committee.authorities` before it's consumed building
    /// the sub-engines. `dispatch_inbound` checks every wire-declared sender against
    /// this BEFORE any census/count path sees the message -- wire messages are
    /// unsigned, so without this gate a single Byzantine node could emit messages
    /// under arbitrarily many fabricated non-committee sender keys, each counted once
    /// by the dedup-only census helpers (AGB echo/ready, control-log Bracha/timeout/
    /// commit, comp-reports, lane availability), inflating any party-count quorum.
    /// `Pacemaker::on_wish` already carries an equivalent members-only check (kept, as
    /// defense in depth) -- this field makes that check the single, centralized
    /// choke point for every other wire-message path too.
    members: HashSet<PublicKey>,
    lm: LaneManager,
    ack_aggregator: SharedAckAggregator,
    rep: Repairer,
    agb: AgbEngine,
    frontier: Frontier,
    cursor: Cursor,
    pacemaker: Pacemaker,
    resolver: Resolver,
    control: ControlLog,
    /// signature-free.tex §8.3 "Digest-named AGB statements" (`Parameters::
    /// digest_statements`) -- the reception-side translation layer sitting between
    /// the wire and `agb`. See `vantage::agb::DigestStatements`'s own module doc
    /// comment for the architecture/correctness rationale.
    digest_stmts: DigestStatements,

    /// Network/wire-transport state (typed senders, MAC-auth keying, cancel-handler
    /// bookkeeping, resolved addresses) -- factored out into `Wire` (`vantage::wire`)
    /// so a second consensus protocol can reuse it. See that module for the per-field
    /// security/perf rationale (carried over verbatim from this struct's previous
    /// copy of each field).
    wire: Wire,

    header_size: usize,
    max_header_delay: u64,
    digests: Vec<(Digest, WorkerId)>,
    payload_size: usize,

    /// `Parameters::ack_watermarks`: when `true`, the N3 per-block ack broadcast is
    /// suppressed at EXECUTION time (`execute`'s `Effect::BroadcastAck` arm) -- the
    /// local self-ack path (`record_local_ack`) still runs unconditionally either way,
    /// preserving this party's own counting toward its own `AckAggregator` exactly.
    /// `LaneManager` itself never sees this flag: it keeps emitting `Effect::
    /// BroadcastAck` on every N3 confirmation exactly as before, byte-identically.
    ack_watermarks: bool,
    /// The ack-watermark broadcast period, ms -- irrelevant when `ack_watermarks` is
    /// off (`run` never even constructs the periodic tick in that case).
    ack_watermark_period_ms: u64,

    /// `Parameters::digest_statements`: when `true`, `execute`'s `Effect::
    /// BroadcastEcho`/`Effect::BroadcastReady` arms (the `Single`, non-batch case)
    /// send the compact `VantageEchoDigest`/`VantageReadyDigest` wire message
    /// instead of the full by-value one. Emission-only gate: `false` (the default)
    /// is byte-identical to today (see `vantage::agb::DigestStatements`'s own module
    /// doc comment) -- reception of either wire encoding is NEVER gated by this
    /// flag, on this or any other party.
    digest_statements: bool,

    /// Mechanism A (sender-side lane resume, `vantage::resume`): the requester-side
    /// per-lane trigger memo (two-consecutive-ticks persistence + request backoff).
    /// Unconditional protocol behavior (no flag, unlike `ack_watermarks`/
    /// `digest_statements` above) -- every party always runs this check.
    resume_trigger: ResumeTrigger,
    /// Mechanism A: the author-side per-requester serve dedup memo.
    resume_serve: ResumeServe,
    /// `Parameters::resume_check_period_ms` -- the `run` loop's `resume_tick` period.
    resume_check_period_ms: u64,
    /// `Parameters::resume_backoff_ms` -- shared by both `resume_trigger` (request
    /// rate limit) and `resume_serve` (serve rate limit).
    resume_backoff_ms: u64,
    /// `Parameters::resume_batch` -- the maximum own blocks served per resume batch.
    resume_batch: u64,

    /// KNOB 1 (measurement ablation): `Parameters::reconnect_replay`'s own copy --
    /// see that field's doc comment for the full rationale (isolating this
    /// mechanism's own effect from the `retry_backoff_max_ms` cap change that
    /// landed alongside it). `true` (the default) is today's existing behavior,
    /// byte-identical. Consulted by `broadcast_recorded` (the single choke point
    /// every one-shot AGB/consensus broadcast passes through),
    /// `resume_tick_replay_effects`/the `run` loop's `reconnect_rx` arm, and
    /// `dispatch_inbound`'s `ResumeHello`/`ReplayDone` arms. Mechanism A
    /// (`vantage::resume`'s `ResumeTrigger`/`ResumeServe`, `try_resume_request`) is
    /// NEVER gated by this flag -- it is a separate mechanism, not part of this
    /// ablation.
    reconnect_replay: bool,
    /// reconnect-replay plan §5 (server-authoritative floor, v3) -- a SEPARATE
    /// mechanism from Mechanism A above (see `vantage::resume`'s own module doc
    /// comment): every one-shot broadcast this node has sent volatile
    /// (`broadcast_recorded`), retained for possible replay. `Parameters::
    /// outbox_max_bytes`-capped. Always constructed (even when `reconnect_replay`
    /// is `false`) but then stays permanently empty, since `broadcast_recorded`
    /// never records into it while disabled.
    outbox: Outbox,
    /// §2.4: per peer X, the lowest filing key X may be missing -- the
    /// authoritative serve floor (absent entry = `None`, "no known gap"). Updated
    /// ONLY by the dirty-map sweep (`sweep_dirty_map`, min-merge) and by a
    /// successfully-enqueued replay's own end (`on_resume_hello`, §14 A2/A4) --
    /// NEVER by an ordinary broadcast, keeping steady-state cost at zero. `HashMap`,
    /// not `BTreeMap`: committee-bounded (one entry per peer), not view-keyed.
    pending_low: HashMap<PublicKey, View>,
    /// §2.6: requester-side "do we have an open ask toward peer X" episode state.
    replay_episodes: ReplayEpisodes,
    /// §6: author-side per-peer served-bytes rolling-window budget.
    serve_budget: ServeBudget,
    /// §2.6/§14 A3: author-side "when did we last serve-or-nudge peer X" cooldown.
    nudge_memo: NudgeMemo,
    /// `Parameters::replay_history_views` -- the outbox's own age-based retention
    /// window (views behind `own_watermark`), consulted by `collect_internal_
    /// garbage`. Clamped to >= 1 at `build` time, mirroring `gc_window`'s identical
    /// clamp for the identical reason (a window of 0 would prune the view a
    /// broadcast was just filed under, in the same tick it was filed).
    replay_history_views: View,
    /// `Parameters::replay_serve_max_bytes` -- the author-side per-peer served-bytes
    /// budget per rolling `resume_backoff_ms` window (§6).
    replay_serve_max_bytes: usize,
    /// `Parameters::replay_episode_max_ms` -- the requester-side episode expiry
    /// valve AND (audit-3 A6) the author-side in-flight-replay-stream TTL; see
    /// `Parameters::replay_episode_max_ms`'s own doc comment for why the two share
    /// one constant.
    replay_episode_max_ms: u64,

    /// §10 timer queue: (deadline, view, kind), drained by the run loop's `sleep_until`
    /// branch (message channels are checked first every iteration -- §5's tie-break
    /// rule: "drain message queues before taking a timer branch"). D7-4 (PHASE7-PREP-
    /// NOTES.md): a min-heap by deadline (`Reverse` makes `BinaryHeap`, a max-heap by
    /// default, pop smallest-`Instant`-first) -- `peek()`/`pop()` are O(1)/O(log n),
    /// replacing the previous plain `Vec`'s O(n) `next_deadline` rescan on every
    /// single message the node processes. Deterministic-equivalent: the identical
    /// timers still fire at the identical deadlines producing the identical effects
    /// (once past the lazy stale-discard below, itself also a no-op-preserving
    /// optimization) -- only the internal data structure's complexity changes.
    timers: BinaryHeap<Reverse<(Instant, View, TimerKind)>>,
    /// PHASE6-SPEC.md §5: the control-round timer queue, mirroring `timers` exactly
    /// (kept separate rather than folding `Round` into the `View`-typed queue above --
    /// the two counters are semantically distinct and this avoids a confusing shared
    /// currency). D7-4: same min-heap fix as `timers`.
    control_timers: BinaryHeap<Reverse<(Instant, Round)>>,

    /// D1 payload-sync bookkeeping and commit-notification output state -- factored
    /// out into `PayloadIo` (`vantage::payload`) so a second consensus protocol can
    /// reuse it. See that module for the per-field rationale (carried over verbatim
    /// from this struct's previous copy of each field).
    payload: PayloadIo,

    /// Vantage internal-state retention window, in VIEWS: once the resolver has proven a
    /// contiguous resolved prefix, component state below `resolved_watermark - gc_window`
    /// can be dropped.
    ///
    /// Sourced from `Parameters::vantage_gc_window_views`, NOT from `gc_depth`. It
    /// originally read `gc_depth`, which is documented and consumed as a depth in Autobahn
    /// ROUNDS -- a different counter with a different cadence -- so the same integer was
    /// silently sizing two unrelated windows and an operator tuning Autobahn's GC was
    /// resizing Vantage's retention.
    gc_window: View,
    last_gc_floor: View,

    /// PHASE7-PREP-NOTES.md Finding A: kept for `sample_metrics`'s 1s progress-gauge
    /// tick -- a separate clone from the ones already handed to `lm`/`rep`/`agb`
    /// (`Arc<Metrics>` is freely shareable; this is not new metrics plumbing, just one
    /// more clone of the same handle every node already builds).
    metrics: Option<Arc<Metrics>>,
    /// Fable perf audit item 7 (cheap subset): `metrics.utilization_timer`'s
    /// `"inbound_dispatch"`/`"payload_sync"`/`"timer_firing"`/`"effect_execution"`
    /// labels, resolved once (first use) and cached -- see
    /// `cached_utilization_timer`'s doc comment. `None` until first resolved, same as
    /// `metrics` itself being `None` in tests that build a `VantageCore` without one.
    ut_inbound_dispatch: Option<IntCounter>,
    ut_payload_sync: Option<IntCounter>,
    ut_timer_firing: Option<IntCounter>,
    ut_effect_execution: Option<IntCounter>,
    /// Fable perf audit (measurement gap): the four `run` branches that carried no
    /// `utilization_timer` scope at all, so their cost was invisible and the labeled
    /// sections did not add up to the core's real busy time.
    ut_avail_flush: Option<IntCounter>,
    ut_resume_tick: Option<IntCounter>,
    ut_metrics_tick: Option<IntCounter>,
    ut_header_seal: Option<IntCounter>,
    /// Running max of `rx_vantage.len()` since the last 1 Hz publish -- see
    /// `Metrics::core_queue_peak`.
    queue_len_peak: usize,
    /// Set whenever something happens that warrants an `AgbEngine::recheck_all`, and
    /// serviced ONCE at the end of `execute`'s drain rather than at every trigger.
    ///
    /// Why: with ack watermarks on, every peer broadcasts its full per-author watermark
    /// map every `ack_watermark_period_ms`, and `credit_refs` credits those refs one at
    /// a time -- so the old per-ref `recheck_all` ran its O(n^2)-per-view scan
    /// n*(n-1)/period times a second on this single-threaded core (~49k full rechecks/s
    /// at n=50). Coalescing is sound because `recheck_all` is idempotent and
    /// order-independent (see its own doc comment): running it once after a batch of
    /// credits reaches the same fixpoint as running it after each individual credit.
    recheck_pending: bool,
}

/// `VantageCore::build`'s return shape: the constructed core, channel ends `spawn`
/// still needs to wire up (or a test needs to drive directly), and the shared
/// ACK accumulator used by both local ACK feedback and the network handler.
/// reconnect-replay plan §2.1/§7 (audit F8): also carries `reconnect_rx`, the
/// receiving half of the MAIN pool's reconnect-event channel -- `run` selects on it
/// directly (§2.6 trigger (i)).
type BuildOutput = (
    VantageCore,
    Receiver<Inbound>,
    // Bulk recovery queue -- see `VantageReceiverHandler::tx_bulk`.
    Receiver<Inbound>,
    Receiver<(Digest, Digest, WorkerId)>,
    Sender<Inbound>,
    Sender<Inbound>,
    SharedAckAggregator,
    Receiver<SocketAddr>,
);

impl VantageCore {
    // clippy::too_many_arguments: see primary/src/committer.rs's identical justification.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        metrics: Option<Arc<Metrics>>,
        rx_our_digests: Receiver<(Digest, WorkerId)>,
        tx_output: Sender<Header>,
    ) -> (Sender<Inbound>, Sender<Inbound>, SharedAckAggregator) {
        let (
            core,
            rx_vantage,
            rx_bulk,
            rx_payload_ready,
            tx_vantage,
            tx_bulk,
            ack_aggregator,
            reconnect_rx,
        ) = Self::build(name, committee, parameters, store, metrics, tx_output);
        tokio::spawn(core.run(
            rx_vantage,
            rx_bulk,
            rx_our_digests,
            rx_payload_ready,
            reconnect_rx,
        ));
        (tx_vantage, tx_bulk, ack_aggregator)
    }

    /// Everything `spawn` used to do up through constructing `core`, split out purely
    /// so tests can obtain a real `VantageCore` and drive `dispatch_inbound` directly
    /// without a live tokio task/network -- `spawn` itself is otherwise byte-identical
    /// (same construction, same order), just calling this then spawning `core.run`.
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
        // Bulk recovery queue, deliberately SMALLER than the consensus queue: its whole
        // purpose is to bound how much re-served payload can be in flight toward the
        // core at once. Oversizing it would just recreate the shared-budget problem it
        // exists to remove (see `VantageReceiverHandler::tx_bulk`).
        let (tx_bulk, rx_bulk) = channel(BULK_CHANNEL_CAPACITY);
        let (tx_payload_ready, rx_payload_ready) = channel(CHANNEL_CAPACITY);

        // SECURITY (Fable audit): captured before `committee` is consumed below building
        // the sub-engines -- the single source of truth `dispatch_inbound` checks every
        // wire-declared sender against.
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
        let control = ControlLog::new(name, committee.clone(), sid, parameters.delta_ms);

        let other_primaries: Vec<(PublicKey, SocketAddr)> = committee
            .others_primaries(&name)
            .into_iter()
            .map(|(pk, addr)| (pk, addr.primary_to_primary))
            .collect();
        // Fable perf audit item 5a: precomputed once here, see the field's own doc
        // comment.
        let other_primary_addrs: Vec<SocketAddr> =
            other_primaries.iter().map(|(_, a)| *a).collect();
        // reconnect-replay plan §2.3/§7: the exact reverse of `other_primaries`,
        // precomputed once (mirrors `other_primary_addrs`'s own reasoning).
        let addr_to_peer: HashMap<SocketAddr, PublicKey> = other_primaries
            .iter()
            .map(|(pk, addr)| (*addr, *pk))
            .collect();
        // reconnect-replay plan §2.3/§6/§7: the two shared maps the MAIN pool's
        // `ReliableSender` (drop map) and the resume-sender task (in-flight map)
        // feed/consume from a different task than this one -- see `Wire::dirty_map`/
        // `Wire::in_flight`'s own doc comments.
        let dirty_map: DirtyMap = Arc::new(Mutex::new(HashMap::new()));
        let in_flight: wire::InFlightMap = Arc::new(Mutex::new(HashMap::new()));
        // reconnect-replay plan §2.1/§7: capacity >= committee size (design doc §7)
        // -- a prompt-Hello latency optimization only, never load-bearing (a
        // dropped/full-channel event is simply recovered by the next `resume_tick`).
        let (reconnect_tx, reconnect_rx) = channel(committee.size().max(1));

        // Data-plane withholding fault injector (`--withhold`): resolved once here,
        // same convention as `latency_map` just below -- `None` (the default, and
        // always the case when `--withhold` is 0) means this node's header broadcasts
        // are untouched (see `Wire::withheld_header_dests`'s own doc comment).
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

        // PHASE7-PREP-NOTES.md (WAN-shaped local runs, optional item): resolved once,
        // relative to OUR OWN committee index -- empty (== current behavior) unless
        // `--latency-table`/`--mimic-latency-ms` set `parameters.latency_table`. Our
        // own worker addresses are never keys in this map (`Committee::latency_map`
        // always skips `other == myself`), so `worker_network` below stays undelayed.
        let latency_map = parameters
            .latency_table
            .as_deref()
            .map(|table| committee.latency_map(&name, table))
            .unwrap_or_default();

        // Transport-level batching, resolved once (mirrors `latency_map`'s own
        // resolve-once-at-spawn convention).
        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        // Mechanism A (sender-side lane resume, `vantage::resume`): this node's own
        // dedicated off-run-loop sender -- see `wire::spawn_resume_sender`'s doc
        // comment for the full design/rationale (fixes the diagnosed loop-starvation
        // defect: Mechanism A's own network sends used to run inline, synchronously,
        // on THIS run loop). Built from the SAME `latency_map`/`batch`/`core_metrics`
        // locals as `network`/`worker_network` just below (identical configuration
        // convention) -- cloned here (rather than moved) since both of those still
        // need their own copies afterward. reconnect-replay plan §5/§6/§7: also
        // hands the task its own clone of `in_flight` (the remove-after-`Done` side)
        // and the chunk-pacing parameters (§9).
        let resume_senders = wire::spawn_resume_sender(
            latency_map.clone(),
            batch,
            core_metrics.clone(),
            in_flight.clone(),
            parameters.replay_chunk_bytes,
            parameters.replay_chunk_interval_ms,
            parameters.replay_serve_max_bytes,
            // KNOB 2 (measurement ablation): applies to this pool too -- see
            // `spawn_resume_sender`'s own doc comment.
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
            pacemaker,
            resolver,
            control,
            digest_stmts,
            wire: Wire {
                network: {
                    let mut s = ReliableSender::new()
                        .with_latency(latency_map.clone())
                        .with_batching(batch)
                        // KNOB 2 (measurement ablation): transport-level, attached
                        // regardless of `reconnect_replay` -- see that field's own
                        // doc comment.
                        .with_retry_backoff_max_ms(parameters.retry_backoff_max_ms);
                    // KNOB 1 (measurement ablation): reconnect-replay plan §14 A7's
                    // "the MAIN pool only" convention is now itself conditional on
                    // the mechanism being enabled -- when disabled, neither the
                    // reconnect-event channel nor the drop map is ever attached, so
                    // this pool's transport-level behavior is byte-identical to
                    // before the mechanism existed (`Parameters::reconnect_replay`'s
                    // own doc comment: "preferably do not even attach" -- the
                    // option this build chooses). `reconnect_tx`/`dirty_map` are
                    // simply dropped/unfed in that case; `reconnect_rx`/`Wire::
                    // dirty_map` stay typed the same either way (see their own
                    // fields' doc comments), so nothing downstream needs an
                    // `Option`.
                    if parameters.reconnect_replay {
                        s = s
                            .with_reconnect_events(reconnect_tx)
                            .with_drop_map(dirty_map.clone())
                            // n=100 straggler fix (2026-08-08): gated on the same
                            // flag as the drop map deliberately -- shedding is only
                            // safe because every volatile message is outbox-recorded
                            // and replay-recoverable, which is exactly what this
                            // flag turns off (see `Parameters::volatile_soft_cap`).
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
            // Clamped to >= 1: mirrors `gc_window`'s identical clamp (a window of 0
            // would prune the view a broadcast was just filed under, in the same
            // tick it was filed).
            replay_history_views: parameters.replay_history_views.max(1),
            replay_serve_max_bytes: parameters.replay_serve_max_bytes,
            replay_episode_max_ms: parameters.replay_episode_max_ms,
            timers: BinaryHeap::new(),
            control_timers: BinaryHeap::new(),
            payload: PayloadIo {
                pending_payload: HashMap::new(),
                store,
                tx_payload_ready,
                tx_output,
            },
            // Clamped to >= 1: a window of 0 would place the GC floor at the resolved
            // watermark itself and prune state for the view being resolved.
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
            ut_header_seal: None,
            queue_len_peak: 0,
            recheck_pending: false,
        };
        (
            core,
            rx_vantage,
            rx_bulk,
            rx_payload_ready,
            tx_vantage,
            tx_bulk,
            ack_aggregator,
            reconnect_rx,
        )
    }

    async fn run(
        mut self,
        mut rx_vantage: Receiver<Inbound>,
        mut rx_bulk: Receiver<Inbound>,
        mut rx_our_digests: Receiver<(Digest, WorkerId)>,
        mut rx_payload_ready: Receiver<(Digest, Digest, WorkerId)>,
        mut reconnect_rx: Receiver<SocketAddr>,
    ) {
        let boot = Instant::now();
        // Genesis bootstrap (§4/PHASE5-SPEC.md W1): every party enters view 1 at boot,
        // then the WISH pacemaker sets its own wish to 2 and broadcasts it.
        let mut effects = self.enter_view_effects(1, boot);
        effects.extend(self.pacemaker.genesis());
        // PHASE6-SPEC.md §5: the control log's own genesis (enter control round 1).
        effects.extend(self.control.genesis());
        self.execute(effects, boot).await;

        let header_timer = tokio::time::sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(header_timer);

        // PHASE7-PREP-NOTES.md Finding A: 1s progress-gauge sample tick. Metrics-only
        // (no protocol effects produced); independent of the message-driven `execute`
        // path entirely, so it cannot perturb ordering/timing of anything else in this
        // loop beyond the tick's own negligible CPU cost.
        let mut metrics_tick = tokio::time::interval(Duration::from_secs(1));
        metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Optional ack-watermark front-end (`Parameters::ack_watermarks`): the
        // periodic broadcast tick, constructed ONLY when the flag is on -- do not add
        // a tick at all when it's off (mirrors the `agb_sleep`/`control_sleep`
        // `Option`-guarded-select-arm idiom just below/above in this same loop).
        let mut avail_tick = if self.ack_watermarks {
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.ack_watermark_period_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            Some(interval)
        } else {
            None
        };

        // Mechanism A (sender-side lane resume, `vantage::resume`): unconditional
        // (no flag -- every party always runs this check, unlike `avail_tick`
        // above), its own dedicated tick so `Parameters::resume_check_period_ms` is
        // genuinely honored rather than piggybacking on `metrics_tick`'s fixed 1s
        // period.
        let mut resume_tick =
            tokio::time::interval(Duration::from_millis(self.resume_check_period_ms));
        resume_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // P4-3, amended by Fable perf audit item 4: bound `cancel_handlers`'
            // otherwise-unbounded growth under sustained honest traffic, but without
            // the O(n) `retain_mut` scan on every single inbound message -- see
            // `maybe_prune_cancel_handlers`'s doc comment. The `metrics_tick` branch
            // below additionally forces an unconditional prune once/sec, bounding
            // staleness even if the list never doubles.
            self.wire.maybe_prune_cancel_handlers();

            // D7-4: O(1) peek instead of the previous O(n) full-`Vec` rescan.
            let next_deadline = self.timers.peek().map(|Reverse((d, _, _))| *d);
            let agb_sleep = async {
                match next_deadline {
                    Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(agb_sleep);

            // D7-4: O(1) peek instead of the previous O(n) full-`Vec` rescan.
            let next_control_deadline = self.control_timers.peek().map(|Reverse((d, _))| *d);
            let control_sleep = async {
                match next_control_deadline {
                    Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(control_sleep);

            // Fable perf audit (measurement gap): sampled EVERY loop iteration, not
            // once/sec -- this is the only place the core is about to yield, so it is
            // the cheapest honest observation point for the backlog it leaves behind.
            // `Receiver::len` is amortized O(1) (an Acquire load of the tail position,
            // with an early return when empty) but tokio documents no complexity, and
            // its `is_maybe_closed` fallback walks the block list from the head, so the
            // worst case is O(depth / 32) pointer chases -- ~31 blocks at
            // CHANNEL_CAPACITY = 1000. That worst case coincides with a deep backlog,
            // i.e. exactly what this gauge exists to observe; it is a handful of cache
            // misses against an iteration that is already doing protocol work.
            self.queue_len_peak = self.queue_len_peak.max(rx_vantage.len());

            // Use Tokio's fair branch selection. After a healed partition,
            // `rx_vantage` can remain continuously ready while thousands of queued
            // lane and consensus messages drain. The previous `biased` selection
            // always chose that first branch, starving AGB/control deadlines,
            // reconnect prompts, and `resume_tick` for seconds at a time—the exact
            // work needed to turn a restored connection back into commits.
            tokio::select! {
                Some(inbound) = rx_vantage.recv() => {
                    let now = Instant::now();
                    let dispatch_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_inbound_dispatch, "inbound_dispatch");
                    let effects = self.dispatch_inbound(inbound, now).await;
                    drop(dispatch_timer);
                    self.execute(effects, now).await;
                }

                // Bulk recovery traffic, on its own queue so it cannot consume the
                // budget consensus messages need (see `VantageReceiverHandler::tx_bulk`
                // for the n=100 collapse this addresses). `select!` is unbiased, so a
                // saturated bulk queue now costs consensus roughly half the core's
                // attention instead of ~95% of it.
                Some(inbound) = rx_bulk.recv() => {
                    let now = Instant::now();
                    let dispatch_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_inbound_dispatch, "inbound_dispatch");
                    let effects = self.dispatch_inbound(inbound, now).await;
                    drop(dispatch_timer);
                    self.execute(effects, now).await;
                }

                Some((header_digest, digest, worker_id)) = rx_payload_ready.recv() => {
                    // The `payload_sync` scope lives INSIDE `on_payload_ready`, which
                    // drops it around its own nested `execute` call -- scoping it here
                    // instead would count all of `effect_execution` twice.
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

                // Mechanism A (design doc step 2): every author besides ourselves,
                // checked against the SAME persistent-gap trigger regardless of
                // whether it's currently gap-free (the common case) or stuck --
                // `ResumeTrigger::check` is the one place that decides, per author,
                // whether this tick actually sends anything. This tick is the
                // EPISODE DETECTOR (the two-consecutive-ticks persistence bar) and
                // the retry/backoff driver for a request that hasn't been answered
                // yet; ONGOING drain of an already-established episode instead runs
                // at receipt pace via `try_resume_request`'s other two call sites
                // (`Inbound::Publish`, `on_payload_ready`), not here.
                //
                // reconnect-replay plan §2.4/§2.6/§14 A3: this SAME tick also (a)
                // sweeps the dirty map (spec: "swept on resume_tick and before
                // serving any Hello") -- FIRST, so the episode/nudge checks below
                // see a fresh `pending_low`; (b) re-Hellos any open replay episode
                // past backoff (or closes it past the expiry valve); (c) runs the
                // server-side nudge loop. A separate, unrelated mechanism from
                // Mechanism A above -- see `vantage::resume`'s own module doc.
                _ = resume_tick.tick() => {
                    let now = Instant::now();
                    let _resume_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_resume_tick, "resume_tick");
                    // KNOB 1 (measurement ablation): the dirty-map sweep feeds
                    // `pending_low`, v3's own bookkeeping -- inert while disabled
                    // (see `Parameters::reconnect_replay`'s own doc comment). In
                    // practice `self.wire.dirty_map` is never fed at all while
                    // disabled either (`build` never attaches `with_drop_map` to
                    // the MAIN pool in that case), so this is already a no-op; the
                    // guard just makes that explicit instead of relying on it.
                    if self.reconnect_replay {
                        self.sweep_dirty_map();
                    }
                    // Cloned out (a plain `Vec<PublicKey>`, already excluding
                    // `self.name` -- `Wire::other_primaries` is "OTHER primaries",
                    // precomputed once and fixed for this node's lifetime, see that
                    // field's own doc comment) so the loop body below is free to
                    // borrow `self` one call at a time across `.await` points,
                    // rather than holding a live borrow of `self` for the whole loop.
                    let authors: Vec<PublicKey> =
                        self.wire.other_primaries.iter().map(|(pk, _)| *pk).collect();
                    let episode_backoff = Duration::from_millis(self.resume_backoff_ms);
                    let episode_max_age = Duration::from_millis(self.replay_episode_max_ms);
                    for author in authors {
                        // Mechanism A -- NOT part of this ablation, always runs
                        // regardless of `reconnect_replay`.
                        self.try_resume_request(author, now);
                        // KNOB 1: v3's own episode re-ask + nudge, self-gated.
                        self.resume_tick_replay_effects(author, now, episode_backoff, episode_max_age).await;
                    }
                }

                // reconnect-replay plan §2.1/§2.6/§7 trigger (i): our own reconnect
                // to `addr`, fired by the MAIN pool's `Connection` on re-establishment
                // after a failure (never the first-ever clean connect) -- a prompt,
                // best-effort latency optimization; a lost/dropped event costs
                // nothing beyond waiting for the next `resume_tick`'s own re-Hello.
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
                        // KNOB 1 (measurement ablation): the Hello/episode-opening
                        // half of this trigger is inert while disabled -- see
                        // `Parameters::reconnect_replay`'s own doc comment.
                        // `try_resume_request` below (Mechanism A) is NOT part of
                        // this ablation and always runs regardless, mirroring the
                        // `resume_tick` arm's identical treatment just above. In
                        // practice this whole arm is unreachable while disabled
                        // anyway (`build` never attaches `with_reconnect_events` to
                        // the MAIN pool in that case, so `reconnect_rx` stays
                        // permanently closed) -- this guard only matters for a
                        // hypothetical mixed configuration.
                        if self.reconnect_replay {
                            self.replay_episodes.open(peer, now);
                            self.send_resume_hello(peer, now, "event").await;
                        }
                        self.try_resume_request(peer, now);
                    }
                }

                _ = metrics_tick.tick() => {
                    let metrics_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_metrics_tick, "metrics_tick");
                    // Fable perf audit item 4: force an unconditional prune once/sec
                    // regardless of `maybe_prune_cancel_handlers`'s doubling
                    // condition, bounding worst-case staleness to ~1s.
                    self.wire.prune_cancel_handlers();
                    self.collect_internal_garbage();
                    // signature-free.tex §8.3: periodic retry for outstanding body
                    // fetches whose backoff has elapsed -- reuses this existing 1s
                    // tick rather than a dedicated timer queue (mirrors `control::
                    // ControlLog`'s own coarse, round-based retry cadence).
                    let retry_now = Instant::now();
                    let retry_effects = self.digest_stmts.retry_fetches(retry_now);
                    // Dropped so the nested `execute` is not double-counted into this
                    // tick's own label; re-opened for the tail below.
                    drop(metrics_timer);
                    self.execute(retry_effects, retry_now).await;
                    let _metrics_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_metrics_tick, "metrics_tick");
                    self.sample_metrics();
                    // METRICS-DASHBOARD-SPEC.md §3: `core_queue_length` -- `rx_vantage`'s
                    // current depth (cheap, `Receiver::len()` is O(1)); `0` (never set)
                    // on the two Autobahn paths, which never construct a `VantageCore`.
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

    /// Seals whatever's accumulated in `self.digests` into our own header via
    /// `LaneManager::publish_own` and executes the resulting effects -- the shared
    /// tail of `run`'s two header-sealing triggers ("header full" on
    /// `rx_our_digests`, and "max header delay elapsed" on `header_timer`). Callers
    /// reset `header_timer` themselves afterwards (a local pinned future owned by
    /// `run`, not a struct field, so it can't be reset from here).
    async fn seal_own_header(&mut self, now: Instant) {
        let seal_timer =
            Self::cached_utilization_timer(&self.metrics, &mut self.ut_header_seal, "header_seal");
        let payload = self.digests.drain(..).collect();
        self.payload_size = 0;
        let (_, effects) = self.lm.publish_own(payload).await;
        // Dropped before `execute` so `effect_execution` is not nested inside (and
        // double-counted into) this label -- same reasoning as `run`'s own
        // `drop(dispatch_timer)`.
        drop(seal_timer);
        self.execute(effects, now).await;
    }

    /// D1 payload-sync bookkeeping: one of `header_digest`'s outstanding
    /// `(digest, worker_id)` keys just arrived. Only the call that empties
    /// `pending_payload[header_digest]` (every missing batch for that header now
    /// present) actually marks the block payload-ready and re-polls the gate --
    /// `LaneManager::set_payload_ready` unconditionally marks payload presence once
    /// called (see its own doc comment), so this must fire exactly once, on the
    /// LAST missing key, never on an earlier one.
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
        if resolved {
            self.payload.pending_payload.remove(&header_digest);
            // Mechanism A receipt-continuation (design doc step 3): the DELAYED
            // half of "a resume-served header advances frontier(author)" -- a
            // header whose bytes already arrived (direct=true) but whose payload
            // was still syncing, per `LaneManager::process_publish_inner`'s own
            // `direct && !payload_ok` arm. `author_of`/the "before" snapshot are
            // taken BEFORE `set_payload_ready`, which is the call that may
            // actually advance `own_direct_frontier` for it (synchronously, inside
            // `refresh_author` -- see that method's own doc comment).
            let author = self.lm.author_of(&header_digest);
            let before = author.map(|a| self.lm.own_direct_frontier(&a));
            // P4-4: payload arriving can be the event that flips
            // `direct_pub`/`author_ok` for a C/T entry the positive gate is
            // waiting on -- re-poll it, same reasoning as the `Ack` arm.
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

    /// Mechanism A (sender-side lane resume, `vantage::resume`): `ResumeTrigger::
    /// check` for a single author, immediately enqueueing the resulting
    /// `VantageLaneResume` (a non-blocking `Wire::enqueue_resume` hand-off onto the
    /// dedicated resume-sender task -- see that method's doc comment) if it fires.
    /// Shared by three call sites: the periodic `resume_tick` (episode detector +
    /// retry/backoff driver), and receipt-triggered continuation from
    /// `Inbound::Publish`/`on_payload_ready` (design doc step 3: "the requester's
    /// frontier advances on receipt, its next request follows" -- drains an
    /// ESTABLISHED episode at RECEIPT pace instead of waiting for the next tick,
    /// matching Starfish's own continuous per-peer stream rather than a 1 Hz
    /// ping-pong). All three call sites hand this the SAME `ResumeTrigger`
    /// instance, so its two-consecutive-ticks/backoff state is coherent regardless
    /// of which one actually fires a given request -- a tick's retry racing a
    /// receipt's continuation for the identical (author, from) is exactly the case
    /// `ResumeTrigger::check`'s own backoff key (author, from) already serializes
    /// to at most one send per `resume_backoff_ms`, whichever call reaches it
    /// first.
    ///
    /// Synchronous (not `async`): once the send itself became a non-blocking
    /// `try_send`, nothing left in this function's body ever awaits -- keeping it
    /// `async fn` anyway would misdescribe it, and every call site below drops the
    /// now-pointless `.await` accordingly.
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

    /// KNOB 1 (measurement ablation): the `resume_tick` arm's own v3 body (episode
    /// re-ask + nudge) for a single author -- self-gated on `reconnect_replay` so
    /// it is directly testable without spawning the whole `run` loop (a real tick
    /// would need either a live task plus real timing -- the class of test this
    /// codebase's own network-crate tests deliberately avoid -- or reaching inside
    /// `resume_tick`, which isn't a standalone method). Extracted out of `run`'s
    /// per-author loop, which still calls `try_resume_request` (Mechanism A, NOT
    /// part of this ablation) unconditionally right before this.
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

    // --- reconnect-replay plan (server-authoritative floor, v3): a SEPARATE
    // mechanism from Mechanism A above (see `vantage::resume`'s own module doc
    // comment) -- resumes one-shot AGB/consensus broadcasts lost to a volatile
    // session death, rather than lane content.

    /// §2.2/§4: the outbox+volatile-send half of every `execute` broadcast arm
    /// EXCEPT `Header(_, false)` (durable lane data; Mechanism A + `--withhold`
    /// gate untouched) and `VantageAvail` (periodic self-superseding, stays durable
    /// and unrecorded -- see the `avail_tick` arm in `run`). Serializes once,
    /// records it in the outbox under the CURRENT `own_watermark` (the filing key
    /// every later Hello/`pending_low`/replay computation keys off -- audit V1:
    /// monotone since `own_watermark` itself never decreases), then sends it
    /// volatile.
    ///
    /// KNOB 1 (measurement ablation, `Parameters::reconnect_replay`): this is the
    /// mechanism's SINGLE choke point -- every one-shot broadcast arm in `execute`
    /// calls this, never `Wire::broadcast_volatile` directly -- so the ablation
    /// branches HERE rather than duplicating a flag check at every call site. When
    /// disabled, this reverts to exactly the behavior that predates the
    /// reconnect-replay mechanism: nothing is recorded (there is no replay left to
    /// serve it from later, so filing history for one would be pure waste), and the
    /// message goes out on the ordinary DURABLE path instead of the volatile one --
    /// this is the load-bearing half of disabling the mechanism: a one-shot
    /// AGB/consensus statement merely dropped on a session death (volatile's whole
    /// point) would otherwise be lost forever, with no replay mechanism left to
    /// recover it.
    async fn broadcast_recorded(&mut self, message: PrimaryMessage) {
        if !self.reconnect_replay {
            self.wire.broadcast_message(message).await;
            return;
        }
        let msg_type = message.type_name();
        let bytes = Bytes::from(bincode::serialize(&message).expect("serializes"));
        let key = self.pacemaker.own_watermark();
        self.outbox.record(key, bytes.clone());
        self.wire.broadcast_volatile(bytes, msg_type, key).await;
    }

    /// §2.3/§7/§14 A8: drains the shared dirty map (never merely reads it -- A8)
    /// then min-merges each `(addr, key)` entry into `pending_low`, translating
    /// `SocketAddr -> PublicKey` via `Wire::addr_to_peer`. Called on `resume_tick`
    /// and before serving any Hello (§2.4).
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

    /// §6/§14 A6: is a replay stream to `peer` currently in flight? Evicts (and
    /// counts) a stale entry if found -- see `resume::InFlightState`'s own doc
    /// comment for the TTL rationale. Shared by the Hello-serving path (a genuine
    /// `InFlight` blocks a fresh serve outright) and the nudge loop (§14 A3's own
    /// "X not in-flight" conjunct).
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

    /// §2.6 triggers (i)/(ii)/(iii): sends a `VantageResumeHello` to `peer` DURABLY
    /// (the ordinary `send_message` unicast path). Contrast `send_nudge_hello`
    /// (trigger (iv), the ONE Hello sent volatile instead -- §14 A7): each of these
    /// three is either a one-shot event or bounded by the requester's own
    /// `replay_episode_max_ms` expiry valve, so durable delivery is safe (never
    /// unboundedly retried/buffered against a permanently-dead peer) and valuable
    /// (A5's belt-and-braces tail actually needs the prompt Hello to land).
    ///
    /// `trigger` is diagnostics-only (one of `"event"` -- the reconnect prompt AND
    /// a `Done(complete=false)` continuation, both one-shot triggers by an event
    /// -- `"tick"`, or `"reciprocal"`; `send_nudge_hello` logs its own fixed
    /// `"nudge"` separately). Every actual send -- from ANY of the four triggers
    /// -- records `last_hello_sent` here, the ONE place that memo is ever written
    /// (`ReplayEpisodes::on_hello_received`'s own doc comment: this is what makes
    /// it immune to the Done-vs-Hello cross-pool race).
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

    /// §2.6 trigger (iv)/§14 A3/A7: the server-side nudge loop's own Hello send,
    /// VOLATILE (self-superseding: a later nudge always supersedes an undelivered
    /// earlier one; a lost nudge is simply re-nudged on the next `resume_tick`) --
    /// unlike the other three triggers, a nudge has no natural bound of its own
    /// (`pending_low[X]` can stay set indefinitely against a peer that never
    /// reconnects), so sending it durably would let one new retried buffer entry
    /// accumulate per backoff period, forever, against a dead peer. A nudge carries
    /// a real floor hint and is served like any other ask, so it counts as our own
    /// ask toward `peer` exactly like `send_resume_hello`'s three triggers --
    /// `last_hello_sent` is recorded here too.
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

    /// §2.6's server-side nudge loop, audit-3 A3's exact condition: `pending_low
    /// [peer].is_some() && peer not in-flight && backoff elapsed since the last
    /// serve-or-nudge to peer` -- deliberately keyed to "since the last serve-OR-
    /// nudge" (not "since `pending_low` was set"), which is the audited fix: the
    /// weaker form would silence this backstop after a single partial serve, even
    /// though `pending_low` can (and, under budget truncation, routinely does)
    /// stay set across many serves.
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

    /// §2/§6/§14 A1/A2/A4/A5: the Hello-serving decision. `hello_floor` is the
    /// sender's own hint (`omega_of` at THEIR end, §2.4/A5's "belt-and-braces
    /// tail"); the AUTHORITATIVE floor is `pending_low[sender]` (fed exclusively by
    /// this node's own exact drop reports, never by anything `sender` claims -- D4
    /// caveat (i)/(ii) in `vantage::resume`'s own module doc). Reciprocation (§2.6
    /// trigger (iii)) and the dirty-map sweep (§2.4: "swept ... before serving any
    /// Hello") both run BEFORE the floor read; everything from the floor read
    /// through the enqueue is await-free (audit V4: single-threaded atomicity --
    /// nothing else on this task's `&mut self` can observe a torn intermediate
    /// state, since there is no `.await` point for the runtime to preempt at).
    async fn on_resume_hello(&mut self, hello_floor: View, sender: PublicKey, now: Instant) {
        log::debug!(
            "vantage node: resume hello received: sender={} floor={}",
            sender,
            hello_floor
        );
        let backoff = Duration::from_millis(self.resume_backoff_ms);

        // (iii): reciprocation, gated by the sent-memo (not episode presence --
        // see `ReplayEpisodes::on_hello_received`'s own doc comment for the
        // Done-vs-Hello cross-pool race this avoids) -- independent of everything
        // below.
        if self.replay_episodes.on_hello_received(sender, now, backoff) {
            self.send_resume_hello(sender, now, "reciprocal").await;
        }

        self.sweep_dirty_map();

        if self.check_in_flight(sender, now) {
            // §6: "the in-flight stream already serves >= pending_low, a superset
            // of any concurrent ask" -- ignored entirely, no Done either.
            log::debug!(
                "vantage node: resume serve suppressed: sender={} gate=in-flight",
                sender
            );
            return;
        }

        // --- await-free from here through the enqueue (audit V4) ---
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
            // §6: "over-budget Hellos are deferred to the next window (the episode
            // tick re-asks)" -- nothing sent at all this time.
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
        // Insert before enqueue: the replay task's intentional immediate first tick
        // may finish a small stream as soon as it becomes visible. The unique
        // generation makes both task completion and enqueue-failure cleanup remove
        // only this exact marker, so a stale completion cannot cross-clear a newer
        // stream installed after TTL expiry.
        let generation = self.wire.next_replay_generation();
        self.wire.in_flight.lock().insert(
            sender,
            InFlightEntry {
                started: now,
                generation,
            },
        );
        let enqueued = self.wire.enqueue_replay(sender, generation, msgs, done);
        // --- end await-free section ---

        if !enqueued {
            // audit-3 A2: `Wire::enqueue_replay` already counted the drop metric;
            // `pending_low` stays untouched -- the next nudge/tick re-asks. The
            // in-flight entry inserted above never corresponded to a real stream.
            // Remove only its generation; a concurrent newer generation wins.
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
            // A4's monotone guard: raise, never lower (a `None`/absent prior value
            // is treated as unconstrained -- `end_key` always wins).
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

    /// §2.6's `VantageReplayDone` arm: episode continuation (send the next Hello
    /// immediately) on `complete = false`; close the episode on `complete = true`.
    /// `end_key` carries no requester-side state update in v3: correctness lives
    /// entirely in the AUTHOR's own `pending_low` (§2.4) -- the requester's floor
    /// is a hint only (D4 caveat (iii)), so there is nothing here to advance --
    /// logged (diagnostics only) below, never otherwise consulted.
    ///
    /// D4 addendum (audit note; no code change): a forged `Done(complete=false,
    /// sender=j)` (`j` need not have any real stream toward us at all) buys `j`
    /// exactly one extra durable Hello from us, via the continuation branch below
    /// -- linear in the number of forged Dones sent, no amplification, and bounded
    /// by the same ordinary durable-send accounting `send_resume_hello` always
    /// costs. Not a suppression lever either way: it can only ever cause an EXTRA
    /// ask toward `j`, never fewer.
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

    /// D7-4: pop-based AGB timer firing (O(log n) per pop, vs. the previous O(n)
    /// `retain` scan) + lazy stale discard -- if the timer's underlying one-shot
    /// event already happened organically (echo/ready already sent for this view),
    /// its dispatch would be a no-op through the handler's own guard anyway (see
    /// `AgbEngine::echo_sent`/`ready_sent`'s doc comments); skip
    /// constructing/dispatching it entirely rather than paying for a call that
    /// immediately returns an empty `Vec`. Deterministic-equivalent: identical
    /// effects either way, since the discarded call was always going to return
    /// nothing. Also re-checks every pending positive gate afterwards
    /// (PHASE6-SPEC.md §2 `MetaOK`: "persistent -- re-evaluate on state change like
    /// the rest of the gate" -- a pending view w's `MetaOK` can depend on THIS
    /// party's own echo/ready for an earlier view u that just changed here).
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

    /// D7-4: pop-based control-round timer firing + lazy stale discard, same
    /// reasoning as `fire_agb_timers` -- a round timer whose round has already
    /// advanced/voted is moot through `on_control_round_timer`'s own `r !=
    /// self.curr_round || self.voted` guard; skip dispatching it.
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

    /// PHASE7-PREP-NOTES.md Finding A: publish the six progress gauges from each
    /// component's own accessor -- metrics-only, no effects, no protocol interaction.
    /// A no-op if this node was spawned without a `Metrics` handle.
    fn sample_metrics(&self) {
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
            .vantage_cursor_next_view
            .set(self.cursor.next_view() as i64);
        metrics
            .vantage_control_round
            .set(self.control.curr_round() as i64);
        metrics
            .vantage_control_delivered_len
            .set(self.control.delivered_log_len() as i64);
        metrics
            .vantage_control_consume_pos
            .set(self.control.consume_pos() as i64);
        // PHASE7-PREP-NOTES.md Delta=1000 investigation: diagnostic-only observational
        // log (no behavior change) -- the linearly-scanned timer queues' current sizes.
        log::info!(
            "vantage node: timers.len()={} control_timers.len()={} cancel_handlers.len()={}",
            self.timers.len(),
            self.control_timers.len(),
            self.wire.cancel_handlers.len()
        );
    }

    fn collect_internal_garbage(&mut self) {
        // reconnect-replay plan §5: the outbox's own retention window tracks
        // `own_watermark` (this party's own wish high-watermark) -- a different,
        // generally FASTER-advancing counter than the resolver's own `resolved_
        // watermark` the rest of this method's floor (below) is keyed to. Pruned
        // UNCONDITIONALLY, every tick, rather than gated behind the resolver-floor
        // early return below -- gating it there would silently stall outbox GC
        // whenever resolution itself stalls, even while `own_watermark` (and the
        // outbox) keep growing.
        let outbox_floor = self
            .pacemaker
            .own_watermark()
            .saturating_sub(self.replay_history_views)
            .max(1);
        self.outbox.prune_below(outbox_floor);
        // Audit-3 (Q3 recommendation): any `pending_low[X]` below the just-advanced
        // outbox floor can never be served from again regardless -- raise it now
        // rather than waiting for a future Hello to discover the gap only after
        // already trying (and getting `Done(clamped=true)`).
        for pending in self.pending_low.values_mut() {
            if *pending < outbox_floor {
                *pending = outbox_floor;
            }
        }

        let floor = self.resolver.gc_floor(self.gc_window);
        if floor <= self.last_gc_floor {
            return;
        }
        // Carrier bodies are kept `SERVE_MARGIN_WINDOWS` extra windows below the state
        // floor so a peer that has fallen behind can still fetch them -- see
        // `ControlLog::min_serve_view`.
        let serve_floor = floor
            .saturating_sub(
                self.gc_window
                    .saturating_mul(ControlLog::SERVE_MARGIN_WINDOWS),
            )
            .max(1);
        self.agb.gc_below(floor);
        self.digest_stmts.gc_below(floor);
        self.frontier.gc_below(floor);
        // NOTE: `Cursor` deliberately has no `gc_below`. Every key in its `pending`/
        // `core_emitted` maps is `>= next_view` by construction (both insertion sites
        // reject `view < next_view`, and `advance` removes the entry it passes), so
        // `next_view` already IS the cursor's floor and a view-GC pass has nothing to
        // prune. The `gc_below` this replaced clamped its argument to `next_view` and was
        // therefore a provable no-op -- misleading, because it implied a bound it did not
        // provide.
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

    /// R1's "should we propose next" check (§4), as a pure effect-producing step so it
    /// can be inlined both at boot and inside `execute`'s `Fixed` handling without
    /// recursive `async fn` calls. PHASE6-SPEC.md §4: when it's genuinely our turn,
    /// consult the `Resolver` for this turn's `M` (data-only `None`, or a recovery
    /// entry) before building the proposal -- computed only when it's actually our
    /// turn, so `Frontier::propose_view`'s own gate (unaffected) stays the sole
    /// authority on whether a proposal is emitted at all.
    ///
    /// Paper R1, early-wish trigger: `p_i` proposes any view `v` it owns and hasn't
    /// proposed yet with `v <= max(a_i + 1, omega_i^+)` -- not just `v = a_i + 1`.
    /// `omega_i^+ > a_i + 1` lets this party mint a proposal for a view still ahead of
    /// its own frontier; such an early proposal is PASSIVE by construction (receivers
    /// buffer/fix it but it only activates once the frontier itself reaches it --
    /// automatic in the existing echo-stage code, untouched here). Iterates the
    /// concrete, small (`omega_i^+` tracks `a_i` within the entry spread) range
    /// `a_i+1..=bound` in increasing order, so lower (sooner-needed) views are always
    /// proposed/broadcast before higher ones; `propose_view`'s own proposed-once guard
    /// makes every call in the range (and every redundant call to this whole method)
    /// idempotent, so this always terminates and is safe to call more than once per
    /// event.
    fn try_propose_effects(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        let bound = std::cmp::max(self.frontier.a_i() + 1, self.pacemaker.omega_plus());
        let mut view = self.frontier.a_i() + 1;
        while view <= bound {
            if self.agb.proposer(view) != self.name || self.frontier.already_proposed(view) {
                view += 1;
                continue;
            }
            // signature-free.tex's "Batched resolution entries" paragraph (narrowed by
            // 704fb29, par:batched-anchors): the recovery-turn scan always uses
            // `decide_prefix`'s prefix logic (0..=f entries), unconditional protocol
            // behavior -- see its own doc comment.
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

    /// PHASE5-SPEC.md §3: execute a formal `Effect::Enter(view)` as `AgbEngine::enter`
    /// + `Frontier::enter`, in that order -- `Frontier::enter`'s W5(c) floor can newly
    ///   activate further views (its own contiguous-advance loop, on top of `view`
    ///   itself), each of which must still run through `AgbEngine::activate` exactly like
    ///   `Effect::Fixed`'s handling does, and a floor raise can newly satisfy R1 too.
    ///   Shared by the genesis boot call (view 1) and every subsequent WISH-driven entry.
    fn enter_view_effects(&mut self, view: View, now: Instant) -> Vec<Effect> {
        let mut effects = self.agb.enter(view, now, &mut self.lm, &mut self.rep);
        let activated = self.frontier.enter(view);
        for v in activated {
            effects.extend(self.agb.activate(v, &mut self.lm, &mut self.rep));
        }
        effects.extend(self.try_propose_effects(now));
        effects
    }

    fn on_ack_availability(&mut self, availability: AckAvailability, _now: Instant) -> Vec<Effect> {
        // Only the availability bookkeeping happens per ref; the resulting
        // `recheck_all` is coalesced to once per effect drain -- see `recheck_pending`
        // and `execute`'s outer loop for why deferring it that far is sound.
        //
        // NOTE `LaneManager::process_ack_availability` returns an empty vec
        // unconditionally, so on this path the coalesced recheck is the ONLY work the
        // resulting `execute` call does. `execute` must therefore keep running its
        // outer loop for an empty `initial`: short-circuiting on `initial.is_empty()`
        // at any call site would silently kill the whole availability -> echo trigger.
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
        let result = {
            let mut aggregator = self.ack_aggregator.lock();
            aggregator.record_ack(ack.sender, ack.reference())
        };
        if !result.accepted {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_rejected_nonmember_total.inc();
            }
            return Vec::new();
        }
        if let Some(metrics) = &self.metrics {
            metrics.vantage_acks_received.inc();
        }
        result
            .availability
            .map(|availability| self.on_ack_availability(availability, now))
            .unwrap_or_default()
    }

    /// Feeds `refs` (from `LaneManager::resolve_watermark`/`retry_pending_avail`)
    /// through the SAME shared `AckAggregator` the per-block ack path uses
    /// (`record_injected_ack`), and the SAME `on_ack_availability` path -- nothing
    /// downstream of the aggregator differs between the two front-ends (see
    /// `resolve_watermark`'s own doc comment: this is the load-bearing property of the
    /// whole design). `sender` is already known to be a committee member
    /// (`dispatch_inbound`'s centralized gate ran before `Inbound::Avail` is ever
    /// reached); the redundant re-check via `record_ack`'s own return is defense in
    /// depth, mirroring `record_injected_ack`'s identical pattern.
    fn credit_refs(&mut self, sender: PublicKey, refs: Vec<BlockRef>, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        for r in refs {
            let result = {
                let mut aggregator = self.ack_aggregator.lock();
                aggregator.record_ack(sender, r)
            };
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

    async fn dispatch_inbound(&mut self, inbound: Inbound, now: Instant) -> Vec<Effect> {
        // SECURITY (Fable audit): the single centralized membership gate -- every
        // wire-declared sender is checked against the trusted committee-membership
        // set BEFORE any census/count path below ever sees the message. Wire messages
        // carry no signature, so without this check a single Byzantine node could
        // forge arbitrarily many distinct non-committee sender keys, each counted once
        // by the dedup-only census helpers downstream, inflating any party-count
        // quorum. Honest senders are always committee members, so this is a no-op on
        // every honest path.
        if !wire::sender_is_member(&inbound, &self.members) {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_rejected_nonmember_total.inc();
            }
            return Vec::new();
        }
        match inbound {
            Inbound::Publish(sender, header) => {
                let author = header.author;
                let before = self.lm.own_direct_frontier(&author);
                let effects = self.lm.process_publish(sender, header).await;
                // Mechanism A receipt-continuation (design doc step 3): this
                // publish -- a resume-served header, or an ordinary broadcast
                // landing on a previously-gapped lane -- may have just advanced
                // `frontier(author)` (synchronously, inside `process_publish`'s own
                // `refresh_author` call). If `author`'s episode is already
                // ESTABLISHED and a gap still remains, ask for the next span now,
                // rather than waiting for the next `resume_tick`. A no-op call
                // (`ResumeTrigger::check` returns `None`) for the overwhelming
                // majority of publishes, which never had an established episode to
                // begin with.
                if self.lm.own_direct_frontier(&author) > before {
                    self.try_resume_request(author, now);
                }
                effects
            }
            Inbound::Serve(header) => self.serve_effects(header).await,
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
                // D4 (PHASE4-SPEC.md §13's standing note): a `ViewProposal`/
                // `BatchViewProposal` carries no sender field and there is no channel
                // identity to check it against (same class of gap as `Header`'s
                // publish path, PHASE3-NOTES.md §5) -- production trusts any received
                // proposal for `view` as if it came from `proposer(view)`.
                // `AgbEngine::on_propose{,_batch}`'s `sender == proposer(view)` guard
                // remains meaningful for unit tests exercising a wrong-sender
                // proposal directly.
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
            // PHASE5-SPEC.md §3: absorb every response's piggybacked wish (W2) BEFORE
            // handing the response to `AgbEngine` -- wish processing (amplification,
            // then entry) is independent of statement counting, so this ordering vs.
            // the engine's own processing is fine either way; absorbing first keeps the
            // four arms below symmetric with `Inbound::Wish`'s own handling.
            Inbound::Echo(echo) => {
                let mut effects = self.pacemaker.on_wish(echo.sender(), echo.wish());
                effects.extend(match echo {
                    EchoOut::Single(e) => self.agb.on_echo(e, &mut self.rep),
                    EchoOut::Batch(e) => self.agb.on_echo_batch(e, &mut self.rep),
                });
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                // Paper R1 early-wish trigger: `on_wish` above may just have raised
                // `omega_i^+`, which can newly satisfy `v <= max(a_i+1, omega_i^+)` for
                // an owned view this party hasn't proposed yet -- redundant/idempotent
                // (`propose_view`'s proposed-once guard) when it hasn't.
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
            // A standalone `VantageWish` (W2 amplification): never an echo/ready/ack/
            // origin bit/resolution justification -- it only ever schedules views.
            // Still a wish-bearing arm for R1's early-wish trigger purposes: `on_wish`
            // can raise `omega_i^+` here exactly as in the four arms above.
            Inbound::Wish(view, sender) => {
                let mut effects = self.pacemaker.on_wish(sender, view);
                effects.extend(self.try_propose_effects(now));
                effects
            }

            // --- PHASE6-SPEC.md §5 (reports + control log) ---
            Inbound::CompReport(view, digest, sender) => {
                self.control.on_comp_report(view, digest, sender)
            }
            Inbound::ControlInit(proposal, b_w) => {
                // Same D4 class as `Propose`: no sender field on the wire, trusted as
                // this round's control leader.
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

            // --- signature-free.tex's "Grounded post-ready skip" (par:skip-seal) ---
            Inbound::SkipVote(view, sender) => {
                // No `recheck_all`/`try_propose_effects` afterward: counting a vote
                // only ever ADDS a new exclusion reason (a known terminal skip) to
                // `TryMetaOK`'s conjunction for OTHER carrying views -- it can never
                // newly SATISFY a pending one, unlike a lock-release-affecting
                // `EchoSkip`/`NoReady`, so there is nothing here for either to unblock.
                self.agb.on_skip_vote(view, sender)
            }

            // --- signature-free.tex §8.3 "Digest-named AGB statements" ---
            // Reception is unconditional regardless of `self.digest_statements` --
            // see `vantage::agb::DigestStatements`'s own module doc comment.
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

            // --- Mechanism A (sender-side lane resume, `vantage::resume`) ---
            // Design doc step 3 (author side): `author` names a lane, `requester` is
            // who's asking (already known to be a committee member -- the gate at
            // the top of this function ran before this arm is ever reached).
            Inbound::LaneResume(author, from, requester) => {
                // "author ignores resume requests for lanes it doesn't own": `author`
                // is UNTRUSTED input (merely a wire field, not itself a declared-
                // sender claim -- see `Inbound::LaneResume`'s own doc comment) until
                // checked against our own identity.
                if author != self.name {
                    return Vec::new();
                }
                // "author clamps below-floor requests": `from` is the requester's own
                // claim about where ITS OWN prefix ends -- clamp up to whatever this
                // party can actually still serve before doing anything else.
                let floor = self.lm.earliest_authored_height(&author);
                let from = from.max(floor);
                let tip = self.lm.own_tip_height();
                if from > tip {
                    return Vec::new(); // nothing new to serve (fully caught up, or
                                       // a request racing ahead of our own tip)
                }
                let backoff = Duration::from_millis(self.resume_backoff_ms);
                if !self
                    .resume_serve
                    .should_serve(requester, from, now, backoff)
                {
                    return Vec::new(); // one-shot dedup: already served this exact
                                       // (requester, from) within resume_backoff_ms
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

            // --- reconnect-replay plan (server-authoritative floor, v3) -- a
            // SEPARATE mechanism from `LaneResume` above (see `vantage::resume`'s
            // own module doc comment). Both arms perform their own I/O directly
            // (dirty-map sweep, `pending_low`, the shared in-flight map, and the
            // resume-sender task hand-off all need synchronous, single-threaded
            // atomicity between a floor read and the resulting enqueue -- audit
            // V4 -- which does not decompose into a `Vec<Effect>` the way the
            // rest of this protocol's pure state machines do) rather than
            // returning effects for `execute` to drain.
            Inbound::ResumeHello(floor, sender) => {
                // KNOB 1 (measurement ablation): ignored while disabled -- no
                // Replay enqueue, no `pending_low`/in-flight change. A uniform run
                // (every node sharing the same `Parameters`) never sends one of
                // these while disabled (see `broadcast_recorded`/`run`'s gated
                // arms), so this branch matters only for a MIXED run, which must
                // not misbehave either -- logged once per receipt since that is
                // otherwise a silent, easy-to-miss divergence between arms.
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
            Inbound::ReplayDone(end_key, complete, clamped, sender) => {
                // KNOB 1: same reasoning as `ResumeHello` above.
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

    async fn serve_effects(&mut self, header: Header) -> Vec<Effect> {
        let digest = header.id.clone();
        let author = header.author;
        let mut effects = self.rep.on_serve(header.clone());
        let accepted = effects
            .iter()
            .any(|effect| matches!(effect, Effect::BlockCached(d) if *d == digest));
        if accepted {
            let missing = self.lm.missing_payload(&header).await;
            if !missing.is_empty() {
                effects.push(Effect::SyncBatches(author, digest, missing));
            }
        }
        effects
    }

    /// Drains `initial` (and every effect transitively produced while draining it)
    /// against real I/O. A plain `VecDeque`-backed loop (not recursive `async fn`
    /// calls) so cross-component chains (e.g. `Fixed` -> `Frontier::record_fixed` ->
    /// `AgbEngine::activate` -> more effects) stay within one `Future`.
    async fn execute(&mut self, initial: Vec<Effect>, now: Instant) {
        // METRICS-DASHBOARD-SPEC.md §3: covers every call site (every `run` branch
        // calls `execute`, plus `on_payload_ready`/`seal_own_header` indirectly) --
        // wrapping here once, rather than at each call site, measures the whole
        // effect-draining loop under one "effect_execution" label regardless of caller.
        let _timer = Self::cached_utilization_timer(
            &self.metrics,
            &mut self.ut_effect_execution,
            "effect_execution",
        );
        let mut queue: VecDeque<Effect> = initial.into();
        // Outer loop: drain the queue, then service any coalesced `recheck_all` ONCE and
        // drain whatever it produced, until both are quiet -- one scan per drain instead
        // of one per credited availability ref (at n=50 the old shape reached ~49k full
        // scans/s, each O(n^2) before `tip_ok` was indexed).
        //
        // Termination: each non-breaking iteration flips at least one view's `echo_sent`
        // false -> true permanently and removes it from `pending_gate`, and a passing
        // gate always emits at least one effect, so `rechecked.is_empty()` holds exactly
        // when nothing transitioned -- since the n=100 straggler fix, "nothing
        // transitioned within the RECHECK_BUDGET-view window scanned this call" (see
        // `AgbEngine::recheck_all`); unscanned views wait for the next trigger, the
        // same eventual-recheck contract deferral has always had here.
        //
        // Equivalence to the old per-trigger calls rests on TWO properties, only the
        // first of which is `recheck_all`'s own. (1) Idempotence: every `recheck_gate`
        // mutation is downstream of the gate passing, which sets `echo_sent` and drops
        // the view from `pending_gate`, so a second call cannot re-enter. (2) The
        // evaluation POINT moved from before the drain to after it, which is only
        // immaterial because no effect that can be queued alongside a set
        // `recheck_pending` mutates `AgbEngine` state: the flag is set from
        // `Effect::BroadcastAck` and `Effect::BlockCached`, both emitted only by
        // `LaneManager`/`Repairer`, and the co-queued lane/repair/cursor effects touch
        // neither `views` nor `pending_gate`. `recheck_all`'s own doc comment covers the
        // one cross-view write that does exist (`stance = NonSkip`) and why it cannot
        // turn a passing gate into a failing one. A future `Effect` variant that reached
        // into `AgbEngine` would break property (2) silently -- that, not idempotence,
        // is the invariant to preserve here.
        loop {
            while let Some(effect) = queue.pop_front() {
                match effect {
                    Effect::BroadcastPublish(header) => {
                        self.wire
                            .broadcast_message(PrimaryMessage::Header(header, false))
                            .await
                    }
                    Effect::BroadcastAck(ack) => {
                        // The self-ack path always runs, flag on or off: our own
                        // holdings must always count toward our own aggregator (see
                        // `ack_watermarks`'s own field doc comment). Only the WIRE
                        // broadcast is suppressed when the watermark front-end replaces
                        // it -- `LaneManager` itself is unaware of the flag and keeps
                        // emitting this effect exactly as before.
                        queue.extend(self.record_local_ack(&ack, now));
                        if !self.ack_watermarks {
                            // reconnect-replay plan §2.2/§4: a one-shot broadcast, not
                            // durable lane data -- outbox-recorded, sent volatile.
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
                        // Ack-watermark front-end: this newly-cached block may complete a
                        // pending watermark's below-the-head segment for its author --
                        // retry before `on_block_available` consumes `digest` by value.
                        for (sender, r) in self.lm.retry_pending_avail(&digest) {
                            queue.extend(self.credit_refs(sender, vec![r], now));
                        }
                        queue.extend(self.rep.on_block_available(digest));
                        // Coalesced: one recheck at the end of the drain, not one per
                        // cached block (see `recheck_pending`).
                        self.recheck_pending = true;
                        queue.extend(self.cursor.retry());
                    }
                    // PHASE7: `Single` rides the pre-PHASE7 `VantagePropose` message
                    // (0/1-entry `M`); `Batch` (`decide_prefix` produced `>= 2` entries)
                    // rides the separate `VantageProposeBatch` message.
                    // reconnect-replay plan §2.2/§4: outbox-recorded, sent volatile.
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
                    // PHASE5-SPEC.md §3/D5-3: every response effect is stamped with our
                    // current wish watermark here, at serialization time -- `AgbEngine`
                    // itself stays watermark-free (its own construction sites use a `0`
                    // placeholder, or none at all for `EchoSkip`/`NoReady`, which are
                    // effects carrying just a `View` to begin with).
                    Effect::BroadcastEcho(mut e) => {
                        e.set_wish(self.pacemaker.own_watermark());
                        match e {
                            // signature-free.tex §8.3 "Digest-named AGB statements"
                            // (`Parameters::digest_statements`): the flag's ENTIRE
                            // emission-side effect -- when on, send the compact
                            // `VantageEchoDigest` instead of the full by-value one.
                            // `AgbEngine` still constructed the identical by-value
                            // `Echo` above (`build_echo_out`, untouched); this is
                            // purely an alternate wire encoding of that same value,
                            // decided here, not inside the engine. Never applies to
                            // `Batch` (out of scope -- see `EchoDigest`'s own doc
                            // comment), so a batched-anchors run is unaffected either
                            // way.
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
                            // signature-free.tex §8.3: mirrors `Effect::BroadcastEcho`'s
                            // identical translation immediately above.
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
                    // No wish piggyback, unlike `BroadcastEchoSkip`/`BroadcastNoReady`
                    // above (see `Inbound::SkipVote`'s doc comment).
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
                        // signature-free.tex §8.3: a proposal just fixed BY VALUE may
                        // already match digest statements buffered before it arrived --
                        // drain them now. A no-op if nothing was ever buffered for this
                        // view, or if `well_formed` is false (`on_local_fixed`'s own
                        // `fixed_proposal` query returns `None` for `Reject`).
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

                    // --- PHASE6-SPEC.md §5 (reports + control log) ---
                    Effect::CompletionReportable(view, proposal) => {
                        // D7-1 (PHASE7-PREP-NOTES.md): this is the FIRST genuine
                        // completion of a carrier with M != None -- whether we proposed
                        // it ourselves or another party did -- so it's exactly the
                        // "observed CompReport for a carrier resolving u" evidence the
                        // in-flight marker should refresh on, independent of (and in
                        // addition to) `Resolver::decide{,_prefix}`'s own immediate
                        // refresh for its own attempts. PHASE7: refreshed for EVERY
                        // target this carrier's `M` names (0/1 for `Single`, `2..=f` for
                        // `Batch`) -- this carrier's genuine completion is fresh
                        // in-flight evidence for all of them, not just the first.
                        for entry in proposal.entries() {
                            self.resolver.note_carrier_report(entry.target_view(), now);
                        }
                        queue.extend(self.control.on_completion_reportable(view, proposal));
                    }
                    // reconnect-replay plan §2.2/§4: the wish-free one-shots
                    // (CompReport, Control*) below are ALSO outbox-recorded/volatile --
                    // every broadcast in `execute` except `BroadcastPublish`/
                    // `VantageAvail`'s own `avail_tick` arm is.
                    Effect::BroadcastCompReport(view, digest) => {
                        self.broadcast_recorded(PrimaryMessage::CompReport(
                            view, digest, self.name,
                        ))
                        .await;
                    }
                    // PHASE7: `Single`/`None` are byte-identical to the pre-PHASE7 path
                    // (same `ControlInit` message); `Batch` rides the separate,
                    // flag-gated `ControlInitBatch` message.
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
                    // PHASE7: `Single` is byte-identical to the pre-PHASE7 path (same
                    // `ControlServe` message); `Batch` rides the separate, flag-gated
                    // `ControlServeBatch` message.
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

                    // --- PHASE6-SPEC.md §6 (anchors) ---
                    Effect::ApplyAnchor(view, outcome, refs) => {
                        for r in refs {
                            queue.extend(self.rep.authorize(r));
                        }
                        queue.extend(self.agb.submit_anchor(view, outcome));
                    }

                    // --- signature-free.tex §8.3 "Digest-named AGB statements" ---
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

                    // --- Mechanism A (sender-side lane resume, `vantage::resume`) ---
                    Effect::ResumeServeTo(requester, header) => {
                        // Non-blocking hand-off onto the dedicated resume-sender task
                        // (`Wire::enqueue_resume_header` -> `enqueue_resume`) -- never
                        // `.await`ed, so a burst of `Inbound::LaneResume` arrivals
                        // queuing many of these back to back (exactly what a
                        // windowed-withhold recovery produces) costs this effect-drain
                        // loop nothing beyond the enqueue itself.
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

    /// Fable perf audit item 7 (cheap subset): resolves `label`'s `IntCounter` from
    /// `metrics.utilization_timer` on first use -- the identical `with_label_values`
    /// call, at the identical first occurrence, that `IntCounterVec::
    /// utilization_timer` would have made anyway (so this metric's first appearance
    /// in a scrape is unchanged) -- then caches the resolved handle in `cache` so
    /// every subsequent call constructs the timer directly from the cached counter,
    /// with no `with_label_values` lookup at all.
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
    /// `Inbound::is_bulk` decides which inbound queue a message lands on, and the two
    /// error directions are not symmetric, so pin both.
    ///
    /// Demoting a message we are BLOCKED ON is how the first version of this split hurt
    /// the very nodes it meant to help (2026-08-07 n=100: lagging nodes dropped 90
    /// msg/s -- 53k median -- and received only 6% of blocks, because their serve
    /// RESPONSES shared one queue with the requests 78 healthy peers were making OF
    /// them). Promoting requests others make of us recreates the shared-budget problem
    /// the split exists to remove.
    #[test]
    fn bulk_class_is_requests_of_us_not_responses_to_us() {
        use crypto::PublicKey;
        let k = PublicKey::default();
        let d = Digest::default();

        // Requests OTHERS make OF US: declining costs the requester a retry, us nothing.
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

        // RESPONSES to requests WE made: data we are blocked on. Never droppable.
        for inbound in [
            Inbound::Serve(Header::default()),
            Inbound::ReplayDone(1, true, false, k),
        ] {
            assert!(
                !inbound.is_bulk(),
                "a response to our own request must not be droppable: {inbound:?}"
            );
        }

        // Consensus traffic proper, including `Publish` -- the organic car-delivery path
        // AND how a resume batch arrives, i.e. the traffic whose starvation collapsed the
        // n=100 run.
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

    // SECURITY (Fable audit) regression coverage: `dispatch_inbound`'s membership
    // gate, exercised against a real `VantageCore` (via the private `build`
    // constructor split out of `spawn` for exactly this purpose -- see `build`'s doc
    // comment) rather than the separate synchronous harness in `vantage/tests/`
    // (whose own `Node::dispatch`, `harness.rs`, is a hand-rolled mirror that never
    // carried this gap in the first place: it always derives the sender from the
    // direct method call the harness itself makes, never from an untrusted wire
    // message -- see `harness::deliver_only_to`'s own doc comment on that boundary).
    // This module lives inside `node.rs`, rather than alongside the rest of the
    // `vantage/tests/` suite, because `VantageCore`'s fields (`members` among them)
    // and `dispatch_inbound`/`build` are private to this module -- a sibling test
    // module under `vantage/tests/` has no access to them without a broader,
    // out-of-scope visibility change.
    use super::*;
    use crate::vantage::agb::{Echo, Ready, ReadyGrade, ViewProposal};
    use crate::vantage::control::ControlProposal;
    use crypto::{generate_keypair, Hash as _};
    use rand::rngs::StdRng;
    use rand::SeedableRng as _;
    use std::collections::BTreeMap;
    use store::Store;

    /// A real `VantageCore` for authority `keys()[idx]`, built through the same
    /// `build` path `spawn` uses (only skipping the final `tokio::spawn(core.run(..))`
    /// so the test can call `dispatch_inbound` directly and inspect the result).
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
            _rx_payload_ready,
            _tx_vantage,
            _tx_bulk,
            _ack_aggregator,
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

    /// A keypair guaranteed not to be in `crate::common::committee()` (whose four
    /// members are seeded from `StdRng::from_seed([0; 32])` -- a disjoint seed here
    /// deterministically avoids any collision).
    fn fabricated_key() -> PublicKey {
        let mut rng = StdRng::from_seed([7; 32]);
        let (pk, _sk) = generate_keypair(&mut rng);
        pk
    }

    fn dummy_proposal() -> ViewProposal {
        ViewProposal {
            view: 1,
            c: Vec::new(),
            t: Vec::new(),
            m: None,
        }
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

        // The duplicate arrives after one batch is in the store but before the core
        // consumes that batch's payload-ready event. Its fresh probe therefore asks
        // only for `missing`; the old pending entry for `first_arrival` must survive.
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

    /// Positive control: an honest, real committee member's Echo is NOT dropped by
    /// the gate -- it reaches `AgbEngine::on_echo` and produces the same effects the
    /// gate-free code path always did (the gate is a no-op on every honest path).
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
        };
        let effects = core
            .dispatch_inbound(Inbound::Echo(EchoOut::Single(echo)), Instant::now())
            .await;

        // Not dropped by the gate: the rejection counter stays at zero. (The specific
        // downstream `AgbEngine`/`Pacemaker` effects for this exact input are already
        // covered by `vantage/tests/agb_echo_tests.rs`; this test's only job is
        // proving the gate doesn't also swallow honest, real-member traffic.)
        assert_eq!(rejected_count(&core), 0);
        let _ = effects;
    }

    /// End-to-end over a real TCP loopback (mirrors `network::receiver_tests::receive`):
    /// a plain ACK still deserializes and reaches `VantageCore` once it advances
    /// aggregate availability.
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
        // This test only exercises consensus-class inbound (an Ack), so the bulk queue
        // is never used -- but it must exist. Held so it is not dropped/closed.
        let (tx_bulk, _rx_bulk) = channel(4);
        let handler = VantageReceiverHandler {
            tx: tx_vantage,
            tx_bulk,
            ack_aggregator,
            metrics: None,
        };

        let address: SocketAddr = "127.0.0.1:14510".parse().unwrap();
        network::Receiver::spawn(address, handler);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let ack = Ack::new(reference.0, reference.1, reference.2.clone(), sender);
        let payload = bincode::serialize(&PrimaryMessage::VantageAck(ack)).unwrap();

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        transport.send(Bytes::from(payload)).await.unwrap();

        let received = tokio::time::timeout(Duration::from_millis(500), rx_vantage.recv()).await;
        assert!(
            matches!(received, Ok(Some(Inbound::AckAvailability(_)))),
            "must deliver the ACK once it advances availability"
        );
    }

    // --- Mechanism A (sender-side lane resume, `vantage::resume`) ---
    //
    // `ResumeTrigger`/`ResumeServe`'s own pure trigger/backoff/dedup logic is covered
    // in `vantage::resume`'s own test module; these tests cover the AUTHOR-side
    // serve path's protocol-level checks (foreign-lane rejection, floor clamp,
    // one-batch-not-tip pacing), which need a real `VantageCore` (`LaneManager`'s
    // block cache) to exercise meaningfully.

    /// "author ignores resume requests for lanes it doesn't own": a `LaneResume`
    /// naming some OTHER committee member's lane must produce no effects at all,
    /// regardless of how well-formed the rest of the message is.
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

    /// "author clamps below-floor requests": `from = 0` is below the earliest real
    /// block height (1 -- height 0 is the implicit, never-transmitted genesis).
    /// Serving must clamp up to 1, not attempt (and fail) to serve height 0.
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

    /// The design doc's "one batch per request" pacing: even when this party's own
    /// tip is far beyond `resume_batch` blocks past `from`, a single `LaneResume`
    /// serves at most `resume_batch` blocks -- it does not loop to tip. (`test_core`
    /// uses `Parameters::default()`, whose `resume_batch` default is 8.)
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

    /// A request whose (already-clamped) `from` is beyond this party's own tip --
    /// e.g. a requester racing ahead, or asking again right after having already
    /// caught up -- must produce no effects (nothing to serve), not panic on an
    /// inverted range.
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

    /// Rate-limit / dedup: two IDENTICAL, back-to-back `LaneResume(author, from,
    /// requester)` requests (the requester retrying before the first batch's effect
    /// on its own frontier could possibly have landed) must not double-serve --
    /// `ResumeServe::should_serve`'s one-shot-per-`resume_backoff_ms` dedup, exercised
    /// end-to-end through `dispatch_inbound`.
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

    /// Pacing fix, design doc step 3: once a gap-episode is ESTABLISHED, receipt of
    /// a publish that advances `frontier(author)` must trigger the next
    /// `VantageLaneResume` immediately, through `Inbound::Publish`'s own
    /// receipt-continuation hook -- NOT only on the next `resume_tick`. Exercised
    /// end to end: two direct `try_resume_request` calls establish the episode
    /// (mirroring two ticks with the gap still open), then a single
    /// `Inbound::Publish` for `author`'s own next height is dispatched with NO
    /// third tick in between, and the resume-request counter must still have
    /// advanced by exactly one -- because the mark is set well AHEAD of that one
    /// height (avail=5, not 1), so a gap genuinely remains after frontier reaches
    /// 1, exactly the "far-ahead gap, one small step closer" shape a real
    /// multi-round-trip catch-up has at any given step.
    ///
    /// `resume_batch` is overridden to 1 here: this test's own concern is the
    /// WIRING (does `Inbound::Publish` actually call `try_resume_request`), not
    /// the in-flight span-sizing `ResumeTrigger::check`'s own unit tests already
    /// cover directly (`established_episode_waits_for_the_whole_in_flight_batch_
    /// before_continuing`) -- with the real default (64) a single-height publish
    /// would correctly NOT fully land an in-flight span and this test would need
    /// to simulate the whole batch instead of just one header.
    #[tokio::test]
    async fn dispatch_publish_continues_established_episode_without_a_third_tick() {
        let mut core = test_core(0, "lane_resume_receipt_continuation");
        core.resume_batch = 1;
        let (author, _) = crate::common::keys()[1];
        let (other_sender, _) = crate::common::keys()[2];

        // Manufacture an (f+1)-availability mark at height 5 for `author`'s lane,
        // via the same shared `AckAggregator` two real acks would use -- crosses
        // `validity_threshold` (2 for this n=4 test committee) without a live
        // network. `avail_high` is a pure ack-census fact, independent of what
        // this party has itself cached -- see that field's own doc comment.
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

        // Two ticks -- the gap (frontier=0, avail=1, from=1) is unchanged across
        // both, exactly as `resume_tick`'s own loop would present it.
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

        // The batch lands: `author`'s own height-1 header arrives as an ordinary
        // publish (this is exactly what a resumed `Header(_, false)` looks like on
        // arrival -- see `Wire::enqueue_resume_header`'s own doc comment). `t2` is
        // well within ONE tick period of `t1` -- no third tick ever runs.
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

    // --- reconnect-replay plan (server-authoritative floor, v3): a SEPARATE
    // mechanism from Mechanism A above (see `vantage::resume`'s own module doc
    // comment). Lives here, not in `vantage::tests::`, for the SAME reason the
    // LaneResume tests above do: `pending_low`/`outbox`/`wire` and `dispatch_
    // inbound`/`build` are private to this module, and the synchronous
    // `vantage::tests::harness` has no transport-level concept at all (no
    // sessions, no volatile sends, no dirty/in-flight maps) to model this
    // mechanism's own tests against -- see `harness.rs`'s own `Inbound::
    // ResumeHello`/`ReplayDone` arm for that boundary. `network::reliable_sender_
    // tests` separately covers the TRANSPORT half (A1's handler-less-entry
    // survival, the four session-death drop-accounting paths, reconnect events,
    // the backoff cap).

    /// `peer`'s real primary address in `core`'s own (fixed, `crate::common::
    /// committee()`-derived) address book.
    fn addr_of(core: &VantageCore, peer: PublicKey) -> SocketAddr {
        core.wire
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
            .expect("peer must be an other-primary of this test committee")
    }

    /// Swaps `core.wire.replay_tx` for a fresh, test-owned channel, returning the
    /// receiving end so a test can inspect exactly what the Hello-serving/nudge
    /// code enqueues. `build()` (via `test_core`) already spawned a real
    /// resume-sender task fed by the ORIGINAL channel -- swapping the `Sender`
    /// simply orphans that task (it idles forever on an empty channel no one
    /// sends to anymore), harmless for a test that then exits and tears down its
    /// whole `#[tokio::test]` runtime, tasks included.
    fn intercept_resume_channel(core: &mut VantageCore) -> Receiver<wire::ReplaySend> {
        let (tx, rx) = wire::ReplaySender::channel(usize::MAX);
        core.wire.replay_tx = tx;
        rx
    }

    /// Forces every subsequent `Wire::enqueue_replay` `try_send` to fail with
    /// `Closed` -- audit-3 A2's own Err arm.
    fn break_resume_channel(core: &mut VantageCore) {
        let (tx, rx) = wire::ReplaySender::channel(usize::MAX);
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

    /// **The B1 killer test** (design doc §11, first bullet -- "the single most
    /// important test in this change"): audit-2's B1 race was a session-2
    /// piggyback landing FIRST and inflating the REQUESTER's own claimed Hello
    /// floor, which a requester-latched-floor design (v1/v2) would have trusted,
    /// permanently skipping the genuinely-dropped earlier suffix. v3 closes this
    /// by construction: the serve floor is `min(hello.floor, pending_low[requester])`,
    /// and `pending_low` is fed EXCLUSIVELY by this node's own exact drop reports,
    /// never by anything the requester claims. Here: `peer` genuinely dropped
    /// everything from view 5 onward (simulated via a direct dirty-map insert --
    /// exactly the report `network::Connection::report_dropped` computes for real
    /// at session death); `peer`'s Hello nonetheless claims a floor of 100 (as it
    /// would if its own tracking had already jumped ahead from a later, out-of-
    /// order session-2 arrival). The served suffix must start at 5 regardless.
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

    /// A healthy, caught-up peer's Hello (`pending_low = None`, floor at or past
    /// the outbox's own tip) serves an EMPTY slice -- `Done(complete=true)` only,
    /// zero duplicate volume in steady state (§2.4).
    #[tokio::test]
    async fn resume_hello_from_a_caught_up_peer_serves_nothing_and_reports_complete() {
        let mut core = test_core(0, "reconnect_caught_up");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        let mut rx = intercept_resume_channel(&mut core);
        // floor=6: "I need key 6 onward" -- the peer already has key 5 (the
        // outbox's only entry), so this genuinely is the caught-up case.
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

    /// GC interaction (§5/§6): once the outbox has pruned below what a peer
    /// actually needs, the serve is clamped up to `outbox_floor` and `Done`
    /// reports it -- the "recovered-with-gap" case.
    #[tokio::test]
    async fn resume_hello_clamps_to_outbox_floor_and_reports_clamped() {
        let mut core = test_core(0, "reconnect_clamped");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));
        core.outbox.record(50, Bytes::from_static(b"fifty"));
        core.outbox.prune_below(20); // views 5..20 are now irrevocably gone

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

    /// §6: "A Hello from X while X is in-flight ... is ignored -- the in-flight
    /// stream already serves >= pending_low, a superset of any concurrent ask."
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

        // A second Hello while that stream is still marked in-flight (nothing
        // drained/removed it -- see `intercept_resume_channel`'s own doc comment).
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

    /// Audit-3 A6: an in-flight entry older than `replay_episode_max_ms` is stale,
    /// not genuinely in flight -- it is evicted (bumping the TTL-expiry metric) and
    /// the Hello is served normally.
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

    /// Audit-3 A2: a `try_send` failure onto the resume-sender task's channel
    /// leaves `pending_low` untouched (never raised, never cleared) and counts the
    /// drop metric instead -- the next ask (once the channel has room again)
    /// re-serves normally.
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

        // The channel recovers (e.g. the task caught up) -- the next ask succeeds.
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

    /// **Adversarial-audit FINDING 1** (MAJOR, the observed 60s serve stalls):
    /// insert-after-enqueue race. `enqueue_replay`'s `try_send` makes the stream
    /// visible to `run_replay_sender` immediately; that task's own ticker is
    /// starved on a quiet system (`if !streams.is_empty()` never polls it while
    /// idle), so with `MissedTickBehavior::Delay` its first poll after a stream
    /// arrives fires IMMEDIATELY -- the task can pop/send/`remove` before the
    /// core's own insert runs, if the insert happens AFTER the enqueue (plain
    /// thread parallelism between two tokio tasks, not reproducible
    /// deterministically in a single-threaded unit test -- what IS deterministic,
    /// and what this test pins, is the STATE INVARIANT the fix establishes:
    /// inserting before enqueueing means the marker is always present by the
    /// time the enqueue's own effects could possibly become visible to a
    /// consumer, so a "task-side remove" -- simulated directly here, standing in
    /// for `run_replay_sender`'s own tail -- always finds it).
    #[tokio::test]
    async fn resume_hello_in_flight_marker_is_present_for_a_task_side_remove_to_find() {
        let mut core = test_core(0, "reconnect_finding1_success");
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
        // Simulate the task-side remove (`run_replay_sender`'s own tail, run
        // after it sends that stream's `Done`) -- it must always find the entry
        // the core placed, never race one that hasn't happened yet.
        assert!(
            core.wire.in_flight.lock().remove(&peer).is_some(),
            "a task-side remove must always find the entry -- insert-before-\
             enqueue closes the window where it could race an insert that \
             hasn't happened yet"
        );
    }

    /// FINDING 1's complementary path: a forced `try_send` failure means no
    /// stream ever reached the task -- the in-flight entry inserted just before
    /// the attempt must be removed again immediately, never left stranded until
    /// the unrelated 60s TTL expiry sweeps it. (Also covered incidentally by
    /// `try_send_failure_leaves_pending_low_unchanged_and_next_ask_recovers`
    /// above; kept as its own focused test for direct traceability to this
    /// finding.)
    #[tokio::test]
    async fn resume_hello_enqueue_failure_leaves_no_stranded_in_flight_entry() {
        let mut core = test_core(0, "reconnect_finding1_failure");
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

    /// §2.6: `Done(complete=false)` is a continuation -- the episode toward the
    /// author re-opens (or stays open) and the NEXT Hello is sent immediately, not
    /// backoff-gated (unlike the periodic tick's own re-Hello).
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

    /// `Done(complete=true)` closes the episode.
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

    /// `clamped = true` bumps the metric regardless of `complete`.
    #[tokio::test]
    async fn replay_done_clamped_bumps_the_metric() {
        let mut core = test_core(0, "reconnect_done_clamped_metric");
        let (author, _) = crate::common::keys()[1];
        let before = done_clamped(&core);

        core.dispatch_inbound(Inbound::ReplayDone(42, true, true, author), Instant::now())
            .await;

        assert_eq!(before + 1, done_clamped(&core));
    }

    /// **Audit-3 A3's own regression**: the nudge condition is keyed to "backoff
    /// elapsed since the last SERVE-OR-NUDGE", not "since `pending_low` was set" --
    /// the weaker (rejected) form would silence the backstop after exactly one
    /// partial serve, even though `pending_low` legitimately stays set across many
    /// serves under budget truncation. A forged `Done(complete=true)` (dispatched
    /// as if `peer` had somehow answered on its own behalf) is interleaved to
    /// demonstrate D4 caveat (iii): it can only ever close OUR OWN requester-side
    /// episode toward `peer` (irrelevant here -- we are the AUTHOR in this test,
    /// not a requester toward `peer`) and has NO effect on `pending_low`/the nudge
    /// condition, both of which are exclusively OUR OWN server-side facts.
    #[tokio::test]
    async fn nudge_fires_after_partial_serve_despite_a_forged_complete_done() {
        let mut core = test_core(0, "reconnect_a3_nudge");
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));
        core.outbox.record(10, Bytes::from_static(b"ten"));

        let mut rx = intercept_resume_channel(&mut core);
        let now = Instant::now();

        // A PARTIAL serve (budget=1 byte forces truncation at the first key) --
        // `pending_low` is raised (not cleared), and the serve itself refreshes
        // `nudge_memo` (a serve counts as "activity" toward the shared cooldown).
        core.replay_serve_max_bytes = 1;
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), now)
            .await;
        let wire::ReplaySend { done, .. } = rx.try_recv().unwrap();
        assert!(matches!(
            done,
            PrimaryMessage::VantageReplayDone(6, false, false, _)
        ));
        assert_eq!(core.pending_low.get(&peer).copied(), Some(6));

        // The forged Done: no effect on `pending_low` or the nudge condition.
        core.dispatch_inbound(Inbound::ReplayDone(999, true, false, peer), now)
            .await;
        assert_eq!(core.pending_low.get(&peer).copied(), Some(6));

        // Immediately after the serve, the nudge must NOT fire (nudge_memo was
        // just refreshed by the serve itself, and the in-flight entry the serve
        // itself set is also still fresh).
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

        // Past both the in-flight TTL and the serve-or-nudge backoff, with
        // `pending_low` STILL set (the serve was partial): the nudge must fire.
        let later = now + Duration::from_millis(65_000);
        core.maybe_nudge(peer, later, Duration::from_millis(4_000))
            .await;
        assert_eq!(
            nudges_before + 1,
            nudges_sent(&core),
            "A3: backoff since the last serve-or-nudge has elapsed, and pending_low \
             is still set -- the nudge must fire regardless of the earlier serve \
             or the forged Done"
        );
    }

    /// The nudge loop's own Hello send rides the VOLATILE class (§14 A7) -- a
    /// smoke check that it does not panic/hang and does bump the metric exactly
    /// once per fired nudge (the send itself is covered end-to-end by
    /// `network::reliable_sender_tests`).
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

    // --- KNOB 1 (measurement ablation, `Parameters::reconnect_replay`): with the
    // mechanism disabled, `VantageCore` must behave exactly as it did before it
    // existed -- see that field's own doc comment for the full rationale. `Parameters
    // ::default()` (what `test_core` builds with) has `reconnect_replay: true`, so
    // these tests flip the field directly on the constructed core, the same
    // established idiom `dispatch_publish_continues_established_episode_without_a_
    // third_tick` above already uses for `resume_batch`.

    /// `broadcast_recorded` -- the single choke point every one-shot AGB/consensus
    /// broadcast passes through -- must leave the outbox empty and send DURABLE
    /// (`Wire::broadcast_message`) rather than volatile when disabled. Observed via
    /// `Wire::cancel_handlers`, the same seam `Wire::prune_cancel_handlers`'s own doc
    /// comment already documents as the durable/volatile discriminator: "a VOLATILE
    /// send never allocates a cancel handler at all ... `cancel_handlers` therefore
    /// only ever grows from the traffic that stays durable".
    #[tokio::test]
    async fn broadcast_recorded_with_replay_disabled_skips_outbox_and_goes_durable() {
        let mut core = test_core(0, "knob1_broadcast_recorded_disabled");
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
            "the outbox must stay empty when the mechanism is disabled"
        );
        assert_eq!(
            core.wire.cancel_handlers.len(),
            other_primaries,
            "a durable broadcast allocates one cancel handler per other primary -- \
             a volatile send would allocate none at all"
        );
    }

    /// An incoming `ResumeHello` while disabled must produce no `Replay` enqueue and
    /// no `pending_low`/in-flight state change -- fully inert, not merely served
    /// less aggressively. Content already sitting in the outbox (e.g. left over
    /// from before the flag was flipped) must not be served from either.
    #[tokio::test]
    async fn resume_hello_is_a_no_op_when_replay_is_disabled() {
        let mut core = test_core(0, "knob1_resume_hello_disabled");
        core.reconnect_replay = false;
        let (peer, _) = crate::common::keys()[1];
        core.outbox.record(5, Bytes::from_static(b"five"));

        let mut rx = intercept_resume_channel(&mut core);
        core.dispatch_inbound(Inbound::ResumeHello(5, peer), Instant::now())
            .await;

        assert!(
            rx.try_recv().is_err(),
            "no Replay must ever be enqueued while the mechanism is disabled"
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

    /// `ReplayDone` while disabled is equally inert -- no episode ever opens.
    #[tokio::test]
    async fn replay_done_is_a_no_op_when_replay_is_disabled() {
        let mut core = test_core(0, "knob1_replay_done_disabled");
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

    /// The `resume_tick` arm's own v3 body (episode re-ask + nudge,
    /// `resume_tick_replay_effects`) is inert when disabled -- no Hello sent (no
    /// episode opens) and no nudge counted, even with `pending_low` set (e.g. stale
    /// state from before the flag was flipped). Mechanism A's own `try_resume_request`
    /// is a separate call in `run`'s loop, not part of this method -- see that
    /// method's own doc comment.
    #[tokio::test]
    async fn resume_tick_replay_effects_are_inert_when_disabled() {
        let mut core = test_core(0, "knob1_resume_tick_disabled");
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
