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
/// Retained candidate checkpoint boundaries (plan section 7.1). Not configurable: the
/// requester only ever wants the HIGHEST certified target above its local head, so older
/// candidates are dead weight, and this exists purely to bound memory against a peer
/// announcing arbitrary future boundaries.
const SEQUENCE_CANDIDATE_WINDOWS: usize = 32;
/// Recent fixed boundaries carried by one announcement frame. At the default 20-view
/// interval this covers 160 views, comfortably beyond normal healthy cursor spread.
const SEQUENCE_ANNOUNCE_BOUNDARIES: usize = 8;
/// Delta views requested in one state-sync range frame. The byte bound is still
/// `sequence_sync_chunk_digests`; this only limits the number of tiny/empty views a
/// single response can advance across.
const SEQUENCE_DELTA_RANGE_VIEWS: usize = 256;
/// Digests per checkpoint-source header request. Responses are separately capped into
/// bounded header batches, so this limits requester and source work per inbound frame.
const SEQUENCE_BLOCK_REQUEST_BATCH: usize = 256;
/// Headers per dedicated sequence response. Header sizes vary with payload manifests;
/// 64 keeps a response near the existing frame norm while amortizing ingress dispatch.
const SEQUENCE_BLOCK_SERVE_BATCH: usize = 64;
/// Unique state-sync header digests outstanding at once. Larger than repair's generic
/// 512-ask cap because each digest goes to one certified source, not a widening fan-out.
const SEQUENCE_BLOCK_MAX_IN_FLIGHT: usize = 2_048;
/// Refill in batches after half the window drains. Topping up one digest per response
/// would turn the batch path back into one-request-per-header traffic.
const SEQUENCE_BLOCK_REFILL_AT: usize = SEQUENCE_BLOCK_MAX_IN_FLIGHT / 2;
/// Installation is latency-sensitive once a target is verified: driving it on the
/// 2-second announcement cadence capped a late joiner at eight views/second with the
/// default 16-view budget. A dedicated tick keeps each core turn bounded while allowing
/// enough turns to overtake the live view rate.
const SEQUENCE_INSTALL_DRIVE_PERIOD_MS: u64 = 100;
/// Slack added above the entry frontier when stamping `sequence_live_intake_floor` at a
/// shed off-edge. Views at the frontier itself are still mid-pipeline -- their echo/ready
/// waves complete after the edge and remain sealable -- but views a wave-or-two below it
/// may have sealed on the fleet just before intake resumed, with their evidence already
/// lost. At the measured ~13 views/s a wave stays in flight for well under a second;
/// 16 views is that horizon doubled. Overshoot only costs one or two more installed
/// checkpoints before the latch; undershoot re-creates the dead-zone wedge.
const SEQUENCE_LIVE_INTAKE_MARGIN: crate::primary::View = 16;

use crate::vantage::cursor::Cursor;
use crate::vantage::frontier::Frontier;
use crate::vantage::install::{RebaseOutcome, SequenceInstall};
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
use crate::vantage::sequence::{
    genesis_head, head_hex, head_prefix_i64, CheckpointCollector, SequenceAnnouncement,
    SequenceDeltaChunk, SequenceDeltaRangeChunk, SequenceDeltaRangeRequest, SequenceDeltaRequest,
    SequenceOutcome, SequenceOutcomeRequest, SequenceOutcomeServe, SequenceRecordChunk,
    SequenceRequest, SequenceStore, SequenceTransfer, SequenceUnavailable, SequenceWant,
    TransferState, SEQUENCE_VERSION,
};
use crate::vantage::wire::{self, Wire};

/// One state-sync response, so the four inbound arms share a single handler.
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
use std::sync::atomic::{AtomicBool, Ordering};
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

    // --- SEQUENCE-CHECKPOINT-SYNC-PLAN.md Phase B. Every variant carries the
    // AUTHENTICATED sender alongside the payload: the payload's own encoded sender is
    // decoration and is checked against this, never trusted in its place.
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

    /// Frames worth processing while a large sequence install is active. Everything else
    /// would only build AGB/control/replay/availability state for views the verified
    /// install is about to replace, or spend work serving peers while this node is the
    /// one being rescued. Live header publishes are deliberately excluded: the install
    /// materializes its verified delta through dedicated `SequenceHeaders` frames, and
    /// ordinary `Publish`/`Serve` traffic can otherwise fill the main queue before the
    /// tail is small enough for normal parking to be useful again.
    fn keep_during_large_sequence_sync(&self) -> bool {
        matches!(
            self,
            // WISH is how a node learns where the fleet actually is, and it is what advances
            // its own AGB view -- nothing in the install path does (`enter_view_effects` is
            // reached only from boot and `Effect::Enter`). Shedding it means the catcher
            // keeps proposing for views the fleet has already left, so peers skip-vote its
            // turns: measured 49 of 117 proposer turns committed, 0.42 against a peer 1.00.
            // It is also two integers on the wire, so keeping it costs nothing.
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

    /// View-scoped consensus/control input whose historical work is replaced by a
    /// verified sequence install. Messages without a view stay on the ordinary path when
    /// the install is nearly caught up; while the gap is still large,
    /// `keep_during_large_sequence_sync` applies a stricter policy.
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

/// Short, stable label for a committee member in per-author metrics. Uses the crate's own
/// `Display`, which is the 16-character base64 prefix already used in every log line, so a
/// label can be matched against logs directly. One series per committee member.
fn author_label(author: &PublicKey) -> String {
    author.to_string()
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
    /// Dedicated queue for checkpoint state-sync frames. These are precisely the frames
    /// a late node needs while its main consensus queue is full of historical traffic.
    pub tx_sequence: Sender<Inbound>,
    /// Set by `VantageCore` while a large active sequence-sync gap is being installed.
    /// The receiver uses it to discard stale consensus/control/service frames before
    /// they occupy the main core queue.
    pub sequence_large_gap_drop: Arc<AtomicBool>,
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
            // Phase B. `a.sender` below is the payload's CLAIM; the authoritative
            // identity is resolved at dispatch against the connection.
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
            // Autobahn-only variants never reach the Vantage assembly's port; ignore
            // rather than panic (defense in depth against a misrouted message).
            _ => return Ok(()),
        };
        if self.sequence_large_gap_drop.load(Ordering::Relaxed)
            && !inbound.keep_during_large_sequence_sync()
        {
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_sequence_install_obsolete_inbound_dropped_total
                    .inc();
            }
            return Ok(());
        }

        // Bulk recovery traffic goes to its own queue, non-blocking: a full bulk
        // channel drops the message rather than stalling this receiver task, and the
        // requester re-asks on its next resume tick. Consensus traffic keeps the
        // original awaiting send on its own channel, so nothing about its delivery
        // guarantees changes -- it simply no longer queues behind re-served payload.
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
        // Do not hand a decoded stale frame to the main queue just because this receiver
        // task started waiting before sequence sync raised the large-gap flag. Reserve a
        // slot first, then re-check the policy at the actual enqueue point; otherwise a
        // full queue drains one stale item only to be refilled by an already-blocked
        // sender task with another stale item.
        let permit = self
            .tx
            .reserve()
            .await
            .expect("Failed to reserve vantage message slot");
        if self.sequence_large_gap_drop.load(Ordering::Relaxed)
            && !inbound.keep_during_large_sequence_sync()
        {
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
    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §9: retained records, outcomes, and deltas.
    /// Phase B announces and verifies these but never installs them.
    sequence: Option<SequenceStore>,
    /// Phase B: the f+1 first-hand head collector. `Some` exactly when `sequence` is.
    sequence_sync: Option<CheckpointCollector>,
    sequence_chunk_records: usize,
    sequence_chunk_outcomes: usize,
    sequence_chunk_outcome_items: usize,
    sequence_chunk_digests: usize,
    sequence_sync_min_gap_views: View,
    sequence_sync_shed_gap_views: View,
    sequence_sync_rearm_gap_views: View,
    sequence_large_gap_drop: Arc<AtomicBool>,
    /// Hysteresis for late-joiner recovery. Enter only at the configured gap, then keep
    /// verifying/installing certified tail checkpoints until the cursor reaches the
    /// newest one. Ordinary future messages are allowed to park once that newest gap is
    /// below the threshold; this flag controls target selection, not ingress shedding.
    sequence_sync_recovery_active: bool,
    /// Latched once this node has recovered: state sync is a ONE-SHOT bulk operation, not a
    /// steady-state mechanism, and without a latch it re-arms on every newly certified
    /// boundary and never stops.
    ///
    /// Measured 2026-08-10 before this existed: a recovered joiner ran transfers forever
    /// (0.12-0.36/s indefinitely) and never reached peer parity, because syncing is
    /// self-sustaining -- a staged install makes `install_replaces_inbound` drop ordinary
    /// view-scoped traffic and parks `pump`, so the node advances only at install speed,
    /// drifts behind the fleet, and the next boundary starts another transfer. The
    /// threshold comparison alone cannot break that loop; only a latch can.
    ///
    /// Re-arms solely on a gap so large it can only be a real outage, never on jitter.
    sequence_sync_recovered: bool,
    /// The first view since which view-scoped intake has been UNINTERRUPTED -- stamped
    /// from the entry frontier at every shed off-edge (wish/entry is retained while
    /// shedding, so `a_i` tracks the fleet through an outage), plus a slack margin for
    /// echo/ready waves already in flight at that edge.
    ///
    /// Why it exists (measured 2026-08-10, run anchor1): the latch used to fire the
    /// moment the install caught up to within the sync gate, at local head 2252 -- but
    /// the node had shed all consensus traffic up to ~view 2440, and views in
    /// (2252, 2440] can never seal ordinarily: the evidence is gone, peers never re-send
    /// old echoes/readys, and peers' resolvers never target views THEY already resolved.
    /// The cursor wedged at 2253, the gap regrew past the shed gate, shedding resumed
    /// while the latch held transfers off, and the node went dark (zombie: not syncing,
    /// not participating). Recovery must therefore stay active -- and installs keep
    /// landing -- until the local head crosses this floor, so the ordinary tail-close
    /// the sync-gate comment below relies on is actually possible.
    sequence_live_intake_floor: View,
    /// Previous shed state, for detecting the off-edge that stamps the floor above.
    sequence_shed_was_active: bool,
    sequence_announce_period_ms: u64,
    sequence_announce_repeat_ms: u64,
    /// Phase B: at most ONE installation target at a time (section 7).
    sequence_transfer: Option<SequenceTransfer>,
    sequence_transfer_seq: u64,
    /// Missing verified-output headers requested in batches from certified checkpoint
    /// sources. Target-bound by `sequence_install` and pruned as blocks arrive/install.
    sequence_block_requests: HashMap<Digest, SequenceBlockRequestState>,
    /// Highest remote target fully verified in Phase B. Since verify-only deliberately
    /// does not move the cursor, no newer transfer starts until ordinary dissemination
    /// reaches this target. Phase C replaces that wait by installing the verified state.
    sequence_verified_target: Option<(View, Digest)>,
    /// Phase C staging: the verified target being turned into locally held blocks. At most
    /// one at a time, for the same reason as `sequence_transfer` -- the cursor advances
    /// through a single sequence, so a second concurrent target could only be a target this
    /// one supersedes.
    sequence_install: Option<SequenceInstall>,
    sequence_install_window_views: usize,
    sequence_install_settle_ceiling: usize,
    sequence_install_enabled: bool,
    sequence_install_views_per_tick: usize,
    sequence_install_digests_per_tick: usize,
    /// "Every view is locally held" is a level, not an edge, and the drive runs every
    /// announce tick -- without this the line would repeat until the target retires.
    sequence_install_ready_logged: bool,
    /// Whether any view of the target now awaiting comparison was INSTALLED rather than
    /// executed locally. Decides which match counter the comparison lands in: an installed
    /// view carries the transfer's own outcome and delta into `record_sequence`, so the
    /// comparison no longer has two independent sides.
    sequence_target_installed: bool,
    /// When the outstanding request was issued, for the failover deadline.
    sequence_request_at: Option<Instant>,
    /// What the last emitted request asked for. A response only re-emits when the WANT
    /// actually changed; see `on_sequence_response`.
    sequence_last_want: Option<SequenceWant>,
    sequence_request_timeout_ms: u64,
    sequence_max_sources: usize,
    /// Last (boundary, instant) we announced, for the repeat rule above.
    last_announced: Option<(View, Instant)>,
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
    /// AVAIL-ECHO-SPEC.md (`Parameters::echo_avail_claims`): carry availability
    /// acknowledgments positionally on the AGB echo instead of `VantageAvail`'s explicit
    /// tuples. Gates BOTH sides of the swap -- emission of the claim in
    /// `Effect::BroadcastEcho`, and whether `avail_tick` is scheduled at all.
    echo_avail_claims: bool,

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
    /// Last published `(chain, direct, settle)` walk-step totals, so `sample_metrics` can
    /// emit DELTAS into `vantage_walk_steps_total` (a Prometheus counter) from three
    /// monotonic in-process `u64`s. The counters themselves live where the walks are
    /// (`BlockCache`, `Repairer`) because incrementing a labeled metric per visited node
    /// would cost as much as the step being counted.
    walk_steps_published: (u64, u64, u64),
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
    // Checkpoint state-sync queue -- see `VantageReceiverHandler::tx_sequence`.
    Receiver<Inbound>,
    Receiver<(Digest, Digest, WorkerId)>,
    Sender<Inbound>,
    Sender<Inbound>,
    Sender<Inbound>,
    SharedAckAggregator,
    Arc<AtomicBool>,
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
    ) -> (
        Sender<Inbound>,
        Sender<Inbound>,
        Sender<Inbound>,
        SharedAckAggregator,
        Arc<AtomicBool>,
    ) {
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
        )
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
        let sequence_capacity = parameters.sequence_sync_inbound_capacity.max(1);
        let (tx_sequence, rx_sequence) = channel(sequence_capacity);
        let (tx_payload_ready, rx_payload_ready) = channel(CHANNEL_CAPACITY);
        let sequence_large_gap_drop = Arc::new(AtomicBool::new(false));

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
        // Built BEFORE `sid` is moved into `ControlLog` below.
        let sequence = parameters.sequence_checkpoints.then(|| {
            SequenceStore::new(sid.clone(), parameters.sequence_checkpoint_interval_views)
        });
        // f+1 for n = 3f+1, derived from the committee here so `CheckpointCollector`
        // stays free of committee plumbing.
        let sequence_sync = parameters.sequence_checkpoints.then(|| {
            let n = committee.size();
            let f = n.saturating_sub(1) / 3;
            // A restarted/late node is, by definition, far behind the fleet. Bounding
            // checkpoint announcements by `local_cursor + K` makes it ignore the very
            // anchors it needs to escape. Memory is already bounded by
            // `SEQUENCE_CANDIDATE_WINDOWS`, and certification still needs f+1 matching
            // authenticated senders, so production accepts any future boundary and lets
            // the bounded candidate map evict low-value ones.
            CheckpointCollector::new(f + 1, SEQUENCE_CANDIDATE_WINDOWS, View::MAX)
        });
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
            sequence_sync_recovery_active: false,
            sequence_sync_recovered: false,
            sequence_live_intake_floor: 0,
            sequence_shed_was_active: false,
            sequence_announce_period_ms: parameters.sequence_announce_period_ms,
            sequence_announce_repeat_ms: parameters.sequence_announce_repeat_ms,
            sequence_transfer: None,
            sequence_transfer_seq: 0,
            sequence_block_requests: HashMap::new(),
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
            echo_avail_claims: parameters.echo_avail_claims,
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
                last_synchronize: HashMap::new(),
                last_retry_synchronize: HashMap::new(),
                last_synchronize_pruned_at: Instant::now(),
                metrics: core_metrics.clone(),
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
            walk_steps_published: (0, 0, 0),
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
        // BEFORE anything can publish. A process that restarts without its lane frontier
        // re-signs a different block at a height it already used, and every honest node's
        // output cursor wedges on the fork -- see `lanes::OWN_FRONTIER_KEY`.
        self.lm.restore_own_frontier().await;
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
        // AVAIL-ECHO-SPEC.md: when claims ride the echo they REPLACE this flush, so the
        // tick is not scheduled at all -- that is where the 92.2% of wire bytes
        // `VantageAvail` accounted for actually goes away. Without this the two paths
        // would both run and the optimization would cost bandwidth instead of saving it.
        let mut avail_tick = if self.ack_watermarks && !self.echo_avail_claims {
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

        // SEQUENCE-CHECKPOINT-SYNC-PLAN.md Phase B: the announcement tick, constructed
        // only when checkpoints are on (same Option-guarded-select idiom as `avail_tick`).
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

                // Checkpoint state-sync traffic must not sit behind the ordinary queue it
                // is trying to bypass for a late node. The queue is bounded and fed with
                // try_send at the receiver; loss is handled by repeated announcements and
                // requester timeouts, same as sequence egress loss.
                Some(inbound) = rx_sequence.recv() => {
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
                    // Fetch and application have their own view/digest/in-flight budgets.
                    // Keep this independent of checkpoint announcements: a verified late
                    // joiner must apply faster than the fleet advances, rather than in a
                    // 16-view burst every two seconds.
                    let install_effects = self.drive_sequence_install().await;
                    if !install_effects.is_empty() {
                        self.execute(install_effects, Instant::now()).await;
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
                    let mut retry_effects = self.digest_stmts.retry_fetches(retry_now);
                    // n=100 recovery fix (2026-08-07): widen the repair fan-out for
                    // digests still outstanding. `Repairer` asks only `FANOUT_FIRST` peers
                    // on the first miss -- asking all n-1 at once is what let a small
                    // asymmetry become a permanent one (see `Repairer::fan_out`) -- so
                    // full coverage, and therefore N6's eventual guarantee, is reached
                    // here. Same tick, same rationale as `retry_fetches` above: coarse,
                    // budgeted, no dedicated timer.
                    // Feed the queue depth to `fan_out`'s per-digest escalation-width cap.
                    // The emit ceiling deliberately does not read it in the queue-backoff
                    // ablation; see `Repairer::adapt_recovery_ceiling`.
                    self.rep.observe_core_queue(rx_vantage.len());
                    retry_effects.extend(self.rep.retry_requests());
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
        // Republish here as well as in `sync_batches`: this is the SHRINK path, and a node
        // that has caught up stops calling `sync_batches` entirely -- without this the
        // gauges would freeze at their high-water mark and read as a permanent backlog.
        self.payload.publish_sizes();
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
    /// Drop cached blocks that every peer has confirmed holding.
    ///
    /// NOT CALLED. Kept, with its mechanism and tests, because the analysis is worth more
    /// than the code: this floor is UNSOUND and the measurement that shows it is cheap to
    /// repeat. Do not wire it back in without fixing the flaw below.
    ///
    /// Motivation (2026-08-07): `BlockCache` has no eviction at all -- "every block this node
    /// has ever obtained" -- and after the `AckAggregator` retirement it is the dominant
    /// remaining leak: 2.504 MB/s/node at n=30, ~4,286 B per entry, `block_cache_len` climbing
    /// at exactly the committee block rate. At n=100 that is OOM against 8 GiB in ~8-10 min.
    /// Enabling this did work as a memory fix -- `block_cache_len` went flat (+0/s from
    /// +584/s) and growth fell to 1.463 MB/s/node.
    ///
    /// WHY IT IS UNSOUND. The floor is "all n-1 peers have credited this lane at or above h"
    /// (`Repairer::universally_held_below`), which establishes that no PEER can still need h
    /// from us. It says nothing about whether WE still need h: `Cursor::expand` requires every
    /// block named by a manifest at any view >= `next_view`, and those manifests can reference
    /// arbitrarily low lane heights. Worse, the condition is perversely inverted -- a lagging
    /// node is exactly the one whose peers are all ahead of it, so the floor comes back HIGH
    /// precisely for the node that can least afford to drop anything, and it evicts the blocks
    /// its own cursor still needs to output.
    ///
    /// Measured, with `--withhold 5` (local-dryrun/repro-anchor.sh, 60s, n=30):
    ///     eviction off:  entered/cursor 1749/532,  vote_skip     60
    ///     eviction on:   entered/cursor 1758/49,   vote_skip 23,575
    /// i.e. the output cursor collapsed 10x and the committee skipped 400x more views.
    ///
    /// Note WHICH test caught it: n=20 @ 1000 tx/s and n=30 @ 100 tx/s both PASSED with
    /// eviction enabled. Only the deliberately asymmetric repro exposed it. A clean-path
    /// suite would have shipped this.
    ///
    /// A correct floor needs a LOCAL-progress term as well -- something bounding the lowest
    /// lane height still reachable from a manifest at or above `Cursor::next_view`. That is
    /// not cheaply invertible from (author, height) today, which is why this is parked rather
    /// than patched.
    ///
    /// SECOND UNSOUNDNESS, found in a 2026-08-08 review and NOT covered by the local-progress
    /// term above -- whoever re-wires eviction must handle both. Evicting a block that is
    /// REQUESTED but not yet SETTLED strands its digest permanently. Such a block exists
    /// whenever a serve arrived but its parent is still missing: it is cached and NOT retained
    /// (retention happens only when a walk verifies through genesis, N8), so it is eligible
    /// for eviction, and `BlockCache::evict_author_below` checks nothing per-entry. Once
    /// evicted, `Repairer` cannot re-ask for it: `requested`/`requested_hashes` are never
    /// pruned (N6), so `settle`'s fan-out gate treats the digest as already covered and emits
    /// nothing, forever. The block is gone, unrequestable, and its whole sub-chain stays in
    /// `pending_settle`. Any eviction floor must therefore also exclude digests in
    /// `requested_hashes` whose ref is not yet in `settled`.
    #[allow(dead_code)]
    fn evict_universally_held_blocks(&mut self) {
        const KEEP_HEIGHTS: crate::primary::Height = 64;
        let mut dropped = 0usize;
        let mut blocked = 0u64;
        for author in self.rep.known_lane_authors() {
            match self.rep.universally_held_below(&author) {
                Some(floor) => {
                    dropped += self.lm.evict_universally_held(&author, floor, KEEP_HEIGHTS);
                }
                None => blocked += 1,
            }
        }
        if let Some(metrics) = &self.metrics {
            if dropped > 0 {
                metrics
                    .vantage_block_cache_evicted_total
                    .inc_by(dropped as u64);
            }
            metrics
                .vantage_block_cache_evict_blocked
                .set(blocked as i64);
        }
    }

    fn sample_metrics(&mut self) {
        // Walk-step deltas first, and via a scoped borrow: the three totals live in
        // `BlockCache`/`Repairer` and this needs `&mut self.walk_steps_published`, which
        // cannot coexist with the long `metrics` borrow the rest of this function holds.
        if self.metrics.is_some() {
            let chain_direct = self.lm.blocks_handle().lock().walk_steps();
            let now = (chain_direct.0, chain_direct.1, self.rep.walk_steps_settle());
            let prev = self.walk_steps_published;
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
            }
            self.walk_steps_published = now;
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
        // Memory fix (2026-08-07): `senders_tracked` must stay near the count of refs
        // still BELOW quorum rather than growing with every block ever seen -- that
        // growth was 13.43 MB/s per node at n=100. `refs_retired` is the residual.
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
        // Sampled HERE, beside the cursor gauge, not at record time. The two are exactly
        // one apart by construction (`head_view == next_view - 1`), but only if they are
        // read at the same instant: setting the head on every record while the cursor
        // gauge refreshes on this periodic tick made the difference transiently negative
        // -- measured -14 views at n=20 -- which would either raise false alarms or force
        // the invariant to be weakened into something that checks nothing.
        if let Some(store) = &self.sequence {
            metrics
                .vantage_sequence_head_view
                .set(store.head_view() as i64);
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

    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §14 Phase A: fold one terminal cursor advance
    /// into the local sequence chain. Record-only -- nothing here announces, fetches, or
    /// installs, and no live AGB state is read or written.
    ///
    /// A rejected record is a LOCAL invariant violation (the cursor finalized a view out
    /// of order), not remote input, so it is logged at error and counted rather than
    /// silently skipped: skipping would leave a gap and produce a head no other correct
    /// party derives, which is exactly the divergence this phase exists to detect. The
    /// store is left untouched so the first bad view stays identifiable.
    fn record_sequence(&mut self, view: View, outcome: &SequenceOutcome, output_delta: &[Digest]) {
        // Consensus contribution, as distinct from lane data blocks: data blocks carry client
        // transactions and are committed by everyone, so they say nothing about whether this
        // node is taking its turn in agreement. What matters is the round-robin PROPOSER turn
        // -- a node that lags simply misses its turns, and the view then seals without its
        // proposal (or skips), which is invisible in any data-block measure.
        if self.agb.proposer(view) == self.name {
            // Diagnostic-only observational log (same convention as the resolver's
            // recovery-attachment line): lets a run correlate exactly WHICH owned views
            // sealed empty with the sender-side events around them -- the 2026-08-10
            // late-joiner diagnosis needed precisely this and had to infer it from
            // peers' echo logs.
            log::info!(
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
                if !matches!(outcome, SequenceOutcome::Skip) {
                    metrics.vantage_own_proposals_committed_total.inc();
                }
            }
        }
        // Taken before `self.sequence` is borrowed mutably below.
        let sid_label = head_hex(self.agb.sid());
        // Phase B's closing check: a target verified against `f+1` announcements is held
        // until ordinary execution independently reaches the same view. Read here, before
        // the store is recorded into, so the comparison uses the head this node derives on
        // its own rather than anything the transfer supplied.
        let awaited = match &self.sequence_verified_target {
            Some((target_view, head)) if *target_view == view => Some(head.clone()),
            _ => None,
        };
        let Some(store) = self.sequence.as_mut() else {
            return;
        };
        match store.record(view, outcome, output_delta) {
            // Cloned rather than held: `record` returns a `&Digest` borrowed from the
            // `&mut store` reborrow, which would block `latest_boundary()` below.
            Ok(head) => {
                let local_head = head.clone();
                let boundary = store.latest_boundary().map(|(v, h)| (v, h.clone()));
                if let Some(metrics) = &self.metrics {
                    metrics
                        .vantage_sequence_delta_digests_total
                        .inc_by(output_delta.len() as u64);
                    metrics.vantage_sequence_records_total.inc();
                }
                // Phase A's entire deliverable: a head that every correct node must
                // agree on at the same boundary. Logged at info so a run can be compared
                // across nodes without a metrics scrape.
                if let Some((boundary_view, head)) = boundary {
                    if boundary_view == view {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_sequence_boundary_view.set(view as i64);
                            // The head must be in the SERIES IDENTITY, not just its
                            // value: two nodes at the same boundary with different heads
                            // is precisely the divergence Phase A hunts, and their
                            // boundary views are identical by construction, so a
                            // view-only export cannot show it. `reset` keeps one active
                            // child per process while Prometheus retains the history the
                            // cross-node comparison reads.
                            metrics.vantage_sequence_boundary_head.reset();
                            metrics
                                .vantage_sequence_boundary_head
                                .with_label_values(&[&sid_label, &head_hex(&head)])
                                .set(view as i64);
                            metrics
                                .vantage_sequence_boundary_head_lo
                                .set(head_prefix_i64(&head));
                        }
                        log::info!(
                            "vantage sequence checkpoint: view={view} head={}",
                            head_hex(&head)
                        );
                    }
                }
                if let Some(expected) = awaited {
                    // Consumed either way: one verified target is compared exactly once,
                    // and leaving it set would keep `drive_sequence_sync`'s "wait for the
                    // cursor to catch up" gate closed against every later target.
                    self.sequence_verified_target = None;
                    // Ordinary execution reached the target under its own power, so the
                    // staged fetch has nothing left to contribute. Dropping it here is what
                    // keeps a node that never actually fell behind from carrying install
                    // state for the rest of the run.
                    self.sequence_install = None;
                    let matched = expected == local_head;
                    let installed = std::mem::take(&mut self.sequence_target_installed);
                    if let Some(metrics) = &self.metrics {
                        if !matched {
                            metrics.vantage_sequence_verify_mismatch_total.inc();
                        } else if installed {
                            // The head compared was derived from the transfer's own
                            // outcomes and deltas, so this is self-consistency, not an
                            // independent agreement. Counted apart so the Phase C gate
                            // cannot be read off a run that installed.
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
                        log::info!(
                            "vantage sequence sync: MATCH at view={view} head={} ({basis})",
                            head_hex(&local_head)
                        );
                    } else {
                        // Nothing is installed in Phase B, so this cannot corrupt state --
                        // it is evidence, and the loudest kind. Either >f announcers
                        // certified a head no correct party derives, or two correct
                        // parties disagree; both forbid Phase C installation.
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

    /// Phase B: start or advance the single active transfer.
    ///
    /// VERIFY ONLY. On reaching `Verified` the downloaded output is counted and dropped --
    /// Phase B deliberately installs nothing, so the cursor, watermarks and output set are
    /// untouched no matter what a peer serves. Installation is Phase C.
    ///
    /// The verified `(view, head)` IS retained, in `sequence_verified_target`, so that
    /// `record_sequence` can compare it against the head ordinary execution derives for
    /// the same view. That comparison is Phase B's actual deliverable: verifying a chain
    /// against `f+1` announcements only proves the peers were self-consistent, whereas
    /// matching it to an independently derived local head proves they were RIGHT.
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

        // Retire a finished transfer before considering a new target.
        match self.sequence_transfer.as_ref().map(|t| t.state()) {
            Some(TransferState::Verified) => {
                // Copied out while the transfer is still alive: `verified_output` borrows
                // it, and the transfer is retired immediately below.
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
                let views = staged.len();
                let (view, head) = transfer.target();
                let (view, head) = (view, head.clone());
                self.sequence_verified_target = Some((view, head.clone()));

                // Phase C staging. Built even though installation is still gated, because
                // the fetch it drives is what makes a later install cheap: by the time the
                // cursor could apply these views the blocks are already local.
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
                    // Checkpoint-source preference (plan §16 decision 4): the parties that
                    // announced this target necessarily held the blocks its manifests name,
                    // so seed repair's holder index with them before any request goes out.
                    // Repair otherwise learns holders only from traffic it has already
                    // seen, which on a node that just fell behind is precisely the traffic
                    // it missed.
                    let tips = install.lane_tips();
                    let announcers = self
                        .sequence_sync
                        .as_ref()
                        .map(|c| c.announcers(view, install.target().1))
                        .unwrap_or_default();
                    for (author, height) in tips {
                        for peer in &announcers {
                            self.rep.note_holder(*peer, author, height);
                        }
                    }
                    self.sequence_block_requests.clear();
                    self.sequence_install = Some(install);
                    self.sequence_install_ready_logged = false;
                    self.sequence_target_installed = false;
                } else {
                    // A verified chain cannot have a hole -- `ChainVerifier` links every
                    // record to its predecessor -- so this means the outcome/delta maps
                    // disagree with the chain. Refuse rather than install a gap.
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

        // Ordinary dissemination won the race. The staged Phase B result is no longer
        // useful and Phase C must never install a target the local cursor has passed.
        if self
            .sequence_transfer
            .as_ref()
            .map(|t| t.target().0 <= local_view)
            .unwrap_or(false)
        {
            let target = self.sequence_transfer.as_ref().expect("present").target().0;
            log::info!(
                "vantage sequence sync: ordinary cursor passed target view={target}; aborting"
            );
            self.sequence_transfer = None;
            self.sequence_request_at = None;
            self.sequence_last_want = None;
        }

        // Active transfers and staged installs are deliberately STICKY. Checkpoint
        // announcements arrive every few seconds; abandoning target 100 for 200, then
        // 300, discarded all verified bytes faster than a stop-and-wait transfer could
        // finish. The collector still remembers newer certified heads. Once this target
        // verifies and installs (or exhausts), the normal target selection below picks
        // the newest one above the now-advanced local head.

        // Pick a target: the highest certified head strictly above what we hold.
        if self.sequence_transfer.is_none() {
            let Some(collector) = self.sequence_sync.as_ref() else {
                return;
            };
            let verified_view = self
                .sequence_verified_target
                .as_ref()
                .map(|(view, _)| *view)
                .unwrap_or(0);
            // Verify-only must not chase every newer boundary while the ordinary cursor
            // is still behind the result it just verified. Phase B waits until normal
            // dissemination reaches that target; Phase C replaces this wait by install.
            if verified_view > local_view {
                return;
            }
            let Some((view, head)) = collector.certified_head(local_view) else {
                return;
            };
            let gap = view.saturating_sub(local_view);
            // Recovered nodes do not sync. Only a gap large enough to be an actual outage
            // re-arms the mechanism; anything smaller is the fleet's ordinary jitter, and
            // treating it as a reason to sync is what produced permanent recovery.
            if self.sequence_sync_recovered {
                if gap < self.sequence_sync_rearm_gap_views {
                    return;
                }
                log::info!(
                    "vantage sequence sync: re-arming after a {gap}-view gap (>= {})",
                    self.sequence_sync_rearm_gap_views
                );
                self.sequence_sync_recovered = false;
                // Must be cleared here too: latch3 reported recovered=1 while the node had
                // already re-armed and was running transfers again, so the gauge asserted the
                // exact opposite of what was happening.
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

        // Fail over on a stalled request rather than waiting forever on a silent peer.
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

    /// Phase C staging drive: admit views of the verified target into the fetch window and
    /// hand their manifest refs to the repairer.
    ///
    /// Returns effects instead of executing them -- `Repairer::authorize` emits ordinary
    /// request effects, and they go through `execute` like every other.
    ///
    /// Pacing lives in `SequenceInstall::admit`: at most `window_views` views outstanding.
    /// Repair backlog is deliberately not a veto here; on a recovering node it is a
    /// symptom of the gap, so using it as a gate disables the rescue path when it is most
    /// needed.
    async fn drive_sequence_install(&mut self) -> Vec<Effect> {
        if self.sequence_install.is_none() {
            self.sequence_block_requests.clear();
            if let Some(metrics) = &self.metrics {
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
        // Ordinary dissemination never stopped while the transfer ran, so the cursor may
        // have moved since the target's base view. Dropping the overtaken prefix keeps a
        // still-useful suffix installable and stops fetching blocks for views already
        // committed; if it overtook the target outright, the target is retired here.
        if !self.rebase_sequence_install() {
            return Vec::new();
        }
        let validation_budget = self.sequence_install_digests_per_tick.max(1);
        let install = self.sequence_install.as_mut().expect("present");
        let examined = install.refresh_budgeted(&blocks, validation_budget);
        let refs = install.admit(self.rep.pending_settle_len());
        let blocks_awaited = install.blocks_awaited(&blocks);

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
            metrics
                .vantage_sequence_install_blocks_awaited
                .set(blocks_awaited as i64);
        }
        if staged_ready && !self.sequence_install_ready_logged {
            self.sequence_install_ready_logged = true;
            if let Some(metrics) = &self.metrics {
                metrics.vantage_sequence_install_ready_total.inc();
            }
            log::info!(
                "vantage sequence install: all {total} views of target view={target} are \
                 locally held"
            );
        }
        effects.extend(self.apply_sequence_install(validation_budget - examined));
        self.drive_sequence_block_fetch(Instant::now()).await;
        effects
    }

    /// Batch missing verified-output headers across certified checkpoint sources.
    ///
    /// Sequence records/outcomes/deltas identify every output digest up front. Feeding
    /// only manifest tips to generic repair is correct but expensive: each requested
    /// body is a separate frame and parent discovery can serialize on WAN RTT. This path
    /// requests the already-committed digest set in 256-item batches, partitions work
    /// across announcers, and rotates an unanswered digest after the ordinary sequence
    /// timeout. Returned headers still pass `Repairer::on_serve`'s `BlockOK` gate.
    async fn drive_sequence_block_fetch(&mut self, now: Instant) {
        let Some((target_view, target_head)) = self
            .sequence_install
            .as_ref()
            .map(|install| (install.target().0, install.target().1.clone()))
        else {
            self.sequence_block_requests.clear();
            return;
        };
        let Some(collector) = self.sequence_sync.as_ref() else {
            return;
        };
        let sources = collector.announcers(target_view, &target_head);
        if sources.is_empty() {
            return;
        }

        let blocks = self.rep.blocks();
        let missing = self
            .sequence_install
            .as_ref()
            .expect("checked")
            .missing_digests(&blocks, usize::MAX);
        let missing_set: HashSet<Digest> = missing.iter().cloned().collect();
        self.sequence_block_requests
            .retain(|digest, _| missing_set.contains(digest));

        let timeout = Duration::from_millis(self.sequence_request_timeout_ms);
        let mut by_source: HashMap<PublicKey, Vec<Digest>> = HashMap::new();
        let mut scheduled = 0usize;

        // Retry timed-out work first, rotating to the next certified source. This runs
        // even while the in-flight window is full; otherwise one withholding source can
        // pin every slot forever.
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

        // Refill only after a substantial part of the window drained, preserving real
        // batches instead of emitting one new request for each arriving header.
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

    /// Re-align the staged target with where the cursor actually is, and revalidate.
    ///
    /// Called before every admission pass. The owner executes one returned effect batch
    /// before this can run again, so the sequence head and cursor boundary observed here
    /// always describe the same applied prefix.
    ///
    /// Returns `false` when the target is gone: either overtaken outright, or refused
    /// because the head local execution derived at the rebase boundary is not the one the
    /// verified chain records.
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
                log::info!(
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
                // The same fact `vantage_sequence_verify_mismatch_total` reports, caught on
                // the install path instead: a correct suffix spliced onto a divergent
                // prefix is still divergent.
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

    /// Apply as many staged views as the per-pass budget allows.
    ///
    /// This is the only path in the system that turns bytes another party derived into
    /// committed output, so it is off unless `sequence_install_enabled` says otherwise, and
    /// bounded at `sequence_install_views_per_tick` per pass on a dedicated 100 ms tick --
    /// the loop runs on the same single-threaded core that serves consensus, and a target
    /// spanning hundreds of views would otherwise stall everything else while it drained.
    ///
    /// Any refusal aborts the WHOLE target rather than skipping the view. `Cursor::install`
    /// only refuses on conditions that mean the target or the local state is not what this
    /// node believed, and continuing past that would install a hole.
    fn apply_sequence_install(&mut self, digest_budget: usize) -> Vec<Effect> {
        let mut effects = Vec::new();
        if !self.sequence_install_enabled || digest_budget == 0 {
            return effects;
        }
        let mut applied = 0usize;
        // Views alone do not bound the work: one view's delta is the entire accumulated
        // lane suffix since the last emitted watermark, so a multi-second gap at n=100 can
        // put thousands of headers behind a single view. The digest budget is what actually
        // keeps a pass off the core, and `Cursor::install` honours it by leaving a view open
        // rather than by refusing it.
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
                    // Any installed prefix makes the eventual target comparison a
                    // self-check, even if ordinary inputs finish this view later.
                    self.sequence_target_installed = true;
                    if !finalized {
                        // Budget exhausted mid-view. The view stays open and resumes next
                        // pass from exactly where it stopped.
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_sequence_install_partial_views_total.inc();
                        }
                        break;
                    }
                    self.sequence_install
                        .as_mut()
                        .expect("present")
                        .mark_installed(view);
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
        // Installation deliberately does not pump parked ordinary inputs between staged
        // views. Release them only after the whole bounded batch is assembled, preserving
        // FIFO ordering of SequenceFinalized effects and the SequenceStore heads they build.
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
                let target = self.sequence_install.as_ref().expect("present").target().0;
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_sequence_install_completed_total.inc();
                    metrics
                        .vantage_sequence_install_completed_view
                        .set(target as i64);
                }
                // Every view at or below the target is now terminally decided. Until the
                // resolver is told, it keeps targeting recovery at a range whose outcome is
                // already committed, and -- since the GC floor is derived from its
                // watermark -- the AGB, control, frontier, digest-statement and timer state
                // for every view skipped over is retained. On the straggler this mechanism
                // exists to rescue, that retained state is the cost that matters.
                self.resolver.note_installed_through(target);
                // Move this node's CONSENSUS position to match its output position.
                //
                // Install advances the cursor and the resolver watermark, but nothing moved
                // the AGB view: `enter_view_effects` is reachable only from boot and
                // `Effect::Enter`, so the view advanced solely through the WISH pacemaker and
                // stayed ~84 views behind. Proposer turns are per-view, so the node arrived
                // at every turn far too late and peers skip-voted it -- measured 49 of 117
                // turns committed, 0.42 against a peer's 1.00. Views at or below `target` are
                // terminally decided here, so wishing past them is exactly what the pacemaker
                // is for; this does not force entry, it declares where this node now is and
                // lets the ordinary quorum rule carry it there.
                effects.push(Effect::RaiseWish(target.saturating_add(1)));
                // Left in place, NOT cleared: the finalize effects this pass produced still
                // have to reach `record_sequence`, and the head comparison there is what
                // proves the installed state matches what was verified. That comparison
                // clears it.
                log::info!("vantage sequence install: applied through view={target}");
            }
        }
        effects
    }

    /// Ask every selected source for the same thing CONCURRENTLY; the first valid copy
    /// wins and the rest are simply ignored when they arrive.
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

    /// Feed one response into the active transfer. An invalid one is counted and dropped:
    /// up to `f` matching announcers may serve corrupt bytes, so this is ordinary
    /// operation, never a fault.
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
        // A valid chunk unblocks the next request immediately rather than waiting out
        // the timeout, so a healthy transfer runs at network speed rather than tick
        // speed -- but ONLY when the want actually advanced.
        //
        // Re-emitting unconditionally is a request amplification bomb: one request goes
        // to `max_sources` peers, each response re-emits to `max_sources` again, and the
        // fan-out squares per round. Measured on a live n=4 cluster as 5.4 MILLION chunks
        // accepted and 22 MILLION frames served for a target of a few dozen views, with
        // zero invalid chunks -- the transfer was not broken, it was screaming. Gating on
        // a changed want makes the duplicate responses from concurrent sources inert,
        // which is exactly what they should be.
        let want = self.sequence_transfer.as_ref().and_then(|t| t.want());
        if want != self.sequence_last_want {
            self.sequence_request_at = None;
            self.drive_sequence_sync();
        }
    }

    /// Phase B: broadcast a bounded suffix of checkpoint boundaries, if any exist.
    ///
    /// Re-sent on an unchanged boundary every `sequence_announce_repeat_ms`, not only
    /// when it advances. Repetition is REQUIRED, not an optimization: a node that starts
    /// late must still be able to collect `f+1` announcements for a boundary the fleet
    /// passed before it existed, and a strictly edge-triggered announcement would never
    /// reach it -- the recovering node is exactly the one that missed the edge.
    ///
    /// Boundaries come from the store only after each record AND its outcome/delta are
    /// retained, so we never advertise a head we cannot serve (section 9's correctness
    /// rule). Sending recent boundaries together is required once the interval is below
    /// healthy cursor spread: latest-only announcements from adjacent boundaries never
    /// form an exact f+1 match even though every head agrees.
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
        // Periodic announcements supersede older ones. Sending them through the durable
        // main pool queued a minute of stale boundary batches for a stopped joiner; the
        // bounded collector then evicted each view before f+1 sender streams aligned.
        // Best-effort sequence egress plus the repeat timer gives the intended latest-
        // state behavior and shares no queue with live consensus.
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

    /// Phase B: count one first-hand checkpoint announcement.
    ///
    /// Never forwarded and never turned into a live-view vote, availability
    /// acknowledgment, or resolution stance -- adopting finalized history must not
    /// interfere with agreement on new views (plan section 5, non-interference).
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

    /// Serve a contiguous record range, or say plainly that we cannot.
    ///
    /// Section 9: never silently clamp a request below the serve floor and present it as
    /// complete -- the requester would treat a short answer as the whole range and
    /// verify a chain that cannot reach the target. An explicit
    /// `SequenceUnavailable` with the authoritative floor lets it try another matching
    /// announcer instead.
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

    /// Serve cached, block-verified committed headers in bounded response frames on
    /// the dedicated sequence transport. The ordinary `HeadersRequest` path responds
    /// with one `Header(_, true)` per digest on the main transport; using it here made a
    /// late joiner refill its own 1000-slot consensus queue with state-sync data.
    fn serve_sequence_headers(&mut self, digests: &[Digest], to: &PublicKey) {
        let blocks = self.rep.blocks();
        let headers: Vec<Header> = {
            let cache = blocks.lock();
            digests
                .iter()
                .filter_map(|digest| {
                    cache
                        .get(digest)
                        .and_then(|entry| entry.block_ok_verified.then(|| entry.block.clone()))
                })
                .collect()
        };
        for chunk in headers.chunks(SEQUENCE_BLOCK_SERVE_BATCH) {
            self.send_sequence(
                to,
                PrimaryMessage::VantageSequenceHeaders(chunk.to_vec(), self.name),
            );
        }
    }

    /// One state-sync frame to one peer.
    ///
    /// Deliberately NOT `broadcast_recorded`: state-sync traffic must not enter the
    /// outbox or the replay accounting, because it is not live-protocol history and
    /// re-delivering it after a reconnect would be pure waste.
    ///
    /// Rides the DEDICATED state-sync sender (section 6.1), not the main pool: this
    /// mechanism exists to relieve a node whose main path is already saturated, so
    /// serving through that path would deepen the congestion it is meant to drain.
    ///
    /// Not `async` and never awaited -- a full egress drops the frame and counts it.
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
            // Repair target choice (2026-08-07): a credit is `sender` telling us it holds
            // `r.0`'s lane at height `r.1`, which is exactly what the repair fan-out needs
            // to pick its first round of peers. The information was already arriving and
            // being discarded; see `Repairer::holders`.
            self.rep.note_holder(sender, r.0, r.1);
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
            Inbound::Serve(header) => {
                let digest = header.id.clone();
                let effects = self.serve_effects(header).await;
                let accepted = effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::BlockCached(d) if d == &digest));
                if accepted && self.sequence_block_requests.remove(&digest).is_some() {
                    if let Some(metrics) = &self.metrics {
                        metrics
                            .vantage_sequence_install_headers_received_total
                            .inc();
                        metrics
                            .vantage_sequence_install_header_requests_in_flight
                            .set(self.sequence_block_requests.len() as i64);
                    }
                    // Receipt-paced refill, but only after half the batch window drains.
                    // Duplicate copies do not remove an entry and therefore cannot
                    // amplify requests.
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
            // --- SEQUENCE-CHECKPOINT-SYNC-PLAN.md Phase B.
            //
            // NOTE on "authenticated": vantage's transport is signature-free, so the
            // authoritative identity here is the DECLARED sender already
            // membership-checked by this function's centralized gate -- the same D4
            // discipline every other first-hand rule uses (Wish, Echo, LaneResume's
            // requester). The f+1 argument therefore rests on the network layer not
            // permitting spoofing, exactly as those rules do; it is not a stronger
            // cryptographic binding, and the plan's proof must say so.
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
            // Requester-side responses. Phase B verifies but never installs; with no
            // active transfer they are unsolicited and changing state on them would be
            // exactly the "answers a pair we never asked for" hazard section 7.3 warns
            // about, so they are counted and dropped.
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
                    // Content-addressed replies need no transfer id, but they must still
                    // name work in the active bounded install window. This also avoids
                    // doing payload/store work for unsolicited historical headers.
                    if !self.sequence_block_requests.contains_key(&digest) {
                        continue;
                    }
                    let header_effects = self.serve_effects(header).await;
                    let valid = header_effects
                        .iter()
                        .any(|effect| matches!(effect, Effect::BlockCached(d) if d == &digest));
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

    /// Highest certified or active sequence-sync target. This includes certified future
    /// work, in-progress transfers, and staged installs: the memory storm starts before
    /// verification if live traffic is allowed to accumulate against a node hundreds of
    /// views behind.
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
            // A sticky transfer may deliberately finish an older boundary while newer
            // ones accumulate. Shedding must follow the REAL certified fleet gap, not
            // just that older target, or it switches ordinary traffic back on halfway
            // through the install and immediately repins the core queue.
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

    /// Target that currently requires ordinary traffic to be shed. Once the newest
    /// certified/selected target is within the configured gap, future messages may park
    /// in the bounded main queue while view-scoped messages covered by the staged target
    /// are still rejected by `install_replaces_inbound`.
    fn large_sequence_sync_target(&self) -> Option<View> {
        // A latched node NEVER sheds. Shedding is only sound while installs will replace
        // the shed range, and the latch is precisely "no more installs" -- shedding while
        // latched is the zombie state observed 2026-08-10 (run anchor1): the wedged node
        // shed the very evidence it needed, went dark at view 2441, and sat with the gap
        // pinned between the shed gate and the re-arm gap. If the gap genuinely reaches
        // the re-arm threshold, the latch clears (drive_sequence_sync) and shedding and
        // syncing resume TOGETHER.
        if self.sequence_sync_recovered {
            return None;
        }
        let local = self
            .sequence
            .as_ref()
            .map(|store| store.head_view())
            .unwrap_or(0);
        let target = self.highest_sequence_sync_target()?;

        // SHED threshold, deliberately far above the sync threshold. These are two
        // independent controls -- back-pressure and mechanism selection -- and driving
        // both from one number is what made a recovering node oscillate: crossing it
        // flipped "drop consensus traffic" and "use sync instead of participation"
        // together, so the node alternated between syncing fast while deaf and
        // participating while falling behind. Measured 2026-08-09 at a single gate of 50:
        // lag cycled 31 -> 66 -> 41 -> 60 -> 31 and never settled.
        //
        // Between the two thresholds the node BOTH participates and syncs, which is the
        // regime that did not previously exist.
        (target.saturating_sub(local) >= self.sequence_sync_shed_gap_views).then_some(target)
    }

    fn refresh_sequence_large_gap_drop(&mut self) {
        let local = self
            .sequence
            .as_ref()
            .map(|store| store.head_view())
            .unwrap_or(0);
        let target = self.highest_sequence_sync_target();
        // Stamp the live-intake floor on the shed OFF-edge, BEFORE recovery is
        // re-evaluated below (the floor is one of its inputs). `a_i` tracks view entry,
        // which the WISH pacemaker keeps advancing through a shed (Inbound::Wish is
        // retained), so at this edge it is the best local estimate of the fleet's
        // current view; the margin covers echo/ready waves already in flight, whose
        // views can still seal from the messages that arrive after this instant.
        // Monotone max: a later, lower stamp must never re-open an already-covered
        // range.
        let shed_active = self.large_sequence_sync_target().is_some();
        if self.sequence_shed_was_active && !shed_active {
            let floor = (self.frontier.a_i() + 1).saturating_add(SEQUENCE_LIVE_INTAKE_MARGIN);
            if floor > self.sequence_live_intake_floor {
                self.sequence_live_intake_floor = floor;
                log::info!(
                    "vantage sequence sync: shed released; live-intake floor stamped at \
                     view={floor}"
                );
            }
        }
        self.sequence_shed_was_active = shed_active;
        // SYNC threshold. State sync can only ever land one cycle behind a moving fleet --
        // each transfer targets a checkpoint that was current when it started, and the
        // fleet advances while it runs, so the residual lag is
        // `cycle_duration * view_rate` (measured 62 views at ~13 views/s ~= one cycle).
        // The tail therefore CANNOT be closed by syncing; it has to be closed by ordinary
        // participation, which is why recovery deactivates here instead of running until
        // the gap reaches zero.
        //
        // ...by ordinary participation OF VIEWS THE NODE HAS EVIDENCE FOR: every view
        // below `sequence_live_intake_floor` was shed, its evidence will never be
        // re-sent, and peers' resolvers never target views they already resolved -- so
        // recovery must also stay active until the local head crosses that floor, or the
        // latch strands the cursor in a dead zone it can never seal (the 2026-08-10
        // zombie: latched at head 2252 with the floor effectively at ~2440, wedged at
        // 2253 forever).
        let was_recovering = self.sequence_sync_recovery_active;
        self.sequence_sync_recovery_active = self.sequence_install_enabled
            && target.is_some_and(|target| {
                target.saturating_sub(local) >= self.sequence_sync_min_gap_views
                    || local < self.sequence_live_intake_floor
            });
        // Inside the sync threshold, stop FETCHING: a transfer competes for exactly the
        // queue and bandwidth the tail now needs for ordinary participation. A staged
        // install is deliberately left to drain -- it applies already-verified state under
        // its own per-tick budget, so it is strict progress, and abandoning it would throw
        // away work the node would immediately have to redo.
        // Latch "recovered" the moment the gap is inside the sync gate. This is the only
        // exit from permanent recovery: the gate alone is re-evaluated against every newly
        // certified boundary, so it re-arms forever, while the latch holds until a gap big
        // enough to be a genuine outage appears.
        // EDGE-triggered on leaving recovery, never a level check. A level check latches at
        // BOOT -- `recovery_active` is trivially false there and no install is staged -- which
        // disables state sync from birth and leaves a genuine joiner stuck at view 1 with zero
        // transfers. Measured exactly that: "RECOVERED at view=0" 59 ms after start.
        if was_recovering
            && !self.sequence_sync_recovery_active
            && !self.sequence_sync_recovered
            && self.sequence_install.is_none()
        {
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
        if !self.sequence_sync_recovery_active && self.sequence_transfer.is_some() {
            log::info!(
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
    }

    /// Once sequence sync is active, consensus work for the covered history is redundant
    /// with the install and harmful to the late node: it is the same replay storm that
    /// filled the core queue in the local late-joiner run. While the gap is large, accept
    /// only materialization and sequence-sync data. Near the tail, allow normal
    /// dissemination again but keep dropping view-scoped messages at or below a staged
    /// target the install will replace.
    fn install_replaces_inbound(&self, inbound: &Inbound) -> bool {
        if !self.sequence_install_enabled {
            return false;
        }

        if self.large_sequence_sync_target().is_some() {
            return !inbound.keep_during_large_sequence_sync();
        }

        // Inside the sync threshold the install RELEASES its claim on the range.
        //
        // Dropping view-scoped traffic at or below the staged target is only sound while
        // the install is actually going to deliver those views. Once the node is close
        // enough that ordinary participation should finish the job, the same rule
        // discards exactly the messages that would finish it -- and hands the range to a
        // mechanism that structurally cannot close a tail, since every transfer lands one
        // cycle behind a moving fleet. Measured with the claim held: ordinary
        // participation contributed 42 of 7,181 views (0.6%).
        //
        // A staged install still drains underneath this; if ordinary progress overtakes
        // it, `rebase` already handles the race (`RebaseOutcome::Overtaken`).
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
                    // AVAIL-ECHO-SPEC.md: a peer's positional availability claims, already
                    // resolved positionally by `on_echo`. Handled here because crediting
                    // needs `LaneManager`/`BlockCache` access the engine deliberately
                    // lacks: `note_claim` applies monotonicity and (for short claims) the
                    // linkage check, and whatever survives is credited through the SAME
                    // `credit_refs` the explicit-tuple path uses -- so the aggregator, the
                    // threshold marks and `Repairer::holders` all behave identically and
                    // this really is only a change of encoding.
                    Effect::AvailClaimed(sender, resolved) => {
                        let refs = self.lm.note_claim(sender, &resolved);
                        queue.extend(self.credit_refs(sender, refs, now));
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
                        // AVAIL-ECHO-SPEC.md (`Parameters::echo_avail_claims`): stamp this
                        // party's positional availability claims here, at the serialization
                        // boundary and never inside `AgbEngine` -- identical discipline to
                        // `set_wish` on the line above, and for the same reason: the engine
                        // is deliberately free of both watermark and availability state.
                        // Only `Single` carries claims; `Batch` is out of scope exactly as
                        // it is for `digest_statements` below.
                        if self.echo_avail_claims {
                            if let EchoOut::Single(inner) = &mut e {
                                inner.avail = Some(self.lm.build_avail_claim(&inner.proposal));
                            }
                        }
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
                        // A recovering node that only CONSUMES output is still a fault from
                        // the committee's point of view. Publishing is not the same as
                        // contributing: a block counts only once it is committed, so this
                        // counter -- against `vantage_blocks_published` -- is what says
                        // whether a late joiner is actually carrying its share again.
                        if let Some(metrics) = &self.metrics {
                            let mut own = 0u64;
                            let mut own_payload = 0u64;
                            for header in headers.iter() {
                                // Per-author attribution, so a caught-up peer can report how
                                // much of a recovering node's output actually got committed.
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
                                // Blocks alone understate contribution: a busy core loop
                                // accumulates more digests before `header_size` is reached,
                                // so a node under recovery load seals FEWER but LARGER
                                // headers at an unchanged client rate. Payload entries are
                                // the work actually carried.
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
            _rx_sequence,
            _rx_payload_ready,
            _tx_vantage,
            _tx_bulk,
            _tx_sequence,
            _ack_aggregator,
            _sequence_large_gap_drop,
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
        // The install only claims its range while recovery is active; below the sync
        // threshold it releases the claim so parked traffic can close the tail.
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
    }

    #[tokio::test]
    async fn recovered_node_does_not_resync_on_ordinary_jitter() {
        // The invariant: once caught up, a node rejoins ordinary consensus and STAYS there.
        // Before the latch, the sync gate was re-evaluated against every newly certified
        // boundary, so a recovered joiner ran transfers forever (measured 0.12-0.36/s
        // indefinitely) and never reached peer parity.
        let mut core = test_core(0, "sequence_sync_recovered_latch");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
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

        // A boot-time node is NOT recovered: latching on a level check here is what disabled
        // state sync from birth and left a real joiner stuck at view 1.
        core.refresh_sequence_large_gap_drop();
        assert!(
            !core.sequence_sync_recovered,
            "a node that has never recovered must be able to sync"
        );

        // Enter recovery: a certified boundary 500 views ahead of a local head of 0.
        certify(&mut core, 500, 0x11);
        core.refresh_sequence_large_gap_drop();
        assert!(
            core.sequence_sync_recovery_active,
            "a 500-view gap must engage recovery"
        );
        assert!(!core.sequence_sync_recovered);

        // Close the gap to 50, inside the sync gate, with nothing staged. Leaving recovery
        // is the EDGE that latches recovered.
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

        // Ordinary jitter: a boundary 150 views ahead. Above the sync gate (100) and so
        // would have restarted a transfer, but far below the re-arm gap (400).
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

        // A genuine outage: 500 views ahead, past the re-arm gap.
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
        // The 2026-08-10 zombie: the latch fired at the installed target (head 2252)
        // while every view shed during recovery (up to ~2440) had no live evidence and
        // could never seal ordinarily -- the cursor wedged one view past the latch,
        // forever. Recovery must stay active until the head crosses the live-intake
        // floor stamped when shedding released.
        let mut core = test_core(0, "sequence_sync_live_floor");
        core.sequence_sync_min_gap_views = 100;
        core.sequence_sync_shed_gap_views = 300;
        core.sequence_sync_rearm_gap_views = 800;
        let keys = crate::common::keys();

        // A certified boundary 500 ahead: recovery on, shedding on.
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

        // Entry tracked the fleet through the shed (wish is retained while shedding).
        core.frontier.enter(520);

        // Installs catch up to 450: the gap (50) is inside BOTH gates, so shedding
        // releases -- stamping the floor -- but recovery must NOT deactivate: the head
        // is still below the floor and the views in between have no live evidence.
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
        assert!(!core.sequence_sync_recovered, "latching here strands the cursor");

        // The head crosses the floor: NOW leaving recovery is sound, and it latches.
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
    async fn recovered_node_never_sheds() {
        // Shedding is only sound while installs will replace the shed range; the latch
        // is precisely "no more installs". A latched node that sheds is the zombie of
        // 2026-08-10: not syncing, not participating, dark until the run ended. Between
        // the shed gate and the re-arm gap it must PARTICIPATE; at the re-arm gap the
        // latch clears and shed+sync resume together.
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
        // Three regimes, and the middle one is the point of the split: between the two
        // thresholds the node BOTH participates and syncs. Driving shedding and mechanism
        // selection from a single number is what made a recovering node oscillate.
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

        // Gap 400: above both. Sync runs and ordinary traffic is shed.
        core.refresh_sequence_large_gap_drop();
        assert!(core.sequence_sync_recovery_active);
        assert!(core.sequence_large_gap_drop.load(Ordering::Relaxed));

        // Gap 250: between the thresholds. Still syncing, no longer deaf.
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

        // Gap 50: below the sync threshold. State sync stops -- the tail can only be
        // closed by ordinary participation, because a transfer always lands one cycle
        // behind a moving fleet. An in-flight transfer is dropped; a staged install is
        // deliberately left to drain.
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
            !core.sequence_sync_recovery_active,
            "state sync must stop inside the sync threshold"
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
        let mut install = SequenceInstall::new(
            0,
            1,
            target_head,
            vec![(1, outcome, vec![first.id.clone(), second.id.clone()])],
            Vec::new(),
            8,
            4096,
        );
        assert_eq!(install.admit(0).len(), 2);
        core.sequence_install = Some(install);

        core.drive_sequence_block_fetch(Instant::now()).await;
        assert_eq!(
            core.sequence_block_requests.len(),
            2,
            "both missing delta digests are batched without waiting for parent walks"
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
            avail: None,
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
        let (tx_sequence, _rx_sequence) = channel(4);
        let handler = VantageReceiverHandler {
            tx: tx_vantage,
            tx_bulk,
            tx_sequence,
            sequence_large_gap_drop: Arc::new(AtomicBool::new(false)),
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
