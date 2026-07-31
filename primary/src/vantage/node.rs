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
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::payload::PayloadIo;
use crate::vantage::repair::Repairer;
use crate::vantage::resolve::Resolver;
use crate::vantage::resume::{ResumeServe, ResumeTrigger};
use crate::vantage::wire::{self, Wire};
use crate::vantage::Effect;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PairwiseKeys, PublicKey};
use metrics::{Metrics, UtilizationTimer};
use network::{BatchConfig, MessageHandler, ReliableSender, SimpleSender, Writer};
use prometheus::IntCounter;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
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
}

/// SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels
/// (`Parameters::authenticate_channels`) -- the candidate sender a MAC tag must verify
/// against, for every `PrimaryMessage` variant this handler ever routes. Mirrors
/// `DeclaredSender::declared_sender`'s classification exactly (declared wire sender
/// field where one exists), PLUS the two D4-class positionally-attributed variants
/// (`VantagePropose`/`ControlInit`), which carry no wire sender field at all but ARE
/// still cryptographically bindable here: `agb::proposer`/`control::control_leader`
/// are pure functions of `committee` alone, so this network-layer boundary can derive
/// the identical trusted sender `VantageCore::dispatch_inbound` derives, without
/// needing a live `VantageCore` (or its mutable AGB/control-log state) to ask.
/// `None` for `Header(_, true)` ("Serve") and `ControlServe`: neither carries any
/// sender claim (wire or positional) to bind -- the same pre-existing D4 gap
/// `declared_sender` already carves out (content is self-authenticating by digest /
/// gated downstream by `pending_fetch`, respectively). A MAC tag is still present on the wire
/// for these two (uniform framing -- every message carries exactly one trailing tag
/// when the flag is on), it is simply never checked, since there is nothing to check
/// it against.
fn mac_candidate_sender(message: &PrimaryMessage, committee: &Committee) -> Option<PublicKey> {
    match message {
        PrimaryMessage::Header(h, false) => Some(h.author),
        PrimaryMessage::Header(_, true) => None,
        PrimaryMessage::HeadersRequest(_, requestor) => Some(*requestor),
        PrimaryMessage::VantageAck(a) => Some(a.sender),
        PrimaryMessage::VantageAvail(_, sender) => Some(*sender),
        PrimaryMessage::VantagePropose(p) => Some(crate::vantage::agb::proposer(committee, p.view)),
        PrimaryMessage::VantageEcho(e) => Some(e.sender),
        PrimaryMessage::VantageEchoSkip(_, s, _) => Some(*s),
        PrimaryMessage::VantageReady(r) => Some(r.sender),
        PrimaryMessage::VantageNoReady(_, s, _) => Some(*s),
        PrimaryMessage::VantageWish(_, s) => Some(*s),
        PrimaryMessage::CompReport(_, _, s) => Some(*s),
        PrimaryMessage::ControlInit(p, _) => {
            Some(crate::vantage::control::control_leader(committee, p.round))
        }
        PrimaryMessage::ControlEcho(_, s) => Some(*s),
        PrimaryMessage::ControlReady(_, s) => Some(*s),
        PrimaryMessage::ControlCommit(_, s) => Some(*s),
        PrimaryMessage::ControlTimeoutVote(_, s) => Some(*s),
        PrimaryMessage::ControlTimeoutAccept(_, s) => Some(*s),
        PrimaryMessage::ControlFetch(_, _, s) => Some(*s),
        PrimaryMessage::ControlServe(_, _) => None,
        // PHASE7: the vector-`M` counterparts, same D4/declared-sender
        // classification as their singular siblings above.
        PrimaryMessage::VantageProposeBatch(p) => {
            Some(crate::vantage::agb::proposer(committee, p.view))
        }
        PrimaryMessage::VantageEchoBatch(e) => Some(e.sender),
        PrimaryMessage::VantageReadyBatch(r) => Some(r.sender),
        PrimaryMessage::ControlInitBatch(p, _) => {
            Some(crate::vantage::control::control_leader(committee, p.round))
        }
        PrimaryMessage::ControlServeBatch(_, _) => None,
        // A real, per-destination-verifiable sender field, same class as
        // `VantageEchoSkip`/`VantageNoReady` above.
        PrimaryMessage::VantageSkipVote(_, s) => Some(*s),
        // signature-free.tex §8.3 "Digest-named AGB statements": `EchoDigest`/
        // `ReadyDigest` carry a real declared sender field directly, same class as
        // `VantageEcho`/`VantageReady` above. `VantageBodyFetch` carries a real
        // declared requester, same class as `ControlFetch`. `VantageBodyServe`
        // carries no sender claim at all (content is verified by hash against an
        // outstanding `pending_fetch` entry, same D4 class as `ControlServe`).
        PrimaryMessage::VantageEchoDigest(d) => Some(d.sender),
        PrimaryMessage::VantageReadyDigest(d) => Some(d.sender),
        PrimaryMessage::VantageBodyFetch(_, _, s) => Some(*s),
        PrimaryMessage::VantageBodyServe(_, _) => None,
        // Mechanism A (`vantage::resume`): a real, per-destination-verifiable
        // declared requester, same class as `VantageBodyFetch`/`ControlFetch`
        // above -- the message asserts "I, requester, am asking author to resume its
        // lane", requester's own claim, MAC-bound to requester regardless of who the
        // (separately, protocol-level-checked) named lane author is.
        PrimaryMessage::VantageLaneResume(_, _, requester) => Some(*requester),
        // Autobahn-only variants never legitimately reach the Vantage assembly's port
        // (`dispatch`'s own catch-all ignores them below); no candidate needed.
        _ => None,
    }
}

/// SECURITY (Fable audit): the OUTBOUND-side mirror of `mac_candidate_sender`'s `None`
/// arms -- true for the three D4-class variants with no sender claim at all (wire or
/// positional) to bind: `Header(_, true)`/`ControlServe` (Vantage's own carrier-body
/// repair) and `SimpleItCutServe` (Simple-IT's sibling cut-proposal repair -- see
/// `simpleit::node::mac_candidate_sender`'s identical `None` arm for that variant:
/// its `CutProposal::proposer` field names the ORIGINAL round leader, never the
/// relaying/serving party, so there is no sender claim here to bind either). This
/// node always sends its OWN messages, so on the outbound side the candidate (when
/// one exists) is trivially `self.name` -- only whether a candidate exists at all
/// matters here, not its value, hence the plain `bool`.
pub(super) fn message_needs_placeholder_tag(message: &PrimaryMessage) -> bool {
    matches!(
        message,
        PrimaryMessage::Header(_, true)
            | PrimaryMessage::ControlServe(_, _)
            | PrimaryMessage::ControlServeBatch(_, _)
            | PrimaryMessage::SimpleItCutServe(_)
            // signature-free.tex §8.3: `VantageBodyServe` is D4-class, same "no
            // sender claim to bind" reasoning as `ControlServe`/`SimpleItCutServe`
            // above -- content is verified by hash against the requester's own
            // outstanding `pending_fetch` entry, not by sender.
            | PrimaryMessage::VantageBodyServe(_, _)
    )
}

/// Network receiver handler for the Vantage assembly's `primary_to_primary` port.
/// Deliberately a distinct type from Autobahn's `PrimaryReceiverHandler` (which stays
/// byte-identical, untouched) -- the two assemblies never share a handler.
#[derive(Clone)]
pub struct VantageReceiverHandler {
    pub tx: Sender<Inbound>,
    pub ack_aggregator: SharedAckAggregator,
    /// METRICS-DASHBOARD-SPEC.md §1: `None` only in tests that construct this handler
    /// directly without wiring metrics (matches `VantageCore`'s own optional-handle
    /// convention) -- production (`Primary::spawn`) always passes `Some`.
    pub metrics: Option<Arc<Metrics>>,
    /// SECURITY (Fable audit): `Some` iff `Parameters::authenticate_channels` is on --
    /// see `mac_candidate_sender`'s doc comment for the verification model. `None`
    /// (the default) is byte-identical to pre-MAC behavior: every received frame is
    /// deserialized and routed exactly as received, no trailing bytes stripped.
    pub channel_auth: Option<Arc<PairwiseKeys>>,
    /// Needed only to derive `VantagePropose`/`ControlInit`'s positionally-attributed
    /// sender (see `mac_candidate_sender`) -- a small, immutable, once-built clone
    /// (mirrors `VantageCore`'s own `committee.clone()` at `build` time). Unused when
    /// `channel_auth` is `None`.
    pub committee: Committee,
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

        // SECURITY (Fable audit): when authenticated channels are on, the trailing
        // `crypto::mac::TAG_LEN` bytes of every frame are the MAC tag -- strip them
        // BEFORE deserializing (a frame too short to even carry a tag is malformed/
        // adversarial; drop it, same "drop, don't propagate/tear down" treatment as a
        // deserialize failure below). `None` (flag off) is byte-identical to before:
        // `payload` is `serialized` verbatim, no tag ever stripped or checked.
        let (payload, tag): (&[u8], Option<[u8; crypto::mac::TAG_LEN]>) = match &self.channel_auth {
            Some(_) => match crypto::mac::split_tag(&serialized) {
                Some((payload, tag)) => (payload, Some(tag)),
                None => return Ok(()),
            },
            None => (&serialized[..], None),
        };

        let message: PrimaryMessage = bincode::deserialize(payload)?;

        // SECURITY (Fable audit): verify the MAC tag against the message's own
        // declared/positionally-derived sender BEFORE this message is ever forwarded
        // to `VantageCore` -- a mismatch is dropped here, never reaching the
        // membership gate or any protocol logic. A `None` candidate (the D4-class
        // `Header(_, true)`/`ControlServe` carve-out, see `mac_candidate_sender`) has
        // no claim to check, so it passes through exactly as it did before this flag
        // existed.
        if let (Some(auth), Some(tag)) = (&self.channel_auth, tag) {
            if let Some(sender) = mac_candidate_sender(&message, &self.committee) {
                if !auth.verify(&sender, payload, &tag) {
                    if let Some(metrics) = &self.metrics {
                        metrics.authenticated_channel_rejected_total.inc();
                    }
                    return Ok(());
                }
            }
        }

        if let Some(metrics) = &self.metrics {
            crate::primary::record_typed_received(metrics, message.type_name(), payload.len());
        }
        let inbound = match message {
            PrimaryMessage::Header(h, false) => Inbound::Publish(h.author, h),
            PrimaryMessage::Header(h, true) => Inbound::Serve(h),
            PrimaryMessage::HeadersRequest(digests, requestor) => {
                Inbound::HeadersRequest(digests, requestor)
            }
            PrimaryMessage::VantageAck(a) => {
                let result = {
                    let mut aggregator = self.ack_aggregator.lock().unwrap();
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
            // Autobahn-only variants never reach the Vantage assembly's port; ignore
            // rather than panic (defense in depth against a misrouted message).
            _ => return Ok(()),
        };
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
}

/// `VantageCore::build`'s return shape: the constructed core, channel ends `spawn`
/// still needs to wire up (or a test needs to drive directly), and the shared
/// ACK accumulator used by both local ACK feedback and the network handler.
type BuildOutput = (
    VantageCore,
    Receiver<Inbound>,
    Receiver<(Digest, Digest, WorkerId)>,
    Sender<Inbound>,
    SharedAckAggregator,
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
    ) -> (Sender<Inbound>, SharedAckAggregator) {
        let (core, rx_vantage, rx_payload_ready, tx_vantage, ack_aggregator) =
            Self::build(name, committee, parameters, store, metrics, tx_output);
        tokio::spawn(core.run(rx_vantage, rx_our_digests, rx_payload_ready));
        (tx_vantage, ack_aggregator)
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
        let (tx_payload_ready, rx_payload_ready) = channel(CHANNEL_CAPACITY);

        // SECURITY (Fable audit): captured before `committee` is consumed below building
        // the sub-engines -- the single source of truth `dispatch_inbound` checks every
        // wire-declared sender against.
        let members: HashSet<PublicKey> = committee.authorities.keys().cloned().collect();

        // SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels.
        // `parameters.authenticate_channels` on with no `mac_secret` set is a
        // misconfiguration (it would otherwise silently run unauthenticated) --
        // panic loudly rather than let it pass, the same "fail fast on
        // misconfiguration" posture already used for `expect`s elsewhere in this
        // constructor (e.g. "Our public key is not in the committee").
        let channel_auth: Option<Arc<PairwiseKeys>> = if parameters.authenticate_channels {
            let secret = parameters
                .mac_secret
                .expect("authenticate_channels is set but mac_secret is None (misconfiguration)");
            Some(Arc::new(committee.pairwise_keys(&name, &secret)))
        } else {
            None
        };

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
                });

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

        // Transient network-level "blip" fault injector (`--blip-at`): resolved once,
        // same convention as `latency_map` just above -- `None` (the default, and
        // always the case when `--blip-at` isn't given, or `Parameters::blip_window`
        // was never armed -- e.g. the distributed `node run` path) means both senders
        // below never even check the blip window.
        let blip_gate = parameters.blip_window.clone().and_then(|window| {
            config::blip_targets(&committee, &name, parameters.blip_node_index)
                .map(|targets| Arc::new(network::BlipGate::new(targets, window)))
        });

        // Transport-level batching, resolved once (mirrors `latency_map`/
        // `compress_network`'s own resolve-once-at-spawn convention).
        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        // Mechanism A (sender-side lane resume, `vantage::resume`): this node's own
        // dedicated off-run-loop sender -- see `wire::spawn_resume_sender`'s doc
        // comment for the full design/rationale (fixes the diagnosed loop-starvation
        // defect: Mechanism A's own network sends used to run inline, synchronously,
        // on THIS run loop). Built from the SAME `latency_map`/`blip_gate`/`batch`/
        // `core_metrics` locals as `network`/`worker_network` just below (identical
        // configuration convention) -- cloned here (rather than moved) since both of
        // those still need their own copies afterward.
        let resume_tx = wire::spawn_resume_sender(
            latency_map.clone(),
            blip_gate.clone(),
            parameters.compress_network,
            batch,
            core_metrics.clone(),
            channel_auth.clone(),
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
                name,
                network: {
                    let mut s = ReliableSender::new()
                        .with_latency(latency_map.clone())
                        .with_blip(blip_gate.clone())
                        .with_compression(parameters.compress_network)
                        .with_batching(batch);
                    if let Some(m) = &core_metrics {
                        s = s.with_metrics(m.clone());
                    }
                    s
                },
                worker_network: {
                    let mut s = SimpleSender::new()
                        .with_latency(latency_map)
                        .with_blip(blip_gate)
                        .with_compression(parameters.compress_network)
                        .with_batching(batch);
                    if let Some(m) = &core_metrics {
                        s = s.with_metrics(m.clone());
                    }
                    s
                },
                resume_tx,
                channel_auth,
                cancel_handlers: Vec::new(),
                last_prune_len: 0,
                other_primaries,
                other_primary_addrs,
                worker_addresses,
                withheld_header_dests,
                withhold_window: parameters.withhold_window.clone(),
                metrics: core_metrics.clone(),
            },
            header_size: parameters.header_size,
            max_header_delay: parameters.max_header_delay,
            digests: Vec::new(),
            payload_size: 0,
            ack_watermarks: parameters.ack_watermarks,
            ack_watermark_period_ms: parameters.ack_watermark_period_ms,
            digest_statements: parameters.digest_statements,
            resume_trigger: ResumeTrigger::new(),
            resume_serve: ResumeServe::new(),
            resume_check_period_ms: parameters.resume_check_period_ms,
            resume_backoff_ms: parameters.resume_backoff_ms,
            resume_batch: parameters.resume_batch,
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
        };
        (
            core,
            rx_vantage,
            rx_payload_ready,
            tx_vantage,
            ack_aggregator,
        )
    }

    async fn run(
        mut self,
        mut rx_vantage: Receiver<Inbound>,
        mut rx_our_digests: Receiver<(Digest, WorkerId)>,
        mut rx_payload_ready: Receiver<(Digest, Digest, WorkerId)>,
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
        let mut resume_tick = tokio::time::interval(Duration::from_millis(self.resume_check_period_ms));
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

            tokio::select! {
                biased;

                Some(inbound) = rx_vantage.recv() => {
                    let now = Instant::now();
                    let dispatch_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_inbound_dispatch, "inbound_dispatch");
                    let effects = self.dispatch_inbound(inbound, now).await;
                    drop(dispatch_timer);
                    self.execute(effects, now).await;
                }

                Some((header_digest, digest, worker_id)) = rx_payload_ready.recv() => {
                    let payload_sync_timer = Self::cached_utilization_timer(&self.metrics, &mut self.ut_payload_sync, "payload_sync");
                    self.on_payload_ready(header_digest, digest, worker_id).await;
                    drop(payload_sync_timer);
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
                _ = resume_tick.tick() => {
                    let now = Instant::now();
                    // Cloned out (a plain `Vec<PublicKey>`, already excluding
                    // `self.name` -- `Wire::other_primaries` is "OTHER primaries",
                    // precomputed once and fixed for this node's lifetime, see that
                    // field's own doc comment) so the loop body below is free to
                    // borrow `self` one call at a time across `.await` points,
                    // rather than holding a live borrow of `self` for the whole loop.
                    let authors: Vec<PublicKey> =
                        self.wire.other_primaries.iter().map(|(pk, _)| *pk).collect();
                    for author in authors {
                        self.try_resume_request(author, now);
                    }
                }

                _ = metrics_tick.tick() => {
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
                    self.execute(retry_effects, retry_now).await;
                    self.sample_metrics();
                    // METRICS-DASHBOARD-SPEC.md §3: `core_queue_length` -- `rx_vantage`'s
                    // current depth (cheap, `Receiver::len()` is O(1)); `0` (never set)
                    // on the two Autobahn paths, which never construct a `VantageCore`.
                    if let Some(metrics) = &self.metrics {
                        metrics.core_queue_length.set(rx_vantage.len() as i64);
                    }
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
        let payload = self.digests.drain(..).collect();
        self.payload_size = 0;
        let (_, effects) = self.lm.publish_own(payload).await;
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
            effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
            self.execute(effects, now).await;
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
        if let Some(from) = self
            .resume_trigger
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
        effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
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
                        self.agb.on_propose(self.name, p, now, &mut self.lm, &mut self.rep)
                    }
                    ProposalOut::Batch(p) => self.agb.on_propose_batch(
                        self.name,
                        p,
                        now,
                        &mut self.lm,
                        &mut self.rep,
                    ),
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
            effects.extend(self.agb.activate(v, now, &mut self.lm, &mut self.rep));
        }
        effects.extend(self.try_propose_effects(now));
        effects
    }

    fn on_ack_availability(&mut self, availability: AckAvailability, now: Instant) -> Vec<Effect> {
        let mut effects = self.lm.process_ack_availability(availability);
        effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
        effects
    }

    fn record_local_ack(&mut self, ack: &Ack, now: Instant) -> Vec<Effect> {
        let availability = {
            let mut aggregator = self.ack_aggregator.lock().unwrap();
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
            let mut aggregator = self.ack_aggregator.lock().unwrap();
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
                let mut aggregator = self.ack_aggregator.lock().unwrap();
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
            Inbound::Serve(header) => self.rep.on_serve(header),
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
                        self.agb.on_propose(claimed_sender, p, now, &mut self.lm, &mut self.rep)
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
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
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
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::Ready(ready) => {
                let mut effects = self.pacemaker.on_wish(ready.sender(), ready.wish());
                effects.extend(match ready {
                    ReadyOut::Single(r) => self.agb.on_ready(r, &mut self.rep),
                    ReadyOut::Batch(r) => self.agb.on_ready_batch(r, &mut self.rep),
                });
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::NoReady(view, sender, wish) => {
                let mut effects = self.pacemaker.on_wish(sender, wish);
                effects.extend(self.agb.on_noready(view, sender));
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
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
                effects.extend(
                    self.digest_stmts
                        .on_echo_digest(msg, now, &mut self.agb, &mut self.rep),
                );
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::ReadyDigest(msg) => {
                let mut effects = self.pacemaker.on_wish(msg.sender, msg.wish);
                effects.extend(
                    self.digest_stmts
                        .on_ready_digest(msg, now, &mut self.agb, &mut self.rep),
                );
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects.extend(self.try_propose_effects(now));
                effects
            }
            Inbound::BodyFetch(view, digest, requester) => {
                self.digest_stmts
                    .on_body_fetch(requester, view, digest, &self.agb)
            }
            Inbound::BodyServe(view, proposal) => {
                let mut effects =
                    self.digest_stmts
                        .on_body_serve(view, proposal, &mut self.agb, &mut self.rep);
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
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
                if !self.resume_serve.should_serve(requester, from, now, backoff) {
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
        }
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
                        self.wire
                            .broadcast_message(PrimaryMessage::VantageAck(ack))
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
                    queue.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                    queue.extend(self.cursor.retry());
                }
                // PHASE7: `Single` rides the pre-PHASE7 `VantagePropose` message
                // (0/1-entry `M`); `Batch` (`decide_prefix` produced `>= 2` entries)
                // rides the separate `VantageProposeBatch` message.
                Effect::BroadcastPropose(p) => match p {
                    ProposalOut::Single(p) => {
                        self.wire
                            .broadcast_message(PrimaryMessage::VantagePropose(p))
                            .await
                    }
                    ProposalOut::Batch(p) => {
                        self.wire
                            .broadcast_message(PrimaryMessage::VantageProposeBatch(p))
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
                            self.wire
                                .broadcast_message(PrimaryMessage::VantageEchoDigest(msg))
                                .await
                        }
                        EchoOut::Single(e) => {
                            self.wire
                                .broadcast_message(PrimaryMessage::VantageEcho(e))
                                .await
                        }
                        EchoOut::Batch(e) => {
                            self.wire
                                .broadcast_message(PrimaryMessage::VantageEchoBatch(e))
                                .await
                        }
                    }
                }
                Effect::BroadcastEchoSkip(view) => {
                    let wish = self.pacemaker.own_watermark();
                    self.wire
                        .broadcast_message(PrimaryMessage::VantageEchoSkip(view, self.name, wish))
                        .await;
                }
                Effect::BroadcastReady(mut r) => {
                    r.set_wish(self.pacemaker.own_watermark());
                    match r {
                        // signature-free.tex §8.3: mirrors `Effect::BroadcastEcho`'s
                        // identical translation immediately above.
                        ReadyOut::Single(r) if self.digest_statements => {
                            let msg = r.to_digest(self.agb.sid());
                            self.wire
                                .broadcast_message(PrimaryMessage::VantageReadyDigest(msg))
                                .await
                        }
                        ReadyOut::Single(r) => {
                            self.wire
                                .broadcast_message(PrimaryMessage::VantageReady(r))
                                .await
                        }
                        ReadyOut::Batch(r) => {
                            self.wire
                                .broadcast_message(PrimaryMessage::VantageReadyBatch(r))
                                .await
                        }
                    }
                }
                Effect::BroadcastNoReady(view) => {
                    let wish = self.pacemaker.own_watermark();
                    self.wire
                        .broadcast_message(PrimaryMessage::VantageNoReady(view, self.name, wish))
                        .await;
                }
                // No wish piggyback, unlike `BroadcastEchoSkip`/`BroadcastNoReady`
                // above (see `Inbound::SkipVote`'s doc comment).
                Effect::BroadcastSkipVote(view) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::VantageSkipVote(view, self.name))
                        .await;
                }
                Effect::Fixed(view, well_formed) => {
                    let activated = self.frontier.record_fixed(view, well_formed);
                    for v in activated {
                        queue.extend(self.agb.activate(v, now, &mut self.lm, &mut self.rep));
                    }
                    queue.extend(self.try_propose_effects(now));
                    // signature-free.tex §8.3: a proposal just fixed BY VALUE may
                    // already match digest statements buffered before it arrived --
                    // drain them now. A no-op if nothing was ever buffered for this
                    // view, or if `well_formed` is false (`on_local_fixed`'s own
                    // `fixed_proposal` query returns `None` for `Reject`).
                    queue.extend(
                        self.digest_stmts
                            .on_local_fixed(view, &mut self.agb, &mut self.rep),
                    );
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
                    self.wire
                        .broadcast_message(PrimaryMessage::VantageWish(view, self.name))
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
                Effect::BroadcastCompReport(view, digest) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::CompReport(view, digest, self.name))
                        .await;
                }
                // PHASE7: `Single`/`None` are byte-identical to the pre-PHASE7 path
                // (same `ControlInit` message); `Batch` rides the separate,
                // flag-gated `ControlInitBatch` message.
                Effect::BroadcastControlInit(proposal, b_w) => match b_w {
                    None => {
                        self.wire
                            .broadcast_message(PrimaryMessage::ControlInit(proposal, None))
                            .await
                    }
                    Some(ProposalOut::Single(p)) => {
                        self.wire
                            .broadcast_message(PrimaryMessage::ControlInit(proposal, Some(p)))
                            .await
                    }
                    Some(ProposalOut::Batch(p)) => {
                        self.wire
                            .broadcast_message(PrimaryMessage::ControlInitBatch(
                                proposal,
                                Some(p),
                            ))
                            .await
                    }
                },
                Effect::BroadcastControlEcho(proposal) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::ControlEcho(proposal, self.name))
                        .await;
                }
                Effect::BroadcastControlReady(proposal) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::ControlReady(proposal, self.name))
                        .await;
                }
                Effect::BroadcastControlCommit(round) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::ControlCommit(round, self.name))
                        .await;
                }
                Effect::BroadcastControlTimeoutVote(round) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::ControlTimeoutVote(round, self.name))
                        .await;
                }
                Effect::BroadcastControlTimeoutAccept(round) => {
                    self.wire
                        .broadcast_message(PrimaryMessage::ControlTimeoutAccept(round, self.name))
                        .await;
                }
                Effect::ControlFetchTo(peer, view, digest) => {
                    self.wire
                        .send_message(peer, PrimaryMessage::ControlFetch(view, digest, self.name))
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
    use crypto::generate_keypair;
    use rand::rngs::StdRng;
    use rand::SeedableRng as _;
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
        let (core, _rx_vantage, _rx_payload_ready, _tx_vantage, _ack_aggregator) =
            VantageCore::build(
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

    // --- SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels ---

    /// `mac_candidate_sender`'s pure classification, exercised directly (no network):
    /// declared-sender variants return that field; the two positionally-attributed
    /// D4-class variants (`VantagePropose`/`ControlInit`) return the committee's own
    /// `agb::proposer`/`control::control_leader` derivation; the two no-claim variants
    /// (`Header(_, true)`/`ControlServe`) return `None`.
    #[test]
    fn mac_candidate_sender_classification() {
        let committee = crate::common::committee();
        let (author, _) = crate::common::keys()[0];
        let (sender, _) = crate::common::keys()[1];

        let header = Header {
            author,
            ..Header::default()
        };
        assert_eq!(
            mac_candidate_sender(&PrimaryMessage::Header(header.clone(), false), &committee),
            Some(author)
        );
        assert_eq!(
            mac_candidate_sender(&PrimaryMessage::Header(header, true), &committee),
            None
        );

        let ack = Ack::new(author, 1, Digest::default(), sender);
        assert_eq!(
            mac_candidate_sender(&PrimaryMessage::VantageAck(ack), &committee),
            Some(sender)
        );

        let proposal = ViewProposal {
            view: 3,
            c: Vec::new(),
            t: Vec::new(),
            m: None,
        };
        assert_eq!(
            mac_candidate_sender(&PrimaryMessage::VantagePropose(proposal), &committee),
            Some(crate::vantage::agb::proposer(&committee, 3))
        );

        let control_proposal = ControlProposal {
            round: 2,
            parent: 0,
            value: None,
        };
        assert_eq!(
            mac_candidate_sender(
                &PrimaryMessage::ControlInit(control_proposal.clone(), None),
                &committee
            ),
            Some(crate::vantage::control::control_leader(&committee, 2))
        );
        assert_eq!(
            mac_candidate_sender(
                &PrimaryMessage::ControlServe(3, dummy_proposal()),
                &committee
            ),
            None
        );

        // Mechanism A (`vantage::resume`): MAC-bound to the declared REQUESTER,
        // regardless of the (separately, protocol-level-checked) named lane author.
        assert_eq!(
            mac_candidate_sender(
                &PrimaryMessage::VantageLaneResume(author, 5, sender),
                &committee
            ),
            Some(sender)
        );
    }

    #[test]
    fn message_needs_placeholder_tag_classification() {
        let header = Header::default();
        assert!(message_needs_placeholder_tag(&PrimaryMessage::Header(
            header.clone(),
            true
        )));
        assert!(!message_needs_placeholder_tag(&PrimaryMessage::Header(
            header, false
        )));
        assert!(message_needs_placeholder_tag(
            &PrimaryMessage::ControlServe(1, dummy_proposal())
        ));
        assert!(!message_needs_placeholder_tag(
            &PrimaryMessage::VantageWish(1, crate::common::keys()[0].0)
        ));
        // Simple-IT's sibling repair-serve: same "no sender claim to bind" class as
        // `ControlServe` above -- see this function's own doc comment.
        assert!(message_needs_placeholder_tag(
            &PrimaryMessage::SimpleItCutServe(crate::simpleit::CutProposal::default())
        ));
        // Its fetch counterpart, by contrast, carries a real declared requester (see
        // `simpleit::node::mac_candidate_sender`'s `SimpleItCutFetch` arm) -- not in
        // this no-claim class.
        assert!(!message_needs_placeholder_tag(
            &PrimaryMessage::SimpleItCutFetch(1, Digest::default(), crate::common::keys()[0].0)
        ));
        // signature-free.tex §8.3 "Digest-named AGB statements": `VantageBodyServe`
        // is D4-class too, same "no sender claim to bind" reasoning as
        // `ControlServe`/`SimpleItCutServe` above; its fetch counterpart
        // `VantageBodyFetch` carries a real declared requester, same class as
        // `ControlFetch`/`SimpleItCutFetch` above -- not in this no-claim class.
        assert!(message_needs_placeholder_tag(&PrimaryMessage::VantageBodyServe(
            1,
            dummy_proposal()
        )));
        assert!(!message_needs_placeholder_tag(
            &PrimaryMessage::VantageBodyFetch(1, Digest::default(), crate::common::keys()[0].0)
        ));
        // Mechanism A: carries a real declared requester (see `mac_candidate_sender`'s
        // own arm for this variant) -- not in the no-claim class.
        assert!(!message_needs_placeholder_tag(
            &PrimaryMessage::VantageLaneResume(
                crate::common::keys()[0].0,
                5,
                crate::common::keys()[1].0
            )
        ));
    }

    /// End-to-end over a real TCP loopback (mirrors `network::receiver_tests::receive`):
    /// flag off (`channel_auth: None`) is byte-identical to pre-MAC behavior -- a plain,
    /// untagged ACK still deserializes and reaches `VantageCore` once it advances
    /// aggregate availability.
    #[tokio::test]
    async fn dispatch_flag_off_delivers_untagged_ack_threshold() {
        use futures::SinkExt as _;
        use tokio_util::codec::{Framed, LengthDelimitedCodec};

        let committee = crate::common::committee();
        let ack_aggregator = Arc::new(Mutex::new(AckAggregator::new(committee.clone())));
        let (sender, _) = crate::common::keys()[1];
        let (pre_sender, _) = crate::common::keys()[2];
        let reference = (sender, 7, Digest::default());
        ack_aggregator
            .lock()
            .unwrap()
            .record_ack(pre_sender, reference.clone());
        let (tx_vantage, mut rx_vantage) = channel(4);
        let handler = VantageReceiverHandler {
            tx: tx_vantage,
            ack_aggregator,
            metrics: None,
            channel_auth: None,
            committee,
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
            "flag off must deliver a plain untagged ACK once it advances availability"
        );
    }

    /// The core impersonation-closure property: a message tagged with the WRONG
    /// sender's key (i.e. someone claiming to be `sender` who doesn't hold `k_{sender,
    /// self}`) is dropped, never reaching `VantageCore`; the SAME message, correctly
    /// tagged by the real `sender`, IS delivered.
    #[tokio::test]
    async fn dispatch_authenticated_rejects_forged_sender_accepts_genuine() {
        use futures::SinkExt as _;
        use tokio_util::codec::{Framed, LengthDelimitedCodec};

        let committee = crate::common::committee();
        let (name, _) = crate::common::keys()[0]; // this node ("self" / the receiver)
        let (sender, _) = crate::common::keys()[1]; // the genuine, honest sender
        let (impostor, _) = crate::common::keys()[2]; // a distinct committee member with no claim to `sender`'s key
        let secret = crypto::MacSecret::generate();

        let receiver_auth = Arc::new(committee.pairwise_keys(&name, &secret));
        let sender_auth = committee.pairwise_keys(&sender, &secret);
        let impostor_auth = committee.pairwise_keys(&impostor, &secret);

        let ack_aggregator = Arc::new(Mutex::new(AckAggregator::new(committee.clone())));
        let (pre_sender, _) = crate::common::keys()[3];
        let reference = (sender, 9, Digest::default());
        ack_aggregator
            .lock()
            .unwrap()
            .record_ack(pre_sender, reference.clone());
        let (tx_vantage, mut rx_vantage) = channel(4);
        let handler = VantageReceiverHandler {
            tx: tx_vantage,
            ack_aggregator,
            metrics: None,
            channel_auth: Some(receiver_auth),
            committee: committee.clone(),
        };

        let address: SocketAddr = "127.0.0.1:14511".parse().unwrap();
        network::Receiver::spawn(address, handler);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A wire `Ack` whose DECLARED sender field is `sender` -- but physically
        // produced (MAC'd) by `impostor`, who does not hold `k_{sender,name}`.
        let ack = Ack::new(reference.0, reference.1, reference.2.clone(), sender);
        let payload = bincode::serialize(&PrimaryMessage::VantageAck(ack.clone())).unwrap();
        let forged_tag = impostor_auth
            .tag_for(&name, &payload)
            .expect("impostor is a committee member");
        let mut forged = payload.clone();
        forged.extend_from_slice(&forged_tag);

        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        transport.send(Bytes::from(forged)).await.unwrap();

        let rejected = tokio::time::timeout(Duration::from_millis(300), rx_vantage.recv()).await;
        assert!(rejected.is_err(), "a forged sender claim (wrong holder of the claimed sender's key) must be dropped, not delivered");

        // The identical logical message, genuinely tagged by `sender` itself.
        let genuine_tag = sender_auth
            .tag_for(&name, &payload)
            .expect("sender is a committee member");
        let mut genuine = payload;
        genuine.extend_from_slice(&genuine_tag);
        transport.send(Bytes::from(genuine)).await.unwrap();

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx_vantage.recv()).await;
        assert!(
            matches!(delivered, Ok(Some(Inbound::AckAvailability(_)))),
            "a genuinely MAC'd ACK from the real declared sender must be delivered when it advances availability"
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
            let mut agg = core.ack_aggregator.lock().unwrap();
            agg.record_ack(author, reference.clone())
        };
        assert!(first_ack.availability.is_none());
        let second_ack = {
            let mut agg = core.ack_aggregator.lock().unwrap();
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
            core.metrics.as_ref().unwrap().vantage_lane_resume_requests_sent.get(),
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
            core.metrics.as_ref().unwrap().vantage_lane_resume_requests_sent.get(),
            sent_after_establish + 1,
            "receipt of the publish must fire the NEXT request immediately -- no \
             third tick was ever run in this test"
        );
    }
}
