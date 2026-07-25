// PHASE4-SPEC.md §1 -- the single spawned `VantageCore` task: owns `LaneManager` +
// `Repairer` + `AgbEngine` + `Frontier` + `Cursor` and executes their returned
// `Effect`s. One owning loop avoids shared locks entirely (the components are
// synchronous/effect-returning state machines); the shared `BlockCache` mutex stays as
// the one piece of genuinely shared state (§3.3's cross-notification hook).

use crate::messages::{Ack, Header};
use crate::primary::{PrimaryMessage, PrimaryWorkerMessage, View, CHANNEL_CAPACITY};
use crate::vantage::agb::{AgbEngine, Echo, Ready, TimerKind, ViewProposal};
use crate::vantage::block;
use crate::vantage::control::{ControlLog, ControlProposal, Round};
use crate::vantage::cursor::Cursor;
use crate::vantage::frontier::Frontier;
use crate::vantage::lanes::{
    AckAggregator, AckAvailability, BlockCache, LaneManager, SharedAckAggregator, SharedBlocks,
};
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::repair::Repairer;
use crate::vantage::resolve::Resolver;
use crate::vantage::Effect;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PairwiseKeys, PublicKey};
use metrics::{Metrics, UtilizationTimer};
use network::{BatchConfig, CancelHandler, MessageHandler, ReliableSender, SimpleSender, Writer};
use prometheus::IntCounter;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::error::TryRecvError;

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
    /// `VantagePropose` carries no sender field on the wire (§2) -- see
    /// `VantageCore::dispatch_inbound` for how the trusted sender is derived.
    Propose(ViewProposal),
    Echo(Echo),
    /// PHASE5-SPEC.md §2: trailing field is the piggybacked wish watermark (D5-2).
    EchoSkip(View, PublicKey, View),
    Ready(Ready),
    /// PHASE5-SPEC.md §2: trailing field is the piggybacked wish watermark (D5-2).
    NoReady(View, PublicKey, View),
    /// PHASE5-SPEC.md §2: a standalone `VantageWish` (W2 amplification).
    Wish(View, PublicKey),
    /// PHASE6-SPEC.md §5.
    CompReport(View, Digest, PublicKey),
    /// `ControlInit` carries no sender field on the wire (same D4 class as `Propose`)
    /// -- the trusted sender is derived as this round's control leader by
    /// `VantageCore::dispatch_inbound`.
    ControlInit(ControlProposal, Option<ViewProposal>),
    ControlEcho(PublicKey, ControlProposal),
    ControlReady(PublicKey, ControlProposal),
    ControlCommit(PublicKey, Round),
    ControlTimeoutVote(PublicKey, Round),
    ControlTimeoutAccept(PublicKey, Round),
    ControlFetch(View, Digest, PublicKey),
    ControlServe(View, ViewProposal),
}

/// SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels
/// (`Parameters::authenticate_channels`) -- the candidate sender a MAC tag must verify
/// against, for every `PrimaryMessage` variant this handler ever routes. Mirrors
/// `VantageCore::wire_sender`'s classification exactly (declared wire sender field
/// where one exists), PLUS the two D4-class positionally-attributed variants
/// (`VantagePropose`/`ControlInit`), which carry no wire sender field at all but ARE
/// still cryptographically bindable here: `agb::proposer`/`control::control_leader`
/// are pure functions of `committee` alone, so this network-layer boundary can derive
/// the identical trusted sender `VantageCore::dispatch_inbound` derives, without
/// needing a live `VantageCore` (or its mutable AGB/control-log state) to ask.
/// `None` for `Header(_, true)` ("Serve") and `ControlServe`: neither carries any
/// sender claim (wire or positional) to bind -- the same pre-existing D4 gap
/// `wire_sender` already carves out (content is self-authenticating by digest / gated
/// downstream by `pending_fetch`, respectively). A MAC tag is still present on the wire
/// for these two (uniform framing -- every message carries exactly one trailing tag
/// when the flag is on), it is simply never checked, since there is nothing to check
/// it against.
fn mac_candidate_sender(message: &PrimaryMessage, committee: &Committee) -> Option<PublicKey> {
    match message {
        PrimaryMessage::Header(h, false) => Some(h.author),
        PrimaryMessage::Header(_, true) => None,
        PrimaryMessage::HeadersRequest(_, requestor) => Some(*requestor),
        PrimaryMessage::VantageAck(a) => Some(a.sender),
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
        // Autobahn-only variants never legitimately reach the Vantage assembly's port
        // (`dispatch`'s own catch-all ignores them below); no candidate needed.
        _ => None,
    }
}

/// SECURITY (Fable audit): the OUTBOUND-side mirror of `mac_candidate_sender`'s `None`
/// arms -- true for the two D4-class variants with no sender claim at all (wire or
/// positional) to bind. This node always sends its OWN messages, so on the outbound
/// side the candidate (when one exists) is trivially `self.name` -- only whether a
/// candidate exists at all matters here, not its value, hence the plain `bool`.
fn message_needs_placeholder_tag(message: &PrimaryMessage) -> bool {
    matches!(
        message,
        PrimaryMessage::Header(_, true) | PrimaryMessage::ControlServe(_, _)
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
            PrimaryMessage::VantagePropose(p) => Inbound::Propose(p),
            PrimaryMessage::VantageEcho(e) => Inbound::Echo(e),
            PrimaryMessage::VantageEchoSkip(v, s, w) => Inbound::EchoSkip(v, s, w),
            PrimaryMessage::VantageReady(r) => Inbound::Ready(r),
            PrimaryMessage::VantageNoReady(v, s, w) => Inbound::NoReady(v, s, w),
            PrimaryMessage::VantageWish(v, s) => Inbound::Wish(v, s),
            PrimaryMessage::CompReport(v, d, s) => Inbound::CompReport(v, d, s),
            PrimaryMessage::ControlInit(p, b) => Inbound::ControlInit(p, b),
            PrimaryMessage::ControlEcho(p, s) => Inbound::ControlEcho(s, p),
            PrimaryMessage::ControlReady(p, s) => Inbound::ControlReady(s, p),
            PrimaryMessage::ControlCommit(r, s) => Inbound::ControlCommit(s, r),
            PrimaryMessage::ControlTimeoutVote(r, s) => Inbound::ControlTimeoutVote(s, r),
            PrimaryMessage::ControlTimeoutAccept(r, s) => Inbound::ControlTimeoutAccept(s, r),
            PrimaryMessage::ControlFetch(v, d, s) => Inbound::ControlFetch(v, d, s),
            PrimaryMessage::ControlServe(v, p) => Inbound::ControlServe(v, p),
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

    network: ReliableSender,
    worker_network: SimpleSender,
    /// SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels
    /// (`Parameters::authenticate_channels`). `None` (the default) is byte-identical
    /// to pre-MAC behavior: `broadcast_message`/`send_message` hand `network`/
    /// `worker_network` the bare serialized message, nothing appended. `Some` when the
    /// flag is on: every outbound message gets a trailing tag over its own serialized
    /// bytes, keyed `k_{self, dest}` (or `k_{self, self}` for the two D4-class
    /// variants with no sender claim to bind -- see `PairwiseKeys::tag_unverified`'s
    /// doc comment) -- computed once and shared across a broadcast's destinations
    /// when the tag itself is destination-independent (the unverified-variant case),
    /// per-destination when it isn't.
    channel_auth: Option<Arc<PairwiseKeys>>,
    cancel_handlers: Vec<CancelHandler>,
    /// Fable perf audit item 4: `cancel_handlers.len()` at the last actual
    /// `prune_cancel_handlers` scan -- `maybe_prune_cancel_handlers` only re-scans
    /// once this has (at least) doubled, instead of every single loop iteration.
    last_prune_len: usize,

    other_primaries: Vec<(PublicKey, SocketAddr)>,
    /// Fable perf audit item 5a: `other_primaries`' addresses only, precomputed once
    /// -- `other_primaries` itself is fixed for this node's whole lifetime (built
    /// once here, never mutated), so `broadcast`'s previous per-call
    /// `.iter().map(|(_, a)| *a).collect()` rebuilt an identical `Vec<SocketAddr>`
    /// every single broadcast for no reason.
    other_primary_addrs: Vec<SocketAddr>,
    worker_addresses: HashMap<WorkerId, SocketAddr>,

    header_size: usize,
    max_header_delay: u64,
    digests: Vec<(Digest, WorkerId)>,
    payload_size: usize,

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

    /// D1 payload-sync bookkeeping: outstanding `(digest, worker_id)` keys per header
    /// digest, so `LaneManager::set_payload_ready` (which unconditionally marks a block
    /// payload-ready once called -- see its doc comment) is only called once *every*
    /// missing batch for that header has actually arrived, not on the first one.
    pending_payload: HashMap<Digest, HashSet<(Digest, WorkerId)>>,
    store: Store,
    tx_payload_ready: Sender<(Digest, Digest, WorkerId)>,

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

    /// PHASE7-PREP-NOTES.md: pays down PHASE4-NOTES.md §6's scope cut -- forwards each
    /// cursor-committed `Header` to the top-level application, the same output-channel
    /// shape `Committer` (Autobahn) already feeds. `Primary::spawn`'s `Vantage` arm
    /// used to drop the `tx_output` it's handed (never referenced it), so this
    /// channel's receiver (`node`/`local_benchmark`'s `rx_output`) closed immediately;
    /// `node::main`'s `analyze(rx_output)` loop returning on a closed channel is what
    /// hit the `unreachable!()` right after every primary's boot line.
    tx_output: Sender<Header>,
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
        let core_metrics = metrics.clone();
        if let Some(m) = metrics {
            lm = lm.with_metrics(m.clone());
            rep = rep.with_metrics(m.clone());
            agb = agb.with_metrics(m);
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

        // Transport-level batching, resolved once (mirrors `latency_map`/
        // `compress_network`'s own resolve-once-at-spawn convention).
        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

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
            network: {
                let mut s = ReliableSender::new()
                    .with_latency(latency_map.clone())
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
                    .with_compression(parameters.compress_network)
                    .with_batching(batch);
                if let Some(m) = &core_metrics {
                    s = s.with_metrics(m.clone());
                }
                s
            },
            channel_auth,
            cancel_handlers: Vec::new(),
            last_prune_len: 0,
            other_primaries,
            other_primary_addrs,
            worker_addresses,
            header_size: parameters.header_size,
            max_header_delay: parameters.max_header_delay,
            digests: Vec::new(),
            payload_size: 0,
            timers: BinaryHeap::new(),
            control_timers: BinaryHeap::new(),
            pending_payload: HashMap::new(),
            store,
            tx_payload_ready,
            // Clamped to >= 1: a window of 0 would place the GC floor at the resolved
            // watermark itself and prune state for the view being resolved.
            gc_window: parameters.vantage_gc_window_views.max(1),
            last_gc_floor: 1,
            metrics: core_metrics,
            ut_inbound_dispatch: None,
            ut_payload_sync: None,
            ut_timer_firing: None,
            ut_effect_execution: None,
            tx_output,
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

        loop {
            // P4-3, amended by Fable perf audit item 4: bound `cancel_handlers`'
            // otherwise-unbounded growth under sustained honest traffic, but without
            // the O(n) `retain_mut` scan on every single inbound message -- see
            // `maybe_prune_cancel_handlers`'s doc comment. The `metrics_tick` branch
            // below additionally forces an unconditional prune once/sec, bounding
            // staleness even if the list never doubles.
            self.maybe_prune_cancel_handlers();

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

                _ = metrics_tick.tick() => {
                    // Fable perf audit item 4: force an unconditional prune once/sec
                    // regardless of `maybe_prune_cancel_handlers`'s doubling
                    // condition, bounding worst-case staleness to ~1s.
                    self.prune_cancel_handlers();
                    self.collect_internal_garbage();
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
        if let Some(set) = self.pending_payload.get_mut(&header_digest) {
            set.remove(&(digest, worker_id));
            resolved = set.is_empty();
        }
        if resolved {
            self.pending_payload.remove(&header_digest);
            // P4-4: payload arriving can be the event that flips
            // `direct_pub`/`author_ok` for a C/T entry the positive gate is
            // waiting on -- re-poll it, same reasoning as the `Ack` arm.
            let mut effects = self.lm.set_payload_ready(&header_digest);
            effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
            self.execute(effects, now).await;
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
            self.cancel_handlers.len()
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
            let m = {
                let agb = &self.agb;
                let control = &self.control;
                self.resolver.decide(agb, view, now, |u| {
                    agb.is_sealed(u) || control.is_anchor_resolved(u)
                })
            };
            if let Some(proposal) = self.frontier.propose_view(view, &self.lm, m) {
                effects.push(Effect::BroadcastPropose(proposal.clone()));
                effects.extend(self.agb.on_propose(
                    self.name,
                    proposal,
                    now,
                    &mut self.lm,
                    &mut self.rep,
                ));
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

    /// SECURITY (Fable audit): extracts the wire-declared sender to validate against
    /// `self.members`, for every `Inbound` variant that carries one. `None` for
    /// internal or positionally-attributed facts: `Serve` (header content is
    /// self-authenticating by digest), `AckAvailability` (already checked by
    /// `AckAggregator`), `Propose`/`ControlInit` (positionally attributed to
    /// `proposer(view)`/`control_leader(round)`, D4's standing claimed-by-position
    /// class -- see `dispatch_inbound`'s own comments on those two arms), and
    /// `ControlServe` (gated downstream by `pending_fetch(view, digest)`).
    fn wire_sender(inbound: &Inbound) -> Option<PublicKey> {
        // `PublicKey` is `Copy` -- dereference rather than `.clone()` (clippy::clone_on_copy).
        match inbound {
            Inbound::Publish(sender, _) => Some(*sender),
            Inbound::HeadersRequest(_, requestor) => Some(*requestor),
            Inbound::Ack(ack) => Some(ack.sender),
            Inbound::Echo(e) => Some(e.sender),
            Inbound::EchoSkip(_, s, _) => Some(*s),
            Inbound::Ready(r) => Some(r.sender),
            Inbound::NoReady(_, s, _) => Some(*s),
            Inbound::Wish(_, s) => Some(*s),
            Inbound::CompReport(_, _, s) => Some(*s),
            Inbound::ControlEcho(s, _) => Some(*s),
            Inbound::ControlReady(s, _) => Some(*s),
            Inbound::ControlCommit(s, _) => Some(*s),
            Inbound::ControlTimeoutVote(s, _) => Some(*s),
            Inbound::ControlTimeoutAccept(s, _) => Some(*s),
            Inbound::ControlFetch(_, _, s) => Some(*s),
            Inbound::Serve(_)
            | Inbound::AckAvailability(_)
            | Inbound::Propose(_)
            | Inbound::ControlInit(_, _)
            | Inbound::ControlServe(_, _) => None,
        }
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

    async fn dispatch_inbound(&mut self, inbound: Inbound, now: Instant) -> Vec<Effect> {
        // SECURITY (Fable audit): the single centralized membership gate -- every
        // wire-declared sender is checked against the trusted committee-membership
        // set BEFORE any census/count path below ever sees the message. Wire messages
        // carry no signature, so without this check a single Byzantine node could
        // forge arbitrarily many distinct non-committee sender keys, each counted once
        // by the dedup-only census helpers downstream, inflating any party-count
        // quorum. Honest senders are always committee members, so this is a no-op on
        // every honest path.
        if let Some(sender) = Self::wire_sender(&inbound) {
            if !self.members.contains(&sender) {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_rejected_nonmember_total.inc();
                }
                return Vec::new();
            }
        }
        match inbound {
            Inbound::Publish(sender, header) => self.lm.process_publish(sender, header).await,
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
            Inbound::Propose(proposal) => {
                // D4 (PHASE4-SPEC.md §13's standing note): `ViewProposal` carries no
                // sender field and there is no channel identity to check it against
                // (same class of gap as `Header`'s publish path, PHASE3-NOTES.md §5) --
                // production trusts any received proposal for `view` as if it came
                // from `proposer(view)`. `AgbEngine::on_propose`'s `sender ==
                // proposer(view)` guard remains meaningful for unit tests exercising a
                // wrong-sender proposal directly.
                let claimed_sender = self.agb.proposer(proposal.view);
                self.agb
                    .on_propose(claimed_sender, proposal, now, &mut self.lm, &mut self.rep)
            }
            // PHASE5-SPEC.md §3: absorb every response's piggybacked wish (W2) BEFORE
            // handing the response to `AgbEngine` -- wish processing (amplification,
            // then entry) is independent of statement counting, so this ordering vs.
            // the engine's own processing is fine either way; absorbing first keeps the
            // four arms below symmetric with `Inbound::Wish`'s own handling.
            Inbound::Echo(echo) => {
                let mut effects = self.pacemaker.on_wish(echo.sender, echo.wish);
                effects.extend(self.agb.on_echo(echo, &mut self.rep));
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
                let mut effects = self.pacemaker.on_wish(ready.sender, ready.wish);
                effects.extend(self.agb.on_ready(ready, &mut self.rep));
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
                    self.broadcast_message(PrimaryMessage::Header(header, false))
                        .await
                }
                Effect::BroadcastAck(ack) => {
                    queue.extend(self.record_local_ack(&ack, now));
                    self.broadcast_message(PrimaryMessage::VantageAck(ack))
                        .await
                }
                Effect::SyncBatches(author, header_digest, missing) => {
                    self.sync_batches(author, header_digest, missing).await;
                }
                Effect::RequestTo(peer, digest) => {
                    self.send_message(
                        peer,
                        PrimaryMessage::HeadersRequest(vec![digest], self.name),
                    )
                    .await;
                }
                Effect::ServeTo(peer, header) => {
                    self.send_message(peer, PrimaryMessage::Header(header, true))
                        .await
                }
                Effect::BlockCached(digest) => {
                    queue.extend(self.rep.on_block_available(digest));
                    queue.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                    queue.extend(self.cursor.retry());
                }
                Effect::BroadcastPropose(p) => {
                    self.broadcast_message(PrimaryMessage::VantagePropose(p))
                        .await
                }
                // PHASE5-SPEC.md §3/D5-3: every response effect is stamped with our
                // current wish watermark here, at serialization time -- `AgbEngine`
                // itself stays watermark-free (its own construction sites use a `0`
                // placeholder, or none at all for `EchoSkip`/`NoReady`, which are
                // effects carrying just a `View` to begin with).
                Effect::BroadcastEcho(mut e) => {
                    e.wish = self.pacemaker.own_watermark();
                    self.broadcast_message(PrimaryMessage::VantageEcho(e)).await;
                }
                Effect::BroadcastEchoSkip(view) => {
                    let wish = self.pacemaker.own_watermark();
                    self.broadcast_message(PrimaryMessage::VantageEchoSkip(view, self.name, wish))
                        .await;
                }
                Effect::BroadcastReady(mut r) => {
                    r.wish = self.pacemaker.own_watermark();
                    self.broadcast_message(PrimaryMessage::VantageReady(r))
                        .await;
                }
                Effect::BroadcastNoReady(view) => {
                    let wish = self.pacemaker.own_watermark();
                    self.broadcast_message(PrimaryMessage::VantageNoReady(view, self.name, wish))
                        .await;
                }
                Effect::Fixed(view, well_formed) => {
                    let activated = self.frontier.record_fixed(view, well_formed);
                    for v in activated {
                        queue.extend(self.agb.activate(v, now, &mut self.lm, &mut self.rep));
                    }
                    queue.extend(self.try_propose_effects(now));
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
                    self.notify_committed(commit_millis, by_worker, headers)
                        .await;
                }
                Effect::BroadcastWish(view) => {
                    self.broadcast_message(PrimaryMessage::VantageWish(view, self.name))
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
                    // addition to) `Resolver::decide`'s own immediate refresh for its
                    // own attempts.
                    if let Some(entry) = &proposal.m {
                        self.resolver.note_carrier_report(entry.target_view(), now);
                    }
                    queue.extend(self.control.on_completion_reportable(view, proposal));
                }
                Effect::BroadcastCompReport(view, digest) => {
                    self.broadcast_message(PrimaryMessage::CompReport(view, digest, self.name))
                        .await;
                }
                Effect::BroadcastControlInit(proposal, b_w) => {
                    self.broadcast_message(PrimaryMessage::ControlInit(proposal, b_w))
                        .await;
                }
                Effect::BroadcastControlEcho(proposal) => {
                    self.broadcast_message(PrimaryMessage::ControlEcho(proposal, self.name))
                        .await;
                }
                Effect::BroadcastControlReady(proposal) => {
                    self.broadcast_message(PrimaryMessage::ControlReady(proposal, self.name))
                        .await;
                }
                Effect::BroadcastControlCommit(round) => {
                    self.broadcast_message(PrimaryMessage::ControlCommit(round, self.name))
                        .await;
                }
                Effect::BroadcastControlTimeoutVote(round) => {
                    self.broadcast_message(PrimaryMessage::ControlTimeoutVote(round, self.name))
                        .await;
                }
                Effect::BroadcastControlTimeoutAccept(round) => {
                    self.broadcast_message(PrimaryMessage::ControlTimeoutAccept(round, self.name))
                        .await;
                }
                Effect::ControlFetchTo(peer, view, digest) => {
                    self.send_message(peer, PrimaryMessage::ControlFetch(view, digest, self.name))
                        .await;
                }
                Effect::ControlServeTo(peer, view, proposal) => {
                    self.send_message(peer, PrimaryMessage::ControlServe(view, proposal))
                        .await;
                }
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
            }
        }
    }

    /// P4-3: drop every cancel handler that has already resolved (message ack'd) or
    /// closed (connection gone, will never resolve) -- keeps only the ones
    /// `ReliableSender` may still be actively retrying, so the retry-until-ack
    /// semantics are unaffected (`Connection::keep_alive` treats a dropped receiver's
    /// closed sender as cancellation, per `network::reliable_sender`, so we must never
    /// drop one that's genuinely still pending).
    fn prune_cancel_handlers(&mut self) {
        self.cancel_handlers
            .retain_mut(|handler| matches!(handler.try_recv(), Err(TryRecvError::Empty)));
        self.last_prune_len = self.cancel_handlers.len();
    }

    /// Fable perf audit item 4: an O(1) length check on every `run` loop iteration,
    /// only actually invoking `prune_cancel_handlers`'s O(n) `retain_mut` scan once
    /// `cancel_handlers` has (at least) doubled since the last prune. `run`'s
    /// `metrics_tick` branch additionally forces an unconditional prune once/sec, so a
    /// slow-growing tail that never doubles is still bounded to ~1s of extra
    /// staleness -- handlers surviving marginally longer than before is harmless (see
    /// `prune_cancel_handlers`'s own doc comment for why dropping one early would
    /// NOT be harmless).
    fn maybe_prune_cancel_handlers(&mut self) {
        if self.cancel_handlers.len() >= 2 * self.last_prune_len.max(1) {
            self.prune_cancel_handlers();
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

    /// Shared shape behind every `execute` arm that just serializes a `PrimaryMessage`
    /// and broadcasts it verbatim (`bincode::serialize` never fails on our own wire
    /// types, hence the `expect`). METRICS-DASHBOARD-SPEC.md §1: `message.type_name()`
    /// is computed before serializing, so it labels the exact variant sent.
    async fn broadcast_message(&mut self, message: PrimaryMessage) {
        let msg_type = message.type_name();
        // METRICS-DASHBOARD-SPEC.md §3: `proposed_block_size_bytes` -- our own
        // self-authored block's serialized size at publish time. `Header(_, false)` is
        // specifically the publish variant (`false` = not a serve/sync reply); the
        // metrics handle is `Option` (unit tests construct `VantageCore` without one).
        let is_own_publish = matches!(message, PrimaryMessage::Header(_, false));
        // SECURITY (Fable audit): `Header(_, true)` ("Serve") never legitimately
        // reaches `broadcast_message` (`ServeTo` always unicasts via `send_message`
        // below), so the only D4-class placeholder-tag variant this method ever sees
        // in practice is `ControlServe` -- kept as a real `match`, not an assert, so
        // this stays correct even if a future effect ever does broadcast one of them.
        let placeholder = message_needs_placeholder_tag(&message);
        let bytes = bincode::serialize(&message).expect("serializes");
        if is_own_publish {
            if let Some(metrics) = &self.metrics {
                metrics.proposed_block_size_bytes.observe(bytes.len());
            }
        }
        self.broadcast(bytes, msg_type, placeholder).await;
    }

    /// Shared shape behind every `execute` arm that just serializes a `PrimaryMessage`
    /// and unicasts it verbatim to one peer.
    async fn send_message(&mut self, peer: PublicKey, message: PrimaryMessage) {
        let msg_type = message.type_name();
        let placeholder = message_needs_placeholder_tag(&message);
        let bytes = bincode::serialize(&message).expect("serializes");
        self.send_to(peer, bytes, msg_type, placeholder).await;
    }

    /// SECURITY (Fable audit): appends this message's MAC tag (when `channel_auth` is
    /// on) before broadcasting to every other primary. `placeholder` (see
    /// `message_needs_placeholder_tag`): the two D4-class variants with no sender
    /// claim to bind get one destination-independent tag (`PairwiseKeys::
    /// tag_unverified`), computed once and shared across every destination -- exactly
    /// like the flag-off path's own `.clone()`-shared `Bytes` -- rather than N
    /// per-destination tags nobody will ever check. Every other variant gets a
    /// genuine per-destination tag (`k_{self, dest}`), since `dest` varies.
    async fn broadcast(&mut self, payload: Vec<u8>, msg_type: &'static str, placeholder: bool) {
        let Some(auth) = self.channel_auth.clone() else {
            // Flag off: byte-identical to pre-MAC behavior. Fable perf audit item 5a:
            // `other_primary_addrs` is precomputed once (see its own doc comment) --
            // this `.clone()` is a straight contiguous `Vec<SocketAddr>` memcpy.
            let handlers = self
                .network
                .broadcast_typed(
                    self.other_primary_addrs.clone(),
                    Bytes::from(payload),
                    msg_type,
                )
                .await;
            self.cancel_handlers.extend(handlers);
            return;
        };
        if placeholder {
            let mut tagged = payload;
            tagged.extend_from_slice(&auth.tag_unverified(&tagged));
            let handlers = self
                .network
                .broadcast_typed(
                    self.other_primary_addrs.clone(),
                    Bytes::from(tagged),
                    msg_type,
                )
                .await;
            self.cancel_handlers.extend(handlers);
            return;
        }
        for (peer, addr) in self.other_primaries.clone() {
            let tag = auth
                .tag_for(&peer, &payload)
                .expect("every `other_primaries` entry is a committee member");
            let mut tagged = payload.clone();
            tagged.extend_from_slice(&tag);
            let handler = self
                .network
                .send_typed(addr, Bytes::from(tagged), msg_type)
                .await;
            self.cancel_handlers.push(handler);
        }
    }

    /// SECURITY (Fable audit): same tag-append contract as `broadcast`, for a single
    /// destination.
    async fn send_to(
        &mut self,
        peer: PublicKey,
        payload: Vec<u8>,
        msg_type: &'static str,
        placeholder: bool,
    ) {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return;
        };
        let data = match &self.channel_auth {
            None => Bytes::from(payload),
            Some(auth) => {
                let tag = if placeholder {
                    auth.tag_unverified(&payload)
                } else {
                    auth.tag_for(&peer, &payload)
                        .expect("peer is a committee member")
                };
                let mut tagged = payload;
                tagged.extend_from_slice(&tag);
                Bytes::from(tagged)
            }
        };
        let handler = self.network.send_typed(addr, data, msg_type).await;
        self.cancel_handlers.push(handler);
    }

    /// SECURITY (Fable audit): appends a tag keyed `k_{self, self}` (the worker<->
    /// primary channel is intra-authority: our own worker's public key IS `self.name`)
    /// before sending to one of our own workers. A no-op (byte-identical) when
    /// `channel_auth` is off.
    async fn send_to_worker(&mut self, addr: SocketAddr, payload: Vec<u8>, msg_type: &'static str) {
        let data = match &self.channel_auth {
            None => Bytes::from(payload),
            Some(auth) => {
                let tag = auth
                    .tag_for(&self.name, &payload)
                    .expect("self is a committee member");
                let mut tagged = payload;
                tagged.extend_from_slice(&tag);
                Bytes::from(tagged)
            }
        };
        self.worker_network.send_typed(addr, data, msg_type).await;
    }

    /// D1/§1: ask our own workers to sync `missing` batches for `author`'s block
    /// (`header_digest`), then spawn one `store.notify_read` waiter per missing key;
    /// once *every* key for this header has resolved, call
    /// `LaneManager::set_payload_ready`.
    async fn sync_batches(
        &mut self,
        author: PublicKey,
        header_digest: Digest,
        missing: Vec<(Digest, WorkerId)>,
    ) {
        if missing.is_empty() {
            return;
        }
        let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        for (digest, worker_id) in &missing {
            by_worker
                .entry(*worker_id)
                .or_default()
                .push(digest.clone());
        }
        for (worker_id, digests) in by_worker {
            if let Some(addr) = self.worker_addresses.get(&worker_id).copied() {
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Synchronize(digests, author))
                    .expect("serializes");
                self.send_to_worker(addr, bytes, "Synchronize").await;
            }
        }

        let set: HashSet<(Digest, WorkerId)> = missing.iter().cloned().collect();
        self.pending_payload.insert(header_digest.clone(), set);
        for (digest, worker_id) in missing {
            let mut store = self.store.clone();
            let tx = self.tx_payload_ready.clone();
            let header_digest = header_digest.clone();
            tokio::spawn(async move {
                let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                if store.notify_read(key).await.is_ok() {
                    let _ = tx.send((header_digest, digest, worker_id)).await;
                }
            });
        }
    }

    /// Commit metric (Phase-2 parity, §9): forward the cursor's per-`WorkerId`
    /// notification to our own workers -- the existing worker-side observe path
    /// (`worker::synchronizer`) does the rest. Also (PHASE7-PREP-NOTES.md, paying down
    /// PHASE4-NOTES.md §6's scope cut) forwards each committed `Header` to the
    /// top-level application via `tx_output`, the same shape/tolerance as Autobahn's
    /// `Committer` (`primary/src/committer.rs`): a closed or full receiver is logged,
    /// not treated as fatal -- `node::main`'s `analyze` loop is a no-op consumer either
    /// way, and other assemblies' equivalent sends already tolerate this identically.
    async fn notify_committed(
        &mut self,
        commit_millis: u64,
        by_worker: Vec<(WorkerId, Vec<Digest>)>,
        headers: Vec<Header>,
    ) {
        for (worker_id, digests) in by_worker {
            if let Some(addr) = self.worker_addresses.get(&worker_id).copied() {
                let bytes =
                    bincode::serialize(&PrimaryWorkerMessage::Committed(commit_millis, digests))
                        .expect("serializes");
                self.send_to_worker(addr, bytes, "Committed").await;
            }
        }
        for header in headers {
            if let Err(e) = self.tx_output.send(header).await {
                log::debug!("Failed to send block through the output channel: {}", e);
            }
        }
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
            .dispatch_inbound(Inbound::Echo(echo), Instant::now())
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
            .dispatch_inbound(Inbound::Ready(ready), Instant::now())
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
            .dispatch_inbound(Inbound::Echo(echo), Instant::now())
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
}
