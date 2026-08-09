// Network/wire-transport helpers for `VantageCore`, split out of `vantage::node` so a
// second consensus protocol can reuse the same primary<->primary/primary<->worker wire
// machinery (cancel-handler bookkeeping, broadcast/unicast dispatch) without depending
// on Vantage's own protocol state (`agb`, `frontier`, `cursor`, `pacemaker`, `resolver`,
// `control`). Pure code motion out of `node.rs`: every field and method below is
// unchanged from its previous home on `VantageCore`, aside from the `self.` ->
// `self.wire.`-style re-pointing the split forces at `VantageCore`'s own call sites
// (see `node.rs` itself for those).
//
// reconnect-replay plan (server-authoritative floor, v3): this module also carries
// the wire-level half of the reconnect-triggered replay mechanism -- `broadcast_
// volatile`/`send_volatile` (the outbox's own send path), the `addr_to_peer`/`dirty_
// map`/`in_flight` handles, and the isolated lane/replay sender scheduling (see
// `ReplaySend` and `spawn_resume_sender`'s own doc comments for the
// full design). Audit-3 A9, stated once here for the whole subsystem: unlike
// Mechanism A's own `Message` traffic (retried end-to-end ABOVE this layer, so a
// crashed sender task only stalls it), a panic in the resume-sender task is a
// PERMANENT-LOSS event for every `pending_low` span already raised on the strength
// of an enqueue that looked successful -- the same acceptance class as any other
// detached `tokio::spawn` task in this codebase, just with a distinct, user-visible
// symptom (`vantage_replay_pending_low_nudges_total` climbing without bound).
//
// Adversarial-audit FINDING 2: `Replay`/`Done` frames ride `network::ReliableSender::
// send_detached_typed` (detached-durable -- see `run_replay_sender`'s own doc comment
// for why they must never carry a `CancelHandler` this task would just drop). Durable
// means requeued forever against a peer that never reconnects, so admission has an
// explicit global logical-byte bound across queued and active replay work: normally
// `2 * replay_serve_max_bytes`, plus the deliberate sole-oversized-stream exception.
// A separate 64-slot cap bounds stream metadata. Per-peer generation tokens prevent
// a stale stream's completion from clearing a newer stream admitted after the TTL.

use crate::messages::Header;
use crate::primary::PrimaryMessage;
use crate::vantage::node::Inbound;
use crate::vantage::resume::InFlightEntry;
use bytes::Bytes;
use config::WorkerId;
use crypto::PublicKey;
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, DirtyMap, ReliableSender, SimpleSender};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot::error::TryRecvError;

/// reconnect-replay plan §6/§14 A8 (clippy::type_complexity): shared in-flight-
/// replay-stream marker -- `VantageCore`'s own insert-on-(successful-)enqueue side
/// lives on `Wire::in_flight`; the resume task's own remove-after-`Done` side is
/// handed the same `Arc` at spawn time (`spawn_resume_sender`). See
/// `resume::InFlightState`'s own doc comment for the pure decision logic layered
/// over a borrowed snapshot of this map -- this alias exists only so the two owners
/// (a struct field and a spawned task's argument) never have to spell out the full
/// nested type.
pub(crate) type InFlightMap = Arc<Mutex<HashMap<PublicKey, InFlightEntry>>>;

/// SECURITY (Fable audit): a message's wire-declared (or positionally-attributed)
/// sender claim, for `sender_is_member`'s generic non-member gate to check. One `impl`
/// per `Inbound` type a protocol assembly in this crate routes, so the gate itself
/// (`sender_is_member`) needs exactly one definition regardless of how many protocols
/// use it -- see `impl DeclaredSender for Inbound` immediately below for Vantage's own,
/// and `simpleit::node`'s `impl DeclaredSender for simpleit::engine::Inbound` for
/// Simple-IT's sibling implementation.
pub trait DeclaredSender {
    fn declared_sender(&self) -> Option<PublicKey>;
}

/// SECURITY (Fable audit): extracts the wire-declared sender to validate against
/// `self.members`, for every `Inbound` variant that carries one. `None` for
/// internal or positionally-attributed facts: `Serve` (header content is
/// self-authenticating by digest), `AckAvailability` (already checked by
/// `AckAggregator`), `Propose`/`ControlInit` (positionally attributed to
/// `proposer(view)`/`control_leader(round)`, D4's standing claimed-by-position
/// class -- see `dispatch_inbound`'s own comments on those two arms), and
/// `ControlServe` (gated downstream by `pending_fetch(view, digest)`).
impl DeclaredSender for Inbound {
    fn declared_sender(&self) -> Option<PublicKey> {
        // `PublicKey` is `Copy` -- dereference rather than `.clone()` (clippy::clone_on_copy).
        match self {
            Inbound::Publish(sender, _) => Some(*sender),
            Inbound::HeadersRequest(_, requestor) => Some(*requestor),
            Inbound::Ack(ack) => Some(ack.sender),
            Inbound::Avail(_, s) => Some(*s),
            Inbound::Echo(e) => Some(e.sender()),
            Inbound::EchoSkip(_, s, _) => Some(*s),
            Inbound::Ready(r) => Some(r.sender()),
            Inbound::NoReady(_, s, _) => Some(*s),
            Inbound::Wish(_, s) => Some(*s),
            // Phase B: the framework compares this against the authenticated
            // connection, which is what makes the f+1 announcement rule first-hand.
            Inbound::SequenceAnnounce(_, s) => Some(*s),
            Inbound::SequenceRequest(_, s) => Some(*s),
            Inbound::SequenceRecords(_, s) => Some(*s),
            Inbound::SequenceDeltaRequest(_, s) => Some(*s),
            Inbound::SequenceDelta(_, s) => Some(*s),
            Inbound::SequenceDeltaRangeRequest(_, s) => Some(*s),
            Inbound::SequenceDeltaRange(_, s) => Some(*s),
            Inbound::SequenceOutcomeRequest(_, s) => Some(*s),
            Inbound::SequenceOutcome(_, s) => Some(*s),
            Inbound::SequenceUnavailable(_, s) => Some(*s),
            Inbound::CompReport(_, _, s) => Some(*s),
            Inbound::ControlEcho(s, _) => Some(*s),
            Inbound::ControlReady(s, _) => Some(*s),
            Inbound::ControlCommit(s, _) => Some(*s),
            Inbound::ControlTimeoutVote(s, _) => Some(*s),
            Inbound::ControlTimeoutAccept(s, _) => Some(*s),
            Inbound::ControlFetch(_, _, s) => Some(*s),
            Inbound::SkipVote(_, s) => Some(*s),
            // signature-free.tex §8.3 "Digest-named AGB statements": `EchoDigest`/
            // `ReadyDigest` carry a real declared sender directly, same class as
            // `Echo`/`Ready` above; `BodyFetch` carries a real declared requester,
            // same class as `ControlFetch` above.
            Inbound::EchoDigest(d) => Some(d.sender),
            Inbound::ReadyDigest(d) => Some(d.sender),
            Inbound::BodyFetch(_, _, s) => Some(*s),
            // Mechanism A (`vantage::resume`): a real declared requester, same class
            // as `HeadersRequest`/`BodyFetch` above -- checked against membership
            // before `dispatch_inbound` ever inspects the (untrusted-until-then)
            // `author` field.
            Inbound::LaneResume(_, _, requester) => Some(*requester),
            // reconnect-replay plan §7: real, declared senders, same D4 class as
            // `LaneResume`'s own `requester` field above.
            Inbound::ResumeHello(_, sender) => Some(*sender),
            Inbound::ReplayDone(_, _, _, sender) => Some(*sender),
            Inbound::Serve(_)
            | Inbound::AckAvailability(_)
            | Inbound::Propose(_)
            | Inbound::ControlInit(_, _)
            | Inbound::ControlServe(_, _)
            // `BodyServe`'s content is self-authenticating by hash against an
            // outstanding `pending_fetch` entry, same D4 class as `ControlServe`.
            | Inbound::BodyServe(_, _) => None,
        }
    }
}

/// The membership half of `VantageCore::dispatch_inbound`'s centralized gate (see that
/// call site for the full SECURITY rationale) -- built on `DeclaredSender`. `true` when
/// `m` carries no declared sender to check (`DeclaredSender::declared_sender`'s own
/// `None` carve-outs), or when it does and that sender is in `members`; `false` only
/// when a declared sender is present and is NOT a member. Generic over `M:
/// DeclaredSender` so this stays the ONE definition of the gate across every protocol
/// assembly this crate routes, rather than each reimplementing the identical
/// `Some(s) => members.contains(&s), None => true` logic for its own `Inbound` type.
pub fn sender_is_member<M: DeclaredSender>(m: &M, members: &HashSet<PublicKey>) -> bool {
    match m.declared_sender() {
        Some(sender) => members.contains(&sender),
        None => true,
    }
}

/// clippy::type_complexity: named alias for `Wire::withheld_header_dests`'s own type
/// (also used identically by `VantageCore::build`/`SimpleItCore::build`, which compute
/// the value this field holds) -- a caller-filtered pair of `Wire::
/// other_primary_addrs`/`Wire::other_primaries` (addresses-only, and the full
/// `(PublicKey, SocketAddr)` pairs `enqueue_resume_header` needs to check whether a
/// given peer is in this node's own allowed, i.e. non-blocked, half), or `None` when
/// this node is not a withholding sender.
pub(crate) type WithheldHeaderDests = Option<(Vec<SocketAddr>, Vec<(PublicKey, SocketAddr)>)>;

/// Network/wire-transport state `VantageCore` owns: the two typed senders, in-flight
/// cancel-handle bookkeeping, and the committee's/our own workers' resolved addresses.
/// Per-field security/perf rationale below is carried over verbatim from
/// `VantageCore`'s previous copy of each field.
pub struct Wire {
    pub(crate) network: ReliableSender,
    pub(crate) worker_network: SimpleSender,
    /// Mechanism A (sender-side lane resume, `vantage::resume`): the channel end
    /// this node's run loop `try_send`s onto (`enqueue_resume`/`enqueue_resume_
    /// header` below) -- NEVER `.await`ed, so a backed-up destination costs
    /// nothing on this side. The receiving half is owned entirely by the
    /// dedicated task `spawn_resume_sender` spawns at construction time, which
    /// owns its OWN `SimpleSender` (a separate connection pool from `network`/
    /// `worker_network` above) -- see that function's doc comment for the full
    /// design. This field, plus those two enqueue methods, is the ENTIRE fix for
    /// the loop-starvation defect this crate's previous per-send `resume::
    /// SEND_TIMEOUT` (deleted) only ever bounded the damage from: a backed-up
    /// destination now costs the sender task's own progress, never
    /// `VantageCore`/`SimpleItCore`'s run loop's. Lane traffic is deliberately
    /// isolated from replay traffic: filling or blocking this queue cannot consume
    /// replay admission capacity or delay the replay pacing task.
    pub(crate) resume_lane_tx: mpsc::Sender<LaneSend>,
    /// Reconnect replay's independently bounded ingress. The wrapper reserves the
    /// stream's complete logical byte footprint before admission and retains that
    /// reservation while the stream is queued or active.
    pub(crate) replay_tx: ReplaySender,
    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md section 6.1: state-sync egress. Same
    /// never-awaited discipline as `resume_lane_tx` -- the run loop `try_send`s and moves
    /// on, so a peer that cannot keep up costs dropped frames, never loop progress.
    pub(crate) sequence_tx: mpsc::Sender<SequenceSend>,
    /// Unique stream generation source. Generations make task-side completion and
    /// enqueue-failure cleanup conditional on the exact stream they refer to.
    pub(crate) replay_generation: AtomicU64,
    pub(crate) cancel_handlers: Vec<CancelHandler>,
    /// Fable perf audit item 4: `cancel_handlers.len()` at the last actual
    /// `prune_cancel_handlers` scan -- `maybe_prune_cancel_handlers` only re-scans
    /// once this has (at least) doubled, instead of every single loop iteration.
    pub(crate) last_prune_len: usize,

    pub(crate) other_primaries: Vec<(PublicKey, SocketAddr)>,
    /// Fable perf audit item 5a: `other_primaries`' addresses only, precomputed once
    /// -- `other_primaries` itself is fixed for this node's whole lifetime (built
    /// once here, never mutated), so `broadcast`'s previous per-call
    /// `.iter().map(|(_, a)| *a).collect()` rebuilt an identical `Vec<SocketAddr>`
    /// every single broadcast for no reason.
    pub(crate) other_primary_addrs: Vec<SocketAddr>,
    pub(crate) worker_addresses: HashMap<WorkerId, SocketAddr>,

    /// Data-plane withholding fault injector (`Parameters::withhold_senders`):
    /// `other_primaries`/`other_primary_addrs` above, with this node's own blocked
    /// half already removed -- precomputed ONCE at construction (`VantageCore::build`/
    /// `SimpleItCore::build`, via `config::withheld_destinations`), never per send.
    /// `None` -- the default, and always the case when `--withhold` is 0 -- means this
    /// node is not a withholding sender: `broadcast_message`'s `Header(_, false)` arm
    /// then falls straight through to the untouched `broadcast` below, so the default
    /// header-dissemination path allocates and branches exactly as it did before this
    /// field existed. `Some((addrs, full))` is used ONLY for that one arm -- every
    /// other broadcast (VantageEcho/Ready/ControlInit/etc.) always uses
    /// `other_primaries`/`other_primary_addrs` unfiltered, regardless of this field.
    pub(crate) withheld_header_dests: WithheldHeaderDests,

    /// Data-plane withholding fault injector, TIME-WINDOWED variant
    /// (`Parameters::withhold_window`): the shared, in-process "has the window opened
    /// yet" cell, cloned straight from `parameters` at construction (`VantageCore::
    /// build`/`SimpleItCore::build`). Consulted (via `config::withhold_active`) in
    /// `broadcast_message`'s `Header(_, false)` arm, ONLY when `withheld_header_dests`
    /// is `Some` -- see that
    /// method's own comment. `None` whenever `--withhold-at` isn't given (including
    /// whenever `withheld_header_dests` itself is already `None`), in which case it's
    /// never even looked at.
    pub(crate) withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,

    /// Cloned from `VantageCore::metrics` at construction time (kept there too, for
    /// `sample_metrics`'s progress gauges) -- `broadcast_message` is the only wire
    /// method that observes a metric (`proposed_block_size_bytes`).
    pub(crate) metrics: Option<Arc<Metrics>>,

    /// reconnect-replay plan §2.3/§7: the exact reverse of `other_primaries`,
    /// precomputed once (mirrors `other_primary_addrs`'s own "fixed for this node's
    /// lifetime" reasoning) -- the dirty-map sweep's ONLY use for it: translating a
    /// `Connection`'s own `SocketAddr` key into the `PublicKey` whose `pending_low`
    /// entry (owned by `VantageCore`, not this struct) should be min-merged. Also
    /// used to resolve `reconnect_rx`'s own `SocketAddr` events the same way.
    pub(crate) addr_to_peer: HashMap<SocketAddr, PublicKey>,
    /// reconnect-replay plan §2.3/§7/§14 A8: the shared dirty map `self.network`
    /// (the MAIN pool only -- A7) min-merges every session-death volatile-drop
    /// report into. `VantageCore`'s own `resume_tick`/Hello handling DRAINS this
    /// (never merely reads it -- A8) then min-merges each entry into `pending_low`.
    /// A fresh, empty, permanently-unfed map on `SimpleItCore` (which never attaches
    /// it to a `ReliableSender` via `with_drop_map` -- "no events" per this crate's
    /// own wiring notes): harmless, since nothing ever writes into it there.
    pub(crate) dirty_map: DirtyMap,
    /// reconnect-replay plan §6/§14 A8: the shared in-flight-replay-stream map --
    /// see `InFlightMap`'s own doc comment. A fresh, empty, permanently-unfed map on
    /// `SimpleItCore` for the identical reason `dirty_map` is.
    pub(crate) in_flight: InFlightMap,
}

impl Wire {
    /// P4-3: drop every cancel handler that has already resolved (message ack'd) or
    /// closed (connection gone, will never resolve) -- keeps only the ones
    /// `ReliableSender` may still be actively retrying, so the retry-until-ack
    /// semantics are unaffected (`Connection::keep_alive` treats a dropped receiver's
    /// closed sender as cancellation, per `network::reliable_sender`, so we must never
    /// drop one that's genuinely still pending).
    ///
    /// reconnect-replay plan §2.2/§7/§14 A2/m8 (updated contract): a VOLATILE send
    /// (`broadcast_recorded`/`broadcast_volatile`, this crate's dominant broadcast
    /// traffic class post-v3) never allocates a cancel handler at all -- there is
    /// nothing here to prune for it. `self.cancel_handlers` therefore now only ever
    /// grows from the traffic that stays durable (`Header(_, false)`, `VantageAvail`,
    /// and every unicast) -- this method's own O(n) cost is unchanged, but the SIZE
    /// of `n` it scans no longer tracks total broadcast volume the way it used to.
    pub(crate) fn prune_cancel_handlers(&mut self) {
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
    pub(crate) fn maybe_prune_cancel_handlers(&mut self) {
        if self.cancel_handlers.len() >= 2 * self.last_prune_len.max(1) {
            self.prune_cancel_handlers();
        }
    }

    /// Shared shape behind every `execute` arm that just serializes a `PrimaryMessage`
    /// and broadcasts it verbatim (`bincode::serialize` never fails on our own wire
    /// types, hence the `expect`). METRICS-DASHBOARD-SPEC.md §1: `message.type_name()`
    /// is computed before serializing, so it labels the exact variant sent.
    pub(crate) async fn broadcast_message(&mut self, message: PrimaryMessage) {
        let msg_type = message.type_name();
        let bytes = bincode::serialize(&message).expect("serializes");
        // METRICS-DASHBOARD-SPEC.md §3 (+ addendum): `proposed_block_size_bytes` (the
        // full wire envelope) and `proposed_header_size_bytes` (the header serialized
        // in isolation) -- our own self-authored block's size at publish time.
        // `Header(_, false)` is specifically the publish variant (`false` = not a
        // serve/sync reply); the metrics handle is `Option` (unit tests construct
        // `VantageCore` without one). Reused verbatim by `SimpleItCore`, which
        // broadcasts its own data-plane headers through this same `Wire`.
        if let PrimaryMessage::Header(header, false) = &message {
            if let Some(metrics) = &self.metrics {
                metrics.proposed_block_size_bytes.observe(bytes.len());
                // `serialized_size` COMPUTES the length without building the buffer --
                // the previous `bincode::serialize(header)` allocated and encoded a
                // whole second copy of every published car purely to call `.len()` on
                // it. Same value, on the hot publish path.
                let header_len = bincode::serialized_size(header).expect("serializes") as usize;
                metrics.proposed_header_size_bytes.observe(header_len);
            }
            // Data-plane withholding fault injector (`--withhold`): this is the ONLY
            // original-dissemination publish of a header (`ServeTo`'s `Header(_,
            // true)` reply unicasts via `send_message`/`send_to`, never through here) --
            // `.clone()` on a `None` `Option` is a no-op, so a non-withholding node
            // (every node when `--withhold` is 0) falls straight through to the
            // untouched `broadcast` call below with no extra allocation. When this
            // node IS a withholding sender, whether the filtered path is actually
            // taken additionally depends on `withhold_window`
            // (`--withhold-at`/`--withhold-for`, `config::withhold_active`) --
            // active: filtered `broadcast_to`; inactive (including the unwindowed
            // `None` case, which is always active -- see that fn's own doc comment):
            // fall through to the untouched `broadcast` below, exactly as if this
            // node were not withholding at all right now.
            if let Some((addrs, _)) = self.withheld_header_dests.clone() {
                if config::withhold_active(self.withhold_window.as_deref(), Instant::now()) {
                    self.broadcast_to(bytes, msg_type, addrs).await;
                    return;
                }
            }
        }
        self.broadcast(bytes, msg_type).await;
    }

    /// Shared shape behind every `execute` arm that just serializes a `PrimaryMessage`
    /// and unicasts it verbatim to one peer.
    pub(crate) async fn send_message(&mut self, peer: PublicKey, message: PrimaryMessage) {
        let msg_type = message.type_name();
        let bytes = bincode::serialize(&message).expect("serializes");
        self.send_to(peer, bytes, msg_type).await;
    }

    /// reconnect-replay plan §2.2/§7: `VantageCore::broadcast_recorded`'s wire half
    /// -- records nothing itself (the outbox lives on `VantageCore`;
    /// `broadcast_recorded` calls `outbox::Outbox::record` then this). `payload` is
    /// already serialized (the Bytes path -- no handler bookkeeping at all, unlike
    /// `broadcast`/`broadcast_to`, which extend `self.cancel_handlers`); `key` is
    /// `Pacemaker::own_watermark()` at send time, the outbox's own filing key -- see
    /// `network::ReliableSender::broadcast_volatile`'s doc comment for how a
    /// coalesced bundle reduces several constituents' keys to one (min).
    pub(crate) async fn broadcast_volatile(
        &mut self,
        payload: Bytes,
        msg_type: &'static str,
        key: u64,
    ) {
        self.network
            .broadcast_volatile_typed(&self.other_primary_addrs, payload, key, msg_type)
            .await;
    }

    /// reconnect-replay plan §14 A7: the unicast counterpart of `broadcast_volatile`,
    /// used ONLY by the server-side nudge loop's own Hello send -- nudges are sent
    /// VOLATILE (self-superseding: a later nudge always supersedes an undelivered
    /// earlier one; a lost nudge is simply re-nudged on the next `resume_tick`) so
    /// that a peer stuck cycling through repeated failed reconnects never
    /// accumulates one durably-retried nudge per backoff period forever (contrast
    /// with the OTHER three Hello triggers -- reconnect-prompt, tick re-Hello, and
    /// reciprocal-on-receipt -- which stay on the ordinary durable `send_message`
    /// path via `VantageCore::send_resume_hello`: each of those is either a one-shot
    /// event or bounded by the requester's own `replay_episode_max_ms` expiry valve,
    /// so durable delivery is both safe and valuable there -- see D4 caveat (ii) in
    /// `resume`'s own module doc for why a forged low floor on any of these can only
    /// ever help, never suppress, the recipient). `key` is `Pacemaker::own_watermark()`
    /// at send time -- see this method's own call site for why an approximate key is
    /// safe here (a nudge is not itself outbox content).
    pub(crate) async fn send_volatile(
        &mut self,
        peer: PublicKey,
        message: PrimaryMessage,
        key: u64,
    ) {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return;
        };
        let msg_type = message.type_name();
        let bytes = Bytes::from(bincode::serialize(&message).expect("serializes"));
        self.network
            .send_volatile_typed(addr, bytes, key, msg_type)
            .await;
    }

    /// Broadcasts `payload` verbatim to every other primary. Fable perf audit item 5a:
    /// `other_primary_addrs` is precomputed once (see its own doc comment) -- this
    /// `.clone()` is a straight contiguous `Vec<SocketAddr>` memcpy.
    async fn broadcast(&mut self, payload: Vec<u8>, msg_type: &'static str) {
        // Slice variant: this is the own-car publish path, and cloning the 99-element
        // address list on every publish allocated and copied for no reason.
        let handlers = self
            .network
            .broadcast_typed_slice(&self.other_primary_addrs, Bytes::from(payload), msg_type)
            .await;
        self.cancel_handlers.extend(handlers);
    }

    /// Data-plane withholding fault injector (`--withhold`): same wire-level contract
    /// as `broadcast`, but against a caller-supplied destination list instead of
    /// `self.other_primary_addrs` -- `addrs` is that field with this node's own
    /// blocked half already removed (`withheld_header_dests`). `broadcast` itself is
    /// left completely untouched (not even refactored to delegate here) so the
    /// default, non-withholding path -- including every non-Header broadcast, which
    /// never reaches this method at all -- keeps its exact original allocation/branch
    /// shape.
    async fn broadcast_to(
        &mut self,
        payload: Vec<u8>,
        msg_type: &'static str,
        addrs: Vec<SocketAddr>,
    ) {
        let handlers = self
            .network
            .broadcast_typed(addrs, Bytes::from(payload), msg_type)
            .await;
        self.cancel_handlers.extend(handlers);
    }

    /// Hand one state-sync frame to the dedicated sender. Returns false when the frame
    /// was dropped because the bounded egress is full.
    ///
    /// NEVER awaited: state-sync must not be able to stall the run loop it exists to
    /// unblock. Dropping is safe because every state-sync message is idempotent and
    /// re-requestable -- a lost response costs the requester one timeout and a failover
    /// to another matching announcer, which the design already requires it to handle
    /// because up to `f` of them may withhold entirely.
    pub(crate) fn try_send_sequence(&self, peer: &PublicKey, message: PrimaryMessage) -> bool {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| pk == peer)
            .map(|(_, a)| *a)
        else {
            return false;
        };
        self.sequence_tx
            .try_send(SequenceSend(addr, message))
            .is_ok()
    }

    /// Unicasts `payload` verbatim to a single peer.
    async fn send_to(&mut self, peer: PublicKey, payload: Vec<u8>, msg_type: &'static str) {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return;
        };
        let handler = self
            .network
            .send_typed(addr, Bytes::from(payload), msg_type)
            .await;
        self.cancel_handlers.push(handler);
    }

    /// Sends to one of our own workers.
    pub(crate) async fn send_to_worker(
        &mut self,
        addr: SocketAddr,
        payload: Vec<u8>,
        msg_type: &'static str,
    ) {
        self.worker_network
            .send_typed(addr, Bytes::from(payload), msg_type)
            .await;
    }

    /// Mechanism A (sender-side lane resume, `vantage::resume`): the run loop's
    /// non-blocking hand-off of `message` (addressed to `peer`) onto the dedicated
    /// resume-sender task's channel (`spawn_resume_sender`) -- the ONLY interaction
    /// `VantageCore`/`SimpleItCore`'s run loop has with Mechanism A's own network
    /// sends, replacing the previous `send_message`-based `send_resume_header`/
    /// `resume::SEND_TIMEOUT` combo entirely (both deleted). `try_send`, NEVER
    /// `.await`ed: fire-and-forget is CORRECT here because resume traffic is
    /// end-to-end retried above this layer already (the requester's own backoff
    /// re-asks an unanswered gap; the author's own `resume::ResumeServe` dedup
    /// absorbs a duplicate re-serve) -- a dropped enqueue costs one attempt, never
    /// this run loop's own progress on anything else.
    ///
    /// `peer` must resolve in `other_primaries` -- mirrors `send_to`'s identical
    /// resolve-or-no-op contract exactly (a `peer` that isn't a known other primary,
    /// e.g. `self.name`, which `other_primaries` never contains by construction, is
    /// silently skipped, same as `send_to` already does).
    pub(crate) fn enqueue_resume(&self, peer: PublicKey, message: PrimaryMessage) {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return;
        };
        if self
            .resume_lane_tx
            .try_send(LaneSend(addr, message))
            .is_err()
        {
            // `Full` (the sender task fell behind draining a backed-up
            // destination) or `Closed` (the task itself is gone -- in practice
            // only reachable if it panicked; the channel's one live `Sender`
            // lives on this `Wire`, tied to this node's own lifetime). Either
            // way, this one resume message did not go out on this attempt --
            // Mechanism A's own end-to-end retry (`resume::ResumeTrigger`'s
            // backoff, `resume::ResumeServe`'s dedup) is what recovers it, not
            // a second attempt from here.
            if let Some(metrics) = &self.metrics {
                metrics.vantage_lane_resume_send_drops.inc();
            }
        }
    }

    /// reconnect-replay plan §5/§6/§14 A2: non-blocking hand-off of a replay stream
    /// (`msgs` -- possibly empty, see `VantageCore`'s `Inbound::ResumeHello` handling
    /// for when that's legitimate -- plus the terminating `done`) onto the
    /// resume-sender task's channel; mirrors `enqueue_resume`'s identical
    /// fire-and-forget `try_send` contract, this time for v3's own durable replay
    /// shape rather than Mechanism A's `Message` shape. Returns whether the enqueue
    /// succeeded: audit-3 A2 makes this the caller's (`VantageCore`'s) gate for
    /// whether `pending_low`/the in-flight map may be updated at all -- "iff the
    /// `try_send` ... returned Ok; on Err, ... leave `pending_low` untouched".
    pub(crate) fn enqueue_replay(
        &self,
        peer: PublicKey,
        generation: u64,
        msgs: Vec<Bytes>,
        done: PrimaryMessage,
    ) -> bool {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return false;
        };
        let ok = self.replay_tx.try_send(ReplaySend {
            to: addr,
            peer,
            generation,
            msgs,
            done,
            reserved_bytes: 0,
        });
        if !ok {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_replay_enqueue_drops_total.inc();
            }
        }
        ok
    }

    /// Allocates the next replay-stream generation. Exhausting all `u64`
    /// generations in one process lifetime is treated as fatal rather than wrapping
    /// and making a stale completion indistinguishable from a current stream.
    pub(crate) fn next_replay_generation(&self) -> u64 {
        self.replay_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("replay generation exhausted")
    }

    /// Mechanism A: the author-side counterpart -- same withholding-fidelity gate
    /// `broadcast_message`'s `Header(_, false)` arm consults (`withheld_header_dests`
    /// together with `config::withhold_active`), applied HERE, at enqueue time in
    /// the run loop (where `withheld_header_dests`/`withhold_window` live -- the
    /// sender task `enqueue_resume` hands off to carries neither field, and must
    /// not): a withholding sender mid-window must not resume-serve its own blocked
    /// half either -- `--withhold` models a sender that never gets this data to
    /// that half AT ALL during the window, and a resume batch is still that same
    /// data, merely addressed differently on the wire. `withheld_header_dests`'s
    /// second component (`full`, this node's ALLOWED, i.e. non-blocked,
    /// destinations) is what's consulted -- `peer` not appearing in it means
    /// `peer` is in this node's blocked half.
    ///
    /// Same wire encoding as a fresh publish (`Header(_, false)`), just unicast
    /// instead of broadcast -- so receipt is DirectPub/ack-eligible through the
    /// existing publish path exactly as a broadcast publish would be.
    pub(crate) fn enqueue_resume_header(&self, peer: PublicKey, header: Header) {
        if let Some((_, allowed)) = &self.withheld_header_dests {
            let peer_allowed = allowed.iter().any(|(pk, _)| *pk == peer);
            if !peer_allowed
                && config::withhold_active(self.withhold_window.as_deref(), Instant::now())
            {
                return;
            }
        }
        self.enqueue_resume(peer, PrimaryMessage::Header(header, false));
    }

    /// Small accessor for `VantageCore::sync_batches`/`notify_committed` (which stay on
    /// `VantageCore` -- they touch `pending_payload`/`store`/`tx_payload_ready`/
    /// `tx_output`, protocol state rather than wire state) to resolve a worker's
    /// address now that `worker_addresses` lives here.
    pub(crate) fn worker_addr(&self, worker_id: WorkerId) -> Option<SocketAddr> {
        self.worker_addresses.get(&worker_id).copied()
    }
}

#[derive(Debug)]
pub(crate) struct LaneSend(SocketAddr, PrimaryMessage);

/// SEQUENCE-CHECKPOINT-SYNC-PLAN.md section 6.1: one state-sync frame for the dedicated
/// sender task. Same shape as `LaneSend`, deliberately a distinct type so state-sync
/// traffic can never be enqueued onto lane/replay capacity by mistake.
pub(crate) struct SequenceSend(pub(crate) SocketAddr, pub(crate) PrimaryMessage);

/// One independently admitted durable replay stream. `reserved_bytes` is populated
/// by `ReplaySender::try_send` and remains reserved across both queued and active
/// states, until all replay frames and `done` have been handed to `ReliableSender`.
#[derive(Debug)]
pub(crate) struct ReplaySend {
    pub(crate) to: SocketAddr,
    pub(crate) peer: PublicKey,
    pub(crate) generation: u64,
    pub(crate) msgs: Vec<Bytes>,
    pub(crate) done: PrimaryMessage,
    pub(crate) reserved_bytes: usize,
}

/// Replay ingress with two independent resource bounds: a small channel slot cap and
/// a shared logical-byte reservation covering queued plus active streams. One stream
/// larger than the normal byte cap is accepted only when it is the sole reservation.
#[derive(Clone)]
pub(crate) struct ReplaySender {
    tx: mpsc::Sender<ReplaySend>,
    reserved_bytes: Arc<AtomicUsize>,
    max_reserved_bytes: usize,
}

impl ReplaySender {
    pub(crate) fn channel(max_reserved_bytes: usize) -> (Self, mpsc::Receiver<ReplaySend>) {
        let (tx, rx) = mpsc::channel(REPLAY_SEND_CHANNEL_CAPACITY);
        (
            Self {
                tx,
                reserved_bytes: Arc::new(AtomicUsize::new(0)),
                max_reserved_bytes: max_reserved_bytes.max(1),
            },
            rx,
        )
    }

    fn try_send(&self, mut item: ReplaySend) -> bool {
        let reserved_bytes = replay_reserved_size(&item.msgs, &item.done);
        let reserved =
            self.reserved_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    if current == 0 && reserved_bytes > self.max_reserved_bytes {
                        Some(reserved_bytes)
                    } else {
                        current
                            .checked_add(reserved_bytes)
                            .filter(|next| *next <= self.max_reserved_bytes)
                    }
                });
        if reserved.is_err() {
            return false;
        }

        item.reserved_bytes = reserved_bytes;
        if self.tx.try_send(item).is_err() {
            self.reserved_bytes
                .fetch_sub(reserved_bytes, Ordering::AcqRel);
            return false;
        }
        true
    }
}

fn replay_reserved_size(msgs: &[Bytes], done: &PrimaryMessage) -> usize {
    let payload_bytes = msgs
        .iter()
        .fold(0usize, |total, msg| total.saturating_add(msg.len()));
    let done_bytes =
        usize::try_from(bincode::serialized_size(done).expect("serializes")).unwrap_or(usize::MAX);
    payload_bytes.saturating_add(done_bytes).max(1)
}

/// reconnect-replay plan §5: one active replay stream's remaining, not-yet-sent
/// chunks -- the resume task's own round-robin queue entry. `msgs` shrinks from the
/// front as `run_replay_sender`'s ticker sends chunks; the stream is done (its
/// `done` frame sent, `peer` removed from the in-flight map) once it's empty.
struct ReplayStream {
    to: SocketAddr,
    peer: PublicKey,
    generation: u64,
    msgs: VecDeque<Bytes>,
    done: PrimaryMessage,
    reserved_bytes: usize,
}

impl From<ReplaySend> for ReplayStream {
    fn from(item: ReplaySend) -> Self {
        Self {
            to: item.to,
            peer: item.peer,
            generation: item.generation,
            msgs: item.msgs.into(),
            done: item.done,
            reserved_bytes: item.reserved_bytes,
        }
    }
}

/// Mechanism A (sender-side lane resume, `vantage::resume`): capacity of
/// `spawn_resume_sender`'s own channel. Sized to absorb a full windowed-withhold
/// recovery burst without forcing every enqueue onto `Wire::enqueue_resume`'s drop
/// path: the measured loop-starvation defect this whole task exists to fix served
/// ~600-header backlogs to ~10 requesters per author (~6.4k unicasts committee-wide)
/// entirely synchronously on the run loop; 4096 comfortably covers one node's own
/// share of that burst (never the whole committee's traffic through one instance's
/// one channel -- every node has its own `Wire`, hence its own channel) while
/// staying a small, fixed amount of memory. Replay has an independent 64-slot and
/// logical-byte bound, so lane congestion cannot consume replay capacity. A full
/// lane channel is never a correctness
/// bug, only a liveness hiccup: `enqueue_resume`'s `try_send` failing drops one
/// message, which Mechanism A's own end-to-end retry (`resume::ResumeTrigger`'s
/// backoff-driven resend, `resume::ResumeServe`'s dedup covering a redundant
/// re-serve) recovers on a later attempt.
/// Section 6.1's `sequence_sync_inbound_capacity` sibling on the EGRESS side. Small on
/// purpose: a peer that cannot keep up must cost us dropped frames, never loop progress.
const SEQUENCE_SEND_CHANNEL_CAPACITY: usize = 256;

const RESUME_LANE_CHANNEL_CAPACITY: usize = 4096;
const REPLAY_SEND_CHANNEL_CAPACITY: usize = 64;

pub(crate) struct ResumeSenders {
    pub(crate) lane: mpsc::Sender<LaneSend>,
    pub(crate) replay: ReplaySender,
    pub(crate) sequence: mpsc::Sender<SequenceSend>,
    pub(crate) generation: AtomicU64,
}

/// Builds this node's two isolated resume-sender tasks and returns their separate
/// bounded ingress handles. Called once, at `VantageCore::build`/`SimpleItCore::build`
/// time, with the
/// SAME `latency_map`/`batch`/`metrics` values those constructors hand `network`/
/// `worker_network` (identical configuration convention) -- but these are
/// DELIBERATELY SEPARATE sender instances (their own connection pools), so a resume
/// destination's connection state never touches `network`'s (primary<->primary AGB/
/// consensus traffic) or `worker_network`'s (primary<->worker) state, or
/// `VantageCore`/`SimpleItCore`'s run loop, ever again once spawned. `in_flight` is
/// `Wire::in_flight`'s own `Arc` clone -- this is the task's remove-after-`Done` side
/// of it (see `InFlightMap`'s own doc comment).
///
/// TWO senders now, not one (reconnect-replay plan §5/§7): `messages` (`SimpleSender`,
/// unchanged -- fire-and-forget is CORRECT for Mechanism A traffic specifically, since
/// it is end-to-end retried ABOVE this layer already: the requester's own backoff
/// re-requests an unanswered gap, the author's own `resume::ResumeServe` dedup absorbs
/// a duplicate re-serve) and `replay` (a NEW, durable-only `ReliableSender` -- v3's own
/// replay/Done frames must survive a session death mid-stream, which is exactly what
/// makes raising `pending_low` at enqueue time sound; audit-3 A2 forbids it from ever
/// sending volatile). Neither attaches `with_reconnect_events`/`with_drop_map` (A7:
/// "apply to the MAIN pool only -- the replay pool feeds neither").
///
/// KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`):
/// UNLIKE `with_reconnect_events`/`with_drop_map` above, the reconnect-waiter
/// backoff cap is NOT MAIN-pool-only -- `replay` gets it too (see this fn's own
/// `retry_backoff_max_ms` parameter), since the cap is a plain transport
/// property, orthogonal to KNOB 1's reconnect-replay mechanism (which stays
/// MAIN-pool-only): every `ReliableSender` this node spawns should reconnect
/// under the SAME cap for the ablation to be uniform.
///
/// The lane task may block on its own `SimpleSender` without delaying replay. The
/// replay task owns the one global ticker and only uses detached-durable sends.
///
/// reconnect-replay plan §14 A9: a panic in this task is a PERMANENT-LOSS event for
/// every `pending_low` span it had already raised on the strength of an enqueue that
/// (from `VantageCore`'s side) looked successful -- unlike Mechanism A's own
/// `Message` traffic (v1's design: end-to-end retried above this layer, so a crashed
/// sender task merely stalls it until Mechanism A's own backoff notices and re-asks),
/// a `Replay` stream this task never gets to finish sending has no other path to
/// delivery. This is the SAME acceptance class as every other detached `tokio::spawn`
/// task in this codebase already carries (a panic here is as fatal as one in
/// `network::Connection::run`), just with a different, now user-visible symptom:
/// `vantage_replay_pending_low_nudges_total` climbing without bound as the nudge loop
/// keeps re-asking a `pending_low` that nothing is left alive to ever answer.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_resume_sender(
    latency_map: HashMap<SocketAddr, Duration>,
    batch: BatchConfig,
    metrics: Option<Arc<Metrics>>,
    in_flight: InFlightMap,
    chunk_bytes: usize,
    chunk_interval_ms: u64,
    replay_serve_max_bytes: usize,
    // KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`):
    // this pool's own `replay` `ReliableSender` below is a SEPARATE connection pool
    // from the main one (`VantageCore`/`SimpleItCore`'s own `Wire::network`) --
    // threaded through here too so the cap applies uniformly to every
    // `ReliableSender` this node spawns, not just the main pool.
    retry_backoff_max_ms: u64,
) -> ResumeSenders {
    let (lane_tx, lane_rx) = mpsc::channel(RESUME_LANE_CHANNEL_CAPACITY);
    let max_reserved_bytes = replay_serve_max_bytes.saturating_mul(2).max(1);
    let (replay_tx, replay_rx) = ReplaySender::channel(max_reserved_bytes);
    // Cloned up front: `latency_map`/`metrics` are moved into the lane and replay
    // senders below.
    let sequence_latency = latency_map.clone();
    let sequence_metrics = metrics.clone();
    let mut messages = SimpleSender::new()
        .with_latency(latency_map.clone())
        .with_batching(batch);
    let mut replay = ReliableSender::new()
        .with_latency(latency_map)
        .with_batching(batch)
        .with_retry_backoff_max_ms(retry_backoff_max_ms);
    if let Some(m) = metrics {
        messages = messages.with_metrics(m.clone());
        replay = replay.with_metrics(m);
    }
    let chunk_interval = Duration::from_millis(chunk_interval_ms.max(1));
    tokio::spawn(run_lane_sender(lane_rx, messages));
    tokio::spawn(run_replay_sender(
        replay_rx,
        replay,
        in_flight,
        replay_tx.reserved_bytes.clone(),
        chunk_bytes.max(1),
        chunk_interval,
    ));
    // Section 6.1: bounded ingress, its own connection pool. Capacity is small on
    // purpose -- overflow DROPS the newest frame rather than blocking, because
    // state-sync responses are idempotent and re-requestable, so a drop costs one retry
    // while blocking would propagate backpressure into the transport and stall live
    // consensus. This is the same best-effort discipline the lane sender uses.
    let (sequence_tx, sequence_rx) = mpsc::channel(SEQUENCE_SEND_CHANNEL_CAPACITY);
    let mut sequence_messages = SimpleSender::new()
        .with_latency(sequence_latency)
        .with_batching(batch);
    if let Some(m) = sequence_metrics {
        sequence_messages = sequence_messages.with_metrics(m);
    }
    tokio::spawn(run_sequence_sender(sequence_rx, sequence_messages));
    ResumeSenders {
        lane: lane_tx,
        replay: replay_tx,
        sequence: sequence_tx,
        generation: AtomicU64::new(1),
    }
}

/// Mechanism A's isolated best-effort sender. Closing the ingress drains every item
/// already accepted by the bounded channel, then exits naturally.
/// The dedicated state-sync sender.
///
/// Owns its OWN `SimpleSender`, i.e. a connection pool separate from `Wire::network`.
/// This is a correctness requirement of the design, not tuning: the mechanism exists to
/// relieve a node whose MAIN inbound/outbound path is already saturated, so serving
/// recovery traffic through that same path would deepen the exact congestion it is meant
/// to drain, and a 32 KB chunk would sit ahead of live consensus frames.
async fn run_sequence_sender(mut rx: mpsc::Receiver<SequenceSend>, mut messages: SimpleSender) {
    while let Some(SequenceSend(to, message)) = rx.recv().await {
        let msg_type = message.type_name();
        let bytes = bincode::serialize(&message).expect("serializes");
        messages.send_typed(to, Bytes::from(bytes), msg_type).await;
    }
}

async fn run_lane_sender(mut rx: mpsc::Receiver<LaneSend>, mut messages: SimpleSender) {
    while let Some(LaneSend(to, message)) = rx.recv().await {
        let msg_type = message.type_name();
        let bytes = bincode::serialize(&message).expect("serializes");
        messages.send_typed(to, Bytes::from(bytes), msg_type).await;
    }
}

/// The dedicated replay task, fed only by replay ingress. Accepted streams join
/// `streams` (a `VecDeque`, round-robin by construction -- a stream that isn't yet
/// fully drained is rotated to the BACK after each chunk, so no single stream can
/// monopolize the task); a `chunk_interval`-paced ticker sends exactly one
/// `chunk_bytes` chunk from the FRONT stream per rotation (a single constituent
/// message larger than the whole chunk cap is still sent whole -- the same
/// documented, bounded overshoot `outbox::Outbox`'s own byte-cap eviction and
/// `resume::ServeBudget`'s own per-key truncation already accept, never split
/// mid-message).
///
/// A biased selection gives a due tick priority over ready replay ingress. Only one
/// stream is admitted per receive iteration; exactly one front stream contributes one
/// chunk per tick. `MissedTickBehavior::Delay` prevents catch-up bursts, while Tokio's
/// intentional immediate first tick starts a newly admitted recovery promptly.
///
/// Closing ingress disables that receive branch; every already accepted stream is
/// drained through `Done` before the task exits, without polling a closed receiver.
///
/// Adversarial-audit FINDING 2 (BLOCKER): both sends in the chunk-tick arm below use
/// `ReliableSender::send_detached_typed`, NEVER `send_typed`. `send_typed` returns a
/// `CancelHandler` (`oneshot::Receiver<Bytes>`); this task has never bound it to
/// anything, so it used to drop at the end of that one statement -- and dropping a
/// `Receiver` flips its paired `Sender::is_closed()` true IMMEDIATELY, which made
/// `all_closed` (network's own A1-guarded predicate) report `true` for that entry at
/// EVERY pre-transmission checkpoint (`network::Connection`'s pre-send skip,
/// delayed-pop skip, waiter retain) between the moment `send_typed` returned and the
/// moment the frame actually left the socket -- a window batching alone widens to up
/// to 5ms. A `Replay` chunk or `Done` frame could therefore vanish AFTER `pending_
/// low` was already raised (or cleared, for `Done`) on the strength of it having
/// been "sent": silent, permanent, one-shot loss -- the exact B1 class v3 exists to
/// exclude. `send_detached_typed` never allocates that `oneshot::channel` at all, so
/// there is nothing to drop and nothing for `all_closed` to see as closed (its
/// `ReplyTargets` is empty, which the A1 guard already treats as never-cancellable);
/// it is durable-requeued across reconnects exactly like an ordinary `send_typed`
/// call whose handler the caller correctly kept alive.
async fn run_replay_sender(
    mut rx: mpsc::Receiver<ReplaySend>,
    mut replay: ReliableSender,
    in_flight: InFlightMap,
    reserved_bytes: Arc<AtomicUsize>,
    chunk_bytes: usize,
    chunk_interval: Duration,
) {
    let mut streams: VecDeque<ReplayStream> = VecDeque::new();
    let mut ticker = tokio::time::interval(chunk_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ingress_open = true;
    loop {
        if !ingress_open && streams.is_empty() {
            return;
        }
        match next_replay_event(&mut rx, &mut ticker, !streams.is_empty(), ingress_open).await {
            ReplayEvent::Tick => {
                let Some(mut stream) = streams.pop_front() else {
                    continue;
                };
                for bytes in take_replay_chunk(&mut stream, chunk_bytes) {
                    // Adversarial-audit FINDING 2 (BLOCKER): `send_detached_typed`,
                    // never `send_typed` -- see this fn's own doc comment for why a
                    // discarded `CancelHandler` here was silently losing frames
                    // pre-transmission.
                    replay.send_detached_typed(stream.to, bytes, "Replay").await;
                }
                if stream.msgs.is_empty() {
                    let msg_type = stream.done.type_name();
                    let done_bytes = bincode::serialize(&stream.done).expect("serializes");
                    replay
                        .send_detached_typed(stream.to, Bytes::from(done_bytes), msg_type)
                        .await;
                    complete_replay_stream(&in_flight, &reserved_bytes, &stream);
                } else {
                    streams.push_back(stream);
                }
            }
            ReplayEvent::Ingress(maybe) => match maybe {
                Some(item) => streams.push_back((*item).into()),
                None => ingress_open = false,
            },
        }
    }
}

enum ReplayEvent {
    Tick,
    Ingress(Option<Box<ReplaySend>>),
}

/// Chooses one scheduler action. A due global tick wins over ready ingress, and each
/// ingress event admits exactly one stream. The caller disables ingress after `None`.
async fn next_replay_event(
    rx: &mut mpsc::Receiver<ReplaySend>,
    ticker: &mut tokio::time::Interval,
    has_streams: bool,
    ingress_open: bool,
) -> ReplayEvent {
    tokio::select! {
        biased;

        _ = ticker.tick(), if has_streams => ReplayEvent::Tick,
        item = rx.recv(), if ingress_open => ReplayEvent::Ingress(item.map(Box::new)),
    }
}

fn take_replay_chunk(stream: &mut ReplayStream, chunk_bytes: usize) -> Vec<Bytes> {
    let mut chunk = Vec::new();
    let mut sent = 0usize;
    while sent < chunk_bytes {
        let Some(bytes) = stream.msgs.pop_front() else {
            break;
        };
        sent = sent.saturating_add(bytes.len());
        chunk.push(bytes);
    }
    chunk
}

fn complete_replay_stream(
    in_flight: &InFlightMap,
    reserved_bytes: &AtomicUsize,
    stream: &ReplayStream,
) {
    remove_in_flight_generation(in_flight, stream.peer, stream.generation);
    reserved_bytes.fetch_sub(stream.reserved_bytes, Ordering::AcqRel);
}

/// Removes only the stream generation that actually completed or failed admission.
/// A stale generation can never clear a newer stream installed for the same peer.
pub(crate) fn remove_in_flight_generation(
    in_flight: &InFlightMap,
    peer: PublicKey,
    generation: u64,
) -> bool {
    let mut guard = in_flight.lock();
    if guard
        .get(&peer)
        .is_some_and(|entry| entry.generation == generation)
    {
        guard.remove(&peer);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    fn key(index: usize) -> PublicKey {
        crate::common::keys()[index].0
    }

    fn replay_item(peer: PublicKey, generation: u64, payloads: &[&'static [u8]]) -> ReplaySend {
        ReplaySend {
            to: "127.0.0.1:9".parse().unwrap(),
            peer,
            generation,
            msgs: payloads
                .iter()
                .map(|payload| Bytes::from_static(payload))
                .collect(),
            done: PrimaryMessage::VantageReplayDone(1, true, false, peer),
            reserved_bytes: 0,
        }
    }

    #[test]
    fn replay_byte_cap_covers_queued_and_active_until_completion() {
        let peer = key(0);
        let first = replay_item(peer, 1, &[b"payload"]);
        let footprint = replay_reserved_size(&first.msgs, &first.done);
        let (sender, mut rx) = ReplaySender::channel(footprint);

        assert!(sender.try_send(first));
        assert!(
            !sender.try_send(replay_item(key(1), 2, &[b"x"])),
            "the CAS reservation must reject work beyond the byte cap"
        );

        let stream = ReplayStream::from(rx.try_recv().unwrap());
        assert_eq!(
            sender.reserved_bytes.load(Ordering::Acquire),
            footprint,
            "receiving a stream must not refund its active reservation"
        );

        let in_flight = Arc::new(Mutex::new(HashMap::from([(
            peer,
            InFlightEntry {
                started: Instant::now(),
                generation: 1,
            },
        )])));
        complete_replay_stream(&in_flight, &sender.reserved_bytes, &stream);
        assert_eq!(sender.reserved_bytes.load(Ordering::Acquire), 0);
        assert!(!in_flight.lock().contains_key(&peer));
    }

    #[test]
    fn failed_replay_channel_send_refunds_reservation_immediately() {
        let (sender, rx) = ReplaySender::channel(usize::MAX);
        drop(rx);

        assert!(!sender.try_send(replay_item(key(0), 1, &[b"payload"])));
        assert_eq!(sender.reserved_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn one_oversized_stream_is_admitted_only_when_alone() {
        let (sender, mut rx) = ReplaySender::channel(1);
        let first_peer = key(0);
        assert!(sender.try_send(replay_item(first_peer, 1, &[b"oversized"])));
        assert!(!sender.try_send(replay_item(key(1), 2, &[b"also oversized"])));

        let stream = ReplayStream::from(rx.try_recv().unwrap());
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        complete_replay_stream(&in_flight, &sender.reserved_bytes, &stream);

        assert!(
            sender.try_send(replay_item(key(1), 2, &[b"also oversized"])),
            "completion must release the sole oversized reservation"
        );
    }

    #[test]
    fn saturated_lane_ingress_cannot_block_replay_admission() {
        let (lane_tx, _lane_rx) = mpsc::channel(1);
        assert!(lane_tx
            .try_send(LaneSend(
                "127.0.0.1:9".parse().unwrap(),
                PrimaryMessage::VantageReplayDone(1, true, false, key(0))
            ))
            .is_ok());
        assert!(lane_tx
            .try_send(LaneSend(
                "127.0.0.1:9".parse().unwrap(),
                PrimaryMessage::VantageReplayDone(1, true, false, key(0))
            ))
            .is_err());

        let (replay_tx, mut replay_rx) = ReplaySender::channel(usize::MAX);
        assert!(replay_tx.try_send(replay_item(key(1), 1, &[b"replay"])));
        assert!(replay_rx.try_recv().is_ok());
    }

    #[test]
    fn replay_chunks_rotate_round_robin() {
        let mut streams = VecDeque::from([
            ReplayStream::from(replay_item(key(0), 1, &[b"a1", b"a2"])),
            ReplayStream::from(replay_item(key(1), 2, &[b"b1", b"b2"])),
        ]);
        let mut order = Vec::new();

        while let Some(mut stream) = streams.pop_front() {
            order.extend(take_replay_chunk(&mut stream, 1));
            if !stream.msgs.is_empty() {
                streams.push_back(stream);
            }
        }

        assert_eq!(
            order,
            vec![
                Bytes::from_static(b"a1"),
                Bytes::from_static(b"b1"),
                Bytes::from_static(b"a2"),
                Bytes::from_static(b"b2"),
            ]
        );
    }

    #[test]
    fn stale_completion_cannot_clear_a_new_generation() {
        let peer = key(0);
        let in_flight = Arc::new(Mutex::new(HashMap::from([(
            peer,
            InFlightEntry {
                started: Instant::now(),
                generation: 2,
            },
        )])));

        assert!(!remove_in_flight_generation(&in_flight, peer, 1));
        assert_eq!(in_flight.lock()[&peer].generation, 2);
        assert!(remove_in_flight_generation(&in_flight, peer, 2));
        assert!(!in_flight.lock().contains_key(&peer));
    }

    #[tokio::test(start_paused = true)]
    async fn due_tick_has_priority_and_first_tick_is_immediate() {
        let (sender, mut rx) = ReplaySender::channel(usize::MAX);
        assert!(sender.try_send(replay_item(key(0), 1, &[b"first"])));
        let mut ticker = tokio::time::interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        assert!(matches!(
            next_replay_event(&mut rx, &mut ticker, false, true).await,
            ReplayEvent::Ingress(Some(_))
        ));
        assert!(sender.try_send(replay_item(key(1), 2, &[b"second"])));
        assert!(matches!(
            next_replay_event(&mut rx, &mut ticker, true, true).await,
            ReplayEvent::Tick
        ));
        assert!(
            rx.try_recv().is_ok(),
            "the ready ingress item must remain queued when the due tick wins"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn replay_ticker_uses_delay_after_a_missed_tick() {
        let mut ticker = tokio::time::interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        assert!(
            ticker.tick().now_or_never().is_some(),
            "first tick is immediate"
        );

        tokio::time::advance(Duration::from_millis(35)).await;
        assert!(ticker.tick().now_or_never().is_some());
        assert!(ticker.tick().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(9)).await;
        assert!(ticker.tick().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(ticker.tick().now_or_never().is_some());
    }

    #[tokio::test]
    async fn closed_replay_ingress_drains_all_accepted_streams() {
        let (sender, rx) = ReplaySender::channel(usize::MAX);
        let reserved_bytes = sender.reserved_bytes.clone();
        let first = key(0);
        let second = key(1);
        assert!(sender.try_send(replay_item(first, 1, &[])));
        assert!(sender.try_send(replay_item(second, 2, &[])));
        let in_flight = Arc::new(Mutex::new(HashMap::from([
            (
                first,
                InFlightEntry {
                    started: Instant::now(),
                    generation: 1,
                },
            ),
            (
                second,
                InFlightEntry {
                    started: Instant::now(),
                    generation: 2,
                },
            ),
        ])));
        drop(sender);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_replay_sender(
                rx,
                ReliableSender::new(),
                in_flight.clone(),
                reserved_bytes.clone(),
                1,
                Duration::from_millis(1),
            ),
        )
        .await
        .expect("closed ingress must drain and exit");

        assert!(in_flight.lock().is_empty());
        assert_eq!(reserved_bytes.load(Ordering::Acquire), 0);
    }
}
