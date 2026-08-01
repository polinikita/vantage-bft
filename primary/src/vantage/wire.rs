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
// map`/`in_flight` handles, and the resume-sender task's `ResumeSend::Replay`
// scheduling (see that enum and `spawn_resume_sender`'s own doc comments for the
// full design). Audit-3 A9, stated once here for the whole subsystem: unlike
// Mechanism A's own `Message` traffic (retried end-to-end ABOVE this layer, so a
// crashed sender task only stalls it), a panic in the resume-sender task is a
// PERMANENT-LOSS event for every `pending_low` span already raised on the strength
// of an enqueue that looked successful -- the same acceptance class as any other
// detached `tokio::spawn` task in this codebase, just with a distinct, user-visible
// symptom (`vantage_replay_pending_low_nudges_total` climbing without bound).
//
// Adversarial-audit FINDING 2: `Replay`/`Done` frames ride `network::ReliableSender::
// send_detached_typed` (detached-durable -- see `run_resume_sender`'s own doc comment
// for why they must never carry a `CancelHandler` this task would just drop). Durable
// means requeued forever against a peer that never reconnects, but this never grows
// unbounded the way a naive "durable, no backpressure" send might: detached-durable
// frames toward a single dead peer accumulate bounded by exactly one in-flight,
// budget-capped replay stream (`replay_serve_max_bytes`, <= 8MB) plus that stream's
// own terminating `Done`, because every serve is Hello-GATED (`Inbound::ResumeHello`
// is what starts a stream at all -- see `VantageCore::on_resume_hello`'s own in-flight
// check) -- nothing re-serves the SAME peer again while its one stream is still
// marked in-flight, regardless of how long that peer stays unreachable.

use crate::messages::Header;
use crate::primary::PrimaryMessage;
use crate::vantage::node::Inbound;
use bytes::Bytes;
use config::WorkerId;
use crypto::PublicKey;
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, DirtyMap, ReliableSender, SimpleSender};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
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
pub(crate) type InFlightMap = Arc<Mutex<HashMap<PublicKey, Instant>>>;

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
    /// `VantageCore`/`SimpleItCore`'s run loop's. reconnect-replay plan §5/§7: now
    /// carries `ResumeSend` (Mechanism A's own `Message` shape, plus v3's own
    /// `Replay` stream shape) rather than a bare `(SocketAddr, PrimaryMessage)`
    /// tuple -- see `ResumeSend`'s own doc comment.
    pub(crate) resume_tx: mpsc::Sender<ResumeSend>,
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
                let header_bytes = bincode::serialize(header).expect("serializes");
                metrics
                    .proposed_header_size_bytes
                    .observe(header_bytes.len());
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
            .broadcast_volatile_typed(self.other_primary_addrs.clone(), payload, key, msg_type)
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
        let handlers = self
            .network
            .broadcast_typed(
                self.other_primary_addrs.clone(),
                Bytes::from(payload),
                msg_type,
            )
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
            .resume_tx
            .try_send(ResumeSend::Message(addr, message))
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
        let ok = self
            .resume_tx
            .try_send(ResumeSend::Replay {
                to: addr,
                peer,
                msgs,
                done,
            })
            .is_ok();
        if !ok {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_replay_enqueue_drops_total.inc();
            }
        }
        ok
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

/// reconnect-replay plan §5/§7: what the resume-sender task actually transmits.
/// `Message` is Mechanism A's pre-existing shape (a single `PrimaryMessage`, sent
/// fire-and-forget via the task's own `SimpleSender` -- unchanged semantics, unchanged
/// wire encoding). `Replay` is v3's own durable replay-stream shape: `msgs` (already-
/// serialized outbox entries, possibly empty -- see `VantageCore`'s
/// `Inbound::ResumeHello` handling for when an empty slice is legitimate) chunked and
/// sent via the task's OWN `ReliableSender` (durable-only -- audit-3 A2: "never sends
/// volatile"), `done` following as the stream's terminating frame once every chunk
/// has gone out; `peer` is who to remove from the shared in-flight map at that point.
#[derive(Debug)]
pub(crate) enum ResumeSend {
    Message(SocketAddr, PrimaryMessage),
    Replay {
        to: SocketAddr,
        peer: PublicKey,
        msgs: Vec<Bytes>,
        done: PrimaryMessage,
    },
}

/// reconnect-replay plan §5: one active replay stream's remaining, not-yet-sent
/// chunks -- the resume task's own round-robin queue entry. `msgs` shrinks from the
/// front as `run_resume_sender`'s ticker sends chunks; the stream is done (its
/// `done` frame sent, `peer` removed from the in-flight map) once it's empty.
struct ReplayStream {
    to: SocketAddr,
    peer: PublicKey,
    msgs: VecDeque<Bytes>,
    done: PrimaryMessage,
}

/// Mechanism A (sender-side lane resume, `vantage::resume`): capacity of
/// `spawn_resume_sender`'s own channel. Sized to absorb a full windowed-withhold
/// recovery burst without forcing every enqueue onto `Wire::enqueue_resume`'s drop
/// path: the measured loop-starvation defect this whole task exists to fix served
/// ~600-header backlogs to ~10 requesters per author (~6.4k unicasts committee-wide)
/// entirely synchronously on the run loop; 4096 comfortably covers one node's own
/// share of that burst (never the whole committee's traffic through one instance's
/// one channel -- every node has its own `Wire`, hence its own channel) while
/// staying a small, fixed amount of memory. A full channel is never a correctness
/// bug, only a liveness hiccup: `enqueue_resume`'s `try_send` failing drops one
/// message, which Mechanism A's own end-to-end retry (`resume::ResumeTrigger`'s
/// backoff-driven resend, `resume::ResumeServe`'s dedup covering a redundant
/// re-serve) recovers on a later attempt; `enqueue_replay`'s own `try_send` failing
/// is audit-3 A2's own Err arm (leaves `pending_low` untouched, recovered by the
/// next nudge/tick).
const RESUME_SEND_CHANNEL_CAPACITY: usize = 4096;

/// Builds this node's ONE dedicated resume-sender task and returns the `mpsc::Sender`
/// end `Wire::enqueue_resume`/`enqueue_resume_header`/`enqueue_replay` `try_send`
/// onto. Called once, at `VantageCore::build`/`SimpleItCore::build` time, with the
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
/// The task this spawns (`run_resume_sender`) is free to block/await on every single
/// send -- that IS its entire reason to exist: letting one slow destination cost
/// only this task's own progress, never the run loop that used to make this exact
/// send inline (the loop-starvation defect `resume::SEND_TIMEOUT`, now deleted, used
/// to merely bound the damage from, rather than eliminate).
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
    // KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`):
    // this pool's own `replay` `ReliableSender` below is a SEPARATE connection pool
    // from the main one (`VantageCore`/`SimpleItCore`'s own `Wire::network`) --
    // threaded through here too so the cap applies uniformly to every
    // `ReliableSender` this node spawns, not just the main pool.
    retry_backoff_max_ms: u64,
) -> mpsc::Sender<ResumeSend> {
    let (tx, rx) = mpsc::channel(RESUME_SEND_CHANNEL_CAPACITY);
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
    tokio::spawn(run_resume_sender(
        rx,
        messages,
        replay,
        in_flight,
        chunk_bytes.max(1),
        chunk_interval,
    ));
    tx
}

/// Sends one `ResumeSend::Message` immediately via `messages` -- shared by
/// `run_resume_sender`'s blocking-recv and drain-the-rest arms.
async fn send_message_item(messages: &mut SimpleSender, to: SocketAddr, message: PrimaryMessage) {
    let msg_type = message.type_name();
    let bytes = bincode::serialize(&message).expect("serializes");
    messages.send_typed(to, Bytes::from(bytes), msg_type).await;
}

/// The dedicated off-run-loop task itself, fed by `spawn_resume_sender`'s channel.
///
/// Mechanism A's `Message` items are sent IMMEDIATELY, unchanged (see `spawn_resume_
/// sender`'s doc comment). reconnect-replay plan §5: `Replay` items instead join
/// `streams` (a `VecDeque`, round-robin by construction -- a stream that isn't yet
/// fully drained is rotated to the BACK after each chunk, so no single stream can
/// monopolize the task); a `chunk_interval`-paced ticker sends exactly one
/// `chunk_bytes` chunk from the FRONT stream per rotation (a single constituent
/// message larger than the whole chunk cap is still sent whole -- the same
/// documented, bounded overshoot `outbox::Outbox`'s own byte-cap eviction and
/// `resume::ServeBudget`'s own per-key truncation already accept, never split
/// mid-message).
///
/// `select!`'s `biased` `rx.recv()` branch (with a `try_recv` drain loop right behind
/// it) gives `Message` items STRICT priority over the next chunk tick -- module doc,
/// design doc §5: "draining ALL queued v1 Message items between chunks". Two bounds
/// keep either side from starving the other, both worth stating explicitly since
/// neither is enforced by a hard cap in this loop itself: v1 traffic (Mechanism A's
/// own `Message` items) is inherently rate-bounded by ITS OWN in-flight-1/round-trip
/// protocol (`resume::ResumeTrigger`'s in-flight cap of 1 per author), so the priority
/// drain this task gives it is itself rate-bounded, never an unbounded flood; replay
/// throughput is bounded from the other side by `chunk_interval`/`chunk_bytes`
/// (`replay_chunk_bytes / replay_chunk_interval_ms` bytes/s, one task, one bucket, by
/// construction -- design doc §5's "global replay ceiling").
///
/// Ends the moment `tx`'s one live clone (held by `Wire`) drops -- `rx.recv()` then
/// returns `None` and this loop, and the task with it, ends; nothing joins it
/// explicitly, mirroring every other detached `tokio::spawn` in this codebase. See
/// `spawn_resume_sender`'s own doc comment (audit-3 A9) for what a PANIC here means.
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
async fn run_resume_sender(
    mut rx: mpsc::Receiver<ResumeSend>,
    mut messages: SimpleSender,
    mut replay: ReliableSender,
    in_flight: InFlightMap,
    chunk_bytes: usize,
    chunk_interval: Duration,
) {
    let mut streams: VecDeque<ReplayStream> = VecDeque::new();
    let mut ticker = tokio::time::interval(chunk_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;

            maybe = rx.recv() => {
                let Some(item) = maybe else { return };
                match item {
                    ResumeSend::Message(to, message) => send_message_item(&mut messages, to, message).await,
                    ResumeSend::Replay { to, peer, msgs, done } => {
                        streams.push_back(ReplayStream { to, peer, msgs: msgs.into(), done });
                    }
                }
                // Strict priority (module doc): absorb everything ELSE already
                // waiting before this task ever considers the next chunk tick.
                while let Ok(item) = rx.try_recv() {
                    match item {
                        ResumeSend::Message(to, message) => send_message_item(&mut messages, to, message).await,
                        ResumeSend::Replay { to, peer, msgs, done } => {
                            streams.push_back(ReplayStream { to, peer, msgs: msgs.into(), done });
                        }
                    }
                }
            }

            _ = ticker.tick(), if !streams.is_empty() => {
                let Some(mut stream) = streams.pop_front() else { continue };
                let mut sent = 0usize;
                while sent < chunk_bytes {
                    let Some(bytes) = stream.msgs.pop_front() else { break };
                    sent += bytes.len();
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
                    in_flight.lock().unwrap().remove(&stream.peer);
                } else {
                    streams.push_back(stream);
                }
            }
        }
    }
}
