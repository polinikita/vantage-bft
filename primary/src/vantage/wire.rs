// Network/wire-transport helpers for `VantageCore`, split out of `vantage::node` so a
// second consensus protocol can reuse the same primary<->primary/primary<->worker wire
// machinery (cancel-handler bookkeeping, broadcast/unicast dispatch) without depending
// on Vantage's own protocol state (`agb`, `frontier`, `cursor`, `pacemaker`, `resolver`,
// `control`). Pure code motion out of `node.rs`: every field and method below is
// unchanged from its previous home on `VantageCore`, aside from the `self.` ->
// `self.wire.`-style re-pointing the split forces at `VantageCore`'s own call sites
// (see `node.rs` itself for those).

use crate::messages::Header;
use crate::primary::PrimaryMessage;
use crate::vantage::node::Inbound;
use bytes::Bytes;
use config::WorkerId;
use crypto::PublicKey;
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, ReliableSender, SimpleSender};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot::error::TryRecvError;

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
    /// `VantageCore`/`SimpleItCore`'s run loop's.
    pub(crate) resume_tx: mpsc::Sender<(SocketAddr, PrimaryMessage)>,
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
}

impl Wire {
    /// P4-3: drop every cancel handler that has already resolved (message ack'd) or
    /// closed (connection gone, will never resolve) -- keeps only the ones
    /// `ReliableSender` may still be actively retrying, so the retry-until-ack
    /// semantics are unaffected (`Connection::keep_alive` treats a dropped receiver's
    /// closed sender as cancellation, per `network::reliable_sender`, so we must never
    /// drop one that's genuinely still pending).
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
    async fn broadcast_to(&mut self, payload: Vec<u8>, msg_type: &'static str, addrs: Vec<SocketAddr>) {
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
        if self.resume_tx.try_send((addr, message)).is_err() {
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
/// re-serve) recovers on a later attempt.
const RESUME_SEND_CHANNEL_CAPACITY: usize = 4096;

/// Mechanism A (sender-side lane resume, `vantage::resume`): builds this node's ONE
/// dedicated resume-sender task and returns the `mpsc::Sender` end `Wire::
/// enqueue_resume`/`enqueue_resume_header` `try_send` onto. Called once, at
/// `VantageCore::build`/`SimpleItCore::build` time, with the SAME `latency_map`/
/// `batch`/`metrics` values those constructors hand `network`/`worker_network`
/// (identical configuration convention) -- but this is a DELIBERATELY SEPARATE
/// `SimpleSender` instance (its own connection pool), so a resume destination's
/// connection state never touches `network`'s (primary<->primary AGB/consensus
/// traffic) or `worker_network`'s (primary<->worker) state, or
/// `VantageCore`/`SimpleItCore`'s run loop, ever again once spawned.
///
/// Fire-and-forget (`SimpleSender`, not `ReliableSender`) is CORRECT for resume
/// traffic specifically, unlike most of this node's other unicast traffic: it is
/// end-to-end retried ABOVE this layer already (the requester's own backoff
/// re-requests an unanswered gap; the author's own `resume::ResumeServe` dedup
/// absorbs a duplicate re-serve), so `ReliableSender`'s retry-until-ack machinery
/// would only add redundant bookkeeping for a guarantee this mechanism does not need.
///
/// The task this spawns (`run_resume_sender`) is free to block/await on every single
/// send -- that IS its entire reason to exist: letting one slow destination cost
/// only this task's own progress, never the run loop that used to make this exact
/// send inline (the loop-starvation defect `resume::SEND_TIMEOUT`, now deleted, used
/// to merely bound the damage from, rather than eliminate).
pub(crate) fn spawn_resume_sender(
    latency_map: HashMap<SocketAddr, Duration>,
    batch: BatchConfig,
    metrics: Option<Arc<Metrics>>,
) -> mpsc::Sender<(SocketAddr, PrimaryMessage)> {
    let (tx, rx) = mpsc::channel(RESUME_SEND_CHANNEL_CAPACITY);
    let mut sender = SimpleSender::new()
        .with_latency(latency_map)
        .with_batching(batch);
    if let Some(m) = metrics {
        sender = sender.with_metrics(m);
    }
    tokio::spawn(run_resume_sender(rx, sender));
    tx
}

/// Mechanism A: the dedicated off-run-loop task itself, fed by `spawn_resume_
/// sender`'s channel. One iteration: recv -> serialize -> `SimpleSender::send_typed`.
/// Every step here may block/await freely -- see `spawn_resume_sender`'s doc comment
/// for why that is this task's entire job rather than a bug. Ends the moment `tx`'s
/// one live clone (held by `Wire`) drops -- `rx.recv()` then returns `None` and this
/// loop, and the task with it, ends; nothing joins it explicitly, mirroring every
/// other detached `tokio::spawn` in this codebase (e.g. `network::Connection::spawn`).
async fn run_resume_sender(
    mut rx: mpsc::Receiver<(SocketAddr, PrimaryMessage)>,
    mut sender: SimpleSender,
) {
    while let Some((addr, message)) = rx.recv().await {
        let msg_type = message.type_name();
        let bytes = bincode::serialize(&message).expect("serializes");
        sender.send_typed(addr, Bytes::from(bytes), msg_type).await;
    }
}
