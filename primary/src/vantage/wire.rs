// Network/wire-transport helpers for `VantageCore`, split out of `vantage::node` so a
// second consensus protocol can reuse the same primary<->primary/primary<->worker wire
// machinery (symmetric pairwise-MAC framing, cancel-handler bookkeeping, broadcast/
// unicast dispatch) without depending on Vantage's own protocol state (`agb`,
// `frontier`, `cursor`, `pacemaker`, `resolver`, `control`). Pure code motion out of
// `node.rs`: every field and method below is unchanged from its previous home on
// `VantageCore`, aside from the `self.` -> `self.wire.`-style re-pointing the split
// forces at `VantageCore`'s own call sites (see `node.rs` itself for those).

use crate::messages::Header;
use crate::primary::PrimaryMessage;
use crate::vantage::node::{message_needs_placeholder_tag, Inbound};
use bytes::Bytes;
use config::WorkerId;
use crypto::{PairwiseKeys, PublicKey};
use metrics::Metrics;
use network::{CancelHandler, ReliableSender, SimpleSender};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
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
/// other_primary_addrs`/`Wire::other_primaries` (addresses-only, and full
/// `(PublicKey, SocketAddr)` pairs for the per-destination MAC-tag path), or `None`
/// when this node is not a withholding sender.
pub(crate) type WithheldHeaderDests = Option<(Vec<SocketAddr>, Vec<(PublicKey, SocketAddr)>)>;

/// Network/wire-transport state `VantageCore` owns: the two typed senders, MAC-auth
/// keying, in-flight cancel-handle bookkeeping, and the committee's/our own workers'
/// resolved addresses. Per-field security/perf rationale below is carried over
/// verbatim from `VantageCore`'s previous copy of each field.
pub struct Wire {
    /// This node's own public key -- cloned from `VantageCore::name` (kept there too,
    /// for the rest of `VantageCore`'s own use) since `send_to_worker` needs it to key
    /// the intra-authority `k_{self, self}` MAC tag.
    pub(crate) name: PublicKey,

    pub(crate) network: ReliableSender,
    pub(crate) worker_network: SimpleSender,
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
    pub(crate) channel_auth: Option<Arc<PairwiseKeys>>,
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
    /// build`/`SimpleItCore::build`) -- same `Arc`-clone convention as `network`'s own
    /// `blip_gate`. Consulted (via `config::withhold_active`) in `broadcast_message`'s
    /// `Header(_, false)` arm, ONLY when `withheld_header_dests` is `Some` -- see that
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
        // SECURITY (Fable audit): `Header(_, true)` ("Serve") never legitimately
        // reaches `broadcast_message` (`ServeTo` always unicasts via `send_message`
        // below), so the only D4-class placeholder-tag variant this method ever sees
        // in practice is `ControlServe` -- kept as a real `match`, not an assert, so
        // this stays correct even if a future effect ever does broadcast one of them.
        let placeholder = message_needs_placeholder_tag(&message);
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
            if let Some((addrs, full)) = self.withheld_header_dests.clone() {
                if config::withhold_active(self.withhold_window.as_deref(), Instant::now()) {
                    self.broadcast_to(bytes, msg_type, placeholder, addrs, full)
                        .await;
                    return;
                }
            }
        }
        self.broadcast(bytes, msg_type, placeholder).await;
    }

    /// Shared shape behind every `execute` arm that just serializes a `PrimaryMessage`
    /// and unicasts it verbatim to one peer.
    pub(crate) async fn send_message(&mut self, peer: PublicKey, message: PrimaryMessage) {
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

    /// Data-plane withholding fault injector (`--withhold`): same wire-level contract
    /// as `broadcast` (identical MAC-tag-append logic), but against a caller-supplied
    /// destination list instead of `self.other_primaries`/`self.other_primary_addrs`
    /// -- `addrs`/`full` are those two fields with this node's own blocked half
    /// already removed (`withheld_header_dests`). `broadcast` itself is left
    /// completely untouched (not even refactored to delegate here) so the default,
    /// non-withholding path -- including every non-Header broadcast, which never
    /// reaches this method at all -- keeps its exact original allocation/branch shape.
    async fn broadcast_to(
        &mut self,
        payload: Vec<u8>,
        msg_type: &'static str,
        placeholder: bool,
        addrs: Vec<SocketAddr>,
        full: Vec<(PublicKey, SocketAddr)>,
    ) {
        let Some(auth) = self.channel_auth.clone() else {
            let handlers = self
                .network
                .broadcast_typed(addrs, Bytes::from(payload), msg_type)
                .await;
            self.cancel_handlers.extend(handlers);
            return;
        };
        if placeholder {
            let mut tagged = payload;
            tagged.extend_from_slice(&auth.tag_unverified(&tagged));
            let handlers = self
                .network
                .broadcast_typed(addrs, Bytes::from(tagged), msg_type)
                .await;
            self.cancel_handlers.extend(handlers);
            return;
        }
        for (peer, addr) in full {
            let tag = auth
                .tag_for(&peer, &payload)
                .expect("every withheld-header destination is a committee member");
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
    pub(crate) async fn send_to_worker(
        &mut self,
        addr: SocketAddr,
        payload: Vec<u8>,
        msg_type: &'static str,
    ) {
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

    /// Mechanism A (sender-side lane resume, `vantage::resume`): unicasts our own
    /// original-dissemination header to `peer` as one entry of a resume batch. Same
    /// wire encoding as a fresh publish (`Header(_, false)`, MAC-bound to
    /// `h.author` -- `mac_candidate_sender`'s existing arm for this variant already
    /// covers it unchanged, since this is only ever called by the block's own author
    /// answering a `VantageLaneResume` for its OWN lane, i.e. `h.author ==
    /// self.name`), just unicast instead of broadcast -- so receipt is DirectPub/
    /// ack-eligible through the existing publish path exactly as a broadcast publish
    /// would be. Deliberately routed through `send_message` (genuine per-destination
    /// MAC tag), never through the placeholder-tag path.
    ///
    /// Consults the SAME withholding predicate `broadcast_message`'s `Header(_,
    /// false)` arm does (`withheld_header_dests` + `config::withhold_active`): a
    /// withholding sender mid-window must not resume-serve its own blocked half
    /// either -- `--withhold` models a sender that never gets this data to that half
    /// AT ALL during the window, and a resume batch is still that same data, merely
    /// addressed differently on the wire. `withheld_header_dests`'s second component
    /// (`full`, this node's ALLOWED, i.e. non-blocked, destinations) is what's
    /// consulted -- `peer` not appearing in it means `peer` is in this node's
    /// blocked half.
    pub(crate) async fn send_resume_header(&mut self, peer: PublicKey, header: Header) {
        if let Some((_, allowed)) = &self.withheld_header_dests {
            let peer_allowed = allowed.iter().any(|(pk, _)| *pk == peer);
            if !peer_allowed && config::withhold_active(self.withhold_window.as_deref(), Instant::now())
            {
                return;
            }
        }
        self.send_message(peer, PrimaryMessage::Header(header, false))
            .await;
    }

    /// Small accessor for `VantageCore::sync_batches`/`notify_committed` (which stay on
    /// `VantageCore` -- they touch `pending_payload`/`store`/`tx_payload_ready`/
    /// `tx_output`, protocol state rather than wire state) to resolve a worker's
    /// address now that `worker_addresses` lives here.
    pub(crate) fn worker_addr(&self, worker_id: WorkerId) -> Option<SocketAddr> {
        self.worker_addresses.get(&worker_id).copied()
    }
}
