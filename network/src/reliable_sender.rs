// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::{encode_bundle, sleep_until_or_pending, BatchConfig, Coalescer};
use crate::error::NetworkError;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use metrics::Metrics;
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, sleep_until, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/reliable_sender_tests.rs"]
pub mod reliable_sender_tests;

/// Convenient alias for cancel handlers returned to the caller task.
pub type CancelHandler = oneshot::Receiver<Bytes>;

/// reconnect-replay plan §2.3/§7/§14 A8 (clippy::type_complexity): the shared,
/// loss-free per-destination min-dropped-volatile-key map. One instance, attached to
/// every `Connection` a `ReliableSender` spawns via `with_drop_map` (same convention
/// as `with_latency`/`with_metrics`) -- each `Connection` only ever min-merges its
/// OWN address's entry into it, at session death (`Connection::report_dropped`). The
/// owning protocol crate (`vantage`/`simpleit`) drains it (translating `SocketAddr ->
/// PublicKey` via its own address map) on its own schedule -- see
/// `vantage::node`'s wiring for the consumer side. `None` on a `ReliableSender` that
/// never called `with_drop_map` (e.g. the resume-replay pool, §14 A7: "with_
/// reconnect_events/with_drop_map apply to the MAIN pool only") -- a `Connection`
/// with no drop map attached simply skips the merge, at zero cost.
/// Genuinely cross-task: written by each per-connection task on a drop, swept by
/// the consensus core. `parking_lot::Mutex` for the cheaper uncontended path and
/// because a poisoned lock is not a state this map can reach (every critical
/// section is a panic-free map operation).
pub type DirtyMap = Arc<parking_lot::Mutex<HashMap<SocketAddr, u64>>>;

/// Every `buffer`/`pending_replies`/`delay_queue` entry carries a Vec of reply targets
/// instead of a single one, so a coalesced bundle's ONE ack can fan out to every
/// constituent original `send()` call's own `CancelHandler`. When batching is off (the
/// default) this Vec always holds exactly one element -- same single send/notify as
/// before batching existed, just wrapped; wire bytes and observable behavior are
/// unchanged. reconnect-replay plan §7: a VOLATILE entry/bundle carries NO reply
/// target at all (audit m8 -- "nothing to bookkeep") -- this Vec is empty for those,
/// never a vec of one-already-closed handler.
type ReplyTargets = Vec<oneshot::Sender<Bytes>>;

/// reconnect-replay plan §2.2/§7: the filing key a volatile entry/bundle is discarded
/// under at session death (`None` for a durable entry/bundle). A coalesced volatile
/// bundle's own key is the MIN over every constituent message's key (§7: "a volatile
/// bundle's key = min over constituents") -- computed once at flush time, so this is
/// always a single, already-reduced value, never a per-constituent list.
type VolatileKey = Option<u64>;

/// One buffered/in-flight/scheduled entry: the wire bytes (a lone message, or an
/// already `encode_bundle`-framed bundle when batching is on), its reply targets
/// (empty for a volatile entry/bundle), and its own filing key (`None` for durable).
type BufferedEntry = (Bytes, ReplyTargets, VolatileKey);

/// KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`): the
/// reconnect-waiter's default exponential-backoff ceiling -- reproduces the value
/// this cap was hardcoded to before `with_retry_backoff_max_ms` existed, so a
/// `ReliableSender` that never calls it is byte-identical to today. See
/// `Connection::run`'s own doc comment (the `delay = min(2*delay, ..)` call site)
/// for where this is consulted, and `config::Parameters::retry_backoff_max_ms`'s
/// own doc comment for the measurement rationale.
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 2_000;

/// We keep alive one TCP connection per peer, each connection is handled by a separate task (called `Connection`).
/// We communicate with our 'connections' through a dedicated channel kept by the HashMap called `connections`.
/// This sender is 'reliable' in the sense that it keeps trying to re-transmit messages for which it didn't
/// receive an ACK back (until they succeed or are canceled).
pub struct ReliableSender {
    /// A map holding the channels to our connections.
    connections: HashMap<SocketAddr, Sender<InnerMessage>>,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): per-destination artificial send
    /// latency, empty by default (current behavior, byte-identical). See
    /// `network/src/lib.rs`'s module doc for the injection point/semantics.
    latency: HashMap<SocketAddr, Duration>,
    /// METRICS-DASHBOARD-SPEC.md §1: optional wire-metrics handle, attached the same
    /// way as `latency` (`with_metrics`, called once right after construction).
    /// `None` by default -- every connection this sender spawns then skips the
    /// `bytes_sent_total` accounting entirely (zero added cost on the untouched
    /// path, mirroring `extra_latency`'s own zero-cost default).
    metrics: Option<Arc<Metrics>>,
    /// Transport-level per-peer outbound batching (coalescing), off by default -- see
    /// `network::batch`'s module doc. Byte-identical wire/behavior when disabled.
    batch: BatchConfig,
    /// reconnect-replay plan §2.1/§7: fired (`tx.try_send(addr)`) by a `Connection`
    /// once it re-establishes a session AFTER a failure (never on the very first
    /// clean connect) -- a pure latency optimization (prompt Hello), never the
    /// load-bearing signal (that's `drop_map`, below): a dropped/lost event is simply
    /// recovered by the next `resume_tick`. `None` by default, same convention as
    /// `latency`/`metrics`.
    reconnect_events: Option<Sender<SocketAddr>>,
    /// reconnect-replay plan §2.3/§7: the shared dirty map every spawned `Connection`
    /// min-merges its own session-death volatile-drop report into. `None` by
    /// default, same convention as `latency`/`metrics`.
    drop_map: Option<DirtyMap>,
    /// KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`):
    /// the reconnect-waiter's exponential-backoff ceiling, in ms -- see
    /// `Connection::run`'s own doc comment for where this is consulted. Defaults to
    /// `DEFAULT_RETRY_BACKOFF_MAX_MS` (reproducing today's previously-hardcoded cap
    /// exactly); `with_retry_backoff_max_ms` overrides it, same
    /// attach-before-any-connection-spawns convention as `with_latency`/
    /// `with_metrics`/`with_batching`/`with_reconnect_events`/`with_drop_map`.
    retry_backoff_max_ms: u64,
}

impl std::default::Default for ReliableSender {
    fn default() -> Self {
        Self::new()
    }
}

impl ReliableSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            rng: SmallRng::from_entropy(),
            latency: HashMap::new(),
            metrics: None,
            batch: BatchConfig::default(),
            reconnect_events: None,
            drop_map: None,
            retry_backoff_max_ms: DEFAULT_RETRY_BACKOFF_MAX_MS,
        }
    }

    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): attach a per-destination
    /// artificial latency map, built by the caller (typically `Committee::
    /// latency_map`) BEFORE any connection to an address in the map is spawned --
    /// each `Connection` reads its own entry once, at spawn time (`spawn_connection`).
    /// A no-op (current behavior) if never called, or if a given destination address
    /// simply isn't a key in `map`.
    pub fn with_latency(mut self, map: HashMap<SocketAddr, Duration>) -> Self {
        self.latency = map;
        self
    }

    /// Enable per-connection outbound batching (coalescing) for every connection this
    /// sender spawns afterwards. Same contract as `with_latency`: call before any
    /// connection is spawned. `BatchConfig::default()` (`enabled: false`) is a no-op
    /// -- byte-identical to never calling this at all.
    pub fn with_batching(mut self, config: BatchConfig) -> Self {
        self.batch = config;
        self
    }

    /// METRICS-DASHBOARD-SPEC.md §1: attach a wire-metrics handle. Call once, right
    /// after construction, before any connection is spawned -- same contract as
    /// `with_latency`. Every `Connection` this sender spawns afterwards records its
    /// own physical writes (length-prefix included) into `metrics.bytes_sent_total`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// reconnect-replay plan §2.1/§7/§14 A7: attach the reconnect-event channel every
    /// connection this sender spawns afterwards fires on re-establishment after a
    /// failure (same contract/convention as `with_latency`/`with_metrics`). The MAIN
    /// pool only -- the resume-replay task's own senders never call this (A7).
    pub fn with_reconnect_events(mut self, tx: Sender<SocketAddr>) -> Self {
        self.reconnect_events = Some(tx);
        self
    }

    /// reconnect-replay plan §2.3/§7/§14 A7: attach the shared dirty map every
    /// connection this sender spawns afterwards min-merges its own session-death
    /// volatile-drop report into (same contract/convention as `with_latency`/
    /// `with_metrics`). The MAIN pool only (A7) -- see `DirtyMap`'s own doc comment.
    pub fn with_drop_map(mut self, map: DirtyMap) -> Self {
        self.drop_map = Some(map);
        self
    }

    /// KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`):
    /// override the reconnect-waiter's exponential-backoff ceiling (default
    /// `DEFAULT_RETRY_BACKOFF_MAX_MS` = 2000ms, reproducing today's
    /// previously-hardcoded cap exactly) -- same attach-before-any-connection-
    /// spawns convention as `with_latency`/`with_metrics`/`with_batching`/
    /// `with_reconnect_events`/`with_drop_map`. See `Connection::run`'s own doc
    /// comment for where this is consulted, and `config::Parameters::
    /// retry_backoff_max_ms`'s own doc comment for the measurement rationale
    /// (isolating this cap's own effect from the reconnect-replay mechanism that
    /// landed alongside it, across three benchmark arms).
    pub fn with_retry_backoff_max_ms(mut self, ms: u64) -> Self {
        self.retry_backoff_max_ms = ms;
        self
    }

    /// Helper function to spawn a new connection.
    fn spawn_connection(&self, address: SocketAddr) -> Sender<InnerMessage> {
        let (tx, rx) = channel(100_000);
        let extra_latency = self.latency.get(&address).copied().unwrap_or_default();
        Connection::spawn(
            address,
            rx,
            extra_latency,
            self.metrics.clone(),
            self.batch,
            self.reconnect_events.clone(),
            self.drop_map.clone(),
            self.retry_backoff_max_ms,
        );
        tx
    }

    /// Reliably send a message to a specific address.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) -> CancelHandler {
        let (sender, receiver) = oneshot::channel();
        if !self.connections.contains_key(&address) {
            let tx = self.spawn_connection(address);
            self.connections.insert(address, tx);
        }
        self.connections
            .get(&address)
            .unwrap()
            .send(InnerMessage {
                data,
                class: SendClass::Durable(sender),
            })
            .await
            .expect("Failed to send internal message");
        receiver
    }

    /// Broadcast the message to all specified addresses in a reliable manner. It returns a vector of
    /// cancel handlers ordered as the input `addresses` vector.
    pub async fn broadcast(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
    ) -> Vec<CancelHandler> {
        let mut handlers = Vec::new();
        for address in addresses {
            let handler = self.send(address, data.clone()).await;
            handlers.push(handler);
        }
        handlers
    }

    /// Pick a few addresses at random (specified by `nodes`) and send the message only to them.
    /// It returns a vector of cancel handlers with no specific order.
    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) -> Vec<CancelHandler> {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }

    /// METRICS-DASHBOARD-SPEC.md §1: same as `send`, plus a `network_messages_sent_total`/
    /// `network_bytes_sent_total` observation labeled `msg_type` (a no-op if this sender
    /// has no metrics handle attached). `msg_type` is the wire variant name -- known at
    /// the call site, not by this generic sender. Counts the LOGICAL message (once per
    /// `send_typed` call) regardless of whether it later gets coalesced into a bundle
    /// with others -- see `record_frame_sent` for the wire-frame-level counterpart.
    pub async fn send_typed(
        &mut self,
        address: SocketAddr,
        data: Bytes,
        msg_type: &'static str,
    ) -> CancelHandler {
        record_typed_sent(&self.metrics, msg_type, data.len());
        self.send(address, data).await
    }

    /// Typed variant of `broadcast` (see `send_typed`) -- records one observation per
    /// destination, matching `bytes_sent_total`'s own per-connection counting.
    pub async fn broadcast_typed(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
        msg_type: &'static str,
    ) -> Vec<CancelHandler> {
        let mut handlers = Vec::new();
        for address in addresses {
            handlers.push(self.send_typed(address, data.clone(), msg_type).await);
        }
        handlers
    }

    /// Typed variant of `lucky_broadcast`.
    pub async fn lucky_broadcast_typed(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
        msg_type: &'static str,
    ) -> Vec<CancelHandler> {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast_typed(addresses, data, msg_type).await
    }

    /// reconnect-replay plan §2.2/§7: send `data` VOLATILE, filed under `key` --
    /// reliable within the current session, silently discarded (counted into the
    /// drop map, never requeued) if the session dies before it's acked. Returns
    /// nothing: there is no cancel handler at all for a volatile send (audit m8) --
    /// nothing for the caller to track once the enqueue below succeeds.
    pub async fn send_volatile(&mut self, address: SocketAddr, data: Bytes, key: u64) {
        if !self.connections.contains_key(&address) {
            let tx = self.spawn_connection(address);
            self.connections.insert(address, tx);
        }
        self.connections
            .get(&address)
            .unwrap()
            .send(InnerMessage {
                data,
                class: SendClass::Volatile(key),
            })
            .await
            .expect("Failed to send internal message");
    }

    /// Broadcast variant of `send_volatile`.
    pub async fn broadcast_volatile(&mut self, addresses: Vec<SocketAddr>, data: Bytes, key: u64) {
        for address in addresses {
            self.send_volatile(address, data.clone(), key).await;
        }
    }

    /// Typed variant of `send_volatile` (see `send_typed`).
    pub async fn send_volatile_typed(
        &mut self,
        address: SocketAddr,
        data: Bytes,
        key: u64,
        msg_type: &'static str,
    ) {
        record_typed_sent(&self.metrics, msg_type, data.len());
        self.send_volatile(address, data, key).await;
    }

    /// Typed variant of `broadcast_volatile`.
    pub async fn broadcast_volatile_typed(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
        key: u64,
        msg_type: &'static str,
    ) {
        for address in addresses {
            self.send_volatile_typed(address, data.clone(), key, msg_type)
                .await;
        }
    }

    /// Adversarial-audit FINDING 2: a durable send with NO cancel handler at all --
    /// see `SendClass::DurableDetached`'s own doc comment for why this is NOT the
    /// same thing as calling `send` and dropping the returned `CancelHandler`
    /// early (that leaves a now-cancellable entry; this never allocates one to
    /// begin with, so there is nothing for a checkpoint to find "all closed").
    /// Retried-until-ack and requeued across reconnects exactly like `send`.
    /// For a caller that genuinely never needs the handle -- e.g. `vantage::wire`'s
    /// resume-sender task, whose `Replay`/`Done` frames must stay durable without
    /// the task itself tracking their individual completion.
    pub async fn send_detached(&mut self, address: SocketAddr, data: Bytes) {
        if !self.connections.contains_key(&address) {
            let tx = self.spawn_connection(address);
            self.connections.insert(address, tx);
        }
        self.connections
            .get(&address)
            .unwrap()
            .send(InnerMessage {
                data,
                class: SendClass::DurableDetached,
            })
            .await
            .expect("Failed to send internal message");
    }

    /// Typed variant of `send_detached` (see `send_typed`).
    pub async fn send_detached_typed(
        &mut self,
        address: SocketAddr,
        data: Bytes,
        msg_type: &'static str,
    ) {
        record_typed_sent(&self.metrics, msg_type, data.len());
        self.send_detached(address, data).await;
    }
}

/// Shared by `ReliableSender`/`SimpleSender`'s `*_typed` methods: increments the two
/// typed counters if `metrics` is attached, a no-op otherwise.
pub(crate) fn record_typed_sent(
    metrics: &Option<Arc<Metrics>>,
    msg_type: &'static str,
    len: usize,
) {
    if let Some(metrics) = metrics {
        metrics
            .network_messages_sent_total
            .with_label_values(&[msg_type])
            .inc();
        metrics
            .network_bytes_sent_total
            .with_label_values(&[msg_type])
            .inc_by(len as u64);
    }
}

/// reconnect-replay plan §7: a message's send class. `Durable` carries a live reply
/// target (today's cancel-handler semantics, retried-until-ack, requeued forever
/// across reconnects). `Volatile` carries a filing key instead (no reply target at
/// all -- audit m8) and is discarded, never requeued, at session death.
///
/// `DurableDetached` (adversarial-audit FINDING 2): durable requeue/retry semantics
/// identical to `Durable`, but with NO reply target at all -- for a caller that
/// truly never wants to track completion, not merely one that forgot to. This is
/// NOT the same as a caller dropping `Durable`'s `CancelHandler` early: doing that
/// leaves a non-empty `ReplyTargets` whose one (or, coalesced, several) entries are
/// all already closed, which `all_closed` correctly reports `true` for -- the A1
/// guard exempts only an EMPTY vec, so a bundle with a dropped-but-once-present
/// handler is (correctly) cancellable, and every pre-transmission checkpoint
/// (pre-send skip, delayed-pop skip, waiter retain) is then free to silently drop it
/// before it ever reaches the socket. `run_resume_sender`'s `Replay`/`Done` frames
/// were hitting exactly this: `send_typed`'s returned `CancelHandler` was discarded
/// at the end of the send statement, `is_closed()` flips true the instant that
/// `oneshot::Receiver` drops, and the frame could vanish pre-transmission -- durable
/// in name only, and the exact silent one-shot loss (the B1 class) v3 exists to
/// exclude, compounded by `pending_low` having already been raised/cleared on the
/// core's side for a span that might never actually transmit. `DurableDetached`
/// never allocates a `oneshot::channel` in the first place, so `ReplyTargets` for
/// such an entry is empty by construction (like `Volatile`'s), which the A1 guard
/// DOES correctly treat as never-closed -- and unlike `Volatile`, its filing key
/// slot is `None`, so the EXISTING buffer/pending_replies/session-death-tail code
/// (which already branches on `key: Option<u64>` -- `None` = durable-requeue,
/// `Some` = volatile-discard) requeues it across reconnects with zero changes to
/// that logic. An enum (rather than parallel `Option`s) makes "exactly one of the
/// three" a type-level invariant instead of a runtime one.
#[derive(Debug)]
enum SendClass {
    Durable(oneshot::Sender<Bytes>),
    DurableDetached,
    Volatile(u64),
}

/// Simple message used by `ReliableSender` to communicate with its connections.
#[derive(Debug)]
struct InnerMessage {
    /// The data to transmit.
    data: Bytes,
    /// This message's send class -- see `SendClass`'s own doc comment.
    class: SendClass,
}

/// True iff every reply target in this bundle/single entry has already been dropped
/// (all its would-be recipients stopped caring) -- mirrors the pre-batching
/// `handler.is_closed()` check, generalized to "all closed" for a bundle of >1.
///
/// reconnect-replay plan §14 A1 (MAJOR, adversarial audit-3): an EMPTY `handlers`
/// (every volatile entry/bundle -- see `ReplyTargets`'s own doc comment) is NEVER
/// "closed" here, regardless of `handlers.iter().all(..)`'s vacuous truth on an empty
/// iterator. Without the `!handlers.is_empty()` guard, a volatile entry would look
/// indistinguishable from an entry whose every (zero) reply target already resolved,
/// and every call site below (`Connection::run`'s waiter retain, the pre-send skip in
/// both `keep_alive_*` loops, the delayed-pop skip) would silently discard it BEFORE
/// it was ever transmitted -- outside all four counted session-death drop paths, so
/// `pending_low` would never learn about the loss. Redefining the predicate here
/// (rather than special-casing each of the four call sites individually) fixes all of
/// them at once and keeps them fixed against any future call site.
fn all_closed(handlers: &ReplyTargets) -> bool {
    !handlers.is_empty() && handlers.iter().all(|h| h.is_closed())
}

/// Notify every reply target with the (shared, cheaply-cloned) ack bytes -- mirrors
/// the pre-batching single `handler.send(bytes)`, generalized to fan out to every
/// constituent of a bundle. A no-op (empty loop) for a volatile bundle, whose
/// `ReplyTargets` is always empty.
fn notify_all(handlers: ReplyTargets, bytes: Bytes) {
    for handler in handlers {
        let _ = handler.send(bytes.clone());
    }
}

/// reconnect-replay plan §2.3/§7: min-merge `key` into the running per-connection
/// drop accumulator -- shared by every discard site in `keep_alive_immediate`/
/// `keep_alive_delayed`'s session-death tail.
fn merge_min_key(acc: &mut Option<u64>, key: u64) {
    *acc = Some(acc.map_or(key, |m| m.min(key)));
}

/// A connection is responsible to reliably establish (and keep alive) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<InnerMessage>,
    /// The initial delay to wait before re-attempting a connection (in ms).
    retry_delay: u64,
    /// Buffer keeping all messages that need to be re-transmitted.
    buffer: VecDeque<BufferedEntry>,
    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): this connection's own fixed
    /// artificial one-way delay to `address` (`Duration::ZERO` = off, the default),
    /// resolved once at spawn time and applied before every real send for this
    /// connection's whole life -- see `keep_alive`.
    extra_latency: Duration,
    /// METRICS-DASHBOARD-SPEC.md §1: resolved once at spawn time (mirrors
    /// `extra_latency`); `bytes_sent_total` is incremented at every successful
    /// physical write, length prefix included, whether the first attempt or a retry.
    metrics: Option<Arc<Metrics>>,
    /// Resolved once at spawn time (mirrors `extra_latency`); `BatchConfig::default()`
    /// (`enabled: false`) never consults a `Coalescer` at all.
    batch: BatchConfig,
    /// reconnect-replay plan §2.1/§7: resolved once at spawn time (mirrors
    /// `extra_latency`). Fired (best-effort `try_send`) exactly once per successful
    /// re-establishment AFTER a failure -- see `had_failure`.
    reconnect_events: Option<Sender<SocketAddr>>,
    /// reconnect-replay plan §2.3/§7: resolved once at spawn time (mirrors
    /// `extra_latency`). Min-merged into (`address` -> min dropped key) at every
    /// session death that actually discarded at least one volatile entry.
    drop_map: Option<DirtyMap>,
    /// KNOB 2 (measurement ablation, `config::Parameters::retry_backoff_max_ms`):
    /// resolved once at spawn time (mirrors `extra_latency`) -- the reconnect-
    /// waiter's exponential-backoff ceiling. See `run`'s own doc comment at the
    /// `delay = min(2*delay, ..)` call site for the measurement rationale.
    retry_backoff_max_ms: u64,
    /// reconnect-replay plan §2.1/§7: has this connection EVER failed before (a
    /// failed connect attempt, or an established session that later died)? Set in
    /// the connect-`Err` arm and right after `keep_alive` returns; checked (never
    /// reset -- monotone) in the connect-`Ok` arm, so the reconnect event fires on
    /// every re-establishment AFTER the first hiccup, but never on this
    /// `Connection`'s very first ever clean connect.
    had_failure: bool,
}

impl Connection {
    // clippy::too_many_arguments: mirrors this codebase's other constructors that
    // thread every one of a growing set of independent, orthogonal optional
    // capabilities (latency/metrics/batch/events/drop-map) through to a spawned task
    // -- a params struct would only add indirection for a private, single-call-site
    // constructor.
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<InnerMessage>,
        extra_latency: Duration,
        metrics: Option<Arc<Metrics>>,
        batch: BatchConfig,
        reconnect_events: Option<Sender<SocketAddr>>,
        drop_map: Option<DirtyMap>,
        retry_backoff_max_ms: u64,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                retry_delay: 200,
                buffer: VecDeque::new(),
                extra_latency,
                metrics,
                batch,
                reconnect_events,
                drop_map,
                retry_backoff_max_ms,
                had_failure: false,
            }
            .run()
            .await;
        });
    }

    /// Length-delimited-codec frame prefix: 4 bytes, fixed (`LengthDelimitedCodec::
    /// new()`'s default `length_field_length`).
    const FRAME_PREFIX_LEN: u64 = 4;

    fn record_bytes_sent(&self, len: usize) {
        if let Some(metrics) = &self.metrics {
            metrics
                .bytes_sent_total
                .inc_by(len as u64 + Self::FRAME_PREFIX_LEN);
        }
    }

    /// One physical wire frame was just written (a lone message when batching is off,
    /// one bundle of N when it's on) -- lets the coalescing ratio be read straight off
    /// Prometheus as `network_messages_sent_total / network_frames_sent_total`.
    fn record_frame_sent(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.network_frames_sent_total.inc();
        }
    }

    /// reconnect-replay plan §2.3/§7: min-merge `min_key` (if any) into the shared
    /// drop map under `self.address` -- called once, at the tail of a session that
    /// just died, with the min filing key over every volatile entry that session's
    /// `keep_alive_*` is about to discard (a no-op if nothing was discarded, or no
    /// drop map is attached).
    fn report_dropped(&self, min_key: Option<u64>) {
        let Some(key) = min_key else { return };
        let Some(map) = &self.drop_map else { return };
        let mut guard = map.lock();
        guard
            .entry(self.address)
            .and_modify(|existing| *existing = (*existing).min(key))
            .or_insert(key);
    }

    /// Main loop trying to connect to the peer and transmit messages.
    async fn run(&mut self) {
        let mut delay = self.retry_delay;
        let mut retry = 0;
        loop {
            match TcpStream::connect(self.address).await {
                Ok(stream) => {
                    // Same Nagle/delayed-ACK rationale as `SimpleSender` (starfish
                    // parity: nodelay on both connect and accept sides).
                    let _ = stream.set_nodelay(true);
                    info!("Outgoing connection established with {}", self.address);

                    // reconnect-replay plan §2.1/§7: a prompt Hello optimization
                    // only -- fires iff this is a re-establishment AFTER a failure
                    // (never the first-ever clean connect). Best-effort: a full or
                    // closed event channel just skips the prompt, recovered by the
                    // next `resume_tick` regardless.
                    if self.had_failure {
                        if let Some(tx) = &self.reconnect_events {
                            let _ = tx.try_send(self.address);
                        }
                    }

                    // Reset the delay.
                    delay = self.retry_delay;
                    retry = 0;

                    // Try to transmit all messages in the buffer and keep transmitting incoming messages.
                    // The following function only returns if there is an error.
                    let error = self.keep_alive(stream).await;
                    self.had_failure = true;
                    warn!("{}", error);
                }
                Err(e) => {
                    self.had_failure = true;
                    warn!("{}", NetworkError::FailedToConnect(self.address, retry, e));
                    let timer = sleep(Duration::from_millis(delay));
                    tokio::pin!(timer);

                    'waiter: loop {
                        tokio::select! {
                            // Wait an increasing delay before attempting to reconnect.
                            () = &mut timer => {
                                // reconnect-replay plan §2.1: cap lowered from 60s to
                                // 2s, uniformly across protocols -- the measured
                                // baseline's ~10s post-restore stall was dominated by
                                // this cap (backoff at 12.8s at the old 60s cap); a
                                // 2s ceiling bounds the worst-case pre-Hello silent
                                // gap without materially increasing reconnect churn.
                                //
                                // KNOB 2 (measurement ablation, `config::Parameters::
                                // retry_backoff_max_ms`, `ReliableSender::
                                // with_retry_backoff_max_ms`): this cap change landed
                                // in the same commits as the reconnect-replay
                                // mechanism above, and an adversarial review of a
                                // before/after benchmark figure found the cap alone
                                // explains most of the measured improvement --
                                // `self.retry_backoff_max_ms` makes it independently
                                // selectable (default `DEFAULT_RETRY_BACKOFF_MAX_MS` =
                                // 2_000, reproducing the hardcoded value this replaced
                                // exactly) so the two changes can be attributed
                                // separately across benchmark arms.
                                delay = min(2*delay, self.retry_backoff_max_ms);
                                retry +=1;
                                break 'waiter;
                            },

                            // Drain the channel to not saturate it and block the caller
                            // task while the link is down. reconnect-replay plan §7's
                            // four discard-path enumeration ("reconnect-waiter drain
                            // (:372-376)") is THIS arm; audit-3 V-b ("mpsc-limbo items
                            // are either waiter-counted or next-session-delivered")
                            // explicitly blesses WAITER-COUNTED here, specifically for
                            // volatile arrivals -- the two classes are handled
                            // differently, not identically:
                            //
                            // - Durable: buffered exactly as before (FIX 1a below);
                            //   The caller is responsible to clean up the buffer
                            //   through the cancel handlers.
                            // - DurableDetached (adversarial-audit FINDING 2): same
                            //   buffering as Durable, minus the handler -- there is
                            //   none to retain-scan for (there never was one), so
                            //   `all_closed` on its empty `ReplyTargets` is always
                            //   `false` (A1) and it survives exactly like Durable
                            //   does, across as many reconnects as it takes.
                            // - Volatile: NEVER buffered -- counted (min-merged into
                            //   the drop map, exactly like the three `keep_alive_*`
                            //   session-death discard sites) and dropped immediately.
                            //   Buffering it instead (durable-style) would let an
                            //   entire outage's backlog of one-shots ride out as ONE
                            //   unpaced flush at the next reconnect -- exactly the
                            //   burst §2.2 exists to eliminate ("after a cut, the only
                            //   transport flush left is the small durable set"); with
                            //   the backlog counted-and-dropped here instead, the
                            //   paced replay stream only ever has to cover what the
                            //   drop accounting reports missing, never re-deliver a
                            //   flush spike itself.
                            //
                            // Coverage (why dropping here is still always safe, never
                            // a hidden loss): `VantageCore::broadcast_recorded`
                            // records a message's filing key into the outbox BEFORE
                            // sending it, so the outbox already holds this message by
                            // the time it ever reaches this arm. The min-merge below
                            // lands in the shared drop map at THIS instant, strictly
                            // BEFORE any later dirty-map sweep can observe it -- so
                            // even a replay that already served (and raised `pending_
                            // low`) past this exact key gets RE-DIPPED by the merge
                            // (a min-merge only ever lowers the recorded floor, audit
                            // Q4's re-dip pattern). Dropped-at-waiter is therefore
                            // always a SUBSET of served-later: coverage is identical
                            // to buffering it, only the pacing differs.
                            //
                            // Contract: a `SendClass::Volatile` arrival on a
                            // `ReliableSender` with no drop map attached (`self.
                            // drop_map: None`) is a CALLER error -- only vantage's
                            // MAIN pool ever calls `send_volatile`/`broadcast_
                            // volatile`, and that pool always attaches one (§14 A7).
                            // `report_dropped` silently no-ops in that case (the key
                            // is lost, unaccounted, never panics) rather than treating
                            // a misconfigured pool as fatal.
                            Some(InnerMessage{data, class}) = self.receiver.recv() => {
                                match class {
                                    SendClass::Durable(h) => {
                                        // FIX 1a (batching adversarial audit): while
                                        // `self.batch.enabled`, EVERY entry that ever
                                        // enters `self.buffer` must already be
                                        // bundle-framed (`encode_bundle` output) --
                                        // `keep_alive_*` writes `buffer` entries to the
                                        // wire verbatim, and the receiver's `decode_
                                        // bundle` assumes every frame it reads while
                                        // batching is on IS a bundle. This arm runs
                                        // while the TCP connect itself is failing, i.e.
                                        // BEFORE any `Connection`-owned coalescer
                                        // exists to do that wrapping -- so it must wrap
                                        // the raw message itself, as a singleton
                                        // bundle, or a raw bincode-serialized
                                        // `PrimaryMessage` would reach the wire and
                                        // either silently mis-parse as a bogus small
                                        // `count` (receiver still acks -> sender
                                        // believes it delivered) or as a truncated
                                        // frame (receiver drops the connection, sender
                                        // retransmits the same poison frame forever).
                                        let data = if self.batch.enabled { encode_bundle(&[data]) } else { data };
                                        self.buffer.push_back((data, vec![h], None));
                                        self.buffer.retain(|(_, handlers, _)| !all_closed(handlers));
                                    }
                                    SendClass::DurableDetached => {
                                        // Same FIX 1a bundle-framing requirement as
                                        // `Durable` above -- the wire format cares
                                        // whether batching is on, not whether a
                                        // handler happens to exist.
                                        let data = if self.batch.enabled { encode_bundle(&[data]) } else { data };
                                        self.buffer.push_back((data, Vec::new(), None));
                                        self.buffer.retain(|(_, handlers, _)| !all_closed(handlers));
                                    }
                                    SendClass::Volatile(k) => self.report_dropped(Some(k)),
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Transmit messages once we have established a connection. D7-3 (PHASE7-PREP-
    /// NOTES.md): the default (`extra_latency.is_zero()`) path is BYTE-IDENTICAL to
    /// the pre-existing code (no scheduling overhead at all, not even one extra
    /// `Instant::now()` call) -- the WAN-shaped-run path is a separate method.
    async fn keep_alive(&mut self, stream: TcpStream) -> NetworkError {
        if self.extra_latency.is_zero() {
            self.keep_alive_immediate(stream).await
        } else {
            self.keep_alive_delayed(stream).await
        }
    }

    /// The release instant for a message being scheduled right now: `now() +
    /// extra_latency`.
    fn scheduled_release(&self) -> Instant {
        Instant::now() + self.extra_latency
    }

    /// reconnect-replay plan §7: route one freshly-arrived `InnerMessage` either into
    /// `self.buffer` directly (batching off) or into the matching one of the two
    /// PER-CLASS coalescers (batching on) -- shared by `keep_alive_immediate`/
    /// `keep_alive_delayed`'s otherwise-identical arrival arm. Durable and volatile
    /// NEVER share a bundle (module doc: "separate coalescers, not flush-on-class-
    /// switch" -- this is the one call site that split would otherwise have
    /// complicated); a volatile bundle's own key is reduced (min) only once, at
    /// flush time (see the two `flush_*` helpers below), never accumulated here.
    ///
    /// Adversarial-audit FINDING 2: `SendClass::DurableDetached` bypasses BOTH
    /// coalescers entirely, always a singleton buffer entry (still bundle-wrapped
    /// when batching is on -- the same wire-format requirement `Durable`'s own
    /// entries have) -- there is no `oneshot::Sender` to fold into `durable_
    /// coalescer`'s own generic slot, and this class's volume is bounded by
    /// construction (`vantage::wire`'s own module doc: at most one budgeted replay
    /// stream + its `Done` per peer at a time), so skipping coalescing costs
    /// nothing worth adding a THIRD coalescer for.
    #[allow(clippy::too_many_arguments)]
    fn on_arrival(
        &mut self,
        data: Bytes,
        class: SendClass,
        durable_coalescer: &mut Coalescer<oneshot::Sender<Bytes>>,
        durable_deadline: &mut Option<Instant>,
        volatile_coalescer: &mut Coalescer<u64>,
        volatile_deadline: &mut Option<Instant>,
    ) {
        match class {
            SendClass::DurableDetached => {
                let data = if self.batch.enabled {
                    encode_bundle(&[data])
                } else {
                    data
                };
                self.buffer.push_back((data, Vec::new(), None));
            }
            SendClass::Durable(h) if !self.batch.enabled => {
                self.buffer.push_back((data, vec![h], None));
            }
            SendClass::Volatile(k) if !self.batch.enabled => {
                self.buffer.push_back((data, Vec::new(), Some(k)));
            }
            SendClass::Durable(h) => {
                if durable_coalescer.push(data, h) {
                    *durable_deadline = Some(Instant::now() + self.batch.max_delay());
                }
                if durable_coalescer.over_cap(self.batch.max_bytes) {
                    let (bundle, handlers) = durable_coalescer.flush();
                    *durable_deadline = None;
                    self.buffer.push_back((bundle, handlers, None));
                }
            }
            SendClass::Volatile(k) => {
                if volatile_coalescer.push(data, k) {
                    *volatile_deadline = Some(Instant::now() + self.batch.max_delay());
                }
                if volatile_coalescer.over_cap(self.batch.max_bytes) {
                    let (bundle, keys) = volatile_coalescer.flush();
                    *volatile_deadline = None;
                    self.buffer
                        .push_back((bundle, Vec::new(), keys.into_iter().min()));
                }
            }
        }
    }

    /// The original, unmodified transmit loop -- used whenever no artificial latency
    /// is configured for this connection (current behavior, unchanged). Batching (when
    /// `self.batch.enabled`) sits BEFORE this loop's own send pipeline: arrivals are
    /// coalesced into bundle frames which are then pushed into `self.buffer` exactly
    /// like any other message -- the pop/send/ack-wait code below is unaware whether a
    /// given `buffer` entry is a lone message or a bundle.
    async fn keep_alive_immediate(&mut self, stream: TcpStream) -> NetworkError {
        // This buffer keeps all messages and handlers that we have successfully transmitted but for
        // which we are still waiting to receive an ACK.
        let mut pending_replies: VecDeque<BufferedEntry> = VecDeque::new();
        // Only ever populated when `self.batch.enabled` -- see `Coalescer`'s doc.
        // reconnect-replay plan §7 ("implementation's choice"): durable and volatile
        // arrivals coalesce into SEPARATE `Coalescer` instances rather than one
        // shared coalescer with a flush-on-class-switch policy -- simpler (no need to
        // track/compare "last pushed class" before every push, no extra forced flush
        // call, and each class's own timer/byte-cap fires completely independently,
        // with no cross-class interference) at the cost of one more (small, empty
        // until used) struct per connection.
        let mut durable_coalescer: Coalescer<oneshot::Sender<Bytes>> = Coalescer::new();
        let mut durable_deadline: Option<Instant> = None;
        let mut volatile_coalescer: Coalescer<u64> = Coalescer::new();
        let mut volatile_deadline: Option<Instant> = None;

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Try to send all messages of the buffer.
            while let Some((data, handlers, key)) = self.buffer.pop_front() {
                // Skip messages that have been cancelled (never true for a volatile
                // entry -- `all_closed` on its empty `handlers` is always `false`,
                // reconnect-replay plan §14 A1).
                if all_closed(&handlers) {
                    continue;
                }

                // Try to send the message.
                match writer.send(data.clone()).await {
                    Ok(()) => {
                        // The message has been sent, we remove it from the buffer and add it to
                        // `pending_replies` while we wait for an ACK.
                        self.record_bytes_sent(data.len());
                        self.record_frame_sent();
                        pending_replies.push_back((data, handlers, key));
                    }
                    Err(e) => {
                        // We failed to send the message, we put it back into the buffer.
                        self.buffer.push_front((data, handlers, key));
                        break 'connection NetworkError::FailedToSendMessage(self.address, e);
                    }
                }
            }

            let durable_due = sleep_until_or_pending(durable_deadline);
            let volatile_due = sleep_until_or_pending(volatile_deadline);

            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                () = durable_due, if self.batch.enabled && !durable_coalescer.is_empty() => {
                    let (bundle, handlers) = durable_coalescer.flush();
                    durable_deadline = None;
                    self.buffer.push_back((bundle, handlers, None));
                },
                () = volatile_due, if self.batch.enabled && !volatile_coalescer.is_empty() => {
                    let (bundle, keys) = volatile_coalescer.flush();
                    volatile_deadline = None;
                    self.buffer.push_back((bundle, Vec::new(), keys.into_iter().min()));
                },
                Some(InnerMessage{data, class}) = self.receiver.recv() => {
                    self.on_arrival(data, class, &mut durable_coalescer, &mut durable_deadline, &mut volatile_coalescer, &mut volatile_deadline);
                },
                response = reader.next() => {
                    let (data, handlers, key) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Some(Ok(bytes)) => {
                            // Notify every reply target that the message has been successfully sent.
                            notify_all(handlers, bytes.freeze());
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            // Put the message back in the buffer, we will try to send it again.
                            pending_replies.push_front((data, handlers, key));
                            break 'connection NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        };

        // If we reach this code, it means something went wrong. Durable entries still
        // awaiting an ACK go back into `self.buffer` to retry after the next
        // reconnect, exactly as before. reconnect-replay plan §2.3/§7: VOLATILE
        // entries are session-scoped -- this session just died, so they are counted
        // into `min_dropped_key` and discarded instead (never requeued).
        let mut min_dropped_key: Option<u64> = None;
        while let Some((data, handlers, key)) = pending_replies.pop_back() {
            match key {
                Some(k) => merge_min_key(&mut min_dropped_key, k),
                None => self.buffer.push_front((data, handlers, None)),
            }
        }
        // Anything still sitting in the DURABLE coalescer (armed but never flushed)
        // goes back as ONE bundle entry (FIX 1b, adversarial audit): these are raw,
        // never-yet-encoded messages -- requeuing them individually would put raw,
        // non-bundle-framed bytes into `self.buffer`, which `keep_alive_*` writes to
        // the wire verbatim while `self.batch.enabled`, breaking the same "every
        // buffered entry is bundle-framed" invariant FIX 1a restores for the
        // reconnect-waiter path (see `run`'s doc comment). `Coalescer::drain` already
        // returns items in arrival order, so no reversal is needed here.
        //
        // FIX 3 (ordering caveat, not safety-critical): this single bundle entry is
        // pushed to the very front, ahead of the `pending_replies` just restored
        // above -- even though everything in the coalescer arrived STRICTLY AFTER
        // whatever is in `pending_replies` (the coalescer only ever holds arrivals
        // that came in after their predecessors were already sent). So a reconnect
        // that catches messages mid-coalesce will retry the newest data before the
        // older, already-attempted `pending_replies` data, inverting strict per-link
        // FIFO exactly at this boundary (unlike the steady-state path, which is
        // strictly FIFO by construction).
        let durable_drained = durable_coalescer.drain();
        if !durable_drained.is_empty() {
            let (msgs, handlers): (Vec<Bytes>, ReplyTargets) = durable_drained.into_iter().unzip();
            self.buffer
                .push_front((encode_bundle(&msgs), handlers, None));
        }
        // reconnect-replay plan §2.3/§7: the VOLATILE coalescer's own unflushed
        // remnant is session-scoped too -- fold its constituents' keys into
        // `min_dropped_key` and discard, mirroring `pending_replies`'s treatment
        // immediately above (never re-encoded/requeued).
        let volatile_drained = volatile_coalescer.drain();
        for (_, k) in volatile_drained {
            merge_min_key(&mut min_dropped_key, k);
        }
        // A write error mid-pre-send-loop breaks 'connection with the failed entry
        // pushed back AND every entry still queued behind it left in `self.buffer` --
        // the one session-death exit whose leftovers the requeue loops above never
        // see. Sweep it here so the "buffer is durable-only at `keep_alive` entry"
        // invariant (reconnect-replay plan §2.2/§7) holds on EVERY exit path:
        // volatile entries are counted into `min_dropped_key` and discarded, durable
        // entries stay for the next session's flush, exactly as everywhere else in
        // this tail.
        self.buffer.retain(|(_, _, key)| match key {
            Some(k) => {
                merge_min_key(&mut min_dropped_key, *k);
                false
            }
            None => true,
        });
        self.report_dropped(min_dropped_key);
        error
    }

    /// D7-3 (PHASE7-PREP-NOTES.md, coordinator-mandated fix for the earlier
    /// serial-FIFO-ceiling finding): a starfish-style "many messages in flight"
    /// pipeline, without starfish's own per-message concurrent tasks (which would risk
    /// `pending_replies`' strict send/ack correlation under jitter/scheduling races --
    /// see the notes for why that was rejected here). Because every message on this
    /// connection gets the IDENTICAL fixed `extra_latency`, and messages are scheduled
    /// in arrival order, their release times are ALSO strictly increasing in that same
    /// order (`t2 > t1 => t2+d > t1+d` for a constant `d`) -- so a single, plain FIFO
    /// delay queue (`delay_queue`, gated on ONLY its own front's scheduled release
    /// instant) preserves strict per-link order by construction, with no jitter and no
    /// concurrency needed. This decouples ARRIVAL (buffering, immediate) from the
    /// actual WRITE (gated on the queue's own due time), restoring the "many in
    /// flight" pipelined throughput a real network link has (latency doesn't reduce a
    /// link's bandwidth) instead of capping this link at `1 / extra_latency`
    /// messages/sec the way the earlier "sleep synchronously before each write"
    /// version did.
    ///
    /// Batching composes the same way as in `keep_alive_immediate`: coalesced bundles
    /// are pushed into `self.buffer`, and the existing `buffer -> delay_queue -> wire`
    /// drain below (unchanged) schedules the WHOLE bundle as a single delay-queue
    /// entry -- one injected latency per bundle, exactly the spec (its messages are
    /// ~simultaneous).
    async fn keep_alive_delayed(&mut self, stream: TcpStream) -> NetworkError {
        let mut pending_replies: VecDeque<BufferedEntry> = VecDeque::new();
        let mut delay_queue: VecDeque<(Instant, Bytes, ReplyTargets, VolatileKey)> =
            VecDeque::new();
        let mut durable_coalescer: Coalescer<oneshot::Sender<Bytes>> = Coalescer::new();
        let mut durable_deadline: Option<Instant> = None;
        let mut volatile_coalescer: Coalescer<u64> = Coalescer::new();
        let mut volatile_deadline: Option<Instant> = None;

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Schedule everything newly arrived (or re-queued after a previous
            // connection attempt's failure) -- cheap, no sends/sleeps happen here, so
            // this never blocks a NEW arrival on an EARLIER message's still-pending
            // delay.
            while let Some((data, handlers, key)) = self.buffer.pop_front() {
                if all_closed(&handlers) {
                    continue;
                }
                delay_queue.push_back((self.scheduled_release(), data, handlers, key));
            }

            let due = async {
                match delay_queue.front() {
                    Some((release_at, _, _, _)) => sleep_until(*release_at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let durable_due = sleep_until_or_pending(durable_deadline);
            let volatile_due = sleep_until_or_pending(volatile_deadline);

            tokio::select! {
                () = due, if !delay_queue.is_empty() => {
                    let (_, data, handlers, key) = delay_queue.pop_front().unwrap();
                    if all_closed(&handlers) {
                        continue;
                    }
                    match writer.send(data.clone()).await {
                        Ok(()) => {
                            self.record_bytes_sent(data.len());
                            self.record_frame_sent();
                            pending_replies.push_back((data, handlers, key));
                        }
                        Err(e) => {
                            self.buffer.push_front((data, handlers, key));
                            break 'connection NetworkError::FailedToSendMessage(self.address, e);
                        }
                    }
                },
                () = durable_due, if self.batch.enabled && !durable_coalescer.is_empty() => {
                    let (bundle, handlers) = durable_coalescer.flush();
                    durable_deadline = None;
                    self.buffer.push_back((bundle, handlers, None));
                },
                () = volatile_due, if self.batch.enabled && !volatile_coalescer.is_empty() => {
                    let (bundle, keys) = volatile_coalescer.flush();
                    volatile_deadline = None;
                    self.buffer.push_back((bundle, Vec::new(), keys.into_iter().min()));
                },
                Some(InnerMessage{data, class}) = self.receiver.recv() => {
                    self.on_arrival(data, class, &mut durable_coalescer, &mut durable_deadline, &mut volatile_coalescer, &mut volatile_deadline);
                },
                response = reader.next() => {
                    let (data, handlers, key) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Some(Ok(bytes)) => {
                            notify_all(handlers, bytes.freeze());
                        },
                        _ => {
                            pending_replies.push_front((data, handlers, key));
                            break 'connection NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        };

        // Everything still awaiting an ack, AND everything still sitting in the delay
        // queue (scheduled but never actually written), goes back to `buffer` for
        // retry after the next reconnect IF durable; if volatile, counted into
        // `min_dropped_key` and discarded -- nothing silently dropped UNACCOUNTED.
        let mut min_dropped_key: Option<u64> = None;
        while let Some((data, handlers, key)) = pending_replies.pop_back() {
            match key {
                Some(k) => merge_min_key(&mut min_dropped_key, k),
                None => self.buffer.push_front((data, handlers, None)),
            }
        }
        while let Some((_, data, handlers, key)) = delay_queue.pop_back() {
            match key {
                Some(k) => merge_min_key(&mut min_dropped_key, k),
                None => self.buffer.push_front((data, handlers, None)),
            }
        }
        // FIX 1b / FIX 3: same as `keep_alive_immediate`'s identical tail -- see its
        // doc comment for both the bundle-framing invariant and the ordering caveat.
        let durable_drained = durable_coalescer.drain();
        if !durable_drained.is_empty() {
            let (msgs, handlers): (Vec<Bytes>, ReplyTargets) = durable_drained.into_iter().unzip();
            self.buffer
                .push_front((encode_bundle(&msgs), handlers, None));
        }
        let volatile_drained = volatile_coalescer.drain();
        for (_, k) in volatile_drained {
            merge_min_key(&mut min_dropped_key, k);
        }
        // A write error mid-pre-send-loop breaks 'connection with the failed entry
        // pushed back AND every entry still queued behind it left in `self.buffer` --
        // the one session-death exit whose leftovers the requeue loops above never
        // see. Sweep it here so the "buffer is durable-only at `keep_alive` entry"
        // invariant (reconnect-replay plan §2.2/§7) holds on EVERY exit path:
        // volatile entries are counted into `min_dropped_key` and discarded, durable
        // entries stay for the next session's flush, exactly as everywhere else in
        // this tail.
        self.buffer.retain(|(_, _, key)| match key {
            Some(k) => {
                merge_min_key(&mut min_dropped_key, *k);
                false
            }
            None => true,
        });
        self.report_dropped(min_dropped_key);
        error
    }
}
