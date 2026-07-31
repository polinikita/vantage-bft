// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::{encode_bundle, sleep_until_or_pending, BatchConfig, Coalescer};
use crate::blip::BlipGate;
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

/// Every `buffer`/`pending_replies`/`delay_queue` entry carries a Vec of reply targets
/// instead of a single one, so a coalesced bundle's ONE ack can fan out to every
/// constituent original `send()` call's own `CancelHandler`. When batching is off (the
/// default) this Vec always holds exactly one element -- same single send/notify as
/// before batching existed, just wrapped; wire bytes and observable behavior are
/// unchanged.
type ReplyTargets = Vec<oneshot::Sender<Bytes>>;

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
    /// Transient network-level "blip" fault injector (`node local-benchmark
    /// --blip-at`), attached via `with_blip` the same way `latency` is attached via
    /// `with_latency`. `None` by default -- every connection this sender spawns then
    /// skips the blip-clamp check entirely (see `Connection::keep_alive`'s branch
    /// condition), zero added cost on the untouched path.
    blip: Option<Arc<BlipGate>>,
    /// METRICS-DASHBOARD-SPEC.md §1: optional wire-metrics handle, attached the same
    /// way as `latency` (`with_metrics`, called once right after construction).
    /// `None` by default -- every connection this sender spawns then skips the
    /// `bytes_sent_total` accounting entirely (zero added cost on the untouched
    /// path, mirroring `extra_latency`'s own zero-cost default).
    metrics: Option<Arc<Metrics>>,
    /// METRICS-DASHBOARD-SPEC.md §8: lz4 compression, off by default -- the default
    /// (`false`) path never calls `lz4_flex::compress_prepend_size` at all, so it's
    /// byte-identical to pre-compression behavior.
    compress: bool,
    /// Transport-level per-peer outbound batching (coalescing), off by default -- see
    /// `network::batch`'s module doc. Byte-identical wire/behavior when disabled.
    batch: BatchConfig,
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
            blip: None,
            metrics: None,
            compress: false,
            batch: BatchConfig::default(),
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

    /// Attach a blip gate (same contract as `with_latency`: call before any
    /// connection is spawned). `None` -- the default, and always the case when
    /// `--blip-at` isn't given -- is a no-op.
    pub fn with_blip(mut self, gate: Option<Arc<BlipGate>>) -> Self {
        self.blip = gate;
        self
    }

    /// METRICS-DASHBOARD-SPEC.md §8: enable lz4 compression for every connection this
    /// sender spawns afterwards. Call before any connection is spawned (same contract
    /// as `with_latency`/`with_metrics`).
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    /// Enable per-connection outbound batching (coalescing) for every connection this
    /// sender spawns afterwards. Same contract as `with_compression`/`with_latency`:
    /// call before any connection is spawned. `BatchConfig::default()` (`enabled:
    /// false`) is a no-op -- byte-identical to never calling this at all.
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

    /// Helper function to spawn a new connection.
    fn spawn_connection(&self, address: SocketAddr) -> Sender<InnerMessage> {
        let (tx, rx) = channel(100_000);
        let extra_latency = self.latency.get(&address).copied().unwrap_or_default();
        // Resolved ONCE here (mirrors `extra_latency` just above), not per-message:
        // `None` unless a blip gate is attached AND `address` is one of its held
        // destinations (see `BlipGate::targets`'s doc comment).
        let blip = self.blip.clone().filter(|gate| gate.targets(&address));
        Connection::spawn(
            address,
            rx,
            extra_latency,
            blip,
            self.metrics.clone(),
            self.compress,
            self.batch,
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
                cancel_handler: sender,
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

/// Simple message used by `ReliableSender` to communicate with its connections.
#[derive(Debug)]
struct InnerMessage {
    /// The data to transmit.
    data: Bytes,
    /// The cancel handler allowing the caller task to cancel the transmission of this message
    /// and to be notified of its successfully transmission.
    cancel_handler: oneshot::Sender<Bytes>,
}

/// True iff every reply target in this bundle/single entry has already been dropped
/// (all its would-be recipients stopped caring) -- mirrors the pre-batching
/// `handler.is_closed()` check, generalized to "all closed" for a bundle of >1.
fn all_closed(handlers: &ReplyTargets) -> bool {
    handlers.iter().all(|h| h.is_closed())
}

/// Notify every reply target with the (shared, cheaply-cloned) ack bytes -- mirrors
/// the pre-batching single `handler.send(bytes)`, generalized to fan out to every
/// constituent of a bundle.
fn notify_all(handlers: ReplyTargets, bytes: Bytes) {
    for handler in handlers {
        let _ = handler.send(bytes.clone());
    }
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
    buffer: VecDeque<(Bytes, ReplyTargets)>,
    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): this connection's own fixed
    /// artificial one-way delay to `address` (`Duration::ZERO` = off, the default),
    /// resolved once at spawn time and applied before every real send for this
    /// connection's whole life -- see `keep_alive`.
    extra_latency: Duration,
    /// Transient network-level "blip" fault injector: resolved once at spawn time
    /// (mirrors `extra_latency`) via `ReliableSender::spawn_connection`'s own
    /// `BlipGate::targets` check -- `None` unless a blip gate is attached to the
    /// owning `ReliableSender` AND `address` is one of its held destinations. See
    /// `keep_alive`'s branch condition and `scheduled_release`.
    blip: Option<Arc<BlipGate>>,
    /// METRICS-DASHBOARD-SPEC.md §1: resolved once at spawn time (mirrors
    /// `extra_latency`); `bytes_sent_total` is incremented at every successful
    /// physical write, length prefix included, whether the first attempt or a retry.
    metrics: Option<Arc<Metrics>>,
    /// METRICS-DASHBOARD-SPEC.md §8: resolved once at spawn time; `false` (the
    /// default) never calls `lz4_flex::compress_prepend_size`.
    compress: bool,
    /// Resolved once at spawn time (mirrors `compress`); `BatchConfig::default()`
    /// (`enabled: false`) never consults a `Coalescer` at all.
    batch: BatchConfig,
}

impl Connection {
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<InnerMessage>,
        extra_latency: Duration,
        blip: Option<Arc<BlipGate>>,
        metrics: Option<Arc<Metrics>>,
        compress: bool,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                retry_delay: 200,
                buffer: VecDeque::new(),
                extra_latency,
                blip,
                metrics,
                compress,
                batch,
            }
            .run()
            .await;
        });
    }

    /// Length-delimited-codec frame prefix: 4 bytes, fixed (`LengthDelimitedCodec::
    /// new()`'s default `length_field_length`).
    const FRAME_PREFIX_LEN: u64 = 4;

    /// METRICS-DASHBOARD-SPEC.md §8: the actual bytes to hand to `writer.send` --
    /// lz4-compressed (`bytes_uncompressed_sent_total` credited with the pre-
    /// compression size) when `compress` is on, `data` verbatim otherwise (the
    /// default, zero-cost path: no compression call is made at all). When batching is
    /// also on, `data` here is already the WHOLE bundle frame -- compression wraps the
    /// bundle, not its individual constituent messages (order: coalesce -> compress ->
    /// outer length-prefix framing).
    fn wire_bytes(&self, data: &Bytes) -> Bytes {
        if !self.compress {
            return data.clone();
        }
        if let Some(metrics) = &self.metrics {
            metrics
                .bytes_uncompressed_sent_total
                .inc_by(data.len() as u64);
        }
        Bytes::from(lz4_flex::compress_prepend_size(data))
    }

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

    /// Main loop trying to connect to the peer and transmit messages.
    async fn run(&mut self) {
        let mut delay = self.retry_delay;
        let mut retry = 0;
        loop {
            match TcpStream::connect(self.address).await {
                Ok(stream) => {
                    info!("Outgoing connection established with {}", self.address);

                    // Reset the delay.
                    delay = self.retry_delay;
                    retry = 0;

                    // Try to transmit all messages in the buffer and keep transmitting incoming messages.
                    // The following function only returns if there is an error.
                    let error = self.keep_alive(stream).await;
                    warn!("{}", error);
                }
                Err(e) => {
                    warn!("{}", NetworkError::FailedToConnect(self.address, retry, e));
                    let timer = sleep(Duration::from_millis(delay));
                    tokio::pin!(timer);

                    'waiter: loop {
                        tokio::select! {
                            // Wait an increasing delay before attempting to reconnect.
                            () = &mut timer => {
                                delay = min(2*delay, 60_000);
                                retry +=1;
                                break 'waiter;
                            },

                            // Drain the channel into the buffer to not saturate the channel and block the caller task.
                            // The caller is responsible to cleanup the buffer through the cancel handlers.
                            //
                            // Invariant (batching adversarial audit, FIX 1a): while
                            // `self.batch.enabled`, EVERY entry that ever enters
                            // `self.buffer` must already be bundle-framed
                            // (`encode_bundle` output) -- `keep_alive_*` writes
                            // `buffer` entries to the wire verbatim (past `wire_bytes`/
                            // compression only), and the receiver's `decode_bundle`
                            // assumes every frame it reads while batching is on IS a
                            // bundle. This arm runs while the TCP connect itself is
                            // failing, i.e. BEFORE any `Connection`-owned coalescer
                            // exists to do that wrapping -- so it must wrap the raw
                            // message itself, as a singleton bundle, or a raw
                            // bincode-serialized `PrimaryMessage` would reach the wire
                            // and either silently mis-parse as a bogus small `count`
                            // (receiver still acks -> sender believes it delivered) or
                            // as a truncated frame (receiver drops the connection,
                            // sender retransmits the same poison frame forever).
                            Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                                let data = if self.batch.enabled { encode_bundle(&[data]) } else { data };
                                self.buffer.push_back((data, vec![cancel_handler]));
                                self.buffer.retain(|(_, handlers)| !all_closed(handlers));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Transmit messages once we have established a connection. D7-3 (PHASE7-PREP-
    /// NOTES.md): the default (`extra_latency.is_zero()` AND no blip gate attached)
    /// path is BYTE-IDENTICAL to the pre-existing code (no scheduling overhead at
    /// all, not even one extra `Instant::now()` call) -- the WAN-shaped-run/blip path
    /// is a separate method. `self.blip.is_none()` is one cheap extra `Option` check
    /// on this branch (mirrors `extra_latency.is_zero()` itself), added because a
    /// blip-gated connection needs the SAME dynamic per-message scheduling
    /// `keep_alive_delayed` already provides even when `extra_latency` itself is
    /// zero (see `scheduled_release`).
    async fn keep_alive(&mut self, stream: TcpStream) -> NetworkError {
        if self.extra_latency.is_zero() && self.blip.is_none() {
            self.keep_alive_immediate(stream).await
        } else {
            self.keep_alive_delayed(stream).await
        }
    }

    /// The release instant for a message being scheduled right now: `now() +
    /// extra_latency`, further clamped forward to the blip window's end if this
    /// connection is gated and that natural release would otherwise land inside the
    /// window (see `BlipGate::clamp`'s doc comment for the ordering-preservation
    /// argument -- this is the ONLY point `keep_alive_delayed` computes a release
    /// instant, so that argument covers this whole connection).
    fn scheduled_release(&self) -> Instant {
        let natural_release = Instant::now() + self.extra_latency;
        match &self.blip {
            Some(gate) => gate.clamp(natural_release),
            None => natural_release,
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
        let mut pending_replies = VecDeque::new();
        // Only ever populated when `self.batch.enabled` -- see `Coalescer`'s doc.
        let mut coalescer: Coalescer<oneshot::Sender<Bytes>> = Coalescer::new();
        let mut coalesce_deadline: Option<Instant> = None;

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Try to send all messages of the buffer.
            while let Some((data, handlers)) = self.buffer.pop_front() {
                // Skip messages that have been cancelled.
                if all_closed(&handlers) {
                    continue;
                }

                // Try to send the message.
                let wire = self.wire_bytes(&data);
                match writer.send(wire.clone()).await {
                    Ok(()) => {
                        // The message has been sent, we remove it from the buffer and add it to
                        // `pending_replies` while we wait for an ACK.
                        self.record_bytes_sent(wire.len());
                        self.record_frame_sent();
                        pending_replies.push_back((data, handlers));
                    }
                    Err(e) => {
                        // We failed to send the message, we put it back into the buffer.
                        self.buffer.push_front((data, handlers));
                        break 'connection NetworkError::FailedToSendMessage(self.address, e);
                    }
                }
            }

            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                () = coalesce_due, if self.batch.enabled && !coalescer.is_empty() => {
                    let (bundle, handlers) = coalescer.flush();
                    coalesce_deadline = None;
                    self.buffer.push_back((bundle, handlers));
                },
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    if self.batch.enabled {
                        if coalescer.push(data, cancel_handler) {
                            coalesce_deadline = Some(Instant::now() + self.batch.max_delay());
                        }
                        if coalescer.over_cap(self.batch.max_bytes) {
                            let (bundle, handlers) = coalescer.flush();
                            coalesce_deadline = None;
                            self.buffer.push_back((bundle, handlers));
                        }
                    } else {
                        // Add the message to the buffer of messages to send.
                        self.buffer.push_back((data, vec![cancel_handler]));
                    }
                },
                response = reader.next() => {
                    let (data, handlers) = match pending_replies.pop_front() {
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
                            pending_replies.push_front((data, handlers));
                            break 'connection NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        };

        // If we reach this code, it means something went wrong. Put the messages for which we didn't receive an ACK
        // back into the sending buffer, we will try to send them again once we manage to establish a new connection.
        while let Some(message) = pending_replies.pop_back() {
            self.buffer.push_front(message);
        }
        // Anything still sitting in the coalescer (armed but never flushed) goes back
        // as ONE bundle entry (FIX 1b, adversarial audit): these are raw,
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
        let drained = coalescer.drain();
        if !drained.is_empty() {
            let (msgs, handlers): (Vec<Bytes>, ReplyTargets) = drained.into_iter().unzip();
            self.buffer.push_front((encode_bundle(&msgs), handlers));
        }
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
        let mut pending_replies = VecDeque::new();
        let mut delay_queue: VecDeque<(Instant, Bytes, ReplyTargets)> = VecDeque::new();
        let mut coalescer: Coalescer<oneshot::Sender<Bytes>> = Coalescer::new();
        let mut coalesce_deadline: Option<Instant> = None;

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Schedule everything newly arrived (or re-queued after a previous
            // connection attempt's failure) -- cheap, no sends/sleeps happen here, so
            // this never blocks a NEW arrival on an EARLIER message's still-pending
            // delay.
            while let Some((data, handlers)) = self.buffer.pop_front() {
                if all_closed(&handlers) {
                    continue;
                }
                delay_queue.push_back((self.scheduled_release(), data, handlers));
            }

            let due = async {
                match delay_queue.front() {
                    Some((release_at, _, _)) => sleep_until(*release_at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            tokio::select! {
                () = due, if !delay_queue.is_empty() => {
                    let (_, data, handlers) = delay_queue.pop_front().unwrap();
                    if all_closed(&handlers) {
                        continue;
                    }
                    let wire = self.wire_bytes(&data);
                    match writer.send(wire.clone()).await {
                        Ok(()) => {
                            self.record_bytes_sent(wire.len());
                            self.record_frame_sent();
                            pending_replies.push_back((data, handlers));
                        }
                        Err(e) => {
                            self.buffer.push_front((data, handlers));
                            break 'connection NetworkError::FailedToSendMessage(self.address, e);
                        }
                    }
                },
                () = coalesce_due, if self.batch.enabled && !coalescer.is_empty() => {
                    let (bundle, handlers) = coalescer.flush();
                    coalesce_deadline = None;
                    self.buffer.push_back((bundle, handlers));
                },
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    if self.batch.enabled {
                        if coalescer.push(data, cancel_handler) {
                            coalesce_deadline = Some(Instant::now() + self.batch.max_delay());
                        }
                        if coalescer.over_cap(self.batch.max_bytes) {
                            let (bundle, handlers) = coalescer.flush();
                            coalesce_deadline = None;
                            self.buffer.push_back((bundle, handlers));
                        }
                    } else {
                        self.buffer.push_back((data, vec![cancel_handler]));
                    }
                },
                response = reader.next() => {
                    let (data, handlers) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Some(Ok(bytes)) => {
                            notify_all(handlers, bytes.freeze());
                        },
                        _ => {
                            pending_replies.push_front((data, handlers));
                            break 'connection NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        };

        // Everything still awaiting an ack, AND everything still sitting in the delay
        // queue (scheduled but never actually written), goes back to `buffer` for
        // retry after the next reconnect -- nothing silently dropped.
        while let Some(message) = pending_replies.pop_back() {
            self.buffer.push_front(message);
        }
        while let Some((_, data, handlers)) = delay_queue.pop_back() {
            self.buffer.push_front((data, handlers));
        }
        // FIX 1b / FIX 3: same as `keep_alive_immediate`'s identical tail -- see its
        // doc comment for both the bundle-framing invariant and the ordering caveat.
        let drained = coalescer.drain();
        if !drained.is_empty() {
            let (msgs, handlers): (Vec<Bytes>, ReplyTargets) = drained.into_iter().unzip();
            self.buffer.push_front((encode_bundle(&msgs), handlers));
        }
        error
    }
}
