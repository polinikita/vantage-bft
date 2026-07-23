// Copyright(C) Facebook, Inc. and its affiliates.
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
    /// METRICS-DASHBOARD-SPEC.md §8: lz4 compression, off by default -- the default
    /// (`false`) path never calls `lz4_flex::compress_prepend_size` at all, so it's
    /// byte-identical to pre-compression behavior.
    compress: bool,
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
            compress: false,
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

    /// METRICS-DASHBOARD-SPEC.md §8: enable lz4 compression for every connection this
    /// sender spawns afterwards. Call before any connection is spawned (same contract
    /// as `with_latency`/`with_metrics`).
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compress = enabled;
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
        let (tx, rx) = channel(1_000);
        let extra_latency = self.latency.get(&address).copied().unwrap_or_default();
        Connection::spawn(address, rx, extra_latency, self.metrics.clone(), self.compress);
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
    /// the call site, not by this generic sender.
    pub async fn send_typed(&mut self, address: SocketAddr, data: Bytes, msg_type: &'static str) -> CancelHandler {
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
pub(crate) fn record_typed_sent(metrics: &Option<Arc<Metrics>>, msg_type: &'static str, len: usize) {
    if let Some(metrics) = metrics {
        metrics.network_messages_sent_total.with_label_values(&[msg_type]).inc();
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

/// A connection is responsible to reliably establish (and keep alive) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<InnerMessage>,
    /// The initial delay to wait before re-attempting a connection (in ms).
    retry_delay: u64,
    /// Buffer keeping all messages that need to be re-transmitted.
    buffer: VecDeque<(Bytes, oneshot::Sender<Bytes>)>,
    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): this connection's own fixed
    /// artificial one-way delay to `address` (`Duration::ZERO` = off, the default),
    /// resolved once at spawn time and applied before every real send for this
    /// connection's whole life -- see `keep_alive`.
    extra_latency: Duration,
    /// METRICS-DASHBOARD-SPEC.md §1: resolved once at spawn time (mirrors
    /// `extra_latency`); `bytes_sent_total` is incremented at every successful
    /// physical write, length prefix included, whether the first attempt or a retry.
    metrics: Option<Arc<Metrics>>,
    /// METRICS-DASHBOARD-SPEC.md §8: resolved once at spawn time; `false` (the
    /// default) never calls `lz4_flex::compress_prepend_size`.
    compress: bool,
}

impl Connection {
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<InnerMessage>,
        extra_latency: Duration,
        metrics: Option<Arc<Metrics>>,
        compress: bool,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                retry_delay: 200,
                buffer: VecDeque::new(),
                extra_latency,
                metrics,
                compress,
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
    /// default, zero-cost path: no compression call is made at all).
    fn wire_bytes(&self, data: &Bytes) -> Bytes {
        if !self.compress {
            return data.clone();
        }
        if let Some(metrics) = &self.metrics {
            metrics.bytes_uncompressed_sent_total.inc_by(data.len() as u64);
        }
        Bytes::from(lz4_flex::compress_prepend_size(data))
    }

    fn record_bytes_sent(&self, len: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.bytes_sent_total.inc_by(len as u64 + Self::FRAME_PREFIX_LEN);
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
                            Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                                self.buffer.push_back((data, cancel_handler));
                                self.buffer.retain(|(_, handler)| !handler.is_closed());
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

    /// The original, unmodified transmit loop -- used whenever no artificial latency
    /// is configured for this connection (current behavior, unchanged).
    async fn keep_alive_immediate(&mut self, stream: TcpStream) -> NetworkError {
        // This buffer keeps all messages and handlers that we have successfully transmitted but for
        // which we are still waiting to receive an ACK.
        let mut pending_replies = VecDeque::new();

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Try to send all messages of the buffer.
            while let Some((data, handler)) = self.buffer.pop_front() {
                // Skip messages that have been cancelled.
                if handler.is_closed() {
                    continue;
                }

                // Try to send the message.
                let wire = self.wire_bytes(&data);
                match writer.send(wire.clone()).await {
                    Ok(()) => {
                        // The message has been sent, we remove it from the buffer and add it to
                        // `pending_replies` while we wait for an ACK.
                        self.record_bytes_sent(wire.len());
                        pending_replies.push_back((data, handler));
                    }
                    Err(e) => {
                        // We failed to send the message, we put it back into the buffer.
                        self.buffer.push_front((data, handler));
                        break 'connection NetworkError::FailedToSendMessage(self.address, e);
                    }
                }
            }

            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    // Add the message to the buffer of messages to send.
                    self.buffer.push_back((data, cancel_handler));
                },
                response = reader.next() => {
                    let (data, handler) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Some(Ok(bytes)) => {
                            // Notify the handler that the message has been successfully sent.
                            let _ = handler.send(bytes.freeze());
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            // Put the message back in the buffer, we will try to send it again.
                            pending_replies.push_front((data, handler));
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
    async fn keep_alive_delayed(&mut self, stream: TcpStream) -> NetworkError {
        let mut pending_replies = VecDeque::new();
        let mut delay_queue: VecDeque<(Instant, Bytes, oneshot::Sender<Bytes>)> = VecDeque::new();

        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();
        let error = 'connection: loop {
            // Schedule everything newly arrived (or re-queued after a previous
            // connection attempt's failure) -- cheap, no sends/sleeps happen here, so
            // this never blocks a NEW arrival on an EARLIER message's still-pending
            // delay.
            while let Some((data, handler)) = self.buffer.pop_front() {
                if handler.is_closed() {
                    continue;
                }
                delay_queue.push_back((Instant::now() + self.extra_latency, data, handler));
            }

            let due = async {
                match delay_queue.front() {
                    Some((release_at, _, _)) => sleep_until(*release_at).await,
                    None => std::future::pending::<()>().await,
                }
            };

            tokio::select! {
                () = due, if !delay_queue.is_empty() => {
                    let (_, data, handler) = delay_queue.pop_front().unwrap();
                    if handler.is_closed() {
                        continue;
                    }
                    let wire = self.wire_bytes(&data);
                    match writer.send(wire.clone()).await {
                        Ok(()) => {
                            self.record_bytes_sent(wire.len());
                            pending_replies.push_back((data, handler));
                        }
                        Err(e) => {
                            self.buffer.push_front((data, handler));
                            break 'connection NetworkError::FailedToSendMessage(self.address, e);
                        }
                    }
                },
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    self.buffer.push_back((data, cancel_handler));
                },
                response = reader.next() => {
                    let (data, handler) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => break 'connection NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Some(Ok(bytes)) => {
                            let _ = handler.send(bytes.freeze());
                        },
                        _ => {
                            pending_replies.push_front((data, handler));
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
        while let Some((_, data, handler)) = delay_queue.pop_back() {
            self.buffer.push_front((data, handler));
        }
        error
    }
}
