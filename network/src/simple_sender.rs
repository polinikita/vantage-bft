// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::{sleep_until_or_pending, BatchConfig, Coalescer};
use crate::error::NetworkError;
use crate::reliable_sender::record_typed_sent;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use metrics::Metrics;
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/simple_sender_tests.rs"]
pub mod simple_sender_tests;

/// We keep alive one TCP connection per peer, each connection is handled by a separate task (called `Connection`).
/// We communicate with our 'connections' through a dedicated channel kept by the HashMap called `connections`.
pub struct SimpleSender {
    /// A map holding the channels to our connections.
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): per-destination artificial send
    /// latency, empty by default (current behavior, byte-identical). See
    /// `network/src/lib.rs`'s module doc for the injection point/semantics.
    latency: HashMap<SocketAddr, Duration>,
    /// METRICS-DASHBOARD-SPEC.md §1: same contract as `ReliableSender::metrics`.
    metrics: Option<Arc<Metrics>>,
    /// METRICS-DASHBOARD-SPEC.md §8: same contract as `ReliableSender::compress`.
    compress: bool,
    /// Same contract as `ReliableSender::batch` (see `network::batch`'s module doc).
    batch: BatchConfig,
}

impl std::default::Default for SimpleSender {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            rng: SmallRng::from_entropy(),
            latency: HashMap::new(),
            metrics: None,
            compress: false,
            batch: BatchConfig::default(),
        }
    }

    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): attach a per-destination
    /// artificial latency map (same contract as `ReliableSender::with_latency`) --
    /// call BEFORE any connection to an address in the map is spawned.
    pub fn with_latency(mut self, map: HashMap<SocketAddr, Duration>) -> Self {
        self.latency = map;
        self
    }

    /// METRICS-DASHBOARD-SPEC.md §1: attach a wire-metrics handle (same contract as
    /// `ReliableSender::with_metrics`).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// METRICS-DASHBOARD-SPEC.md §8: same contract as `ReliableSender::with_compression`.
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    /// Same contract as `ReliableSender::with_batching`.
    pub fn with_batching(mut self, config: BatchConfig) -> Self {
        self.batch = config;
        self
    }

    /// Helper function to spawn a new connection.
    fn spawn_connection(&self, address: SocketAddr) -> Sender<Bytes> {
        let (tx, rx) = channel(1_000);
        let extra_latency = self.latency.get(&address).copied().unwrap_or_default();
        Connection::spawn(
            address,
            rx,
            extra_latency,
            self.metrics.clone(),
            self.compress,
            self.batch,
        );
        tx
    }

    /// Try (best-effort) to send a message to a specific address.
    /// This is useful to answer sync requests.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) {
        // Try to re-use an existing connection if possible.
        if let Some(tx) = self.connections.get(&address) {
            if tx.send(data.clone()).await.is_ok() {
                return;
            }
        }

        // Otherwise make a new connection.
        let tx = self.spawn_connection(address);
        if tx.send(data).await.is_ok() {
            self.connections.insert(address, tx);
        }
    }

    /// Try (best-effort) to broadcast the message to all specified addresses.
    pub async fn broadcast(&mut self, addresses: Vec<SocketAddr>, data: Bytes) {
        for address in addresses {
            self.send(address, data.clone()).await;
        }
    }

    /// Pick a few addresses at random (specified by `nodes`) and try (best-effort) to send the
    /// message only to them. This is useful to pick nodes with whom to sync.
    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }

    /// METRICS-DASHBOARD-SPEC.md §1: typed variant of `send` (see `ReliableSender::
    /// send_typed`).
    pub async fn send_typed(&mut self, address: SocketAddr, data: Bytes, msg_type: &'static str) {
        record_typed_sent(&self.metrics, msg_type, data.len());
        self.send(address, data).await;
    }

    /// Typed variant of `broadcast`.
    pub async fn broadcast_typed(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
        msg_type: &'static str,
    ) {
        for address in addresses {
            self.send_typed(address, data.clone(), msg_type).await;
        }
    }

    /// Typed variant of `lucky_broadcast`.
    pub async fn lucky_broadcast_typed(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
        msg_type: &'static str,
    ) {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast_typed(addresses, data, msg_type).await
    }
}

/// A connection is responsible to establish and keep alive (if possible) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<Bytes>,
    /// PHASE7-PREP-NOTES.md (WAN-shaped local runs): this connection's own fixed
    /// artificial one-way delay to `address` (`Duration::ZERO` = off, the default).
    extra_latency: Duration,
    /// METRICS-DASHBOARD-SPEC.md §1: same contract as `ReliableSender::Connection::metrics`.
    metrics: Option<Arc<Metrics>>,
    /// METRICS-DASHBOARD-SPEC.md §8: same contract as `ReliableSender::Connection::compress`.
    compress: bool,
    /// Same contract as `ReliableSender::Connection::batch`.
    batch: BatchConfig,
}

impl Connection {
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<Bytes>,
        extra_latency: Duration,
        metrics: Option<Arc<Metrics>>,
        compress: bool,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                extra_latency,
                metrics,
                compress,
                batch,
            }
            .run()
            .await;
        });
    }

    /// METRICS-DASHBOARD-SPEC.md §8: same contract as `ReliableSender::wire_bytes`.
    /// When batching is also on, `data` here is already the whole bundle frame (same
    /// composition order as `ReliableSender`: coalesce -> compress -> outer frame).
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

    fn record_frame_sent(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.network_frames_sent_total.inc();
        }
    }

    /// Main loop trying to connect to the peer and transmit messages. Fable perf
    /// audit item 6: dispatches to one of two loops depending on whether a
    /// per-destination latency is actually configured -- mirrors `ReliableSender::
    /// keep_alive`'s existing `extra_latency.is_zero()` split (`keep_alive_immediate`
    /// vs. `keep_alive_delayed`).
    async fn run(&mut self) {
        if self.extra_latency.is_zero() {
            self.run_immediate().await
        } else {
            self.run_delayed().await
        }
    }

    /// Fable perf audit item 6: the zero-latency fast path -- byte-identical to the
    /// pre-D7-3 loop (no delay queue, no `Instant::now()`/`sleep_until` bookkeeping at
    /// all), used whenever no artificial per-destination latency is configured (the
    /// default). Every message still goes through `wire_bytes`/metrics exactly as
    /// `run_delayed` does, so the bytes actually written to the wire are identical
    /// either way -- only the zero-cost scheduling differs.
    ///
    /// Batching (`self.batch.enabled`) coalesces arrivals into bundle frames before
    /// they ever reach `writer.send` -- best-effort, same as everything else here: if
    /// the connection drops mid-accumulation, whatever is still buffered is simply
    /// dropped (SimpleSender never retries, on or off batching).
    async fn run_immediate(&mut self) {
        // Try to connect to the peer.
        let (mut writer, mut reader) = match TcpStream::connect(self.address).await {
            Ok(stream) => Framed::new(stream, LengthDelimitedCodec::new()).split(),
            Err(e) => {
                warn!(
                    "{}",
                    NetworkError::FailedToConnect(self.address, /* retry */ 0, e)
                );
                return;
            }
        };
        info!("Outgoing connection established with {}", self.address);

        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        // Transmit messages once we have established a connection.
        loop {
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            tokio::select! {
                () = coalesce_due, if self.batch.enabled && !coalescer.is_empty() => {
                    let (bundle, _) = coalescer.flush();
                    coalesce_deadline = None;
                    let wire = self.wire_bytes(&bundle);
                    let len = wire.len();
                    if let Err(e) = writer.send(wire).await {
                        warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                        return;
                    }
                    if let Some(metrics) = &self.metrics {
                        metrics.bytes_sent_total.inc_by(len as u64 + 4);
                    }
                    self.record_frame_sent();
                },
                Some(data) = self.receiver.recv() => {
                    if self.batch.enabled {
                        if coalescer.push(data, ()) {
                            coalesce_deadline = Some(tokio::time::Instant::now() + self.batch.max_delay());
                        }
                        if coalescer.over_cap(self.batch.max_bytes) {
                            let (bundle, _) = coalescer.flush();
                            coalesce_deadline = None;
                            let wire = self.wire_bytes(&bundle);
                            let len = wire.len();
                            if let Err(e) = writer.send(wire).await {
                                warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                                return;
                            }
                            if let Some(metrics) = &self.metrics {
                                metrics.bytes_sent_total.inc_by(len as u64 + 4);
                            }
                            self.record_frame_sent();
                        }
                    } else {
                        let wire = self.wire_bytes(&data);
                        let len = wire.len();
                        if let Err(e) = writer.send(wire).await {
                            warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                            return;
                        }
                        if let Some(metrics) = &self.metrics {
                            metrics.bytes_sent_total.inc_by(len as u64 + 4);
                        }
                        self.record_frame_sent();
                    }
                },
                response = reader.next() => {
                    match response {
                        Some(Ok(_)) => {
                            // Sink the reply.
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }

    /// The pre-existing delay-queue loop, used whenever a nonzero per-destination
    /// latency is configured. See `Connection::run`'s doc comment. Batching coalesces
    /// arrivals the same way as `run_immediate`; a flushed bundle is treated as a
    /// single fresh "arrival" into `delay_queue` (one release time, one injected
    /// latency for the whole bundle), computed at flush time.
    async fn run_delayed(&mut self) {
        // Try to connect to the peer.
        let (mut writer, mut reader) = match TcpStream::connect(self.address).await {
            Ok(stream) => Framed::new(stream, LengthDelimitedCodec::new()).split(),
            Err(e) => {
                warn!(
                    "{}",
                    NetworkError::FailedToConnect(self.address, /* retry */ 0, e)
                );
                return;
            }
        };
        info!("Outgoing connection established with {}", self.address);

        // D7-3: a plain FIFO delay queue, same reasoning as `ReliableSender::
        // keep_alive_delayed` (every message on this link gets the identical fixed
        // delay, so arrival order implies release-order -- no jitter, no concurrency,
        // strict ordering preserved by construction).
        let mut delay_queue: std::collections::VecDeque<(tokio::time::Instant, Bytes)> =
            std::collections::VecDeque::new();
        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        // Transmit messages once we have established a connection.
        loop {
            let due = async {
                match delay_queue.front() {
                    Some((release_at, _)) => tokio::time::sleep_until(*release_at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                () = due, if !delay_queue.is_empty() => {
                    let (_, data) = delay_queue.pop_front().unwrap();
                    let wire = self.wire_bytes(&data);
                    let len = wire.len();
                    if let Err(e) = writer.send(wire).await {
                        warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                        return;
                    }
                    if let Some(metrics) = &self.metrics {
                        metrics.bytes_sent_total.inc_by(len as u64 + 4);
                    }
                    self.record_frame_sent();
                },
                () = coalesce_due, if self.batch.enabled && !coalescer.is_empty() => {
                    let (bundle, _) = coalescer.flush();
                    coalesce_deadline = None;
                    delay_queue.push_back((tokio::time::Instant::now() + self.extra_latency, bundle));
                },
                Some(data) = self.receiver.recv() => {
                    if self.batch.enabled {
                        if coalescer.push(data, ()) {
                            coalesce_deadline = Some(tokio::time::Instant::now() + self.batch.max_delay());
                        }
                        if coalescer.over_cap(self.batch.max_bytes) {
                            let (bundle, _) = coalescer.flush();
                            coalesce_deadline = None;
                            delay_queue.push_back((tokio::time::Instant::now() + self.extra_latency, bundle));
                        }
                    } else {
                        delay_queue.push_back((tokio::time::Instant::now() + self.extra_latency, data));
                    }
                },
                response = reader.next() => {
                    match response {
                        Some(Ok(_)) => {
                            // Sink the reply.
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }
}
