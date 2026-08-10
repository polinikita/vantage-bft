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
use std::cmp::min;
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

const STARTUP_RETRY_DELAY_MS: u64 = 200;
const STARTUP_RETRY_BACKOFF_MAX_MS: u64 = 2_000;

/// Maintains one TCP connection per peer and sends through a per-peer channel.
pub struct SimpleSender {
    /// A map holding the channels to our connections.
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    /// RNG used to randomize broadcast destinations.
    rng: SmallRng,
    /// Optional fixed per-destination send latency.
    latency: HashMap<SocketAddr, Duration>,
    /// same contract as `ReliableSender::metrics`.
    metrics: Option<Arc<Metrics>>,
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
            batch: BatchConfig::default(),
        }
    }

    /// Set fixed per-destination send latency before spawning connections.
    pub fn with_latency(mut self, map: HashMap<SocketAddr, Duration>) -> Self {
        self.latency = map;
        self
    }

    /// Attach wire metrics before spawning connections.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Same contract as `ReliableSender::with_batching`.
    pub fn with_batching(mut self, config: BatchConfig) -> Self {
        self.batch = config;
        self
    }

    /// Helper function to spawn a new connection.
    fn spawn_connection(&self, address: SocketAddr) -> Sender<Bytes> {
        let (tx, rx) = channel(100_000);
        let extra_latency = self.latency.get(&address).copied().unwrap_or_default();
        Connection::spawn(address, rx, extra_latency, self.metrics.clone(), self.batch);
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

    /// Send to up to `nodes` randomly selected addresses.
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

    /// Send with a message-type metric.
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

/// Maintains a connection to one peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<Bytes>,
    /// Fixed one-way delay for this connection.
    extra_latency: Duration,
    /// same contract as `ReliableSender::Connection::metrics`.
    metrics: Option<Arc<Metrics>>,
    /// Same contract as `ReliableSender::Connection::batch`.
    batch: BatchConfig,
}

impl Connection {
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<Bytes>,
        extra_latency: Duration,
        metrics: Option<Arc<Metrics>>,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                extra_latency,
                metrics,
                batch,
            }
            .run()
            .await;
        });
    }

    fn record_frame_sent(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.network_frames_sent_total.inc();
        }
    }

    /// Connects to the peer and selects the zero-delay or delayed send loop.
    /// Connection attempts use capped exponential backoff.
    async fn run(&mut self) {
        if self.extra_latency.is_zero() {
            self.run_immediate().await
        } else {
            self.run_delayed().await
        }
    }

    /// The release instant for a message being scheduled right now -- same contract
    /// as `ReliableSender::Connection::scheduled_release`.
    fn scheduled_release(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.extra_latency
    }

    /// Wait between connection attempts while discarding queued best-effort messages.
    /// Returns false when the channel is closed and drained.
    async fn drain_until(&mut self, delay: Duration) -> bool {
        // Clone metrics before borrowing the receiver in `select!`.
        let metrics = self.metrics.clone();
        let timer = tokio::time::sleep(delay);
        tokio::pin!(timer);
        loop {
            tokio::select! {
                () = &mut timer => return true,
                message = self.receiver.recv() => match message {
                    Some(_discarded) => {
                        if let Some(metrics) = &metrics {
                            metrics.network_connect_wait_discarded_total.inc();
                        }
                    }
                    // `recv` returns buffered messages before `None`.
                    None => return false,
                },
            }
        }
    }

    async fn connect(&mut self) -> Option<TcpStream> {
        let mut delay = STARTUP_RETRY_DELAY_MS;
        let mut retry = 0;
        loop {
            match TcpStream::connect(self.address).await {
                Ok(stream) => {
                    // Disable Nagle buffering for small protocol frames.
                    let _ = stream.set_nodelay(true);
                    info!("Outgoing connection established with {}", self.address);
                    return Some(stream);
                }
                Err(e) => {
                    warn!("{}", NetworkError::FailedToConnect(self.address, retry, e));
                    if !self.drain_until(Duration::from_millis(delay)).await {
                        return None;
                    }
                    delay = min(2 * delay, STARTUP_RETRY_BACKOFF_MAX_MS);
                    retry += 1;
                }
            }
        }
    }

    /// Send loop without an artificial delay. Batching is applied before writing.
    async fn run_immediate(&mut self) {
        let Some(stream) = self.connect().await else {
            return;
        };
        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();

        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        // Transmit messages after connecting.
        loop {
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            tokio::select! {
                () = coalesce_due, if self.batch.enabled && !coalescer.is_empty() => {
                    let (bundle, _) = coalescer.flush();
                    coalesce_deadline = None;
                    let len = bundle.len();
                    if let Err(e) = writer.send(bundle).await {
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
                            let len = bundle.len();
                            if let Err(e) = writer.send(bundle).await {
                                warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                                return;
                            }
                            if let Some(metrics) = &self.metrics {
                                metrics.bytes_sent_total.inc_by(len as u64 + 4);
                            }
                            self.record_frame_sent();
                        }
                    } else {
                        let len = data.len();
                        if let Err(e) = writer.send(data).await {
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
                            // The connection or acknowledgement stream failed.
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }

    /// Send loop for a nonzero per-destination delay. Bundles receive one release time.
    async fn run_delayed(&mut self) {
        let Some(stream) = self.connect().await else {
            return;
        };
        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();

        // A FIFO queue preserves per-connection ordering.
        let mut delay_queue: std::collections::VecDeque<(tokio::time::Instant, Bytes)> =
            std::collections::VecDeque::new();
        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        // Transmit messages after connecting.
        loop {
            let due = async {
                match delay_queue.front() {
                    Some((release_at, _)) => tokio::time::sleep_until(*release_at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            // Wait for new messages, due messages, or acknowledgements.
            tokio::select! {
                () = due, if !delay_queue.is_empty() => {
                    let (_, data) = delay_queue.pop_front().unwrap();
                    let len = data.len();
                    if let Err(e) = writer.send(data).await {
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
                    delay_queue.push_back((self.scheduled_release(), bundle));
                },
                Some(data) = self.receiver.recv() => {
                    if self.batch.enabled {
                        if coalescer.push(data, ()) {
                            coalesce_deadline = Some(tokio::time::Instant::now() + self.batch.max_delay());
                        }
                        if coalescer.over_cap(self.batch.max_bytes) {
                            let (bundle, _) = coalescer.flush();
                            coalesce_deadline = None;
                            delay_queue.push_back((self.scheduled_release(), bundle));
                        }
                    } else {
                        delay_queue.push_back((self.scheduled_release(), data));
                    }
                },
                response = reader.next() => {
                    match response {
                        Some(Ok(_)) => {
                            // Sink the reply.
                        },
                        _ => {
                            // The connection or acknowledgement stream failed.
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }
}
