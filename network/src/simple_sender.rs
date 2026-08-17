// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::{sleep_until_or_pending, BatchConfig, Coalescer};
use crate::error::NetworkError;
use crate::reliable_sender::record_typed_sent;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{debug, warn};
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
use tokio_util::codec::Framed;

use crate::channel_auth::{ChannelAuth, Role};
use crate::codec::{authenticated_frame_codec, frame_codec, AuthCodec};
use crate::receiver::record_auth_frame;

#[cfg(test)]
#[path = "tests/simple_sender_tests.rs"]
pub mod simple_sender_tests;

const STARTUP_RETRY_DELAY_MS: u64 = 200;
const STARTUP_RETRY_BACKOFF_MAX_MS: u64 = 2_000;

/// Maintains one TCP connection per peer.
pub struct SimpleSender {
    /// Per-peer connection channels.
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    /// RNG used to randomize broadcast destinations.
    rng: SmallRng,
    /// Optional fixed per-destination send latency.
    latency: HashMap<SocketAddr, Duration>,
    /// Optional wire metrics.
    metrics: Option<Arc<Metrics>>,
    /// Outbound batching configuration.
    batch: BatchConfig,
    /// Pairwise channel keys, when channel authentication is enabled.
    auth: Option<Arc<ChannelAuth>>,
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
            auth: None,
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

    /// Sets outbound batching.
    pub fn with_batching(mut self, config: BatchConfig) -> Self {
        self.batch = config;
        self
    }

    /// Attach pairwise channel keys before spawning connections.
    ///
    /// Only destinations this map covers are authenticated, which leaves client and
    /// same-host connections untouched without a decision at each call site.
    pub fn with_channel_auth(mut self, auth: Option<Arc<ChannelAuth>>) -> Self {
        self.auth = auth;
        self
    }

    /// Spawns a connection task.
    fn spawn_connection(&self, address: SocketAddr) -> Sender<Bytes> {
        let (tx, rx) = channel(100_000);
        let extra_latency = self.latency.get(&address).copied().unwrap_or_default();
        Connection::spawn(
            address,
            rx,
            extra_latency,
            self.metrics.clone(),
            self.batch,
            self.auth.clone(),
        );
        tx
    }

    /// Best-effort send to one address.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) {
        if let Some(tx) = self.connections.get(&address) {
            if tx.send(data.clone()).await.is_ok() {
                return;
            }
        }

        let tx = self.spawn_connection(address);
        if tx.send(data).await.is_ok() {
            self.connections.insert(address, tx);
        }
    }

    /// Best-effort broadcast to all specified addresses.
    pub async fn broadcast(&mut self, addresses: Vec<SocketAddr>, data: Bytes) {
        for address in addresses {
            self.send(address, data.clone()).await;
        }
    }

    /// Sends to up to `nodes` random addresses.
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

    /// Broadcast with a message-type metric.
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

    /// Random broadcast with a message-type metric.
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
    /// Outbound message channel.
    receiver: Receiver<Bytes>,
    /// Fixed one-way delay for this connection.
    extra_latency: Duration,
    /// Optional wire metrics.
    metrics: Option<Arc<Metrics>>,
    /// Outbound batching configuration.
    batch: BatchConfig,
    /// Pairwise channel keys, when channel authentication is enabled.
    auth: Option<Arc<ChannelAuth>>,
}

impl Connection {
    fn spawn(
        address: SocketAddr,
        receiver: Receiver<Bytes>,
        extra_latency: Duration,
        metrics: Option<Arc<Metrics>>,
        batch: BatchConfig,
        auth: Option<Arc<ChannelAuth>>,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                extra_latency,
                metrics,
                batch,
                auth,
            }
            .run()
            .await;
        });
    }

    /// Records one transmitted frame.
    fn record_sent(&self, len: usize, authenticated: bool) {
        let Some(metrics) = &self.metrics else { return };
        metrics.bytes_sent_total.inc_by(len as u64 + 4);
        metrics.network_frames_sent_total.inc();
        if authenticated {
            record_auth_frame(metrics, "sent", len);
        }
    }

    /// Connects to the peer and selects the send loop.
    async fn run(&mut self) {
        if self.extra_latency.is_zero() {
            self.run_immediate().await
        } else {
            self.run_delayed().await
        }
    }

    /// Returns the release instant for a newly scheduled message.
    fn scheduled_release(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.extra_latency
    }

    /// Wait between connection attempts while discarding queued best-effort messages.
    /// Returns false when the channel is closed and drained.
    async fn drain_until(&mut self, delay: Duration) -> bool {
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
                    None => return false,
                },
            }
        }
    }

    /// Connects and, on an authenticated destination, binds the connection to the peer's
    /// committee identity. A failed handshake is retried like a failed connect.
    async fn connect(&mut self) -> Option<(TcpStream, AuthCodec)> {
        let mut delay = STARTUP_RETRY_DELAY_MS;
        let mut retry = 0;
        loop {
            let failure = match TcpStream::connect(self.address).await {
                Ok(mut stream) => {
                    // Disable Nagle buffering for small protocol frames.
                    let _ = stream.set_nodelay(true);
                    match self.handshake(&mut stream).await {
                        Ok(codec) => {
                            debug!("Outgoing connection established with {}", self.address);
                            return Some((stream, codec));
                        }
                        Err(e) => e,
                    }
                }
                Err(e) => NetworkError::FailedToConnect(self.address, retry, e),
            };

            warn!("{}", failure);
            if !self.drain_until(Duration::from_millis(delay)).await {
                return None;
            }
            delay = min(2 * delay, STARTUP_RETRY_BACKOFF_MAX_MS);
            retry += 1;
        }
    }

    /// Builds the codec for a new connection, authenticating first when required.
    async fn handshake(&self, stream: &mut TcpStream) -> Result<AuthCodec, NetworkError> {
        let Some(peer) = self
            .auth
            .as_ref()
            .and_then(|auth| auth.peer_index(&self.address).map(|index| (auth, index)))
        else {
            return Ok(frame_codec());
        };
        let (auth, index) = peer;
        match auth.handshake_dialer(stream, index).await {
            Ok(key) => Ok(authenticated_frame_codec(key, Role::Dialer)),
            Err(e) => {
                if let Some(metrics) = &self.metrics {
                    metrics
                        .channel_auth_failures_total
                        .with_label_values(&["dialer"])
                        .inc();
                }
                Err(NetworkError::ChannelAuthFailed(self.address, e))
            }
        }
    }

    /// Send loop without an artificial delay. Batching is applied before writing.
    async fn run_immediate(&mut self) {
        let Some((stream, codec)) = self.connect().await else {
            return;
        };
        let authenticated = codec.is_authenticated();
        let (mut writer, mut reader) = Framed::new(stream, codec).split();

        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        loop {
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            tokio::select! {
                () = coalesce_due, if self.batch.enabled && !coalescer.is_empty() => {
                    let (bundle, _) = coalescer.flush();
                    coalesce_deadline = None;
                    let len = bundle.len();
                    if let Err(e) = writer.send(bundle).await {
                        warn!("{}", NetworkError::FailedToSendMessage(self.address, len, e));
                        return;
                    }
                    self.record_sent(len, authenticated);
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
                                warn!("{}", NetworkError::FailedToSendMessage(self.address, len, e));
                                return;
                            }
                            self.record_sent(len, authenticated);
                        }
                    } else {
                        let len = data.len();
                        if let Err(e) = writer.send(data).await {
                            warn!("{}", NetworkError::FailedToSendMessage(self.address, len, e));
                            return;
                        }
                        self.record_sent(len, authenticated);
                    }
                },
                response = reader.next() => {
                    match response {
                        Some(Ok(_)) => {
                        },
                        _ => {
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }

    /// Sends with a fixed per-destination delay.
    async fn run_delayed(&mut self) {
        let Some((stream, codec)) = self.connect().await else {
            return;
        };
        let authenticated = codec.is_authenticated();
        let (mut writer, mut reader) = Framed::new(stream, codec).split();

        // Preserve per-connection ordering.
        let mut delay_queue: std::collections::VecDeque<(tokio::time::Instant, Bytes)> =
            std::collections::VecDeque::new();
        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        loop {
            let due = async {
                match delay_queue.front() {
                    Some((release_at, _)) => tokio::time::sleep_until(*release_at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let coalesce_due = sleep_until_or_pending(coalesce_deadline);

            tokio::select! {
                () = due, if !delay_queue.is_empty() => {
                    let (_, data) = delay_queue.pop_front().unwrap();
                    let len = data.len();
                    if let Err(e) = writer.send(data).await {
                        warn!("{}", NetworkError::FailedToSendMessage(self.address, len, e));
                        return;
                    }
                    self.record_sent(len, authenticated);
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
                        },
                        _ => {
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }
}
