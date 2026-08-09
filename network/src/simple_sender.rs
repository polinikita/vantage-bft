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

    /// Main loop trying to connect to the peer and transmit messages. Fable perf
    /// audit item 6: dispatches to one of two loops depending on whether a
    /// per-destination latency is actually configured -- mirrors `ReliableSender::
    /// keep_alive`'s existing `extra_latency.is_zero()` split (`keep_alive_immediate`
    /// vs. `keep_alive_delayed`). Startup connect failures are retried with capped
    /// backoff so local/AWS launch skew does not discard the first queued frame.
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

    /// Wait out `delay` between connect attempts, DRAINING (and discarding) anything
    /// queued for a peer we are not connected to yet. Returns false if the channel is
    /// closed and empty -- `SimpleSender` is gone, so this task should exit instead of
    /// retrying forever.
    ///
    /// This is `ReliableSender::run`'s `'waiter` arm minus the durable/volatile split,
    /// which `SimpleSender` has no concept of: every send here is best-effort by the
    /// API's own contract ("Try (best-effort) to send"), so everything drained is
    /// dropped rather than replayed.
    ///
    /// NOT OPTIONAL, and the reason is subtle. `self.receiver` stays alive for as long
    /// as this task retries, so the 100_000-slot channel behind it stays OPEN rather
    /// than closing the way it did when a failed connect returned outright. A plain
    /// `sleep` here lets that queue fill; `SimpleSender::send`'s `tx.send(..).await`
    /// then blocks on a FULL channel instead of failing fast, and since `broadcast`
    /// walks its addresses sequentially, one unreachable peer stops delivery to every
    /// other peer. Draining also keeps a reconnect from flushing a whole outage's
    /// backlog in one burst at a peer that has just come back.
    ///
    /// Starfish reaches the same guarantee structurally instead of by draining:
    /// `network.rs`'s `make_connection` mints fresh per-session channels, so a peer
    /// that is down has no channel to accumulate into at all.
    async fn drain_until(&mut self, delay: Duration) -> bool {
        // `metrics` is cloned out first so the discard arm does not borrow `self`
        // while `self.receiver.recv()` holds it mutably inside the `select!`.
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
                    // Closed AND drained: `recv` yields buffered messages first and
                    // only then `None`, so this cannot cut a pending queue short.
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
                    // Nagle + delayed-ACK stalls sub-MSS consensus frames by up to an
                    // RTT on real WAN links (invisible on loopback); starfish sets
                    // nodelay on both sides (network.rs:423,444). Best-effort, like
                    // every other socket option here.
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

    /// Fable perf audit item 6: the zero-latency fast path -- byte-identical to the
    /// pre-D7-3 loop (no delay queue, no `Instant::now()`/`sleep_until` bookkeeping at
    /// all), used whenever no artificial per-destination latency is configured (the
    /// default). Every message still goes through the same metrics accounting as
    /// `run_delayed` does, so the bytes actually written to the wire are identical
    /// either way -- only the zero-cost scheduling differs.
    ///
    /// Batching (`self.batch.enabled`) coalesces arrivals into bundle frames before
    /// they ever reach `writer.send` -- best-effort, same as everything else here: if
    /// the connection drops mid-accumulation, whatever is still buffered is simply
    /// dropped (after a connection is established, SimpleSender still does not retry
    /// mid-session failures, on or off batching).
    async fn run_immediate(&mut self) {
        let Some(stream) = self.connect().await else {
            return;
        };
        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();

        let mut coalescer: Coalescer<()> = Coalescer::new();
        let mut coalesce_deadline: Option<tokio::time::Instant> = None;

        // Transmit messages once we have established a connection.
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
        let Some(stream) = self.connect().await else {
            return;
        };
        let (mut writer, mut reader) = Framed::new(stream, LengthDelimitedCodec::new()).split();

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
