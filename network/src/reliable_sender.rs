// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::{encode_bundle, sleep_until_or_pending, BatchConfig, Coalescer};
use crate::error::NetworkError;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{debug, warn};
use metrics::Metrics;
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, error::TrySendError, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, sleep_until, Duration, Instant};
use tokio_util::codec::Framed;

use crate::channel_auth::{ChannelAuth, Role};
use crate::codec::{authenticated_frame_codec, frame_codec, AuthCodec};
use crate::receiver::record_auth_frame;

#[cfg(test)]
#[path = "tests/reliable_sender_tests.rs"]
pub mod reliable_sender_tests;

/// Cancellation receiver returned for a durable send.
pub type CancelHandler = oneshot::Receiver<Bytes>;

/// Per-destination minimum key dropped from a volatile session.
pub type DirtyMap = Arc<parking_lot::Mutex<HashMap<SocketAddr, u64>>>;

/// Reply targets for a buffered entry.
type ReplyTargets = Vec<oneshot::Sender<Bytes>>;

/// Minimum key used to account for a discarded volatile entry.
type VolatileKey = Option<u64>;

/// Wire bytes, reply targets, and the optional volatile key for one entry.
type BufferedEntry = (Bytes, ReplyTargets, VolatileKey);

/// Default maximum delay between reconnect attempts, in milliseconds.
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 2_000;

/// Set immediately before an embedding process tears down its Tokio runtime.
///
/// Runtime shutdown cancels connection actors and their callers in an arbitrary
/// order. A caller that happens to run after its connection actor was cancelled
/// must park until it is cancelled too, rather than reporting a false network
/// failure during otherwise successful benchmark teardown.
static PROCESS_SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

/// Marks the current process as intentionally tearing down its Tokio runtime.
///
/// Normal node processes never call this: an unexpectedly closed internal
/// connection channel therefore remains a fatal invariant violation.
pub fn begin_process_shutdown() {
    PROCESS_SHUTTING_DOWN.store(true, Ordering::Release);
}

fn process_is_shutting_down() -> bool {
    PROCESS_SHUTTING_DOWN.load(Ordering::Acquire)
}

/// Maintains one TCP connection per peer and retries messages until acknowledgement or
/// cancellation.
pub struct ReliableSender {
    /// Per-peer connection channels.
    connections: HashMap<SocketAddr, Sender<InnerMessage>>,
    /// RNG used to randomize broadcast destinations.
    rng: SmallRng,
    /// Optional fixed per-destination send latency.
    latency: HashMap<SocketAddr, Duration>,
    /// Optional wire metrics. Disabled when `None`.
    metrics: Option<Arc<Metrics>>,
    /// Per-peer outbound batching configuration.
    batch: BatchConfig,
    /// Optional notification sent after a connection recovers from a failure.
    reconnect_events: Option<Sender<SocketAddr>>,
    /// Optional map for volatile-drop accounting.
    drop_map: Option<DirtyMap>,
    /// Maximum reconnect backoff, in milliseconds.
    retry_backoff_max_ms: u64,
    /// Queue depth at which volatile sends are shed. Zero disables shedding.
    volatile_soft_cap: usize,
    /// Pairwise channel keys, when channel authentication is enabled.
    auth: Option<Arc<ChannelAuth>>,
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
            volatile_soft_cap: 0,
            auth: None,
        }
    }

    /// Set fixed per-destination latency before spawning connections.
    pub fn with_latency(mut self, map: HashMap<SocketAddr, Duration>) -> Self {
        self.latency = map;
        self
    }

    /// Set outbound batching for subsequently spawned connections.
    pub fn with_batching(mut self, config: BatchConfig) -> Self {
        self.batch = config;
        self
    }

    /// Attach wire metrics before spawning connections.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attach a recovery notification channel.
    pub fn with_reconnect_events(mut self, tx: Sender<SocketAddr>) -> Self {
        self.reconnect_events = Some(tx);
        self
    }

    /// Attach the map used for volatile-drop accounting.
    pub fn with_drop_map(mut self, map: DirtyMap) -> Self {
        self.drop_map = Some(map);
        self
    }

    /// Set the maximum reconnect backoff in milliseconds.
    pub fn with_retry_backoff_max_ms(mut self, ms: u64) -> Self {
        self.retry_backoff_max_ms = ms;
        self
    }

    /// Set the queue depth at which volatile messages are shed.
    pub fn with_volatile_soft_cap(mut self, cap: usize) -> Self {
        self.volatile_soft_cap = cap;
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

    /// Spawn a connection task.
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
            self.auth.clone(),
        );
        tx
    }

    /// Queue a message for one connection actor.
    async fn enqueue(&mut self, address: SocketAddr, message: InnerMessage) {
        if !self.connections.contains_key(&address) {
            let tx = self.spawn_connection(address);
            self.connections.insert(address, tx);
        }
        if self
            .connections
            .get(&address)
            .unwrap()
            .send(message)
            .await
            .is_err()
        {
            if process_is_shutting_down() {
                std::future::pending::<()>().await;
            }
            panic!("Failed to send internal message");
        }
    }

    /// Reliably send a message to a specific address.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) -> CancelHandler {
        let (sender, receiver) = oneshot::channel();
        self.enqueue(
            address,
            InnerMessage {
                data,
                class: SendClass::Durable(sender),
            },
        )
        .await;
        receiver
    }

    /// Broadcast reliably and return cancel handlers in address order.
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

    /// Send to up to `nodes` randomly selected addresses.
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

    /// Send with message-type and byte metrics when metrics are enabled.
    pub async fn send_typed(
        &mut self,
        address: SocketAddr,
        data: Bytes,
        msg_type: &'static str,
    ) -> CancelHandler {
        record_typed_sent(&self.metrics, msg_type, data.len());
        self.send(address, data).await
    }

    /// Broadcast with message-type and byte metrics.
    pub async fn broadcast_typed(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
        msg_type: &'static str,
    ) -> Vec<CancelHandler> {
        // Record aggregate metrics before sending to each destination.
        record_typed_sent_n(&self.metrics, msg_type, data.len(), addresses.len() as u64);
        let mut handlers = Vec::with_capacity(addresses.len());
        for address in addresses {
            handlers.push(self.send(address, data.clone()).await);
        }
        handlers
    }

    /// Broadcast over a borrowed address list without copying it.
    pub async fn broadcast_typed_slice(
        &mut self,
        addresses: &[SocketAddr],
        data: Bytes,
        msg_type: &'static str,
    ) -> Vec<CancelHandler> {
        record_typed_sent_n(&self.metrics, msg_type, data.len(), addresses.len() as u64);
        let mut handlers = Vec::with_capacity(addresses.len());
        for address in addresses {
            handlers.push(self.send(*address, data.clone()).await);
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

    /// Send a volatile message. It is discarded at session end and accounted by key.
    /// When `volatile_soft_cap` is reached, the message is recorded as dropped without
    /// waiting for channel capacity.
    pub async fn send_volatile(&mut self, address: SocketAddr, data: Bytes, key: u64) {
        if !self.connections.contains_key(&address) {
            let tx = self.spawn_connection(address);
            self.connections.insert(address, tx);
        }
        let tx = self.connections.get(&address).unwrap();
        if self.volatile_soft_cap == 0 {
            self.enqueue(
                address,
                InnerMessage {
                    data,
                    class: SendClass::Volatile(key),
                },
            )
            .await;
            return;
        }
        let depth = tx.max_capacity().saturating_sub(tx.capacity());
        if depth >= self.volatile_soft_cap {
            self.record_volatile_shed(address, key);
            return;
        }
        match tx.try_send(InnerMessage {
            data,
            class: SendClass::Volatile(key),
        }) {
            Ok(()) => {}
            // The depth check normally handles this case first.
            Err(TrySendError::Full(_)) => self.record_volatile_shed(address, key),
            // A closed connection channel is a sender error, not a dropped message.
            Err(TrySendError::Closed(_)) if process_is_shutting_down() => {}
            Err(TrySendError::Closed(_)) => panic!("Failed to send internal message"),
        }
    }

    /// Record a shed key and increment its metric.
    fn record_volatile_shed(&self, address: SocketAddr, key: u64) {
        if let Some(map) = &self.drop_map {
            let mut guard = map.lock();
            guard
                .entry(address)
                .and_modify(|existing| *existing = (*existing).min(key))
                .or_insert(key);
        }
        if let Some(metrics) = &self.metrics {
            metrics.network_volatile_shed_total.inc();
        }
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
    ///
    /// Broadcast a volatile message over a borrowed address list with aggregate metrics.
    pub async fn broadcast_volatile_typed(
        &mut self,
        addresses: &[SocketAddr],
        data: Bytes,
        key: u64,
        msg_type: &'static str,
    ) {
        record_typed_sent_n(&self.metrics, msg_type, data.len(), addresses.len() as u64);
        for address in addresses {
            self.send_volatile(*address, data.clone(), key).await;
        }
    }

    /// Send a durable message without returning a cancellation handler.
    pub async fn send_detached(&mut self, address: SocketAddr, data: Bytes) {
        self.enqueue(
            address,
            InnerMessage {
                data,
                class: SendClass::DurableDetached,
            },
        )
        .await;
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

/// Updates typed wire metrics when metrics are enabled.
pub(crate) fn record_typed_sent(
    metrics: &Option<Arc<Metrics>>,
    msg_type: &'static str,
    len: usize,
) {
    record_typed_sent_n(metrics, msg_type, len, 1);
}

/// Records typed metrics for `n` destinations.
pub(crate) fn record_typed_sent_n(
    metrics: &Option<Arc<Metrics>>,
    msg_type: &'static str,
    len: usize,
    n: u64,
) {
    if n == 0 {
        return;
    }
    if let Some(metrics) = metrics {
        metrics
            .network_messages_sent_total
            .with_label_values(&[msg_type])
            .inc_by(n);
        metrics
            .network_bytes_sent_total
            .with_label_values(&[msg_type])
            .inc_by(len as u64 * n);
    }
}

/// Message delivery class.
#[derive(Debug)]
enum SendClass {
    Durable(oneshot::Sender<Bytes>),
    DurableDetached,
    Volatile(u64),
}

/// Message sent to a reliable connection task.
#[derive(Debug)]
struct InnerMessage {
    /// The data to transmit.
    data: Bytes,
    /// Delivery class.
    class: SendClass,
}

/// Returns true when all non-empty reply targets are closed.
fn all_closed(handlers: &ReplyTargets) -> bool {
    !handlers.is_empty() && handlers.iter().all(|h| h.is_closed())
}

/// Sends acknowledgement bytes to all reply targets.
fn notify_all(handlers: ReplyTargets, bytes: Bytes) {
    for handler in handlers {
        let _ = handler.send(bytes.clone());
    }
}

/// Merge a dropped key into the per-connection minimum.
fn merge_min_key(acc: &mut Option<u64>, key: u64) {
    *acc = Some(acc.map_or(key, |m| m.min(key)));
}

/// Reliable connection to one peer.
struct Connection {
    /// Destination address.
    address: SocketAddr,
    /// Incoming send commands.
    receiver: Receiver<InnerMessage>,
    /// Initial reconnect delay, in milliseconds.
    retry_delay: u64,
    /// Messages waiting for transmission or retry.
    buffer: VecDeque<BufferedEntry>,
    /// Fixed one-way delay for this connection.
    extra_latency: Duration,
    /// Optional wire metrics. Bytes include the frame prefix.
    metrics: Option<Arc<Metrics>>,
    /// Batching configuration for this connection.
    batch: BatchConfig,
    /// Optional recovery notification channel.
    reconnect_events: Option<Sender<SocketAddr>>,
    /// Optional volatile-drop map.
    drop_map: Option<DirtyMap>,
    /// Maximum reconnect backoff, in milliseconds.
    retry_backoff_max_ms: u64,
    /// Whether this connection has failed before.
    had_failure: bool,
    /// Pairwise channel keys, when channel authentication is enabled.
    auth: Option<Arc<ChannelAuth>>,
}

impl Connection {
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
        auth: Option<Arc<ChannelAuth>>,
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
                auth,
            }
            .run()
            .await;
        });
    }

    /// Length-delimited frame prefix size.
    const FRAME_PREFIX_LEN: u64 = 4;

    /// Record one physical wire frame.
    fn record_sent(&self, len: usize, authenticated: bool) {
        let Some(metrics) = &self.metrics else { return };
        metrics
            .bytes_sent_total
            .inc_by(len as u64 + Self::FRAME_PREFIX_LEN);
        metrics.network_frames_sent_total.inc();
        if authenticated {
            record_auth_frame(metrics, "sent", len);
        }
    }

    /// Connects and, on an authenticated destination, binds the connection to the peer's
    /// committee identity.
    ///
    /// The tag is applied by the codec, so a payload requeued from a session that failed
    /// is re-tagged under the new session's key and counter when it goes out again.
    async fn connect(&self, retry: u16) -> Result<(TcpStream, AuthCodec), NetworkError> {
        let mut stream = TcpStream::connect(self.address)
            .await
            .map_err(|e| NetworkError::FailedToConnect(self.address, retry, e))?;
        // Disable Nagle buffering for small protocol frames.
        let _ = stream.set_nodelay(true);

        let Some((auth, index)) = self
            .auth
            .as_ref()
            .and_then(|auth| auth.peer_index(&self.address).map(|index| (auth, index)))
        else {
            return Ok((stream, frame_codec()));
        };
        match auth.handshake_dialer(&mut stream, index).await {
            Ok(key) => Ok((stream, authenticated_frame_codec(key, Role::Dialer))),
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

    /// Record the lowest volatile key discarded for this destination.
    fn report_dropped(&self, min_key: Option<u64>) {
        let Some(key) = min_key else { return };
        let Some(map) = &self.drop_map else { return };
        let mut guard = map.lock();
        guard
            .entry(self.address)
            .and_modify(|existing| *existing = (*existing).min(key))
            .or_insert(key);
    }

    /// Connects and transmits messages until the session fails.
    async fn run(&mut self) {
        let mut delay = self.retry_delay;
        let mut retry = 0;
        loop {
            match self.connect(retry).await {
                Ok((stream, codec)) => {
                    debug!("Outgoing connection established with {}", self.address);

                    if self.had_failure {
                        if let Some(tx) = &self.reconnect_events {
                            let _ = tx.try_send(self.address);
                        }
                    }

                    delay = self.retry_delay;
                    retry = 0;

                    let error = self.keep_alive(stream, codec).await;
                    self.had_failure = true;
                    warn!("{}", error);
                }
                Err(e) => {
                    self.had_failure = true;
                    warn!("{}", e);
                    let timer = sleep(Duration::from_millis(delay));
                    tokio::pin!(timer);

                    'waiter: loop {
                        tokio::select! {
                            () = &mut timer => {
                                delay = min(2*delay, self.retry_backoff_max_ms);
                                retry +=1;
                                break 'waiter;
                            },

                            Some(InnerMessage{data, class}) = self.receiver.recv() => {
                                match class {
                                    SendClass::Durable(h) => {
                                        let data = if self.batch.enabled { encode_bundle(&[data]) } else { data };
                                        self.buffer.push_back((data, vec![h], None));
                                        self.buffer.retain(|(_, handlers, _)| !all_closed(handlers));
                                    }
                                    SendClass::DurableDetached => {
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

    /// Transmit messages using the immediate or delayed path.
    async fn keep_alive(&mut self, stream: TcpStream, codec: AuthCodec) -> NetworkError {
        if self.extra_latency.is_zero() {
            self.keep_alive_immediate(stream, codec).await
        } else {
            self.keep_alive_delayed(stream, codec).await
        }
    }

    /// Release instant for a newly scheduled message.
    fn scheduled_release(&self) -> Instant {
        Instant::now() + self.extra_latency
    }

    /// Routes an incoming message to its class-specific buffer or coalescer.
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

    /// Transmit without an artificial delay. Batching occurs before this loop.
    async fn keep_alive_immediate(&mut self, stream: TcpStream, codec: AuthCodec) -> NetworkError {
        let mut pending_replies: VecDeque<BufferedEntry> = VecDeque::new();
        let mut durable_coalescer: Coalescer<oneshot::Sender<Bytes>> = Coalescer::new();
        let mut durable_deadline: Option<Instant> = None;
        let mut volatile_coalescer: Coalescer<u64> = Coalescer::new();
        let mut volatile_deadline: Option<Instant> = None;

        let authenticated = codec.is_authenticated();
        let (mut writer, mut reader) = Framed::new(stream, codec).split();
        let error = 'connection: loop {
            while let Some((data, handlers, key)) = self.buffer.pop_front() {
                if all_closed(&handlers) {
                    continue;
                }

                match writer.send(data.clone()).await {
                    Ok(()) => {
                        self.record_sent(data.len(), authenticated);
                        pending_replies.push_back((data, handlers, key));
                    }
                    Err(e) => {
                        let len = data.len();
                        self.buffer.push_front((data, handlers, key));
                        break 'connection NetworkError::FailedToSendMessage(self.address, len, e);
                    }
                }
            }

            let durable_due = sleep_until_or_pending(durable_deadline);
            let volatile_due = sleep_until_or_pending(volatile_deadline);

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

        // Requeue durable entries and account for dropped volatile entries.
        let mut min_dropped_key: Option<u64> = None;
        while let Some((data, handlers, key)) = pending_replies.pop_back() {
            match key {
                Some(k) => merge_min_key(&mut min_dropped_key, k),
                None => self.buffer.push_front((data, handlers, None)),
            }
        }
        // Preserve bundle framing for unflushed durable messages.
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

    /// Transmit with a fixed per-connection delay.
    async fn keep_alive_delayed(&mut self, stream: TcpStream, codec: AuthCodec) -> NetworkError {
        let mut pending_replies: VecDeque<BufferedEntry> = VecDeque::new();
        let mut delay_queue: VecDeque<(Instant, Bytes, ReplyTargets, VolatileKey)> =
            VecDeque::new();
        let mut durable_coalescer: Coalescer<oneshot::Sender<Bytes>> = Coalescer::new();
        let mut durable_deadline: Option<Instant> = None;
        let mut volatile_coalescer: Coalescer<u64> = Coalescer::new();
        let mut volatile_deadline: Option<Instant> = None;

        let authenticated = codec.is_authenticated();
        let (mut writer, mut reader) = Framed::new(stream, codec).split();
        let error = 'connection: loop {
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
                            self.record_sent(data.len(), authenticated);
                            pending_replies.push_back((data, handlers, key));
                        }
                        Err(e) => {
                            let len = data.len();
                            self.buffer.push_front((data, handlers, key));
                            break 'connection NetworkError::FailedToSendMessage(self.address, len, e);
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

        // Requeue durable entries and account for dropped volatile entries.
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
