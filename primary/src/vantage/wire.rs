use crate::delayed_header::DelayedHeaderSender;
use crate::messages::Header;
use crate::primary::PrimaryMessage;
use crate::vantage::node::Inbound;
use crate::vantage::resume::InFlightEntry;
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{with_public_key_index_codec, PublicKey, PublicKeyIndexCodec, PublicKeyIndexError};
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, ChannelAuth, DirtyMap, ReliableSender, SimpleSender};
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot::error::TryRecvError;

pub(crate) type InFlightMap = Arc<Mutex<HashMap<PublicKey, InFlightEntry>>>;

const COMPACT_ID_PREFIX: &[u8; 4] = b"VCI1";

/// Encodes Vantage primary messages with committee indices and reads legacy frames.
#[derive(Clone, Debug)]
pub(crate) struct VantageWireCodec {
    compact_outbound: bool,
    public_keys: Option<PublicKeyIndexCodec>,
}

impl VantageWireCodec {
    pub(crate) fn new(
        committee: &Committee,
        compact_outbound: bool,
    ) -> Result<Self, PublicKeyIndexError> {
        let public_keys = match PublicKeyIndexCodec::new(committee.authorities.keys().copied()) {
            Ok(codec) => Some(codec),
            Err(_) if !compact_outbound => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            compact_outbound,
            public_keys,
        })
    }

    pub(crate) fn serialize(&self, message: &PrimaryMessage) -> bincode::Result<Vec<u8>> {
        if !self.compact_outbound {
            return bincode::serialize(message);
        }
        let codec = self.compact_codec()?;
        let body_len = with_public_key_index_codec(codec, || bincode::serialized_size(message))?;
        let body_len =
            usize::try_from(body_len).map_err(|_| Box::new(bincode::ErrorKind::SizeLimit))?;
        let capacity = COMPACT_ID_PREFIX
            .len()
            .checked_add(body_len)
            .ok_or_else(|| Box::new(bincode::ErrorKind::SizeLimit))?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(COMPACT_ID_PREFIX);
        with_public_key_index_codec(codec, || bincode::serialize_into(&mut bytes, message))?;
        Ok(bytes)
    }

    pub(crate) fn deserialize(&self, bytes: &[u8]) -> bincode::Result<PrimaryMessage> {
        let Some(body) = bytes.strip_prefix(COMPACT_ID_PREFIX) else {
            return bincode::deserialize(bytes);
        };
        let codec = self.compact_codec()?;
        with_public_key_index_codec(codec, || bincode::deserialize(body))
    }

    fn serialized_value_size<T: Serialize>(&self, value: &T) -> bincode::Result<u64> {
        if !self.compact_outbound {
            return bincode::serialized_size(value);
        }
        let codec = self.compact_codec()?;
        with_public_key_index_codec(codec, || bincode::serialized_size(value))
    }

    fn serialized_message_size(&self, message: &PrimaryMessage) -> bincode::Result<u64> {
        let size = self.serialized_value_size(message)?;
        if self.compact_outbound {
            size.checked_add(COMPACT_ID_PREFIX.len() as u64)
                .ok_or_else(|| Box::new(bincode::ErrorKind::SizeLimit))
        } else {
            Ok(size)
        }
    }

    fn compact_codec(&self) -> bincode::Result<&PublicKeyIndexCodec> {
        self.public_keys.as_ref().ok_or_else(|| {
            Box::new(bincode::ErrorKind::Custom(
                "compact Vantage frame requires at most 256 committee members".to_owned(),
            ))
        })
    }
}

/// Returns the sender identity claimed by a message, when the message carries one.
pub trait DeclaredSender {
    fn declared_sender(&self) -> Option<PublicKey>;
}

impl DeclaredSender for Inbound {
    fn declared_sender(&self) -> Option<PublicKey> {
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
            Inbound::SequenceAnnounce(_, s) => Some(*s),
            Inbound::SequenceAnnounceBatch(_, s) => Some(*s),
            Inbound::SequenceRequest(_, s) => Some(*s),
            Inbound::SequenceRecords(_, s) => Some(*s),
            Inbound::SequenceDeltaRequest(_, s) => Some(*s),
            Inbound::SequenceDelta(_, s) => Some(*s),
            Inbound::SequenceDeltaRangeRequest(_, s) => Some(*s),
            Inbound::SequenceDeltaRange(_, s) => Some(*s),
            Inbound::SequenceOutcomeRequest(_, s) => Some(*s),
            Inbound::SequenceOutcome(_, s) => Some(*s),
            Inbound::SequenceUnavailable(_, s) => Some(*s),
            Inbound::SequenceHeadersRequest(_, s) => Some(*s),
            Inbound::SequenceHeaders(_, s) => Some(*s),
            Inbound::ResolutionWitness(w) => Some(w.sender),
            Inbound::ResolutionWish(w) => Some(w.sender),
            Inbound::ResolutionSuggest(s) => Some(s.sender),
            Inbound::ResolutionProof(p) => Some(p.sender),
            Inbound::ResolutionProposal(p) => Some(p.sender),
            Inbound::ResolutionStatement(s) => Some(s.sender),
            Inbound::ResolutionDone(d) => Some(d.sender),
            Inbound::ResolutionCarrierFetch(_, _, requester) => Some(*requester),
            Inbound::ResolutionBlockFetch(_, _, requester) => Some(*requester),
            Inbound::ResolutionDecisionRequest(_, requester) => Some(*requester),
            Inbound::SkipVote(_, s) => Some(*s),
            Inbound::EchoDigest(d) => Some(d.sender),
            Inbound::ReadyDigest(d) => Some(d.sender),
            Inbound::BodyFetch(_, _, s) => Some(*s),
            Inbound::LaneResume(_, _, requester) => Some(*requester),
            Inbound::ResumeHello(_, sender) => Some(*sender),
            Inbound::ReplayDone(_, _, _, sender) => Some(*sender),
            // These messages are authenticated by position, content, or downstream state.
            Inbound::Serve(_)
            | Inbound::AckAvailability(_)
            | Inbound::Propose(_)
            | Inbound::ResolutionCarrierServe(_, _)
            | Inbound::ResolutionBlockServe(_)
            | Inbound::BodyServe(_, _) => None,
        }
    }
}

/// Rejects a declared sender that is not a committee member.
pub fn sender_is_member<M: DeclaredSender>(m: &M, members: &HashSet<PublicKey>) -> bool {
    match m.declared_sender() {
        Some(sender) => members.contains(&sender),
        None => true,
    }
}

pub(crate) type WithheldHeaderDests = Option<(Vec<SocketAddr>, Vec<(PublicKey, SocketAddr)>)>;

pub struct Wire {
    pub(crate) codec: VantageWireCodec,
    pub(crate) network: ReliableSender,
    pub(crate) worker_network: SimpleSender,
    pub(crate) resume_lane_tx: mpsc::Sender<LaneSend>,
    pub(crate) replay_tx: ReplaySender,
    pub(crate) sequence_tx: mpsc::Sender<SequenceSend>,
    pub(crate) serve_tx: mpsc::Sender<ServeSend>,
    pub(crate) replay_generation: AtomicU64,
    pub(crate) cancel_handlers: Vec<CancelHandler>,
    pub(crate) last_prune_len: usize,

    pub(crate) other_primaries: Vec<(PublicKey, SocketAddr)>,
    pub(crate) other_primary_addrs: Vec<SocketAddr>,
    pub(crate) worker_addresses: HashMap<WorkerId, SocketAddr>,

    pub(crate) withheld_header_dests: WithheldHeaderDests,
    /// Peers to which this selected publisher refuses lane repair.
    pub(crate) suppressed_repair_destinations: Option<HashSet<PublicKey>>,

    /// Prompt recipients and the dedicated finite-delay sender, for selected authors.
    pub(crate) late_header: Option<(Vec<SocketAddr>, DelayedHeaderSender)>,

    pub(crate) withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,

    pub(crate) metrics: Option<Arc<Metrics>>,

    pub(crate) addr_to_peer: HashMap<SocketAddr, PublicKey>,
    pub(crate) dirty_map: DirtyMap,
    pub(crate) in_flight: InFlightMap,
}

impl Wire {
    pub(crate) fn serialize_message(&self, message: &PrimaryMessage) -> Vec<u8> {
        self.codec.serialize(message).expect("serializes")
    }

    /// Removes only handlers that resolved or closed; dropping a pending handler cancels retries.
    pub(crate) fn prune_cancel_handlers(&mut self) {
        self.cancel_handlers
            .retain_mut(|handler| matches!(handler.try_recv(), Err(TryRecvError::Empty)));
        self.last_prune_len = self.cancel_handlers.len();
    }

    pub(crate) fn maybe_prune_cancel_handlers(&mut self) {
        if self.cancel_handlers.len() >= 2 * self.last_prune_len.max(1) {
            self.prune_cancel_handlers();
        }
    }

    pub(crate) async fn broadcast_message(&mut self, message: PrimaryMessage) {
        let msg_type = message.type_name();
        let bytes = self.serialize_message(&message);
        if let PrimaryMessage::Header(header, false) = &message {
            if let Some(metrics) = &self.metrics {
                metrics.proposed_block_size_bytes.observe(bytes.len());
                let header_len = self
                    .codec
                    .serialized_value_size(header)
                    .expect("serializes") as usize;
                metrics.proposed_header_size_bytes.observe(header_len);
            }
            if let Some((addrs, _)) = self.withheld_header_dests.clone() {
                if config::withhold_active(self.withhold_window.as_deref(), Instant::now()) {
                    self.broadcast_to(bytes, msg_type, addrs).await;
                    return;
                }
            }
            if let Some((prompt, delayed)) = &mut self.late_header {
                let payload = Bytes::from(bytes);
                let mut handlers = self
                    .network
                    .broadcast_typed_slice(prompt, payload.clone(), msg_type)
                    .await;
                handlers.extend(delayed.broadcast(payload).await);
                self.cancel_handlers.extend(handlers);
                return;
            }
        }
        self.broadcast(bytes, msg_type).await;
    }

    pub(crate) async fn send_message(&mut self, peer: PublicKey, message: PrimaryMessage) {
        let msg_type = message.type_name();
        let bytes = self.serialize_message(&message);
        self.send_to(peer, bytes, msg_type).await;
    }

    pub(crate) async fn send_repair_message(&mut self, peer: PublicKey, message: PrimaryMessage) {
        if self.repair_suppressed_for(&peer) {
            return;
        }
        self.send_message(peer, message).await;
    }

    pub(crate) async fn broadcast_volatile(
        &mut self,
        payload: Bytes,
        msg_type: &'static str,
        key: u64,
    ) {
        self.network
            .broadcast_volatile_typed(&self.other_primary_addrs, payload, key, msg_type)
            .await;
    }

    pub(crate) async fn send_volatile(
        &mut self,
        peer: PublicKey,
        message: PrimaryMessage,
        key: u64,
    ) {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return;
        };
        let msg_type = message.type_name();
        let bytes = Bytes::from(self.serialize_message(&message));
        self.network
            .send_volatile_typed(addr, bytes, key, msg_type)
            .await;
    }

    async fn broadcast(&mut self, payload: Vec<u8>, msg_type: &'static str) {
        let handlers = self
            .network
            .broadcast_typed_slice(&self.other_primary_addrs, Bytes::from(payload), msg_type)
            .await;
        self.cancel_handlers.extend(handlers);
    }

    async fn broadcast_to(
        &mut self,
        payload: Vec<u8>,
        msg_type: &'static str,
        addrs: Vec<SocketAddr>,
    ) {
        let handlers = self
            .network
            .broadcast_typed(addrs, Bytes::from(payload), msg_type)
            .await;
        self.cancel_handlers.extend(handlers);
    }

    /// Enqueues state-sync traffic without blocking the core run loop.
    pub(crate) fn try_send_sequence(&self, peer: &PublicKey, message: PrimaryMessage) -> bool {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| pk == peer)
            .map(|(_, a)| *a)
        else {
            return false;
        };
        self.sequence_tx
            .try_send(SequenceSend(addr, message))
            .is_ok()
    }

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

    /// Enqueues lane recovery without blocking; the requester retries dropped work.
    pub(crate) fn enqueue_resume(&self, peer: PublicKey, message: PrimaryMessage) {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return;
        };
        if self
            .resume_lane_tx
            .try_send(LaneSend(addr, message))
            .is_err()
        {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_lane_resume_send_drops.inc();
            }
        }
    }

    /// Reserves the complete replay size before admitting the stream.
    pub(crate) fn enqueue_replay(
        &self,
        peer: PublicKey,
        generation: u64,
        msgs: Vec<Bytes>,
        done: PrimaryMessage,
    ) -> bool {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return false;
        };
        let ok = self.replay_tx.try_send(ReplaySend {
            to: addr,
            peer,
            generation,
            msgs,
            done,
            reserved_bytes: 0,
        });
        if !ok {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_replay_enqueue_drops_total.inc();
            }
        }
        ok
    }

    /// Returns a generation that makes completion conditional on the admitted stream.
    pub(crate) fn next_replay_generation(&self) -> u64 {
        self.replay_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("replay generation exhausted")
    }

    pub(crate) fn enqueue_resume_header(&self, peer: PublicKey, header: Header) {
        if self.repair_suppressed_for(&peer) {
            return;
        }
        self.enqueue_resume(peer, PrimaryMessage::Header(header, false));
    }

    /// Enqueues a response to a peer request off the core; returns whether it was handled.
    ///
    /// An unknown peer counts as handled because the direct send path also has no
    /// destination for one.
    pub(crate) fn try_enqueue_serve(&self, peer: PublicKey, message: PrimaryMessage) -> bool {
        let Some(addr) = self
            .other_primaries
            .iter()
            .find(|(pk, _)| *pk == peer)
            .map(|(_, a)| *a)
        else {
            return true;
        };
        if self.serve_tx.try_send(ServeSend(addr, message)).is_err() {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_serve_send_drops_total.inc();
            }
            return false;
        }
        true
    }

    /// Reports a suppressed destination as handled: the withhold experiment refuses the
    /// header on purpose, so the caller must keep its at-most-once mark.
    pub(crate) fn enqueue_repair_serve(&self, peer: PublicKey, header: Header) -> bool {
        if self.repair_suppressed_for(&peer) {
            return true;
        }
        self.try_enqueue_serve(peer, PrimaryMessage::Header(header, true))
    }

    pub(crate) fn repair_suppressed_for(&self, peer: &PublicKey) -> bool {
        self.suppressed_repair_destinations
            .as_ref()
            .is_some_and(|blocked| blocked.contains(peer))
            && config::withhold_active(self.withhold_window.as_deref(), Instant::now())
    }

    pub(crate) fn worker_addr(&self, worker_id: WorkerId) -> Option<SocketAddr> {
        self.worker_addresses.get(&worker_id).copied()
    }
}

#[derive(Debug)]
pub(crate) struct LaneSend(SocketAddr, PrimaryMessage);

pub(crate) struct SequenceSend(pub(crate) SocketAddr, pub(crate) PrimaryMessage);

pub(crate) struct ServeSend(SocketAddr, PrimaryMessage);

#[derive(Debug)]
pub(crate) struct ReplaySend {
    pub(crate) to: SocketAddr,
    pub(crate) peer: PublicKey,
    pub(crate) generation: u64,
    pub(crate) msgs: Vec<Bytes>,
    pub(crate) done: PrimaryMessage,
    pub(crate) reserved_bytes: usize,
}

#[derive(Clone)]
pub(crate) struct ReplaySender {
    tx: mpsc::Sender<ReplaySend>,
    reserved_bytes: Arc<AtomicUsize>,
    max_reserved_bytes: usize,
    codec: VantageWireCodec,
}

impl ReplaySender {
    pub(crate) fn channel(
        max_reserved_bytes: usize,
        codec: VantageWireCodec,
    ) -> (Self, mpsc::Receiver<ReplaySend>) {
        let (tx, rx) = mpsc::channel(REPLAY_SEND_CHANNEL_CAPACITY);
        (
            Self {
                tx,
                reserved_bytes: Arc::new(AtomicUsize::new(0)),
                max_reserved_bytes: max_reserved_bytes.max(1),
                codec,
            },
            rx,
        )
    }

    /// Admits work within the global byte bound, except for one oversized stream when idle.
    fn try_send(&self, mut item: ReplaySend) -> bool {
        let reserved_bytes = replay_reserved_size(&item.msgs, &item.done, &self.codec);
        let reserved =
            self.reserved_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    if current == 0 && reserved_bytes > self.max_reserved_bytes {
                        Some(reserved_bytes)
                    } else {
                        current
                            .checked_add(reserved_bytes)
                            .filter(|next| *next <= self.max_reserved_bytes)
                    }
                });
        if reserved.is_err() {
            return false;
        }

        item.reserved_bytes = reserved_bytes;
        if self.tx.try_send(item).is_err() {
            self.reserved_bytes
                .fetch_sub(reserved_bytes, Ordering::AcqRel);
            return false;
        }
        true
    }
}

fn replay_reserved_size(msgs: &[Bytes], done: &PrimaryMessage, codec: &VantageWireCodec) -> usize {
    let payload_bytes = msgs
        .iter()
        .fold(0usize, |total, msg| total.saturating_add(msg.len()));
    let done_bytes = usize::try_from(codec.serialized_message_size(done).expect("serializes"))
        .unwrap_or(usize::MAX);
    payload_bytes.saturating_add(done_bytes).max(1)
}

struct ReplayStream {
    to: SocketAddr,
    peer: PublicKey,
    generation: u64,
    msgs: VecDeque<Bytes>,
    done: PrimaryMessage,
    reserved_bytes: usize,
}

impl From<ReplaySend> for ReplayStream {
    fn from(item: ReplaySend) -> Self {
        Self {
            to: item.to,
            peer: item.peer,
            generation: item.generation,
            msgs: item.msgs.into(),
            done: item.done,
            reserved_bytes: item.reserved_bytes,
        }
    }
}

// Each recovery class has a separate bounded queue.
const SEQUENCE_SEND_CHANNEL_CAPACITY: usize = 256;
const RESUME_LANE_CHANNEL_CAPACITY: usize = 4096;
const REPLAY_SEND_CHANNEL_CAPACITY: usize = 64;
const SERVE_SEND_CHANNEL_CAPACITY: usize = 4096;

pub(crate) struct ResumeSenders {
    pub(crate) lane: mpsc::Sender<LaneSend>,
    pub(crate) replay: ReplaySender,
    pub(crate) sequence: mpsc::Sender<SequenceSend>,
    pub(crate) serve: mpsc::Sender<ServeSend>,
    pub(crate) generation: AtomicU64,
}

/// Starts isolated lane, replay, sequence, and serve senders with bounded ingress queues.
///
/// `chunk_interval_ms` and `retry_backoff_max_ms` are milliseconds.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_resume_sender(
    latency_map: HashMap<SocketAddr, Duration>,
    batch: BatchConfig,
    metrics: Option<Arc<Metrics>>,
    in_flight: InFlightMap,
    codec: VantageWireCodec,
    chunk_bytes: usize,
    chunk_interval_ms: u64,
    replay_serve_max_bytes: usize,
    retry_backoff_max_ms: u64,
    auth: Option<Arc<ChannelAuth>>,
) -> ResumeSenders {
    let (lane_tx, lane_rx) = mpsc::channel(RESUME_LANE_CHANNEL_CAPACITY);
    let max_reserved_bytes = replay_serve_max_bytes.saturating_mul(2).max(1);
    let (replay_tx, replay_rx) = ReplaySender::channel(max_reserved_bytes, codec.clone());
    let sequence_latency = latency_map.clone();
    let sequence_metrics = metrics.clone();
    let sequence_auth = auth.clone();
    let mut messages = SimpleSender::new()
        .with_queue_role("lane")
        .with_latency(latency_map.clone())
        .with_batching(batch)
        .with_channel_auth(auth.clone());
    let mut replay = ReliableSender::new()
        .with_queue_role("replay")
        .with_latency(latency_map.clone())
        .with_batching(batch)
        .with_channel_auth(auth.clone())
        .with_retry_backoff_max_ms(retry_backoff_max_ms);
    let mut serve = ReliableSender::new()
        .with_queue_role("serve")
        .with_latency(latency_map)
        .with_batching(batch)
        .with_channel_auth(auth)
        .with_retry_backoff_max_ms(retry_backoff_max_ms);
    if let Some(m) = metrics {
        messages = messages.with_metrics(m.clone());
        replay = replay.with_metrics(m.clone());
        serve = serve.with_metrics(m);
    }
    let chunk_interval = Duration::from_millis(chunk_interval_ms.max(1));
    tokio::spawn(run_lane_sender(lane_rx, messages, codec.clone()));
    tokio::spawn(run_replay_sender(
        replay_rx,
        replay,
        in_flight,
        replay_tx.reserved_bytes.clone(),
        chunk_bytes.max(1),
        chunk_interval,
        codec.clone(),
    ));
    let (serve_tx, serve_rx) = mpsc::channel(SERVE_SEND_CHANNEL_CAPACITY);
    tokio::spawn(run_serve_sender(serve_rx, serve, codec.clone()));
    let (sequence_tx, sequence_rx) = mpsc::channel(SEQUENCE_SEND_CHANNEL_CAPACITY);
    let mut sequence_messages = SimpleSender::new()
        .with_queue_role("sequence")
        .with_latency(sequence_latency)
        .with_batching(batch)
        .with_channel_auth(sequence_auth);
    if let Some(m) = sequence_metrics {
        sequence_messages = sequence_messages.with_metrics(m);
    }
    tokio::spawn(run_sequence_sender(sequence_rx, sequence_messages, codec));
    ResumeSenders {
        lane: lane_tx,
        replay: replay_tx,
        sequence: sequence_tx,
        serve: serve_tx,
        generation: AtomicU64::new(1),
    }
}

/// Retries are detached because the core keeps no cancel handler for a served response.
async fn run_serve_sender(
    mut rx: mpsc::Receiver<ServeSend>,
    mut serve: ReliableSender,
    codec: VantageWireCodec,
) {
    while let Some(ServeSend(to, message)) = rx.recv().await {
        let msg_type = message.type_name();
        let bytes = codec.serialize(&message).expect("serializes");
        serve
            .send_detached_typed(to, Bytes::from(bytes), msg_type)
            .await;
    }
}

async fn run_sequence_sender(
    mut rx: mpsc::Receiver<SequenceSend>,
    mut messages: SimpleSender,
    codec: VantageWireCodec,
) {
    while let Some(SequenceSend(to, message)) = rx.recv().await {
        let msg_type = message.type_name();
        let bytes = codec.serialize(&message).expect("serializes");
        messages.send_typed(to, Bytes::from(bytes), msg_type).await;
    }
}

async fn run_lane_sender(
    mut rx: mpsc::Receiver<LaneSend>,
    mut messages: SimpleSender,
    codec: VantageWireCodec,
) {
    while let Some(LaneSend(to, message)) = rx.recv().await {
        let msg_type = message.type_name();
        let bytes = codec.serialize(&message).expect("serializes");
        messages.send_typed(to, Bytes::from(bytes), msg_type).await;
    }
}

/// Sends one bounded chunk per tick and rotates unfinished streams in FIFO order.
///
/// Closing ingress drains every admitted stream through its `Done` frame before exit.
async fn run_replay_sender(
    mut rx: mpsc::Receiver<ReplaySend>,
    mut replay: ReliableSender,
    in_flight: InFlightMap,
    reserved_bytes: Arc<AtomicUsize>,
    chunk_bytes: usize,
    chunk_interval: Duration,
    codec: VantageWireCodec,
) {
    let mut streams: VecDeque<ReplayStream> = VecDeque::new();
    let mut ticker = tokio::time::interval(chunk_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ingress_open = true;
    loop {
        if !ingress_open && streams.is_empty() {
            return;
        }
        match next_replay_event(&mut rx, &mut ticker, !streams.is_empty(), ingress_open).await {
            ReplayEvent::Tick => {
                let Some(mut stream) = streams.pop_front() else {
                    continue;
                };
                // Detached sends must not create handlers owned by this task.
                for bytes in take_replay_chunk(&mut stream, chunk_bytes) {
                    replay.send_detached_typed(stream.to, bytes, "Replay").await;
                }
                if stream.msgs.is_empty() {
                    let msg_type = stream.done.type_name();
                    let done_bytes = codec.serialize(&stream.done).expect("serializes");
                    replay
                        .send_detached_typed(stream.to, Bytes::from(done_bytes), msg_type)
                        .await;
                    complete_replay_stream(&in_flight, &reserved_bytes, &stream);
                } else {
                    streams.push_back(stream);
                }
            }
            ReplayEvent::Ingress(maybe) => match maybe {
                Some(item) => streams.push_back((*item).into()),
                None => ingress_open = false,
            },
        }
    }
}

enum ReplayEvent {
    Tick,
    Ingress(Option<Box<ReplaySend>>),
}

/// Prioritizes a due pacing tick and admits at most one stream per ingress event.
async fn next_replay_event(
    rx: &mut mpsc::Receiver<ReplaySend>,
    ticker: &mut tokio::time::Interval,
    has_streams: bool,
    ingress_open: bool,
) -> ReplayEvent {
    tokio::select! {
        biased;

        _ = ticker.tick(), if has_streams => ReplayEvent::Tick,
        item = rx.recv(), if ingress_open => ReplayEvent::Ingress(item.map(Box::new)),
    }
}

fn take_replay_chunk(stream: &mut ReplayStream, chunk_bytes: usize) -> Vec<Bytes> {
    let mut chunk = Vec::new();
    let mut sent = 0usize;
    while sent < chunk_bytes {
        let Some(bytes) = stream.msgs.pop_front() else {
            break;
        };
        sent = sent.saturating_add(bytes.len());
        chunk.push(bytes);
    }
    chunk
}

fn complete_replay_stream(
    in_flight: &InFlightMap,
    reserved_bytes: &AtomicUsize,
    stream: &ReplayStream,
) {
    remove_in_flight_generation(in_flight, stream.peer, stream.generation);
    reserved_bytes.fetch_sub(stream.reserved_bytes, Ordering::AcqRel);
}

/// Removes an in-flight entry only when the completed generation still owns it.
pub(crate) fn remove_in_flight_generation(
    in_flight: &InFlightMap,
    peer: PublicKey,
    generation: u64,
) -> bool {
    let mut guard = in_flight.lock();
    if guard
        .get(&peer)
        .is_some_and(|entry| entry.generation == generation)
    {
        guard.remove(&peer);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::Digest;
    use futures::FutureExt as _;

    fn key(index: usize) -> PublicKey {
        crate::common::keys()[index].0
    }

    fn codec() -> VantageWireCodec {
        VantageWireCodec::new(&crate::common::committee(), true).unwrap()
    }

    fn replay_item(peer: PublicKey, generation: u64, payloads: &[&'static [u8]]) -> ReplaySend {
        ReplaySend {
            to: "127.0.0.1:9".parse().unwrap(),
            peer,
            generation,
            msgs: payloads
                .iter()
                .map(|payload| Bytes::from_static(payload))
                .collect(),
            done: PrimaryMessage::VantageReplayDone(1, true, false, peer),
            reserved_bytes: 0,
        }
    }

    #[test]
    fn compact_codec_roundtrips_large_proposals_and_reads_legacy_frames() {
        let (committee, _) = Committee::local_benchmark(100, 1, 10_000);
        let manifest: Vec<_> = committee
            .authorities
            .keys()
            .copied()
            .enumerate()
            .map(|(index, author)| (author, index as u64, Digest([index as u8; 32])))
            .collect();
        let proposal = crate::vantage::ViewProposal {
            view: 7,
            c: manifest.clone(),
            t: manifest,
            m: None,
        };
        let message = PrimaryMessage::VantagePropose(proposal.clone());
        let compact = VantageWireCodec::new(&committee, true).unwrap();
        let legacy = VantageWireCodec::new(&committee, false).unwrap();

        let compact_bytes = compact.serialize(&message).unwrap();
        let legacy_bytes = legacy.serialize(&message).unwrap();
        assert!(compact_bytes.starts_with(COMPACT_ID_PREFIX));
        assert_eq!(legacy_bytes, bincode::serialize(&message).unwrap());
        assert_eq!(legacy_bytes.len() - compact_bytes.len(), 51 * 200 - 4);

        for decoder in [&compact, &legacy] {
            for bytes in [&compact_bytes, &legacy_bytes] {
                let PrimaryMessage::VantagePropose(decoded) = decoder.deserialize(bytes).unwrap()
                else {
                    panic!("decoded the wrong message variant");
                };
                assert_eq!(decoded, proposal);
            }
        }
    }

    #[test]
    fn compact_codec_roundtrips_resolution_messages_and_reads_legacy_frames() {
        let committee = crate::common::committee();
        let sender = *committee.authorities.keys().next().unwrap();
        let block = crate::vantage::ResolutionBlock {
            height: 3,
            parent: Digest([1; 32]),
            anchors: vec![crate::vantage::AnchorRef {
                view: 17,
                digest: Digest([2; 32]),
            }],
        };
        let value = block.digest(&Digest([3; 32]));
        let done = crate::vantage::ResolutionDone {
            height: block.height,
            parent: block.parent.clone(),
            value,
            block,
            sender,
        };
        let message = PrimaryMessage::VantageResolutionDone(done.clone());
        let compact = VantageWireCodec::new(&committee, true).unwrap();
        let legacy = VantageWireCodec::new(&committee, false).unwrap();
        let compact_bytes = compact.serialize(&message).unwrap();
        let legacy_bytes = legacy.serialize(&message).unwrap();

        assert!(compact_bytes.starts_with(COMPACT_ID_PREFIX));
        assert_eq!(legacy_bytes, bincode::serialize(&message).unwrap());
        for decoder in [&compact, &legacy] {
            for bytes in [&compact_bytes, &legacy_bytes] {
                let PrimaryMessage::VantageResolutionDone(decoded) =
                    decoder.deserialize(bytes).unwrap()
                else {
                    panic!("decoded the wrong message variant");
                };
                assert_eq!(decoded, done);
            }
        }
    }

    #[test]
    fn compact_codec_rejects_unknown_indices_and_oversized_committees() {
        let committee = crate::common::committee();
        let codec = VantageWireCodec::new(&committee, true).unwrap();
        let sender = *committee.authorities.keys().next().unwrap();
        let mut bytes = codec
            .serialize(&PrimaryMessage::VantageWish(9, sender))
            .unwrap();
        *bytes.last_mut().unwrap() = committee.size() as u8;
        assert!(codec.deserialize(&bytes).is_err());

        let (oversized, _) = Committee::local_benchmark(257, 1, 20_000);
        assert!(matches!(
            VantageWireCodec::new(&oversized, true),
            Err(PublicKeyIndexError::TooManyKeys(257))
        ));
        assert!(VantageWireCodec::new(&oversized, false).is_ok());
    }

    #[test]
    fn replay_byte_cap_covers_queued_and_active_until_completion() {
        let peer = key(0);
        let first = replay_item(peer, 1, &[b"payload"]);
        let footprint = replay_reserved_size(&first.msgs, &first.done, &codec());
        let (sender, mut rx) = ReplaySender::channel(footprint, codec());

        assert!(sender.try_send(first));
        assert!(
            !sender.try_send(replay_item(key(1), 2, &[b"x"])),
            "the CAS reservation must reject work beyond the byte cap"
        );

        let stream = ReplayStream::from(rx.try_recv().unwrap());
        assert_eq!(
            sender.reserved_bytes.load(Ordering::Acquire),
            footprint,
            "receiving a stream must not refund its active reservation"
        );

        let in_flight = Arc::new(Mutex::new(HashMap::from([(
            peer,
            InFlightEntry {
                started: Instant::now(),
                generation: 1,
            },
        )])));
        complete_replay_stream(&in_flight, &sender.reserved_bytes, &stream);
        assert_eq!(sender.reserved_bytes.load(Ordering::Acquire), 0);
        assert!(!in_flight.lock().contains_key(&peer));
    }

    #[test]
    fn failed_replay_channel_send_refunds_reservation_immediately() {
        let (sender, rx) = ReplaySender::channel(usize::MAX, codec());
        drop(rx);

        assert!(!sender.try_send(replay_item(key(0), 1, &[b"payload"])));
        assert_eq!(sender.reserved_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn one_oversized_stream_is_admitted_only_when_alone() {
        let (sender, mut rx) = ReplaySender::channel(1, codec());
        let first_peer = key(0);
        assert!(sender.try_send(replay_item(first_peer, 1, &[b"oversized"])));
        assert!(!sender.try_send(replay_item(key(1), 2, &[b"also oversized"])));

        let stream = ReplayStream::from(rx.try_recv().unwrap());
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        complete_replay_stream(&in_flight, &sender.reserved_bytes, &stream);

        assert!(
            sender.try_send(replay_item(key(1), 2, &[b"also oversized"])),
            "completion must release the sole oversized reservation"
        );
    }

    #[test]
    fn saturated_lane_ingress_cannot_block_replay_admission() {
        let (lane_tx, _lane_rx) = mpsc::channel(1);
        assert!(lane_tx
            .try_send(LaneSend(
                "127.0.0.1:9".parse().unwrap(),
                PrimaryMessage::VantageReplayDone(1, true, false, key(0))
            ))
            .is_ok());
        assert!(lane_tx
            .try_send(LaneSend(
                "127.0.0.1:9".parse().unwrap(),
                PrimaryMessage::VantageReplayDone(1, true, false, key(0))
            ))
            .is_err());

        let (replay_tx, mut replay_rx) = ReplaySender::channel(usize::MAX, codec());
        assert!(replay_tx.try_send(replay_item(key(1), 1, &[b"replay"])));
        assert!(replay_rx.try_recv().is_ok());
    }

    #[test]
    fn replay_chunks_rotate_round_robin() {
        let mut streams = VecDeque::from([
            ReplayStream::from(replay_item(key(0), 1, &[b"a1", b"a2"])),
            ReplayStream::from(replay_item(key(1), 2, &[b"b1", b"b2"])),
        ]);
        let mut order = Vec::new();

        while let Some(mut stream) = streams.pop_front() {
            order.extend(take_replay_chunk(&mut stream, 1));
            if !stream.msgs.is_empty() {
                streams.push_back(stream);
            }
        }

        assert_eq!(
            order,
            vec![
                Bytes::from_static(b"a1"),
                Bytes::from_static(b"b1"),
                Bytes::from_static(b"a2"),
                Bytes::from_static(b"b2"),
            ]
        );
    }

    #[test]
    fn stale_completion_cannot_clear_a_new_generation() {
        let peer = key(0);
        let in_flight = Arc::new(Mutex::new(HashMap::from([(
            peer,
            InFlightEntry {
                started: Instant::now(),
                generation: 2,
            },
        )])));

        assert!(!remove_in_flight_generation(&in_flight, peer, 1));
        assert_eq!(in_flight.lock()[&peer].generation, 2);
        assert!(remove_in_flight_generation(&in_flight, peer, 2));
        assert!(!in_flight.lock().contains_key(&peer));
    }

    #[tokio::test(start_paused = true)]
    async fn due_tick_has_priority_and_first_tick_is_immediate() {
        let (sender, mut rx) = ReplaySender::channel(usize::MAX, codec());
        assert!(sender.try_send(replay_item(key(0), 1, &[b"first"])));
        let mut ticker = tokio::time::interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        assert!(matches!(
            next_replay_event(&mut rx, &mut ticker, false, true).await,
            ReplayEvent::Ingress(Some(_))
        ));
        assert!(sender.try_send(replay_item(key(1), 2, &[b"second"])));
        assert!(matches!(
            next_replay_event(&mut rx, &mut ticker, true, true).await,
            ReplayEvent::Tick
        ));
        assert!(
            rx.try_recv().is_ok(),
            "the ready ingress item must remain queued when the due tick wins"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn replay_ticker_uses_delay_after_a_missed_tick() {
        let mut ticker = tokio::time::interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        assert!(
            ticker.tick().now_or_never().is_some(),
            "first tick is immediate"
        );

        tokio::time::advance(Duration::from_millis(35)).await;
        assert!(ticker.tick().now_or_never().is_some());
        assert!(ticker.tick().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(9)).await;
        assert!(ticker.tick().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(ticker.tick().now_or_never().is_some());
    }

    #[tokio::test]
    async fn closed_replay_ingress_drains_all_accepted_streams() {
        let codec = codec();
        let (sender, rx) = ReplaySender::channel(usize::MAX, codec.clone());
        let reserved_bytes = sender.reserved_bytes.clone();
        let first = key(0);
        let second = key(1);
        assert!(sender.try_send(replay_item(first, 1, &[])));
        assert!(sender.try_send(replay_item(second, 2, &[])));
        let in_flight = Arc::new(Mutex::new(HashMap::from([
            (
                first,
                InFlightEntry {
                    started: Instant::now(),
                    generation: 1,
                },
            ),
            (
                second,
                InFlightEntry {
                    started: Instant::now(),
                    generation: 2,
                },
            ),
        ])));
        drop(sender);

        tokio::time::timeout(
            Duration::from_secs(1),
            run_replay_sender(
                rx,
                ReliableSender::new(),
                in_flight.clone(),
                reserved_bytes.clone(),
                1,
                Duration::from_millis(1),
                codec,
            ),
        )
        .await
        .expect("closed ingress must drain and exit");

        assert!(in_flight.lock().is_empty());
        assert_eq!(reserved_bytes.load(Ordering::Acquire), 0);
    }
}
