// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::decode_bundle;
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use log::{debug, warn};
use metrics::Metrics;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::channel_auth::{ChannelAuth, Role};
use crate::codec::{authenticated_frame_codec, frame_codec, AuthCodec, LENGTH_PREFIX_LEN, TAG_LEN};

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Writer half of a framed TCP connection.
pub type Writer = SplitSink<Framed<TcpStream, AuthCodec>, Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Handles one decoded message and may write a response on `writer`.
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// Accepts connections and dispatches their messages to a handler.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Message handler.
    handler: Handler,
    /// Optional wire metrics. Received bytes include the four-byte frame length.
    metrics: Option<Arc<Metrics>>,
    /// Whether to send one acknowledgement per received frame. This is frame-based so
    /// a bundled frame produces one acknowledgement.
    acks: bool,
    /// Whether incoming frames use the bundle format. When false, each frame is one
    /// logical message.
    batch: bool,
    /// Stable listener-role label for current inbound connection gauges.
    listener: &'static str,
    /// Pairwise channel keys. When set, every connection must authenticate.
    auth: Option<Arc<ChannelAuth>>,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawns a receiver for incoming peer connections.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        Self::spawn_full(address, handler, None, false, false, "unlabeled", None);
    }

    /// Spawns a receiver with optional per-frame byte metrics.
    pub fn spawn_with_metrics(
        address: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
    ) {
        Self::spawn_full(address, handler, metrics, false, false, "unlabeled", None);
    }

    /// Full receiver configuration. Sender and receiver batching must match.
    ///
    /// `auth` is set only on listeners that serve other committee members. Client and
    /// same-host listeners pass `None` and stay unauthenticated.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_full(
        address: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
        acks: bool,
        batch: bool,
        listener: &'static str,
        auth: Option<Arc<ChannelAuth>>,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                handler,
                metrics,
                acks,
                batch,
                listener,
                auth,
            }
            .run()
            .await;
        });
    }

    /// Accept connections and spawn one runner per connection.
    async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");
        let peers = Arc::new(PeerSessions::default());

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::FailedToListen(e));
                    continue;
                }
            };
            // Disable Nagle buffering for small protocol frames and acknowledgements.
            let _ = socket.set_nodelay(true);
            debug!("Incoming connection established with {}", peer);
            Self::spawn_runner(
                socket,
                peer,
                self.handler.clone(),
                RunnerConfig {
                    metrics: self.metrics.clone(),
                    acks: self.acks,
                    batch: self.batch,
                    listener: self.listener,
                    peers: peers.clone(),
                    auth: self.auth.clone(),
                },
            )
            .await;
        }
    }

    /// Spawn a runner for one TCP connection.
    async fn spawn_runner(
        socket: TcpStream,
        peer: SocketAddr,
        handler: Handler,
        config: RunnerConfig,
    ) {
        tokio::spawn(async move {
            let mut socket = socket;
            let rtt_micros = tcp_rtt_micros(&socket);
            let _connection = ConnectionMetricGuard::new(
                config.metrics.clone(),
                config.listener,
                peer.ip(),
                config.peers,
                rtt_micros,
            );

            // Bind the connection to a committee identity before dispatching anything from
            // it. A peer that cannot produce a valid tag never reaches the handler.
            let codec = match &config.auth {
                Some(auth) => match auth.handshake_listener(&mut socket).await {
                    Ok((index, key)) => {
                        debug!("Authenticated connection from committee member {}", index);
                        authenticated_frame_codec(key, Role::Listener)
                    }
                    Err(e) => {
                        warn!("{}", NetworkError::ChannelAuthFailed(peer, e));
                        record_auth_failure(&config.metrics, config.listener);
                        return;
                    }
                },
                None => frame_codec(),
            };
            let authenticated = codec.is_authenticated();
            let (mut writer, mut reader) = Framed::new(socket, codec).split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        if let Some(metrics) = &config.metrics {
                            // The decoder strips the tag before returning, so add it back:
                            // this counter measures what crossed the wire.
                            let tag = if authenticated { TAG_LEN as u64 } else { 0 };
                            metrics
                                .bytes_received_total
                                .inc_by(message.len() as u64 + LENGTH_PREFIX_LEN as u64 + tag);
                            if authenticated {
                                record_auth_frame(metrics, "received", message.len());
                            }
                        }
                        let payload = message.freeze();

                        // Acknowledge before dispatching the frame.
                        if config.acks {
                            let _ = writer.send(Bytes::from("Ack")).await;
                        }

                        if config.batch {
                            match decode_bundle(&payload) {
                                Ok(messages) => {
                                    for sub_message in messages {
                                        if let Err(e) =
                                            handler.dispatch(&mut writer, sub_message).await
                                        {
                                            warn!("{}", e);
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to decode bundle frame from {}: {}", peer, e);
                                    return;
                                }
                            }
                        } else if let Err(e) = handler.dispatch(&mut writer, payload).await {
                            warn!("{}", e);
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            debug!("Connection closed by peer {}", peer);
        });
    }
}

struct RunnerConfig {
    metrics: Option<Arc<Metrics>>,
    acks: bool,
    batch: bool,
    listener: &'static str,
    peers: Arc<PeerSessions>,
    auth: Option<Arc<ChannelAuth>>,
}

/// Records a rejected connection: a bad hello, a timeout, or a tag that did not verify.
fn record_auth_failure(metrics: &Option<Arc<Metrics>>, listener: &'static str) {
    if let Some(metrics) = metrics {
        metrics
            .channel_auth_failures_total
            .with_label_values(&[listener])
            .inc();
    }
}

/// Records one frame that carried a tag.
///
/// These counters exist to distinguish an authenticated run that costs little from a run
/// in which nothing was authenticated at all.
pub(crate) fn record_auth_frame(metrics: &Metrics, direction: &'static str, payload_len: usize) {
    metrics
        .channel_auth_frames_total
        .with_label_values(&[direction])
        .inc();
    metrics
        .channel_auth_bytes_total
        .with_label_values(&[direction])
        .inc_by(payload_len as u64);
}

#[derive(Default)]
struct PeerSessions {
    counts: Mutex<HashMap<IpAddr, usize>>,
}

impl PeerSessions {
    /// Returns true when this is the peer's first open session.
    fn open(&self, peer: IpAddr) -> bool {
        let mut counts = self.counts.lock();
        let count = counts.entry(peer).or_default();
        *count += 1;
        *count == 1
    }

    /// Returns true when the peer has no remaining sessions.
    fn close(&self, peer: IpAddr) -> bool {
        let mut counts = self.counts.lock();
        let Some(count) = counts.get_mut(&peer) else {
            return false;
        };
        *count -= 1;
        if *count != 0 {
            return false;
        }
        counts.remove(&peer);
        true
    }
}

struct ConnectionMetricGuard {
    metrics: Option<Arc<Metrics>>,
    listener: &'static str,
    peer: IpAddr,
    peers: Arc<PeerSessions>,
}

impl ConnectionMetricGuard {
    fn new(
        metrics: Option<Arc<Metrics>>,
        listener: &'static str,
        peer: IpAddr,
        peers: Arc<PeerSessions>,
        rtt_micros: Option<u64>,
    ) -> Self {
        let first = peers.open(peer);
        if let Some(metrics) = &metrics {
            metrics
                .network_connections
                .with_label_values(&[listener])
                .inc();
            metrics
                .network_connections_accepted_total
                .with_label_values(&[listener])
                .inc();
            if first {
                metrics
                    .network_unique_peers
                    .with_label_values(&[listener])
                    .inc();
                if let Some(rtt_micros) = rtt_micros {
                    metrics
                        .network_peer_rtt_microseconds_total
                        .with_label_values(&[listener])
                        .inc_by(rtt_micros);
                    metrics
                        .network_peer_rtt_samples_total
                        .with_label_values(&[listener])
                        .inc();
                }
            }
        }
        Self {
            metrics,
            listener,
            peer,
            peers,
        }
    }
}

impl Drop for ConnectionMetricGuard {
    fn drop(&mut self) {
        let last = self.peers.close(self.peer);
        if let Some(metrics) = &self.metrics {
            metrics
                .network_connections
                .with_label_values(&[self.listener])
                .dec();
            metrics
                .network_connections_closed_total
                .with_label_values(&[self.listener])
                .inc();
            if last {
                metrics
                    .network_unique_peers
                    .with_label_values(&[self.listener])
                    .dec();
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn tcp_rtt_micros(stream: &TcpStream) -> Option<u64> {
    use std::mem::{size_of, zeroed};
    use std::os::fd::AsRawFd;

    // Linux reports the smoothed TCP RTT in microseconds.
    // SAFETY: A zeroed tcp_info is valid output storage for getsockopt.
    let mut info: libc::tcp_info = unsafe { zeroed() };
    let mut len = size_of::<libc::tcp_info>() as libc::socklen_t;
    // SAFETY: The descriptor is live, and both output pointers remain valid for len.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            (&mut info as *mut libc::tcp_info).cast(),
            &mut len,
        )
    };
    (result == 0 && info.tcpi_rtt > 0).then_some(info.tcpi_rtt as u64)
}

#[cfg(not(target_os = "linux"))]
fn tcp_rtt_micros(_stream: &TcpStream) -> Option<u64> {
    None
}
