// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch::decode_bundle;
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use metrics::Metrics;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Convenient alias for the writer end of the TCP channel.
pub type Writer = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Handles one decoded message and may write a response on `writer`.
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// Accepts connections and dispatches their messages to a handler.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
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
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        Self::spawn_full(address, handler, None, false, false, "unlabeled");
    }

    /// Same as `spawn`, plus a `bytes_received_total` observation for every frame this
    /// receiver's connections read (a no-op if `metrics` is `None`).
    pub fn spawn_with_metrics(
        address: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
    ) {
        Self::spawn_full(address, handler, metrics, false, false, "unlabeled");
    }

    /// Full form with metrics, acknowledgement, batching, and listener settings.
    /// `batch` must match the sender setting. `listener` labels inbound connections.
    pub fn spawn_full(
        address: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
        acks: bool,
        batch: bool,
        listener: &'static str,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                handler,
                metrics,
                acks,
                batch,
                listener,
            }
            .run()
            .await;
        });
    }

    /// Accept connections and spawn one runner per connection.
    async fn run(&self) {
        //println!("receiver address {}", self.address.clone().to_string());
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

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
            info!("Incoming connection established with {}", peer);
            Self::spawn_runner(
                socket,
                peer,
                self.handler.clone(),
                self.metrics.clone(),
                self.acks,
                self.batch,
                self.listener,
            )
            .await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(
        socket: TcpStream,
        peer: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
        acks: bool,
        batch: bool,
        listener: &'static str,
    ) {
        tokio::spawn(async move {
            let _connection = ConnectionMetricGuard::new(metrics.clone(), listener);
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        if let Some(metrics) = &metrics {
                            metrics
                                .bytes_received_total
                                .inc_by(message.len() as u64 + 4);
                        }
                        let payload = message.freeze();

                        // Send the frame acknowledgement before dispatch.
                        if acks {
                            let _ = writer.send(Bytes::from("Ack")).await;
                        }

                        if batch {
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
            warn!("Connection closed by peer {}", peer);
        });
    }
}

struct ConnectionMetricGuard {
    metrics: Option<Arc<Metrics>>,
    listener: &'static str,
}

impl ConnectionMetricGuard {
    fn new(metrics: Option<Arc<Metrics>>, listener: &'static str) -> Self {
        if let Some(metrics) = &metrics {
            metrics
                .network_connections
                .with_label_values(&[listener])
                .inc();
        }
        Self { metrics, listener }
    }
}

impl Drop for ConnectionMetricGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics
                .network_connections
                .with_label_values(&[self.listener])
                .dec();
        }
    }
}
