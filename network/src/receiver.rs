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
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgements to the sender machine (see unit tests for examples).
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
    /// METRICS-DASHBOARD-SPEC.md §1: optional wire-metrics handle, `None` by default
    /// (zero added cost -- `spawn` is the only entry point most callers use).
    /// `bytes_received_total` is incremented once per received frame, length prefix
    /// included, before the frame is handed to `handler.dispatch`.
    metrics: Option<Arc<Metrics>>,
    /// METRICS-DASHBOARD-SPEC.md §8: `false` (the default) never calls
    /// `lz4_flex::decompress_size_prepended` -- byte-identical to pre-compression
    /// behavior. Committee-wide consistent by construction (see `Parameters::
    /// compress_network`'s doc): every sender this receiver's peers use shares the
    /// same setting, so "compressed frame arrives while this flag is off" is a
    /// misconfiguration, not a case this code needs to tolerate gracefully -- a
    /// decode failure in that case is `warn!`-logged and the connection dropped, the
    /// same way any other malformed frame is already handled.
    compress: bool,
    /// Whether THIS handler's peers ack every received frame -- matches, per
    /// call-site, whatever the corresponding `MessageHandler::dispatch` impl used to
    /// do itself (`writer.send(Bytes::from("Ack"))` as its first action) before the
    /// batching design moved every acking handler's ack out of `dispatch` and in here
    /// instead. Moving it here is what makes "exactly one ack per received FRAME"
    /// achievable when several logical messages share a bundle frame -- `dispatch` is
    /// called once per logical (sub-)message, so an ack inside `dispatch` would fire
    /// once per sub-message instead of once per frame, desyncing `ReliableSender`'s
    /// `pending_replies` FIFO. `false` (the default) never writes an ack at all,
    /// matching every non-acking handler (e.g. `TxReceiverHandler`) exactly.
    acks: bool,
    /// Whether frames from this handler's peers may be multi-message bundles (see
    /// `network::batch`'s module doc). `false` (the default): a frame is exactly one
    /// logical message, `handler.dispatch` is called once per frame -- byte-identical
    /// to pre-batching behavior.
    batch: bool,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        Self::spawn_full(address, handler, None, false, false, false);
    }

    /// Same as `spawn`, plus a `bytes_received_total` observation for every frame this
    /// receiver's connections read (a no-op if `metrics` is `None`).
    pub fn spawn_with_metrics(address: SocketAddr, handler: Handler, metrics: Option<Arc<Metrics>>) {
        Self::spawn_full(address, handler, metrics, false, false, false);
    }

    /// Full form: metrics handle + lz4 compression flag (§8) + ack/batch flags. See
    /// `acks`/`batch`'s own doc comments for their exact contracts. `compress` must
    /// match what every peer's own sender is configured with (see `Parameters::
    /// compress_network`'s doc on committee-wide consistency) -- same requirement
    /// applies to `batch` vs. `Parameters::batch_messages`.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_full(
        address: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
        compress: bool,
        acks: bool,
        batch: bool,
    ) {
        tokio::spawn(async move {
            Self { address, handler, metrics, compress, acks, batch }.run().await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
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
            info!("Incoming connection established with {}", peer);
            Self::spawn_runner(socket, peer, self.handler.clone(), self.metrics.clone(), self.compress, self.acks, self.batch).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    #[allow(clippy::too_many_arguments)]
    async fn spawn_runner(
        socket: TcpStream,
        peer: SocketAddr,
        handler: Handler,
        metrics: Option<Arc<Metrics>>,
        compress: bool,
        acks: bool,
        batch: bool,
    ) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        if let Some(metrics) = &metrics {
                            metrics.bytes_received_total.inc_by(message.len() as u64 + 4);
                        }
                        // METRICS-DASHBOARD-SPEC.md §8: decompress AFTER length-prefix
                        // framing (the frame itself is already delimited by the codec
                        // above) -- mirrors the send side compressing BEFORE framing.
                        let payload = if compress {
                            match lz4_flex::decompress_size_prepended(&message) {
                                Ok(decompressed) => Bytes::from(decompressed),
                                Err(e) => {
                                    warn!("Failed to lz4-decompress frame from {}: {}", peer, e);
                                    return;
                                }
                            }
                        } else {
                            message.freeze()
                        };

                        // Exactly one ack per received FRAME (see `Receiver::acks`'s
                        // doc comment), sent before dispatch -- identical relative
                        // ordering to every acking handler's pre-batching behavior
                        // (the ack was the very first statement inside `dispatch`,
                        // before deserializing/routing; nothing observable happens
                        // between "frame decoded" and "dispatch called" either way, so
                        // moving the write here doesn't change the bytes or their
                        // timing on the wire).
                        if acks {
                            let _ = writer.send(Bytes::from("Ack")).await;
                        }

                        if batch {
                            match decode_bundle(&payload) {
                                Ok(messages) => {
                                    for sub_message in messages {
                                        if let Err(e) = handler.dispatch(&mut writer, sub_message).await {
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
