// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use crypto::{PairwiseKeys, PublicKey};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use primary::WorkerPrimaryMessage;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// This worker's own public key -- the worker<->primary channel is intra-
    /// authority (our own primary shares our own public key), so every message this
    /// connector sends is tagged `k_{name,name}` (see `PairwiseKeys::build`'s doc
    /// comment). Unused when `channel_auth` is `None`.
    name: PublicKey,
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,
    /// SECURITY (Fable audit): `Parameters::authenticate_channels`. `None` is
    /// byte-identical to pre-MAC behavior.
    channel_auth: Option<Arc<PairwiseKeys>>,
}

impl PrimaryConnector {
    pub fn spawn(
        name: PublicKey,
        primary_address: SocketAddr,
        rx_digest: Receiver<SerializedBatchDigestMessage>,
        // METRICS-DASHBOARD-SPEC.md §1: appended last, same convention as other
        // `::spawn` functions.
        metrics: Arc<Metrics>,
        // METRICS-DASHBOARD-SPEC.md §8: appended last, same convention.
        compress_network: bool,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
        // SECURITY (Fable audit): appended last, same convention as every other
        // MAC-consuming `::spawn`.
        channel_auth: Option<Arc<PairwiseKeys>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                primary_address,
                rx_digest,
                network: SimpleSender::new()
                    .with_metrics(metrics)
                    .with_compression(compress_network)
                    .with_batching(batch),
                channel_auth,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(digest) = self.rx_digest.recv().await {
            // METRICS-DASHBOARD-SPEC.md §1: this channel only ever carries an
            // already-serialized `WorkerPrimaryMessage` (`Processor::spawn`) -- one
            // cheap deserialize per BATCH (not per transaction) to recover the exact
            // variant name for `network_messages_sent_total`/`network_bytes_sent_total`.
            // Falls back to a generic label rather than panicking if that ever isn't
            // true (defense in depth, matching this crate's existing tolerance style).
            let msg_type = bincode::deserialize::<WorkerPrimaryMessage>(&digest)
                .map(|m| m.type_name())
                .unwrap_or("WorkerPrimaryMessage");
            // SECURITY (Fable audit): appends a tag keyed `k_{name,name}` (byte-
            // identical, unappended, when `channel_auth` is off).
            let data = match &self.channel_auth {
                None => Bytes::from(digest),
                Some(auth) => {
                    let tag = auth
                        .tag_for(&self.name, &digest)
                        .expect("self is a committee member");
                    let mut tagged = digest;
                    tagged.extend_from_slice(&tag);
                    Bytes::from(tagged)
                }
            };
            // Send the digest through the network.
            self.network
                .send_typed(self.primary_address, data, msg_type)
                .await;
        }
    }
}
