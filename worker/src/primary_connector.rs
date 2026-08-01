// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use primary::WorkerPrimaryMessage;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,
}

impl PrimaryConnector {
    pub fn spawn(
        primary_address: SocketAddr,
        rx_digest: Receiver<SerializedBatchDigestMessage>,
        // METRICS-DASHBOARD-SPEC.md §1: appended last, same convention as other
        // `::spawn` functions.
        metrics: Arc<Metrics>,
        // Transport-level batching: appended last, same convention.
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: SimpleSender::new()
                    .with_metrics(metrics)
                    .with_batching(batch),
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
            // Send the digest through the network.
            self.network
                .send_typed(self.primary_address, Bytes::from(digest), msg_type)
                .await;
        }
    }
}
