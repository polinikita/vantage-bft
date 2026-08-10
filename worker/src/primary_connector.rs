// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use primary::WorkerPrimaryMessage;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

/// Sends batch digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Batch digests to send.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// Sends digests to the primary.
    network: SimpleSender,
}

impl PrimaryConnector {
    pub fn spawn(
        primary_address: SocketAddr,
        rx_digest: Receiver<SerializedBatchDigestMessage>,
        metrics: Arc<Metrics>,
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
            // Use a fallback label for malformed messages.
            let msg_type = bincode::deserialize::<WorkerPrimaryMessage>(&digest)
                .map(|m| m.type_name())
                .unwrap_or("WorkerPrimaryMessage");
            self.network
                .send_typed(self.primary_address, Bytes::from(digest), msg_type)
                .await;
        }
    }
}
