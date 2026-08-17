// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::PublicKey;
use metrics::Metrics;
use network::{BatchConfig, ChannelAuth, SimpleSender};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// Propagates the latest committed round and triggers cleanup.
pub struct GarbageCollector {
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
    /// A loopback channel to the primary's core.
    tx_loopback: Sender<Certificate>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
}

impl GarbageCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate>,
        tx_loopback: Sender<Certificate>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
        auth: Option<Arc<ChannelAuth>>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                store,
                consensus_round,
                rx_consensus,
                tx_loopback,
                addresses,
                network: SimpleSender::new()
                    .with_metrics(metrics)
                    .with_batching(batch)
                    .with_channel_auth(auth),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        while let Some(certificate) = self.rx_consensus.recv().await {
            // Return unseen certificates to the core.
            if self
                .store
                .read(certificate.digest().to_vec())
                .await
                .expect("Failed to read from store")
                .is_none()
            {
                self.tx_loopback
                    .send(certificate.clone())
                    .await
                    .expect("Failed to loop back certificate to core");
            }

            let round = certificate.height();
            if round > last_committed_round {
                last_committed_round = round;

                self.consensus_round.store(round, Ordering::Relaxed);

                let bytes = bincode::serialize(&PrimaryWorkerMessage::Cleanup(round))
                    .expect("Failed to serialize our own message");
                self.network
                    .broadcast_typed(self.addresses.clone(), Bytes::from(bytes), "Cleanup")
                    .await;
            }
        }
    }
}
