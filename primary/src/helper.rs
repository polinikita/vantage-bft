// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::Receiver;

/// Serves certificate and header requests from other authorities.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Certificate requests.
    rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,

    /// Header requests.
    rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey, bool)>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
    metrics: Arc<Metrics>,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,
        rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey, bool)>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries_certs,
                rx_primaries_headers,
                network: SimpleSender::new()
                    .with_metrics(metrics.clone())
                    .with_batching(batch),
                metrics,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some((digests, origin)) = self.rx_primaries_certs.recv() => {

                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected header request: {}", e);
                            continue;
                        }
                    };

                    for digest in digests {
                        match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => {
                                let certificate = bincode::deserialize(&data)
                                    .expect("Failed to deserialize our own certificate");
                                let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                                    .expect("Failed to serialize our own certificate");
                                self.network.send_typed(address, Bytes::from(bytes), "Certificate").await;
                            }
                            Ok(None) => (),
                            Err(e) => error!("{}", e),
                        }
                    }
                },
                Some((digests, origin, prepare_repair)) = self.rx_primaries_headers.recv() => {

                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected certificate request: {}", e);
                            continue;
                        }
                    };

                    if prepare_repair {
                        self.metrics.autobahn_prepare_repair_requests_served_total.inc();
                    }
                    for digest in digests {
                        match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => {
                                let header = bincode::deserialize(&data)
                                    .expect("Failed to deserialize our own header");
                                let bytes = bincode::serialize(&PrimaryMessage::Header(header, true))
                                    .expect("Failed to serialize our own header");
                                if prepare_repair {
                                    self.metrics.autobahn_prepare_repair_headers_served_total.inc();
                                    self.metrics
                                        .autobahn_prepare_repair_bytes_served_total
                                        .inc_by(bytes.len() as u64);
                                }
                                let msg_type = if prepare_repair { "PrepareHeader" } else { "Header" };
                                self.network.send_typed(address, Bytes::from(bytes), msg_type).await;
                                }
                                Ok(None) => (),
                                Err(e) => error!("{}", e),
                        }
                    }

                },
            };
        }
    }
}
