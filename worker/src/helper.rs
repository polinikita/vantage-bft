// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, warn};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

/// A task dedicated to help other authorities by replying to their batch requests.
pub struct Helper {
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Batch requests from other workers.
    rx_request: Receiver<(Vec<Digest>, PublicKey)>,
    /// Sends batches to other workers.
    network: SimpleSender,
}

impl Helper {
    // The constructor has more arguments than Clippy's default limit.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        id: WorkerId,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Digest>, PublicKey)>,
        latency_map: HashMap<SocketAddr, Duration>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
    ) {
        tokio::spawn(async move {
            Self {
                id,
                committee,
                store,
                rx_request,
                network: SimpleSender::new()
                    .with_latency(latency_map)
                    .with_metrics(metrics)
                    .with_batching(batch),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin)) = self.rx_request.recv().await {
            let address = match self.committee.worker(&origin, &self.id) {
                Ok(x) => x.worker_to_worker,
                Err(e) => {
                    warn!("Unexpected batch request: {}", e);
                    continue;
                }
            };

            for digest in digests {
                match self.store.read(digest.to_vec()).await {
                    Ok(Some(data)) => {
                        self.network
                            .send_typed(address, Bytes::from(data), "Batch")
                            .await
                    }
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}
