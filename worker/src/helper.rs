// Copyright(C) Facebook, Inc. and its affiliates.
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use log::{debug, error, warn};
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
    rx_request: Receiver<(Vec<Digest>, PublicKey, bool)>,
    /// Sends batches to other workers.
    network: SimpleSender,
    /// Benchmark-only Byzantine behavior: narrowcast original batches, then
    /// remain silent when peers ask for repair.
    suppress_repair: bool,
}

impl Helper {
    // The constructor has more arguments than Clippy's default limit.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        id: WorkerId,
        committee: Committee,
        store: Store,
        rx_request: Receiver<(Vec<Digest>, PublicKey, bool)>,
        latency_map: HashMap<SocketAddr, Duration>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
        suppress_repair: bool,
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
                suppress_repair,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some((digests, origin, optimistic_leader_repair)) = self.rx_request.recv().await {
            if self.suppress_repair {
                debug!(
                    "Suppressing Byzantine repair response for {} requested batch(es)",
                    digests.len()
                );
                continue;
            }
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
                        let kind = if optimistic_leader_repair {
                            "OptimisticBatch"
                        } else {
                            "Batch"
                        };
                        self.network
                            .send_typed(address, Bytes::from(data), kind)
                            .await
                    }
                    Ok(None) => (),
                    Err(e) => error!("{}", e),
                }
            }
        }
    }
}
