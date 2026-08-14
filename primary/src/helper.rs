// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Header, Proposal};
use crate::primary::{Height, PrimaryMessage};
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use metrics::Metrics;
use network::{BatchConfig, SimpleSender};
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
#[path = "tests/helper_tests.rs"]
pub mod helper_tests;

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
    /// Whole Autobahn suffix requests selected by a PoA or elected leader.
    rx_proposal_headers: Receiver<(Proposal, Height, PublicKey)>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
    metrics: Arc<Metrics>,
    /// Benchmark-only Byzantine behavior: do not serve lane metadata to peers
    /// excluded from the original narrowcast.
    suppressed_repair_destinations: Option<HashSet<PublicKey>>,
    /// Shared time window controlling when repair suppression is active.
    withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,
}

impl Helper {
    // The independent request channels keep receiver dispatch typed.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,
        rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey, bool)>,
        rx_proposal_headers: Receiver<(Proposal, Height, PublicKey)>,
        metrics: Arc<Metrics>,
        batch: BatchConfig,
        suppressed_repair_destinations: Option<HashSet<PublicKey>>,
        withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries_certs,
                rx_primaries_headers,
                rx_proposal_headers,
                network: SimpleSender::new()
                    .with_metrics(metrics.clone())
                    .with_batching(batch),
                metrics,
                suppressed_repair_destinations,
                withhold_window,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some((digests, origin)) = self.rx_primaries_certs.recv() => {

                    if self.suppress_repair_to(&origin) {
                        continue;
                    }

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

                    if self.suppress_repair_to(&origin) {
                        continue;
                    }

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
                Some((proposal, stop_height, origin)) = self.rx_proposal_headers.recv() => {
                    if self.suppress_repair_to(&origin) {
                        continue;
                    }
                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected proposal suffix request: {}", e);
                            continue;
                        }
                    };
                    let Some(poa) = proposal.poa.as_ref() else {
                        warn!("Ignoring proof-free Autobahn suffix request");
                        continue;
                    };
                    let lane = poa.author;
                    if stop_height >= proposal.height
                        || proposal.verify(&lane, &self.committee).is_err()
                    {
                        warn!("Ignoring malformed Autobahn suffix request");
                        continue;
                    }
                    let mut digest = proposal.header_digest.clone();
                    let mut height = proposal.height;
                    let mut headers = Vec::new();
                    while height > stop_height {
                        let data = match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => data,
                            Ok(None) => break,
                            Err(e) => {
                                error!("{}", e);
                                break;
                            }
                        };
                        let header: Header = match bincode::deserialize(&data) {
                            Ok(header) => header,
                            Err(error) => {
                                error!("Failed to deserialize stored proposal header: {}", error);
                                break;
                            }
                        };
                        if header.author != lane || header.height != height || header.id != digest {
                            warn!("Stored header does not match requested Autobahn suffix");
                            break;
                        }
                        digest = header.parent_cert.header_digest.clone();
                        height = header.parent_cert.height;
                        headers.push(header);
                    }
                    if headers.is_empty() {
                        continue;
                    }
                    // The requester processes one signed header at a time; oldest
                    // first ensures that observing the tip implies its prefix has
                    // already entered the validation pipeline.
                    headers.reverse();
                    let response = PrimaryMessage::ProposalHeaders(headers);
                    let bytes = bincode::serialize(&response)
                        .expect("Failed to serialize proposal suffix response");
                    self.network
                        .send_typed(address, Bytes::from(bytes), "ProposalHeaders")
                        .await;
                },
            };
        }
    }

    fn suppress_repair_to(&self, origin: &PublicKey) -> bool {
        self.suppressed_repair_destinations
            .as_ref()
            .is_some_and(|blocked| {
                blocked.contains(origin)
                    && config::withhold_active(self.withhold_window.as_deref(), Instant::now())
            })
    }
}
