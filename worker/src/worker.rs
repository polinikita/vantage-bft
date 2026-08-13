// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
#[cfg(feature = "benchmark")]
use crate::synchronizer::CommitObserver;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use log::{error, info, warn};
use metrics::{
    spawn_queue_sampler, start_prometheus_server, MetricReporter, Metrics, QueueProbe, StoreProbe,
};
use network::{BatchConfig, MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{channel, Sender};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Build an occupancy probe for a bounded worker channel.
fn probe<T: Send + 'static>(stage: &'static str, tx: Sender<T>) -> QueueProbe {
    QueueProbe {
        stage,
        occupancy: Box::new(move || (tx.max_capacity() - tx.capacity(), tx.max_capacity())),
    }
}

/// Build the store occupancy probe.
fn store_probe(store: Store) -> StoreProbe {
    let depth = store.clone();
    let beat = store.clone();
    StoreProbe {
        occupancy: Box::new(move || (depth.queue_depth(), depth.queue_capacity())),
        heartbeat_millis: Box::new(move || beat.heartbeat_millis()),
        commands_drained: Box::new(move || store.commands_drained()),
    }
}

/// Primary round number.
pub type Round = u64;

/// Serialized worker-to-primary message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// Message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, PublicKey),
    /// Batch request on the optimistic proposal's critical path. The current
    /// consensus leader, rather than the lane author, serves this request.
    OptimisticBatchRequest(Vec<Digest>, PublicKey),
}

#[derive(Deserialize)]
enum BorrowedWorkerMessage<'a> {
    Batch(#[serde(borrow)] Vec<&'a [u8]>),
    BatchRequest(Vec<Digest>, PublicKey),
    OptimisticBatchRequest(Vec<Digest>, PublicKey),
}

enum WorkerMessageRoute {
    Batch,
    BatchRequest(Vec<Digest>, PublicKey, bool),
}

fn route_worker_message(serialized: &[u8]) -> bincode::Result<WorkerMessageRoute> {
    match bincode::deserialize::<BorrowedWorkerMessage<'_>>(serialized)? {
        BorrowedWorkerMessage::Batch(transactions) => {
            let _ = transactions.len();
            Ok(WorkerMessageRoute::Batch)
        }
        BorrowedWorkerMessage::BatchRequest(missing, requestor) => {
            Ok(WorkerMessageRoute::BatchRequest(missing, requestor, false))
        }
        BorrowedWorkerMessage::OptimisticBatchRequest(missing, requestor) => {
            Ok(WorkerMessageRoute::BatchRequest(missing, requestor, true))
        }
    }
}

pub struct Worker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The configuration parameters.
    parameters: Parameters,
    /// The persistent storage.
    store: Store,
    /// Metrics registry and worker transaction counters.
    metrics: Arc<Metrics>,
    /// Per-destination network latency applied to worker senders.
    latency_map: HashMap<SocketAddr, Duration>,
    /// Peers excluded from worker batch broadcasts by the withholding configuration.
    withheld_destinations: Option<HashSet<PublicKey>>,
    /// Shared time window controlling when withholding is active.
    withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,
    /// Transport-level batching for worker-to-worker and worker-to-primary traffic.
    batch: BatchConfig,
}

impl Worker {
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        parameters: Parameters,
        store: Store,
    ) -> (Arc<Metrics>, Arc<MetricReporter>, Registry) {
        let metrics_address = committee
            .worker(&name, &id)
            .expect("Our public key or worker id is not in the committee")
            .metrics;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip("0.0.0.0".parse().unwrap());
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry);
        metrics.set_protocol_info(parameters.protocol.label());
        if let Some(mode) = parameters.tx_mode.as_deref() {
            metrics.set_transaction_mode_info(mode);
        }
        metrics.set_active_from_millis(parameters.metrics_active_at_ms);
        reporter.clone().start();
        start_prometheus_server(binding_metrics_address, &registry);
        info!("Worker {} metrics listening on {}", id, metrics_address);

        let latency_map = parameters
            .latency_table
            .as_deref()
            .map(|table| committee.latency_map(&name, table))
            .unwrap_or_default();

        let withheld_destinations = config::withheld_destinations(
            &committee,
            &name,
            parameters.withhold_senders,
            &parameters.withhold_publishers,
            parameters.withhold_count,
            parameters.withhold_stride,
            &parameters.withhold_receivers,
        );

        let withhold_window = parameters.withhold_window.clone();

        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
            metrics: metrics.clone(),
            latency_map,
            withheld_destinations,
            withhold_window,
            batch,
        };

        Metrics::install_panic_hook(metrics.clone());

        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        let mut probes = vec![probe("primary_connector", tx_primary.clone())];
        probes.extend(worker.handle_primary_messages());
        probes.extend(worker.handle_clients_transactions(tx_primary.clone()));
        probes.extend(worker.handle_workers_messages(tx_primary));
        spawn_queue_sampler(probes, store_probe(worker.store.clone()), metrics.clone());

        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,
            rx_primary,
            worker.metrics.clone(),
            worker.batch,
        );

        info!(
            "Worker {} successfully booted on {}",
            id,
            worker
                .committee
                .worker(&worker.name, &worker.id)
                .expect("Our public key or worker id is not in the committee")
                .transactions
                .ip()
        );

        (metrics, reporter, registry)
    }

    /// Spawn tasks for messages from the primary.
    /// Returns occupancy probes for the created channels.
    fn handle_primary_messages(&self) -> Vec<QueueProbe> {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY);
        let probes = vec![probe("synchronizer", tx_synchronizer.clone())];
        #[cfg(feature = "benchmark")]
        let (mut probes, tx_committed, rx_committed) = {
            let (tx_committed, rx_committed) = channel(CHANNEL_CAPACITY);
            (probes, tx_committed, rx_committed)
        };
        #[cfg(feature = "benchmark")]
        probes.push(probe("commit_observer", tx_committed.clone()));

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address,
            PrimaryReceiverHandler {
                tx_synchronizer,
                #[cfg(feature = "benchmark")]
                tx_committed,
                metrics: self.metrics.clone(),
            },
            Some(self.metrics.clone()),
            // Primary frames are not acknowledged.
            false,
            self.parameters.batch_messages,
            "primary_to_worker",
        );

        #[cfg(feature = "benchmark")]
        CommitObserver::spawn(self.store.clone(), rx_committed, self.metrics.clone());

        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            rx_synchronizer,
            self.latency_map.clone(),
            self.metrics.clone(),
            self.batch,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
        probes
    }

    /// Spawn tasks for client transactions.
    /// Returns occupancy probes for the created channels.
    fn handle_clients_transactions(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) -> Vec<QueueProbe> {
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);
        let probes = vec![
            probe("batch_maker", tx_batch_maker.clone()),
            probe("processor_own", tx_processor.clone()),
        ];

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address,
            TxReceiverHandler {
                tx_batch_maker,
                yield_counter: Arc::new(AtomicU64::new(0)),
            },
            Some(self.metrics.clone()),
            // Client frames are not acknowledged.
            false,
            // Client frames are unbundled.
            false,
            "transactions",
        );

        let full_workers_addresses: Vec<(PublicKey, SocketAddr)> = self
            .committee
            .others_workers(&self.name, &self.id)
            .iter()
            .map(|(name, addresses)| (*name, addresses.worker_to_worker))
            .collect();
        let withheld_workers_addresses: Option<Vec<(PublicKey, SocketAddr)>> =
            self.withheld_destinations.as_ref().map(|blocked| {
                full_workers_addresses
                    .iter()
                    .filter(|(name, _)| !blocked.contains(name))
                    .copied()
                    .collect()
            });

        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            rx_batch_maker,
            tx_processor,
            full_workers_addresses,
            withheld_workers_addresses,
            self.withhold_window.clone(),
            self.latency_map.clone(),
            self.metrics.clone(),
            self.batch,
        );

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            tx_primary,
            true,
            #[cfg(feature = "pipeline-tracing")]
            self.metrics.clone(),
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
        probes
    }

    /// Spawn tasks for messages from other workers.
    /// Returns occupancy probes for the created channels.
    fn handle_workers_messages(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) -> Vec<QueueProbe> {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);
        let probes = vec![
            probe("helper", tx_helper.clone()),
            probe("processor_peer", tx_processor.clone()),
        ];

        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address,
            WorkerReceiverHandler {
                tx_helper,
                tx_processor,
                metrics: self.metrics.clone(),
            },
            Some(self.metrics.clone()),
            // Acknowledge each received frame.
            true,
            self.parameters.batch_messages,
            "worker_to_worker",
        );

        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            rx_helper,
            self.latency_map.clone(),
            self.metrics.clone(),
            self.batch,
            self.parameters.withhold_repair && self.withheld_destinations.is_some(),
        );

        Processor::spawn(
            self.id,
            self.store.clone(),
            rx_processor,
            tx_primary,
            false,
            #[cfg(feature = "pipeline-tracing")]
            self.metrics.clone(),
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
        probes
    }
}

/// Handles client transactions from the network.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,
    /// Shared counter controlling cooperative yields.
    yield_counter: Arc<AtomicU64>,
}

/// Yield interval for client transaction handling.
const TX_YIELD_EVERY: u64 = 128;

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Forward the owned frame without copying.
        self.tx_batch_maker
            .send(message)
            .await
            .expect("Failed to send transaction");

        if self
            .yield_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            .is_multiple_of(TX_YIELD_EVERY)
        {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// Handles batches and batch requests from workers.
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey, bool)>,
    tx_processor: Sender<SerializedBatchMessage>,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match route_worker_message(&serialized) {
            Ok(WorkerMessageRoute::Batch) => {
                self.metrics
                    .network_messages_received_total
                    .with_label_values(&["Batch"])
                    .inc();
                self.metrics
                    .network_bytes_received_total
                    .with_label_values(&["Batch"])
                    .inc_by(serialized.len() as u64);
                // Forward the owned frame; storage performs the required copy.
                self.tx_processor
                    .send(serialized)
                    .await
                    .expect("Failed to send batch")
            }
            Ok(WorkerMessageRoute::BatchRequest(missing, requestor, optimistic_leader_repair)) => {
                let kind = if optimistic_leader_repair {
                    "OptimisticBatchRequest"
                } else {
                    "BatchRequest"
                };
                self.metrics
                    .network_messages_received_total
                    .with_label_values(&[kind])
                    .inc();
                self.metrics
                    .network_bytes_received_total
                    .with_label_values(&[kind])
                    .inc_by(serialized.len() as u64);
                self.tx_helper
                    .send((missing, requestor, optimistic_leader_repair))
                    .await
                    .expect("Failed to send batch request")
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Handles synchronization messages from the primary.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,
    #[cfg(feature = "benchmark")]
    tx_committed: Sender<(u64, Vec<Digest>)>,
    metrics: Arc<Metrics>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize::<PrimaryWorkerMessage>(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => {
                let kind = message.type_name();
                self.metrics
                    .network_messages_received_total
                    .with_label_values(&[kind])
                    .inc();
                self.metrics
                    .network_bytes_received_total
                    .with_label_values(&[kind])
                    .inc_by(serialized.len() as u64);
                match message {
                    PrimaryWorkerMessage::Committed(commit_millis, digests) => {
                        #[cfg(feature = "benchmark")]
                        self.tx_committed
                            .send((commit_millis, digests))
                            .await
                            .expect("Failed to send committed batches");
                        #[cfg(not(feature = "benchmark"))]
                        let _ = (commit_millis, digests);
                    }
                    message => self
                        .tx_synchronizer
                        .send(message)
                        .await
                        .expect("Failed to send primary message"),
                }
            }
        }
        Ok(())
    }
}
