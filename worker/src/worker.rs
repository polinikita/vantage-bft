// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
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

/// Build a probe over one of this worker's bounded channels, for the shared sampler in
/// `metrics::spawn_queue_sampler`.
///
/// Holding a `Sender` clone keeps that channel open for the process's lifetime. That is
/// intended and harmless -- every one of these senders is already cloned into a long-lived
/// network handler or spawned task, so none was ever going to be dropped while the node
/// runs -- but it does mean a probe observes occupancy only, never closure.
fn probe<T: Send + 'static>(stage: &'static str, tx: Sender<T>) -> QueueProbe {
    QueueProbe {
        stage,
        occupancy: Box::new(move || (tx.max_capacity() - tx.capacity(), tx.max_capacity())),
    }
}

/// Wrap a `Store` as the sampler's store probe.
fn store_probe(store: Store) -> StoreProbe {
    let depth = store.clone();
    let beat = store.clone();
    StoreProbe {
        occupancy: Box::new(move || (depth.queue_depth(), depth.queue_capacity())),
        heartbeat_millis: Box::new(move || beat.heartbeat_millis()),
        commands_drained: Box::new(move || store.commands_drained()),
    }
}

/// The primary round number.
// TODO: Move to the primary.
pub type Round = u64;

/// Indicates a serialized `WorkerPrimaryMessage` message.
pub type SerializedBatchDigestMessage = Vec<u8>;

/// The message exchanged between workers.
#[derive(Debug, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
    BatchRequest(Vec<Digest>, /* origin */ PublicKey),
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
    /// Starfish-parity metrics (PHASE2-SPEC.md #5): the registry is always served, this
    /// worker's own real-transaction-latency counters are only observed into under the
    /// `benchmark` feature (see `Synchronizer::observe_committed`).
    metrics: Arc<Metrics>,
    /// Fable audit item 4 (WAN latency injection): this authority's own
    /// per-destination artificial latency map, resolved once at spawn time exactly
    /// the way `Core::spawn`/`vantage::node::VantageCore::spawn` resolve theirs (same
    /// `Committee::latency_map` call, same `name`/`parameters.latency_table`) --
    /// empty (== current behavior, byte-identical) unless `--latency-table`/
    /// `--mimic-latency-ms` set `parameters.latency_table`. Threaded into every
    /// worker-to-worker/worker-to-primary-reply `SimpleSender` this worker spawns
    /// (`BatchMaker`, `Synchronizer`, `Helper`), which previously ran at zero
    /// injected delay even under a WAN-shaped run.
    latency_map: HashMap<SocketAddr, Duration>,
    /// Data-plane withholding fault injector (`Parameters::withhold_senders`),
    /// resolved once at spawn time (same convention as `latency_map` above) via
    /// `config::withheld_destinations`. `None` -- the default, and always the case
    /// when `--withhold` is 0 -- means this authority is not a withholding sender:
    /// `handle_clients_transactions`'s `workers_addresses` list is built exactly as
    /// before. `Some(blocked)` excludes every peer in `blocked` from that list, once,
    /// at the same point it's otherwise constructed -- `BatchMaker` itself never sees
    /// this field, and its own broadcast in `seal` is completely unchanged either way.
    withheld_destinations: Option<HashSet<PublicKey>>,
    /// Data-plane withholding fault injector, TIME-WINDOWED variant
    /// (`Parameters::withhold_window`): the shared, in-process "has the window opened
    /// yet" cell, cloned straight from `parameters` -- no `config::` resolution
    /// needed here, unlike `withheld_destinations`, since this cell doesn't depend on
    /// OUR OWN committee position at all. Threaded through to `BatchMaker::spawn`, which
    /// consults it (via `config::withhold_active`) once per seal to decide whether
    /// `withheld_destinations`' filter is currently active -- see `handle_clients_
    /// transactions`'s own comment at that call site. `None` whenever `--withhold-at`
    /// isn't given (including whenever `withheld_destinations` itself is already
    /// `None`), in which case `BatchMaker` never even looks at it.
    withhold_window: Option<Arc<OnceLock<(Instant, Instant)>>>,
    /// Transport-level batching config, resolved once at spawn time from
    /// `parameters.batch_{messages,max_bytes,max_delay_ms}` (mirrors `latency_map`'s
    /// own resolve-once-at-spawn convention). Threaded into every worker-to-worker/
    /// worker-to-primary-reply `SimpleSender` this worker spawns, and into the
    /// matching `network::Receiver`s -- EXCEPT the client transaction port, which
    /// never batches (see `handle_clients_transactions`).
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
        // Boot the (always-on, starfish-parity) Prometheus metrics server.
        let metrics_address = committee
            .worker(&name, &id)
            .expect("Our public key or worker id is not in the committee")
            .metrics;
        let mut binding_metrics_address = metrics_address;
        binding_metrics_address.set_ip("0.0.0.0".parse().unwrap());
        let registry = Registry::new();
        let (metrics, reporter) = Metrics::new(&registry);
        // METRICS-DASHBOARD-SPEC.md §8: write-once at boot.
        metrics.set_protocol_info(parameters.protocol.label());
        if let Some(mode) = parameters.tx_mode.as_deref() {
            metrics.set_transaction_mode_info(mode);
        }
        // Metrics-active window: the worker owns the commit-time observation path
        // (`synchronizer::read_and_observe_batch`), so this is where the gate must be
        // armed. Absent from parameters.json -> no gate, as before.
        metrics.set_active_from_millis(parameters.metrics_active_at_ms);
        reporter.clone().start();
        start_prometheus_server(binding_metrics_address, &registry);
        info!("Worker {} metrics listening on {}", id, metrics_address);

        // Fable audit item 4: resolved once, relative to OUR OWN committee index --
        // empty (== current behavior) unless `--latency-table`/`--mimic-latency-ms`
        // set `parameters.latency_table`. Identical construction to
        // `vantage::node::VantageCore::spawn`'s own `latency_map` (same table, same
        // per-authority resolution), so worker-to-worker traffic is delayed the same
        // way primary-to-primary traffic already is.
        let latency_map = parameters
            .latency_table
            .as_deref()
            .map(|table| committee.latency_map(&name, table))
            .unwrap_or_default();

        // Data-plane withholding fault injector: resolved once, same convention as
        // `latency_map` above.
        let withheld_destinations =
            config::withheld_destinations(&committee, &name, parameters.withhold_senders);

        // Data-plane withholding fault injector, time-windowed variant: just a clone
        // of the shared cell (no `config::` resolution needed -- see this field's own
        // doc comment on `Worker`).
        let withhold_window = parameters.withhold_window.clone();

        // Resolved once, same convention as `latency_map` above.
        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        // Define a worker instance.
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

        // Task panics are otherwise dropped on the floor (nothing awaits a
        // `JoinHandle` anywhere in this codebase) -- see `install_panic_hook`.
        Metrics::install_panic_hook(metrics.clone());

        // Spawn all worker tasks. Each `handle_*` returns occupancy probes over the
        // bounded channels it created, which `spawn_queue_sampler` then publishes.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        let mut probes = vec![probe("primary_connector", tx_primary.clone())];
        probes.extend(worker.handle_primary_messages()); //spawns async task that listens for network message from Primary
        probes.extend(worker.handle_clients_transactions(tx_primary.clone())); //spawns async task that listens for network messages from Client
        probes.extend(worker.handle_workers_messages(tx_primary)); //spawns async task that listens for network messages from other Workers
        spawn_queue_sampler(probes, store_probe(worker.store.clone()), metrics.clone());

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary, //filter primary associated with current worker based on the committee config.
            rx_primary, //receiver channel to connect to primary channel (i.e. how other listener functions can invoke to PrimaryConnector)
            worker.metrics.clone(),
            worker.batch,
        );

        // NOTE: This log entry is used to compute performance.
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

    ///////////////////////// TASK INSTANTIATORS ///////////////////////////////////

    /// Spawn all tasks responsible to handle messages from our primary.
    ///
    /// Returns occupancy probes over the channels it creates, for the sampler in
    /// `spawn`. `synchronizer` is the path that carried the ~600 `Synchronize`/s flood
    /// on wedged nodes on 2026-08-08 against ~11/s healthy.
    fn handle_primary_messages(&self) -> Vec<QueueProbe> {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY); //channel between PrimaryReceiverHandler and Synchronizer
        let probes = vec![probe("synchronizer", tx_synchronizer.clone())];

        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address, //socket to receive Primary messages from
            /* handler */
            PrimaryReceiverHandler {
                tx_synchronizer,
                metrics: self.metrics.clone(),
            }, //handler for received Primary messages, forwards them to synchronizer
            Some(self.metrics.clone()),
            // This handler never acked (see its `dispatch`'s doc comment).
            /* acks */
            false,
            self.parameters.batch_messages,
            "primary_to_worker",
        );

        // The `Synchronizer` is responsible to keep the worker in sync with the others. It handles the commands
        // it receives from the primary (which are mainly notifications that we are out of sync).
        Synchronizer::spawn(
            self.name,
            self.id,
            self.committee.clone(),
            self.store.clone(),
            self.parameters.gc_depth,
            self.parameters.sync_retry_delay,
            self.parameters.sync_retry_nodes,
            /* rx_message */ rx_synchronizer,
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

    /// Spawn all tasks responsible to handle clients transactions.
    ///
    /// Returns occupancy probes over the channels it creates (see
    /// `handle_primary_messages`). `processor_own` carries OUR sealed batches; the
    /// same-typed channel in `handle_workers_messages` carries peers' and is labeled
    /// `processor_peer`, because a wedge on one and not the other means very different
    /// things.
    fn handle_clients_transactions(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) -> Vec<QueueProbe> {
        //tx_primary: channel between processor and PrimaryConnector
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY); //channel between TxReceive (Client) and batch maker
                                                                          //let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);  //channel between batch maker and quorum waiter
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY); //channel between quorum waiter and processor
        let probes = vec![
            probe("batch_maker", tx_batch_maker.clone()),
            probe("processor_own", tx_processor.clone()),
        ];

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address, //socket to receive Client messages from
            /* handler */
            TxReceiverHandler {
                tx_batch_maker,
                yield_counter: Arc::new(AtomicU64::new(0)),
            }, //handler for received Client messages, forwards them to batch maker
            Some(self.metrics.clone()),
            // This handler never acked either (see its `dispatch`).
            /* acks */
            false,
            // Client traffic is NEVER batched -- `node::client::Client` sends raw,
            // unbundled frames (bypasses `network::{Simple,Reliable}Sender` entirely,
            // see `node/src/client.rs`). Always `false` here, independent of
            // `self.parameters.batch_messages`.
            /* batch */
            false,
            "transactions",
        );

        // Data-plane withholding fault injector, time-windowed variant: `BatchMaker`
        // now makes the filtered-vs-full choice PER SEAL (time-windowed withholding
        // can turn on/off mid-run, unlike c35fc4a's original whole-run filter), so
        // BOTH the full and the filtered address list must be resolved here --
        // `full_workers_addresses` is exactly `workers_addresses`' old (unfiltered)
        // construction; `withheld_workers_addresses` is `None` unless THIS authority
        // is a withholding sender at all (`--withhold`), in which case `BatchMaker::
        // seal` never even consults `self.withhold_window` -- one cheap `match`
        // discriminant, no allocation, no perturbation, on that default path.
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

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */
            rx_batch_maker, //receiver channel to connect to TxReceiverHandler
            // tx_message tx_quorum_waiter,   //sender channel to connect to quorum waiter
            /* tx_batch */
            tx_processor, //sender channel to connect to processor
            /* workers_addresses */ full_workers_addresses,
            /* withheld_workers_addresses */ withheld_workers_addresses,
            /* withhold_window */ self.withhold_window.clone(),
            self.latency_map.clone(),
            self.metrics.clone(),
            self.batch,
        );

        // // The `QuorumWaiter` waits for 2f authorities to acknowledge reception of the batch. It then forwards
        // // the batch to the `Processor`.
        // QuorumWaiter::spawn(
        //     self.committee.clone(),
        //     /* stake */ self.committee.stake(&self.name),
        //     /* rx_message */ rx_quorum_waiter, //receiver channel to connect to batch maker.
        //     /* tx_batch */ tx_processor,  //sender channel to connect to processor
        // );

        // The `Processor` hashes and stores the batch. It then forwards the batch's digest to the `PrimaryConnector`
        // that will send it to our primary machine.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor, //receiver channel to connect to quorum waiter
            /* tx_digest */ tx_primary, //sender channel to connect to PrimaryConnector
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
        probes
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    ///
    /// Returns occupancy probes over the channels it creates (see
    /// `handle_primary_messages`). `processor_peer` is the inbound `Batch` path whose
    /// byte counter read a flat zero on every wedged node on 2026-08-08.
    fn handle_workers_messages(
        &self,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) -> Vec<QueueProbe> {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY); //channel between WorkReceiverHandler and Helper
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY); //channel between WorkReceiverHandler and Processor
        let probes = vec![
            probe("helper", tx_helper.clone()),
            probe("processor_peer", tx_processor.clone()),
        ];

        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address, //socket to receive Worker messages from
            /* handler */
            WorkerReceiverHandler {
                //handler for received Worker messages, forwards them either to helper, or processor -- depending on (?)
                tx_helper,    //sender channel to connect to helper
                tx_processor, //sender channel to connect to processor
                metrics: self.metrics.clone(),
            },
            Some(self.metrics.clone()),
            // This handler acks every received frame (moved out of `dispatch` -- see
            // its doc comment).
            /* acks */
            true,
            self.parameters.batch_messages,
            "worker_to_worker",
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */
            rx_helper, //receiver channel to connect to WorkerReceiverHandler
            self.latency_map.clone(),
            self.metrics.clone(),
            self.batch,
        );

        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */
            rx_processor, //receiver channel to connect to WorkerReceiverHandler
            /* tx_digest */ tx_primary, //sender channel to connect to PrimaryConnector
            /* own_batch */ false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
        probes
    }
}

/////////////////////////// Network Handlers ///////////////////////////////

/// Defines how the network receiver handles incoming transactions.
//Note: Only expect to receive client messages submitting new transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>, //sender channel to connect to batch maker
    /// Fable perf audit item 1: shared (across every connection this handler's
    /// `Receiver` accepts -- `handler.clone()` per accepted connection, see
    /// `network::Receiver::run`) counter gating how often `dispatch` actually yields.
    /// Purely a scheduling-fairness knob (see `dispatch`'s doc comment); no protocol
    /// effect either way.
    yield_counter: Arc<AtomicU64>,
}

/// See `TxReceiverHandler::yield_counter`'s doc comment.
const TX_YIELD_EVERY: u64 = 128;

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Send the transaction to the batch maker. `message` is already an owned
        // `Bytes` handed to us by `network::Receiver` -- forward it directly instead
        // of the previous `message.to_vec()`, which copied every single transaction
        // out of an already-owned buffer for no reason (Fable perf audit item 1).
        self.tx_batch_maker
            .send(message)
            .await
            .expect("Failed to send transaction");

        // Occasionally give the chance to schedule other tasks, instead of on every
        // single transaction (Fable perf audit item 1) -- tokio's own cooperative
        // scheduling budget already yields periodically regardless; this just adds
        // an explicit, cheap backstop under sustained client load.
        if self
            .yield_counter
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(TX_YIELD_EVERY)
        {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
//Note: Only expect to receive worker messages that are a) proposing batches, or b) acknowledging batches
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>, //sender channel to connect to helper
    tx_processor: Sender<SerializedBatchMessage>, //sender channel to connect to processor
    metrics: Arc<Metrics>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // The ack (kept for debugging -- `SimpleSender` never required it, it just
        // sinks whatever reply arrives) is now sent by `network::Receiver` itself,
        // once per received FRAME rather than once per `dispatch` call -- required
        // for batching (several logical messages can share one frame, and only one
        // ack may be sent per frame). See `Receiver::acks`'s doc comment.

        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(..)) => {
                //If receive batch message from another worker. Store the batch, and process.
                self.metrics
                    .network_messages_received_total
                    .with_label_values(&["Batch"])
                    .inc();
                self.metrics
                    .network_bytes_received_total
                    .with_label_values(&["Batch"])
                    .inc_by(serialized.len() as u64);
                // `serialized` is already an owned `Bytes` -- forward it directly
                // instead of the previous `serialized.to_vec()`, which copied every
                // received batch a second time for no reason (Fable perf audit item
                // 3). `Processor` now does the one unavoidable `Bytes -> Vec<u8>`
                // copy itself, right at `Store::write`'s fixed `Vec<u8>` boundary.
                self.tx_processor
                    .send(serialized)
                    .await
                    .expect("Failed to send batch")
            }
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => {
                //If receive message from another worker that is missing a batch. Reply if we have batch ourselves.
                self.metrics
                    .network_messages_received_total
                    .with_label_values(&["BatchRequest"])
                    .inc();
                self.metrics
                    .network_bytes_received_total
                    .with_label_values(&["BatchRequest"])
                    .inc_by(serialized.len() as u64);
                self.tx_helper
                    .send((missing, requestor))
                    .await
                    .expect("Failed to send batch request")
            }
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
//Note: Only expect to receive primary messages requesting synchronization.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>, //sender channel to connect to synchronizer.
    metrics: Arc<Metrics>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize::<PrimaryWorkerMessage>(&serialized) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => {
                self.metrics
                    .network_messages_received_total
                    .with_label_values(&[message.type_name()])
                    .inc();
                self.metrics
                    .network_bytes_received_total
                    .with_label_values(&[message.type_name()])
                    .inc_by(serialized.len() as u64);
                self.tx_synchronizer
                    .send(message)
                    .await
                    .expect("Failed to send transaction")
            }
        }
        Ok(())
    }
}
