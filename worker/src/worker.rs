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
use futures::sink::SinkExt as _;
use log::{error, info, warn};
use metrics::{start_prometheus_server, MetricReporter, Metrics};
use network::{MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Sender};

#[cfg(test)]
#[path = "tests/worker_tests.rs"]
pub mod worker_tests;

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

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
        reporter.clone().start();
        start_prometheus_server(binding_metrics_address, &registry);
        info!("Worker {} metrics listening on {}", id, metrics_address);

        // Define a worker instance.
        let worker = Self {
            name,
            id,
            committee,
            parameters,
            store,
            metrics: metrics.clone(),
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages();                         //spawns async task that listens for network message from Primary
        worker.handle_clients_transactions(tx_primary.clone());   //spawns async task that listens for network messages from Client
        worker.handle_workers_messages(tx_primary);               //spawns async task that listens for network messages from other Workers

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,                              //filter primary associated with current worker based on the committee config.
            rx_primary,                                          //receiver channel to connect to primary channel (i.e. how other listener functions can invoke to PrimaryConnector)
            worker.metrics.clone(),
            worker.parameters.compress_network,
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
    fn handle_primary_messages(&self) {
        let (tx_synchronizer, rx_synchronizer) = channel(CHANNEL_CAPACITY); //channel between PrimaryReceiverHandler and Synchronizer

        // Receive incoming messages from our primary.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .primary_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address,                                    //socket to receive Primary messages from
            /* handler */
            PrimaryReceiverHandler { tx_synchronizer, metrics: self.metrics.clone() }, //handler for received Primary messages, forwards them to synchronizer
            Some(self.metrics.clone()),
            self.parameters.compress_network,
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
            self.metrics.clone(),
            self.parameters.compress_network,
        );

        info!(
            "Worker {} listening to primary messages on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle clients transactions.
    fn handle_clients_transactions(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {  //tx_primary: channel between processor and PrimaryConnector
        let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);      //channel between TxReceive (Client) and batch maker
        //let (tx_quorum_waiter, rx_quorum_waiter) = channel(CHANNEL_CAPACITY);  //channel between batch maker and quorum waiter
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);          //channel between quorum waiter and processor

        // We first receive clients' transactions from the network.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address,                                            //socket to receive Client messages from
            /* handler */ TxReceiverHandler { tx_batch_maker, yield_counter: Arc::new(AtomicU64::new(0)) }, //handler for received Client messages, forwards them to batch maker
            Some(self.metrics.clone()),
            // METRICS-DASHBOARD-SPEC.md §8: client traffic is NEVER compressed --
            // `node::client::Client` builds its own raw `Framed`/`TcpStream` directly
            // (bypasses `network::SimpleSender` entirely, see `node/src/client.rs`),
            // so it never compresses regardless of this committee's own
            // `compress_network` setting. Always `false` here, independent of
            // `self.parameters.compress_network` (which only governs primary<->worker/
            // primary<->primary/worker<->worker traffic, all of which DOES go through
            // `network::{Simple,Reliable}Sender`).
            false,
        );

        // The transactions are sent to the `BatchMaker` that assembles them into batches. It then broadcasts
        // (in a reliable manner) the batches to all other workers that share the same `id` as us. Finally, it
        // gathers the 'cancel handlers' of the messages and send them to the `QuorumWaiter`.
        BatchMaker::spawn(
            self.parameters.batch_size,
            self.parameters.max_batch_delay,
            /* rx_transaction */ rx_batch_maker,  //receiver channel to connect to TxReceiverHandler 
            // tx_message tx_quorum_waiter,   //sender channel to connect to quorum waiter
           /* tx_batch */ tx_processor,  //sender channel to connect to processor
            /* workers_addresses */
            self.committee
                .others_workers(&self.name, &self.id)
                .iter()
                .map(|(name, addresses)| (*name, addresses.worker_to_worker))
                .collect(),
            self.metrics.clone(),
            self.parameters.compress_network,
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
            /* rx_batch */ rx_processor,  //receiver channel to connect to quorum waiter
            /* tx_digest */ tx_primary,   //sender channel to connect to PrimaryConnector
            /* own_batch */ true,
        );

        info!(
            "Worker {} listening to client transactions on {}",
            self.id, address
        );
    }

    /// Spawn all tasks responsible to handle messages from other workers.
    fn handle_workers_messages(&self, tx_primary: Sender<SerializedBatchDigestMessage>) {
        let (tx_helper, rx_helper) = channel(CHANNEL_CAPACITY);         //channel between WorkReceiverHandler and Helper
        let (tx_processor, rx_processor) = channel(CHANNEL_CAPACITY);   //channel between WorkReceiverHandler and Processor

        // Receive incoming messages from other workers.
        let mut address = self
            .committee
            .worker(&self.name, &self.id)
            .expect("Our public key or worker id is not in the committee")
            .worker_to_worker;
        address.set_ip("0.0.0.0".parse().unwrap());
        Receiver::spawn_full(
            address,                     //socket to receive Worker messages from
            /* handler */
            WorkerReceiverHandler {      //handler for received Worker messages, forwards them either to helper, or processor -- depending on (?)
                tx_helper,               //sender channel to connect to helper
                tx_processor,            //sender channel to connect to processor
                metrics: self.metrics.clone(),
            },
            Some(self.metrics.clone()),
            self.parameters.compress_network,
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,   //receiver channel to connect to WorkerReceiverHandler
            self.metrics.clone(),
            self.parameters.compress_network,
        );

        // This `Processor` hashes and stores the batches we receive from the other workers. It then forwards the
        // batch's digest to the `PrimaryConnector` that will send it to our primary.
        Processor::spawn(
            self.id,
            self.store.clone(),
            /* rx_batch */ rx_processor,   //receiver channel to connect to WorkerReceiverHandler
            /* tx_digest */ tx_primary,    //sender channel to connect to PrimaryConnector
            /* own_batch */ false,
        );

        info!(
            "Worker {} listening to worker messages on {}",
            self.id, address
        );
    }
}

/////////////////////////// Network Handlers ///////////////////////////////


/// Defines how the network receiver handles incoming transactions.
//Note: Only expect to receive client messages submitting new transactions.
#[derive(Clone)]
struct TxReceiverHandler {
    tx_batch_maker: Sender<Transaction>,  //sender channel to connect to batch maker
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
        if self.yield_counter.fetch_add(1, Ordering::Relaxed).is_multiple_of(TX_YIELD_EVERY) {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming workers messages.
//Note: Only expect to receive worker messages that are a) proposing batches, or b) acknowledging batches
#[derive(Clone)]
struct WorkerReceiverHandler {
    tx_helper: Sender<(Vec<Digest>, PublicKey)>,   //sender channel to connect to helper
    tx_processor: Sender<SerializedBatchMessage>,  //sender channel to connect to processor
    metrics: Arc<Metrics>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        //NEW: Do not need to Reply with an ack... Currently simple sender expects it though so we keep it (useful for debugging). Simple sender just sinks the reply.
        // // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;     //Question: Where is ack signed? Is authenticated channel assumed? TLS?
        // //Acknowledge Batches received.
        // //Note: Missing Batch Requests don't expect an ack (they use simple sender) -- seems like it is sent anyways, but origin probably simply ignores it.

        // Deserialize and parse the message.
        match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::Batch(..)) => {     //If receive batch message from another worker. Store the batch, and process.
                self.metrics.network_messages_received_total.with_label_values(&["Batch"]).inc();
                self.metrics.network_bytes_received_total.with_label_values(&["Batch"]).inc_by(serialized.len() as u64);
                // `serialized` is already an owned `Bytes` -- forward it directly
                // instead of the previous `serialized.to_vec()`, which copied every
                // received batch a second time for no reason (Fable perf audit item
                // 3). `Processor` now does the one unavoidable `Bytes -> Vec<u8>`
                // copy itself, right at `Store::write`'s fixed `Vec<u8>` boundary.
                self
                .tx_processor
                .send(serialized)
                .await
                .expect("Failed to send batch")
            },
            Ok(WorkerMessage::BatchRequest(missing, requestor)) => {  //If receive message from another worker that is missing a batch. Reply if we have batch ourselves.
                self.metrics.network_messages_received_total.with_label_values(&["BatchRequest"]).inc();
                self.metrics.network_bytes_received_total.with_label_values(&["BatchRequest"]).inc_by(serialized.len() as u64);
                self
                .tx_helper
                .send((missing, requestor))
                .await
                .expect("Failed to send batch request")
            },
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}

/// Defines how the network receiver handles incoming primary messages.
//Note: Only expect to receive primary messages requesting synchronization.
#[derive(Clone)]
struct PrimaryReceiverHandler {
    tx_synchronizer: Sender<PrimaryWorkerMessage>,  //sender channel to connect to synchronizer.
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
                self.metrics.network_messages_received_total.with_label_values(&[message.type_name()]).inc();
                self.metrics.network_bytes_received_total.with_label_values(&[message.type_name()]).inc_by(serialized.len() as u64);
                self
                .tx_synchronizer
                .send(message)
                .await
                .expect("Failed to send transaction")
            },
        }
        Ok(())
    }
}
