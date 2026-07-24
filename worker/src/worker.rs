// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{Batch, BatchMaker, Transaction};
use crate::helper::Helper;
use crate::primary_connector::PrimaryConnector;
use crate::processor::{Processor, SerializedBatchMessage};
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PairwiseKeys, PublicKey};
use log::{error, info, warn};
use metrics::{start_prometheus_server, MetricReporter, Metrics};
use network::{BatchConfig, MessageHandler, Receiver, Writer};
use primary::PrimaryWorkerMessage;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    /// Transport-level batching config, resolved once at spawn time from
    /// `parameters.batch_{messages,max_bytes,max_delay_ms}` (mirrors `latency_map`'s
    /// own resolve-once-at-spawn convention). Threaded into every worker-to-worker/
    /// worker-to-primary-reply `SimpleSender` this worker spawns, and into the
    /// matching `network::Receiver`s -- EXCEPT the client transaction port, which
    /// never batches (see `handle_clients_transactions`).
    batch: BatchConfig,
    /// SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels
    /// (`Parameters::authenticate_channels`), resolved once at spawn time (same
    /// convention as `latency_map`/`batch`). `None` (the default) is byte-identical
    /// to pre-MAC behavior. Threaded into every worker-to-worker/worker-to-primary
    /// sender and receiver this worker spawns -- EXCEPT the client transaction port
    /// (clients aren't committee members and hold no key, same carve-out as
    /// `compress_network`/`batch_messages`).
    channel_auth: Option<Arc<PairwiseKeys>>,
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

        // Resolved once, same convention as `latency_map` above.
        let batch = BatchConfig {
            enabled: parameters.batch_messages,
            max_bytes: parameters.batch_max_bytes,
            max_delay_ms: parameters.batch_max_delay_ms,
        };

        // SECURITY (Fable audit): symmetric pairwise-MAC authenticated channels,
        // resolved once, same convention as `latency_map`/`batch` above.
        // `authenticate_channels` on with no `mac_secret` set is a misconfiguration
        // (would otherwise silently run unauthenticated) -- panic loudly rather than
        // let it pass.
        let channel_auth: Option<Arc<PairwiseKeys>> = if parameters.authenticate_channels {
            let secret = parameters.mac_secret.expect("authenticate_channels is set but mac_secret is None (misconfiguration)");
            Some(Arc::new(committee.pairwise_keys(&name, &secret)))
        } else {
            None
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
            batch,
            channel_auth,
        };

        // Spawn all worker tasks.
        let (tx_primary, rx_primary) = channel(CHANNEL_CAPACITY);
        worker.handle_primary_messages();                         //spawns async task that listens for network message from Primary
        worker.handle_clients_transactions(tx_primary.clone());   //spawns async task that listens for network messages from Client
        worker.handle_workers_messages(tx_primary);               //spawns async task that listens for network messages from other Workers

        // The `PrimaryConnector` allows the worker to send messages to its primary.
        PrimaryConnector::spawn(
            worker.name,
            worker
                .committee
                .primary(&worker.name)
                .expect("Our public key is not in the committee")
                .worker_to_primary,                              //filter primary associated with current worker based on the committee config.
            rx_primary,                                          //receiver channel to connect to primary channel (i.e. how other listener functions can invoke to PrimaryConnector)
            worker.metrics.clone(),
            worker.parameters.compress_network,
            worker.batch,
            worker.channel_auth.clone(),
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
            PrimaryReceiverHandler { tx_synchronizer, metrics: self.metrics.clone(), name: self.name, channel_auth: self.channel_auth.clone() }, //handler for received Primary messages, forwards them to synchronizer
            Some(self.metrics.clone()),
            self.parameters.compress_network,
            // This handler never acked (see its `dispatch`'s doc comment).
            /* acks */ false,
            self.parameters.batch_messages,
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
            self.parameters.compress_network,
            self.batch,
            self.channel_auth.clone(),
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
            // This handler never acked either (see its `dispatch`).
            /* acks */ false,
            // Client traffic is NEVER batched -- `node::client::Client` sends raw,
            // unbundled frames (same bypass-of-`network::{Simple,Reliable}Sender`
            // reasoning as the `compress` argument just above). Always `false` here,
            // independent of `self.parameters.batch_messages`.
            /* batch */ false,
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
            self.latency_map.clone(),
            self.metrics.clone(),
            self.parameters.compress_network,
            self.batch,
            self.channel_auth.clone(),
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
                channel_auth: self.channel_auth.clone(),
            },
            Some(self.metrics.clone()),
            self.parameters.compress_network,
            // This handler acks every received frame (moved out of `dispatch` -- see
            // its doc comment).
            /* acks */ true,
            self.parameters.batch_messages,
        );

        // The `Helper` is dedicated to reply to batch requests from other workers.
        Helper::spawn(
            self.id,
            self.committee.clone(),
            self.store.clone(),
            /* rx_request */ rx_helper,   //receiver channel to connect to WorkerReceiverHandler
            self.latency_map.clone(),
            self.metrics.clone(),
            self.parameters.compress_network,
            self.batch,
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
    /// SECURITY (Fable audit): `Parameters::authenticate_channels`. `None` is
    /// byte-identical to pre-MAC behavior. `WorkerMessage::Batch` carries no sender
    /// claim at all (own-batch broadcasts and `Helper`'s cross-authority relays are
    /// indistinguishable on the wire -- the same D4-class gap as `Header(_, true)`/
    /// `ControlServe` on the primary side) -- its bytes (tag included, if the flag is
    /// on) are forwarded to `Processor` completely untouched: `Processor` content-
    /// addresses a batch by hashing EXACTLY the bytes it's handed, so every copy of
    /// "the same" batch floating around the network (the original seal, and every
    /// relayed/gossiped copy) must stay byte-identical for `store.read(digest)`
    /// lookups to ever hit -- this handler must never strip or alter a `Batch`'s
    /// bytes. `WorkerMessage::BatchRequest` DOES carry a genuine sender claim
    /// (`origin`) and IS verified normally, once we know that's the variant we have.
    channel_auth: Option<Arc<PairwiseKeys>>,
}

#[async_trait]
impl MessageHandler for WorkerReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // The ack (kept for debugging -- `SimpleSender` never required it, it just
        // sinks whatever reply arrives) is now sent by `network::Receiver` itself,
        // once per received FRAME rather than once per `dispatch` call -- required
        // for batching (several logical messages can share one frame, and only one
        // ack may be sent per frame). See `Receiver::acks`'s doc comment.

        // SECURITY (Fable audit): deserialize the FULL received bytes first -- bincode
        // tolerates (ignores) any trailing bytes beyond what a value actually needs,
        // so this succeeds identically whether or not a MAC tag is appended, without
        // this handler needing to guess up front how many trailing bytes (if any) to
        // strip. See `channel_auth`'s doc comment for why `Batch`'s bytes are then
        // never touched, while `BatchRequest`'s tag IS split off and verified.
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
                if let Some(auth) = &self.channel_auth {
                    let Some((payload, tag)) = crypto::mac::split_tag(&serialized) else {
                        return Ok(());
                    };
                    if !auth.verify(&requestor, payload, &tag) {
                        self.metrics.authenticated_channel_rejected_total.inc();
                        return Ok(());
                    }
                }
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
    /// SECURITY (Fable audit): this worker's own public key -- the worker<->primary
    /// channel is intra-authority (our own primary shares our own public key), so the
    /// MAC candidate sender for every message on this port is always `name` itself
    /// (`k_{name,name}`, the degenerate self-pair key). Unused when `channel_auth` is
    /// `None`.
    name: PublicKey,
    /// `Parameters::authenticate_channels`; `None` is byte-identical to pre-MAC
    /// behavior.
    channel_auth: Option<Arc<PairwiseKeys>>,
}

#[async_trait]
impl MessageHandler for PrimaryReceiverHandler {
    async fn dispatch(
        &self,
        _writer: &mut Writer,
        serialized: Bytes,
    ) -> Result<(), Box<dyn Error>> {
        // SECURITY (Fable audit): strip and verify the trailing MAC tag before
        // deserializing -- same contract as `crate::vantage::node::
        // VantageReceiverHandler::dispatch`.
        let (payload, tag): (&[u8], Option<[u8; crypto::mac::TAG_LEN]>) = match &self.channel_auth {
            Some(_) => match crypto::mac::split_tag(&serialized) {
                Some((payload, tag)) => (payload, Some(tag)),
                None => return Ok(()),
            },
            None => (&serialized[..], None),
        };

        // Deserialize the message and send it to the synchronizer.
        match bincode::deserialize::<PrimaryWorkerMessage>(payload) {
            Err(e) => error!("Failed to deserialize primary message: {}", e),
            Ok(message) => {
                if let (Some(auth), Some(tag)) = (&self.channel_auth, tag) {
                    if !auth.verify(&self.name, payload, &tag) {
                        self.metrics.authenticated_channel_rejected_total.inc();
                        return Ok(());
                    }
                }
                self.metrics.network_messages_received_total.with_label_values(&[message.type_name()]).inc();
                self.metrics.network_bytes_received_total.with_label_values(&[message.type_name()]).inc_by(payload.len() as u64);
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
