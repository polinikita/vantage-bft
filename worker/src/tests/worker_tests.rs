// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;

#[test]
fn worker_message_routing_matches_wire_format() {
    let batch = WorkerMessage::Batch(vec![transaction()]);
    let bytes = bincode::serialize(&batch).unwrap();
    assert!(matches!(
        route_worker_message(&bytes).unwrap(),
        WorkerMessageRoute::Batch
    ));

    let request = WorkerMessage::BatchRequest(vec![batch_digest()], keys()[0].0);
    let bytes = bincode::serialize(&request).unwrap();
    assert!(matches!(
        route_worker_message(&bytes).unwrap(),
        WorkerMessageRoute::BatchRequest(missing, requestor, false, false)
            if missing == vec![batch_digest()] && requestor == keys()[0].0
    ));

    let request = WorkerMessage::OptimisticBatchRequest(vec![batch_digest()], keys()[0].0);
    let bytes = bincode::serialize(&request).unwrap();
    assert!(matches!(
        route_worker_message(&bytes).unwrap(),
        WorkerMessageRoute::BatchRequest(missing, requestor, true, false)
            if missing == vec![batch_digest()] && requestor == keys()[0].0
    ));

    let request = WorkerMessage::CommittedBatchRequest(vec![batch_digest()], keys()[0].0);
    let bytes = bincode::serialize(&request).unwrap();
    assert!(matches!(
        route_worker_message(&bytes).unwrap(),
        WorkerMessageRoute::BatchRequest(missing, requestor, false, true)
            if missing == vec![batch_digest()] && requestor == keys()[0].0
    ));

    let original = bincode::serialize(&WorkerMessage::Batch(vec![transaction()])).unwrap();
    let response = WorkerMessage::CommittedBatch(original.clone());
    let bytes = bincode::serialize(&response).unwrap();
    assert!(matches!(
        route_worker_message(&bytes).unwrap(),
        WorkerMessageRoute::CommittedBatch(batch) if batch == original
    ));
}

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        // This test verifies the unbundled worker-to-primary wire shape.
        batch_messages: false,
        ..Parameters::default()
    };

    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    Worker::spawn(name, id, committee.clone(), parameters, store);

    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let expected = bincode::serialize(&WorkerPrimaryMessage::OurBatch(batch_digest(), id)).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected)));

    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        drop(listener(address, None));
    }

    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, transaction()).await;
    network.send(address, transaction()).await;

    assert!(handle.await.is_ok());
}

/// A probe reports occupancy of a bounded channel.
#[tokio::test]
async fn probe_reports_channel_occupancy() {
    let (tx, _rx) = channel::<u64>(4);
    let p = probe("under_test", tx.clone());

    assert_eq!((p.occupancy)(), (0, 4));

    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    tx.send(3).await.unwrap();
    assert_eq!((p.occupancy)(), (3, 4));
}

/// The sampler publishes depth, peak, and capacity for every probe and the store.
#[tokio::test]
async fn sampler_publishes_depth_peak_and_capacity() {
    let path = ".db_test_worker_sampler";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);

    let (tx, _rx) = channel::<u64>(4);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();

    let mut writer = store.clone();
    writer.write(vec![9u8], vec![9u8]).await;

    spawn_queue_sampler(
        vec![probe("under_test", tx.clone())],
        store_probe(store),
        metrics.clone(),
    );

    assert_eq!(
        metrics
            .worker_queue_capacity
            .with_label_values(&["under_test"])
            .get(),
        4
    );
    assert_eq!(
        metrics
            .worker_queue_capacity
            .with_label_values(&["store"])
            .get(),
        100
    );

    tokio::time::sleep(Duration::from_millis(1_250)).await;

    assert_eq!(
        metrics
            .worker_queue_depth
            .with_label_values(&["under_test"])
            .get(),
        2
    );
    assert_eq!(
        metrics
            .worker_queue_peak
            .with_label_values(&["under_test"])
            .get(),
        2
    );
    assert_eq!(
        metrics
            .worker_queue_depth
            .with_label_values(&["store"])
            .get(),
        0
    );
    assert!(
        metrics.store_commands_drained_total.get() >= 1,
        "drain counter never advanced despite a completed write: {}",
        metrics.store_commands_drained_total.get()
    );
    assert!(
        metrics.store_actor_heartbeat_age_ms_peak.get() < 500,
        "store actor peak staleness too high for an idle test: {} ms",
        metrics.store_actor_heartbeat_age_ms_peak.get()
    );
    assert!(
        metrics.store_actor_heartbeat_age_ms.get() < 500,
        "store actor looks stalled in a test with no load: {} ms",
        metrics.store_actor_heartbeat_age_ms.get()
    );
}
