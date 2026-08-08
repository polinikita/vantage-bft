// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        // This test pins the UNBATCHED wire shape end to end (the worker-to-primary
        // listener asserts exact frame bytes); transport batching -- on by default --
        // would wrap the same bytes in bundle framing, which is covered by the
        // network crate's own batch tests, not this one's subject.
        batch_messages: false,
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, id, committee.clone(), parameters, store);

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let expected = bincode::serialize(&WorkerPrimaryMessage::OurBatch(batch_digest(), id)).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected)));

    // Spawn enough workers' listeners to acknowledge our batches.
    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        // Fire-and-forget: `tokio::spawn` inside `listener` already scheduled the task
        // by the time it returns the handle, so dropping the handle doesn't cancel it
        // -- we just don't need to await this one's completion.
        drop(listener(address, /* expected */ None));
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, transaction()).await;
    network.send(address, transaction()).await;

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}

/// A probe reports real occupancy of a bounded channel.
///
/// The whole worker-queue instrument rests on `max_capacity() - capacity()` being the
/// number of buffered messages, with no counter on the send path. If that identity were
/// wrong the gauges would read plausible-but-meaningless numbers, which is worse than
/// having none.
#[tokio::test]
async fn probe_reports_channel_occupancy() {
    let (tx, _rx) = channel::<u64>(4);
    let p = probe("under_test", tx.clone());

    assert_eq!((p.occupancy)(), (0, 4));

    // `_rx` is held but never polled, so these stay buffered.
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    tx.send(3).await.unwrap();
    assert_eq!((p.occupancy)(), (3, 4));
}

/// The sampler publishes depth, peak and capacity for every probe plus the store.
///
/// Guards the publish cadence itself: with the modulo wrong, or the store label
/// forgotten, every gauge here stays absent and a dashboard shows blank panels rather
/// than an error -- the exact failure mode that made the missing worker metrics costly
/// in the first place.
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

    // A little traffic, so the drain counter has something to report.
    let mut writer = store.clone();
    writer.write(vec![9u8], vec![9u8]).await;

    spawn_queue_sampler(
        vec![probe("under_test", tx.clone())],
        store_probe(store),
        metrics.clone(),
    );

    // Capacity is written synchronously, before the task is spawned.
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

    // One publish interval (1s) plus margin. Deliberately a literal rather than the
    // sampler's own constants: those are private to `metrics`, and a test that reads them
    // would pass even if the cadence were changed to something useless.
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
    // The store actor drains and its heartbeat is refreshed by the flush ticker, so
    // this is a liveness assertion: depth back to 0, age well inside one publish.
    assert_eq!(
        metrics
            .worker_queue_depth
            .with_label_values(&["store"])
            .get(),
        0
    );
    // The drain counter must have seen the write above -- this is the reading that
    // separates a saturated actor from one whose permits are held by unpollable senders.
    assert!(
        metrics.store_commands_drained_total.get() >= 1,
        "drain counter never advanced despite a completed write: {}",
        metrics.store_commands_drained_total.get()
    );
    // Peak staleness is published too, and on an idle store it stays small.
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
