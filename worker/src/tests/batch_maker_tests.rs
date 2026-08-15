// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use tokio::sync::mpsc::channel;

#[test]
fn leader_relay_keeps_the_byzantine_cohort_and_rotates_one_correct_holder() {
    let address = |port| {
        (
            PublicKey::default(),
            format!("127.0.0.1:{port}").parse().unwrap(),
        )
    };
    // The author is local, so the five remote cohort members below make all
    // six Byzantine validators direct holders at n=20.
    let cohort: Vec<_> = (1..=5).map(|port| address(10_000 + port)).collect();
    let correct: Vec<_> = (1..=14).map(|port| address(20_000 + port)).collect();

    for epoch in 0..correct.len() as u64 {
        for within_epoch in 0..5 {
            let sequence = epoch * 5 + within_epoch;
            let recipients = leader_relay_batch_addresses(&cohort, &correct, sequence, 5, 1, 1);
            assert_eq!(recipients.len() + 1, 7, "include the local author");
            assert_eq!(
                &recipients[..cohort.len()],
                &cohort.iter().map(|x| x.1).collect::<Vec<_>>()
            );
            assert_eq!(recipients.last(), Some(&correct[epoch as usize].1));
        }
    }
    assert_eq!(
        leader_relay_batch_addresses(&cohort, &correct, correct.len() as u64 * 5, 5, 1, 1).last(),
        Some(&correct[0].1),
    );
}

#[test]
fn leader_relay_two_f_holders_cover_every_correct_leader_in_rotating_groups() {
    let address = |port| {
        (
            PublicKey::default(),
            format!("127.0.0.1:{port}").parse().unwrap(),
        )
    };
    let cohort: Vec<_> = (1..=5).map(|port| address(10_000 + port)).collect();
    let correct: Vec<_> = (1..=14).map(|port| address(20_000 + port)).collect();
    let mut covered = std::collections::HashSet::new();

    // At n=20, every Byzantine batch has six Byzantine and six correct direct
    // holders: 2f, one below quorum. Three f-wide epochs cover all 14 correct
    // consensus leaders.
    for epoch in 0..3 {
        let recipients = leader_relay_batch_addresses(&cohort, &correct, epoch * 5, 5, 6, 6);
        assert_eq!(recipients.len() + 1, 12);
        covered.extend(recipients[cohort.len()..].iter().copied());
    }
    assert_eq!(covered.len(), correct.len());
}

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        200,
        1_000_000,
        rx_transaction,
        tx_message,
        dummy_addresses,
        None,
        None,
        None,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
    );

    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    let expected_batch = vec![transaction(), transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        200,
        50,
        rx_transaction,
        tx_message,
        dummy_addresses,
        None,
        None,
        None,
        std::collections::HashMap::new(),
        Metrics::new(&prometheus::Registry::new()).0,
        BatchConfig::default(),
    );

    tx_transaction.send(transaction()).await.unwrap();

    let expected_batch = vec![transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[cfg(feature = "pipeline-tracing")]
#[tokio::test]
async fn pipeline_metric_observes_every_transaction() {
    let (tx_transaction, rx_transaction) = channel(2);
    let (tx_message, mut rx_message) = channel(1);
    let registry = prometheus::Registry::new();
    let (metrics, reporter) = Metrics::new(&registry);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        200,
        1_000_000,
        rx_transaction,
        tx_message,
        dummy_addresses,
        None,
        None,
        None,
        std::collections::HashMap::new(),
        metrics,
        BatchConfig::default(),
    );

    let submitted_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for marker in [0, 1] {
        let mut transaction = vec![marker; 100];
        transaction[9..17].copy_from_slice(&submitted_millis.to_le_bytes());
        tx_transaction.send(Bytes::from(transaction)).await.unwrap();
    }
    rx_message.recv().await.unwrap();
    reporter.force_report();

    let snapshot = metrics::read_duration_snapshot(&registry, "transaction_to_batch_seal_latency")
        .expect("pipeline metric");
    assert_eq!(snapshot.count, 2);
}
