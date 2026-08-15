// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use tokio::sync::mpsc::channel;

#[test]
fn leader_relay_rotates_one_correct_holder_without_sending_to_byzantine_peers() {
    let address = |port| {
        (
            PublicKey::default(),
            format!("127.0.0.1:{port}").parse().unwrap(),
        )
    };
    let correct: Vec<_> = (1..=14).map(|port| address(20_000 + port)).collect();

    for epoch in 0..correct.len() as u64 {
        for within_epoch in 0..5 {
            let sequence = epoch * 5 + within_epoch;
            let recipients = leader_relay_batch_addresses(&correct, sequence, 5, 1, 1);
            assert_eq!(recipients.len() + 1, 2, "include the local author");
            assert_eq!(recipients.last(), Some(&correct[epoch as usize].1));
        }
    }
    assert_eq!(
        leader_relay_batch_addresses(&correct, correct.len() as u64 * 5, 5, 1, 1).last(),
        Some(&correct[0].1),
    );
}

#[test]
fn leader_relay_f_holders_stay_below_poa_and_cover_every_correct_leader() {
    let address = |port| {
        (
            PublicKey::default(),
            format!("127.0.0.1:{port}").parse().unwrap(),
        )
    };
    let correct: Vec<_> = (1..=14).map(|port| address(20_000 + port)).collect();
    let mut covered = std::collections::HashSet::new();

    // At n=20, the local Byzantine author plus five correct recipients give
    // exactly f=6 direct holders, one below the f+1=7 PoA threshold. Three
    // (f-1)-wide epochs cover all 14 correct consensus leaders.
    for epoch in 0..3 {
        let recipients = leader_relay_batch_addresses(&correct, epoch * 5, 5, 5, 5);
        assert_eq!(recipients.len() + 1, 6);
        covered.extend(recipients);
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
