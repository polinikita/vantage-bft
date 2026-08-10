// Copyright(C) Facebook, Inc. and its affiliates.
use super::panic;
use super::*;
use crate::{
    common::{
        certificate, committee, committee_with_base_port, header, header_from_cert, headers, keys,
        listener, special_header, special_votes, votes,
    },
    header_waiter::HeaderWaiter,
};
use config::Parameters;
use crypto::Hash;
use serial_test::serial;
use std::{fs, time::Duration};
use tokio::{sync::mpsc::channel, time::sleep};

/// Returns a fresh metrics handle for tests that
/// don't care about wire counters, matching every other `Core::spawn` caller's shape.
fn test_metrics() -> std::sync::Arc<Metrics> {
    Metrics::new(&prometheus::Registry::new()).0
}

#[tokio::test]
#[serial]
async fn process_header_missing_parent() {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let (name, secret) = keys.pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_000);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let (tx_committer, _rx_committer) = channel(1);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(1);
    let (tx_info, _rx_info) = channel(1);
    let (_tx_header_waiter_instances, rx_header_waiter_instances) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    let leader_elector = LeaderElector::new(committee.clone());
    let _timeout_delay = 1000;

    let parameters = Parameters::default();

    // Spawn the core.
    Core::spawn(
        name,
        committee,
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        rx_header_waiter_instances,
        /* rx_proposer */ rx_headers,
        tx_committer,
        /* tx_proposer */ tx_parents,
        rx_request_header_sync,
        tx_info,
        leader_elector,
        parameters.timeout_delay,
        parameters.use_optimistic_tips,
        parameters.use_parallel_proposals,
        parameters.k,
        parameters.use_fast_path,
        parameters.fast_path_timeout,
        parameters.use_ride_share,
        parameters.all_to_all,
        parameters.simulate_asynchrony,
        parameters.asynchrony_start,
        parameters.asynchrony_duration,
        // No latency injection.
        HashMap::new(),
        // No data-plane withholding.
        None,
        // No time-windowed withholding.
        None,
        // Keep metrics last in the constructor argument list.
        test_metrics(),
        BatchConfig::default(),
        // KNOB 2 (measurement ablation): appended last, same convention as
        // `Core::spawn`'s own doc comment for this param.
        parameters.retry_backoff_max_ms,
    );

    let header_one = header();
    let cert_one = certificate(&header_one);
    let header_two: Header = Header {
        author: header_one.author,
        height: header_one.height + 1,
        payload: header_one.payload,
        parent_cert: cert_one,
        id: header_one.id,
        signature: header_one.signature,
        sid: None,
        consensus_messages: HashMap::new(),
        num_active_instances: 0,
        special: false,
    };
    let id = header_two.digest().clone();

    // Send a header to the core.
    tx_primary_messages
        .send(PrimaryMessage::Header(header_two, false))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
#[serial]
async fn process_header_invalid_height() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let (tx_committer, _rx_committer) = channel(1);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(1);
    let (tx_info, _rx_info) = channel(1);
    let (_tx_header_waiter_instances, rx_header_waiter_instances) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_parent";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    let leader_elector = LeaderElector::new(committee().clone());
    let _timeout_delay = 1000;

    let parameters = Parameters::default();

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        rx_header_waiter_instances,
        /* rx_proposer */ rx_headers,
        tx_committer,
        /* tx_proposer */ tx_parents,
        rx_request_header_sync,
        tx_info,
        leader_elector,
        parameters.timeout_delay,
        parameters.use_optimistic_tips,
        parameters.use_parallel_proposals,
        parameters.k,
        parameters.use_fast_path,
        parameters.fast_path_timeout,
        parameters.use_ride_share,
        parameters.all_to_all,
        parameters.simulate_asynchrony,
        parameters.asynchrony_start,
        parameters.asynchrony_duration,
        // Optional latency injection; empty keeps the default behavior.
        // (zero injected delay), matching every OTHER existing test's expectations.
        HashMap::new(),
        // No data-plane withholding.
        None,
        // No time-windowed withholding.
        None,
        // Keep metrics last in the constructor argument list.
        test_metrics(),
        BatchConfig::default(),
        // KNOB 2 (measurement ablation): appended last, same convention as
        // `Core::spawn`'s own doc comment for this param.
        parameters.retry_backoff_max_ms,
    );

    // Send a header to the core.
    let header = Header {
        parent_cert: Certificate::genesis_cert(&committee()), //[Digest::default()].iter().cloned().collect(),
        height: 2,
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header, false))
        .await
        .unwrap();

    // Sleep to ensure header is processed
    sleep(Duration::from_millis(1000)).await;

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
#[serial]
async fn process_header_missing_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let (tx_committer, _rx_committer) = channel(1);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(1);
    let (tx_info, _rx_info) = channel(1);
    let (_tx_header_waiter_instances, rx_header_waiter_instances) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    let leader_elector = LeaderElector::new(committee().clone());
    let _timeout_delay = 1000;

    let parameters = Parameters::default();

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        rx_header_waiter_instances,
        /* rx_proposer */ rx_headers,
        tx_committer,
        /* tx_proposer */ tx_parents,
        rx_request_header_sync,
        tx_info,
        leader_elector,
        parameters.timeout_delay,
        parameters.use_optimistic_tips,
        parameters.use_parallel_proposals,
        parameters.k,
        parameters.use_fast_path,
        parameters.fast_path_timeout,
        parameters.use_ride_share,
        parameters.all_to_all,
        parameters.simulate_asynchrony,
        parameters.asynchrony_start,
        parameters.asynchrony_duration,
        // Optional latency injection; empty keeps the default behavior.
        // (zero injected delay), matching every OTHER existing test's expectations.
        HashMap::new(),
        // No data-plane withholding.
        None,
        // No time-windowed withholding.
        None,
        // Keep metrics last in the constructor argument list.
        test_metrics(),
        BatchConfig::default(),
        // KNOB 2 (measurement ablation): appended last, same convention as
        // `Core::spawn`'s own doc comment for this param.
        parameters.retry_backoff_max_ms,
    );

    // Send a header to the core.
    let header = Header {
        payload: [(Digest::default(), 0)].iter().cloned().collect(),
        ..header()
    };
    let id = header.id.clone();
    tx_primary_messages
        .send(PrimaryMessage::Header(header, false))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(store.read(id.to_vec()).await.unwrap().is_none());
}

#[tokio::test]
#[serial]
async fn process_votes() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_100);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (tx_headers, rx_headers) = channel(1);
    let (tx_parents, mut rx_parents) = channel(1);

    let (tx_committer, _rx_committer) = channel(1);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(1);
    let (tx_info, _rx_info) = channel(1);
    let (_tx_header_waiter_instances, rx_header_waiter_instances) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_vote";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    let leader_elector = LeaderElector::new(committee.clone());
    let _timeout_delay = 1000;

    let parameters = Parameters::default();

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        rx_header_waiter_instances,
        /* rx_proposer */ rx_headers,
        tx_committer,
        /* tx_proposer */ tx_parents,
        rx_request_header_sync,
        tx_info,
        leader_elector,
        parameters.timeout_delay,
        parameters.use_optimistic_tips,
        parameters.use_parallel_proposals,
        parameters.k,
        parameters.use_fast_path,
        parameters.fast_path_timeout,
        parameters.use_ride_share,
        parameters.all_to_all,
        parameters.simulate_asynchrony,
        parameters.asynchrony_start,
        parameters.asynchrony_duration,
        // Optional latency injection; empty keeps the default behavior.
        // (zero injected delay), matching every OTHER existing test's expectations.
        HashMap::new(),
        // No data-plane withholding.
        None,
        // No time-windowed withholding.
        None,
        // Keep metrics last in the constructor argument list.
        test_metrics(),
        BatchConfig::default(),
        // KNOB 2 (measurement ablation): appended last, same convention as
        // `Core::spawn`'s own doc comment for this param.
        parameters.retry_backoff_max_ms,
    );

    // Receive geneis parent cert from the proposer
    rx_parents.recv().await.unwrap();

    let header = header();
    // Make the certificate we expect to receive.
    let expected = certificate(&header);

    //Note: core uses Header::genesis instead of Header::default now
    tx_headers.send(header.clone()).await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Send a votes to the core.
    for vote in votes(&header) {
        tx_primary_messages
            .send(PrimaryMessage::Vote(vote))
            .await
            .unwrap();
    }

    let received_cert = rx_parents.recv().await.unwrap();
    assert_eq!(received_cert.height, expected.height);
    assert_eq!(received_cert.author, expected.author);
    assert_eq!(received_cert.header_digest, expected.header_digest);
}

#[tokio::test]
#[serial]
async fn process_certificates() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (tx_primary_messages, rx_primary_messages) = channel(3);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let (tx_committer, _rx_committer) = channel(3);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(1);
    let (tx_info, _rx_info) = channel(1);
    let (_tx_header_waiter_instances, rx_header_waiter_instances) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_certificates";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee(),
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    let leader_elector = LeaderElector::new(committee().clone());
    let _timeout_delay = 1000;

    let parameters = Parameters::default();

    // Spawn the core.
    Core::spawn(
        name,
        committee(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        rx_header_waiter_instances,
        /* rx_proposer */ rx_headers,
        tx_committer,
        /* tx_proposer */ tx_parents,
        rx_request_header_sync,
        tx_info,
        leader_elector,
        parameters.timeout_delay,
        parameters.use_optimistic_tips,
        parameters.use_parallel_proposals,
        parameters.k,
        parameters.use_fast_path,
        parameters.fast_path_timeout,
        parameters.use_ride_share,
        parameters.all_to_all,
        parameters.simulate_asynchrony,
        parameters.asynchrony_start,
        parameters.asynchrony_duration,
        // Optional latency injection; empty keeps the default behavior.
        // (zero injected delay), matching every OTHER existing test's expectations.
        HashMap::new(),
        // No data-plane withholding.
        None,
        // No time-windowed withholding.
        None,
        // Keep metrics last in the constructor argument list.
        test_metrics(),
        BatchConfig::default(),
        // KNOB 2 (measurement ablation): appended last, same convention as
        // `Core::spawn`'s own doc comment for this param.
        parameters.retry_backoff_max_ms,
    );

    // Send enough certificates to the core.
    let certificates: Vec<Certificate> = headers().iter().map(certificate).collect();

    // Send enough headers to the core.
    let headers_from_certs: Vec<Header> = certificates.iter().map(header_from_cert).collect();

    for x in headers().iter() {
        tx_primary_messages
            .send(PrimaryMessage::Header(x.clone(), false))
            .await
            .unwrap();
    }

    for x in headers_from_certs {
        tx_primary_messages
            .send(PrimaryMessage::Header(x, false))
            .await
            .unwrap();
    }

    // Ensure the certificates are stored.
    for x in &certificates {
        let stored = store.read(x.digest().to_vec()).await.unwrap();
        let serialized = bincode::serialize(x).unwrap();
        assert_eq!(stored, Some(serialized));
    }
}

#[tokio::test]
#[serial]
async fn local_timeout_view() {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let (name, secret) = keys.pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let committee = committee_with_base_port(13_000);

    let (tx_sync_headers, _rx_sync_headers) = channel(1);
    let (tx_sync_certificates, _rx_sync_certificates) = channel(1);
    let (_tx_primary_messages, rx_primary_messages) = channel(1);
    let (_tx_headers_loopback, rx_headers_loopback) = channel(1);
    let (_tx_headers, rx_headers) = channel(1);
    let (tx_parents, _rx_parents) = channel(1);

    let (tx_committer, _rx_committer) = channel(1);
    let (_tx_request_header_sync, rx_request_header_sync) = channel(1);
    let (tx_info, _rx_info) = channel(1);
    let (_tx_header_waiter_instances, rx_header_waiter_instances) = channel(1);

    // Create a new test store.
    let path = ".db_test_process_header";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a listener to receive the vote.
    let address = committee
        .primary(&header().author)
        .unwrap()
        .primary_to_primary;
    let _handle = listener(address);

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name,
        &committee,
        store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        /* tx_certificate_waiter */ tx_sync_certificates,
    );

    let leader_elector = LeaderElector::new(committee.clone());
    let _timeout_delay = 1000;

    let parameters = Parameters::default();

    // Spawn the core.
    Core::spawn(
        name,
        committee.clone(),
        store.clone(),
        synchronizer,
        signature_service,
        /* consensus_round */ Arc::new(AtomicU64::new(0)),
        /* gc_depth */ 50,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        rx_header_waiter_instances,
        /* rx_proposer */ rx_headers,
        tx_committer,
        /* tx_proposer */ tx_parents,
        rx_request_header_sync,
        tx_info,
        leader_elector,
        parameters.timeout_delay,
        parameters.use_optimistic_tips,
        parameters.use_parallel_proposals,
        parameters.k,
        parameters.use_fast_path,
        parameters.fast_path_timeout,
        parameters.use_ride_share,
        parameters.all_to_all,
        parameters.simulate_asynchrony,
        parameters.asynchrony_start,
        parameters.asynchrony_duration,
        // Optional latency injection; empty keeps the default behavior.
        // (zero injected delay), matching every OTHER existing test's expectations.
        HashMap::new(),
        // No data-plane withholding.
        None,
        // No time-windowed withholding.
        None,
        // Keep metrics last in the constructor argument list.
        test_metrics(),
        BatchConfig::default(),
        // KNOB 2 (measurement ablation): appended last, same convention as
        // `Core::spawn`'s own doc comment for this param.
        parameters.retry_backoff_max_ms,
    );

}
