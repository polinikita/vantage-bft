// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::{
    common::{committee, keys},
    messages::{ConsensusMessage, Header, Vote},
};
use serial_test::serial;
use tokio::sync::mpsc::channel;

#[tokio::test]
#[serial]
async fn propose_empty() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* max_payload_digests */ None,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.height, 1);
    assert!(header.payload.is_empty());
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn propose_payload() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* max_payload_digests */ None,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    sleep(Duration::from_millis(500)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn propose_normal() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* max_payload_digests */ None,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    sleep(Duration::from_millis(500)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());

    let votes: Vec<_> = keys()
        .iter()
        .take(1)
        .map(|(public_key, secret_key)| {
            Vote::new_from_key(header.clone(), Vec::new(), *public_key, secret_key)
        })
        .map(|x| (x.author, x.signature))
        .collect();

    let certificate = Certificate {
        author: header.origin(),
        header_digest: header.digest(),
        height: header.height,
        votes,
    };
    tx_parents.send(certificate).await.unwrap();

    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header1 = rx_headers.recv().await.unwrap();

    assert_eq!(header1.height, 2);
    assert_eq!(header1.payload.get(&digest), Some(&worker_id));
    assert!(header1.verify(&committee()).is_ok());
    assert_eq!(header1.parent_cert.header_digest, header.digest());
}

#[tokio::test]
#[serial]
async fn payload_cap_keeps_extra_digests_for_successive_lane_blocks() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);
    let (tx_parents, rx_parents) = channel(3);
    let (tx_our_digests, rx_our_digests) = channel(3);
    let (tx_headers, mut rx_headers) = channel(3);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 1,
        /* max_header_delay */ 1_000_000,
        /* max_payload_digests */ Some(1),
        rx_parents,
        rx_our_digests,
        rx_ticket,
        tx_headers,
    );

    tx_parents
        .send(Certificate::genesis_for(name, &committee()))
        .await
        .unwrap();
    let digests = [Digest([41; 32]), Digest([42; 32]), Digest([43; 32])];
    tx_our_digests.send((digests[0].clone(), 0)).await.unwrap();
    let first = rx_headers.recv().await.unwrap();
    assert_eq!(first.payload.len(), 1);
    assert!(first.payload.contains_key(&digests[0]));

    tx_our_digests.send((digests[1].clone(), 0)).await.unwrap();
    tx_our_digests.send((digests[2].clone(), 0)).await.unwrap();
    tx_parents
        .send(Certificate {
            author: name,
            header_digest: first.digest(),
            height: first.height,
            votes: Vec::new(),
        })
        .await
        .unwrap();
    let second = rx_headers.recv().await.unwrap();
    assert_eq!(second.payload.len(), 1);
    assert!(second.payload.contains_key(&digests[1]));

    tx_parents
        .send(Certificate {
            author: name,
            header_digest: second.digest(),
            height: second.height,
            votes: Vec::new(),
        })
        .await
        .unwrap();
    let third = rx_headers.recv().await.unwrap();
    assert_eq!(third.payload.len(), 1);
    assert!(third.payload.contains_key(&digests[2]));
}

#[tokio::test]
#[serial]
async fn propose_special_ticket_first() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* max_payload_digests */ None,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    let _gen_header = Header::genesis(&committee());

    sleep(Duration::from_secs(1)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn propose_confirm_message() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (_tx_ticket, rx_ticket) = channel(1);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* max_payload_digests */ None,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    let genesis_cert = Certificate::genesis_certs(&committee())
        .get(&name)
        .unwrap()
        .clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    let _gen_header = Header::genesis(&committee());

    sleep(Duration::from_secs(1)).await;

    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}

#[tokio::test]
#[serial]
async fn duplicate_consensus_info_is_counted_once() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let (tx_ticket, rx_ticket) = channel(2);

    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000,
        /* max_payload_digests */ None,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

    tx_parents
        .send(Certificate::genesis_for(name, &committee()))
        .await
        .unwrap();
    let prepare = ConsensusMessage::Prepare {
        slot: 1,
        view: 1,
        tc: None,
        qc_ticket: None,
        proposals: Header::genesis_proposals(&committee()),
    };
    tx_ticket.send(prepare.clone()).await.unwrap();
    tx_ticket.send(prepare).await.unwrap();
    sleep(Duration::from_millis(20)).await;

    let payload = Digest(name.0);
    tx_our_digests.send((payload, 0)).await.unwrap();
    let header = rx_headers.recv().await.unwrap();

    assert_eq!(header.consensus_messages.len(), 1);
    assert_eq!(header.num_active_instances, 1);
    assert!(header.verify(&committee()).is_ok());
}
