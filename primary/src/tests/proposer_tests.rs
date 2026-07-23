use core::time;
use std::collections::{BTreeMap, HashMap};

// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::{common::{committee, keys}, messages::Vote};
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
    let(_tx_ticket, rx_ticket) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 1_000,
        /* max_header_delay */ 20,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket, 
        /* tx_core */ tx_headers,
    );


    let genesis_cert = Certificate::genesis_certs(&committee()).get(&name).unwrap().clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    //assert_eq!(header.prepare_info_list.is_empty(), true);
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
    let(_tx_ticket, rx_ticket) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );


    let genesis_cert = Certificate::genesis_certs(&committee()).get(&name).unwrap().clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    sleep(Duration::from_millis(500)).await;

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    //assert_eq!(header.prepare_info_list.is_empty(), true);
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
    let(_tx_ticket, rx_ticket) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );


    let genesis_cert = Certificate::genesis_certs(&committee()).get(&name).unwrap().clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    sleep(Duration::from_millis(500)).await;

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();

    //assert_eq!(header.prepare_info_list.is_empty(), true);
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

    let certificate = Certificate { author: header.origin(), header_digest: header.digest(), height: header.height, votes };
    tx_parents.send(certificate).await.unwrap();

    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    let header1 = rx_headers.recv().await.unwrap();

    //assert_eq!(header1.prepare_info_list.is_empty(), true);
    assert_eq!(header1.height, 2);
    assert_eq!(header1.payload.get(&digest), Some(&worker_id));
    assert!(header1.verify(&committee()).is_ok());
    assert_eq!(header1.parent_cert.header_digest, header.digest());
}

// Special header tests
#[tokio::test]
#[serial]
async fn propose_special_ticket_first() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let(tx_ticket, rx_ticket) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );


    let genesis_cert = Certificate::genesis_certs(&committee()).get(&name).unwrap().clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");

    //Send ticket to form a special header
    let gen_header = Header::genesis(&committee());

    //let ticket: Ticket = Ticket::new(Some(gen_header), None, 1, HashMap::new()).await;
    /*let consensus_info: InstanceInfo = InstanceInfo { slot: 2, view: 1 };
    let prepare_info: PrepareInfo = PrepareInfo { consensus_info, ticket, proposals: HashMap::new() };*/

    /*tx_ticket
        .send(prepare_info)
        .await
        .unwrap();*/

    sleep(Duration::from_secs(1)).await; //just to guarantee ticket arrives before digest (else normal block can be triggered.)

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer makes a correct special header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
   
    /*assert_eq!(header.prepare_info_list.is_empty(), false);
    assert_eq!(header.prepare_info_list.get(0).unwrap().consensus_info.slot, 2);
    assert_eq!(header.prepare_info_list.get(0).unwrap().consensus_info.view, 1);*/
    
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
    let(tx_ticket, rx_ticket) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );


    let genesis_cert = Certificate::genesis_certs(&committee()).get(&name).unwrap().clone();
    tx_parents
        .send(genesis_cert)
        .await
        .expect("failed to send cert to proposer");


    //Send ticket to form a special header
    let gen_header = Header::genesis(&committee());

    //let ticket: Ticket = Ticket::new(Some(gen_header), None, 1, HashMap::new()).await;
    /*let consensus_info: InstanceInfo = InstanceInfo { slot: 2, view: 1 };
    let prepare_info: PrepareInfo = PrepareInfo { consensus_info, ticket, proposals: HashMap::new() };*/

    /*tx_ticket
        .send(prepare_info)
        .await
        .unwrap();*/

    sleep(Duration::from_secs(1)).await; //just to guarantee ticket arrives before digest (else normal block can be triggered.)

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    // Ensure the proposer makes a correct special header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
   
    /*assert_eq!(header.prepare_info_list.is_empty(), false);
    assert_eq!(header.prepare_info_list.get(0).unwrap().consensus_info.slot, 2);
    assert_eq!(header.prepare_info_list.get(0).unwrap().consensus_info.view, 1);*/
    
    assert_eq!(header.height, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());
}





/*#[tokio::test]
async fn propose_special_ticket_after_requiring_parents() {
    let (name, secret) = keys().pop().unwrap();
    let signature_service = SignatureService::new(secret);

    let (tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);
    let(tx_ticket, rx_ticket) = channel(1);

    // Spawn the proposer.
    Proposer::spawn(
        name,
        &committee(),
        signature_service,
        /* header_size */ 32,
        /* max_header_delay */ 1_000_000, // Ensure it is not triggered.
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_ticket,
        /* tx_core */ tx_headers,
    );

     // Send enough digests for the header payload.
     let digest = Digest(name.0);
     let worker_id = 0;
     tx_our_digests
         .send((digest.clone(), worker_id))
         .await
         .unwrap();

     // Ensure the proposer makes a correct header from the provided payload.
     let header = rx_headers.recv().await.unwrap();
     assert_eq!(header.is_special, false);
     assert_eq!(header.height, 1);
     assert_eq!(header.payload.get(&digest), Some(&worker_id));
     assert!(header.verify(&committee()).is_ok());
     let last_header_id = header.id;
     let last_header_round = header.height;

    //Send ticket to form a special header
  

     //Send ticket to form a special header
     let gen_header = Header::genesis(&committee());
     let gen_qc = QC::genesis(&committee());
 
     let view = 0; //gen_header.view = 0
     let round = 5; //gen_header.round = 0 ==> this is not a valid round, but we just use it to test
     let ticket: Ticket = Ticket::new(gen_header.digest(), view, round, gen_qc, None).await;

    tx_ticket
        .send(ticket)
        .await
        .unwrap();

    //Note: No sleep needed. Parents are unavailable ==> will form special edge.

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();


    // Ensure the proposer makes a correct special header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
   
    assert_eq!(header.is_special, true);
    assert_eq!(header.view, 1);
    assert_eq!(header.prev_view_round, 5);
    //TODO: special_parent round
    assert_eq!(header.parent_cert_digest.len(), 0);
    assert_eq!(header.special_parent.is_some(), true);
    assert_eq!(header.special_parent_height, last_header_round);
    let special_parent: &Digest = header.special_parent.as_ref().unwrap(); 
    assert_eq!(*special_parent, last_header_id);

    assert_eq!(header.height, 6);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());

    //Send another ticket. But this time it won't be able to form a special edge. Must wait for parents.

    //Send ticket to form a special header
    let gen_header = Header::genesis(&committee());
    let gen_qc = QC::genesis(&committee());

    let view = 1; //gen_header.view = 0 ==> this is not a valid view for the ticket, but we use it to test
    let last_round = 6; //gen_header.round = 0 ==> this is not a valid round for the ticket, but we just use it to test
    let ticket: Ticket = Ticket::new(gen_header.digest(), view, last_round, gen_qc, None).await;
    
    tx_ticket
        .send(ticket)
        .await
        .unwrap();

    

    // Send enough digests for the header payload.
    let digest = Digest(name.0);
    let worker_id = 0;
    tx_our_digests
        .send((digest.clone(), worker_id))
        .await
        .unwrap();

    sleep(Duration::from_millis(1000)).await;  //sleep to guarantee ticket arrives before parents --> otherwise normal block can form.
    //Generate dummy parents (creating 4 here)
    let parents = Certificate::genesis(&committee())
        .iter()
        .map(|x| x.digest())
        .collect();
    let last_round = 6;
    tx_parents
        .send((parents, last_round))
        .await
        .unwrap();

     // Ensure the proposer makes a correct special header from the provided payload.
     let header = rx_headers.recv().await.unwrap();


    assert_eq!(header.is_special, true);
    assert_eq!(header.view, 2);
    assert_eq!(header.prev_view_round, 6);
    //TODO: special_parent round
    assert_eq!(header.parent_cert_digest.len(), 4);
    // assert_eq!(header.special_parent_round, last_header_round);
    // let special_parent: Digest = header.parents.iter().cloned().next().unwrap();
    // assert_eq!(special_parent, last_header_id);


    assert_eq!(header.height, 7);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee()).is_ok());

}*/

