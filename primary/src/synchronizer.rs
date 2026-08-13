// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{ConsensusMessage, Header, Proposal};
use crate::Height;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::debug;
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::Sender;

/// Checks header dependencies and requests missing data.
#[derive(Clone)]
pub struct Synchronizer {
    /// The public key of this primary.
    name: PublicKey,
    /// The persistent storage.
    store: Store,
    /// Send commands to the `HeaderWaiter`.
    tx_header_waiter: Sender<WaiterMessage>,
    /// Genesis headers by authority.
    genesis_headers: HashMap<PublicKey, Header>,
}

impl Synchronizer {
    pub fn new(
        name: PublicKey,
        committee: &Committee,
        store: Store,
        tx_header_waiter: Sender<WaiterMessage>,
    ) -> Self {
        Self {
            name,
            store,
            tx_header_waiter,
            genesis_headers: Header::genesis_headers(committee),
        }
    }

    /// Returns whether payload synchronization was scheduled.
    pub async fn missing_payload(&mut self, header: &Header, force_sync: bool) -> DagResult<bool> {
        // We don't store the payload of our own workers.
        if header.author == self.name {
            return Ok(false);
        }

        let mut missing = HashMap::new();
        for (digest, worker_id) in header.payload.iter() {
            // Bind each digest to its declared worker.
            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
            if self.store.read(key).await?.is_none() {
                debug!(
                    "Missing Digest: {}, Author: {}. Name: {}. Round {}",
                    digest, header.author, self.name, header.height
                );
                missing.insert(digest.clone(), *worker_id);
            }
        }

        if missing.is_empty() {
            return Ok(false);
        }

        self.tx_header_waiter
            .send(WaiterMessage::SyncBatches(
                missing,
                header.clone(),
                force_sync,
            ))
            .await
            .expect("Failed to send sync batch request");
        Ok(true)
    }

    pub async fn fetch_header(&mut self, header_digest: Digest) -> DagResult<()> {
        self.tx_header_waiter
            .send(WaiterMessage::SyncHeader(header_digest))
            .await
            .expect("Failed to send sync special parent request");
        Ok(())
    }

    /// Returns all referenced proposals, or schedules synchronization if any are missing.
    pub async fn get_proposals(
        &mut self,
        consensus_message: &ConsensusMessage,
        delivered_header: &Header,
    ) -> DagResult<Vec<Header>> {
        let mut missing = Vec::new();
        let mut proposals_vector = Vec::new();

        match consensus_message {
            ConsensusMessage::Prepare {
                slot: _,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals,
            } => {
                for (pk, proposal) in proposals {
                    if proposal.header_digest == self.genesis_headers.get(pk).unwrap().digest() {
                        proposals_vector.push(self.genesis_headers.get(pk).unwrap().clone());
                        continue;
                    }

                    if proposal.header_digest == delivered_header.digest() {
                        proposals_vector.push(delivered_header.clone());
                        continue;
                    }

                    match self.store.read(proposal.header_digest.to_vec()).await? {
                        Some(header) => {
                            proposals_vector.push(bincode::deserialize(&header)?);
                        }
                        None => missing.push((*pk, proposal.clone())),
                    }
                }
            }
            ConsensusMessage::Confirm {
                slot: _,
                view: _,
                qc: _,
                proposals,
            } => {
                for (pk, proposal) in proposals {
                    if proposal.header_digest == self.genesis_headers.get(pk).unwrap().digest() {
                        proposals_vector.push(self.genesis_headers.get(pk).unwrap().clone());
                        continue;
                    }

                    match self.store.read(proposal.header_digest.to_vec()).await? {
                        Some(header) => proposals_vector.push(bincode::deserialize(&header)?),
                        None => missing.push((*pk, proposal.clone())),
                    }
                }
            }
            ConsensusMessage::Commit {
                slot: _,
                view: _,
                qc: _,
                proposals,
            } => {
                for (pk, proposal) in proposals {
                    if proposal.height == 0 {
                        continue;
                    }
                    if proposal.header_digest == self.genesis_headers.get(pk).unwrap().digest() {
                        proposals_vector.push(self.genesis_headers.get(pk).unwrap().clone());
                        continue;
                    }

                    match self.store.read(proposal.header_digest.to_vec()).await? {
                        Some(header) => proposals_vector.push(bincode::deserialize(&header)?),
                        None => missing.push((*pk, proposal.clone())),
                    }
                }
            }
        }

        if missing.is_empty() {
            debug!("have all proposals and their ancestors");
            return Ok(proposals_vector);
        }

        debug!("Triggering sync for proposals");
        debug!("missing proposals are {:?}", missing);
        self.tx_header_waiter
            .send(WaiterMessage::SyncProposals(
                missing,
                consensus_message.clone(),
                delivered_header.clone(),
            ))
            .await
            .expect("Failed to send sync parents request");
        Ok(Vec::new())
    }
    pub async fn get_all_headers_for_proposal(
        &mut self,
        proposal: Proposal,
        stop_height: Height,
    ) -> DagResult<Vec<Header>> {
        // Collect ancestors down to the committed height.
        let mut ancestors: Vec<Header> = Vec::new();

        debug!("proposal height is {:?}", proposal.height);
        let mut header: Header = self
            .get_header(proposal.header_digest)
            .await
            .expect("already synced should have header")
            .unwrap();

        let mut current_height = proposal.height;
        while current_height > stop_height {
            debug!(
                "current height is {:?}, stop height is {:?}",
                current_height, stop_height
            );
            ancestors.push(header.clone());
            header = self
                .get_parent_header(&header)
                .await?
                .expect("should have parent by now");
            current_height = header.height();
        }

        Ok(ancestors)
    }

    pub async fn get_parent_header(&mut self, header: &Header) -> DagResult<Option<Header>> {
        if header.parent_cert.header_digest
            == self.genesis_headers.get(&header.author).unwrap().digest()
        {
            return Ok(Some(
                self.genesis_headers.get(&header.author).unwrap().clone(),
            ));
        }

        let parent = header.parent_cert.header_digest.clone();
        match self.store.read(parent.to_vec()).await? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => {
                self.tx_header_waiter
                    .send(WaiterMessage::SyncParent(parent, header.clone()))
                    .await
                    .expect("Failed to send sync parent request");
                Ok(None)
            }
        }
    }

    pub async fn get_header(&mut self, header_digest: Digest) -> DagResult<Option<Header>> {
        match self.store.read(header_digest.to_vec()).await? {
            Some(bytes) => {
                debug!("get_header: in the store");
                Ok(Some(bincode::deserialize(&bytes)?))
            }
            None => {
                debug!("get_header not in the store");
                Ok(None)
            }
        }
    }
}
