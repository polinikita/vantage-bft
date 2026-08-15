// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::DagResult;
use crate::header_waiter::WaiterMessage;
use crate::messages::{
    proposals_digest, Certificate, ConsensusMessage, Header, Proposal, ProposalKind,
};
use crate::primary::Slot;
use crate::Height;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use store::Store;
use tokio::sync::mpsc::Sender;

type CutCoordinate = (Slot, Digest);
type CutSourceCache = Arc<RwLock<HashMap<CutCoordinate, Vec<PublicKey>>>>;

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
    /// Committee used to validate lane-tip proofs.
    committee: Committee,
    /// Materialized height per lane, shared with the committer.
    last_executed_heights: Arc<RwLock<HashMap<PublicKey, Height>>>,
    /// Availability witnesses for cuts implicitly certified by a TC. Later
    /// phases omit the TC, so retain the witnesses by slot and canonical cut
    /// digest until that slot is fully materialized.
    implicit_cut_sources: CutSourceCache,
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
            committee: committee.clone(),
            last_executed_heights: Arc::new(RwLock::new(
                committee
                    .authorities
                    .keys()
                    .map(|author| (*author, 0))
                    .collect(),
            )),
            implicit_cut_sources: Arc::new(RwLock::new(HashMap::new())),
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

    fn executed_height(&self, lane: &PublicKey) -> Height {
        *self
            .last_executed_heights
            .read()
            .expect("executed-height lock poisoned")
            .get(lane)
            .unwrap_or(&0)
    }

    pub fn mark_executed(&self, lane: PublicKey, height: Height) {
        let mut heights = self
            .last_executed_heights
            .write()
            .expect("executed-height lock poisoned");
        let current = heights.entry(lane).or_default();
        *current = (*current).max(height);
    }

    /// Register a verified parent PoA before retrying a locally known parent.
    ///
    /// The PoA voters, rather than the possibly Byzantine lane author, are the
    /// protocol-justified repair sources for the certified parent and its
    /// unexecuted suffix. Sending this command before `process_header` retries
    /// the parent also upgrades its pending payload request in FIFO order.
    pub async fn register_parent_poa_sources(&mut self, parent: &Certificate) {
        let stop_height = self.executed_height(&parent.author);
        self.tx_header_waiter
            .send(WaiterMessage::SyncCertified(vec![(
                parent.author,
                Proposal::certified(parent.clone()),
                stop_height,
            )]))
            .await
            .expect("Failed to register certified-parent repair sources");
    }

    async fn read_proposal_header(
        &mut self,
        lane: &PublicKey,
        proposal: &Proposal,
        delivered_header: &Header,
    ) -> DagResult<Option<Header>> {
        let header = if proposal.header_digest == delivered_header.id {
            Some(delivered_header.clone())
        } else {
            match self.store.read(proposal.header_digest.to_vec()).await? {
                Some(bytes) => Some(bincode::deserialize(&bytes)?),
                None => None,
            }
        };
        if let Some(header) = &header {
            if header.author != *lane
                || header.height != proposal.height
                || header.id != proposal.header_digest
            {
                return Err(crate::error::DagError::InvalidProposal(
                    proposal.header_digest.clone(),
                ));
            }
        }
        Ok(header)
    }

    /// Returns the highest certified coordinate whose suffix is not local.
    async fn first_missing_certified(
        &mut self,
        lane: &PublicKey,
        proposal: &Proposal,
        stop_height: Height,
        delivered_header: &Header,
    ) -> DagResult<Option<Proposal>> {
        let mut current = proposal.clone();
        while current.height > stop_height {
            let Some(header) = self
                .read_proposal_header(lane, &current, delivered_header)
                .await?
            else {
                return Ok(Some(current));
            };
            if header.height == 0 {
                break;
            }
            header.parent_cert.verify(&self.committee)?;
            if header.parent_cert.author != *lane
                || header.parent_cert.height.checked_add(1) != Some(header.height)
            {
                return Err(crate::error::DagError::InvalidProposal(header.id));
            }
            current = Proposal::certified(header.parent_cert);
        }
        Ok(None)
    }

    fn note_implicit_cut_sources(
        &self,
        slot: Slot,
        proposals: &HashMap<PublicKey, Proposal>,
        mut sources: Vec<PublicKey>,
    ) -> Vec<PublicKey> {
        sources.sort_unstable();
        sources.dedup();
        self.implicit_cut_sources
            .write()
            .expect("implicit-cut lock poisoned")
            .insert((slot, proposals_digest(proposals)), sources.clone());
        sources
    }

    fn known_implicit_cut_sources(
        &self,
        slot: Slot,
        proposals: &HashMap<PublicKey, Proposal>,
    ) -> Option<Vec<PublicKey>> {
        self.implicit_cut_sources
            .read()
            .expect("implicit-cut lock poisoned")
            .get(&(slot, proposals_digest(proposals)))
            .cloned()
    }

    fn forget_implicit_cut_sources(&self, slot: Slot) {
        self.implicit_cut_sources
            .write()
            .expect("implicit-cut lock poisoned")
            .retain(|(candidate_slot, _), _| *candidate_slot != slot);
    }

    /// Applies the paper's phase-specific availability rule. Certified suffix
    /// repair is asynchronous; only a missing optimistic tip delays Prepare;
    /// Commit waits for complete materialization.
    pub async fn get_proposals(
        &mut self,
        consensus_message: &ConsensusMessage,
        delivered_header: &Header,
    ) -> DagResult<bool> {
        let (slot, view, proposals, is_prepare, is_commit) = match consensus_message {
            ConsensusMessage::Prepare {
                slot,
                view,
                proposals,
                ..
            } => (*slot, *view, proposals, true, false),
            ConsensusMessage::Confirm {
                slot,
                view,
                proposals,
                ..
            } => (*slot, *view, proposals, false, false),
            ConsensusMessage::Commit {
                slot,
                view,
                proposals,
                ..
            } => (*slot, *view, proposals, false, true),
        };

        let proposal_leader =
            crate::leader::LeaderElector::new(self.committee.clone()).get_leader(slot, view);

        // A winning proposal carried by a TC is implicitly certified: either
        // f+1 timeout reporters named the same Prepare, or a PrepareQC names a
        // quorum of Prep-Voters. It therefore keeps synchronization off the
        // voting path in later views (Autobahn Section 5.5.2).
        let implicit_sources = match consensus_message {
            ConsensusMessage::Prepare {
                tc: Some(tc),
                proposals,
                ..
            } if tc.verify(&self.committee).is_ok() => tc
                .get_winning_proposal(&self.committee)
                .filter(|(winner, _)| winner == proposals)
                .map(|(_, sources)| self.note_implicit_cut_sources(slot, proposals, sources)),
            ConsensusMessage::Confirm { qc, proposals, .. }
            | ConsensusMessage::Commit { qc, proposals, .. } => self
                .known_implicit_cut_sources(slot, proposals)
                .or_else(|| {
                    let sources = qc.votes.iter().map(|(author, _)| *author).collect();
                    Some(self.note_implicit_cut_sources(slot, proposals, sources))
                }),
            ConsensusMessage::Prepare { .. } => None,
        };
        let mut certified_repairs = Vec::new();
        let mut optimistic_repairs = Vec::new();
        let mut implicit_repairs = Vec::new();
        let mut commit_wait = Vec::new();

        for (lane, proposal) in proposals {
            let stop_height = self.executed_height(lane);
            match proposal.verify(lane, &self.committee)? {
                ProposalKind::Genesis => continue,
                ProposalKind::Certified => {
                    if let Some(missing) = self
                        .first_missing_certified(lane, proposal, stop_height, delivered_header)
                        .await?
                    {
                        commit_wait.push(missing.header_digest.clone());
                        certified_repairs.push((*lane, missing, stop_height));
                    }
                }
                ProposalKind::Optimistic => {
                    let parent_poa = proposal
                        .poa
                        .as_ref()
                        .expect("verified optimistic proposal has a parent PoA")
                        .clone();
                    let parent = Proposal::certified(parent_poa.clone());
                    if parent.height > stop_height {
                        if let Some(missing) = self
                            .first_missing_certified(lane, &parent, stop_height, delivered_header)
                            .await?
                        {
                            commit_wait.push(missing.header_digest.clone());
                            certified_repairs.push((*lane, missing, stop_height));
                        }
                    }

                    let tip = self
                        .read_proposal_header(lane, proposal, delivered_header)
                        .await?;
                    let tip_ready = tip.as_ref().is_some_and(|header| {
                        header.parent_cert.author == *lane
                            && header.parent_cert.height.checked_add(1) == Some(header.height)
                            && header.parent_cert == parent_poa
                    });
                    if !tip_ready {
                        if implicit_sources.is_some() {
                            implicit_repairs.push((*lane, proposal.clone()));
                        } else {
                            optimistic_repairs.push((*lane, proposal.clone()));
                        }
                        commit_wait.push(proposal.header_digest.clone());
                    }
                }
            }
        }

        if !certified_repairs.is_empty() {
            self.tx_header_waiter
                .send(WaiterMessage::SyncCertified(certified_repairs))
                .await
                .expect("Failed to schedule certified suffix repair");
        }

        if !optimistic_repairs.is_empty() {
            let resume = is_prepare.then(|| (consensus_message.clone(), delivered_header.clone()));
            self.tx_header_waiter
                .send(WaiterMessage::SyncOptimistic(
                    optimistic_repairs.clone(),
                    proposal_leader,
                    resume,
                ))
                .await
                .expect("Failed to schedule optimistic-tip repair");
        }

        if !implicit_repairs.is_empty() {
            self.tx_header_waiter
                .send(WaiterMessage::SyncImplicit(
                    implicit_repairs,
                    implicit_sources.expect("implicit repairs have evidence sources"),
                ))
                .await
                .expect("Failed to schedule implicitly certified tip repair");
        }

        if is_commit && !commit_wait.is_empty() {
            commit_wait.sort_unstable();
            commit_wait.dedup();
            self.tx_header_waiter
                .send(WaiterMessage::WaitForCommit(
                    commit_wait.clone(),
                    consensus_message.clone(),
                    delivered_header.clone(),
                ))
                .await
                .expect("Failed to schedule committed suffix wait");
        }

        if is_commit && commit_wait.is_empty() {
            self.forget_implicit_cut_sources(slot);
        }

        Ok(if is_commit {
            commit_wait.is_empty()
        } else if is_prepare {
            optimistic_repairs.is_empty()
        } else {
            true
        })
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

        while header.height() > stop_height {
            debug!(
                "current height is {:?}, stop height is {:?}",
                header.height(),
                stop_height
            );
            ancestors.push(header.clone());
            // The block at `stop_height` was already executed; do not make it
            // an artificial materialization dependency for this suffix.
            if header.height() == stop_height.saturating_add(1) {
                break;
            }
            header = self
                .get_parent_header(&header)
                .await?
                .expect("should have parent by now");
        }

        ancestors.reverse();
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
                let stop_height = self
                    .executed_height(&header.author)
                    .min(header.parent_cert.height.saturating_sub(1));
                self.tx_header_waiter
                    .send(WaiterMessage::SyncParent(
                        parent,
                        header.clone(),
                        stop_height,
                    ))
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

#[cfg(test)]
mod alignment_tests {
    use super::*;
    use crate::messages::QC;
    use std::fs;
    use tokio::sync::mpsc::channel;

    fn missing_certified_tip() -> (Committee, PublicKey, Proposal) {
        let committee = crate::common::committee();
        let header = crate::common::header();
        let lane = header.author;
        let mut certificate = crate::common::certificate(&header);
        certificate
            .votes
            .truncate(committee.validity_threshold() as usize);
        (committee, lane, Proposal::certified(certificate))
    }

    fn cut_with(
        committee: &Committee,
        lane: PublicKey,
        proposal: Proposal,
    ) -> HashMap<PublicKey, Proposal> {
        let mut cut = Header::genesis_proposals(committee);
        cut.insert(lane, proposal);
        cut
    }

    #[tokio::test]
    async fn verified_parent_poa_registers_its_voters_as_repair_sources() {
        let (committee, lane, proposal) = missing_certified_tip();
        let parent = proposal.poa.expect("certified proposal");
        let expected = Proposal::certified(parent.clone());
        let (tx, mut rx) = channel(1);
        let path = ".db_test_parent_poa_sources";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();
        let mut synchronizer = Synchronizer::new(lane, &committee, store, tx);

        synchronizer.register_parent_poa_sources(&parent).await;

        match rx.recv().await {
            Some(WaiterMessage::SyncCertified(repairs)) => {
                assert_eq!(repairs, vec![(lane, expected, 0)]);
            }
            other => panic!("unexpected waiter command: {other:?}"),
        }
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn certified_miss_repairs_asynchronously_without_blocking_prepare() {
        let (committee, lane, proposal) = missing_certified_tip();
        let (tx, mut rx) = channel(4);
        let path = ".db_test_certified_prepare_alignment";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();
        let mut synchronizer = Synchronizer::new(lane, &committee, store, tx);
        let prepare = ConsensusMessage::Prepare {
            slot: 3,
            view: 1,
            tc: None,
            qc_ticket: None,
            proposals: cut_with(&committee, lane, proposal),
        };

        assert!(synchronizer
            .get_proposals(&prepare, &Header::default())
            .await
            .unwrap());
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::SyncCertified(_))
        ));
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn optimistic_miss_is_the_only_prepare_blocker() {
        let (committee, lane, certified_parent) = missing_certified_tip();
        let optimistic = Proposal {
            header_digest: Digest([42; 32]),
            height: certified_parent.height + 1,
            poa: certified_parent.poa.clone(),
        };
        let (tx, mut rx) = channel(4);
        let path = ".db_test_optimistic_prepare_alignment";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();
        let mut synchronizer = Synchronizer::new(lane, &committee, store, tx);
        let prepare = ConsensusMessage::Prepare {
            slot: 3,
            view: 1,
            tc: None,
            qc_ticket: None,
            proposals: cut_with(&committee, lane, optimistic),
        };

        assert!(!synchronizer
            .get_proposals(&prepare, &Header::default())
            .await
            .unwrap());
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::SyncCertified(_))
        ));
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::SyncOptimistic(_, _, Some(_)))
        ));
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn winning_view_change_tip_is_implicitly_certified_and_nonblocking() {
        let (committee, lane, certified_parent) = missing_certified_tip();
        let optimistic = Proposal {
            header_digest: Digest([43; 32]),
            height: certified_parent.height + 1,
            poa: certified_parent.poa.clone(),
        };
        let proposals = cut_with(&committee, lane, optimistic);
        let high_prepare = ConsensusMessage::Prepare {
            slot: 3,
            view: 1,
            tc: None,
            qc_ticket: None,
            proposals: proposals.clone(),
        };
        let keys = crate::common::keys();
        let timeouts = keys
            .iter()
            .take(committee.quorum_threshold() as usize)
            .enumerate()
            .map(|(index, (author, secret))| {
                crate::messages::Timeout::new_from_key(
                    (index < committee.validity_threshold() as usize).then(|| high_prepare.clone()),
                    None,
                    3,
                    1,
                    *author,
                    secret,
                )
            })
            .collect();
        let prepare = ConsensusMessage::Prepare {
            slot: 3,
            view: 2,
            tc: Some(crate::messages::TC::new(&committee, 3, 1, timeouts)),
            qc_ticket: None,
            proposals,
        };
        let (tx, mut rx) = channel(4);
        let path = ".db_test_implicit_prepare_alignment";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();
        let mut synchronizer = Synchronizer::new(lane, &committee, store, tx);

        assert!(synchronizer
            .get_proposals(&prepare, &Header::default())
            .await
            .unwrap());
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::SyncCertified(_))
        ));
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::SyncImplicit(_, _))
        ));
        let _ = fs::remove_dir_all(path);
    }

    #[tokio::test]
    async fn commit_waits_for_a_missing_certified_suffix() {
        let (committee, lane, proposal) = missing_certified_tip();
        let (tx, mut rx) = channel(4);
        let path = ".db_test_commit_materialization_alignment";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();
        let mut synchronizer = Synchronizer::new(lane, &committee, store, tx);
        let commit = ConsensusMessage::Commit {
            slot: 3,
            view: 1,
            qc: QC::default(),
            proposals: cut_with(&committee, lane, proposal),
        };

        assert!(!synchronizer
            .get_proposals(&commit, &Header::default())
            .await
            .unwrap());
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::SyncCertified(_))
        ));
        assert!(matches!(
            rx.recv().await,
            Some(WaiterMessage::WaitForCommit(..))
        ));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn optimistic_proposal_shape_uses_the_parent_poa() {
        let (committee, lane, parent) = missing_certified_tip();
        let proposal = Proposal {
            header_digest: Digest([7; 32]),
            height: parent.height + 1,
            poa: parent.poa,
        };
        assert_eq!(
            proposal.verify(&lane, &committee).unwrap(),
            ProposalKind::Optimistic
        );
    }
}
