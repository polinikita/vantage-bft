// Copyright(C) Facebook, Inc. and its affiliates.

use crate::error::{ConsensusError, ConsensusResult, DagError, DagResult};
use crate::primary::{Height, Slot, View};
use config::{Committee, Stake, WorkerId};
use crypto::{Blake3Hasher, Digest, Hash, PublicKey, SecretKey, Signature, SignatureService};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::OnceLock;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Proposal {
    pub header_digest: Digest,
    pub height: Height,
    /// Exact PoA for a certified tip, or the parent PoA for an optimistic tip.
    /// Simple-IT reuses this coordinate type and leaves the field empty because
    /// it carries availability evidence in its own cut protocol.
    #[serde(default)]
    pub poa: Option<Certificate>,
    /// Memoized content digest; a proposal must not be mutated after `digest()`.
    #[serde(skip)]
    pub(crate) digest_memo: OnceLock<Digest>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ProposalKind {
    Genesis,
    Certified,
    Optimistic,
}

impl Proposal {
    pub async fn new(header_digest: Digest, height: Height) -> Self {
        Self {
            header_digest,
            height,
            poa: None,
            digest_memo: OnceLock::new(),
        }
    }

    pub fn certified(poa: Certificate) -> Self {
        Self {
            header_digest: poa.header_digest.clone(),
            height: poa.height,
            poa: Some(poa),
            digest_memo: OnceLock::new(),
        }
    }

    pub fn optimistic(header: &Header) -> Self {
        Self {
            header_digest: header.id.clone(),
            height: header.height,
            poa: Some(header.parent_cert.clone()),
            digest_memo: OnceLock::new(),
        }
    }

    pub fn genesis(author: PublicKey, committee: &Committee) -> Self {
        Self::certified(Certificate::genesis_for(author, committee))
    }

    /// Verifies the proof shape and signatures without requiring the tip body.
    pub fn verify(&self, lane: &PublicKey, committee: &Committee) -> DagResult<ProposalKind> {
        let kind = self.classify(lane, committee)?;
        self.poa
            .as_ref()
            .expect("classified proposals carry a PoA")
            .verify(committee)?;
        Ok(kind)
    }

    /// The crypto-free part of [`Self::verify`]: shape and coordinate checks
    /// only. The caller is responsible for verifying the PoA signatures.
    pub fn classify(&self, lane: &PublicKey, committee: &Committee) -> DagResult<ProposalKind> {
        let poa = self
            .poa
            .as_ref()
            .ok_or_else(|| DagError::InvalidProposal(self.header_digest.clone()))?;
        ensure!(
            poa.author == *lane,
            DagError::InvalidProposal(self.header_digest.clone())
        );

        if self.height == 0 {
            ensure!(
                poa.is_genesis_for(lane, committee) && self.header_digest == poa.header_digest,
                DagError::InvalidProposal(self.header_digest.clone())
            );
            return Ok(ProposalKind::Genesis);
        }
        if poa.height == self.height && poa.header_digest == self.header_digest {
            return Ok(ProposalKind::Certified);
        }
        if poa.height.checked_add(1) == Some(self.height) {
            return Ok(ProposalKind::Optimistic);
        }
        Err(DagError::InvalidProposal(self.header_digest.clone()))
    }
}

impl PartialEq for Proposal {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl Eq for Proposal {}

impl fmt::Debug for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "P({}, {})", self.height, self.header_digest)
    }
}

impl fmt::Display for Proposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "P({}, {})", self.height, self.header_digest)
    }
}

impl Hash for Proposal {
    fn digest(&self) -> Digest {
        self.digest_memo
            .get_or_init(|| {
                let mut hasher = Blake3Hasher::new();
                // Preserve Simple-IT's pre-existing coordinate digest. Autobahn
                // cuts always carry `Some(PoA)` and use the evidence-bound
                // domain below.
                if self.poa.is_none() {
                    hasher.update(&self.header_digest.0);
                    hasher.update(&self.height.to_le_bytes());
                    return Digest(hasher.finalize().into());
                }
                hasher.update(b"autobahn-tip-v1");
                hasher.update(&self.header_digest.0);
                hasher.update(&self.height.to_le_bytes());
                match &self.poa {
                    Some(poa) => {
                        hasher.update(&[1]);
                        hasher.update(&poa.evidence_digest().0);
                    }
                    None => unreachable!("proof-free proposals returned above"),
                };
                Digest(hasher.finalize().into())
            })
            .clone()
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct CommitQC {
    pub slot: Slot,
    view: View,
    qc: QC,
    proposals: HashMap<PublicKey, Proposal>,
}

impl CommitQC {
    pub async fn new(
        slot: Slot,
        view: View,
        qc: QC,
        proposals: HashMap<PublicKey, Proposal>,
    ) -> Self {
        Self {
            slot,
            view,
            qc,
            proposals,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    Prepare {
        slot: Slot,
        view: View,
        /// Previous-view TC. View 1 uses `qc_ticket` instead.
        tc: Option<TC>,
        qc_ticket: Option<CommitQC>,
        proposals: HashMap<PublicKey, Proposal>,
    },
    Confirm {
        slot: Slot,
        view: View,
        qc: QC,
        proposals: HashMap<PublicKey, Proposal>,
    },
    Commit {
        slot: Slot,
        view: View,
        qc: QC,
        proposals: HashMap<PublicKey, Proposal>,
    },
}

pub fn transform_commit_qc(commit_qc: CommitQC) -> ConsensusMessage {
    ConsensusMessage::Commit {
        slot: commit_qc.slot,
        view: commit_qc.view,
        qc: commit_qc.qc,
        proposals: commit_qc.proposals,
    }
}

pub fn verify_commit(consensus_message: &ConsensusMessage, committee: &Committee) -> bool {
    match consensus_message {
        ConsensusMessage::Commit {
            slot,
            view,
            qc,
            proposals,
        } => {
            let prepare_id = prepare_digest(*slot, *view, proposals);

            debug!(
                "PrepareIDCheck has slot: {}, view: {}, digest: {}",
                slot, view, prepare_id
            );

            if qc.id == prepare_id {
                qc.verify_at(committee, committee.fast_threshold()).is_ok()
            } else {
                let confirm_id = confirm_digest(*slot, *view, &prepare_id);

                debug!(
                    "ConfirmIDCheck for slot: {}, view: {}, qc_dig {:?} -> has digest: {}",
                    slot, view, prepare_id, confirm_id
                );

                confirm_id == qc.id && qc.verify(committee).is_ok()
            }
        }
        _ => false,
    }
}

pub fn verify_confirm(consensus_message: &ConsensusMessage, committee: &Committee) -> bool {
    match consensus_message {
        ConsensusMessage::Confirm {
            slot,
            view,
            qc,
            proposals,
        } => {
            let prepare_id = prepare_digest(*slot, *view, proposals);

            if prepare_id != qc.id {
                return false;
            }

            qc.verify(committee).is_ok()
        }
        _ => false,
    }
}

pub fn proposal_digest(consensus_message: &ConsensusMessage) -> Digest {
    let proposals = match consensus_message {
        ConsensusMessage::Prepare {
            slot: _,
            view: _,
            tc: _,
            qc_ticket: _,
            proposals,
        } => proposals,
        ConsensusMessage::Confirm {
            slot: _,
            view: _,
            qc: _,
            proposals,
        } => proposals,
        ConsensusMessage::Commit {
            slot: _,
            view: _,
            qc: _,
            proposals,
        } => proposals,
    };
    proposals_digest(proposals)
}

/// Canonical, map-order-independent digest of an Autobahn lane cut.
pub fn proposals_digest(proposals: &HashMap<PublicKey, Proposal>) -> Digest {
    let mut entries: Vec<_> = proposals.iter().collect();
    entries.sort_unstable_by_key(|(author, _)| **author);
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"autobahn-cut-v1");
    hasher.update(&(entries.len() as u64).to_le_bytes());
    for (author, proposal) in entries {
        hasher.update(&author.0);
        hasher.update(&proposal.digest().0);
    }
    Digest(hasher.finalize().into())
}

pub fn prepare_digest(slot: Slot, view: View, proposals: &HashMap<PublicKey, Proposal>) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"autobahn-prepare-v1");
    hasher.update(&slot.to_le_bytes());
    hasher.update(&view.to_le_bytes());
    hasher.update(&proposals_digest(proposals).0);
    Digest(hasher.finalize().into())
}

pub fn confirm_digest(slot: Slot, view: View, prepare_id: &Digest) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"autobahn-confirm-v1");
    hasher.update(&slot.to_le_bytes());
    hasher.update(&view.to_le_bytes());
    hasher.update(&prepare_id.0);
    Digest(hasher.finalize().into())
}

impl Hash for ConsensusMessage {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        match self {
            ConsensusMessage::Prepare {
                slot,
                view,
                tc: _,
                qc_ticket: _,
                proposals,
            } => return prepare_digest(*slot, *view, proposals),
            ConsensusMessage::Confirm {
                slot,
                view,
                qc: _,
                proposals,
            } => return confirm_digest(*slot, *view, &prepare_digest(*slot, *view, proposals)),
            ConsensusMessage::Commit {
                slot,
                view,
                qc,
                proposals,
            } => {
                hasher.update(b"autobahn-commit-v1");
                hasher.update(&slot.to_le_bytes());
                hasher.update(&view.to_le_bytes());
                hasher.update(&qc.id.0);
                hasher.update(&proposals_digest(proposals).0);
            }
        }
        Digest(hasher.finalize().into())
    }
}

impl std::hash::Hash for ConsensusMessage {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(&self.digest().0)
    }
}

impl PartialEq for ConsensusMessage {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl Eq for ConsensusMessage {}

impl fmt::Debug for ConsensusMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            ConsensusMessage::Prepare {
                slot,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                write!(f, "Prepare({})", slot,)
            }

            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                write!(f, "Confirm({})", slot,)
            }

            ConsensusMessage::Commit {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                write!(f, "Commit({})", slot,)
            }
        }
    }
}

impl fmt::Display for ConsensusMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            ConsensusMessage::Prepare {
                slot,
                view: _,
                tc: _,
                qc_ticket: _,
                proposals: _,
            } => {
                write!(f, "T{})", slot,)
            }

            ConsensusMessage::Confirm {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                write!(f, "T{})", slot,)
            }

            ConsensusMessage::Commit {
                slot,
                view: _,
                qc: _,
                proposals: _,
            } => {
                write!(f, "T{})", slot,)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub author: PublicKey,
    pub height: Height,
    pub payload: BTreeMap<Digest, WorkerId>,
    pub parent_cert: Certificate,
    pub id: Digest,
    /// `None` for unsigned Vantage headers.
    pub signature: Option<Signature>,
    /// Vantage session identifier.
    pub sid: Option<Digest>,

    pub consensus_messages: HashMap<Digest, ConsensusMessage>,
    pub num_active_instances: usize,
    pub special: bool,
}

impl Header {
    pub async fn new(
        author: PublicKey,
        height: Height,
        payload: BTreeMap<Digest, WorkerId>,
        parent_cert: Certificate,
        signature_service: &mut SignatureService,
        consensus_instances: HashMap<Digest, ConsensusMessage>,
        num_active_instances: usize,
    ) -> Self {
        let header = Self {
            author,
            height,
            payload,
            parent_cert,
            id: Digest::default(),
            signature: Some(Signature::default()),
            sid: None,
            consensus_messages: consensus_instances,
            num_active_instances,
            special: false,
        };
        let id = header.digest();
        let signature = signature_service.request_signature(id.clone()).await;
        Self {
            id,
            signature: Some(signature),
            ..header
        }
    }

    /// Constructs an unsigned, session-bound Vantage header.
    /// `parent_cert` stores only the predecessor coordinate.
    pub fn new_vantage(
        author: PublicKey,
        height: Height,
        payload: BTreeMap<Digest, WorkerId>,
        prev_digest: Digest,
        sid: Digest,
    ) -> Self {
        let parent_cert = Certificate {
            author,
            header_digest: prev_digest,
            height: height.saturating_sub(1),
            votes: Vec::new(),
            evidence_memo: OnceLock::new(),
        };
        let header = Self {
            author,
            height,
            payload,
            parent_cert,
            id: Digest::default(),
            signature: None,
            sid: Some(sid),
            consensus_messages: HashMap::new(),
            num_active_instances: 0,
            special: false,
        };
        let id = header.digest();
        Self { id, ..header }
    }

    /// Returns the committee genesis header.
    pub fn genesis(committee: &Committee) -> Self {
        let (name, _) = committee.authorities.iter().next().unwrap();
        Header {
            author: *name,
            ..Self::default()
        }
    }

    pub fn genesis_headers(committee: &Committee) -> HashMap<PublicKey, Self> {
        committee
            .authorities
            .keys()
            .map(|pk| {
                (
                    *pk,
                    Header {
                        author: *pk,
                        ..Self::default()
                    },
                )
            })
            .collect()
    }

    pub fn genesis_proposals(committee: &Committee) -> HashMap<PublicKey, Proposal> {
        committee
            .authorities
            .keys()
            .map(|pk| (*pk, Proposal::genesis(*pk, committee)))
            .collect()
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(self.digest() == self.id, DagError::InvalidHeaderId);

        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(self.author));

        for worker_id in self.payload.values() {
            committee
                .worker(&self.author, worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }

        let active_instances =
            self.consensus_messages
                .iter()
                .try_fold(0usize, |active, (advertised, message)| {
                    ensure!(
                        advertised == &message.digest(),
                        DagError::MalformedHeader(self.id.clone())
                    );
                    Ok::<_, DagError>(
                        active
                            + usize::from(matches!(
                                message,
                                ConsensusMessage::Prepare { .. } | ConsensusMessage::Confirm { .. }
                            )),
                    )
                })?;
        ensure!(
            active_instances == self.num_active_instances,
            DagError::MalformedHeader(self.id.clone())
        );

        // Vantage validates unsigned headers through `vantage::block::block_ok`.
        self.signature
            .as_ref()
            .ok_or_else(|| DagError::MalformedHeader(self.id.clone()))?
            .verify(&self.id, &self.author)
            .map_err(DagError::from)
    }

    pub fn height(&self) -> Height {
        self.height
    }

    pub fn origin(&self) -> PublicKey {
        self.author
    }

    pub fn new_from_key(
        author: PublicKey,
        _view: View,
        round: Height,
        secret: &SecretKey,
        _committee: &Committee,
    ) -> Header {
        let header = Header {
            author,
            height: round,
            signature: Some(Signature::default()),
            ..Header::default()
        };
        let id = header.digest();
        let signature = Signature::new(&id, secret);
        Self {
            id,
            signature: Some(signature),
            ..header
        }
    }
}

impl Hash for Header {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.author.0);
        hasher.update(&self.height.to_le_bytes());
        for (x, y) in &self.payload {
            hasher.update(&x.0);
            hasher.update(&y.to_le_bytes());
        }
        hasher.update(&self.parent_cert.header_digest.0);

        // Encode both session presence and value; signatures are excluded.
        match &self.sid {
            Some(sid) => {
                hasher.update(&[1u8]);
                hasher.update(&sid.0);
            }
            None => {
                hasher.update(&[0u8]);
            }
        };

        // Ride-sharing is an Autobahn-only optimization. When present, bind
        // the embedded consensus values and their advertised count to the car
        // signature. Empty maps retain the legacy/Vantage header digest.
        if !self.consensus_messages.is_empty() {
            let mut messages: Vec<_> = self.consensus_messages.values().collect();
            messages.sort_unstable_by_key(|message| message.digest());
            hasher.update(b"autobahn-rideshare-v1");
            hasher.update(&(self.num_active_instances as u64).to_le_bytes());
            hasher.update(&(messages.len() as u64).to_le_bytes());
            for message in messages {
                hasher.update(&message.digest().0);
            }
        }

        Digest(hasher.finalize().into())
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Header id: {}: height: {}, # of consensus messages: {}, author: {:?}, payload: {:?})",
            self.id,
            self.height,
            self.consensus_messages.len(),
            self.author,
            self.payload.keys().map(|x| x.size()).sum::<usize>(),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.height, self.author)
    }
}

/// Unsigned Vantage availability acknowledgement.
/// Receivers authenticate `sender` against the channel identity before counting it.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Ack {
    pub author: PublicKey,
    pub height: Height,
    pub digest: Digest,
    pub sender: PublicKey,
}

impl Ack {
    pub fn new(author: PublicKey, height: Height, digest: Digest, sender: PublicKey) -> Self {
        Self {
            author,
            height,
            digest,
            sender,
        }
    }

    /// Returns the acknowledged `(author, height, digest)`.
    pub fn reference(&self) -> (PublicKey, Height, Digest) {
        (self.author, self.height, self.digest.clone())
    }
}

impl fmt::Display for Ack {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Ack({}, {}, {}, from {})",
            self.author, self.height, self.digest, self.sender
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConsensusRequest {
    pub author: PublicKey,
    pub message: ConsensusMessage,
    pub sig: Signature,
}
impl ConsensusRequest {
    pub async fn new(
        author: PublicKey,
        message: ConsensusMessage,
        signature_service: &mut SignatureService,
    ) -> Self {
        let req = Self {
            author,
            message,
            sig: Signature::default(),
        };
        let sig = signature_service
            .request_signature(req.message.digest())
            .await;
        Self { sig, ..req }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        self.sig
            .verify(&self.message.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl fmt::Debug for ConsensusRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.message.fmt(f)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConsensusVote {
    pub author: PublicKey,
    pub slot: Slot,
    /// Digest of the voted consensus message.
    pub digest: Digest,
    pub sig: Signature,
}
impl ConsensusVote {
    pub async fn new(
        author: PublicKey,
        slot: Slot,
        digest: Digest,
        signature_service: &mut SignatureService,
    ) -> Self {
        let vote = Self {
            author,
            slot,
            digest,
            sig: Signature::default(),
        };
        let sig = signature_service
            .request_signature(vote.digest.clone())
            .await;
        Self { sig, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        self.sig
            .verify(&self.digest, &self.author)
            .map_err(DagError::from)
    }
}

impl fmt::Debug for ConsensusVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "CV{}({}, {})", self.digest, self.slot, self.author,)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub id: Digest,
    pub height: Height,
    pub origin: PublicKey,
    pub author: PublicKey,
    pub signature: Signature,
    pub consensus_votes: Vec<(Slot, Digest, Signature)>,
}

impl Vote {
    pub async fn new(
        header: &Header,
        author: &PublicKey,
        signature_service: &mut SignatureService,
        consensus_votes: Vec<(Slot, Digest, Signature)>,
    ) -> Self {
        let vote = Self {
            id: header.id.clone(),
            height: header.height,
            origin: header.author,
            author: *author,
            signature: Signature::default(),
            consensus_votes,
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        car_vote_digest(self.origin, self.height, &self.id)
    }
}

pub fn car_vote_digest(author: PublicKey, height: Height, header_digest: &Digest) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(b"autobahn-car-vote-v1");
    hasher.update(&author.0);
    hasher.update(&height.to_le_bytes());
    hasher.update(&header_digest.0);
    Digest(hasher.finalize().into())
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {})",
            self.digest(),
            self.height,
            self.author,
            self.id
        )
    }
}

impl Vote {
    pub fn new_from_key(
        header: Header,
        consensus_votes: Vec<(Slot, Digest, Signature)>,
        author: PublicKey,
        secret: &SecretKey,
    ) -> Self {
        let vote = Vote {
            id: header.id.clone(),
            height: header.height(),
            origin: header.origin(),
            author,
            signature: Signature::default(),
            consensus_votes,
        };
        let signature = Signature::new(&vote.digest(), secret);
        Self { signature, ..vote }
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    pub author: PublicKey,
    pub header_digest: Digest,
    pub height: Height,
    pub votes: Vec<(PublicKey, Signature)>,
    /// Memoized evidence digest; `votes` must not be mutated after
    /// `evidence_digest()`.
    #[serde(skip)]
    pub(crate) evidence_memo: OnceLock<Digest>,
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|name| Self::genesis_for(*name, committee))
            .collect()
    }

    pub fn genesis_cert(committee: &Committee) -> Self {
        let author = *committee
            .authorities
            .keys()
            .next()
            .expect("committee cannot be empty");
        Self::genesis_for(author, committee)
    }

    pub fn genesis_for(author: PublicKey, _committee: &Committee) -> Self {
        Self {
            author,
            header_digest: Header {
                author,
                ..Header::default()
            }
            .digest(),
            height: 0,
            votes: Vec::new(),
            evidence_memo: OnceLock::new(),
        }
    }

    pub fn is_genesis_for(&self, author: &PublicKey, committee: &Committee) -> bool {
        committee.stake(author) > 0
            && self == &Self::genesis_for(*author, committee)
            && self.votes.is_empty()
    }

    pub fn genesis_certs(committee: &Committee) -> HashMap<PublicKey, Self> {
        committee
            .authorities
            .keys()
            .map(|name| (*name, Self::genesis_for(*name, committee)))
            .collect()
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Genesis certificates are always valid.
        if self.is_genesis_for(&self.author, committee) {
            return Ok(());
        }
        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), DagError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, DagError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.validity_threshold(),
            DagError::CertificateRequiresQuorum
        );

        let digest = car_vote_digest(self.author, self.height, &self.header_digest);
        let digests = vec![digest; self.votes.len()];
        Signature::verify_batch_multi(&digests, &self.votes).map_err(DagError::from)
    }

    /// Hashes the complete evidence canonically, independent of vote arrival order.
    pub fn evidence_digest(&self) -> Digest {
        self.evidence_memo
            .get_or_init(|| {
                let mut votes: Vec<_> = self.votes.iter().collect();
                votes.sort_unstable_by_key(|(author, _)| *author);
                let mut hasher = Blake3Hasher::new();
                hasher.update(b"autobahn-poa-v1");
                hasher.update(&self.author.0);
                hasher.update(&self.height.to_le_bytes());
                hasher.update(&self.header_digest.0);
                hasher.update(&(votes.len() as u64).to_le_bytes());
                for (author, signature) in votes {
                    hasher.update(&author.0);
                    // The raw 64 bytes are exactly the bincode encoding of the
                    // signature's two fixed-size parts, so the digest is
                    // unchanged while skipping the per-vote allocation.
                    let encoded = signature.to_bytes();
                    hasher.update(&(encoded.len() as u64).to_le_bytes());
                    hasher.update(&encoded);
                }
                Digest(hasher.finalize().into())
            })
            .clone()
    }

    pub fn height(&self) -> Height {
        self.height
    }

    pub fn origin(&self) -> PublicKey {
        self.author
    }
}

impl Hash for Certificate {
    fn digest(&self) -> Digest {
        car_vote_digest(self.author, self.height, &self.header_digest)
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C{}({},,,, view: )",
            self.digest(),
            self.height(),
            self.header_digest,
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        self.author == other.author
            && self.header_digest == other.header_digest
            && self.height == other.height
    }
}

impl Eq for Certificate {}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub id: Digest,
    pub votes: Vec<(PublicKey, Signature)>,
    /// Memoized content digest; `votes` must not be mutated after `digest()`.
    #[serde(skip)]
    pub(crate) digest_memo: OnceLock<Digest>,
}

impl QC {
    pub fn genesis(_committee: &Committee) -> Self {
        QC::default()
    }

    #[allow(clippy::result_large_err)]
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        self.verify_at(committee, committee.quorum_threshold())
    }

    #[allow(clippy::result_large_err)]
    pub fn verify_at(&self, committee: &Committee, threshold: Stake) -> ConsensusResult<()> {
        if Self::genesis(committee) == *self {
            return Ok(());
        }

        let mut weight = 0;
        let mut used = HashSet::new();
        for (name, _) in self.votes.iter() {
            ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(weight >= threshold, ConsensusError::QCRequiresQuorum);

        Signature::verify_batch(&self.id, &self.votes).map_err(ConsensusError::from)
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        self.digest_memo
            .get_or_init(|| {
                let mut votes: Vec<_> = self.votes.iter().collect();
                votes.sort_unstable_by_key(|(author, _)| *author);
                let mut hasher = Blake3Hasher::new();
                hasher.update(b"autobahn-qc-v1");
                hasher.update(&self.id.0);
                hasher.update(&(votes.len() as u64).to_le_bytes());
                for (author, signature) in votes {
                    hasher.update(&author.0);
                    // Raw 64 bytes == the bincode encoding; digest unchanged.
                    let encoded = signature.to_bytes();
                    hasher.update(&(encoded.len() as u64).to_le_bytes());
                    hasher.update(&encoded);
                }
                Digest(hasher.finalize().into())
            })
            .clone()
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC({}, {})", 1, 1)
    }
}

impl PartialEq for QC {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl Eq for QC {}

#[derive(Clone)]
pub struct Timeout {
    pub slot: Slot,
    pub view: View,
    pub high_qc: Option<ConsensusMessage>,
    pub high_prop: Option<ConsensusMessage>,

    pub author: PublicKey,
    pub signature: Signature,
}

impl Timeout {
    pub async fn new(
        slot: Slot,
        view: View,
        high_qc: Option<ConsensusMessage>,
        high_prop: Option<ConsensusMessage>,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let timeout = Self {
            slot,
            view,
            high_qc,
            high_prop,
            author,
            signature: Signature::default(),
        };

        let signature = signature_service.request_signature(timeout.digest()).await;
        Self {
            signature,
            ..timeout
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        self.verify_with(committee, &mut verify_confirm)
    }

    /// [`Self::verify`] with the (expensive) embedded-Confirm check supplied
    /// by the caller, so a verification cache can be threaded through without
    /// duplicating this logic.
    pub fn verify_with(
        &self,
        committee: &Committee,
        check_confirm: &mut dyn FnMut(&ConsensusMessage, &Committee) -> bool,
    ) -> DagResult<()> {
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        self.signature.verify(&self.digest(), &self.author)?;

        if let Some(high_qc) = &self.high_qc {
            let well_formed = match high_qc {
                ConsensusMessage::Confirm { slot, view, .. } => {
                    *slot == self.slot && *view <= self.view && check_confirm(high_qc, committee)
                }
                ConsensusMessage::Prepare { .. } | ConsensusMessage::Commit { .. } => false,
            };
            ensure!(well_formed, DagError::MalformedTimeout(self.digest()));
        }

        if let Some(high_prop) = &self.high_prop {
            let well_formed = matches!(
                high_prop,
                ConsensusMessage::Prepare { slot, view, .. }
                    if *slot == self.slot && *view <= self.view
            );
            ensure!(well_formed, DagError::MalformedTimeout(self.digest()));
        }

        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(b"autobahn-timeout-v1");
        hasher.update(&self.slot.to_le_bytes());
        hasher.update(&self.view.to_le_bytes());
        hasher.update(&self.author.0);
        match &self.high_qc {
            Some(qc) => {
                hasher.update(&[1]);
                hasher.update(&qc.digest().0);
            }
            None => {
                hasher.update(&[0]);
            }
        };
        match &self.high_prop {
            Some(proposal) => {
                hasher.update(&[1]);
                hasher.update(&proposal.digest().0);
            }
            None => {
                hasher.update(&[0]);
            }
        };
        Digest(hasher.finalize().into())
    }
}

impl fmt::Debug for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TV({}, {:?})", self.author, self.high_qc)
    }
}

impl Timeout {
    pub fn new_from_key(
        high_prop: Option<ConsensusMessage>,
        high_qc: Option<ConsensusMessage>,
        slot: Slot,
        view: View,
        author: PublicKey,
        secret: &SecretKey,
    ) -> Self {
        let timeout = Timeout {
            high_prop,
            high_qc,
            slot,
            view,
            author,
            signature: Signature::default(),
        };
        let signature = Signature::new(&timeout.digest(), secret);
        Self {
            signature,
            ..timeout
        }
    }
}

impl PartialEq for Timeout {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

#[derive(Clone, Default)]
pub struct TC {
    pub slot: Slot,
    pub view: View,
    pub timeouts: Vec<Timeout>,
}

impl PartialEq for TC {
    fn eq(&self, other: &Self) -> bool {
        self.slot == other.slot && self.view == other.view && self.timeouts == other.timeouts
    }
}

impl Eq for TC {}

type ViewChangeCandidate = (View, Digest, HashMap<PublicKey, Proposal>, Vec<PublicKey>);
type PrepareReports = (Stake, HashMap<PublicKey, Proposal>, Vec<PublicKey>);

impl TC {
    pub fn new(_committee: &Committee, slot: Slot, view: View, timeouts: Vec<Timeout>) -> Self {
        Self {
            slot,
            view,
            timeouts,
        }
    }

    pub fn genesis(_committee: &Committee) -> Self {
        Self::default()
    }

    /// Returns the paper's deterministic view-change winner together with the
    /// replicas whose evidence proves possession of that Prepare's optimistic
    /// tips. The candidate with the highest view wins; a PrepareQC wins a tie
    /// against f+1 matching high-Prepare reports, exactly as in Section 5.3.
    pub fn get_winning_proposal(
        &self,
        committee: &Committee,
    ) -> Option<(HashMap<PublicKey, Proposal>, Vec<PublicKey>)> {
        let mut highest_qc: Option<ViewChangeCandidate> = None;

        for timeout in &self.timeouts {
            let Some(message) = &timeout.high_qc else {
                continue;
            };
            let (slot, view, qc, proposals) = match message {
                ConsensusMessage::Confirm {
                    slot,
                    view,
                    qc,
                    proposals,
                } => (*slot, *view, qc, proposals),
                ConsensusMessage::Prepare { .. } | ConsensusMessage::Commit { .. } => continue,
            };
            if slot != self.slot {
                continue;
            }
            let digest = message.digest();
            let replace = highest_qc
                .as_ref()
                .is_none_or(|(best_view, best_digest, ..)| {
                    (view, &digest) > (*best_view, best_digest)
                });
            if replace {
                let mut sources: Vec<_> = qc.votes.iter().map(|(author, _)| *author).collect();
                sources.sort_unstable();
                sources.dedup();
                highest_qc = Some((view, digest, proposals.clone(), sources));
            }
        }

        let mut prepares: HashMap<(View, Digest), PrepareReports> = HashMap::new();
        for timeout in &self.timeouts {
            let Some(prepare) = &timeout.high_prop else {
                continue;
            };
            let ConsensusMessage::Prepare {
                slot,
                view,
                proposals,
                ..
            } = prepare
            else {
                continue;
            };
            if *slot != self.slot || *view > self.view {
                continue;
            }
            let entry = prepares
                .entry((*view, prepare.digest()))
                .or_insert_with(|| (0, proposals.clone(), Vec::new()));
            entry.0 += committee.stake(&timeout.author);
            entry.2.push(timeout.author);
        }

        let highest_prepare = prepares
            .into_iter()
            .filter(|(_, (stake, _, _))| *stake >= committee.validity_threshold())
            .max_by(|((view_a, digest_a), _), ((view_b, digest_b), _)| {
                (view_a, digest_a).cmp(&(view_b, digest_b))
            })
            .map(|((view, digest), (_, proposals, mut sources))| {
                sources.sort_unstable();
                sources.dedup();
                (view, digest, proposals, sources)
            });

        let winner = match (highest_qc, highest_prepare) {
            (Some(qc), Some(prepare)) if prepare.0 > qc.0 => prepare,
            (Some(qc), _) => qc,
            (None, Some(prepare)) => prepare,
            (None, None) => return None,
        };
        Some((winner.2, winner.3))
    }

    pub fn get_winning_proposals(&self, committee: &Committee) -> HashMap<PublicKey, Proposal> {
        self.get_winning_proposal(committee)
            .map(|(proposals, _)| proposals)
            .unwrap_or_default()
    }

    #[allow(clippy::result_large_err)]
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        self.verify_with(committee, &mut verify_confirm)
    }

    /// [`Self::verify`] with the embedded-Confirm check supplied by the
    /// caller, so a verification cache can be threaded through.
    #[allow(clippy::result_large_err)]
    pub fn verify_with(
        &self,
        committee: &Committee,
        check_confirm: &mut dyn FnMut(&ConsensusMessage, &Committee) -> bool,
    ) -> ConsensusResult<()> {
        if self.slot == 0 && self.view == 0 && self.timeouts.is_empty() {
            return Ok(());
        }

        let mut weight = 0;
        let mut used = HashSet::new();
        for timeout in self.timeouts.iter() {
            ensure!(
                timeout.slot == self.slot && timeout.view == self.view,
                ConsensusError::InvalidTimeout(timeout.clone())
            );
            let name = &timeout.author;
            ensure!(!used.contains(name), ConsensusError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, ConsensusError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::TCRequiresQuorum
        );

        for timeout in &self.timeouts {
            timeout.verify_with(committee, check_confirm)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TC({}, {:?})", self.slot, self.view)
    }
}

// ---------------------------------------------------------------------------
// Deduplicated wire encoding for Timeout and TC.
//
// A Timeout carries up to two evidence messages whose cuts are usually the
// same map, and a TC carries 2f+1 timeouts that in the honest case all cite
// the same one or two messages. Encoding each distinct cut and each distinct
// evidence message once (index-referenced) shrinks a TC at n=100 from tens of
// megabytes to roughly one cut. Only the encoding changes: reconstruction is
// byte-exact per referenced object, so digests, signatures, and verification
// outcomes are unaffected. Evidence entries are interned by their complete
// serialized content, never by the (vote-set-blind) message digest.
// ---------------------------------------------------------------------------

type Cut = HashMap<PublicKey, Proposal>;

#[derive(Serialize, Deserialize, Clone)]
enum ConsensusMessageWire {
    Prepare {
        slot: Slot,
        view: View,
        tc: Option<TC>,
        qc_ticket: Option<CommitQC>,
        cut: u16,
    },
    Confirm {
        slot: Slot,
        view: View,
        qc: QC,
        cut: u16,
    },
    Commit {
        slot: Slot,
        view: View,
        qc: QC,
        cut: u16,
    },
}

fn intern_cut(cuts: &mut Vec<Cut>, index: &mut HashMap<Digest, u16>, cut: &Cut) -> u16 {
    let digest = proposals_digest(cut);
    *index.entry(digest).or_insert_with(|| {
        let position = u16::try_from(cuts.len()).expect("too many distinct cuts in one timeout");
        cuts.push(cut.clone());
        position
    })
}

fn message_to_wire(
    message: &ConsensusMessage,
    cuts: &mut Vec<Cut>,
    cut_index: &mut HashMap<Digest, u16>,
) -> ConsensusMessageWire {
    match message {
        ConsensusMessage::Prepare {
            slot,
            view,
            tc,
            qc_ticket,
            proposals,
        } => ConsensusMessageWire::Prepare {
            slot: *slot,
            view: *view,
            tc: tc.clone(),
            qc_ticket: qc_ticket.clone(),
            cut: intern_cut(cuts, cut_index, proposals),
        },
        ConsensusMessage::Confirm {
            slot,
            view,
            qc,
            proposals,
        } => ConsensusMessageWire::Confirm {
            slot: *slot,
            view: *view,
            qc: qc.clone(),
            cut: intern_cut(cuts, cut_index, proposals),
        },
        ConsensusMessage::Commit {
            slot,
            view,
            qc,
            proposals,
        } => ConsensusMessageWire::Commit {
            slot: *slot,
            view: *view,
            qc: qc.clone(),
            cut: intern_cut(cuts, cut_index, proposals),
        },
    }
}

fn message_from_wire(
    wire: &ConsensusMessageWire,
    cuts: &[Cut],
) -> Result<ConsensusMessage, String> {
    let resolve = |cut: u16| {
        cuts.get(cut as usize)
            .cloned()
            .ok_or_else(|| "dangling cut index in a timeout certificate".to_string())
    };
    Ok(match wire {
        ConsensusMessageWire::Prepare {
            slot,
            view,
            tc,
            qc_ticket,
            cut,
        } => ConsensusMessage::Prepare {
            slot: *slot,
            view: *view,
            tc: tc.clone(),
            qc_ticket: qc_ticket.clone(),
            proposals: resolve(*cut)?,
        },
        ConsensusMessageWire::Confirm {
            slot,
            view,
            qc,
            cut,
        } => ConsensusMessage::Confirm {
            slot: *slot,
            view: *view,
            qc: qc.clone(),
            proposals: resolve(*cut)?,
        },
        ConsensusMessageWire::Commit {
            slot,
            view,
            qc,
            cut,
        } => ConsensusMessage::Commit {
            slot: *slot,
            view: *view,
            qc: qc.clone(),
            proposals: resolve(*cut)?,
        },
    })
}

#[derive(Serialize, Deserialize)]
struct TimeoutWire {
    slot: Slot,
    view: View,
    author: PublicKey,
    signature: Signature,
    cuts: Vec<Cut>,
    high_qc: Option<ConsensusMessageWire>,
    high_prop: Option<ConsensusMessageWire>,
}

impl Serialize for Timeout {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut cuts = Vec::new();
        let mut cut_index = HashMap::new();
        let high_qc = self
            .high_qc
            .as_ref()
            .map(|message| message_to_wire(message, &mut cuts, &mut cut_index));
        let high_prop = self
            .high_prop
            .as_ref()
            .map(|message| message_to_wire(message, &mut cuts, &mut cut_index));
        TimeoutWire {
            slot: self.slot,
            view: self.view,
            author: self.author,
            signature: self.signature.clone(),
            cuts,
            high_qc,
            high_prop,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timeout {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = TimeoutWire::deserialize(deserializer)?;
        if wire.cuts.len() > 2 {
            return Err(serde::de::Error::custom(
                "a timeout references at most two cuts",
            ));
        }
        let high_qc = wire
            .high_qc
            .as_ref()
            .map(|message| message_from_wire(message, &wire.cuts))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        let high_prop = wire
            .high_prop
            .as_ref()
            .map(|message| message_from_wire(message, &wire.cuts))
            .transpose()
            .map_err(serde::de::Error::custom)?;
        Ok(Timeout {
            slot: wire.slot,
            view: wire.view,
            high_qc,
            high_prop,
            author: wire.author,
            signature: wire.signature,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct TimeoutLite {
    slot: Slot,
    view: View,
    author: PublicKey,
    signature: Signature,
    high_qc: Option<u16>,
    high_prop: Option<u16>,
}

#[derive(Serialize, Deserialize)]
struct TCWire {
    slot: Slot,
    view: View,
    cuts: Vec<Cut>,
    evidence: Vec<ConsensusMessageWire>,
    timeouts: Vec<TimeoutLite>,
}

/// Interns an evidence message by its complete serialized content, so two
/// evidence values collapse only when they are byte-identical.
fn intern_evidence(
    evidence: &mut Vec<ConsensusMessageWire>,
    index: &mut HashMap<Digest, u16>,
    wire: ConsensusMessageWire,
) -> u16 {
    let bytes = bincode::serialize(&wire).expect("evidence serialization cannot fail");
    let mut hasher = Blake3Hasher::new();
    hasher.update(&bytes);
    let key = Digest(hasher.finalize().into());
    *index.entry(key).or_insert_with(|| {
        let position =
            u16::try_from(evidence.len()).expect("too many distinct evidence messages in one TC");
        evidence.push(wire);
        position
    })
}

impl Serialize for TC {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut cuts = Vec::new();
        let mut cut_index = HashMap::new();
        let mut evidence = Vec::new();
        let mut evidence_index = HashMap::new();
        let timeouts = self
            .timeouts
            .iter()
            .map(|timeout| TimeoutLite {
                slot: timeout.slot,
                view: timeout.view,
                author: timeout.author,
                signature: timeout.signature.clone(),
                high_qc: timeout.high_qc.as_ref().map(|message| {
                    let wire = message_to_wire(message, &mut cuts, &mut cut_index);
                    intern_evidence(&mut evidence, &mut evidence_index, wire)
                }),
                high_prop: timeout.high_prop.as_ref().map(|message| {
                    let wire = message_to_wire(message, &mut cuts, &mut cut_index);
                    intern_evidence(&mut evidence, &mut evidence_index, wire)
                }),
            })
            .collect();
        TCWire {
            slot: self.slot,
            view: self.view,
            cuts,
            evidence,
            timeouts,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TC {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = TCWire::deserialize(deserializer)?;
        let reference_budget = wire.timeouts.len().saturating_mul(2);
        if wire.evidence.len() > reference_budget || wire.cuts.len() > reference_budget {
            return Err(serde::de::Error::custom(
                "timeout certificate carries unreferenced evidence",
            ));
        }
        let timeouts = wire
            .timeouts
            .iter()
            .map(|lite| {
                let rebuild = |index: Option<u16>| {
                    index
                        .map(|index| {
                            let message = wire.evidence.get(index as usize).ok_or_else(|| {
                                "dangling evidence index in a timeout certificate".to_string()
                            })?;
                            message_from_wire(message, &wire.cuts)
                        })
                        .transpose()
                };
                Ok(Timeout {
                    slot: lite.slot,
                    view: lite.view,
                    high_qc: rebuild(lite.high_qc)?,
                    high_prop: rebuild(lite.high_prop)?,
                    author: lite.author,
                    signature: lite.signature.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(serde::de::Error::custom)?;
        Ok(TC {
            slot: wire.slot,
            view: wire.view,
            timeouts,
        })
    }
}

#[cfg(test)]
mod autobahn_alignment_tests {
    use super::*;

    fn poa_with_votes(count: usize) -> (Committee, Certificate) {
        let committee = crate::common::committee();
        let header = crate::common::header();
        let mut certificate = crate::common::certificate(&header);
        certificate.votes.truncate(count);
        (committee, certificate)
    }

    #[test]
    fn poa_uses_f_plus_one_not_consensus_quorum() {
        let (committee, enough) = poa_with_votes(2);
        assert_eq!(committee.validity_threshold(), 2);
        assert_eq!(committee.quorum_threshold(), 3);
        assert!(enough.verify(&committee).is_ok());

        let (_, too_small) = poa_with_votes(1);
        assert!(too_small.verify(&committee).is_err());
    }

    #[test]
    fn lane_proof_cannot_be_transplanted_to_another_author() {
        let (committee, certificate) = poa_with_votes(2);
        let proposal = Proposal::certified(certificate.clone());
        assert_eq!(
            proposal.verify(&certificate.author, &committee).unwrap(),
            ProposalKind::Certified
        );
        let other = committee
            .authorities
            .keys()
            .copied()
            .find(|author| *author != certificate.author)
            .unwrap();
        assert!(proposal.verify(&other, &committee).is_err());
    }

    #[test]
    fn genesis_proof_requires_a_committee_lane() {
        let committee = crate::common::committee();
        let outsider = PublicKey([91; 32]);
        assert!(Certificate::genesis_for(outsider, &committee)
            .verify(&committee)
            .is_err());
    }

    fn signed_timeout(author_index: usize, slot: Slot, view: View) -> Timeout {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let cut = Header::genesis_proposals(&committee);
        let prepare_id = prepare_digest(slot, view, &cut);
        let votes: Vec<_> = keys
            .iter()
            .take(committee.quorum_threshold() as usize)
            .map(|(author, secret)| (*author, Signature::new(&prepare_id, secret)))
            .collect();
        let high_prop = ConsensusMessage::Prepare {
            slot,
            view,
            tc: None,
            qc_ticket: None,
            proposals: cut.clone(),
        };
        let high_qc = ConsensusMessage::Confirm {
            slot,
            view,
            qc: QC {
                id: prepare_id,
                votes,
                ..Default::default()
            },
            proposals: cut,
        };
        let (author, secret) = &keys[author_index];
        Timeout::new_from_key(Some(high_prop), Some(high_qc), slot, view, *author, secret)
    }

    #[test]
    fn timeout_and_tc_wire_roundtrip_is_exact() {
        let committee = crate::common::committee();
        let timeout = signed_timeout(0, 7, 2);
        let bytes = bincode::serialize(&timeout).unwrap();
        let back: Timeout = bincode::deserialize(&bytes).unwrap();
        assert_eq!(timeout.digest(), back.digest());
        assert!(back.verify(&committee).is_ok());

        let timeouts: Vec<_> = (0..3).map(|i| signed_timeout(i, 7, 2)).collect();
        let tc = TC::new(&committee, 7, 2, timeouts);
        let bytes = bincode::serialize(&tc).unwrap();
        let back: TC = bincode::deserialize(&bytes).unwrap();
        assert_eq!(tc, back);
        assert_eq!(
            tc.verify(&committee).is_ok(),
            back.verify(&committee).is_ok()
        );
        for (original, rebuilt) in tc.timeouts.iter().zip(back.timeouts.iter()) {
            let (
                Some(ConsensusMessage::Confirm { qc: a, .. }),
                Some(ConsensusMessage::Confirm { qc: b, .. }),
            ) = (&original.high_qc, &rebuilt.high_qc)
            else {
                panic!("round trip changed the evidence variant");
            };
            assert_eq!(a.digest(), b.digest());
        }
    }

    /// The 2f+1 timeouts of an honest TC cite the same evidence; the wire
    /// encoding must carry the shared cut once, not once per timeout.
    #[test]
    fn tc_wire_encoding_deduplicates_shared_evidence() {
        let committee = crate::common::committee();
        let one = bincode::serialize(&signed_timeout(0, 7, 2)).unwrap().len();
        let timeouts: Vec<_> = (0..3).map(|i| signed_timeout(i, 7, 2)).collect();
        let tc = bincode::serialize(&TC::new(&committee, 7, 2, timeouts))
            .unwrap()
            .len();
        assert!(
            tc < one + one / 2,
            "three timeouts encoded to {} bytes, one alone is {}",
            tc,
            one
        );
    }

    #[test]
    fn tc_wire_rejects_dangling_and_unreferenced_evidence() {
        // Mirror of the private wire layout (bincode is positional).
        #[derive(Serialize)]
        struct LiteMirror {
            slot: Slot,
            view: View,
            author: PublicKey,
            signature: Signature,
            high_qc: Option<u16>,
            high_prop: Option<u16>,
        }
        #[derive(Serialize)]
        struct TCMirror {
            slot: Slot,
            view: View,
            cuts: Vec<HashMap<PublicKey, Proposal>>,
            evidence: Vec<u8>, // empty vec encodes identically for any element type
            timeouts: Vec<LiteMirror>,
        }

        let dangling = TCMirror {
            slot: 7,
            view: 2,
            cuts: Vec::new(),
            evidence: Vec::new(),
            timeouts: vec![LiteMirror {
                slot: 7,
                view: 2,
                author: PublicKey::default(),
                signature: Signature::default(),
                high_qc: Some(3),
                high_prop: None,
            }],
        };
        let bytes = bincode::serialize(&dangling).unwrap();
        assert!(bincode::deserialize::<TC>(&bytes).is_err());

        let unreferenced = TCMirror {
            slot: 7,
            view: 2,
            cuts: vec![HashMap::new()],
            evidence: Vec::new(),
            timeouts: Vec::new(),
        };
        let bytes = bincode::serialize(&unreferenced).unwrap();
        assert!(bincode::deserialize::<TC>(&bytes).is_err());
    }

    /// The digests must stay byte-identical to the original formulation that
    /// hashed each signature through `bincode::serialize`.
    #[test]
    fn evidence_and_qc_digests_match_the_bincode_formulation() {
        let (_, certificate) = poa_with_votes(2);
        let mut votes: Vec<_> = certificate.votes.iter().collect();
        votes.sort_unstable_by_key(|(author, _)| *author);
        let mut hasher = Blake3Hasher::new();
        hasher.update(b"autobahn-poa-v1");
        hasher.update(&certificate.author.0);
        hasher.update(&certificate.height.to_le_bytes());
        hasher.update(&certificate.header_digest.0);
        hasher.update(&(votes.len() as u64).to_le_bytes());
        for (author, signature) in &votes {
            hasher.update(&author.0);
            let encoded = bincode::serialize(signature).unwrap();
            hasher.update(&(encoded.len() as u64).to_le_bytes());
            hasher.update(&encoded);
        }
        assert_eq!(
            certificate.evidence_digest(),
            Digest(hasher.finalize().into())
        );

        let qc = QC {
            id: Digest([5; 32]),
            votes: certificate.votes.clone(),
            ..Default::default()
        };
        let mut votes: Vec<_> = qc.votes.iter().collect();
        votes.sort_unstable_by_key(|(author, _)| *author);
        let mut hasher = Blake3Hasher::new();
        hasher.update(b"autobahn-qc-v1");
        hasher.update(&qc.id.0);
        hasher.update(&(votes.len() as u64).to_le_bytes());
        for (author, signature) in &votes {
            hasher.update(&author.0);
            let encoded = bincode::serialize(signature).unwrap();
            hasher.update(&(encoded.len() as u64).to_le_bytes());
            hasher.update(&encoded);
        }
        assert_eq!(qc.digest(), Digest(hasher.finalize().into()));
    }

    #[test]
    fn cut_digest_is_canonical_across_hashmap_order() {
        let committee = crate::common::committee();
        let first = Header::genesis_proposals(&committee);
        let mut entries: Vec<_> = first.clone().into_iter().collect();
        entries.reverse();
        let second: HashMap<_, _> = entries.into_iter().collect();
        assert_eq!(proposals_digest(&first), proposals_digest(&second));
        assert_eq!(prepare_digest(7, 3, &first), prepare_digest(7, 3, &second));
    }

    #[test]
    fn proof_free_simpleit_coordinate_keeps_its_legacy_digest() {
        let proposal = Proposal {
            header_digest: Digest([17; 32]),
            height: 9,
            poa: None,
            ..Default::default()
        };
        let mut expected = Blake3Hasher::new();
        expected.update(&proposal.header_digest.0);
        expected.update(&proposal.height.to_le_bytes());
        assert_eq!(proposal.digest(), Digest(expected.finalize().into()));
    }

    #[test]
    fn direct_consensus_vote_rejects_non_members() {
        let committee = crate::common::committee();
        let vote = ConsensusVote {
            author: PublicKey([91; 32]),
            slot: 3,
            digest: Digest([27; 32]),
            sig: Signature::default(),
        };
        assert!(vote.verify(&committee).is_err());
    }

    #[test]
    fn prepare_qc_is_bound_to_the_complete_cut() {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let cut = Header::genesis_proposals(&committee);
        let prepare_id = prepare_digest(7, 2, &cut);
        let votes = keys
            .iter()
            .take(committee.quorum_threshold() as usize)
            .map(|(author, secret)| (*author, Signature::new(&prepare_id, secret)))
            .collect();
        let qc = QC {
            id: prepare_id,
            votes,
            ..Default::default()
        };
        let confirm = ConsensusMessage::Confirm {
            slot: 7,
            view: 2,
            qc: qc.clone(),
            proposals: cut.clone(),
        };
        assert!(verify_confirm(&confirm, &committee));

        let mut changed = cut;
        let lane = *changed.keys().next().unwrap();
        let bumped = Proposal {
            header_digest: changed[&lane].header_digest.clone(),
            height: changed[&lane].height + 1,
            poa: changed[&lane].poa.clone(),
            ..Default::default()
        };
        changed.insert(lane, bumped);
        let forged = ConsensusMessage::Confirm {
            slot: 7,
            view: 2,
            qc,
            proposals: changed,
        };
        assert!(!verify_confirm(&forged, &committee));
    }

    #[test]
    fn ride_shared_consensus_is_bound_by_the_car_signature_digest() {
        let committee = crate::common::committee();
        let mut header = crate::common::header();
        let prepare = ConsensusMessage::Prepare {
            slot: 1,
            view: 1,
            tc: None,
            qc_ticket: None,
            proposals: Header::genesis_proposals(&committee),
        };
        header
            .consensus_messages
            .insert(prepare.digest(), prepare.clone());
        header.num_active_instances = 1;
        let with_prepare = header.digest();
        header.consensus_messages.clear();
        assert_ne!(with_prepare, header.digest());
    }

    #[test]
    fn ride_shared_map_keys_and_active_count_are_canonical() {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let (author, secret) = keys.into_iter().next().unwrap();
        let prepare = ConsensusMessage::Prepare {
            slot: 1,
            view: 1,
            tc: None,
            qc_ticket: None,
            proposals: Header::genesis_proposals(&committee),
        };
        let mut header = Header {
            author,
            height: 1,
            consensus_messages: HashMap::from([(prepare.digest(), prepare.clone())]),
            num_active_instances: 1,
            signature: Some(Signature::default()),
            ..Header::default()
        };
        header.id = header.digest();
        header.signature = Some(Signature::new(&header.id, &secret));
        assert!(header.verify(&committee).is_ok());

        header.consensus_messages = HashMap::from([(Digest([88; 32]), prepare)]);
        assert_eq!(header.digest(), header.id, "map keys are not wire values");
        assert!(header.verify(&committee).is_err());

        header.consensus_messages.clear();
        header.num_active_instances = 1;
        header.id = header.digest();
        header.signature = Some(Signature::new(&header.id, &secret));
        assert!(header.verify(&committee).is_err());
    }

    #[test]
    fn timeout_high_qc_must_be_a_prepare_qc_not_a_commit_qc() {
        let committee = crate::common::committee();
        let (author, secret) = crate::common::keys().into_iter().next().unwrap();
        let commit = ConsensusMessage::Commit {
            slot: 4,
            view: 2,
            qc: QC::default(),
            proposals: Header::genesis_proposals(&committee),
        };
        let timeout = Timeout::new_from_key(None, Some(commit), 4, 2, author, &secret);
        assert!(timeout.verify(&committee).is_err());
    }

    #[test]
    fn tc_selects_f_plus_one_matching_prepare_reports() {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let proposals = Header::genesis_proposals(&committee);
        let prepare = ConsensusMessage::Prepare {
            slot: 9,
            view: 3,
            tc: None,
            qc_ticket: None,
            proposals: proposals.clone(),
        };
        let timeouts = keys
            .iter()
            .take(committee.validity_threshold() as usize)
            .map(|(author, _)| Timeout {
                slot: 9,
                view: 4,
                high_qc: None,
                high_prop: Some(prepare.clone()),
                author: *author,
                signature: Signature::default(),
            })
            .collect();
        let tc = TC::new(&committee, 9, 4, timeouts);
        let (winner, sources) = tc.get_winning_proposal(&committee).unwrap();
        assert_eq!(winner, proposals);
        assert_eq!(sources.len() as u32, committee.validity_threshold());
    }

    #[test]
    fn tc_compares_qc_and_prepare_views_and_gives_qc_the_tie() {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let qc_cut = Header::genesis_proposals(&committee);
        let mut prepare_cut = qc_cut.clone();
        let lane = *prepare_cut.keys().next().unwrap();
        prepare_cut.get_mut(&lane).unwrap().header_digest = Digest([33; 32]);

        let high_prepare = ConsensusMessage::Prepare {
            slot: 11,
            view: 3,
            tc: None,
            qc_ticket: None,
            proposals: prepare_cut.clone(),
        };
        let high_qc = |view| ConsensusMessage::Confirm {
            slot: 11,
            view,
            qc: QC::default(),
            proposals: qc_cut.clone(),
        };
        let make_tc = |qc_view| {
            TC::new(
                &committee,
                11,
                4,
                keys.iter()
                    .take(committee.validity_threshold() as usize)
                    .enumerate()
                    .map(|(index, (author, _))| Timeout {
                        slot: 11,
                        view: 4,
                        high_qc: (index == 0).then(|| high_qc(qc_view)),
                        high_prop: Some(high_prepare.clone()),
                        author: *author,
                        signature: Signature::default(),
                    })
                    .collect(),
            )
        };

        assert_eq!(make_tc(2).get_winning_proposals(&committee), prepare_cut);
        assert_eq!(make_tc(3).get_winning_proposals(&committee), qc_cut);
    }

    #[test]
    fn timeout_digest_binds_reported_evidence() {
        let committee = crate::common::committee();
        let author = *committee.authorities.keys().next().unwrap();
        let mut first = Timeout {
            slot: 4,
            view: 2,
            high_qc: None,
            high_prop: None,
            author,
            signature: Signature::default(),
        };
        let baseline = first.digest();
        first.high_prop = Some(ConsensusMessage::Prepare {
            slot: 4,
            view: 2,
            tc: None,
            qc_ticket: None,
            proposals: Header::genesis_proposals(&committee),
        });
        assert_ne!(baseline, first.digest());
        assert_ne!(
            TC::genesis(&committee),
            TC::new(&committee, 4, 2, vec![first])
        );
    }
}
