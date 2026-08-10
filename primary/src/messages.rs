// Copyright(C) Facebook, Inc. and its affiliates.

use crate::error::{ConsensusError, ConsensusResult, DagError, DagResult};
use crate::primary::{Height, Slot, View};
use config::{Committee, WorkerId};
use core::panic;
use crypto::{Blake3Hasher, Digest, Hash, PublicKey, SecretKey, Signature, SignatureService};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Proposal {
    pub header_digest: Digest,
    pub height: Height,
}

impl Proposal {
    pub async fn new(header_digest: Digest, height: Height) -> Self {
        Self {
            header_digest,
            height,
        }
    }
}

impl PartialEq for Proposal {
    fn eq(&self, other: &Self) -> bool {
        self.height == other.height && self.header_digest == other.header_digest
    }
}

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
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.header_digest.0);
        hasher.update(&self.height.to_le_bytes());
        Digest(hasher.finalize().into())
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
            proposals: _,
        } => {
            let mut hasher = Blake3Hasher::new();
            hasher.update(&slot.to_le_bytes());
            hasher.update(&view.to_le_bytes());
            hasher.update(&0_u8.to_le_bytes());
            let prepare_id = Digest(hasher.finalize().into());

            debug!(
                "PrepareIDCheck has slot: {}, view: {}, digest: {}",
                slot, view, prepare_id
            );

            if qc.votes.len() == committee.size() {
                if prepare_id != qc.id {
                    return false;
                }
                qc.verify(committee).is_ok()
            } else {
                // Slow-path Confirm votes include the Prepare identifier.
                let mut hasher = Blake3Hasher::new();
                hasher.update(&slot.to_le_bytes());
                hasher.update(&view.to_le_bytes());
                hasher.update(&prepare_id.0);
                hasher.update(&1_u8.to_le_bytes());
                let confirm_id = Digest(hasher.finalize().into());

                debug!(
                    "ConfirmIDCheck for slot: {}, view: {}, qc_dig {:?} -> has digest: {}",
                    slot, view, prepare_id, confirm_id
                );

                if confirm_id != qc.id {
                    panic!("ids don't match");
                }
                qc.verify(committee).is_ok()
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
            proposals: _,
        } => {
            let mut hasher = Blake3Hasher::new();
            hasher.update(&slot.to_le_bytes());
            hasher.update(&view.to_le_bytes());
            hasher.update(&0_u8.to_le_bytes());
            let prepare_id = Digest(hasher.finalize().into());

            if prepare_id != qc.id {
                return false;
            }

            qc.verify(committee).is_ok()
        }
        _ => false,
    }
}

pub fn proposal_digest(consensus_message: &ConsensusMessage) -> Digest {
    let mut hasher = Blake3Hasher::new();
    match consensus_message {
        ConsensusMessage::Prepare {
            slot: _,
            view: _,
            tc: _,
            qc_ticket: _,
            proposals,
        } => {
            for proposal in proposals.values() {
                hasher.update(&proposal.header_digest.0);
            }
        }
        ConsensusMessage::Confirm {
            slot: _,
            view: _,
            qc: _,
            proposals,
        } => {
            for proposal in proposals.values() {
                hasher.update(&proposal.header_digest.0);
            }
        }
        ConsensusMessage::Commit {
            slot: _,
            view: _,
            qc: _,
            proposals,
        } => {
            for proposal in proposals.values() {
                hasher.update(&proposal.header_digest.0);
            }
        }
    }
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
                proposals: _,
            } => {
                hasher.update(&slot.to_le_bytes());
                hasher.update(&view.to_le_bytes());
                // Prepare variant tag.
                hasher.update(&0_u8.to_le_bytes());
            }
            ConsensusMessage::Confirm {
                slot,
                view,
                qc,
                proposals: _,
            } => {
                hasher.update(&slot.to_le_bytes());
                hasher.update(&view.to_le_bytes());
                hasher.update(&qc.id.0);
                // Confirm variant tag.
                hasher.update(&1_u8.to_le_bytes());
            }
            ConsensusMessage::Commit {
                slot,
                view,
                qc,
                proposals: _,
            } => {
                hasher.update(&slot.to_le_bytes());
                hasher.update(&view.to_le_bytes());
                hasher.update(&qc.id.0);
                // Commit variant tag.
                hasher.update(&2_u8.to_le_bytes());
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
        match self {
            ConsensusMessage::Prepare {
                slot,
                view,
                tc,
                qc_ticket: _,
                proposals,
            } => match other {
                ConsensusMessage::Prepare {
                    slot: other_slot,
                    view: other_view,
                    tc: other_tc,
                    qc_ticket: _other_ticket,
                    proposals: other_proposals,
                } => {
                    slot == other_slot
                        && view == other_view
                        && tc == other_tc
                        && proposals == other_proposals
                }
                _ => false,
            },
            ConsensusMessage::Confirm {
                slot,
                view,
                qc,
                proposals,
            } => match other {
                ConsensusMessage::Confirm {
                    slot: other_slot,
                    view: other_view,
                    qc: other_qc,
                    proposals: other_proposals,
                } => {
                    slot == other_slot
                        && view == other_view
                        && qc == other_qc
                        && proposals == other_proposals
                }
                _ => false,
            },
            ConsensusMessage::Commit {
                slot,
                view,
                qc,
                proposals,
            } => match other {
                ConsensusMessage::Commit {
                    slot: other_slot,
                    view: other_view,
                    qc: other_qc,
                    proposals: other_proposals,
                } => {
                    slot == other_slot
                        && view == other_view
                        && qc == other_qc
                        && proposals == other_proposals
                }
                _ => false,
            },
        }
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
            .map(|pk| {
                (
                    *pk,
                    Proposal {
                        header_digest: Header {
                            author: *pk,
                            ..Self::default()
                        }
                        .digest(),
                        height: 0,
                    },
                )
            })
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
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.id.0);
        hasher.update(&self.height.to_le_bytes());
        Digest(hasher.finalize().into())
    }
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
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities
            .keys()
            .map(|name| Self {
                header_digest: Header {
                    author: *name,
                    ..Header::genesis(committee)
                }
                .digest(),
                author: *name,
                ..Self::default()
            })
            .collect()
    }

    pub fn genesis_cert(committee: &Committee) -> Self {
        Self {
            header_digest: Header::genesis(committee).digest(),
            ..Self::default()
        }
    }

    pub fn genesis_certs(committee: &Committee) -> HashMap<PublicKey, Self> {
        committee
            .authorities
            .keys()
            .map(|name| {
                (
                    *name,
                    Self {
                        header_digest: Header {
                            author: *name,
                            ..Header::genesis(committee)
                        }
                        .digest(),
                        author: *name,
                        ..Self::default()
                    },
                )
            })
            .collect()
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Genesis certificates are always valid.
        if Self::genesis(committee).contains(self) {
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
            weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        let mut digests = Vec::with_capacity(self.votes.len());
        for _ in &self.votes {
            let mut hasher = Blake3Hasher::new();
            hasher.update(&self.header_digest.0);
            hasher.update(&self.height().to_le_bytes());
            digests.push(Digest(hasher.finalize().into()));
        }
        Signature::verify_batch_multi(&digests, &self.votes).map_err(DagError::from)
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
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.header_digest.0);
        hasher.update(&self.height().to_le_bytes());

        Digest(hasher.finalize().into())
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
        let mut ret = self.header_digest == other.header_digest;
        ret &= self.height() == other.height();
        ret
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub id: Digest,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis(_committee: &Committee) -> Self {
        QC::default()
    }

    #[allow(clippy::result_large_err)]
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
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
        ensure!(
            weight >= committee.quorum_threshold(),
            ConsensusError::QCRequiresQuorum
        );

        Signature::verify_batch(&self.id, &self.votes).map_err(ConsensusError::from)
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let hasher = Blake3Hasher::new();
        Digest(hasher.finalize().into())
    }
}

impl fmt::Debug for QC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "QC({}, {})", 1, 1)
    }
}

impl PartialEq for QC {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Clone, Serialize, Deserialize)]
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
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        self.signature.verify(&self.digest(), &self.author)?;

        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let hasher = Blake3Hasher::new();
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

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct TC {
    pub slot: Slot,
    pub view: View,
    pub timeouts: Vec<Timeout>,
}

impl PartialEq for TC {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

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

    pub fn get_winning_proposals(&self, committee: &Committee) -> HashMap<PublicKey, Proposal> {
        let mut winning_proposals = HashMap::new();
        let mut winning_view = 0;
        let mut prepared_feq: HashMap<Digest, u32> = HashMap::new();

        // Prefer the highest justified proposal set.
        for timeout in &self.timeouts {
            if let Some(qc) = &timeout.high_qc {
                match qc {
                    ConsensusMessage::Confirm {
                        slot: _,
                        view: other_view,
                        qc: _,
                        proposals,
                    } if other_view > &winning_view => {
                        winning_view = timeout.view;
                        winning_proposals = proposals.clone();
                    }

                    ConsensusMessage::Commit {
                        slot: _,
                        view: _,
                        qc: _,
                        proposals,
                    } => {
                        winning_proposals = proposals.clone();
                        break;
                    }

                    _ => {}
                }
            };
            if let Some(prepare) = &timeout.high_prop {
                if let ConsensusMessage::Prepare {
                    slot: _,
                    view,
                    tc: _,
                    qc_ticket: _,
                    proposals,
                } = prepare
                {
                    if view > &winning_view {
                        let weight = prepared_feq.entry(prepare.digest()).or_default();
                        *weight += committee.stake(&timeout.author);

                        if *weight >= committee.validity_threshold() {
                            winning_view = *view;
                            winning_proposals = proposals.clone();
                        }
                    }
                }
            }
        }
        winning_proposals
    }

    #[allow(clippy::result_large_err)]
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        if Self::genesis(committee) == *self {
            return Ok(());
        }

        let mut weight = 0;
        let mut used = HashSet::new();
        for timeout in self.timeouts.iter() {
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
            timeout.verify(committee)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TC({}, {:?})", self.slot, self.view)
    }
}
