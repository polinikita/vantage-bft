// Copyright(C) Facebook, Inc. and its affiliates.

use crate::error::{ConsensusError, ConsensusResult, DagError, DagResult};
use crate::primary::{Height, Slot, View};
use config::{Committee, Stake, WorkerId};
use core::panic;
use crypto::{Blake3Hasher, Digest, Hash, PublicKey, SecretKey, Signature, SignatureService};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

///////////
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

#[derive(Clone, Serialize, Deserialize)]
pub enum ConsensusType {
    Prepare,
    Confirm,
    Commit,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct CommitQC {
    pub slot: Slot,
    view: View,
    qc: QC, //ConfirmQC
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
        tc: Option<TC>, //TC for previous view; if None => must have ticket for previous slot
        qc_ticket: Option<CommitQC>,
        proposals: HashMap<PublicKey, Proposal>,
    },
    Confirm {
        slot: Slot,
        view: View,
        qc: QC, //PrepareQC
        proposals: HashMap<PublicKey, Proposal>,
    },
    Commit {
        slot: Slot,
        view: View,
        qc: QC, //ConfirmQC
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
                //verify fast QC (3f+1 Prepare Votes)
                //Alternatively could check stake if stake is full
                //Check ID
                if prepare_id != qc.id {
                    return false;
                }
                //Check Sigs.
                qc.verify(committee).is_ok()
            } else {
                //SlowQC only has 2f+1 Confirm Votes
                // Confirm votes hash the derived prepare identifier.
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
                //Check Sigs.
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
            //Check ID
            let mut hasher = Blake3Hasher::new();
            hasher.update(&slot.to_le_bytes());
            hasher.update(&view.to_le_bytes());
            hasher.update(&0_u8.to_le_bytes());
            let prepare_id = Digest(hasher.finalize().into());

            if prepare_id != qc.id {
                return false;
            }

            //Check Sigs.
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
                //hasher.update(tc.digest().0);
                // NOTE: Indicates a prepare message
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
                //hasher.update(qc.digest().0);
                // NOTE: Indicates a confirm message
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
                //hasher.update(qc.digest().0);
                // NOTE: Indicates a commit message
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
    // `None` on the Vantage path; Vantage carries no
    // signatures anywhere. Autobahn constructors always set `Some(sig)`;
    // `Header::verify` (the only reader) requires presence.
    pub signature: Option<Signature>,
    // Vantage only: `Some(sid)` binds a block to its
    // session; `None` on both Autobahn paths, where it is meaningless.
    pub sid: Option<Digest>,

    // Consensus metadata
    pub consensus_messages: HashMap<Digest, ConsensusMessage>,
    pub num_active_instances: usize, //Number of Prepare/Confirm messages
    pub special: bool,               //Trying out special car
}

//NOTE: A header is special if "is_special = true". It contains a view, prev_view_round, and its parents may be just a single edge -- a Digest of its parent header (notably not of a Cert)
// Special headers currently do not need to carry the QC/TC to justify their ticket -- we keep that at the consensu layer. The view and prev_view_round references the relevant QC/TC.
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

    /// Vantage constructor without a signature service or consensus state.
    /// metadata, `parent_cert` is an empty-votes certificate acting as a pure predecessor
    /// pointer (never `Certificate::verify`d on this path -- N2's chain check is a plain
    /// height+digest match, see `vantage::block::block_ok`). `prev_digest` is the
    /// predecessor's block hash, or the session's genesis digest at height 1.
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

    //Note: This is essentially equivalent to Header::default() but with an author name. ==> Currently no difference in functionality; can use them interchangeably
    //genesis.digest() == default.digest() because we currently don't compute the digest based off the author (we just use ..Self::default)
    //Purpose: The construct provides easier compatibility for modifications. I.e. if one wants to change genesis Header, genesis QC etc. will adapt automatically
    pub fn genesis(committee: &Committee) -> Self {
        let (name, _) = committee.authorities.iter().next().unwrap();
        Header {
            author: *name,
            //parents: Certificate::genesis(committee).iter().map(|x| x.digest()).collect(), //Note: Can't use these parents, because both parents and current header would be in round 0 => malformed
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
        // Ensure the header id is well formed.
        ensure!(self.digest() == self.id, DagError::InvalidHeaderId);

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(self.author));

        // Ensure all worker ids are correct.
        for worker_id in self.payload.values() {
            committee
                .worker(&self.author, worker_id)
                .map_err(|_| DagError::MalformedHeader(self.id.clone()))?;
        }

        // Check the signature (Autobahn paths only -- always `Some`; the vantage path
        // never calls this, it uses `vantage::block::block_ok` instead, which requires
        // `signature.is_none()`).
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
        hasher.update(&self.parent_cert.header_digest.0); //Need to hash the chain parent(?)
                                                          //hasher.update(&self.parent_cert);

        // Fold the new `sid` option injectively (presence byte, then content for `Some`)
        // after the existing fields so Autobahn's
        // digest is unaffected in shape (only in value, since every Autobahn header now
        // also folds a `0` byte for `sid: None`). `signature` stays outside the digest,
        // as before.
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

/// Vantage-only: the protocol's unsigned
/// `⟨ack, a, k, h⟩`. `sender` is the acking party's declared identity -- trusted the same
/// way `HeadersRequest`'s `requestor` already is (D4); first-hand counting keys on the
/// *channel* sender at the receiving end (network dispatch), not this field alone, but
/// the field must still be carried so the receiver knows whose channel it should have
/// arrived on and so tests can construct/inspect acks without a live network.
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

    /// The exact tuple `(a, k, h)` N3/N4 key acks by (sender is not part of the tuple --
    /// it is the *value* counted per tuple, one distinct sender at a time).
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
    //Signed wrapper around ONE ConsensusMessage
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
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
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
    pub digest: Digest, //This uniquely captures the (slot,view,proposal) of ConsensusMessage we are voting for. => Can simply sign this, don't need to take Digest of CV
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
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
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
    pub id: Digest, //the header we are voting for.
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
            //consensus_instance: None,
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature
            .verify(&self.digest(), &self.author)
            .map_err(DagError::from)
    }
}

impl Hash for Vote {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        // hasher.update(&self.id);
        // hasher.update(self.view.to_le_bytes());
        hasher.update(&self.id.0);
        hasher.update(&self.height.to_le_bytes());
        //hasher.update(&self.origin);
        //hasher.update(self.special_valid.to_le_bytes());
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
            //consensus_instance: None,
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
                    ..Header::genesis(committee) //..Header::default()
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
                            ..Header::genesis(committee) //..Header::default()
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
        // Check the embedded header.

        // Ensure the certificate has a quorum.
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

        // Check the signatures.

        // Verify each vote against its header digest.
        if false {
            Signature::verify_batch(&self.verifiable_digest(), &self.votes).map_err(DagError::from)
        } else {
            // Compute and verify each vote digest.
            let mut digests = Vec::new();
            for _ in self.votes.iter() {
                digests.push({
                    let mut hasher = Blake3Hasher::new();
                    hasher.update(&self.header_digest.0);
                    hasher.update(&self.height().to_le_bytes());
                    Digest(hasher.finalize().into())
                })
                //Check special valid.
                //Does one still need to check  QC? Or can one trust cert?  ==> Yes, because only invalid ones need proof => invalid = not forwarded to consensus. For Dag layer makes no difference.
                // If a byz leader doesn't want to forward to consensus.. thats fine.. same as timing out.
            }
            Signature::verify_batch_multi(&digests, &self.votes).map_err(DagError::from)
        }
    }

    pub fn height(&self) -> Height {
        self.height
    }

    pub fn origin(&self) -> PublicKey {
        self.author
    }

    pub fn verifiable_digest(&self) -> Digest {
        if false {
            //matching_valids(&self.special_valids) {
            let mut hasher = Blake3Hasher::new();
            hasher.update(&self.header_digest.0);
            hasher.update(&self.height().to_le_bytes());
            //hasher.update(&self.origin());
            //hasher.update(&self.special_valids[0].to_le_bytes());
            Digest(hasher.finalize().into())
        } else {
            panic!("This verfiable digest branch should never be used");
            /*let mut hasher = Sha512::new();
            hasher.update(&self.header.id);
            hasher.update(self.round().to_le_bytes());
            hasher.update(&self.origin());*/
            /*for i in &self.special_valids {
                hasher.update(i.to_le_bytes());
            }*/
            //Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
        }
    }

    fn valid_weight(&self, committee: &Committee) -> Stake {
        self.votes
            .iter()
            .map(|(author, _)| {
                committee.stake(author) * (1 /*self.special_valids[i]*/ as Stake)
            })
            .sum()
    }

    pub fn is_special_valid(&self, committee: &Committee) -> bool {
        debug!("Special_valid weight: {}", self.valid_weight(committee));
        self.valid_weight(committee) >= committee.quorum_threshold()
    }
    pub fn is_special_fast(&self, committee: &Committee) -> bool {
        self.valid_weight(committee) >= committee.fast_threshold()
    }
}

pub fn matching_valids(vec: &[u8]) -> bool {
    vec.iter().min() == vec.iter().max()
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
            /*self.header
                .parent_cert
                .iter()
                .map(|x| format!("{}", x))
                .collect::<Vec<_>>(),
            if self.header.special_parent.is_some() {self.header.special_parent.clone().unwrap()} else {Digest::default()},
            self.header.view,*/
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header_digest == other.header_digest;
        ret &= self.height() == other.height();
        //ret &= self.origin() == other.origin();
        ret
    }
}

/*#[derive(Serialize, Deserialize, Default, Clone)]
pub struct Block {
    pub qc: QC, // QC is equivalent to Commit Certificate in our terminology. Certificate is equivalent to Vote-QC in our terminology
    pub tc: Option<TC>,
    pub author: PublicKey,
    pub view: View,
    pub payload: Vec<Header>, // Change this to be the payload of a header (vector of digests representing mini-batches)
    pub signature: Signature,
}

impl Block {
    pub async fn new(
        qc: QC,
        tc: Option<TC>,
        author: PublicKey,
        view: View,
        payload: Vec<Header>,
        mut signature_service: SignatureService,
    ) -> Self {
        let block = Self {
            qc,
            tc,
            author,
            view,
            payload,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(block.digest()).await;
        Self { signature, ..block }
    }

    pub fn genesis() -> Self {
        Block::default()
    }

    pub fn parent(&self) -> &Digest {
        &self.qc.hash
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake(&self.author);
        ensure!(
            voting_rights > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        // Check the embedded QC.
        if self.qc != QC::genesis(committee) {
            self.qc.verify(committee)?;
        }

        // Check the TC embedded in the block (if any).
        if let Some(ref tc) = self.tc {
            tc.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for Block {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(self.author.0);
        hasher.update(self.view.to_le_bytes());
        for x in &self.payload {
            hasher.update(&x.id);
        }
        hasher.update(&self.qc.hash);
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: HSB({}, {}, {:?}, {})",
            self.digest(),
            self.author,
            self.view,
            self.qc,
            self.payload.len(),
        )
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "HSB{}", self.view)
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl Block {
    pub fn new_from_key(
        qc: QC,
        author: PublicKey,
        view: View,
        payload: Vec<Header>,
        secret: &SecretKey,
    ) -> Block {
        let block = Block {
            qc,
            tc: None,
            author,
            view,
            payload,
            signature: Signature::default(),
        };
        let signature = Signature::new(&block.digest(), secret);
        Self { signature, ..block }
    }
}




#[derive(Clone, Serialize, Deserialize)]
pub struct AcceptVote {
    pub hash: Digest,
    pub view: View,
    pub view_round: Height,
    pub author: PublicKey,
    pub signature: Signature,
}

impl AcceptVote {
    pub async fn new(
        header: &Header,
        author: PublicKey,
        mut signature_service: SignatureService,
    ) -> Self {
        let vote = Self {
            hash: header.id.clone(),
            view: header.consensus_info.clone().unwrap().view,
            view_round: header.height,
            author,
            signature: Signature::default(),
        };
        let signature = signature_service.request_signature(vote.digest()).await;
        Self { signature, ..vote }
    }

    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            ConsensusError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;
        Ok(())
    }
}

impl AcceptVote {
    pub fn new_from_key(id: Digest, view: View, round: Height, author: PublicKey, secret: &SecretKey) -> Self {
        let vote = AcceptVote {
            hash: id.clone(),
            view: view,
            view_round: round,
            author,
            signature: Signature::default(),
        };
        let signature = Signature::new(&vote.digest(), &secret);
        Self { signature, ..vote }
    }
}

impl PartialEq for AcceptVote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}


impl Hash for AcceptVote {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(&self.hash);
        hasher.update(self.view.to_le_bytes());
        hasher.update(self.view_round.to_le_bytes());
        Digest(hasher.finalize().as_slice()[..32].try_into().unwrap())
    }
}

impl fmt::Debug for AcceptVote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "V({}, {}, {})", self.author, self.view, self.hash)
    }
}*/

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct QC {
    pub id: Digest,
    pub votes: Vec<(PublicKey, Signature)>,
}

impl QC {
    pub fn genesis(_committee: &Committee) -> Self {
        QC::default()
    }

    // clippy::result_large_err: see error.rs's `ConsensusError` doc comment.
    #[allow(clippy::result_large_err)]
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        //genesis QC always valid
        if Self::genesis(committee) == *self {
            return Ok(());
        }

        // Ensure the QC has a quorum.
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

        // Check the signatures.
        Signature::verify_batch(&self.id, &self.votes).map_err(ConsensusError::from)
    }
}

impl Hash for QC {
    fn digest(&self) -> Digest {
        let hasher = Blake3Hasher::new(); //NOTE: We are not using this digest ever currently. QC verification happens on the ID included in the QC
                                          //hasher.update(&self.hash);
                                          //hasher.update(self.view.to_le_bytes());
                                          //hasher.update(self.view_round.to_le_bytes());
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
    // The slot and view the timeout is for
    pub slot: Slot,
    pub view: View,
    // The highest qc the replica has for its state
    pub high_qc: Option<ConsensusMessage>,   //Confirm message
    pub high_prop: Option<ConsensusMessage>, //Prepare message

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
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );

        // Check the signature.
        self.signature.verify(&self.digest(), &self.author)?;

        //NOTE: When verifying TC, we have purged all vote contents besides the winner --> so this step is skipped. Verification is only necessary for the winning proposal

        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let hasher = Blake3Hasher::new();
        /*hasher.update(self.view.to_le_bytes());
        if let Some(qc_view) = self.vote_high_qc {
            hasher.update(qc_view.to_le_bytes());
        }*/

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
        //*self.winning_proposal == *other.winning_proposal
        true
    }
}

impl TC {
    pub fn new(_committee: &Committee, slot: Slot, view: View, timeouts: Vec<Timeout>) -> Self {
        TC {
            slot,
            view,
            timeouts,
        }
        //tc.determine_winning_proposal(committee)
    }

    pub fn genesis(committee: &Committee) -> Self {
        //QC::default()
        let _genesis_header = Header::genesis(committee);
        TC {
            //hash: genesis_header.id,
            //view: genesis_header.consensus_info.unwrap().view,
            //winning_proposal: Box::new((Some(genesis_header), None, None)),
            //view_round: genesis_header.round,
            ..TC::default()
        }
    }

    pub fn get_winning_proposals(&self, committee: &Committee) -> HashMap<PublicKey, Proposal> {
        let mut winning_proposals = HashMap::new();
        let mut winning_view = 0;
        let mut prepared_feq: HashMap<Digest, u32> = HashMap::new();

        // Find the timeout message containing the highest QC, and use that as the winning
        // proposal for the view change
        for timeout in &self.timeouts {
            if let Some(qc) = &timeout.high_qc {
                match qc {
                    ConsensusMessage::Confirm {
                        slot: _,
                        view: other_view,
                        qc: _,
                        proposals,
                    }
                        // Update the highest QC view if we see a higher one
                        if other_view > &winning_view => {
                            winning_view = timeout.view;
                            winning_proposals = proposals.clone();
                        }

                    ConsensusMessage::Commit {
                        slot: _,
                        view: _,
                        qc: _,
                        proposals,
                    } => {
                        // Adopt the proposals of a commit qc
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
                            // Keep the highest-view proposal set with sufficient weight.
                            winning_proposals = proposals.clone();
                        }
                    }
                }
            }
        }
        winning_proposals
    }

    pub fn determine_winning_proposal(self, _committee: &Committee) -> Self {
        self
    }

    pub fn set_winning_proposal(
        self,
        _header: Option<Header>,
        _cert: Option<Certificate>,
        _qc: Option<Certificate>,
    ) -> Self {
        self
    }

    // clippy::result_large_err: see error.rs's `ConsensusError` doc comment.
    #[allow(clippy::result_large_err)]
    pub fn validate_winning_proposal(&self, _committee: &Committee) -> ConsensusResult<()> {
        Ok(())
    }

    // clippy::result_large_err: see error.rs's `ConsensusError` doc comment.
    #[allow(clippy::result_large_err)]
    pub fn verify(&self, committee: &Committee) -> ConsensusResult<()> {
        //genesis TC always valid
        if Self::genesis(committee) == *self {
            return Ok(());
        }

        // Ensure the QC has a quorum.
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

        //Verify each vote
        for timeout in &self.timeouts {
            //timeout.signature.verify(&timeout.digest(), &timeout.author)?; // Check the signatures. (Note: these are only the signatures for the timeout votes, not the signatures for the proposals. We check those in determine/validate winner)
            timeout.verify(committee)?;
        }
        Ok(())
    }

    //Used for debugging: Returns all voted views. 0 by default if no vote was cast for specific type (prepare/accept/qc)
    /*pub fn high_qc_views(&self) -> Vec<(View, View, View)> {
        self.votes.iter().map(|timeout| {
                                    let mut hp_view = 0;
                                    let mut ha_view = 0;
                                    let mut hqc_view = 0;

                                    if let Some((_, view)) = timeout.vote_high_prepare.as_ref() {
                                        hp_view = view.clone()
                                    }
                                    if let Some(view) = timeout.vote_high_accept.as_ref() {
                                        ha_view = view.clone()
                                    }
                                    if let Some(view) = timeout.vote_high_qc.as_ref() {
                                        hqc_view = view.clone()
                                    }

                                    (hp_view, ha_view, hqc_view)
                                })
                            .collect()
    }*/
}

impl fmt::Debug for TC {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TC({}, {:?})", self.slot, self.view)
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Committment {
    pub commit_view: View,
}

impl Hash for Committment {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.commit_view.to_le_bytes());
        Digest(hasher.finalize().into())
    }
}
