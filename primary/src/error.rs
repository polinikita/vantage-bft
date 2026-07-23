// Copyright(C) Facebook, Inc. and its affiliates.
use crate::{primary::{Height, View}, messages::Timeout};
use crypto::{CryptoError, Digest, PublicKey};
use store::StoreError;
use thiserror::Error;

#[macro_export(local_inner_macros)]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}


pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Invalid header id")]
    InvalidHeaderId,

    #[error("Malformed header {0}")]
    MalformedHeader(Digest),

    #[error("Malformed special header {0}")]
    MalformedSpecialHeader(Digest),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(PublicKey),

    #[error("Received unexpected vote fo header {0}")]
    UnexpectedVote(Digest),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Parents of header {0} are not a quorum")]
    HeaderRequiresQuorum(Digest),

    #[error("Header {0} (round {1}) too old")]
    HeaderTooOld(Digest, Height),

    #[error("Vote {0} (round {1}) too old")]
    VoteTooOld(Digest, Height),

    #[error("Certificate {0} (round {1}) too old")]
    CertificateTooOld(Digest, Height),

    #[error("Invalid vote invalidation")]
    InvalidVoteInvalidation,

    #[error("Invalid special parent")]
    InvalidSpecialParent,

    #[error("Slow QC not ready")]
    InvalidSlowQCRequest, //TODO: move to ConsensusError

    #[error("Do not need to process this signature")]
    CarAlreadySatisfied,

    #[error("Wrong QC ticket")]
    InvalidQCTicket,

}



pub type ConsensusResult<T> = Result<T, ConsensusError>;

// clippy::large_enum_variant / clippy::result_large_err: `InvalidTimeout(Timeout)`
// (~560 B) makes this enum, and every `ConsensusResult<T>` return type, large --
// boxing it would touch every construction site (`ConsensusError::X(...)`) and every
// `match`/`if let` arm across the audited Autobahn consensus path for a pure
// stack-size optimization with no functional benefit at this workspace's message
// volumes; not done. Applies to every `ConsensusResult<T>`-returning fn too
// (messages.rs's `verify`/`validate_winning_proposal`), so allowed here rather than
// separately at each call site.
#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Store error: {0}")]
    StoreError(#[from] StoreError),

    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Received more than one vote from {0}")]
    AuthorityReuse(PublicKey),

    #[error("Received vote from unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("Received QC without a quorum")]
    QCRequiresQuorum,

    #[error("Received TC without a quorum")]
    TCRequiresQuorum,

    #[error("Malformed block {0}")]
    MalformedBlock(Digest),

    #[error("Received block {digest} from leader {leader} at view {view}")]
    WrongLeader {
        digest: Digest,
        leader: PublicKey,
        view: View,
    },

    #[error("Invalid payload")]
    InvalidPayload,

    #[error("Cert {0} (view {1}) too old")]
    TooOld(Digest, View),

    #[error("Already voted for Cert {0} (view {1})")]
    AlreadyVoted(Digest, View),

    #[error(transparent)]
    DagError(#[from] DagError),

    #[error("Header proposer != block leader")]
    WrongProposer,

    #[error("Received block for round {0} smaller than current_round {1}")]
    NonMonotonicRounds(Height, Height),

    #[error("Header proposer provided invalid Header")]
    InvalidHeader,

    #[error("Timeout invalid {0:?}")]
    InvalidTimeout(Timeout),

    #[error("Header proposer provided no ticket")]
    InvalidTicket,

    #[error("Parent ticket not committed")]
    UncommittedParentTicket,

    #[error("Header of parent ticket not committed")]
    MissingParentTicketHeader,
        
}
