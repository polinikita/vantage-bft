// Simple-IT cut-consensus wire types.
//
// The protocol counts individually verified votes and commits. No certificate
// message is defined. `CutReady` is used only by the Bracha variant.
use crate::error::{DagError, DagResult};
use crate::messages::Proposal;
use config::Committee;
use crypto::{Blake3Hasher, Digest, Hash, PublicKey};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;

/// Cut-consensus round number.
pub type CutRound = u64;

#[derive(Clone, Serialize, Deserialize)]
pub struct Timeout {
    pub round: CutRound,
    pub author: PublicKey,
}

impl Timeout {
    pub async fn new(round: CutRound, author: PublicKey) -> Self {
        Self { round, author }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        Ok(())
    }
}

impl Hash for Timeout {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.round.to_le_bytes());
        hasher.update(&self.author.0);
        Digest(hasher.finalize().into())
    }
}

impl fmt::Debug for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Timeout: R{}({})", self.round, self.author)
    }
}

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Round {} Timeout by {}", self.round, self.author)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TimeoutAccept {
    pub round: CutRound,
    pub author: PublicKey,
}

impl TimeoutAccept {
    pub fn new(round: CutRound, author: PublicKey) -> Self {
        Self { round, author }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        Ok(())
    }
}

impl Hash for TimeoutAccept {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.round.to_le_bytes());
        hasher.update(&self.author.0);
        Digest(hasher.finalize().into())
    }
}

impl fmt::Debug for TimeoutAccept {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "TimeoutAccept: R{}({})", self.round, self.author)
    }
}

impl fmt::Display for TimeoutAccept {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Round {} TimeoutAccept by {}", self.round, self.author)
    }
}

/// Commit message for a round.
#[derive(Clone, Serialize, Deserialize)]
pub struct Decide {
    pub id: Digest,
    pub round: CutRound,
    pub author: PublicKey,
}

impl Decide {
    /// Creates a commit message.
    pub async fn new(header_id: Digest, round: CutRound, author: &PublicKey) -> Self {
        Self {
            id: header_id,
            round,
            author: *author,
        }
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        Ok(())
    }
}

impl fmt::Debug for Decide {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: D{}({}, {})",
            self.id, self.round, self.author, self.id
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct TimeoutCert {
    pub round: CutRound,
    /// The authors of the timeouts certified here (unsigned protocol: reaching
    /// `quorum_threshold` distinct, committee-recognized authors *is* the
    /// certificate -- cf. `Ack`/`CutVote` elsewhere in this crate).
    pub timeouts: Vec<PublicKey>,
}

impl TimeoutCert {
    pub fn new(round: CutRound) -> Self {
        Self {
            round,
            timeouts: Vec::new(),
        }
    }

    /// Adds a timeout to the certificate.
    pub fn add_timeout(&mut self, author: PublicKey) -> DagResult<()> {
        // Ensure this public key hasn't already submitted a timeout for this round.
        if self.timeouts.contains(&author) {
            return Err(DagError::AuthorityReuse(author));
        }
        self.timeouts.push(author);
        Ok(())
    }

    /// Verifies the timeout certificate against the committee. Requires
    /// `quorum_threshold` (2f+1 at n=3f+1) distinct, committee-recognized authors.
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        let mut weight = 0;

        let mut used = HashSet::new();
        for name in self.timeouts.iter() {
            ensure!(!used.contains(name), DagError::AuthorityReuse(*name));
            let voting_rights = committee.stake(name);
            ensure!(voting_rights > 0, DagError::UnknownAuthority(*name));
            used.insert(*name);
            weight += voting_rights;
        }

        // Check if the accumulated weight meets the quorum threshold.
        ensure!(
            weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        Ok(())
    }
}

/// Snapshot of certified tips across all lanes.
pub type Cut = BTreeMap<PublicKey, Proposal>;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct CutProposal {
    pub round: CutRound,
    pub proposer: PublicKey,
    pub parent_cut: Digest,
    pub tips: Cut,
}

impl CutProposal {
    pub fn id(&self) -> Digest {
        self.digest()
    }

    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the proposer has voting rights.
        ensure!(
            committee.stake(&self.proposer) > 0,
            DagError::UnknownAuthority(self.proposer)
        );
        Ok(())
    }
}

impl Hash for CutProposal {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.round.to_le_bytes());
        hasher.update(&self.proposer.0);
        hasher.update(&self.parent_cut.0);
        for (author, proposal) in &self.tips {
            hasher.update(&author.0);
            hasher.update(&proposal.header_digest.0);
            hasher.update(&proposal.height.to_le_bytes());
        }
        Digest(hasher.finalize().into())
    }
}

impl fmt::Debug for CutProposal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "CutProposal(r={}, proposer={}, parent={}, tips={})",
            self.round,
            self.proposer,
            self.parent_cut,
            self.tips.len()
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct CutVote {
    pub round: CutRound,
    pub cut_id: Digest,
    pub author: PublicKey,
}

impl CutVote {
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        Ok(())
    }
}

impl Hash for CutVote {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.round.to_le_bytes());
        hasher.update(&self.cut_id.0);
        hasher.update(&self.author.0);
        Digest(hasher.finalize().into())
    }
}

/// Bracha variant's second echo message, counted first-hand by each recipient.
#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct CutReady {
    pub round: CutRound,
    pub cut_id: Digest,
    pub author: PublicKey,
}

impl CutReady {
    pub fn verify(&self, committee: &Committee) -> DagResult<()> {
        // Ensure the authority has voting rights.
        ensure!(
            committee.stake(&self.author) > 0,
            DagError::UnknownAuthority(self.author)
        );
        Ok(())
    }
}

impl Hash for CutReady {
    fn digest(&self) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(&self.round.to_le_bytes());
        hasher.update(&self.cut_id.0);
        hasher.update(&self.author.0);
        Digest(hasher.finalize().into())
    }
}
