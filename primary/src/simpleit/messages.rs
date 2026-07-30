// Simple-IT cut-consensus wire types (stage 1 of a port from the upstream
// `simpleit/Opt-Mempool-Simple-IT-Failure` branch's primary/src/messages.rs).
//
// Ported (exact upstream line ranges noted per type below): `Timeout`, `TimeoutAccept`,
// `Decide`, `TimeoutCert`, `CutProposal`, `CutVote`, plus the `Cut` type alias.
// `Proposal` (upstream messages.rs:692-731) is deliberately NOT redefined here: this
// crate's own `crate::messages::Proposal` already has the identical shape (header
// digest + height) and already hashes with `Blake3Hasher`, so `Cut` below aliases that
// type directly, exactly as upstream aliases its own local `Proposal`.
//
// NOT ported: upstream's `CutCertificate` (upstream messages.rs:813-836). Upstream's
// `CutCertificate::verify` (and this crate's own former copy of it) checks only that
// `votes: Vec<PublicKey>` names distinct, stake-bearing committee members -- never that
// those parties actually voted, since this protocol is signature-free and a vote
// carries no transferable proof of its own authorship. Any party could thus assert a
// certificate for any `cut_id`. The paper this engine implements (arXiv:2606.14404,
// Fig. 2) has no certificate message at all: safety comes from each party counting
// votes/commits FIRST-HAND, never from accepting another party's relayed aggregate --
// see `primary/src/simpleit/engine.rs`'s module doc comment ("FIGURE-2 REWRITE") for
// the full rationale and the replacement mechanism (`CutVoteAggregator` plus
// `CutEngine::mark_cut_safe`, both stage-1/stage-2 respectively).
//
// `Decide` additionally drops upstream's dead `origin: PublicKey` field (AUDIT FIX,
// finding F5): never read anywhere in this crate, and upstream's own single call site
// passes `&self.name` for both `origin` and `author`, so even upstream never
// distinguished the two. `Decide` is this engine's `⟨commit, r⟩` (Fig. 2's Vote step);
// only `id`/`round`/`author` are load-bearing.
//
// Required deviations from upstream (see primary/src/simpleit/mod.rs for the full
// rationale):
//   1. Every `impl Hash` uses `Blake3Hasher`, not `Sha512`. The sequence of fields fed
//      to the hasher, and every `to_le_bytes()` encoding, is otherwise unchanged --
//      only the hash function, and the mechanical `&x.0`-style byte access it requires
//      (matching `crate::messages`/`crate::vantage::block`), differ.
//   2. `CutRound` (`= u64`, defined below) replaces upstream's local `Round` alias
//      (used by `Timeout`/`TimeoutAccept`/`Decide`/`TimeoutCert`) and upstream's bare
//      `u64` (used by `CutProposal`/`CutVote`) uniformly, so there is one unambiguous
//      cut-round type. Neither `primary::primary::Slot` nor `primary::primary::View`
//      is used anywhere in this module, and `CutRound` is deliberately distinct from
//      `crate::vantage::control::Round` (an unrelated control-round counter re-exported
//      from `crate::vantage`).
//   3. None of these six types holds a collection keyed by cut round -- the per-round
//      maps this rule targets (e.g. a future `BTreeMap<CutRound, CutVoteAggregator>`)
//      belong to the not-yet-ported state machine. `Cut` itself (`BTreeMap<PublicKey,
//      Proposal>`) is keyed by author, already a `BTreeMap` upstream, and unchanged.
//   4. See primary/src/simpleit/aggregators.rs for the named-`Committee`-threshold
//      requirement; `TimeoutCert::verify` below uses `quorum_threshold` (2f+1 at
//      n=3f+1), noted inline. `TimeoutCert` (unlike the removed `CutCertificate`) never
//      had this defect: it is purely a LOCAL data structure, never broadcast (see
//      `effects.rs`'s `CutOut` doc comment) -- each party only ever verifies its OWN,
//      independently-assembled `TimeoutCert`, never one asserted by a peer.

use crate::error::{DagError, DagResult};
use crate::messages::Proposal;
use config::Committee;
use crypto::{Blake3Hasher, Digest, Hash, PublicKey};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;

/// One cut-consensus round counter. Upstream mixes a local `Round = u64` alias (on
/// `Timeout`/`TimeoutAccept`/`Decide`/`TimeoutCert`) with a bare `u64` (on
/// `CutProposal`/`CutVote`) for what is, across all six types, the same counter.
/// Unified here under one explicit name.
pub type CutRound = u64;

/// Upstream primary/src/messages.rs:172-213.
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

/// Upstream primary/src/messages.rs:215-254.
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

/// Commit message in the protocol -- Fig. 2's `⟨commit, r⟩` (the **Vote** step: "send
/// `⟨commit, curr_round⟩` to all parties"). Upstream primary/src/messages.rs:375-417,
/// minus the dead `origin` field (AUDIT FIX F5 -- see the module doc comment).
#[derive(Clone, Serialize, Deserialize)]
pub struct Decide {
    pub id: Digest,
    pub round: CutRound,
    pub author: PublicKey,
}

impl Decide {
    /// Mirrors `Timeout::new`/`TimeoutAccept::new`'s shape (round + author, no
    /// `origin`) now that the dead field is gone -- upstream's own single call site
    /// passed `&self.name` for both `origin` and `author`, so this loses no
    /// information upstream itself ever populated distinctly.
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

/// Upstream primary/src/messages.rs:419-468. No `impl Hash`/`Debug` upstream either --
/// a certified `TimeoutCert` is identified by its `round` field directly.
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

/// A cut is a snapshot of certified tips across all lanes. Upstream
/// primary/src/messages.rs:734. Aliases `crate::messages::Proposal` directly -- see
/// the module doc comment above -- rather than a locally redefined tip type.
pub type Cut = BTreeMap<PublicKey, Proposal>;

/// Upstream primary/src/messages.rs:736-784.
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

/// Upstream primary/src/messages.rs:786-811.
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

