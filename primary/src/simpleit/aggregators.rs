// Vote and timeout aggregators for Simple-IT.
//
// Aggregators deduplicate authors and return a threshold crossing to the engine.

use crate::error::{DagError, DagResult};
use crate::simpleit::messages::{
    CutReady, CutRound, CutVote, Decide, Timeout, TimeoutAccept, TimeoutCert,
};
use config::{Committee, Stake};
use crypto::{Digest, PublicKey};
use std::collections::HashSet;

/// Aggregates individually verified cut votes and returns the authors counted when
/// the caller-supplied threshold is reached.
pub struct CutVoteAggregator {
    weight: Stake,
    used: HashSet<PublicKey>,
    /// Distinct authors counted in arrival order. Returned when the threshold is met.
    voters: Vec<PublicKey>,
}

impl Default for CutVoteAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl CutVoteAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            voters: Vec::new(),
        }
    }

    /// `threshold` is the weight required to return the counted authors.
    pub fn append(
        &mut self,
        vote: &CutVote,
        committee: &Committee,
        threshold: Stake,
    ) -> DagResult<Option<Vec<PublicKey>>> {
        let author = vote.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));
        self.voters.push(author);
        self.weight += committee.stake(&author);
        if self.weight >= threshold {
            self.weight = 0;
            return Ok(Some(std::mem::take(&mut self.voters)));
        }
        Ok(None)
    }
}

/// Uses the maximum of the optimistic and quorum thresholds so safety always requires
/// a quorum.
pub(super) fn mint_threshold(committee: &Committee) -> Stake {
    optimistic_threshold(committee).max(committee.quorum_threshold())
}

/// Computes the optimistic threshold with signed intermediate arithmetic so degenerate
/// committees do not underflow.
fn optimistic_threshold(committee: &Committee) -> Stake {
    let total_stake: i64 = committee
        .authorities
        .values()
        .map(|authority| authority.stake as i64)
        .sum();
    let f = (total_stake - 1) / 3;
    let numerator = (total_stake + 2 * f - 2).max(0);
    ((numerator + 1) / 2) as Stake
}

/// Aggregates individually verified `CutReady` messages for the Bracha variant.
pub struct CutReadyAggregator {
    weight: Stake,
    used: HashSet<PublicKey>,
    /// Distinct authors counted in arrival order. Returned when quorum is met.
    voters: Vec<PublicKey>,
}

impl Default for CutReadyAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl CutReadyAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            voters: Vec::new(),
        }
    }

    pub fn append(
        &mut self,
        ready: &CutReady,
        committee: &Committee,
    ) -> DagResult<Option<Vec<PublicKey>>> {
        let author = ready.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));
        self.voters.push(author);
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0;
            return Ok(Some(std::mem::take(&mut self.voters)));
        }
        Ok(None)
    }
}

/// Aggregates `Decide` messages for one round and cut until quorum.
pub struct DecideAggregator {
    weight: Stake,
    used: HashSet<PublicKey>,
    round: Option<CutRound>,
    cut_id: Option<Digest>,
}

impl Default for DecideAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl DecideAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
            round: None,
            cut_id: None,
        }
    }

    pub fn append(&mut self, decide: &Decide, committee: &Committee) -> DagResult<Option<Decide>> {
        if let Some(round) = self.round {
            ensure!(round == decide.round, DagError::InvalidHeaderId);
        } else {
            self.round = Some(decide.round);
        }

        if let Some(cut_id) = &self.cut_id {
            ensure!(*cut_id == decide.id, DagError::InvalidHeaderId);
        } else {
            self.cut_id = Some(decide.id.clone());
        }

        let author = decide.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));
        self.weight += committee.stake(&author);

        if self.weight >= committee.quorum_threshold() {
            self.weight = 0;
            return Ok(Some(decide.clone()));
        }

        Ok(None)
    }
}

/// Aggregates timeout votes for a cut round into an accept trigger. Upstream
/// primary/src/aggregators.rs:106-129.
///
/// Threshold: `quorum_threshold` (2f+1 at n=3f+1).
pub struct TimeoutAggregator {
    weight: Stake,
    used: HashSet<PublicKey>,
}

impl Default for TimeoutAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeoutAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            used: HashSet::new(),
        }
    }

    pub fn append(&mut self, timeout: Timeout, committee: &Committee) -> DagResult<Option<()>> {
        let author = timeout.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            return Ok(Some(()));
        }
        Ok(None)
    }
}

/// Aggregates timeout accepts for a cut round into a timeout certificate. Upstream
/// primary/src/aggregators.rs:132-168.
///
/// Threshold: `quorum_threshold` (2f+1 at n=3f+1).
pub struct TimeoutAcceptAggregator {
    weight: Stake,
    accepts: Vec<PublicKey>,
    used: HashSet<PublicKey>,
}

impl Default for TimeoutAcceptAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeoutAcceptAggregator {
    pub fn new() -> Self {
        Self {
            weight: 0,
            accepts: Vec::new(),
            used: HashSet::new(),
        }
    }

    pub fn append(
        &mut self,
        accept: TimeoutAccept,
        committee: &Committee,
    ) -> DagResult<(Stake, Option<TimeoutCert>)> {
        let author = accept.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.accepts.push(author);
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            return Ok((
                self.weight,
                Some(TimeoutCert {
                    round: accept.round,
                    timeouts: std::mem::take(&mut self.accepts),
                }),
            ));
        }
        Ok((self.weight, None))
    }
}
