// Simple-IT cut-consensus vote aggregators (stage 1 of a port from the upstream
// `simpleit/Opt-Mempool-Simple-IT-Failure` branch's primary/src/aggregators.rs).
//
// Ported (exact upstream line ranges noted per aggregator below): `CutVoteAggregator`,
// `DecideAggregator`, `TimeoutAggregator`, `TimeoutAcceptAggregator`. Everything else in
// upstream aggregators.rs (`VoteAggregator`, `QCMaker`, `TCMaker`, ...) is Autobahn
// residue or shared substrate and is intentionally left out -- see
// primary/src/simpleit/mod.rs.
//
// Deviations from upstream beyond the module-wide ones documented in mod.rs:
//
//   - `impl Default` is added for all four aggregators (in terms of `new()`; zero
//     behavior change). Upstream's `aggregators` module is private (`mod aggregators;`
//     in that branch's primary/src/lib.rs), so these types are not part of its crate's
//     public API and `clippy::new_without_default` never considers them. This module
//     is `pub mod` end to end (a stage-1 requirement, so the not-yet-written state
//     machine can reach every item without `#[allow(dead_code)]`), which makes these
//     types genuinely public and brings them back into that lint's scope; implementing
//     `Default` is the fix it asks for, not a suppression.
//
//   - `CutVoteAggregator::append` cannot call `committee.optimistic_threshold()`:
//     this repo's `config::Committee` (config/src/lib.rs) carries no `f_num` field and
//     has no `optimistic_threshold` method -- only `quorum_threshold`/
//     `validity_threshold`/`fast_threshold`, each derived purely from total stake.
//     Adding the method to `config::Committee` is out of scope (only
//     `primary/src/lib.rs` may be touched among existing files), so `optimistic_threshold`
//     is reproduced below as a private free function computing the identical formula,
//     with `f` derived from total stake the same implicit way `quorum_threshold`/
//     `validity_threshold` already do (`f = (n - 1) / 3`, i.e. `n = 3f + 1`). See its
//     doc comment for the worked values and the (strictly more defined) handling of
//     degenerate committee sizes.

use crate::error::{DagError, DagResult};
use crate::simpleit::messages::{CutRound, CutVote, Decide, Timeout, TimeoutAccept, TimeoutCert};
use config::{Committee, Stake};
use crypto::{Digest, PublicKey};
use std::collections::HashSet;

/// Aggregates cut votes for one proposed cut, counted strictly FIRST-HAND: every
/// `append` call verifies and dedups one individual `CutVote` this party itself
/// received, and once the accumulated weight reaches `mint_threshold` this returns the
/// exact set of voters counted, for the caller (`CutEngine::process_cut_vote`) to mark
/// the round `safe` locally -- see `engine.rs`'s module doc comment ("FIGURE-2
/// REWRITE") for why this replaces the removed `CutCertificate`: a certificate would
/// let a party accept ANOTHER party's relayed claim about who voted, which this
/// signature-free protocol cannot authenticate. This aggregator never does that --
/// each party runs its own, over votes it individually verified. Upstream
/// primary/src/aggregators.rs:56-60 (struct), 170-198 (impl) minted a `CutCertificate`
/// at this same point instead; the counting logic below is otherwise unchanged.
///
/// Threshold: `mint_threshold` -- `max(optimistic_threshold, quorum_threshold)`. See
/// that function for why the clamp is required for correctness at small committee
/// sizes. At n=3f+1 the optimistic term dominates from f >= 3 onwards: 7 at n=10,
/// 40 at n=50.
pub struct CutVoteAggregator {
    weight: Stake,
    used: HashSet<PublicKey>,
    /// Every distinct author counted so far towards `mint_threshold`, in arrival
    /// order. Returned (not merely counted) once the threshold is crossed: this is
    /// the fetch-target set `CutEngine::mark_cut_safe` hands to `ensure_cut_fetch`
    /// when this round's own `CutProposal` is still unknown -- the closest available
    /// analogue to the removed `CutCertificate::votes`, but never transmitted or
    /// trusted from a peer; it is this party's own first-hand record of who it
    /// received a vote from.
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

    pub fn append(
        &mut self,
        vote: &CutVote,
        committee: &Committee,
    ) -> DagResult<Option<Vec<PublicKey>>> {
        let author = vote.author;
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));
        self.voters.push(author);
        self.weight += committee.stake(&author);
        if self.weight >= mint_threshold(committee) {
            self.weight = 0;
            return Ok(Some(std::mem::take(&mut self.voters)));
        }
        Ok(None)
    }
}

/// The threshold at which `CutVoteAggregator` marks a round safe (Fig. 2's Mark-safe
/// step, evaluated locally by each party rather than via a relayed certificate):
/// `max(optimistic_threshold, quorum_threshold)`.
///
/// AUDIT FIX (finding predates the Fig.-2 rewrite; still load-bearing under it).
/// Upstream mints at `optimistic_threshold` alone, but upstream's own
/// `CutCertificate::verify` required `quorum_threshold`. Since `optimistic_threshold =
/// ceil((5f-1)/2)` and `quorum_threshold = 2f+1` at n=3f+1, the former is STRICTLY
/// SMALLER for f <= 2, only catching up once f reaches 3 (`ceil((5f-1)/2)` first
/// reaches `2f+1` exactly at f = 3). Under the old certificate design, at those sizes
/// the minting party rejected the certificate it had just built, so
/// `sent_decide_rounds` was never set, no party ever sent a `Decide`, and the round
/// could never commit. Under this design there is no separate verify step at all --
/// `mint_threshold` IS the only safety gate a round ever passes through -- so an
/// unclamped threshold would be worse, not merely inert: it would let a round be
/// marked `safe` on fewer than a quorum's worth of first-hand votes. Concretely
/// would-be-broken (unclamped optimistic term alone) at n = 4, 5, 6, 8, 9, 12 -- and
/// n=4 is `fab remote`'s default committee size. Upstream only ever benchmarked n=10
/// and n=50, where the optimistic term already dominates.
///
/// Clamping to `quorum_threshold` is the minimal sound fix: marking a round safe on
/// fewer than 2f+1 first-hand votes is not a quorum and could not be safely acted on
/// anyway. It is provably a no-op at every size we benchmark -- n=10 (max(7,7) = 7) and
/// n=50 (max(40,34) = 40) -- so it cannot move any measured number.
fn mint_threshold(committee: &Committee) -> Stake {
    optimistic_threshold(committee).max(committee.quorum_threshold())
}

/// `optimistic_threshold` = ceil((n + 2f - 2) / 2), where n is total stake and f is
/// derived as `(n - 1) / 3` (i.e. n = 3f+1) -- see the module doc comment above for why
/// this is a free function rather than `committee.optimistic_threshold()`. Worked
/// values: n=10, f=3 -> ceil(14/2) = 7. n=50, f=16 -> ceil(80/2) = 40.
///
/// `f` and the numerator are computed in `i64` and the numerator is clamped at 0 before
/// the (now non-negative) division, so a degenerate committee (n<2) returns 0 rather
/// than panicking on subtraction underflow. Upstream's own `Stake` (`u32`) arithmetic
/// carries the identical underflow risk at those sizes; this is strictly more defined
/// and never produces a different answer for any committee with n>=2.
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

/// Aggregates `Decide` messages for one (round, cut) pair into a certified decision.
/// Upstream primary/src/aggregators.rs:62-103.
///
/// Threshold: `quorum_threshold` (2f+1 at n=3f+1).
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
