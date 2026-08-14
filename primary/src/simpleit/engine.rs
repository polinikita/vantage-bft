// Simple-IT cut-consensus state machine. The caller executes returned effects.
// Safety requires an available proposal and verified quorum messages.

use crate::error::{DagError, DagResult};
use crate::leader::RoundRobin;
use crate::messages::Proposal;
use crate::simpleit::aggregators::{
    mint_threshold, CutReadyAggregator, CutVoteAggregator, DecideAggregator,
    TimeoutAcceptAggregator, TimeoutAggregator,
};
use crate::simpleit::effects::{CutEffect, CutOut};
use crate::simpleit::messages::{
    Cut, CutProposal, CutReady, CutRound, CutVote, Decide, Timeout, TimeoutAccept, TimeoutCert,
};
#[cfg(test)]
use crate::vantage::agb;
use config::{Committee, Stake};
use crypto::{Digest, Hash as _, PublicKey};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

/// Messages and timer events consumed by `CutEngine`. The exhaustive dispatch match
/// defines the complete input surface.
#[derive(Clone, Debug)]
pub enum Inbound {
    CutProposal(CutProposal),
    CutVote(CutVote),
    Decide(Decide),
    Timeout(Timeout),
    TimeoutAccept(TimeoutAccept),
    /// Second echo round: Bracha's ready and Opt-RBC's fallback.
    CutReady(CutReady),
    /// A scheduled deadline for this round has elapsed.
    TimerFired(CutRound),
    /// A peer requests the `CutProposal` identified by `(round, cut_id)`.
    CutFetch(CutRound, Digest, PublicKey),
    /// A peer answers a fetch request.
    CutServe(CutProposal),
}

/// Reports whether the local node has validity-threshold evidence for a proposal tip.
pub trait TipOracle {
    fn available_at_validity(&self, author: &PublicKey, tip: &Proposal) -> bool;
}

/// Selects the cut-consensus message flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Variant {
    /// `Opt` uses one first-hand `CutVote` census and reaches `mark_cut_safe` directly.
    #[default]
    Opt,
    /// `Bracha` uses a first-hand `CutVote` census, thresholded at
    /// `quorum_threshold`, broadcasts `CutReady`; a second first-hand `CutReady`
    /// census, also at `quorum_threshold`, reaches `mark_cut_safe`.
    Bracha,
}

/// Simple-IT cut-consensus state machine.
pub struct CutEngine {
    name: PublicKey,
    committee: Committee,
    leaders: RoundRobin,
    /// Relative delay used by `schedule_cut_timer`.
    timeout_delay: u64,
    /// Gates `process_cut_proposal` on f+1 tip availability when enabled.
    gate_tips: bool,
    /// Selects the cut-consensus variant. The default is `Variant::Opt`.
    variant: Variant,

    /// Current cut round. Starts at 1; round 0 is the genesis-parent sentinel.
    cut_round: CutRound,
    /// Safe cut used as the parent for the next proposal.
    highest_safe_cut: Digest,
    /// Last pruning floor. Also rejects stale timeout accepts.
    gc_floor: CutRound,

    /// Vote aggregators keyed by round and cut. Round-prunable.
    cut_vote_aggregators: BTreeMap<(CutRound, Digest), CutVoteAggregator>,
    /// Ready aggregators keyed by round and cut, used by both variants. Round-prunable.
    cut_ready_aggregators: BTreeMap<(CutRound, Digest), CutReadyAggregator>,
    /// Timeout aggregators by round.
    timeouts_aggregators: BTreeMap<CutRound, TimeoutAggregator>,
    /// Timeout-accept aggregators by round.
    timeout_accept_aggregators: BTreeMap<CutRound, TimeoutAcceptAggregator>,
    /// Proposals by round and cut. Round-prunable.
    cut_proposals: BTreeMap<(CutRound, Digest), CutProposal>,
    /// Proposals waiting for their parent, keyed by child round and parent cut.
    pending_cut_children: BTreeMap<(CutRound, Digest), Vec<CutProposal>>,
    /// Maps a cut id to its round. Entries are removed with their proposals.
    cut_round_by_id: BTreeMap<Digest, CutRound>,
    /// Leader cut by round. Round-prunable.
    leader_cut_by_round: BTreeMap<CutRound, Digest>,
    /// Safe cut by round. Presence means the round passed the vote threshold.
    safe: BTreeMap<CutRound, Digest>,
    /// Decide aggregators by round and cut. Round-prunable.
    decide_aggregators: BTreeMap<(CutRound, Digest), DecideAggregator>,
    /// Quorum-crossing decide by round. Presence marks the round committed.
    committed: BTreeMap<CutRound, Decide>,
    /// Records whether this node sent its cut vote for each round.
    sent_cut_votes: BTreeSet<CutRound>,
    /// Latch for this node's one-shot ready broadcast per round.
    sent_cut_ready: BTreeSet<CutRound>,
    /// Records whether this node proposed a cut for each round.
    proposed_cut_rounds: BTreeSet<CutRound>,
    /// Records whether this node sent its decide for each round.
    voted: BTreeSet<CutRound>,
    /// Records whether a committed round was delivered locally.
    sent_commit_rounds: BTreeSet<CutRound>,
    /// Records whether this node sent a timeout for each round.
    timed_out: BTreeSet<CutRound>,
    /// Records whether this node sent a timeout accept for each round.
    sent_timeout_accepts: BTreeSet<CutRound>,
    /// Records rounds certified as timed out.
    certified_timed_out: BTreeSet<CutRound>,
    /// Records rounds with an armed cut timer.
    scheduled_cut_timers: BTreeSet<CutRound>,

    /// Outstanding proposal fetches and the cut round when they were last sent.
    pending_cut_fetch: BTreeMap<(CutRound, Digest), CutRound>,
    /// Per-requester deduplication for fetch responses.
    fetch_answered: BTreeSet<(CutRound, Digest, PublicKey)>,
}

impl CutEngine {
    pub fn new(name: PublicKey, committee: Committee, timeout_delay: u64) -> Self {
        let leaders = RoundRobin::new(&committee);
        Self {
            name,
            committee,
            leaders,
            timeout_delay,
            gate_tips: true,
            variant: Variant::default(),
            cut_round: 1,
            highest_safe_cut: Digest::default(),
            gc_floor: 0,
            cut_vote_aggregators: BTreeMap::new(),
            cut_ready_aggregators: BTreeMap::new(),
            timeouts_aggregators: BTreeMap::new(),
            timeout_accept_aggregators: BTreeMap::new(),
            cut_proposals: BTreeMap::new(),
            pending_cut_children: BTreeMap::new(),
            cut_round_by_id: BTreeMap::new(),
            leader_cut_by_round: BTreeMap::new(),
            safe: BTreeMap::new(),
            decide_aggregators: BTreeMap::new(),
            committed: BTreeMap::new(),
            sent_cut_votes: BTreeSet::new(),
            sent_cut_ready: BTreeSet::new(),
            proposed_cut_rounds: BTreeSet::new(),
            voted: BTreeSet::new(),
            sent_commit_rounds: BTreeSet::new(),
            timed_out: BTreeSet::new(),
            sent_timeout_accepts: BTreeSet::new(),
            certified_timed_out: BTreeSet::new(),
            scheduled_cut_timers: BTreeSet::new(),
            pending_cut_fetch: BTreeMap::new(),
            fetch_answered: BTreeSet::new(),
        }
    }

    /// Enables or disables the f+1 tip-availability gate before voting.
    pub fn with_gate_tips(mut self, gate_tips: bool) -> Self {
        self.gate_tips = gate_tips;
        self
    }

    /// Selects the cut-consensus variant.
    pub fn with_variant(mut self, variant: Variant) -> Self {
        self.variant = variant;
        self
    }

    /// Dispatches one wire message or timer event.
    pub fn handle(
        &mut self,
        inbound: Inbound,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        match inbound {
            Inbound::CutProposal(p) => self.process_cut_proposal(p, tips, oracle),
            Inbound::CutVote(v) => self.process_cut_vote(v, tips, oracle),
            Inbound::CutReady(r) => self.process_cut_ready(r, tips, oracle),
            Inbound::Decide(d) => self.process_decide(d),
            Inbound::Timeout(t) => self.handle_timeout(t, tips, oracle),
            Inbound::TimeoutAccept(a) => {
                if self.sanitize_timeout_accept(&a).is_err() {
                    return Vec::new();
                }
                self.process_timeout_accept(a, tips, oracle)
            }
            Inbound::TimerFired(r) => self.process_cut_timer(r, tips, oracle),
            Inbound::CutFetch(round, cut_id, requester) => {
                self.on_cut_fetch(requester, round, cut_id)
            }
            Inbound::CutServe(proposal) => self.on_cut_serve(proposal, tips, oracle),
        }
    }

    /// Returns the committee leader for `round`.
    fn leader_for_round(&self, round: CutRound) -> PublicKey {
        self.leaders.one_based(round + 1)
    }

    /// Returns the current cut round.
    pub fn cut_round(&self) -> CutRound {
        self.cut_round
    }

    /// Removes round-indexed state below `floor`.
    pub fn prune_below(&mut self, floor: CutRound) {
        if floor <= self.gc_floor {
            return;
        }

        let kept_cut_proposals = self.cut_proposals.split_off(&(floor, Digest::default()));
        for (_, digest) in self.cut_proposals.keys() {
            self.cut_round_by_id.remove(digest);
        }
        self.cut_proposals = kept_cut_proposals;

        self.cut_vote_aggregators = self
            .cut_vote_aggregators
            .split_off(&(floor, Digest::default()));
        self.cut_ready_aggregators = self
            .cut_ready_aggregators
            .split_off(&(floor, Digest::default()));
        self.pending_cut_children = self
            .pending_cut_children
            .split_off(&(floor, Digest::default()));
        self.decide_aggregators = self
            .decide_aggregators
            .split_off(&(floor, Digest::default()));

        self.timeouts_aggregators = self.timeouts_aggregators.split_off(&floor);
        self.timeout_accept_aggregators = self.timeout_accept_aggregators.split_off(&floor);
        self.leader_cut_by_round = self.leader_cut_by_round.split_off(&floor);
        self.safe = self.safe.split_off(&floor);
        self.committed = self.committed.split_off(&floor);
        self.sent_cut_votes = self.sent_cut_votes.split_off(&floor);
        self.sent_cut_ready = self.sent_cut_ready.split_off(&floor);
        self.proposed_cut_rounds = self.proposed_cut_rounds.split_off(&floor);
        self.voted = self.voted.split_off(&floor);
        self.sent_commit_rounds = self.sent_commit_rounds.split_off(&floor);
        self.timed_out = self.timed_out.split_off(&floor);
        self.sent_timeout_accepts = self.sent_timeout_accepts.split_off(&floor);
        self.certified_timed_out = self.certified_timed_out.split_off(&floor);
        self.scheduled_cut_timers = self.scheduled_cut_timers.split_off(&floor);

        self.pending_cut_fetch = self
            .pending_cut_fetch
            .split_off(&(floor, Digest::default()));
        self.fetch_answered =
            self.fetch_answered
                .split_off(&(floor, Digest::default(), PublicKey::default()));

        self.gc_floor = floor;
    }

    const FETCH_RETRY_ROUNDS: CutRound = 8;

    fn all_other_committee_members(&self) -> Vec<PublicKey> {
        self.committee
            .authorities
            .keys()
            .filter(|k| **k != self.name)
            .copied()
            .collect()
    }

    /// Requests a missing proposal at most once per retry window.
    fn ensure_cut_fetch(
        &mut self,
        round: CutRound,
        cut_id: &Digest,
        targets: Vec<PublicKey>,
    ) -> Vec<CutEffect> {
        if round < self.gc_floor || self.cut_round_by_id.contains_key(cut_id) {
            return Vec::new();
        }
        let key = (round, cut_id.clone());
        match self.pending_cut_fetch.get(&key) {
            Some(&last) if self.cut_round.saturating_sub(last) < Self::FETCH_RETRY_ROUNDS => {
                return Vec::new();
            }
            _ => {}
        }
        self.pending_cut_fetch.insert(key, self.cut_round);
        targets
            .into_iter()
            .map(|peer| CutEffect::FetchTo {
                peer,
                round,
                cut_id: cut_id.clone(),
            })
            .collect()
    }

    /// Counts each verified vote once. Both variants broadcast `CutReady` at
    /// quorum — the Opt-RBC fallback round that keeps delivering with `n - f`
    /// responsive parties. Opt additionally marks the cut safe directly once
    /// the optimistic `mint_threshold` is met: fewer tolerated faults, two
    /// fewer message delays.
    pub fn process_cut_vote(
        &mut self,
        vote: CutVote,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if vote.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = vote.round;
        let cut_id = vote.cut_id.clone();
        let key = (round, cut_id.clone());
        let quorum = self.committee.quorum_threshold();
        let mint = mint_threshold(&self.committee);
        let (previous, current, witnesses) = {
            let aggregator = self.cut_vote_aggregators.entry(key).or_default();
            let Ok((previous, current)) = aggregator.append(&vote, &self.committee) else {
                return Vec::new();
            };
            (previous, current, aggregator.voters().to_vec())
        };
        let mut effects = Vec::new();
        if previous < quorum && current >= quorum {
            effects.extend(self.broadcast_cut_ready(round, cut_id.clone(), tips, oracle));
        }
        if self.variant == Variant::Opt && previous < mint && current >= mint {
            effects.extend(self.mark_cut_safe(round, cut_id, witnesses, tips, oracle));
        }
        effects
    }

    fn broadcast_cut_ready(
        &mut self,
        round: CutRound,
        cut_id: Digest,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if !self.sent_cut_ready.insert(round) {
            return Vec::new();
        }
        let ready = CutReady {
            round,
            cut_id,
            author: self.name,
        };
        let mut effects = vec![CutEffect::Broadcast(CutOut::CutReady(ready.clone()))];
        effects.extend(self.process_cut_ready(ready, tips, oracle));
        effects
    }

    pub fn process_cut_ready(
        &mut self,
        ready: CutReady,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if ready.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = ready.round;
        let cut_id = ready.cut_id.clone();
        let key = (round, cut_id.clone());
        let aggregator = self.cut_ready_aggregators.entry(key).or_default();
        let Ok(Some(witnesses)) = aggregator.append(&ready, &self.committee) else {
            return Vec::new();
        };
        self.mark_cut_safe(round, cut_id, witnesses, tips, oracle)
    }

    pub fn process_cut_proposal(
        &mut self,
        proposal: CutProposal,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let mut effects = Vec::new();
        let mut queue = VecDeque::from([proposal]);
        while let Some(proposal) = queue.pop_front() {
            if proposal.verify(&self.committee).is_err() {
                continue;
            }
            let round = proposal.round;
            if self.certified_timed_out.contains(&round) {
                continue;
            }
            if proposal.proposer != self.leader_for_round(round) {
                continue;
            }

            if !self.safe_cut_parent(round, &proposal.parent_cut) {
                let parent_cut = proposal.parent_cut.clone();
                // Request the parent in the preceding round; later votes can refine it.
                if parent_cut != Digest::default()
                    && !self.cut_round_by_id.contains_key(&parent_cut)
                {
                    let targets = self.all_other_committee_members();
                    effects.extend(self.ensure_cut_fetch(
                        round.saturating_sub(1),
                        &parent_cut,
                        targets,
                    ));
                }
                self.pending_cut_children
                    .entry((round, parent_cut))
                    .or_default()
                    .push(proposal);
                continue;
            }

            let cut_id = proposal.id();
            if self.cut_proposals.contains_key(&(round, cut_id)) {
                continue;
            }

            let tips_ok = !self.gate_tips
                || proposal
                    .tips
                    .iter()
                    .all(|(author, tip)| oracle.available_at_validity(author, tip));

            let cut_id = self.record_cut_proposal(proposal);
            self.leader_cut_by_round
                .entry(round)
                .or_insert_with(|| cut_id.clone());

            // Retry children that named this proposal as their parent.
            let waiting_rounds: Vec<CutRound> = self
                .pending_cut_children
                .keys()
                .filter(|(_, parent)| *parent == cut_id)
                .map(|(r, _)| *r)
                .collect();
            for r in waiting_rounds {
                if let Some(children) = self.pending_cut_children.remove(&(r, cut_id.clone())) {
                    queue.extend(children);
                }
            }

            if tips_ok && self.sent_cut_votes.insert(round) {
                let vote = CutVote {
                    round,
                    cut_id: cut_id.clone(),
                    author: self.name,
                };
                effects.push(CutEffect::Broadcast(CutOut::CutVote(vote.clone())));
                effects.extend(self.process_cut_vote(vote, tips, oracle));
            }

            effects.extend(self.try_commit_round(round));
        }
        effects
    }

    pub fn retry_pending_cut_proposals(
        &mut self,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if self.pending_cut_children.is_empty() {
            return Vec::new();
        }

        let pending = std::mem::take(&mut self.pending_cut_children);
        let mut still_pending = BTreeMap::new();
        let mut ready = Vec::new();

        for ((round, parent_cut), proposals) in pending {
            let mut deferred = Vec::new();
            for proposal in proposals {
                if self.safe_cut_parent(proposal.round, &proposal.parent_cut) {
                    ready.push(proposal);
                } else {
                    deferred.push(proposal);
                }
            }
            if !deferred.is_empty() {
                still_pending.insert((round, parent_cut), deferred);
            }
        }

        self.pending_cut_children = still_pending;

        let mut effects = Vec::new();
        for proposal in ready {
            effects.extend(self.process_cut_proposal(proposal, tips, oracle));
        }
        effects
    }

    fn current_cut(&self, tips: &Cut) -> Cut {
        tips.clone()
    }

    fn make_cut_proposal(&self, round: CutRound, parent_cut: Digest, tips: &Cut) -> CutProposal {
        CutProposal {
            round,
            proposer: self.name,
            parent_cut,
            tips: self.current_cut(tips),
        }
    }

    fn record_cut_proposal(&mut self, proposal: CutProposal) -> Digest {
        let cut_id = proposal.id();
        let round = proposal.round;
        self.cut_round_by_id.insert(cut_id.clone(), round);
        self.cut_proposals.insert((round, cut_id.clone()), proposal);
        cut_id
    }

    /// Marks a cut safe after a local threshold crossing and broadcasts this node's decide.
    /// `witnesses` are used only to fetch a missing proposal.
    fn mark_cut_safe(
        &mut self,
        round: CutRound,
        cut_id: Digest,
        witnesses: Vec<PublicKey>,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if self.certified_timed_out.contains(&round) {
            return Vec::new();
        }
        self.safe.entry(round).or_insert_with(|| cut_id.clone());
        if round + 1 >= self.cut_round {
            self.highest_safe_cut = cut_id.clone();
        }
        self.cut_round = self.cut_round.max(round + 1);
        self.advance_timed_out_cut_rounds();

        // The vote quorum may identify a proposal this node has not received.
        let mut effects = self.ensure_cut_fetch(round, &cut_id, witnesses);
        if self.voted.insert(round) {
            let decide = Decide {
                id: cut_id,
                round,
                author: self.name,
            };
            effects.push(CutEffect::Broadcast(CutOut::Decide(decide.clone())));
            effects.extend(self.process_decide(decide));
        }

        effects.extend(self.try_propose_cut_for_current_round(tips, oracle));
        effects.extend(self.schedule_cut_timer(self.cut_round));
        effects
    }

    pub fn process_decide(&mut self, decide: Decide) -> Vec<CutEffect> {
        if decide.verify(&self.committee).is_err() {
            return Vec::new();
        }
        if self.committed.contains_key(&decide.round) {
            return Vec::new();
        }

        let key = (decide.round, decide.id.clone());
        let aggregator = self.decide_aggregators.entry(key).or_default();
        let Ok(Some(quorum_decide)) = aggregator.append(&decide, &self.committee) else {
            return Vec::new();
        };
        let round = quorum_decide.round;
        self.committed.entry(round).or_insert(quorum_decide);
        self.try_commit_round(round)
    }

    fn try_commit_round(&mut self, round: CutRound) -> Vec<CutEffect> {
        let Some(decide) = self.committed.get(&round) else {
            return Vec::new();
        };
        let Some(leader_cut) = self.leader_cut_by_round.get(&round) else {
            return Vec::new();
        };

        if decide.id == *leader_cut {
            let leader_cut = leader_cut.clone();
            return self.emit_commit_to_committer(round, &leader_cut);
        }
        Vec::new()
    }

    fn emit_commit_to_committer(&mut self, round: CutRound, cut_id: &Digest) -> Vec<CutEffect> {
        if self.sent_commit_rounds.contains(&round) {
            return Vec::new();
        }
        let Some(cut) = self.cut_proposals.get(&(round, cut_id.clone())) else {
            return Vec::new();
        };
        let proposals = cut.tips.clone();
        self.sent_commit_rounds.insert(round);
        vec![CutEffect::Commit { round, proposals }]
    }

    pub fn try_propose_cut_for_current_round(
        &mut self,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let round = self.cut_round;
        if self.name != self.leader_for_round(round) {
            return Vec::new();
        }
        if !self.safe_cut_parent(round, &self.highest_safe_cut) {
            return Vec::new();
        }
        if !self.proposed_cut_rounds.insert(round) {
            return Vec::new();
        }

        let parent_cut = self.highest_safe_cut.clone();
        let proposal = self.make_cut_proposal(round, parent_cut, tips);
        let mut effects = vec![CutEffect::Broadcast(CutOut::CutProposal(proposal.clone()))];
        effects.extend(self.process_cut_proposal(proposal, tips, oracle));
        effects
    }

    /// Arms one deadline per round.
    pub fn schedule_cut_timer(&mut self, round: CutRound) -> Vec<CutEffect> {
        if self.scheduled_cut_timers.insert(round) {
            log::debug!(
                "BENCH event=round_start round={} leader={:?} node={:?}",
                round,
                self.leader_for_round(round),
                self.name
            );
            let deadline = Instant::now() + Duration::from_millis(self.timeout_delay);
            return vec![CutEffect::ArmTimer { round, deadline }];
        }
        Vec::new()
    }

    fn safe_cut_parent(&self, round: CutRound, parent_cut: &Digest) -> bool {
        let parent_round = if *parent_cut == Digest::default() {
            0
        } else if let Some(parent_round) = self.cut_round_by_id.get(parent_cut) {
            *parent_round
        } else {
            return false;
        };

        if parent_round >= round {
            return false;
        }

        ((parent_round + 1)..round).all(|r| self.certified_timed_out.contains(&r))
    }

    fn advance_timed_out_cut_rounds(&mut self) -> bool {
        let old_cut_round = self.cut_round;
        while self.certified_timed_out.contains(&self.cut_round)
            && self.safe_cut_parent(self.cut_round + 1, &self.highest_safe_cut)
        {
            self.cut_round += 1;
        }
        self.cut_round != old_cut_round
    }

    pub fn process_cut_timer(
        &mut self,
        round: CutRound,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if round != self.cut_round
            || self.safe.contains_key(&round)
            || self.certified_timed_out.contains(&round)
            || !self.timed_out.insert(round)
        {
            return Vec::new();
        }

        log::debug!(
            "BENCH event=timeout_sent round={} node={:?}",
            round,
            self.name
        );
        let timeout = Timeout {
            round,
            author: self.name,
        };
        let mut effects = vec![CutEffect::Broadcast(CutOut::Timeout(timeout.clone()))];
        effects.extend(self.process_timeout(timeout, tips, oracle));
        effects
    }

    pub fn process_timeout(
        &mut self,
        timeout: Timeout,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if timeout.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = timeout.round;
        if self.certified_timed_out.contains(&round) || self.safe.contains_key(&round) {
            return Vec::new();
        }

        let aggregator = self.timeouts_aggregators.entry(round).or_default();
        let Ok(Some(())) = aggregator.append(timeout, &self.committee) else {
            return Vec::new();
        };

        let (mut effects, maybe) = self.send_timeout_accept(round);
        if let Some((weight, timeout_cert)) = maybe {
            effects.extend(self.handle_timeout_accept_action(
                round,
                weight,
                timeout_cert,
                tips,
                oracle,
            ));
        }
        effects
    }

    fn broadcast_timeout_accept(&self, accept: &TimeoutAccept) -> Vec<CutEffect> {
        vec![CutEffect::Broadcast(CutOut::TimeoutAccept(accept.clone()))]
    }

    fn send_timeout_accept(
        &mut self,
        round: CutRound,
    ) -> (Vec<CutEffect>, Option<(Stake, Option<TimeoutCert>)>) {
        if !self.sent_timeout_accepts.insert(round) {
            return (Vec::new(), None);
        }
        let accept = TimeoutAccept {
            round,
            author: self.name,
        };
        let effects = self.broadcast_timeout_accept(&accept);
        let result = self.record_timeout_accept(accept);
        (effects, Some(result))
    }

    fn record_timeout_accept(&mut self, accept: TimeoutAccept) -> (Stake, Option<TimeoutCert>) {
        if accept.verify(&self.committee).is_err() {
            return (0, None);
        }
        let round = accept.round;
        if self.certified_timed_out.contains(&round) || self.safe.contains_key(&round) {
            return (0, None);
        }

        let aggregator = self.timeout_accept_aggregators.entry(round).or_default();
        aggregator
            .append(accept, &self.committee)
            .unwrap_or((0, None))
    }

    pub fn process_timeout_accept(
        &mut self,
        accept: TimeoutAccept,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let round = accept.round;
        let (weight, timeout_cert) = self.record_timeout_accept(accept);
        self.handle_timeout_accept_action(round, weight, timeout_cert, tips, oracle)
    }

    fn handle_timeout_accept_action(
        &mut self,
        round: CutRound,
        weight: Stake,
        mut timeout_cert: Option<TimeoutCert>,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let mut effects = Vec::new();
        if weight >= self.committee.validity_threshold() {
            let (amplify_effects, maybe) = self.send_timeout_accept(round);
            effects.extend(amplify_effects);
            if let Some((_, own_cert)) = maybe {
                timeout_cert = timeout_cert.or(own_cert);
            }
        }

        if let Some(timeout_cert) = timeout_cert {
            if timeout_cert.verify(&self.committee).is_err() {
                return effects;
            }
            if self.certified_timed_out.insert(round) {
                effects.extend(self.retry_pending_cut_proposals(tips, oracle));
                if self.advance_timed_out_cut_rounds() {
                    effects.extend(self.try_propose_cut_for_current_round(tips, oracle));
                    effects.extend(self.schedule_cut_timer(self.cut_round));
                }
            }
        }
        effects
    }

    pub fn handle_timeout(
        &mut self,
        timeout: Timeout,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        self.process_timeout(timeout, tips, oracle)
    }

    /// Rejects timeout accepts below the cut-round GC floor.
    pub fn sanitize_timeout_accept(&self, accept: &TimeoutAccept) -> DagResult<()> {
        ensure!(
            self.gc_floor <= accept.round,
            DagError::CertificateTooOld(accept.digest(), accept.round)
        );
        Ok(())
    }

    pub fn on_cut_fetch(
        &mut self,
        requester: PublicKey,
        round: CutRound,
        cut_id: Digest,
    ) -> Vec<CutEffect> {
        if round < self.gc_floor {
            return Vec::new();
        }
        let answered_key = (round, cut_id.clone(), requester);
        if self.fetch_answered.contains(&answered_key) {
            return Vec::new();
        }
        let Some(proposal) = self.cut_proposals.get(&(round, cut_id)) else {
            return Vec::new();
        };
        let proposal = proposal.clone();
        self.fetch_answered.insert(answered_key);
        vec![CutEffect::ServeTo {
            peer: requester,
            proposal,
        }]
    }

    pub fn on_cut_serve(
        &mut self,
        proposal: CutProposal,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let key = (proposal.round, proposal.id());
        if !self.pending_cut_fetch.contains_key(&key) {
            return Vec::new();
        }
        self.pending_cut_fetch.remove(&key);
        self.process_cut_proposal(proposal, tips, oracle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> PublicKey {
        PublicKey([byte; 32])
    }

    fn committee_of(n: u8) -> (Committee, Vec<PublicKey>) {
        let keys: Vec<PublicKey> = (1..=n).map(key).collect();
        let info = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    *k,
                    1u32,
                    format!("127.0.0.1:{}", 9000 + i as u16).parse().unwrap(),
                )
            })
            .collect();
        (Committee::new(info), keys)
    }

    fn sample_tips(keys: &[PublicKey]) -> Cut {
        keys.iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    *k,
                    Proposal {
                        header_digest: Digest([i as u8 + 1; 32]),
                        height: 1,
                        poa: None,
                    },
                )
            })
            .collect()
    }

    struct AllAvailable;
    impl TipOracle for AllAvailable {
        fn available_at_validity(&self, _author: &PublicKey, _tip: &Proposal) -> bool {
            true
        }
    }

    struct DenyAuthor(PublicKey);
    impl TipOracle for DenyAuthor {
        fn available_at_validity(&self, author: &PublicKey, _tip: &Proposal) -> bool {
            *author != self.0
        }
    }

    fn find_proposal(effects: &[CutEffect]) -> Option<CutProposal> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::CutProposal(p)) => Some(p.clone()),
            _ => None,
        })
    }

    fn find_vote_for_round(effects: &[CutEffect], round: CutRound) -> Option<CutVote> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::CutVote(v)) if v.round == round => Some(v.clone()),
            _ => None,
        })
    }

    fn find_decide_for_round(effects: &[CutEffect], round: CutRound) -> Option<Decide> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::Decide(d)) if d.round == round => Some(d.clone()),
            _ => None,
        })
    }

    fn find_commits(effects: &[CutEffect], round: CutRound) -> Vec<(CutRound, Cut)> {
        effects
            .iter()
            .filter_map(|e| match e {
                CutEffect::Commit {
                    round: r,
                    proposals,
                } if *r == round => Some((*r, proposals.clone())),
                _ => None,
            })
            .collect()
    }

    fn find_fetches(effects: &[CutEffect]) -> Vec<(PublicKey, CutRound, Digest)> {
        effects
            .iter()
            .filter_map(|e| match e {
                CutEffect::FetchTo {
                    peer,
                    round,
                    cut_id,
                } => Some((*peer, *round, cut_id.clone())),
                _ => None,
            })
            .collect()
    }

    fn find_ready_for_round(effects: &[CutEffect], round: CutRound) -> Option<CutReady> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::CutReady(r)) if r.round == round => Some(r.clone()),
            _ => None,
        })
    }

    fn happy_path_commit(n: u8) {
        let (committee, keys) = committee_of(n);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2);

        let mut engine = CutEngine::new(leader, committee, 1_000);

        // The leader broadcasts and self-votes.
        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();
        assert!(
            find_vote_for_round(&effects, round).is_some(),
            "leader self-votes for its own proposal"
        );
        assert!(!engine.safe.contains_key(&round));

        // Add votes until the cut is safe.
        let mut others = keys.iter().filter(|k| **k != leader);
        loop {
            if engine.safe.contains_key(&round) {
                break;
            }
            let author = *others
                .next()
                .expect("committee is large enough to reach mint_threshold");
            let vote = CutVote {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_vote(vote, &tips, &oracle);
        }
        assert_eq!(engine.safe.get(&round), Some(&cut_id));
        assert!(
            find_decide_for_round(&effects, round).is_some(),
            "the vote that crosses mint_threshold broadcasts this party's own Decide \
             in the SAME step -- one fewer message delay than the old \
             certificate-broadcast design"
        );

        // Add decides until the cut commits.
        let mut others = keys.iter().filter(|k| **k != leader);
        let mut commits = find_commits(&effects, round);
        while commits.is_empty() {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let decide = Decide {
                id: cut_id.clone(),
                round,
                author,
            };
            effects = engine.process_decide(decide);
            commits = find_commits(&effects, round);
        }

        assert_eq!(
            commits.len(),
            1,
            "commit should be emitted exactly once for the round"
        );
        assert_eq!(commits[0].1, proposal.tips);

        // Commit output is one-shot per round.
        let repeat = engine.try_commit_round(round);
        assert!(find_commits(&repeat, round).is_empty());
    }

    #[test]
    fn happy_path_commit_n4() {
        happy_path_commit(4);
    }

    #[test]
    fn happy_path_commit_n10() {
        happy_path_commit(10);
    }

    fn happy_path_commit_bracha(n: u8) {
        let (committee, keys) = committee_of(n);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2);

        let mut engine = CutEngine::new(leader, committee, 1_000).with_variant(Variant::Bracha);

        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();
        assert!(
            find_vote_for_round(&effects, round).is_some(),
            "leader self-votes for its own proposal"
        );
        assert!(
            find_ready_for_round(&effects, round).is_none(),
            "a single self-vote is not enough to cross quorum_threshold"
        );
        assert!(!engine.safe.contains_key(&round));

        // A CutVote quorum broadcasts CutReady but does not mark the cut safe.
        let mut others = keys.iter().filter(|k| **k != leader);
        loop {
            if find_ready_for_round(&effects, round).is_some() {
                break;
            }
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let vote = CutVote {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_vote(vote, &tips, &oracle);
        }
        assert!(
            !engine.safe.contains_key(&round),
            "crossing the FIRST echo (vote) threshold only broadcasts CutReady -- it \
             does not mark the round safe directly"
        );

        // A CutReady quorum marks the cut safe.
        let mut others = keys.iter().filter(|k| **k != leader);
        while !engine.safe.contains_key(&round) {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let ready = CutReady {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_ready(ready, &tips, &oracle);
        }
        assert_eq!(engine.safe.get(&round), Some(&cut_id));
        assert!(
            find_decide_for_round(&effects, round).is_some(),
            "the CutReady that crosses quorum_threshold broadcasts this party's own \
             Decide in the SAME step, exactly like the Opt variant's mark_cut_safe"
        );

        let mut others = keys.iter().filter(|k| **k != leader);
        let mut commits = find_commits(&effects, round);
        while commits.is_empty() {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let decide = Decide {
                id: cut_id.clone(),
                round,
                author,
            };
            effects = engine.process_decide(decide);
            commits = find_commits(&effects, round);
        }

        assert_eq!(
            commits.len(),
            1,
            "commit should be emitted exactly once for the round"
        );
        assert_eq!(commits[0].1, proposal.tips);

        let repeat = engine.try_commit_round(round);
        assert!(find_commits(&repeat, round).is_empty());
    }

    #[test]
    fn happy_path_commit_bracha_n4() {
        happy_path_commit_bracha(4);
    }

    #[test]
    fn happy_path_commit_bracha_n10() {
        happy_path_commit_bracha(10);
    }

    #[test]
    fn bracha_cut_ready_broadcasts_at_most_once_per_round() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let mut engine = CutEngine::new(keys[0], committee, 1_000).with_variant(Variant::Bracha);

        let cut_id_a = Digest([1; 32]);
        let effects = engine.broadcast_cut_ready(1, cut_id_a.clone(), &tips, &oracle);
        match effects.first() {
            Some(CutEffect::Broadcast(CutOut::CutReady(r))) => {
                assert_eq!(
                    r.cut_id, cut_id_a,
                    "the first call for a round broadcasts CutReady"
                );
            }
            other => panic!("expected a CutReady broadcast first, got {other:?}"),
        }

        let cut_id_b = Digest([2; 32]);
        let effects = engine.broadcast_cut_ready(1, cut_id_b, &tips, &oracle);
        assert!(
            effects.is_empty(),
            "a second CutReady for the same round must not be sent, even for a \
             different cut_id: {effects:?}"
        );
    }

    #[test]
    fn bracha_cut_ready_census_dedups_by_author_and_reaches_safe_at_quorum() {
        let (committee, keys) = committee_of(10);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let mut engine =
            CutEngine::new(keys[0], committee.clone(), 1_000).with_variant(Variant::Bracha);
        let cut_id = Digest([9; 32]);
        let round: CutRound = 1;

        let quorum = committee.quorum_threshold();
        let mut counted = 0u32;
        for author in keys.iter().copied() {
            if engine.safe.contains_key(&round) {
                break;
            }
            engine.process_cut_ready(
                CutReady {
                    round,
                    cut_id: cut_id.clone(),
                    author,
                },
                &tips,
                &oracle,
            );
            counted += 1;
        }
        assert!(
            engine.safe.contains_key(&round),
            "n=10 has enough distinct authors to reach quorum_threshold"
        );
        assert_eq!(
            counted, quorum,
            "safe should be reached at exactly quorum_threshold distinct CutReadys"
        );

        let before = engine.safe.get(&round).cloned();
        let repeat_author = keys[0];
        engine.process_cut_ready(
            CutReady {
                round,
                cut_id: cut_id.clone(),
                author: repeat_author,
            },
            &tips,
            &oracle,
        );
        assert_eq!(
            engine.safe.get(&round),
            before.as_ref(),
            "a replayed CutReady changes nothing"
        );
    }

    /// The Opt variant must deliver through the ready fallback when the
    /// optimistic threshold exceeds the number of live authors.
    #[test]
    fn opt_ready_fallback_reaches_safe_with_only_fourteen_of_twenty_live_authors() {
        let (committee, keys) = committee_of(20);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2);

        assert!(
            mint_threshold(&committee) > committee.quorum_threshold(),
            "test setup requires the optimistic threshold to exceed the quorum, \
             so that safety is reachable only through the ready fallback"
        );

        let live: Vec<PublicKey> = keys.iter().copied().take(14).collect();
        assert!(live.contains(&leader));

        let mut engine = CutEngine::new(leader, committee, 1_000);

        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();

        for author in live.iter().filter(|k| **k != leader) {
            effects = engine.process_cut_vote(
                CutVote {
                    round,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
        }
        assert!(
            find_ready_for_round(&effects, round).is_some(),
            "quorum votes without the optimistic threshold must broadcast the \
             Opt-RBC fallback CutReady"
        );
        assert!(
            !engine.safe.contains_key(&round),
            "14 votes stay below the optimistic threshold, so the fast path \
             alone must not mark the cut safe"
        );

        for author in live.iter().filter(|k| **k != leader) {
            if engine.safe.contains_key(&round) {
                break;
            }
            effects = engine.process_cut_ready(
                CutReady {
                    round,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
        }
        assert_eq!(
            engine.safe.get(&round),
            Some(&cut_id),
            "a quorum of readies must mark the cut safe under Opt"
        );

        let mut commits = find_commits(&effects, round);
        for author in live.iter().filter(|k| **k != leader) {
            if !commits.is_empty() {
                break;
            }
            effects = engine.process_decide(Decide {
                id: cut_id.clone(),
                round,
                author: *author,
            });
            commits = find_commits(&effects, round);
        }
        assert!(
            !commits.is_empty(),
            "a quorum of decides must commit the round with 14 of 20 live"
        );
        assert_eq!(commits[0].1, proposal.tips);
    }

    #[test]
    fn bracha_reaches_safe_and_commits_with_only_fourteen_of_twenty_live_authors() {
        let (committee, keys) = committee_of(20);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2);

        assert_eq!(
            committee.quorum_threshold(),
            14,
            "test setup assumes n=20's quorum_threshold is exactly 14"
        );
        assert_eq!(
            mint_threshold(&committee),
            15,
            "test setup assumes n=20's mint_threshold is exactly 15 -- one MORE than \
             the number of live authors below, which is exactly why Opt could never \
             reach safe in this scenario"
        );

        let live: Vec<PublicKey> = keys.iter().copied().take(14).collect();
        assert!(
            live.contains(&leader),
            "test setup requires the round-1 leader to be among the live authors"
        );

        let mut engine = CutEngine::new(leader, committee, 1_000).with_variant(Variant::Bracha);

        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();

        // Add votes from the other live authors.
        for author in live.iter().filter(|k| **k != leader) {
            effects = engine.process_cut_vote(
                CutVote {
                    round,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
        }
        assert!(
            find_ready_for_round(&effects, round).is_some(),
            "14 live authors is exactly quorum_threshold -- the vote census should \
             have crossed it and broadcast our own CutReady"
        );
        assert!(!engine.safe.contains_key(&round));

        for author in live.iter().filter(|k| **k != leader) {
            if engine.safe.contains_key(&round) {
                break;
            }
            effects = engine.process_cut_ready(
                CutReady {
                    round,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
        }
        assert_eq!(
            engine.safe.get(&round),
            Some(&cut_id),
            "14 live authors are exactly quorum_threshold under Bracha"
        );

        let mut commits = find_commits(&effects, round);
        for author in live.iter().filter(|k| **k != leader) {
            if !commits.is_empty() {
                break;
            }
            effects = engine.process_decide(Decide {
                id: cut_id.clone(),
                round,
                author: *author,
            });
            commits = find_commits(&effects, round);
        }

        assert_eq!(
            commits.len(),
            1,
            "the round should commit exactly once, using only the 14 live authors' \
             own messages"
        );
        assert_eq!(commits[0].1, proposal.tips);
    }

    #[test]
    fn party_reaches_safe_at_the_correctly_clamped_local_vote_threshold() {
        for n in [4u8, 5, 6, 8, 9] {
            let (committee, keys) = committee_of(n);
            let tips = sample_tips(&keys);
            let oracle = AllAvailable;
            let leader = agb::proposer(&committee, 2);
            let mut engine = CutEngine::new(leader, committee.clone(), 1_000);

            let effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
            let cut_id = find_proposal(&effects).expect("leader proposes").id();
            assert!(
                !engine.safe.contains_key(&1),
                "n={n}: self-vote alone is not enough"
            );

            let mut others = keys.iter().filter(|k| **k != leader);
            while !engine.safe.contains_key(&1) {
                let author = *others
                    .next()
                    .expect("committee large enough to reach mint_threshold");
                engine.process_cut_vote(
                    CutVote {
                        round: 1,
                        cut_id: cut_id.clone(),
                        author,
                    },
                    &tips,
                    &oracle,
                );
            }

            assert_eq!(
                engine.safe.get(&1),
                Some(&cut_id),
                "n={n}: safe[1] should hold exactly the cut this party's own votes converged on"
            );
            assert!(
                engine.voted.contains(&1),
                "n={n}: reaching safe should have sent this party's own Decide (Fig. 2's Vote step)"
            );
        }
    }

    /// Safety requires at least a quorum of distinct, verified votes.
    #[test]
    fn safe_is_reached_only_by_counting_distinct_votes_never_below_quorum() {
        let (committee, keys) = committee_of(10);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let leader = agb::proposer(&committee, 2);
        let mut engine = CutEngine::new(leader, committee.clone(), 1_000);

        let effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let cut_id = find_proposal(&effects).expect("leader proposes").id();
        assert!(
            !engine.safe.contains_key(&1),
            "the leader's own self-vote alone is not enough"
        );

        let quorum = committee.quorum_threshold();
        let mut voted = 1u32;
        for author in keys.iter().filter(|k| **k != leader) {
            if engine.safe.contains_key(&1) {
                break;
            }
            engine.process_cut_vote(
                CutVote {
                    round: 1,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
            voted += 1;
            if !engine.safe.contains_key(&1) {
                assert!(
                    voted <= quorum,
                    "still not safe with {voted} distinct votes counted (quorum is {quorum}) \
                     -- mint_threshold should never exceed n"
                );
            }
        }

        assert!(
            engine.safe.contains_key(&1),
            "committee is large enough to reach mint_threshold"
        );
        assert_eq!(engine.safe.get(&1), Some(&cut_id));
        assert!(
            voted >= quorum,
            "safe was reached with only {voted} distinct votes, fewer than quorum_threshold \
             ({quorum}) -- mint_threshold's clamp is not holding"
        );

        // A repeated author adds no weight.
        let repeat_author = keys[0];
        let before = engine.safe.get(&1).cloned();
        engine.process_cut_vote(
            CutVote {
                round: 1,
                cut_id: cut_id.clone(),
                author: repeat_author,
            },
            &tips,
            &oracle,
        );
        assert_eq!(
            engine.safe.get(&1),
            before.as_ref(),
            "a replayed vote changes nothing"
        );
    }

    /// Keeps the inbound surface free of relayed certificate messages.
    #[test]
    fn inbound_has_no_certificate_shaped_variant() {
        fn assert_exhaustive_with_no_certificate_arm(inbound: Inbound) {
            match inbound {
                Inbound::CutProposal(_)
                | Inbound::CutVote(_)
                | Inbound::CutReady(_)
                | Inbound::Decide(_)
                | Inbound::Timeout(_)
                | Inbound::TimeoutAccept(_)
                | Inbound::TimerFired(_)
                | Inbound::CutFetch(_, _, _)
                | Inbound::CutServe(_) => {}
            }
        }
        assert_exhaustive_with_no_certificate_arm(Inbound::TimerFired(0));
    }

    #[test]
    fn missing_proposal_stalls_the_chain_rather_than_skipping_a_round() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        // Use a node that leads neither round.
        let observer = *keys
            .iter()
            .find(|k| **k != round1_leader && **k != round2_leader)
            .expect("n=4 has a non-leader");
        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        // Reach safety from votes without receiving the round-1 proposal.
        let round1_cut = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let round1_id = round1_cut.id();
        let voters: Vec<PublicKey> = keys
            .iter()
            .take(committee.quorum_threshold() as usize)
            .copied()
            .collect();
        let mut effects = Vec::new();
        for author in voters {
            effects = engine.process_cut_vote(
                CutVote {
                    round: 1,
                    cut_id: round1_id.clone(),
                    author,
                },
                &tips,
                &oracle,
            );
        }
        assert_eq!(
            engine.safe.get(&1),
            Some(&round1_id),
            "mint_threshold distinct votes mark round 1 safe locally"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, CutEffect::Broadcast(CutOut::Decide(d)) if d.round == 1)),
            "reaching safe locally produces a Decide for round 1"
        );

        // The round-2 parent remains unknown locally.
        let round2 = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: round1_id,
            tips: tips.clone(),
        };
        let effects = engine.process_cut_proposal(round2, &tips, &oracle);
        assert!(
            effects.is_empty(),
            "round 2 cannot be recorded or voted: its parent is unknown here"
        );
        assert!(
            !engine.pending_cut_children.is_empty(),
            "round 2 is buffered pending round 1's proposal"
        );

        // Decides cannot commit a proposal that was never recorded.
        for author in keys.iter().copied() {
            let effects = engine.process_decide(Decide {
                id: round2_leader_cut_id(&tips, round2_leader),
                round: 2,
                author,
            });
            assert!(
                find_commits(&effects, 2).is_empty(),
                "round 2 must never commit while round 1's proposal is missing"
            );
        }
    }

    fn round2_leader_cut_id(tips: &Cut, leader: PublicKey) -> Digest {
        CutProposal {
            round: 2,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        }
        .id()
    }

    #[test]
    fn local_safe_with_unknown_proposal_triggers_fetch_to_its_witnesses() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let observer = *keys
            .iter()
            .find(|k| **k != round1_leader && **k != round2_leader)
            .expect("n=4 has a non-leader");
        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        let round1_cut = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let round1_id = round1_cut.id();
        let voters: Vec<PublicKey> = keys
            .iter()
            .take(committee.quorum_threshold() as usize)
            .copied()
            .collect();

        let mut effects = Vec::new();
        for author in voters.clone() {
            effects = engine.process_cut_vote(
                CutVote {
                    round: 1,
                    cut_id: round1_id.clone(),
                    author,
                },
                &tips,
                &oracle,
            );
        }
        assert!(
            engine.safe.contains_key(&1),
            "test setup must actually cross mint_threshold"
        );

        let fetches = find_fetches(&effects);
        assert!(
            fetches.iter().all(|(_, r, id)| *r == 1 && *id == round1_id),
            "every fetch should name round 1's own cut_id: {fetches:?}"
        );
        let mut fetch_targets: Vec<PublicKey> = fetches.iter().map(|(p, _, _)| *p).collect();
        fetch_targets.sort();
        let mut expected = voters;
        expected.sort();
        assert_eq!(
            fetch_targets, expected,
            "the fetch should be addressed to exactly the witnesses whose votes were counted"
        );
        assert_eq!(
            engine.pending_cut_fetch.get(&(1, round1_id)),
            Some(&engine.cut_round),
            "the fetch should be latched for retry bookkeeping"
        );
    }

    #[test]
    fn buffered_child_with_unknown_parent_triggers_fetch_to_committee() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round2_leader = agb::proposer(&committee, 3);
        let mut engine = CutEngine::new(keys[0], committee.clone(), 1_000);

        let unknown_parent = Digest([42; 32]);
        let round2_cut = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: unknown_parent.clone(),
            tips: tips.clone(),
        };

        let effects = engine.process_cut_proposal(round2_cut, &tips, &oracle);

        assert!(
            !engine.pending_cut_children.is_empty(),
            "the proposal should still be buffered, exactly as before this fix"
        );
        let fetches = find_fetches(&effects);
        assert!(
            fetches
                .iter()
                .all(|(_, r, id)| *r == 1 && *id == unknown_parent),
            "the fetch should name round 1 (the best-effort round - 1 guess) and the \
             unknown parent digest: {fetches:?}"
        );
        let mut fetch_targets: Vec<PublicKey> = fetches.iter().map(|(p, _, _)| *p).collect();
        fetch_targets.sort();
        let mut expected: Vec<PublicKey> =
            keys.iter().filter(|k| **k != keys[0]).copied().collect();
        expected.sort();
        assert_eq!(
            fetch_targets, expected,
            "with no narrower evidence, the fetch should go to every other committee member"
        );
    }

    #[test]
    fn served_proposal_matching_request_unblocks_reparents_and_commits() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let observer = *keys
            .iter()
            .find(|k| **k != round1_leader && **k != round2_leader)
            .expect("n=4 has a non-leader");
        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        let round1_cut = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let round1_id = round1_cut.id();
        let round2_cut = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: round1_id.clone(),
            tips: tips.clone(),
        };
        let round2_id = round2_cut.id();

        // Seed a pending child and its parent fetch.
        engine
            .pending_cut_children
            .insert((2, round1_id.clone()), vec![round2_cut.clone()]);
        engine
            .pending_cut_fetch
            .insert((1, round1_id.clone()), engine.cut_round());

        for author in keys.iter().copied() {
            let effects = engine.process_decide(Decide {
                id: round2_id.clone(),
                round: 2,
                author,
            });
            assert!(find_commits(&effects, 2).is_empty());
        }
        assert!(engine.committed.contains_key(&2));

        let effects = engine.on_cut_serve(round1_cut, &tips, &oracle);

        assert_eq!(
            engine.cut_round_by_id.get(&round1_id),
            Some(&1),
            "the served proposal should have been recorded"
        );
        assert!(
            !engine
                .pending_cut_children
                .contains_key(&(2, round1_id.clone())),
            "the buffered round-2 child should have been reparented"
        );
        assert!(
            find_vote_for_round(&effects, 2).is_some(),
            "the reparented round-2 proposal should have been voted on"
        );
        assert_eq!(
            find_commits(&effects, 2),
            vec![(2, tips.clone())],
            "round 2's already-quorate Decide should now commit"
        );
        assert!(
            !engine.pending_cut_fetch.contains_key(&(1, round1_id)),
            "the satisfied fetch should be cleared"
        );
    }

    /// Rejects a served proposal that matches no outstanding request.
    #[test]
    fn served_proposal_not_matching_any_request_is_rejected() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let leader = agb::proposer(&committee, 2);
        let mut engine = CutEngine::new(keys[0], committee, 1_000);

        let proposal = CutProposal {
            round: 1,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let cut_id = proposal.id();

        // The outstanding request is for another round.
        engine.pending_cut_fetch.insert((2, cut_id.clone()), 1);

        let effects = engine.on_cut_serve(proposal, &tips, &oracle);

        assert!(
            effects.is_empty(),
            "a serve matching no requested pair must produce no effects"
        );
        assert!(
            !engine.cut_round_by_id.contains_key(&cut_id),
            "an unmatched serve must not be recorded"
        );
        assert!(engine.cut_proposals.is_empty());
        assert!(
            engine.pending_cut_fetch.contains_key(&(2, cut_id)),
            "the unrelated pending entry must be untouched"
        );
    }

    /// Serves a held proposal once per requester and rejects pruned rounds.
    #[test]
    fn on_cut_fetch_answers_when_held_once_per_requester_and_respects_gc_floor() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let leader = agb::proposer(&committee, 2);
        let mut engine = CutEngine::new(keys[0], committee, 1_000);

        let proposal = CutProposal {
            round: 3,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let cut_id = proposal.id();
        engine.record_cut_proposal(proposal);

        let requester = keys[1];
        let effects = engine.on_cut_fetch(requester, 3, cut_id.clone());
        match effects.as_slice() {
            [CutEffect::ServeTo {
                peer,
                proposal: served,
            }] => {
                assert_eq!(*peer, requester);
                assert_eq!(served.id(), cut_id);
            }
            other => panic!("expected exactly one ServeTo effect, got {other:?}"),
        }

        let effects = engine.on_cut_fetch(requester, 3, cut_id.clone());
        assert!(
            effects.is_empty(),
            "the same requester must not be answered twice"
        );

        let other_requester = keys[2];
        let effects = engine.on_cut_fetch(other_requester, 3, cut_id.clone());
        assert_eq!(
            effects.len(),
            1,
            "a different requester gets its own answer"
        );

        engine.prune_below(4);
        let fresh_requester = keys[3];
        let effects = engine.on_cut_fetch(fresh_requester, 3, cut_id);
        assert!(
            effects.is_empty(),
            "a round pruned below the GC floor must not be served"
        );
    }

    /// Proposal fetches retry only after `FETCH_RETRY_ROUNDS`.
    #[test]
    fn cut_fetch_retry_backoff_holds_until_the_window_elapses() {
        let (committee, keys) = committee_of(4);
        let mut engine = CutEngine::new(keys[0], committee, 1_000);
        let cut_id = Digest([7; 32]);
        let targets = vec![keys[1], keys[2]];

        let first = engine.ensure_cut_fetch(1, &cut_id, targets.clone());
        assert_eq!(first.len(), 2, "the first call fans out to every target");

        // An immediate retry is suppressed.
        let again = engine.ensure_cut_fetch(1, &cut_id, targets.clone());
        assert!(again.is_empty(), "retried too soon -- must not re-fan");

        // Stay below the retry threshold.
        engine.cut_round = 1 + CutEngine::FETCH_RETRY_ROUNDS - 1;
        let still_too_soon = engine.ensure_cut_fetch(1, &cut_id, targets.clone());
        assert!(
            still_too_soon.is_empty(),
            "one round short of the window -- still no-op"
        );

        engine.cut_round = 1 + CutEngine::FETCH_RETRY_ROUNDS;
        let retried = engine.ensure_cut_fetch(1, &cut_id, targets);
        assert_eq!(retried.len(), 2, "past the retry window -- fans out again");
    }

    /// A certified timeout advances the round and retries pending children.
    #[test]
    fn timeout_path_advances_round_and_retries_pending_child() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;

        // Use distinct leaders for the two rounds.
        let observer = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        assert_ne!(
            observer, round2_leader,
            "test setup needs two distinct leaders"
        );

        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        // The round-1 leader stays silent.

        // Buffer round 2 until round 1 is certified as timed out.
        let pending_child = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let effects = engine.process_cut_proposal(pending_child.clone(), &tips, &oracle);
        assert!(
            effects.is_empty(),
            "round 2 is not yet safe, nothing to do yet"
        );
        assert!(
            !engine.pending_cut_children.is_empty(),
            "the round-2 proposal should be buffered pending round 1's resolution"
        );

        let mut effects = engine.process_cut_timer(1, &tips, &oracle);
        assert!(matches!(
            effects.as_slice(),
            [CutEffect::Broadcast(CutOut::Timeout(t))] if t.round == 1
        ));

        // Reach the timeout threshold.
        let mut others = keys.iter().filter(|k| **k != observer);
        loop {
            if effects.iter().any(
                |e| matches!(e, CutEffect::Broadcast(CutOut::TimeoutAccept(a)) if a.round == 1),
            ) {
                break;
            }
            let author = *others.next().expect("enough members to reach quorum");
            let timeout = Timeout { round: 1, author };
            effects = engine.process_timeout(timeout, &tips, &oracle);
        }
        assert!(!engine.certified_timed_out.contains(&1));

        // Reach the timeout-accept threshold.
        let mut others = keys.iter().filter(|k| **k != observer);
        let mut saw_cert = false;
        while !saw_cert {
            let author = *others.next().expect("enough members to reach quorum");
            let accept = TimeoutAccept { round: 1, author };
            effects = engine.process_timeout_accept(accept, &tips, &oracle);
            saw_cert = engine.certified_timed_out.contains(&1);
        }

        assert!(
            engine.certified_timed_out.contains(&1),
            "round 1 is certified timed out"
        );
        assert_eq!(
            engine.cut_round, 2,
            "cut_round should advance past the timed-out round"
        );
        assert!(
            engine.pending_cut_children.is_empty(),
            "the pending round-2 child should have been retried"
        );
        assert!(
            find_vote_for_round(&effects, 2).is_some(),
            "the retried round-2 proposal should have been voted on"
        );
    }

    /// The tip gate blocks voting when any proposal tip is unavailable.
    #[test]
    fn gate_tips_blocks_vote_when_tip_unavailable() {
        let (committee, keys) = committee_of(4);
        let leader = agb::proposer(&committee, 2);
        let unavailable_author = keys.iter().find(|k| **k != leader).copied().unwrap();
        let proposal_tips = sample_tips(&keys);
        let proposal = CutProposal {
            round: 1,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: proposal_tips,
        };
        let oracle = DenyAuthor(unavailable_author);
        let dummy_tips = Cut::new();

        let mut gated = CutEngine::new(keys[2], committee.clone(), 1_000);
        assert_ne!(keys[2], leader);
        let effects = gated.process_cut_proposal(proposal.clone(), &dummy_tips, &oracle);
        assert!(
            find_vote_for_round(&effects, 1).is_none(),
            "gate_tips defaults to true and one tip is unavailable -- no vote"
        );

        let mut ungated = CutEngine::new(keys[2], committee, 1_000).with_gate_tips(false);
        let effects = ungated.process_cut_proposal(proposal, &dummy_tips, &oracle);
        assert!(
            find_vote_for_round(&effects, 1).is_some(),
            "gate_tips: false reproduces upstream's blind vote for the same input"
        );
    }

    #[test]
    fn queue_with_invalid_sibling_still_processes_valid_one() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;

        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let not_round2_leader = keys.iter().copied().find(|k| *k != round2_leader).unwrap();

        let mut engine = CutEngine::new(round1_leader, committee, 1_000);

        let parent = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let parent_id = parent.id();

        let invalid_child = CutProposal {
            round: 2,
            proposer: not_round2_leader,
            parent_cut: parent_id.clone(),
            tips: tips.clone(),
        };
        let valid_child = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: parent_id.clone(),
            tips: tips.clone(),
        };
        let valid_child_id = valid_child.id();

        engine
            .pending_cut_children
            .insert((2, parent_id.clone()), vec![invalid_child, valid_child]);

        // Process both children after their parent arrives.
        let effects = engine.process_cut_proposal(parent, &tips, &oracle);

        assert!(
            !engine.pending_cut_children.contains_key(&(2, parent_id)),
            "both siblings should have been drained from the pending queue"
        );
        let vote =
            find_vote_for_round(&effects, 2).expect("the valid sibling should have been voted on");
        assert_eq!(vote.cut_id, valid_child_id);
    }

    #[test]
    fn prune_below_is_exact() {
        let (committee, _keys) = committee_of(4);
        let mut engine = CutEngine::new(key(1), committee, 1_000);

        let d1 = Digest([1; 32]);
        let d2 = Digest([2; 32]);
        let proposal_at = |round: CutRound| CutProposal {
            round,
            ..CutProposal::default()
        };

        engine
            .cut_vote_aggregators
            .insert((1, d1.clone()), CutVoteAggregator::new());
        engine
            .cut_vote_aggregators
            .insert((2, d2.clone()), CutVoteAggregator::new());
        engine
            .timeouts_aggregators
            .insert(1, TimeoutAggregator::new());
        engine
            .timeouts_aggregators
            .insert(2, TimeoutAggregator::new());
        engine
            .timeout_accept_aggregators
            .insert(1, TimeoutAcceptAggregator::new());
        engine
            .timeout_accept_aggregators
            .insert(2, TimeoutAcceptAggregator::new());
        engine.cut_proposals.insert((1, d1.clone()), proposal_at(1));
        engine.cut_proposals.insert((2, d2.clone()), proposal_at(2));
        engine
            .pending_cut_children
            .insert((1, d1.clone()), vec![proposal_at(1)]);
        engine
            .pending_cut_children
            .insert((2, d2.clone()), vec![proposal_at(2)]);
        engine.cut_round_by_id.insert(d1.clone(), 1);
        engine.cut_round_by_id.insert(d2.clone(), 2);
        engine.leader_cut_by_round.insert(1, d1.clone());
        engine.leader_cut_by_round.insert(2, d2.clone());
        engine.safe.insert(1, d1.clone());
        engine.safe.insert(2, d2.clone());
        engine
            .decide_aggregators
            .insert((1, d1.clone()), DecideAggregator::new());
        engine
            .decide_aggregators
            .insert((2, d2.clone()), DecideAggregator::new());
        engine.committed.insert(
            1,
            Decide {
                id: d1.clone(),
                round: 1,
                author: key(1),
            },
        );
        engine.committed.insert(
            2,
            Decide {
                id: d2.clone(),
                round: 2,
                author: key(1),
            },
        );
        for set in [
            &mut engine.sent_cut_votes,
            &mut engine.proposed_cut_rounds,
            &mut engine.voted,
            &mut engine.sent_commit_rounds,
            &mut engine.timed_out,
            &mut engine.sent_timeout_accepts,
            &mut engine.certified_timed_out,
            &mut engine.scheduled_cut_timers,
        ] {
            set.insert(1);
            set.insert(2);
        }
        engine.pending_cut_fetch.insert((1, d1.clone()), 1);
        engine.pending_cut_fetch.insert((2, d2.clone()), 2);
        engine.fetch_answered.insert((1, d1.clone(), key(1)));
        engine.fetch_answered.insert((2, d2.clone(), key(1)));

        engine.prune_below(2);

        assert!(!engine.cut_vote_aggregators.contains_key(&(1, d1.clone())));
        assert!(engine.cut_vote_aggregators.contains_key(&(2, d2.clone())));
        assert!(!engine.timeouts_aggregators.contains_key(&1));
        assert!(engine.timeouts_aggregators.contains_key(&2));
        assert!(!engine.timeout_accept_aggregators.contains_key(&1));
        assert!(engine.timeout_accept_aggregators.contains_key(&2));
        assert!(!engine.cut_proposals.contains_key(&(1, d1.clone())));
        assert!(engine.cut_proposals.contains_key(&(2, d2.clone())));
        assert!(!engine.pending_cut_children.contains_key(&(1, d1.clone())));
        assert!(engine.pending_cut_children.contains_key(&(2, d2.clone())));
        assert!(
            !engine.cut_round_by_id.contains_key(&d1),
            "cut_round_by_id should be cleaned up alongside cut_proposals"
        );
        assert!(engine.cut_round_by_id.contains_key(&d2));
        assert!(!engine.leader_cut_by_round.contains_key(&1));
        assert!(engine.leader_cut_by_round.contains_key(&2));
        assert!(!engine.safe.contains_key(&1));
        assert!(engine.safe.contains_key(&2));
        assert!(!engine.pending_cut_fetch.contains_key(&(1, d1.clone())));
        assert!(engine.pending_cut_fetch.contains_key(&(2, d2.clone())));
        assert!(!engine.fetch_answered.contains(&(1, d1.clone(), key(1))));
        assert!(engine.fetch_answered.contains(&(2, d2.clone(), key(1))));
        assert!(!engine.decide_aggregators.contains_key(&(1, d1)));
        assert!(engine.decide_aggregators.contains_key(&(2, d2)));
        assert!(!engine.committed.contains_key(&1));
        assert!(engine.committed.contains_key(&2));
        for set in [
            &engine.sent_cut_votes,
            &engine.proposed_cut_rounds,
            &engine.voted,
            &engine.sent_commit_rounds,
            &engine.timed_out,
            &engine.sent_timeout_accepts,
            &engine.certified_timed_out,
            &engine.scheduled_cut_timers,
        ] {
            assert!(!set.contains(&1));
            assert!(set.contains(&2));
        }

        // The new floor rejects round 1.
        assert!(engine
            .sanitize_timeout_accept(&TimeoutAccept {
                round: 1,
                author: key(1)
            })
            .is_err());
        assert!(engine
            .sanitize_timeout_accept(&TimeoutAccept {
                round: 2,
                author: key(1)
            })
            .is_ok());

        // Pruning to an earlier floor has no effect.
        engine.prune_below(1);
        assert!(engine.safe.contains_key(&2));
    }

    #[test]
    fn prune_below_covers_bracha_ready_state() {
        let (committee, _keys) = committee_of(4);
        let mut engine = CutEngine::new(key(1), committee, 1_000).with_variant(Variant::Bracha);

        let d1 = Digest([1; 32]);
        let d2 = Digest([2; 32]);
        engine
            .cut_ready_aggregators
            .insert((1, d1.clone()), CutReadyAggregator::new());
        engine
            .cut_ready_aggregators
            .insert((2, d2.clone()), CutReadyAggregator::new());
        engine.sent_cut_ready.insert(1);
        engine.sent_cut_ready.insert(2);

        engine.prune_below(2);

        assert!(!engine.cut_ready_aggregators.contains_key(&(1, d1)));
        assert!(engine.cut_ready_aggregators.contains_key(&(2, d2)));
        assert!(!engine.sent_cut_ready.contains(&1));
        assert!(engine.sent_cut_ready.contains(&2));

        engine.prune_below(1);
        assert!(engine.sent_cut_ready.contains(&2));
    }
}
