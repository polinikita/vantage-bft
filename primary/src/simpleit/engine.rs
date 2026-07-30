// Simple-IT cut-consensus state machine (stage 2 of a port from the upstream
// `simpleit/Opt-Mempool-Simple-IT-Failure` branch's primary/src/core.rs). Ports exactly
// 25 named methods (the cut-consensus half of upstream's `Core`); everything else in
// that file -- header/vote/certificate processing, the Autobahn view-based consensus
// (`process_consensus_*`), the run loop, every network/store/channel field -- is
// Autobahn residue or data-plane wiring and is not ported. See each method's doc
// comment below for its exact upstream line range.
//
// Architecture: `CutEngine` is a pure state machine. It never holds a `ReliableSender`,
// a `Wire`, a `LaneManager`, a `Store`, or any channel -- upstream's `Core` holds all of
// these, and removing them is the entire point of this port. Every public method
// returns `Vec<CutEffect>` (see effects.rs) in place of upstream's direct
// `self.network.broadcast(...)`/`self.tx_committer.send(...)` calls; the caller (a
// not-yet-written production core, or this module's own tests) executes those effects
// against the real transport/timer/committer. This mirrors
// `primary/src/vantage/agb.rs`'s `AgbEngine` and `primary/src/vantage/control.rs`'s
// `ControlLog` -- both effect-returning, network-free engines already in this crate --
// rather than upstream's own style.
//
// Two oracles are *given* to the engine rather than held by it:
//   1. `tips: &Cut` -- upstream's `current_cut()` reads `self.current_certified_tips`,
//      an Autobahn-DAG field this engine does not have. The caller builds this fresh,
//      per call, from `LaneManager::c_candidate(author)` for each committee member
//      (`BlockRef = (PublicKey, Height, Digest)` maps onto `(author, Proposal
//      { header_digest, height })`) -- `CutEngine` never imports `LaneManager`.
//   2. `oracle: &dyn TipOracle` -- the f+1 tip-availability gate (deviation 3 below)
//      needs to ask "have I seen enough evidence for this tip", which is also
//      `LaneManager` state. The trait is defined here; the not-yet-written production
//      core implements it over `LaneManager::is_q_available(r, validity_threshold())`.
//      `&dyn` (not `&impl`) deliberately: nearly every one of the 25 methods can
//      transitively reach a vote or a re-propose decision (see `try_propose_cut_for_
//      current_round`'s callers), so `tips`/`oracle` thread through almost the entire
//      call graph -- a generic parameter would have to repeat on every one of those
//      signatures for no benefit, since the engine only ever calls one method on it.
//
// Required deviations from upstream (beyond the two oracle-passing changes above,
// which are themselves required -- see the task brief):
//   1. Leader schedule: `leader_for_round` below, not `leader.rs`/`LeaderElector`.
//   2. Prunable state is `BTreeMap`/`BTreeSet`, never `HashMap`/`HashSet`; `prune_below`
//      is the one `split_off`-based GC entry point (no `retain` anywhere).
//   3. The f+1 tip-availability gate in `process_cut_proposal`, behind `gate_tips`
//      (defaults to `true`, paper-faithful; upstream never checks `proposal.tips` at
//      all -- `gate_tips: false` reproduces that).
//   4. `process_cut_proposal`'s internal queue no longer aborts (and silently drops
//      every already-dequeued sibling) on one bad proposal -- see that method's doc
//      comment.
//   5. No Autobahn types anywhere (`Slot`/`View`/`QC`/`TC`/`CommitQC`/`ConsensusMessage`);
//      `CutRound` (from `simpleit::messages`) is the one round type. `agb::proposer`
//      takes a `View` -- `leader_for_round` is the one, explicitly documented place
//      that type ever appears, as a same-width conversion (`View = CutRound = u64`).
//
// None of the ported methods are `async fn`, unlike upstream. Every reason upstream's
// versions were async is gone here: network broadcasts and the committer channel send
// are replaced by effect values (no `.await` left to perform), and the three trivial
// `Timeout::new`/`TimeoutAccept::new`/`Decide::new` upstream `async fn` constructors
// (stage 1, primary/src/simpleit/messages.rs) are bypassed via plain struct literals
// (their fields are all `pub`) rather than requiring an async context for a
// constructor that does no actual `.await` work. This matches `AgbEngine`/`ControlLog`,
// neither of which has a single `async fn` either.

use crate::error::{DagError, DagResult};
use crate::messages::Proposal;
use crate::simpleit::aggregators::{
    CutVoteAggregator, DecideAggregator, TimeoutAcceptAggregator, TimeoutAggregator,
};
use crate::simpleit::effects::{CutEffect, CutOut};
use crate::simpleit::messages::{
    Cut, CutCertificate, CutProposal, CutRound, CutVote, Decide, Timeout, TimeoutAccept,
    TimeoutCert,
};
use crate::vantage::agb;
use config::{Committee, Stake};
use crypto::{Digest, Hash as _, PublicKey};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

/// The messages `CutEngine` consumes, plus the one non-message input (a previously
/// armed timer firing). Wiring this to the real wire enum (`PrimaryMessage`) and to
/// `vantage::node::Inbound` is a separate, later task -- this type is deliberately not
/// related to either.
#[derive(Clone, Debug)]
pub enum Inbound {
    CutProposal(CutProposal),
    CutVote(CutVote),
    CutCertificate(CutCertificate),
    Decide(Decide),
    Timeout(Timeout),
    TimeoutAccept(TimeoutAccept),
    /// A previously `CutEffect::ArmTimer`-requested deadline for this round has
    /// elapsed. Corresponds to upstream's `cut_timer_futures` yielding a round.
    TimerFired(CutRound),
}

/// Tip-availability oracle for the f+1 gate (deviation 3): "has this party itself seen
/// at least f+1 evidence for `tip`, authored by `author`". `CutEngine` never
/// implements this -- the production core implements it over
/// `LaneManager::is_q_available(&(author, tip.height, tip.header_digest), committee.
/// validity_threshold())`; a test implements it directly (see `mod tests` below).
pub trait TipOracle {
    fn available_at_validity(&self, author: &PublicKey, tip: &Proposal) -> bool;
}

/// Fault-injection scaffolding for benchmark/simulation runs -- NOT protocol. Upstream
/// (`Core`) always carried these five fields (`crash_author`, `crash_on_proposal`,
/// `crash_duration`, `cut_proposal_count`, `crash_triggered`), using
/// `crash_on_proposal == 0` as an always-present "disabled" sentinel. Here they exist
/// at all only when a benchmark opts in via `CutEngine::with_crash_sim` -- production
/// (`CutEngine::crash_sim: None`, the default) carries none of this bookkeeping,
/// including the proposal counter, which has no protocol meaning of its own (its only
/// reader is `CutEngine::maybe_skip_cut_proposal`).
#[derive(Clone, Debug)]
pub struct CrashSim {
    author: PublicKey,
    on_proposal: u64,
    duration_ms: u64,
    proposal_count: u64,
    triggered: bool,
}

impl CrashSim {
    pub fn new(author: PublicKey, on_proposal: u64, duration_ms: u64) -> Self {
        Self {
            author,
            on_proposal,
            duration_ms,
            proposal_count: 0,
            triggered: false,
        }
    }
}

/// The Simple-IT cut-consensus state machine. See the module doc comment above for the
/// architecture; see each method below for its upstream provenance.
pub struct CutEngine {
    name: PublicKey,
    committee: Committee,
    /// Upstream `Core::timeout_delay`: the relative delay `schedule_cut_timer` arms
    /// each round's timeout deadline for.
    timeout_delay: u64,
    /// Deviation 3: gates `process_cut_proposal`'s vote on f+1 tip availability when
    /// `true` (the paper-faithful default). `false` reproduces upstream exactly (votes
    /// without ever inspecting `proposal.tips`).
    gate_tips: bool,
    /// Deviation from upstream's always-present crash fields -- see `CrashSim`'s doc
    /// comment.
    crash_sim: Option<CrashSim>,

    /// Upstream `Core::cut_round`. Starts at 1 (confirmed by reading upstream's own
    /// `Core::spawn` constructor -- NOT 0; round 0 is reserved as `safe_cut_parent`'s
    /// genesis-parent sentinel and is never an actual contested round; see
    /// `leader_for_round`'s doc comment for why this discrepancy from the task brief
    /// does not affect the round -> leader mapping either way).
    cut_round: CutRound,
    /// Upstream `Core::highest_certified_cut`.
    highest_certified_cut: Digest,
    /// The floor `prune_below` was last called with (0 if never). Doubles as
    /// `sanitize_timeout_accept`'s staleness floor -- see that method's doc comment for
    /// why this replaces upstream's Autobahn-typed `gc_round: Height`.
    gc_floor: CutRound,

    /// Upstream `cut_vote_aggregators: HashMap<Digest, CutVoteAggregator>`. Re-keyed
    /// per deviation 2: every read site (`process_cut_vote`) already has `vote.round`
    /// in hand, so keying by `(CutRound, Digest)` costs nothing at the lookup sites and
    /// makes this `prune_below`-able by `split_off`. Round-prunable; covered.
    cut_vote_aggregators: BTreeMap<(CutRound, Digest), CutVoteAggregator>,
    /// Upstream `timeouts_aggregators: HashMap<u64, TimeoutAggregator>`. Already
    /// round-keyed; container swap only. Round-prunable; covered.
    timeouts_aggregators: BTreeMap<CutRound, TimeoutAggregator>,
    /// Upstream `timeout_accept_aggregators: HashMap<u64, TimeoutAcceptAggregator>`.
    /// Already round-keyed; container swap only. Round-prunable; covered.
    timeout_accept_aggregators: BTreeMap<CutRound, TimeoutAcceptAggregator>,
    /// Upstream `cut_proposals: HashMap<Digest, CutProposal>`. Re-keyed per deviation 2
    /// exactly as `cut_vote_aggregators` above: every read site
    /// (`process_cut_proposal`'s dedup check, `emit_commit_to_committer`) already has
    /// the round in hand. Round-prunable; covered. `prune_below` also uses this map's
    /// own split_off to drive `cut_round_by_id`'s cleanup (see that field).
    cut_proposals: BTreeMap<(CutRound, Digest), CutProposal>,
    /// Upstream `pending_cut_children: HashMap<Digest, Vec<CutProposal>>` (keyed by the
    /// *parent* cut's digest). Re-keyed as `(CutRound, Digest)` where the round is each
    /// buffered *child* proposal's own round and the digest is still the cited parent
    /// -- a strict refinement of upstream's grouping (splits one upstream bucket into
    /// one bucket per distinct child round citing that parent; every buffered proposal
    /// upstream would ever have held is still held, just possibly in a different
    /// bucket alongside fewer/other siblings), not a behavior change. This makes
    /// `prune_below` a clean `split_off` (a child whose own round is already GC'd can
    /// never be validly processed anyway). The cost: reparenting when a parent becomes
    /// known (`process_cut_proposal`) can no longer do a single `HashMap::remove`,
    /// since the round component of a pending child's key is not known in advance from
    /// the parent's digest alone -- it scans this map's (bounded-by-current-backlog)
    /// keys for matches. That scan is a lookup, not the GC-pruning operation the "no
    /// retain" rule targets, so it is not a violation of that rule. Round-prunable;
    /// covered.
    pending_cut_children: BTreeMap<(CutRound, Digest), Vec<CutProposal>>,
    /// Upstream `cut_round_by_id: HashMap<Digest, u64>`. Its one read site
    /// (`safe_cut_parent`) looks up a round FROM a digest -- the opposite direction
    /// from every other map above, so it cannot itself be re-keyed by round without
    /// destroying that lookup's whole purpose (the round is exactly the unknown being
    /// looked up). Kept `Digest`-keyed (switched to `BTreeMap` for consistency, not for
    /// `split_off`-ability on its own key). NOT independently round-prunable by its own
    /// key -- see `prune_below`'s doc comment for how it is still covered, by riding
    /// `cut_proposals`' split_off (the two maps are populated together, one insert
    /// each, only by `record_cut_proposal`).
    cut_round_by_id: BTreeMap<Digest, CutRound>,
    /// Upstream `leader_cut_by_round: HashMap<u64, Digest>`. Already round-keyed.
    /// Round-prunable; covered.
    leader_cut_by_round: BTreeMap<CutRound, Digest>,
    /// Upstream `cut_certificates: HashMap<u64, CutCertificate>`. Already round-keyed.
    /// Round-prunable; covered.
    cut_certificates: BTreeMap<CutRound, CutCertificate>,
    /// Upstream `decide_aggregators: HashMap<(u64, Digest), DecideAggregator>`. Already
    /// tuple-keyed by round; container swap only. Round-prunable; covered.
    decide_aggregators: BTreeMap<(CutRound, Digest), DecideAggregator>,
    /// Upstream `decides_by_round: HashMap<u64, Decide>`. Already round-keyed.
    /// Round-prunable; covered.
    decides_by_round: BTreeMap<CutRound, Decide>,
    /// Upstream `voted_cut_rounds: HashSet<u64>`. Round-prunable; covered.
    voted_cut_rounds: BTreeSet<CutRound>,
    /// Upstream `proposed_cut_rounds: HashSet<u64>`. Round-prunable; covered.
    proposed_cut_rounds: BTreeSet<CutRound>,
    /// Upstream `sent_decide_rounds: HashSet<u64>`. Round-prunable; covered.
    sent_decide_rounds: BTreeSet<CutRound>,
    /// Upstream `sent_commit_rounds: HashSet<u64>`. Round-prunable; covered.
    sent_commit_rounds: BTreeSet<CutRound>,
    /// Upstream `sent_timeouts: HashSet<u64>`. Round-prunable; covered.
    sent_timeouts: BTreeSet<CutRound>,
    /// Upstream `sent_timeout_accepts: HashSet<u64>`. Round-prunable; covered.
    sent_timeout_accepts: BTreeSet<CutRound>,
    /// Upstream `certified_timed_out: HashSet<u64>`. Round-prunable; covered.
    certified_timed_out: BTreeSet<CutRound>,
    /// Upstream `scheduled_cut_timers: HashSet<u64>`. Round-prunable; covered.
    scheduled_cut_timers: BTreeSet<CutRound>,
}

impl CutEngine {
    pub fn new(name: PublicKey, committee: Committee, timeout_delay: u64) -> Self {
        Self {
            name,
            committee,
            timeout_delay,
            gate_tips: true,
            crash_sim: None,
            cut_round: 1,
            highest_certified_cut: Digest::default(),
            gc_floor: 0,
            cut_vote_aggregators: BTreeMap::new(),
            timeouts_aggregators: BTreeMap::new(),
            timeout_accept_aggregators: BTreeMap::new(),
            cut_proposals: BTreeMap::new(),
            pending_cut_children: BTreeMap::new(),
            cut_round_by_id: BTreeMap::new(),
            leader_cut_by_round: BTreeMap::new(),
            cut_certificates: BTreeMap::new(),
            decide_aggregators: BTreeMap::new(),
            decides_by_round: BTreeMap::new(),
            voted_cut_rounds: BTreeSet::new(),
            proposed_cut_rounds: BTreeSet::new(),
            sent_decide_rounds: BTreeSet::new(),
            sent_commit_rounds: BTreeSet::new(),
            sent_timeouts: BTreeSet::new(),
            sent_timeout_accepts: BTreeSet::new(),
            certified_timed_out: BTreeSet::new(),
            scheduled_cut_timers: BTreeSet::new(),
        }
    }

    /// Deviation 3's switch. Defaults to `true` (paper-faithful); pass `false` to
    /// reproduce upstream's blind-vote behavior.
    pub fn with_gate_tips(mut self, gate_tips: bool) -> Self {
        self.gate_tips = gate_tips;
        self
    }

    /// Opt into the fault-injection scaffolding -- see `CrashSim`'s doc comment.
    pub fn with_crash_sim(mut self, crash_sim: CrashSim) -> Self {
        self.crash_sim = Some(crash_sim);
        self
    }

    /// Single dispatch entry point over `Inbound` -- not itself one of the 25 ported
    /// methods (upstream has no equivalent single function; its `run()` select-loop,
    /// not ported, plays this role there), but required by the "engine consumes
    /// `Inbound`" architecture. Mirrors upstream's own dispatch shape exactly:
    /// `TimeoutAccept` is `sanitize_timeout_accept`-checked before `process_timeout_
    /// accept` (matching upstream's sanitize-then-process pattern), a wire `Timeout`
    /// goes through `handle_timeout` (not `process_timeout` directly -- that direct
    /// path is reserved for `process_cut_timer`'s own locally-generated timeout,
    /// exactly as upstream keeps the two named entry points distinct).
    pub fn handle(&mut self, inbound: Inbound, tips: &Cut, oracle: &dyn TipOracle) -> Vec<CutEffect> {
        match inbound {
            Inbound::CutProposal(p) => self.process_cut_proposal(p, tips, oracle),
            Inbound::CutVote(v) => self.process_cut_vote(v, tips, oracle),
            Inbound::CutCertificate(c) => self.process_cut_certificate(c, tips, oracle),
            Inbound::Decide(d) => self.process_decide(d),
            Inbound::Timeout(t) => self.handle_timeout(t, tips, oracle),
            Inbound::TimeoutAccept(a) => {
                if self.sanitize_timeout_accept(&a).is_err() {
                    return Vec::new();
                }
                self.process_timeout_accept(a, tips, oracle)
            }
            Inbound::TimerFired(r) => self.process_cut_timer(r, tips, oracle),
        }
    }

    /// Deviation 1: upstream's `leader.rs`/`LeaderElector`/`fixed_leader_order` are not
    /// ported; every leader lookup in this engine goes through this one function.
    ///
    /// The mapping: `agb::proposer(committee, view)` computes `names[(view - 1) % n]`
    /// over `committee.authorities.keys()` (`BTreeMap` order, i.e. raw `PublicKey`
    /// byte order) and requires `view >= 1`. Upstream computes `leaders[round % n]`
    /// over a *separately sorted* `Vec` (by `node_id` if every authority has one, else
    /// also raw `PublicKey` order -- `fixed_leader_order`), with `round` always >= 1 in
    /// practice (confirmed by reading upstream's `Core::spawn`: `cut_round: 1`, and it
    /// only ever increases; `round` is never 0 for an actual contested round, only as
    /// `safe_cut_parent`'s parent-round sentinel).
    ///
    /// Choosing `view = round + 1` makes `(view - 1) % n == round % n` an *identity*
    /// (not merely mod-equivalent -- `view - 1` is literally `round`), for every
    /// `round >= 0`. So for every round upstream would ever actually query, this
    /// reproduces the exact same 0-indexed slot into whichever list is in play -- only
    /// the list's *ordering* differs (raw key order here vs upstream's node-id-aware
    /// order there), which is exactly deviation 1's "use ours, not theirs" instruction,
    /// never the arithmetic. `View` (upstream's `u64` alias, from `primary::primary`)
    /// never appears as a named type anywhere in this module -- `round + 1` is a
    /// `CutRound` (`= u64`) value that coerces to `agb::proposer`'s `View` (`= u64`)
    /// parameter because the two are the same underlying type; this call site is the
    /// only place that coercion ever happens.
    fn leader_for_round(&self, round: CutRound) -> PublicKey {
        agb::proposer(&self.committee, round + 1)
    }

    /// `pub` (production wiring, `simpleit::node::SimpleItCore`): a read-only accessor
    /// so the wiring layer can compute its own GC floor (`cut_round.saturating_sub
    /// (gc_window)`) for `prune_below` below -- neither `cut_round` itself nor any
    /// other progress indicator was otherwise exposed. Not one of the 25 ported
    /// methods; upstream has no equivalent (its own GC, if any, would have lived
    /// inside the un-ported `run()` loop with direct field access).
    pub fn cut_round(&self) -> CutRound {
        self.cut_round
    }

    /// GC floor: `split_off`-prune every round-prunable structure at `floor`. No-op if
    /// `floor` is at or behind the current floor (matching `AgbEngine::gc_below`/
    /// `ControlLog::gc_below`'s own monotonic-guard shape). Not one of the 25 ported
    /// methods -- upstream has no GC for this state at all; this is the standing
    /// project rule ("prunable structures... `split_off` at floor, never `retain`
    /// scans") applied proactively.
    ///
    /// `cut_round_by_id` is the one field with no `split_off` of its own (see that
    /// field's doc comment for why: it is keyed by `Digest`, needed for
    /// `safe_cut_parent`'s digest -> round lookup). Instead of a `.retain` scan over
    /// it, this reads the digests `cut_proposals`' own split_off is about to discard --
    /// `record_cut_proposal` populates both maps together for the same cut_id, so the
    /// two are always in lockstep -- and removes exactly those keys: cost proportional
    /// to what is being dropped, not to what survives.
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
        self.pending_cut_children = self
            .pending_cut_children
            .split_off(&(floor, Digest::default()));
        self.decide_aggregators = self
            .decide_aggregators
            .split_off(&(floor, Digest::default()));

        self.timeouts_aggregators = self.timeouts_aggregators.split_off(&floor);
        self.timeout_accept_aggregators = self.timeout_accept_aggregators.split_off(&floor);
        self.leader_cut_by_round = self.leader_cut_by_round.split_off(&floor);
        self.cut_certificates = self.cut_certificates.split_off(&floor);
        self.decides_by_round = self.decides_by_round.split_off(&floor);
        self.voted_cut_rounds = self.voted_cut_rounds.split_off(&floor);
        self.proposed_cut_rounds = self.proposed_cut_rounds.split_off(&floor);
        self.sent_decide_rounds = self.sent_decide_rounds.split_off(&floor);
        self.sent_commit_rounds = self.sent_commit_rounds.split_off(&floor);
        self.sent_timeouts = self.sent_timeouts.split_off(&floor);
        self.sent_timeout_accepts = self.sent_timeout_accepts.split_off(&floor);
        self.certified_timed_out = self.certified_timed_out.split_off(&floor);
        self.scheduled_cut_timers = self.scheduled_cut_timers.split_off(&floor);

        self.gc_floor = floor;
    }

    /// Upstream primary/src/core.rs:448-478.
    pub fn process_cut_vote(&mut self, vote: CutVote, tips: &Cut, oracle: &dyn TipOracle) -> Vec<CutEffect> {
        if vote.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let key = (vote.round, vote.cut_id.clone());
        let aggregator = self.cut_vote_aggregators.entry(key).or_default();
        let Ok(Some(certificate)) = aggregator.append(&vote, &self.committee) else {
            return Vec::new();
        };
        let mut effects = vec![CutEffect::Broadcast(CutOut::CutCertificate(
            certificate.clone(),
        ))];
        effects.extend(self.process_cut_certificate(certificate, tips, oracle));
        effects
    }

    /// Upstream primary/src/core.rs:480-544.
    ///
    /// Deviation 4: upstream's `while let Some(proposal) = queue.pop_front()` uses
    /// `?`/`ensure!` inside the loop for the verify check and the leader-authenticity
    /// check. Either failing aborts `process_cut_proposal` entirely via `Err`, which
    /// (since `queue` was already extended with every sibling `pending_cut_children`
    /// had buffered for the cut that just became known, and `record_cut_proposal`
    /// already ran for the just-recorded parent before the loop even reaches a bad
    /// sibling) silently drops every valid sibling still sitting in `queue` -- they are
    /// simply never dequeued. Fixed by rejecting a bad proposal individually (`continue`)
    /// without discarding the rest of the queue; no check below accepts anything
    /// upstream would have rejected, or rejects anything upstream would have accepted --
    /// only the "one bad item takes the whole batch down with it" failure mode is gone.
    /// (`retry_pending_cut_proposals`'s own `for proposal in ready { ...await?... }`
    /// loop had the identical failure mode one level up; making this function infallible
    /// fixes that call site too, for free, as a consequence of the effect-returning
    /// design rather than a second, separate fix.)
    ///
    /// Deviation 3: the f+1 tip-availability gate sits immediately before the one
    /// voting decision (`voted_cut_rounds.insert`), gating only the vote -- recording
    /// the proposal and reparenting its own pending children happen unconditionally,
    /// exactly as upstream does, since the paper's gate is about "casting a vote", not
    /// about learning of/relaying a proposal. A gate failure does not consume the
    /// per-round vote latch (`voted_cut_rounds`), unlike upstream's original passing
    /// case (which always consumes it): this is deliberate, so a proposal that fails
    /// the gate leaves the round eligible to vote later if some other satisfying event
    /// re-drives processing for it -- this engine does not itself invent such a retry
    /// (none was asked for), it only avoids permanently foreclosing one.
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
                let key = (round, proposal.parent_cut.clone());
                self.pending_cut_children
                    .entry(key)
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

            // Reparent: any proposals buffered while `cut_id` was unknown, across
            // whichever round(s) named it as `parent_cut` -- see `pending_cut_children`'s
            // doc comment for why this is a bounded scan rather than a single removal.
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

            if tips_ok && self.voted_cut_rounds.insert(round) {
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

    /// Upstream primary/src/core.rs:546-576.
    pub fn retry_pending_cut_proposals(&mut self, tips: &Cut, oracle: &dyn TipOracle) -> Vec<CutEffect> {
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

    /// Upstream primary/src/core.rs:609-615. Upstream builds a fresh `BTreeMap` from
    /// `self.current_certified_tips` (a `HashMap`); since the caller now hands us the
    /// cut directly as a `BTreeMap` already (see the module doc comment's oracle-1),
    /// this is just that value, cloned.
    fn current_cut(&self, tips: &Cut) -> Cut {
        tips.clone()
    }

    /// Upstream primary/src/core.rs:617-624.
    fn make_cut_proposal(&self, round: CutRound, parent_cut: Digest, tips: &Cut) -> CutProposal {
        CutProposal {
            round,
            proposer: self.name,
            parent_cut,
            tips: self.current_cut(tips),
        }
    }

    /// Upstream primary/src/core.rs:632-639. Upstream also writes
    /// `self.cut_parents.insert(cut_id, proposal.parent_cut)` here -- `cut_parents` is
    /// dead (write-only upstream; read only from code already commented out there) and
    /// is deliberately not ported at all, per the task brief.
    fn record_cut_proposal(&mut self, proposal: CutProposal) -> Digest {
        let cut_id = proposal.id();
        let round = proposal.round;
        self.cut_round_by_id.insert(cut_id.clone(), round);
        self.cut_proposals.insert((round, cut_id.clone()), proposal);
        cut_id
    }

    /// Upstream primary/src/core.rs:652-687.
    pub fn process_cut_certificate(
        &mut self,
        certificate: CutCertificate,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if certificate.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = certificate.round;
        if self.certified_timed_out.contains(&round) {
            return Vec::new();
        }
        let cut_id = certificate.cut_id.clone();
        self.cut_certificates.entry(round).or_insert(certificate);
        if round + 1 >= self.cut_round {
            self.highest_certified_cut = cut_id.clone();
        }
        self.cut_round = self.cut_round.max(round + 1);
        self.advance_timed_out_cut_rounds();

        let mut effects = Vec::new();
        if self.sent_decide_rounds.insert(round) {
            let decide = Decide {
                id: cut_id,
                round,
                origin: self.name,
                author: self.name,
            };
            effects.push(CutEffect::Broadcast(CutOut::Decide(decide.clone())));
            effects.extend(self.process_decide(decide));
        }

        effects.extend(self.try_propose_cut_for_current_round(tips, oracle));
        effects.extend(self.schedule_cut_timer(self.cut_round));
        effects
    }

    /// Upstream primary/src/core.rs:689-712.
    pub fn process_decide(&mut self, decide: Decide) -> Vec<CutEffect> {
        if decide.verify(&self.committee).is_err() {
            return Vec::new();
        }
        if self.decides_by_round.contains_key(&decide.round) {
            return Vec::new();
        }

        let key = (decide.round, decide.id.clone());
        let aggregator = self.decide_aggregators.entry(key).or_default();
        let Ok(Some(quorum_decide)) = aggregator.append(&decide, &self.committee) else {
            return Vec::new();
        };
        let round = quorum_decide.round;
        self.decides_by_round.entry(round).or_insert(quorum_decide);
        self.try_commit_round(round)
    }

    /// Upstream primary/src/core.rs:714-727.
    fn try_commit_round(&mut self, round: CutRound) -> Vec<CutEffect> {
        let Some(decide) = self.decides_by_round.get(&round) else {
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

    /// Upstream primary/src/core.rs:729-751. Upstream only marks `sent_commit_rounds`
    /// after a successful `self.tx_committer.send(...).await` (skipping it on a send
    /// error, e.g. a dropped receiver); there is no channel here to fail, so once this
    /// engine decides to emit the effect it is unconditionally considered sent -- the
    /// caller's own delivery of the effect is outside the state machine's concern,
    /// exactly as every other `CutEffect::Broadcast` already is.
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

    /// Upstream primary/src/core.rs:753-788.
    pub fn try_propose_cut_for_current_round(
        &mut self,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let round = self.cut_round;
        if self.name != self.leader_for_round(round) {
            return Vec::new();
        }
        if !self.safe_cut_parent(round, &self.highest_certified_cut) {
            return Vec::new();
        }
        if !self.proposed_cut_rounds.insert(round) {
            return Vec::new();
        }
        if let Some(sim) = self.crash_sim.as_mut() {
            sim.proposal_count += 1;
        }
        if self.maybe_skip_cut_proposal(round, "cut") {
            return Vec::new();
        }

        let parent_cut = self.highest_certified_cut.clone();
        let proposal = self.make_cut_proposal(round, parent_cut, tips);
        let mut effects = vec![CutEffect::Broadcast(CutOut::CutProposal(proposal.clone()))];
        effects.extend(self.process_cut_proposal(proposal, tips, oracle));
        effects
    }

    /// Upstream primary/src/core.rs:790-806.
    fn maybe_skip_cut_proposal(&mut self, round: CutRound, source: &'static str) -> bool {
        let Some(sim) = self.crash_sim.as_mut() else {
            return false;
        };
        if sim.triggered
            || sim.on_proposal == 0
            || sim.author != self.name
            || sim.proposal_count != sim.on_proposal
        {
            return false;
        }

        sim.triggered = true;
        log::info!(
            "BENCH event=proposal_skip node={:?} round={} proposal_index={} duration_ms={} source={} one_shot=true",
            self.name,
            round,
            sim.proposal_count,
            sim.duration_ms,
            source
        );
        true
    }

    /// Upstream primary/src/core.rs:808-822. `ArmTimer`'s deadline is computed here
    /// (`Instant::now() + timeout_delay`), matching `control::ControlLog::
    /// enter_round_core`'s own `Effect::ArmControlTimer(r, Instant::now() + ...)` style
    /// -- not `agb::AgbEngine`'s threaded-`now` parameter style -- because upstream's own
    /// `schedule_cut_timer(&mut self, round: u64)` took no `now`-like parameter either;
    /// this preserves that signature exactly rather than introducing one upstream never
    /// had.
    ///
    /// `pub` (production wiring, `simpleit::node::SimpleItCore`): every OTHER caller of
    /// this method is internal (`process_cut_certificate`/`handle_timeout_accept_action`,
    /// both only ever reachable after `cut_round` has already advanced PAST round 1), so
    /// round 1 itself never gets a timer armed this way -- the production wiring calls
    /// this directly, once, at boot (`schedule_cut_timer(1)`), exactly mirroring how
    /// `try_propose_cut_for_current_round` (already `pub`) must also be called directly
    /// at boot for the round-1 leader to ever propose. Zero behavior change: same
    /// one-shot-per-round latch (`scheduled_cut_timers`), same effect.
    pub fn schedule_cut_timer(&mut self, round: CutRound) -> Vec<CutEffect> {
        if self.scheduled_cut_timers.insert(round) {
            log::info!(
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

    /// Upstream primary/src/core.rs:824-838.
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

    /// Upstream primary/src/core.rs:840-848.
    fn advance_timed_out_cut_rounds(&mut self) -> bool {
        let old_cut_round = self.cut_round;
        while self.certified_timed_out.contains(&self.cut_round)
            && self.safe_cut_parent(self.cut_round + 1, &self.highest_certified_cut)
        {
            self.cut_round += 1;
        }
        self.cut_round != old_cut_round
    }

    /// Upstream primary/src/core.rs:850-878.
    pub fn process_cut_timer(
        &mut self,
        round: CutRound,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if round != self.cut_round
            || self.cut_certificates.contains_key(&round)
            || self.certified_timed_out.contains(&round)
            || !self.sent_timeouts.insert(round)
        {
            return Vec::new();
        }

        log::info!("BENCH event=timeout_sent round={} node={:?}", round, self.name);
        let timeout = Timeout {
            round,
            author: self.name,
        };
        let mut effects = vec![CutEffect::Broadcast(CutOut::Timeout(timeout.clone()))];
        effects.extend(self.process_timeout(timeout, tips, oracle));
        effects
    }

    /// Upstream primary/src/core.rs:880-900.
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
        if self.certified_timed_out.contains(&round) || self.cut_certificates.contains_key(&round)
        {
            return Vec::new();
        }

        let aggregator = self.timeouts_aggregators.entry(round).or_default();
        let Ok(Some(())) = aggregator.append(timeout, &self.committee) else {
            return Vec::new();
        };

        let (mut effects, maybe) = self.send_timeout_accept(round);
        if let Some((weight, timeout_cert)) = maybe {
            effects.extend(self.handle_timeout_accept_action(round, weight, timeout_cert, tips, oracle));
        }
        effects
    }

    /// Upstream primary/src/core.rs:902-917.
    fn broadcast_timeout_accept(&self, accept: &TimeoutAccept) -> Vec<CutEffect> {
        vec![CutEffect::Broadcast(CutOut::TimeoutAccept(accept.clone()))]
    }

    /// Upstream primary/src/core.rs:919-930. Returns the broadcast effect (if this call
    /// is the first-ever send for `round` -- upstream's `sent_timeout_accepts.insert`
    /// one-shot latch) alongside the `(weight, maybe-cert)` `record_timeout_accept`
    /// produced for our own accept; `None` in the second position exactly when upstream
    /// would have returned `Ok(None)` (already sent).
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

    /// Upstream primary/src/core.rs:932-950.
    fn record_timeout_accept(&mut self, accept: TimeoutAccept) -> (Stake, Option<TimeoutCert>) {
        if accept.verify(&self.committee).is_err() {
            return (0, None);
        }
        let round = accept.round;
        if self.certified_timed_out.contains(&round) || self.cut_certificates.contains_key(&round)
        {
            return (0, None);
        }

        let aggregator = self.timeout_accept_aggregators.entry(round).or_default();
        aggregator
            .append(accept, &self.committee)
            .unwrap_or((0, None))
    }

    /// Upstream primary/src/core.rs:952-957.
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

    /// Upstream primary/src/core.rs:959-982.
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

    /// Upstream primary/src/core.rs:1015-1017. A thin wrapper, kept distinct from
    /// `process_timeout` because upstream keeps it distinct -- see `handle`'s doc
    /// comment for which call site uses which name.
    pub fn handle_timeout(&mut self, timeout: Timeout, tips: &Cut, oracle: &dyn TipOracle) -> Vec<CutEffect> {
        self.process_timeout(timeout, tips, oracle)
    }

    /// Upstream primary/src/core.rs:1023-1029. Upstream checks `self.gc_round <=
    /// accept.round`, where `gc_round: Height` is an Autobahn header-height GC floor --
    /// a different unit entirely from a cut round, and not ported (see `gc_floor`'s
    /// doc comment on `CutEngine`). This checks the same shape of thing
    /// (`accept.round` is not older than our own GC floor) against `gc_floor`, this
    /// engine's own analogous floor.
    pub fn sanitize_timeout_accept(&self, accept: &TimeoutAccept) -> DagResult<()> {
        ensure!(
            self.gc_floor <= accept.round,
            DagError::CertificateTooOld(accept.digest(), accept.round)
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> PublicKey {
        PublicKey([byte; 32])
    }

    /// `n` committee members, `key(1)..=key(n)`, equal stake -- ascending byte value so
    /// `committee.authorities.keys()` (BTreeMap order) yields exactly `key(1), key(2),
    /// ..., key(n)`, letting tests reason about `leader_for_round`'s output directly.
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

    fn find_certificate(effects: &[CutEffect], round: CutRound) -> bool {
        effects.iter().any(
            |e| matches!(e, CutEffect::Broadcast(CutOut::CutCertificate(c)) if c.round == round),
        )
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

    /// Test 1: happy path end to end, at both n=4 and n=10 -- proposal -> votes to
    /// `optimistic_threshold` -> certificate -> decides to `quorum_threshold` -> commit
    /// emitted exactly once for the round.
    fn happy_path_commit(n: u8) {
        let (committee, keys) = committee_of(n);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2); // round 1 -> view 2, see leader_for_round

        let mut engine = CutEngine::new(leader, committee, 1_000);

        // Propose: the leader broadcasts its own proposal and immediately self-votes
        // (upstream: the leader also processes its own proposal locally).
        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();
        assert!(
            find_vote_for_round(&effects, round).is_some(),
            "leader self-votes for its own proposal"
        );
        assert!(!find_certificate(&effects, round));

        // Votes: bring in other committee members' votes for the same cut_id until the
        // certificate effect appears.
        let mut others = keys.iter().filter(|k| **k != leader);
        loop {
            if find_certificate(&effects, round) {
                break;
            }
            let author = *others
                .next()
                .expect("committee is large enough to reach optimistic_threshold");
            let vote = CutVote {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_vote(vote, &tips, &oracle);
        }

        // Decides: bring in other committee members' decides for the same (round,
        // cut_id) until commit appears. The certificate-forming call above already
        // produced our own self-decide.
        let mut others = keys.iter().filter(|k| **k != leader);
        let mut commits = find_commits(&effects, round);
        while commits.is_empty() {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let decide = Decide {
                id: cut_id.clone(),
                round,
                origin: leader,
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

        // Re-delivering the same decide-quorum-crossing event again must not emit a
        // second commit (the per-round `sent_commit_rounds` latch).
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

    /// Test 2: timeout path -- leader silent, timer fires, `Timeout` reaches quorum,
    /// `TimeoutAccept` amplifies at f+1 and certifies at quorum, the round is marked
    /// timed-out, `cut_round` advances, and a pending child whose parent was skipped is
    /// retried.
    #[test]
    fn timeout_path_advances_round_and_retries_pending_child() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;

        // An observer distinct from round 2's leader (see the assertions below for
        // why that separation matters).
        let observer = agb::proposer(&committee, 2); // round 1's leader, used only as
                                                      // this engine's own identity
        let round2_leader = agb::proposer(&committee, 3);
        assert_ne!(observer, round2_leader, "test setup needs two distinct leaders");

        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        // Round 1's leader stays silent: nobody ever calls try_propose/process_cut_proposal
        // for round 1 on this engine.

        // Before round 1 certifies as timed out, a round-2 proposal citing the
        // (still-genesis) parent is not yet `safe_cut_parent` -- it gets buffered.
        let pending_child = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let effects = engine.process_cut_proposal(pending_child.clone(), &tips, &oracle);
        assert!(effects.is_empty(), "round 2 is not yet safe, nothing to do yet");
        assert!(
            !engine.pending_cut_children.is_empty(),
            "the round-2 proposal should be buffered pending round 1's resolution"
        );

        // The round-1 timer fires.
        let mut effects = engine.process_cut_timer(1, &tips, &oracle);
        assert!(matches!(
            effects.as_slice(),
            [CutEffect::Broadcast(CutOut::Timeout(t))] if t.round == 1
        ));

        // Bring in other committee members' timeouts until quorum_threshold.
        let mut others = keys.iter().filter(|k| **k != observer);
        loop {
            if effects
                .iter()
                .any(|e| matches!(e, CutEffect::Broadcast(CutOut::TimeoutAccept(a)) if a.round == 1))
            {
                break;
            }
            let author = *others.next().expect("enough members to reach quorum");
            let timeout = Timeout { round: 1, author };
            effects = engine.process_timeout(timeout, &tips, &oracle);
        }
        assert!(!engine.certified_timed_out.contains(&1));

        // Bring in other committee members' timeout-accepts. f+1 should amplify our own
        // accept (already sent above, so a no-op) and quorum should certify.
        let mut others = keys.iter().filter(|k| **k != observer);
        let mut saw_cert = false;
        while !saw_cert {
            let author = *others.next().expect("enough members to reach quorum");
            let accept = TimeoutAccept { round: 1, author };
            effects = engine.process_timeout_accept(accept, &tips, &oracle);
            saw_cert = engine.certified_timed_out.contains(&1);
        }

        assert!(engine.certified_timed_out.contains(&1), "round 1 is certified timed out");
        assert_eq!(engine.cut_round, 2, "cut_round should advance past the timed-out round");
        assert!(
            engine.pending_cut_children.is_empty(),
            "the pending round-2 child should have been retried"
        );
        assert!(
            find_vote_for_round(&effects, 2).is_some(),
            "the retried round-2 proposal should have been voted on"
        );
    }

    /// Test 3: the f+1 gate. With `gate_tips: true` and one tip unavailable, no vote is
    /// emitted; with `gate_tips: false`, a vote IS emitted for the same input.
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

    /// Test 4: deviation-4's fix. A `pending_cut_children` bucket with one invalid and
    /// one valid sibling still processes the valid one once the shared parent becomes
    /// known.
    ///
    /// Note on construction: upstream's verify()/leader-authenticity checks both
    /// precede the "is the parent known yet" buffering step, so an item that would
    /// fail either check is rejected on first contact and never actually reaches
    /// `pending_cut_children` organically -- anything that *is* buffered has, by
    /// construction, already passed both checks once, and since neither depends on
    /// anything that can change while this engine runs (the proposal's own fields, and
    /// the fixed committee), it will pass them again when dequeued. This test isolates
    /// the loop's own per-item-rejection behavior (what deviation 4 actually changes)
    /// from that fact by seeding the pending bucket directly -- exactly as
    /// `prune_below_is_exact` seeds other fields directly -- rather than relying on
    /// two separate `process_cut_proposal` calls to organically buffer both siblings.
    #[test]
    fn queue_with_invalid_sibling_still_processes_valid_one() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;

        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let not_round2_leader = keys
            .iter()
            .copied()
            .find(|k| *k != round2_leader)
            .unwrap();

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
            proposer: not_round2_leader, // wrong leader for round 2 -- rejected at dequeue
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

        // The parent arrives: both seeded children move into the internal queue,
        // invalid one first (insertion order). Without the deviation-4 fix, rejecting
        // the invalid one would abort the whole call and the valid sibling -- already
        // removed from `pending_cut_children` at that point -- would be lost.
        let effects = engine.process_cut_proposal(parent, &tips, &oracle);

        assert!(
            !engine.pending_cut_children.contains_key(&(2, parent_id)),
            "both siblings should have been drained from the pending queue"
        );
        let vote = find_vote_for_round(&effects, 2).expect("the valid sibling should have been voted on");
        assert_eq!(vote.cut_id, valid_child_id);
    }

    /// Test 5: `prune_below` removes exactly the entries strictly below the floor and
    /// nothing at or above it, across every round-prunable field.
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
        engine.timeouts_aggregators.insert(1, TimeoutAggregator::new());
        engine.timeouts_aggregators.insert(2, TimeoutAggregator::new());
        engine
            .timeout_accept_aggregators
            .insert(1, TimeoutAcceptAggregator::new());
        engine
            .timeout_accept_aggregators
            .insert(2, TimeoutAcceptAggregator::new());
        engine
            .cut_proposals
            .insert((1, d1.clone()), proposal_at(1));
        engine
            .cut_proposals
            .insert((2, d2.clone()), proposal_at(2));
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
        engine.cut_certificates.insert(1, CutCertificate::default());
        engine.cut_certificates.insert(2, CutCertificate::default());
        engine
            .decide_aggregators
            .insert((1, d1.clone()), DecideAggregator::new());
        engine
            .decide_aggregators
            .insert((2, d2.clone()), DecideAggregator::new());
        engine.decides_by_round.insert(
            1,
            Decide {
                id: d1.clone(),
                round: 1,
                origin: key(1),
                author: key(1),
            },
        );
        engine.decides_by_round.insert(
            2,
            Decide {
                id: d2.clone(),
                round: 2,
                origin: key(1),
                author: key(1),
            },
        );
        for set in [
            &mut engine.voted_cut_rounds,
            &mut engine.proposed_cut_rounds,
            &mut engine.sent_decide_rounds,
            &mut engine.sent_commit_rounds,
            &mut engine.sent_timeouts,
            &mut engine.sent_timeout_accepts,
            &mut engine.certified_timed_out,
            &mut engine.scheduled_cut_timers,
        ] {
            set.insert(1);
            set.insert(2);
        }

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
        assert!(!engine.cut_certificates.contains_key(&1));
        assert!(engine.cut_certificates.contains_key(&2));
        assert!(!engine.decide_aggregators.contains_key(&(1, d1)));
        assert!(engine.decide_aggregators.contains_key(&(2, d2)));
        assert!(!engine.decides_by_round.contains_key(&1));
        assert!(engine.decides_by_round.contains_key(&2));
        for set in [
            &engine.voted_cut_rounds,
            &engine.proposed_cut_rounds,
            &engine.sent_decide_rounds,
            &engine.sent_commit_rounds,
            &engine.sent_timeouts,
            &engine.sent_timeout_accepts,
            &engine.certified_timed_out,
            &engine.scheduled_cut_timers,
        ] {
            assert!(!set.contains(&1));
            assert!(set.contains(&2));
        }

        // gc_floor moved with it -- sanitize_timeout_accept now rejects round 1.
        assert!(engine
            .sanitize_timeout_accept(&TimeoutAccept { round: 1, author: key(1) })
            .is_err());
        assert!(engine
            .sanitize_timeout_accept(&TimeoutAccept { round: 2, author: key(1) })
            .is_ok());

        // Idempotent / monotonic: pruning to an earlier-or-equal floor is a no-op.
        engine.prune_below(1);
        assert!(engine.cut_certificates.contains_key(&2));
    }
}
