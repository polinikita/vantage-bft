// PHASE6-SPEC.md §5 -- completion reports, validated Bracha broadcast, non-speculative
// Simple-IT rounds (protocol S, Bracha-RBC), log assembly + contiguous consumption, and
// §6's A_u derivation + fetch server/client. See PHASE6-NOTES.md §5 for the full
// Simple-IT distillation (page cites against the granted reference PDF) this module
// implements against.
//
// `ControlLog` owns everything: reports census, per-round validated-Bracha state,
// per-round Simple-IT state (safe/disabled/committed/voted/timed_out), per-round
// reliable-notification state, the delivered log `L`, and fetch bookkeeping. Effect-
// returning like every other Vantage component -- no direct network/timer I/O.

use crate::primary::View;
use crate::vantage::agb::{self, Outcome, ResolutionEntry, ViewProposal};
use crate::vantage::block::BlockRef;
use crate::vantage::{Effect, Thresholds};
use config::Committee;
use crypto::{Digest, PublicKey};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub type Round = u64;

/// A control proposal's identity: round + parent + value (§5's "identity covers round
/// + parent + block x"). `parent` is a plain `Round` (0 is a legitimate, non-optional
///   value -- the genesis root; `SafeParent`'s own `0 <= r' < r` bound is the only place
///   "no real parent" is expressible, and it's already satisfied by `parent = 0`).
///   `value = None` is `⊥`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ControlProposal {
    pub round: Round,
    pub parent: Round,
    pub value: Option<(View, Digest)>,
}

/// Per-round validated-Bracha state (one instance per round; multiple rounds' RBC
/// instances can be concurrently in flight since entering round r+1 only requires
/// round r to be marked SAFE, not committed -- Fig. 1's pipelining).
#[derive(Default)]
struct BrachaRoundState {
    /// The FIRST complete proposal received from this round's leader (sticky), paired
    /// with its attached `B_w` (present only for a non-empty value).
    received_init: Option<(ControlProposal, Option<ViewProposal>)>,
    echo_sent: bool,
    ready_sent: bool,
    /// First-hand dedup: at most one counted ECHO per sender per round, whichever
    /// `ControlProposal` they named.
    echo_statements: HashMap<PublicKey, ControlProposal>,
    ready_statements: HashMap<PublicKey, ControlProposal>,
    delivered: Option<ControlProposal>,
}

/// Per-round reliable-notification state (Fig. 4). The only event this protocol ever
/// raises is `<timeout, r>`, so there is no generic event type -- each round's own
/// vote/accept census IS the notification state for that round's timeout event.
#[derive(Default)]
struct NotifRoundState {
    vote_sent: bool,
    accept_sent: bool,
    votes: HashSet<PublicKey>,
    accepts: HashSet<PublicKey>,
    confirmed: bool,
}

pub struct ControlLog {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    delta: Duration,
    n: usize,
    f_plus_1_parties: usize,
    two_f_plus_1_parties: usize,
    n_minus_f_parties: usize,

    // --- reports census (§5) ---
    /// First-hand, one report per (view, sender), ever.
    reports: HashMap<View, HashMap<PublicKey, Digest>>,
    /// Held + verified `B_w`'s, keyed by view (at most one correct value can ever
    /// exist per view by quorum intersection).
    blocks: HashMap<View, ViewProposal>,
    /// Fable audit pass 1, P6-1: views we have broadcast our OWN `CompReport` for,
    /// ever -- the once-guard `on_completion_reportable` uses. Deliberately SEPARATE
    /// from `blocks`' own keys: `blocks[w]` can be populated by `try_echo`'s
    /// INIT-attachment store or by `on_control_serve` BEFORE our own genuine R4
    /// completion of `w` ever happens, and the report obligation is unconditional on
    /// "first genuine completion" (the paper's own rule), not on "do we already hold
    /// the body" -- gating on `blocks.contains_key` let a party that validated/fetched
    /// `B_w` early suppress its own report forever, starving O3's `>= 2f+1` universal-
    /// completion progress argument.
    reported: HashSet<View>,

    // --- Simple-IT round state (Fig. 2) ---
    curr_round: Round,
    voted: bool,
    timed_out: bool,
    safe: HashSet<Round>,
    disabled: HashSet<Round>,
    committed: HashSet<Round>,
    /// `proposal[r]` once RB-delivered (mirrors `BrachaRoundState::delivered`, kept
    /// separately keyed for `Log(r)` traversal after a round's Bracha state could, in
    /// principle, be pruned -- kept simple here, no pruning yet).
    proposal: HashMap<Round, ControlProposal>,
    proposed_this_round: HashSet<Round>,
    commit_votes: HashMap<Round, HashSet<PublicKey>>,

    bracha: HashMap<Round, BrachaRoundState>,
    notif: HashMap<Round, NotifRoundState>,

    // --- log assembly + anchors (§6) ---
    delivered_log: Vec<(View, Digest)>,
    delivered_set: HashSet<(View, Digest)>,
    consume_pos: usize,
    anchored: HashSet<View>,

    // --- fetch bookkeeping ---
    pending_fetch: HashSet<(View, Digest)>,
    fetch_answered: HashSet<(PublicKey, View, Digest)>,

    /// Test-only cap on how far `try_propose` will ever lead a round (mirrors
    /// `harness::Node::max_views`'s exact reasoning): nothing throttles a `⊥`-valued
    /// round's advance in a synchronous, timer-less-by-default test harness (every
    /// round with no submittable pair proposes/delivers/commits/advances instantly),
    /// so left unbounded it cascades forever. `None` (the constructor's default) is
    /// unbounded -- production never sets this.
    #[cfg(test)]
    max_rounds_for_test: Option<Round>,
}

impl ControlLog {
    pub fn new(name: PublicKey, committee: Committee, sid: Digest, delta_ms: u64) -> Self {
        let n = committee.size();
        let thresholds = Thresholds::from_party_count(n);
        Self {
            name,
            committee,
            sid,
            delta: Duration::from_millis(delta_ms),
            n,
            f_plus_1_parties: thresholds.f_plus_1_parties,
            two_f_plus_1_parties: thresholds.two_f_plus_1_parties,
            n_minus_f_parties: thresholds.n_minus_f_parties,
            reports: HashMap::new(),
            blocks: HashMap::new(),
            reported: HashSet::new(),
            curr_round: 0,
            voted: false,
            timed_out: false,
            safe: HashSet::from([0]), // safe[0] = true, always
            disabled: HashSet::new(),
            committed: HashSet::new(),
            proposal: HashMap::new(),
            proposed_this_round: HashSet::new(),
            commit_votes: HashMap::new(),
            bracha: HashMap::new(),
            notif: HashMap::new(),
            delivered_log: Vec::new(),
            delivered_set: HashSet::new(),
            consume_pos: 0,
            anchored: HashSet::new(),
            pending_fetch: HashSet::new(),
            fetch_answered: HashSet::new(),
            #[cfg(test)]
            max_rounds_for_test: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_max_rounds_for_test(&mut self, max: Round) {
        self.max_rounds_for_test = Some(max);
    }

    pub fn control_round_timeout(&self) -> Duration {
        self.delta * 6
    }

    /// §5: round-robin control leader, independent of the data-view proposer rotation
    /// (a different counter -- `Round`, not `View`) -- same formula as `agb::proposer`.
    pub fn control_leader(&self, round: Round) -> PublicKey {
        let names: Vec<PublicKey> = self.committee.authorities.keys().cloned().collect();
        names[((round.saturating_sub(1)) % self.n as u64) as usize]
    }

    fn is_our_turn_to_lead(&self, round: Round) -> bool {
        round >= 1 && self.control_leader(round) == self.name
    }

    // ============================================================ Init/Enter round

    /// **Init** (Fig. 2): enter round 1 at boot.
    pub fn genesis(&mut self) -> Vec<Effect> {
        self.enter_round(1)
    }

    /// **Enter round**'s pure bookkeeping: `curr_round <- r`, reset the round timer,
    /// `voted = timed_out = false`, propose if leader, and (Fable audit P2-2) retry
    /// Vote -- a round that became `safe` before we ever entered it (its `try_vote`
    /// no-op'd earlier because `r != curr_round` at the time) is never retried on its
    /// own otherwise. No-op unless `safe[r]` is already true (`voted`/`timed_out` were
    /// both just reset above).
    ///
    /// Deliberately NOT itself recursive/iterative on Advance -- Fable re-audit pass 1:
    /// this used to live directly in `enter_round`, which called `try_vote`, which
    /// called `try_advance_round`, which called `enter_round` again -- a 3-function
    /// mutual-recursion cycle with one full cycle of stack depth PER round a single
    /// `mark_safe` cascade could advance through. Since `mark_safe`'s own cascade over
    /// K consecutive already-RB-delivered rounds is adversarially controllable (a
    /// lagging/rejoining node's safe backlog), that was an O(K) stack-overflow risk --
    /// exactly the class `mark_safe` itself was already rewritten iteratively to avoid.
    /// Split out as a shared helper that itself NEVER drives Advance (its `try_vote` is
    /// voting-only): `enter_round` (the one-shot boot entry) and `try_advance_round`'s
    /// loop (the sole repeated driver, now bounded O(1) stack regardless of K) both
    /// call this. `try_advance_round` calls `enter_round_core` but not `enter_round`,
    /// and nothing `enter_round_core` calls re-enters `try_advance_round`, so the cycle
    /// is gone -- `enter_round`'s own single `try_advance_round` call (boot only) is not
    /// on any loop and costs O(1).
    fn enter_round_core(&mut self, r: Round) -> Vec<Effect> {
        self.curr_round = r;
        self.voted = false;
        self.timed_out = false;
        let mut effects = vec![Effect::ArmControlTimer(r, std::time::Instant::now() + self.control_round_timeout())];
        effects.extend(self.try_propose(r));
        effects.extend(self.try_vote(r));
        effects
    }

    /// **Init** (Fig. 2): the one-shot boot entry point (`genesis` -> `enter_round(1)`).
    /// Enters `r` via `enter_round_core`, then drives Advance iteratively in case `r`
    /// is somehow already safe/disabled at the moment of entry (never true for `r=1`
    /// in practice -- nothing can be safe before round 1 even exists -- but keeps this
    /// the same shape as every other post-entry site, all of which already call
    /// `try_advance_round` themselves rather than relying on `enter_round_core` to
    /// cascade on its own).
    fn enter_round(&mut self, r: Round) -> Vec<Effect> {
        let mut effects = self.enter_round_core(r);
        effects.extend(self.try_advance_round());
        effects
    }

    /// **Propose**: leader-only, one-shot per round. `SafeParent(curr_round, r')`'s
    /// highest satisfying `r'`, scanned downward; `None` if local knowledge has a gap
    /// (an intervening round neither safe nor disabled yet) -- deferred, retried via
    /// `retry_propose` whenever new safe/disabled state arrives.
    fn try_propose(&mut self, r: Round) -> Vec<Effect> {
        if !self.is_our_turn_to_lead(r) || self.proposed_this_round.contains(&r) {
            return Vec::new();
        }
        #[cfg(test)]
        if let Some(max) = self.max_rounds_for_test {
            if r > max {
                return Vec::new(); // test-only cap -- see the field's doc comment
            }
        }
        let Some(parent) = self.safe_parent_for(r) else {
            return Vec::new(); // deferred
        };
        let value = self.pick_submittable_value(parent);
        self.proposed_this_round.insert(r);
        let proposal = ControlProposal { round: r, parent, value: value.clone() };
        let b_w = value.and_then(|(w, _)| self.blocks.get(&w).cloned());
        let mut effects = vec![Effect::BroadcastControlInit(proposal.clone(), b_w.clone())];
        // Bracha self-delivery: the leader also processes its own INIT locally (same
        // pattern as `AgbEngine::on_propose`'s "we broadcast AND locally process our
        // own proposal" -- a leader that only ever broadcast, never counting its own
        // first-hand echo/ready, could never contribute to its own round's quorum).
        let name = self.name;
        effects.extend(self.on_control_init(name, proposal, b_w));
        effects
    }

    /// Highest `r' < r` with `safe[r']` and every round strictly between `r'` and `r`
    /// disabled -- scanned downward so a gap in local knowledge (neither safe nor
    /// disabled) stops the scan (defer, don't guess).
    fn safe_parent_for(&self, r: Round) -> Option<Round> {
        let mut candidate = r.checked_sub(1)?;
        loop {
            if candidate == 0 || self.safe.contains(&candidate) {
                return Some(candidate);
            }
            if self.disabled.contains(&candidate) {
                if candidate == 0 {
                    return Some(0);
                }
                candidate -= 1;
                continue;
            }
            return None; // gap -- defer
        }
    }

    /// The smallest-view submitted pair not already delivered or in `Log(parent)`, or
    /// `None` (⊥) if there isn't one. "Submitted" = §5's submittable predicate (>=2f+1
    /// matching reports AND we hold + verified `B_w`).
    fn pick_submittable_value(&self, parent: Round) -> Option<(View, Digest)> {
        let in_chain: HashSet<(View, Digest)> = self.log_chain(parent).into_iter().collect();
        let mut best: Option<(View, Digest)> = None;
        for (&view, reporters) in &self.reports {
            let Some(proposal) = self.blocks.get(&view) else { continue };
            let digest = proposal.digest(&self.sid);
            let matching = reporters.values().filter(|d| **d == digest).count();
            if matching < self.two_f_plus_1_parties {
                continue;
            }
            let pair = (view, digest);
            if self.delivered_set.contains(&pair) || in_chain.contains(&pair) {
                continue;
            }
            // D7-2: leader-side "smallest STILL-USEFUL view" -- a reported view here
            // always carries a resolution entry (reports are only ever populated for
            // M != None proposals); if its target is already anchored, some earlier
            // carrier already resolved it, so this pair is moot -- skip it to avoid
            // burning this leader's own per-round bandwidth re-delivering a no-op.
            if let Some(entry) = &proposal.m {
                if self.anchored.contains(&entry.target_view()) {
                    continue;
                }
            }
            if best.as_ref().is_none_or(|(bv, _)| view < *bv) {
                best = Some(pair);
            }
        }
        best
    }

    /// `Log(r)`: the chain of (view,digest) pairs (skipping `⊥` rounds) from round 0
    /// down to `r`, via `proposal[.]`'s parent pointers.
    fn log_chain(&self, r: Round) -> Vec<(View, Digest)> {
        let mut chain = Vec::new();
        let mut cur = r;
        loop {
            if cur == 0 {
                break;
            }
            let Some(p) = self.proposal.get(&cur) else { break };
            if let Some(pair) = &p.value {
                chain.push(pair.clone());
            }
            cur = p.parent;
        }
        chain.reverse();
        chain
    }

    /// Retry every not-yet-proposed round we lead whose `curr_round` we're still in
    /// (only `curr_round` can ever be pending -- earlier rounds are either proposed or
    /// belong to a predecessor's leader) -- called after any safe/disabled transition.
    fn retry_propose(&mut self) -> Vec<Effect> {
        self.try_propose(self.curr_round)
    }

    // ============================================================ Validated Bracha

    /// A received `ControlInit`. D4-class declared-sender trust: only accepted from
    /// `control_leader(round)`. Sticky: only the FIRST complete proposal ever received
    /// from the leader is stored (Bracha's own uniqueness -- "first" and "matching"
    /// collapse to the same thing).
    pub fn on_control_init(&mut self, sender: PublicKey, proposal: ControlProposal, b_w: Option<ViewProposal>) -> Vec<Effect> {
        if sender != self.control_leader(proposal.round) {
            return Vec::new();
        }
        let round = proposal.round;
        let state = self.bracha.entry(round).or_default();
        if state.received_init.is_some() {
            return Vec::new();
        }
        state.received_init = Some((proposal, b_w));
        let mut effects = self.try_echo(round);
        effects.extend(self.pump_log()); // a freshly-fetched-or-completed B_w might
                                          // unblock an already-pending log position
        effects
    }

    /// **ECHO**: only the stored (first) proposal, only once validated -- `⊥` passes
    /// immediately; a non-empty value needs `>= f+1` matching reports AND the attached
    /// `B_w` to verify. Persistent: re-evaluated by `retry_pending_echoes` whenever new
    /// reports arrive or a `B_w` becomes newly held.
    fn try_echo(&mut self, round: Round) -> Vec<Effect> {
        let Some(state) = self.bracha.get(&round) else { return Vec::new() };
        if state.echo_sent {
            return Vec::new();
        }
        let Some((proposal, b_w)) = state.received_init.clone() else { return Vec::new() };
        let valid = match &proposal.value {
            None => true, // ⊥ passes immediately
            Some((w, h)) => {
                let reports_ok = self.report_count_for(*w, h) >= self.f_plus_1_parties;
                let b_w_ok = b_w.as_ref().is_some_and(|p| self.verify_b_w(*w, h, p));
                reports_ok && b_w_ok
            }
        };
        if !valid {
            return Vec::new();
        }
        if let (Some((w, h)), Some(p)) = (&proposal.value, &b_w) {
            if self.verify_b_w(*w, h, p) {
                self.blocks.entry(*w).or_insert_with(|| p.clone());
            }
        }
        let state = self.bracha.get_mut(&round).unwrap();
        state.echo_sent = true;
        let name = self.name;
        state.echo_statements.insert(name, proposal.clone());
        let mut effects = vec![Effect::BroadcastControlEcho(proposal)];
        effects.extend(self.recheck_bracha_ready(round));
        effects
    }

    /// Retry `try_echo` for every round with a still-pending (not-yet-echoed) stored
    /// init -- called whenever new reports arrive or a `B_w` becomes newly held (the
    /// same "persistent gate, explicit retry" pattern as `MetaOK`/`recheck_all`).
    fn retry_pending_echoes(&mut self) -> Vec<Effect> {
        let pending: Vec<Round> = self
            .bracha
            .iter()
            .filter(|(_, s)| !s.echo_sent && s.received_init.is_some())
            .map(|(r, _)| *r)
            .collect();
        let mut effects = Vec::new();
        for r in pending {
            effects.extend(self.try_echo(r));
        }
        effects
    }

    /// A counted `ControlEcho`.
    pub fn on_control_echo(&mut self, sender: PublicKey, proposal: ControlProposal) -> Vec<Effect> {
        let round = proposal.round;
        let state = self.bracha.entry(round).or_default();
        if state.echo_statements.contains_key(&sender) {
            return Vec::new();
        }
        state.echo_statements.insert(sender, proposal);
        self.recheck_bracha_ready(round)
    }

    /// **READY** on `2f+1` matching ECHOes or `f+1` matching READYs (Bracha's relay
    /// rule) -- once, ever, per round.
    fn recheck_bracha_ready(&mut self, round: Round) -> Vec<Effect> {
        let Some(state) = self.bracha.get(&round) else { return Vec::new() };
        if state.ready_sent {
            return Vec::new();
        }
        let echo_tally = Self::tally(&state.echo_statements);
        let ready_tally = Self::tally(&state.ready_statements);
        let winner = echo_tally
            .iter()
            .find(|(_, count)| **count >= self.two_f_plus_1_parties)
            .or_else(|| ready_tally.iter().find(|(_, count)| **count >= self.f_plus_1_parties))
            .map(|(p, _)| p.clone());
        let Some(proposal) = winner else { return Vec::new() };
        let state = self.bracha.get_mut(&round).unwrap();
        state.ready_sent = true;
        let name = self.name;
        state.ready_statements.insert(name, proposal.clone());
        let mut effects = vec![Effect::BroadcastControlReady(proposal)];
        effects.extend(self.recheck_bracha_deliver(round));
        effects
    }

    /// A counted `ControlReady`.
    pub fn on_control_ready(&mut self, sender: PublicKey, proposal: ControlProposal) -> Vec<Effect> {
        let round = proposal.round;
        let state = self.bracha.entry(round).or_default();
        if state.ready_statements.contains_key(&sender) {
            return Vec::new();
        }
        state.ready_statements.insert(sender, proposal);
        let mut effects = self.recheck_bracha_ready(round); // f+1-READY relay trigger
        effects.extend(self.recheck_bracha_deliver(round));
        effects
    }

    /// **Deliver** (the underlying broadcast's OWN delivery, `RB-deliver` in Fig. 2's
    /// vocabulary -- distinct from the higher round-protocol's "Deliver" step): 2f+1
    /// matching READYs. Sets `proposal[round]`, runs Mark-safe, requests `B_w` early if
    /// missing (§5's "missing validation data at delivery time").
    fn recheck_bracha_deliver(&mut self, round: Round) -> Vec<Effect> {
        let Some(state) = self.bracha.get(&round) else { return Vec::new() };
        if state.delivered.is_some() {
            return Vec::new();
        }
        let ready_tally = Self::tally(&state.ready_statements);
        let Some((proposal, _)) = ready_tally.iter().find(|(_, count)| **count >= self.two_f_plus_1_parties) else {
            return Vec::new();
        };
        let proposal = proposal.clone();
        self.bracha.get_mut(&round).unwrap().delivered = Some(proposal.clone());
        self.proposal.insert(round, proposal.clone());
        let mut effects = Vec::new();
        if let Some((w, h)) = &proposal.value {
            if !self.blocks.contains_key(w) {
                effects.extend(self.ensure_fetch(*w, h, round));
            }
        }
        effects.extend(self.mark_safe(round));
        effects
    }

    fn tally(statements: &HashMap<PublicKey, ControlProposal>) -> HashMap<ControlProposal, usize> {
        let mut out: HashMap<ControlProposal, usize> = HashMap::new();
        for p in statements.values() {
            *out.entry(p.clone()).or_insert(0) += 1;
        }
        out
    }

    // ============================================================ Round machine

    /// **Mark safe**: `proposal[r] = <r',b>` and `safe[r']` => `safe[r] = true`.
    ///
    /// Fable audit P2-1/P2-2/P2-3/P2-4: `safe[r]` becoming true is a transition every
    /// one of Vote/Deliver/Advance's predicates depends on, AND a transition a child
    /// round's OWN `safe` predicate depends on (`safe[r]` is child `r+1`'s `p.parent`
    /// condition) -- under cross-round reordering a child can RB-deliver while its
    /// parent is still unsafe, and back then `mark_safe(child)` bailed for good (the
    /// parent's later `mark_safe` never used to look at it). Rewritten as an ITERATIVE
    /// worklist (not recursion -- an adversarial chain must not cost O(chain) stack
    /// depth): every round this call newly marks safe (a) retries Vote (P2-2's other
    /// half: `enter_round` retries the case safe-before-entry; this retries the case
    /// safe-after-entry, both against the SAME `try_vote` no-op-unless-ready guard),
    /// (b) retries Deliver (P2-3: `committed[r]` may already hold from commits that
    /// arrived before `r` was safe), (c) retries Advance directly (see the note below),
    /// and (d) pushes any already-RB-delivered child whose parent is this now-safe
    /// round onto the worklist (P2-1's cascade).
    ///
    /// Deviation from the audit's literal fix list, flagged per its own "STOP and
    /// report rather than guess" instruction rather than silently applied: the audit's
    /// bug description names TWO sites that fail to drive Advance's `timed_out`
    /// disjunct -- "neither the timer path ... nor mark_safe" -- but its concrete fix
    /// list only patches the timer path (`on_control_round_timer` retrying Advance).
    /// That alone only fixes the safe-before-timeout ordering. Confirmed empirically
    /// (a throwaway probe test) that with ONLY the timer-path + `enter_round` +
    /// `try_deliver`-in-`mark_safe` fixes applied, the timeout-before-safe ordering
    /// with zero commits counted still wedges forever: `try_vote(r)` no-ops once
    /// `timed_out`, and `try_deliver(r)` no-ops while `!committed[r]` -- neither ever
    /// reaches `try_advance_round`, even though Advance's own predicate
    /// (`safe[curr] && (voted || timed_out)`) has no `committed` term and is already
    /// true. The direct call below closes that second named site, exactly mirroring
    /// the same "re-check the predicate at every site that can newly satisfy it"
    /// principle already applied to Vote/Deliver/the child cascade/the timer path --
    /// no threshold, format, or safety predicate changed, just one more idempotent
    /// recheck call (harmless if `r != curr_round`: `try_advance_round` reads
    /// `self.curr_round` itself and no-ops otherwise).
    fn mark_safe(&mut self, r: Round) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut worklist = vec![r];
        while let Some(r) = worklist.pop() {
            if self.safe.contains(&r) {
                continue;
            }
            let Some(p) = self.proposal.get(&r) else { continue };
            let parent = p.parent;
            if !self.safe.contains(&parent) {
                continue;
            }
            self.safe.insert(r);
            effects.extend(self.try_vote(r));
            effects.extend(self.try_deliver(r));
            effects.extend(self.try_advance_round());
            // P2-1 cascade: any already-RB-delivered child whose parent is the
            // now-safe `r`, collected into an owned `Vec` first so this doesn't hold a
            // borrow of `self.proposal`/`self.bracha` across the `worklist.extend` (no
            // further `self` calls needed once collected).
            let children: Vec<Round> = self
                .proposal
                .iter()
                .filter(|(cr, cp)| cp.parent == r && !self.safe.contains(cr) && self.bracha.get(cr).is_some_and(|b| b.delivered.is_some()))
                .map(|(cr, _)| *cr)
                .collect();
            worklist.extend(children);
        }
        effects.extend(self.retry_propose()); // a newly-safe round can unblock a
                                               // pending leader proposal at curr_round
        effects
    }

    /// **Vote**: `safe[curr_round] && !timed_out && !voted` => broadcast
    /// `<commit,curr_round>`, `voted = true`. Only ever fires for `curr_round` (the
    /// paper's own per-round state).
    ///
    /// Fable re-audit pass 1: deliberately does NOT itself call `try_advance_round`
    /// anymore (it used to) -- see `enter_round_core`'s doc comment for the
    /// mutual-recursion cycle that call used to close. Voting only; every call site
    /// (`enter_round_core`, `mark_safe`) is responsible for driving Advance itself
    /// afterward, which they all already do (directly, or via `try_deliver`'s own
    /// existing `try_advance_round` call).
    fn try_vote(&mut self, r: Round) -> Vec<Effect> {
        if r != self.curr_round || self.voted || self.timed_out {
            return Vec::new();
        }
        if !self.safe.contains(&r) {
            return Vec::new();
        }
        self.voted = true;
        let name = self.name;
        self.commit_votes.entry(r).or_default().insert(name);
        // Voting ONLY -- deliberately does not drive Advance (it used to, which closed
        // the mutual-recursion cycle enter_round_core -> try_vote -> try_advance_round
        // -> enter_round_core ...). Every caller (`enter_round_core`, `mark_safe`)
        // drives `try_advance_round` -- now the single iterative driver -- itself.
        vec![Effect::BroadcastControlCommit(r)]
    }

    /// **Timeout**: the control-round timer firing.
    pub fn on_control_round_timer(&mut self, r: Round) -> Vec<Effect> {
        if r != self.curr_round || self.voted {
            return Vec::new();
        }
        self.timed_out = true;
        // Fable audit P2-4: Advance's own predicate (`safe[curr] && (voted ||
        // timed_out)`) just gained a new true disjunct at THIS transition site (`timed_out`
        // becoming true) -- re-evaluate it here too, not only at the `voted`/`disabled`
        // transition sites that already did. No-op unless `safe[r]` is already true.
        let mut effects = self.rn_raise(r);
        effects.extend(self.try_advance_round());
        effects
    }

    /// Reliable notification's **Vote** step: `rn_raise(<timeout,r>)`.
    fn rn_raise(&mut self, r: Round) -> Vec<Effect> {
        let state = self.notif.entry(r).or_default();
        if state.vote_sent {
            return Vec::new();
        }
        state.vote_sent = true;
        let name = self.name;
        state.votes.insert(name);
        vec![Effect::BroadcastControlTimeoutVote(r)]
    }

    /// A counted `ControlTimeoutVote` -- **Accept** on `n-f` matching votes.
    pub fn on_control_timeout_vote(&mut self, sender: PublicKey, r: Round) -> Vec<Effect> {
        let state = self.notif.entry(r).or_default();
        if !state.votes.insert(sender) {
            return Vec::new();
        }
        if state.votes.len() >= self.n_minus_f_parties && !state.accept_sent {
            state.accept_sent = true;
            let name = self.name;
            state.accepts.insert(name);
            let mut effects = vec![Effect::BroadcastControlTimeoutAccept(r)];
            effects.extend(self.recheck_confirm(r));
            return effects;
        }
        Vec::new()
    }

    /// A counted `ControlTimeoutAccept` -- **Cascade** on `f+1` (if we haven't sent our
    /// own accept yet), **Confirm** on `2f+1`.
    pub fn on_control_timeout_accept(&mut self, sender: PublicKey, r: Round) -> Vec<Effect> {
        let state = self.notif.entry(r).or_default();
        if !state.accepts.insert(sender) {
            return Vec::new();
        }
        let mut effects = Vec::new();
        if state.accepts.len() >= self.f_plus_1_parties && !state.accept_sent {
            let s = self.notif.get_mut(&r).unwrap();
            s.accept_sent = true;
            let name = self.name;
            s.accepts.insert(name);
            effects.push(Effect::BroadcastControlTimeoutAccept(r));
        }
        effects.extend(self.recheck_confirm(r));
        effects
    }

    /// **Confirm**: `2f+1` matching accepts => `rn_confirm(<timeout,r>)` => **Disable**:
    /// `disabled[r] <- true`.
    fn recheck_confirm(&mut self, r: Round) -> Vec<Effect> {
        let state = self.notif.entry(r).or_default();
        if state.confirmed || state.accepts.len() < self.two_f_plus_1_parties {
            return Vec::new();
        }
        state.confirmed = true;
        self.disabled.insert(r);
        let mut effects = self.retry_propose(); // a newly-disabled round can complete
                                                 // a downstream SafeParent scan
        effects.extend(self.try_advance_round());
        effects
    }

    /// A counted `ControlCommit` -- **Commit**: `n-f` matching commits =>
    /// `committed[r] <- true` => **Deliver** (append `Log(r)`'s new suffix to `L`).
    pub fn on_control_commit(&mut self, sender: PublicKey, r: Round) -> Vec<Effect> {
        let voters = self.commit_votes.entry(r).or_default();
        if !voters.insert(sender) {
            return Vec::new();
        }
        if voters.len() < self.n_minus_f_parties || self.committed.contains(&r) {
            return Vec::new();
        }
        self.committed.insert(r);
        self.try_deliver(r)
    }

    /// **Deliver**: `committed[r] && safe[r]` => append every not-yet-delivered
    /// non-`⊥` block of `Log(r)` to `L`, in order.
    fn try_deliver(&mut self, r: Round) -> Vec<Effect> {
        if !self.committed.contains(&r) || !self.safe.contains(&r) {
            return Vec::new();
        }
        let chain = self.log_chain(r);
        let mut effects = Vec::new();
        for pair in chain {
            if self.delivered_set.insert(pair.clone()) {
                self.delivered_log.push(pair);
            }
        }
        effects.extend(self.try_advance_round());
        effects.extend(self.pump_log());
        effects
    }

    /// **Advance round**: `safe[curr_round] && (voted || timed_out)`, or
    /// `disabled[curr_round]` => enter `curr_round + 1`.
    ///
    /// Fable re-audit pass 1: the SOLE driver of round advancement, rewritten as an
    /// ITERATIVE loop (was: `self.enter_round(r + 1)`, which itself called `try_vote`,
    /// which used to call back into this function -- a mutual-recursion cycle costing
    /// a few stack frames per round advanced through). A single `mark_safe` cascade
    /// establishing K consecutive already-safe rounds now advances through all K here,
    /// in one bounded loop, however large K is -- O(1) stack regardless. Each iteration
    /// re-reads `self.curr_round`/`self.safe`/`self.voted`/`self.timed_out`/
    /// `self.disabled` fresh (no stale captured state), so a round that `enter_round_core`
    /// just voted for (setting `voted = true`) can immediately satisfy the NEXT
    /// iteration's own Advance check if it too is already safe -- exactly the K-deep
    /// cascade case.
    ///
    /// The loop's only callee is `enter_round_core` (voting-only `try_vote` + `try_propose`).
    /// One static path does lead from `enter_round_core` back here -- the leader's own
    /// INIT self-delivery: `try_propose -> on_control_init(self) -> try_echo ->
    /// recheck_bracha_ready -> recheck_bracha_deliver -> mark_safe -> try_advance_round`.
    /// It is dynamically DEAD for every fault-tolerant committee (n >= 4): closing it
    /// needs `recheck_bracha_deliver` to reach `2f+1` READYs from the locally-available
    /// set, which after a self-INIT is at most the leader's own 1 (+ f Byzantine) = f+1,
    /// and `f+1 >= 2f+1` only at `f = 0` (n <= 3). So for any real deployment nothing the
    /// loop calls re-enters it, and stack stays O(1); the constrained-stack regression
    /// test (`tests/control_tests.rs`) confirms it empirically.
    fn try_advance_round(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        loop {
            let r = self.curr_round;
            let ready = (self.safe.contains(&r) && (self.voted || self.timed_out)) || self.disabled.contains(&r);
            if !ready {
                break;
            }
            effects.extend(self.enter_round_core(r + 1));
        }
        effects
    }

    // ============================================================ Reports (§5)

    /// A first-hand `CompReport`.
    pub fn on_comp_report(&mut self, view: View, digest: Digest, sender: PublicKey) -> Vec<Effect> {
        let entry = self.reports.entry(view).or_default();
        if entry.contains_key(&sender) {
            return Vec::new();
        }
        entry.insert(sender, digest);
        let mut effects = self.retry_pending_echoes();
        effects.extend(self.retry_propose());
        effects
    }

    /// `AgbEngine`'s `Effect::CompletionReportable` hook: retain `B_w` indefinitely and
    /// broadcast our own `CompReport` (once, ever, per view -- "the FIRST genuine R4
    /// complete(w) -> B_w", UNCONDITIONAL on whether the body is already held).
    /// Fable audit pass 1, P6-1: the once-guard is `reported`, NOT `blocks.contains_key`
    /// -- `blocks[view]` can already be populated (by `try_echo`'s INIT-attachment
    /// store, or by `on_control_serve`) before our own genuine completion of `view`
    /// ever runs; gating the report on "do we already hold the body" let a party that
    /// validated/fetched `B_w` early NEVER report its own completion, which can starve
    /// the `>= 2f+1` submittability threshold at every correct leader (O3's progress
    /// proof needs universal completion to report, not merely "whoever completes
    /// first"). The `blocks` insert itself is unchanged (still unconditional -- by
    /// quorum intersection, any two verified values for the same view can only ever be
    /// content-identical, so re-inserting here is harmless even when `blocks[view]`
    /// was already held).
    pub fn on_completion_reportable(&mut self, view: View, proposal: ViewProposal) -> Vec<Effect> {
        if self.reported.contains(&view) {
            return Vec::new();
        }
        self.reported.insert(view);
        let digest = proposal.digest(&self.sid);
        // PHASE7-PREP-NOTES.md Finding A: diagnostic-only observational log -- every
        // M-carrying view this node itself genuinely completed and is about to report
        // (the entry point into the reports census `pick_submittable_value` scans).
        log::info!("vantage control log: own CompReport for carrier w={}", view);
        self.blocks.insert(view, proposal);
        let name = self.name;
        let mut effects = self.on_comp_report(view, digest.clone(), name);
        effects.push(Effect::BroadcastCompReport(view, digest));
        effects
    }

    /// Counted first-hand reports for `view` naming exactly `digest` (party count).
    pub(crate) fn report_count_for(&self, view: View, digest: &Digest) -> usize {
        self.reports.get(&view).map_or(0, |m| m.values().filter(|d| *d == digest).count())
    }

    /// §5: "verified `B_w`" -- view matches, `M_w != ∅`, digest matches, and the
    /// proposal itself is `Formed_v` (reuses `agb::formed`, the same well-formedness
    /// check the AGB layer applies to every fixed proposal).
    fn verify_b_w(&self, view: View, digest: &Digest, proposal: &ViewProposal) -> bool {
        proposal.view == view
            && proposal.m.is_some()
            && proposal.digest(&self.sid) == *digest
            && agb::formed(&self.committee, proposal.view, &proposal.c, &proposal.t, &proposal.m)
    }

    // ============================================================ Fetch (§5/§6)

    /// Every party who has EITHER sent a matching REPORT for `(w,h)` OR echoed (in any
    /// round) a `ControlProposal` naming `(w,h)` -- §5's "request B_w once from every
    /// matching REPORT and ECHO author".
    fn matching_report_and_echo_authors(&self, w: View, h: &Digest) -> Vec<PublicKey> {
        let mut out: HashSet<PublicKey> = HashSet::new();
        if let Some(m) = self.reports.get(&w) {
            for (sender, d) in m {
                if d == h {
                    out.insert(*sender);
                }
            }
        }
        for state in self.bracha.values() {
            for (sender, p) in &state.echo_statements {
                if p.value.as_ref().is_some_and(|(pw, ph)| pw == &w && ph == h) {
                    out.insert(*sender);
                }
            }
        }
        out.into_iter().collect()
    }

    fn ensure_fetch(&mut self, w: View, h: &Digest, _round: Round) -> Vec<Effect> {
        if self.blocks.contains_key(&w) || !self.pending_fetch.insert((w, h.clone())) {
            return Vec::new();
        }
        self.matching_report_and_echo_authors(w, h)
            .into_iter()
            .map(|peer| Effect::ControlFetchTo(peer, w, h.clone()))
            .collect()
    }

    /// A peer's `ControlFetch(w, h)` request -- answer with our held, verified `B_w` if
    /// we have it and haven't already answered this requester for this pair.
    pub fn on_control_fetch(&mut self, requester: PublicKey, w: View, h: Digest) -> Vec<Effect> {
        if self.fetch_answered.contains(&(requester, w, h.clone())) {
            return Vec::new();
        }
        let Some(proposal) = self.blocks.get(&w) else { return Vec::new() };
        if proposal.digest(&self.sid) != h {
            return Vec::new();
        }
        self.fetch_answered.insert((requester, w, h));
        vec![Effect::ControlServeTo(requester, w, proposal.clone())]
    }

    /// A peer's `ControlServe(w, proposal)` answer -- accept the FIRST valid response
    /// (§5: "do not re-impose the f+1-report predicate after delivery" -- structural
    /// verification only). Fable audit pass 1, P6-2 (RS1 safety fix): "valid" means
    /// hash-matching a REQUESTED pair, not merely well-formed -- same normative class
    /// as Phase 3's P1-2 (an unsolicited-but-hash-correct serve must change no state).
    /// Without the `pending_fetch` gate, a Byzantine peer could poison `blocks[w]`
    /// with a DIFFERENT well-formed proposal for `w`; `pump_log` would then hit its
    /// digest-mismatch branch at the TRUE anchor position and defensively skip it,
    /// diverging which anchor a poisoned party consumes for `u` from everyone else
    /// (an RS1/agreement violation). Every rejecting path below changes NO state
    /// (`pending_fetch` is only ever `.remove`d on the accepting path, after every
    /// other check has already passed, so a defensive check-then-remove ordering
    /// still leaves nothing partially mutated on a rejection).
    pub fn on_control_serve(&mut self, view: View, proposal: ViewProposal) -> Vec<Effect> {
        if self.blocks.contains_key(&view) || proposal.view != view {
            return Vec::new();
        }
        let digest = proposal.digest(&self.sid);
        if !self.pending_fetch.contains(&(view, digest.clone())) {
            return Vec::new(); // unsolicited, or answers a DIFFERENT pending pair -- ignored
        }
        if proposal.m.is_none() || !agb::formed(&self.committee, proposal.view, &proposal.c, &proposal.t, &proposal.m) {
            return Vec::new();
        }
        self.pending_fetch.remove(&(view, digest));
        self.blocks.insert(view, proposal);
        let mut effects = self.retry_pending_echoes();
        effects.extend(self.pump_log());
        effects
    }

    // ============================================================ Log + anchors (§6)

    /// Consume the contiguous prefix of `L` (`delivered_log`), position-minimal: the
    /// FIRST occurrence of a resolution entry for view `u` (in log order) is `A_u`;
    /// later occurrences are skipped (still advance the pointer). Blocks (without
    /// advancing) at the first position whose `B_w` isn't held yet -- "obtain B_w
    /// before processing".
    ///
    /// Invariant (Fable audit pass 1, recorded per the audit's request -- no code
    /// change needed): this function never re-checks `w >= u + 3` for the entry it
    /// reads out of `proposal.m` -- `agb::formed` already enforces that bound (`u < 1
    /// || u > view - 3` is rejected) on EVERY path that ever admits a body into
    /// `blocks` (`verify_b_w`/`on_control_serve` both call `agb::formed` directly;
    /// `on_completion_reportable`'s own `proposal` came from a genuine R4 completion,
    /// which is itself gated on `AgbEngine::on_propose`'s `formed(...)` check at
    /// fixing time). So by the time `pump_log` reads `proposal.m` here, `formed_v`
    /// already holds for it, transitively.
    // clippy::while_let_loop: this `loop` has 4 distinct `continue`/`break` exits
    // (missing entry, digest mismatch, no recovery target, already-anchored) plus the
    // extensively audited invariant argument in the doc comment above ("Fable audit
    // pass 1, P6-2") -- restructuring the loop shape, even via clippy's own
    // mechanically-equivalent suggestion, is exactly the kind of touch this
    // cleanup pass avoids in control-log delivery code; not done.
    #[allow(clippy::while_let_loop)]
    fn pump_log(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        loop {
            let Some((w, h)) = self.delivered_log.get(self.consume_pos).cloned() else { break };
            let Some(proposal) = self.blocks.get(&w) else {
                effects.extend(self.ensure_fetch(w, &h, self.curr_round));
                break;
            };
            if proposal.digest(&self.sid) != h {
                // Fable audit pass 1, P6-2: Byzantine-UNREACHABLE as of the
                // `on_control_serve` fix above. Every path that can ever install a
                // value into `blocks[w]` now ties it to the SPECIFIC digest it
                // verifies: `on_completion_reportable` computes `digest` from the same
                // `proposal` it inserts (trivially equal); `try_echo`'s INIT-attachment
                // store only runs after `verify_b_w(w, h, p)` (`p.digest() == h`,
                // checked there); `on_control_serve` (P6-2) only accepts a served
                // proposal whose digest matches an outstanding `pending_fetch` entry
                // for exactly `(w, h)`. So a mismatch here would mean one of those
                // three invariants broke elsewhere -- kept as a defensive
                // `debug_assert` + skip (never a panic/unwrap) rather than assumed,
                // in case a future change to one of those paths silently violates it.
                debug_assert!(false, "blocks[{:?}] digest mismatch at pump_log position {} -- should be Byzantine-unreachable, see comment", w, self.consume_pos);
                self.consume_pos += 1;
                continue;
            }
            let Some(entry) = &proposal.m else {
                self.consume_pos += 1;
                continue;
            };
            let u = entry.target_view();
            if self.anchored.contains(&u) {
                self.consume_pos += 1;
                continue; // a later anchor for an already-resolved u -- ignored
            }
            self.anchored.insert(u);
            let (outcome, refs) = Self::derive_anchor(entry);
            // PHASE7-PREP-NOTES.md Finding A: diagnostic-only observational log (no
            // behavior change) -- the FIRST-occurrence anchor application for each
            // target view, with the carrier (w) and control round it rode in on, so a
            // run's log can show the actual wall-clock/round distance between a target
            // becoming unresolved and its anchor finally landing.
            log::info!("vantage control log: anchor applied for u={} via carrier w={} at control round={}", u, w, self.curr_round);
            effects.push(Effect::ApplyAnchor(u, outcome, refs));
            self.consume_pos += 1;
        }
        effects
    }

    /// `X_u` derivation (§6): `Full -> gfull(C,T)`, `Core -> gcore(C)` with backing
    /// `(C,T)` retained for authorization, `Skip -> gskip`.
    fn derive_anchor(entry: &ResolutionEntry) -> (Outcome, Vec<BlockRef>) {
        match entry {
            ResolutionEntry::Full(_, c, t) => (Outcome::Full(c.clone(), t.clone()), c.iter().chain(t.iter()).cloned().collect()),
            ResolutionEntry::Core(_, c, t) => (Outcome::Core(c.clone()), c.iter().chain(t.iter()).cloned().collect()),
            ResolutionEntry::Skip(_) => (Outcome::Skip, Vec::new()),
        }
    }

    /// `resolve.rs`'s `resolved` predicate: whether `u` has already been anchor-
    /// resolved through the control log (folded together with `AgbEngine::is_sealed`
    /// by the caller).
    pub fn is_anchor_resolved(&self, view: View) -> bool {
        self.anchored.contains(&view)
    }

    #[cfg(test)]
    pub(crate) fn delivered_log_for_test(&self) -> &[(View, Digest)] {
        &self.delivered_log
    }

    /// PHASE7-PREP-NOTES.md Finding A: metrics-only accessors for the 1s progress-gauge
    /// sampler (production code, so not `#[cfg(test)]` like the ones below them).
    pub fn curr_round(&self) -> Round {
        self.curr_round
    }

    /// D7-4 (PHASE7-PREP-NOTES.md): read-only mirror of `on_control_round_timer`'s own
    /// `self.voted` check -- combined with `curr_round()` at the call site, reproduces
    /// that handler's exact `r != self.curr_round || self.voted` guard for the
    /// timer-queue's lazy stale-discard at pop time.
    pub fn voted(&self) -> bool {
        self.voted
    }

    pub fn delivered_log_len(&self) -> usize {
        self.delivered_log.len()
    }

    pub fn consume_pos(&self) -> usize {
        self.consume_pos
    }

    #[cfg(test)]
    pub(crate) fn curr_round_for_test(&self) -> Round {
        self.curr_round
    }

    #[cfg(test)]
    pub(crate) fn is_safe_for_test(&self, r: Round) -> bool {
        self.safe.contains(&r)
    }

    #[cfg(test)]
    pub(crate) fn is_disabled_for_test(&self, r: Round) -> bool {
        self.disabled.contains(&r)
    }

    #[cfg(test)]
    pub(crate) fn is_committed_for_test(&self, r: Round) -> bool {
        self.committed.contains(&r)
    }

    #[cfg(test)]
    pub(crate) fn holds_block_for_test(&self, view: View) -> bool {
        self.blocks.contains_key(&view)
    }
}
