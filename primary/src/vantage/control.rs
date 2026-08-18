use crate::leader::{one_based_authority, RoundRobin};
use crate::primary::View;
use crate::vantage::agb::{Outcome, ProposalOut, ResolutionEntry};
use crate::vantage::block::BlockRef;
use crate::vantage::{Effect, Thresholds};
use config::Committee;
use crypto::{Digest, PublicKey};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

/// Control-round number; round zero is the genesis parent.
pub type Round = u64;

/// Control proposal identity, including its round, parent, and optional log value.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ControlProposal {
    pub round: Round,
    pub parent: Round,
    pub value: Option<(View, Digest)>,
}

/// Selects leaders round-robin, with round one assigned to committee index zero.
pub fn control_leader(committee: &Committee, round: Round) -> PublicKey {
    one_based_authority(committee, round)
}

#[derive(Default)]
struct BrachaRoundState {
    received_init: Option<(ControlProposal, Option<ProposalOut>)>,
    echo_sent: bool,
    ready_sent: bool,
    echo_statements: HashMap<PublicKey, ControlProposal>,
    ready_statements: HashMap<PublicKey, ControlProposal>,
    delivered: Option<ControlProposal>,
}

#[derive(Default)]
struct NotifRoundState {
    vote_sent: bool,
    accept_sent: bool,
    votes: HashSet<PublicKey>,
    accepts: HashSet<PublicKey>,
    confirmed: bool,
}

/// Maintains validated control broadcast, round state, and ordered log consumption.
pub struct ControlLog {
    name: PublicKey,
    committee: Committee,
    leaders: RoundRobin,
    sid: Digest,
    delta: Duration,
    f_plus_1_parties: usize,
    two_f_plus_1_parties: usize,
    n_minus_f_parties: usize,

    reports: BTreeMap<View, HashMap<PublicKey, Digest>>,
    /// Held proposals that passed structural and digest validation.
    blocks: BTreeMap<View, ProposalOut>,
    /// Views for which this node has emitted its one completion report.
    reported: BTreeSet<View>,

    curr_round: Round,
    voted: bool,
    timed_out: bool,
    safe: HashSet<Round>,
    disabled: HashSet<Round>,
    committed: HashSet<Round>,
    proposal: HashMap<Round, ControlProposal>,
    children_by_parent: HashMap<Round, Vec<Round>>,
    proposed_this_round: HashSet<Round>,
    commit_votes: HashMap<Round, HashSet<PublicKey>>,

    bracha: HashMap<Round, BrachaRoundState>,
    notif: HashMap<Round, NotifRoundState>,
    pending_echo_rounds: BTreeSet<Round>,

    delivered_log: Vec<(View, Digest)>,
    delivered_set: BTreeSet<(View, Digest)>,
    consume_pos: usize,
    anchored: BTreeSet<View>,

    pending_fetch: BTreeMap<(View, Digest), Round>,
    fetch_answered: BTreeSet<(View, Digest, PublicKey)>,
    min_live_view: View,
    min_serve_view: View,

    #[cfg(test)]
    max_rounds_for_test: Option<Round>,
}

impl ControlLog {
    /// Creates a control log using `delta_ms` as the protocol delay unit.
    pub fn new(name: PublicKey, committee: Committee, sid: Digest, delta_ms: u64) -> Self {
        let n = committee.size();
        let thresholds = Thresholds::from_party_count(n);
        let leaders = RoundRobin::new(&committee);
        Self {
            name,
            committee,
            leaders,
            sid,
            delta: Duration::from_millis(delta_ms),
            f_plus_1_parties: thresholds.f_plus_1_parties,
            two_f_plus_1_parties: thresholds.two_f_plus_1_parties,
            n_minus_f_parties: thresholds.n_minus_f_parties,
            reports: BTreeMap::new(),
            blocks: BTreeMap::new(),
            reported: BTreeSet::new(),
            curr_round: 0,
            voted: false,
            timed_out: false,
            safe: HashSet::from([0]),
            disabled: HashSet::new(),
            committed: HashSet::new(),
            proposal: HashMap::new(),
            children_by_parent: HashMap::new(),
            proposed_this_round: HashSet::new(),
            commit_votes: HashMap::new(),
            bracha: HashMap::new(),
            notif: HashMap::new(),
            delivered_log: Vec::new(),
            delivered_set: BTreeSet::new(),
            consume_pos: 0,
            anchored: BTreeSet::new(),
            pending_fetch: BTreeMap::new(),
            fetch_answered: BTreeSet::new(),
            min_live_view: 1,
            min_serve_view: 1,
            pending_echo_rounds: BTreeSet::new(),
            #[cfg(test)]
            max_rounds_for_test: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_max_rounds_for_test(&mut self, max: Round) {
        self.max_rounds_for_test = Some(max);
    }

    /// Returns the control-round timeout of `6 * delta`.
    pub fn control_round_timeout(&self) -> Duration {
        self.delta * 6
    }

    const FETCH_RETRY_ROUNDS: Round = 8;

    /// Number of garbage-collection windows retained for serving proposal bodies.
    pub const SERVE_MARGIN_WINDOWS: View = 2;

    fn is_pruned_view(&self, view: View) -> bool {
        view < self.min_live_view
    }

    fn anchor_resolved(&self, view: View) -> bool {
        self.is_pruned_view(view) || self.anchored.contains(&view)
    }

    fn compact_delivered_log(&mut self) {
        if self.consume_pos == 0 {
            return;
        }
        self.delivered_log.drain(..self.consume_pos);
        self.consume_pos = 0;
    }

    /// Prunes protocol state below `floor` while retaining bodies through `serve_floor`.
    pub fn gc_below(&mut self, floor: View, serve_floor: View) {
        if floor <= self.min_live_view {
            return;
        }
        let serve_floor = serve_floor.min(floor);
        self.reports = self.reports.split_off(&floor);
        self.reported = self.reported.split_off(&floor);
        self.delivered_set = self.delivered_set.split_off(&(floor, Digest::default()));
        self.anchored = self.anchored.split_off(&floor);
        self.pending_fetch = self.pending_fetch.split_off(&(floor, Digest::default()));
        self.min_live_view = floor;

        self.blocks = self.blocks.split_off(&serve_floor);
        self.fetch_answered =
            self.fetch_answered
                .split_off(&(serve_floor, Digest::default(), PublicKey::default()));
        self.min_serve_view = serve_floor;

        self.compact_delivered_log();
    }

    pub fn control_leader(&self, round: Round) -> PublicKey {
        self.leaders.one_based(round)
    }

    fn is_our_turn_to_lead(&self, round: Round) -> bool {
        round >= 1 && self.control_leader(round) == self.name
    }

    pub fn genesis(&mut self) -> Vec<Effect> {
        self.enter_round(1)
    }

    /// Enters one round without recursively driving further round advancement.
    fn enter_round_core(&mut self, r: Round) -> Vec<Effect> {
        self.curr_round = r;
        self.voted = false;
        self.timed_out = false;
        let mut effects = vec![Effect::ArmControlTimer(
            r,
            std::time::Instant::now() + self.control_round_timeout(),
        )];
        effects.extend(self.try_propose(r));
        effects.extend(self.try_vote(r));
        effects
    }

    fn enter_round(&mut self, r: Round) -> Vec<Effect> {
        let mut effects = self.enter_round_core(r);
        effects.extend(self.try_advance_round());
        effects
    }

    fn try_propose(&mut self, r: Round) -> Vec<Effect> {
        if !self.is_our_turn_to_lead(r) || self.proposed_this_round.contains(&r) {
            return Vec::new();
        }
        #[cfg(test)]
        if let Some(max) = self.max_rounds_for_test {
            if r > max {
                return Vec::new();
            }
        }
        let Some(parent) = self.safe_parent_for(r) else {
            return Vec::new();
        };
        let value = self.pick_submittable_value(parent);
        self.proposed_this_round.insert(r);
        let proposal = ControlProposal {
            round: r,
            parent,
            value: value.clone(),
        };
        let b_w = value.and_then(|(w, _)| self.blocks.get(&w).cloned());
        let mut effects = vec![Effect::BroadcastControlInit(proposal.clone(), b_w.clone())];
        let name = self.name;
        effects.extend(self.on_control_init(name, proposal, b_w));
        effects
    }

    /// Returns the latest safe ancestor reachable through only disabled intervening rounds.
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
            return None;
        }
    }

    /// Selects the oldest undelivered value with `2f + 1` matching reports.
    fn pick_submittable_value(&self, parent: Round) -> Option<(View, Digest)> {
        let in_chain: HashSet<(View, Digest)> = self.log_chain(parent).into_iter().collect();
        let mut best: Option<(View, Digest)> = None;
        for (&view, reporters) in &self.reports {
            if self.is_pruned_view(view) {
                continue;
            }
            let Some(proposal) = self.blocks.get(&view) else {
                continue;
            };
            let digest = proposal.digest(&self.sid);
            let matching = reporters.values().filter(|d| **d == digest).count();
            if matching < self.two_f_plus_1_parties {
                continue;
            }
            let pair = (view, digest);
            if self.delivered_set.contains(&pair) || in_chain.contains(&pair) {
                continue;
            }
            let entries = proposal.entries();
            if !entries.is_empty()
                && entries
                    .iter()
                    .all(|entry| self.anchor_resolved(entry.target_view()))
            {
                continue;
            }
            if best.as_ref().is_none_or(|(bv, _)| view < *bv) {
                best = Some(pair);
            }
        }
        best
    }

    fn log_chain(&self, r: Round) -> Vec<(View, Digest)> {
        let mut chain = Vec::new();
        let mut cur = r;
        loop {
            if cur == 0 {
                break;
            }
            let Some(p) = self.proposal.get(&cur) else {
                break;
            };
            if let Some(pair) = &p.value {
                chain.push(pair.clone());
            }
            cur = p.parent;
        }
        chain.reverse();
        chain
    }

    fn retry_propose(&mut self) -> Vec<Effect> {
        self.try_propose(self.curr_round)
    }

    pub fn on_control_init(
        &mut self,
        sender: PublicKey,
        proposal: ControlProposal,
        b_w: Option<ProposalOut>,
    ) -> Vec<Effect> {
        if sender != self.control_leader(proposal.round) {
            return Vec::new();
        }
        let round = proposal.round;
        let state = self.bracha.entry(round).or_default();
        if state.received_init.is_some() {
            return Vec::new();
        }
        state.received_init = Some((proposal, b_w));
        self.pending_echo_rounds.insert(round);
        let mut effects = self.try_echo(round);
        effects.extend(self.pump_log());
        effects
    }

    /// Echoes only a leader proposal whose non-empty value has `f + 1` reports and a
    /// matching well-formed body.
    fn try_echo(&mut self, round: Round) -> Vec<Effect> {
        let Some(state) = self.bracha.get(&round) else {
            return Vec::new();
        };
        if state.echo_sent {
            return Vec::new();
        }
        let Some((proposal, b_w)) = state.received_init.clone() else {
            return Vec::new();
        };
        let valid = match &proposal.value {
            None => true,
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
        self.pending_echo_rounds.remove(&round);
        let name = self.name;
        state.echo_statements.insert(name, proposal.clone());
        let mut effects = vec![Effect::BroadcastControlEcho(proposal)];
        effects.extend(self.recheck_bracha_ready(round));
        effects
    }

    fn retry_pending_echoes(&mut self) -> Vec<Effect> {
        let pending: Vec<Round> = self.pending_echo_rounds.iter().copied().collect();
        let mut effects = Vec::new();
        for r in pending {
            effects.extend(self.try_echo(r));
        }
        effects
    }

    pub fn on_control_echo(&mut self, sender: PublicKey, proposal: ControlProposal) -> Vec<Effect> {
        let round = proposal.round;
        let state = self.bracha.entry(round).or_default();
        if state.echo_statements.contains_key(&sender) {
            return Vec::new();
        }
        state.echo_statements.insert(sender, proposal);
        self.recheck_bracha_ready(round)
    }

    /// Sends ready after `2f + 1` matching echoes or `f + 1` matching ready statements.
    fn recheck_bracha_ready(&mut self, round: Round) -> Vec<Effect> {
        let Some(state) = self.bracha.get(&round) else {
            return Vec::new();
        };
        if state.ready_sent {
            return Vec::new();
        }
        let echo_tally = Self::tally(&state.echo_statements);
        let ready_tally = Self::tally(&state.ready_statements);
        let winner = echo_tally
            .iter()
            .find(|(_, count)| **count >= self.two_f_plus_1_parties)
            .or_else(|| {
                ready_tally
                    .iter()
                    .find(|(_, count)| **count >= self.f_plus_1_parties)
            })
            .map(|(p, _)| p.clone());
        let Some(proposal) = winner else {
            return Vec::new();
        };
        let state = self.bracha.get_mut(&round).unwrap();
        state.ready_sent = true;
        let name = self.name;
        state.ready_statements.insert(name, proposal.clone());
        let mut effects = vec![Effect::BroadcastControlReady(proposal)];
        effects.extend(self.recheck_bracha_deliver(round));
        effects
    }

    pub fn on_control_ready(
        &mut self,
        sender: PublicKey,
        proposal: ControlProposal,
    ) -> Vec<Effect> {
        let round = proposal.round;
        let state = self.bracha.entry(round).or_default();
        if state.ready_statements.contains_key(&sender) {
            return Vec::new();
        }
        state.ready_statements.insert(sender, proposal);
        let mut effects = self.recheck_bracha_ready(round);
        effects.extend(self.recheck_bracha_deliver(round));
        effects
    }

    /// Delivers a proposal after `2f + 1` matching ready statements.
    fn recheck_bracha_deliver(&mut self, round: Round) -> Vec<Effect> {
        let Some(state) = self.bracha.get(&round) else {
            return Vec::new();
        };
        if state.delivered.is_some() {
            return Vec::new();
        }
        let ready_tally = Self::tally(&state.ready_statements);
        let Some((proposal, _)) = ready_tally
            .iter()
            .find(|(_, count)| **count >= self.two_f_plus_1_parties)
        else {
            return Vec::new();
        };
        let proposal = proposal.clone();
        self.bracha.get_mut(&round).unwrap().delivered = Some(proposal.clone());
        self.children_by_parent
            .entry(proposal.parent)
            .or_default()
            .push(round);
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

    /// Propagates safety to delivered children without recursive stack growth.
    fn mark_safe(&mut self, r: Round) -> Vec<Effect> {
        let mut effects = Vec::new();
        let mut worklist = vec![r];
        while let Some(r) = worklist.pop() {
            if self.safe.contains(&r) {
                continue;
            }
            let Some(p) = self.proposal.get(&r) else {
                continue;
            };
            let parent = p.parent;
            if !self.safe.contains(&parent) {
                continue;
            }
            self.safe.insert(r);
            effects.extend(self.try_vote(r));
            effects.extend(self.try_deliver(r));
            effects.extend(self.try_advance_round());
            if let Some(kids) = self.children_by_parent.get(&r) {
                let children: Vec<Round> = kids
                    .iter()
                    .copied()
                    .filter(|cr| !self.safe.contains(cr))
                    .collect();
                worklist.extend(children);
            }
        }
        effects.extend(self.retry_propose());
        effects
    }

    /// Votes once for the current safe round before its timeout.
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
        vec![Effect::BroadcastControlCommit(r)]
    }

    pub fn on_control_round_timer(&mut self, r: Round) -> Vec<Effect> {
        if r != self.curr_round || self.voted {
            return Vec::new();
        }
        self.timed_out = true;
        let mut effects = self.rn_raise(r);
        effects.extend(self.try_advance_round());
        effects
    }

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

    /// Sends an accept after `n - f` distinct timeout votes.
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

    /// Cascades at `f + 1` accepts and confirms at `2f + 1` accepts.
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

    fn recheck_confirm(&mut self, r: Round) -> Vec<Effect> {
        let state = self.notif.entry(r).or_default();
        if state.confirmed || state.accepts.len() < self.two_f_plus_1_parties {
            return Vec::new();
        }
        state.confirmed = true;
        self.disabled.insert(r);
        let mut effects = self.retry_propose();
        effects.extend(self.try_advance_round());
        effects
    }

    /// Commits after `n - f` distinct votes.
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

    /// Delivers only committed, safe round values and preserves control-log order.
    fn try_deliver(&mut self, r: Round) -> Vec<Effect> {
        if !self.committed.contains(&r) || !self.safe.contains(&r) {
            return Vec::new();
        }
        let chain = self.log_chain(r);
        let mut effects = Vec::new();
        for pair in chain {
            if self.is_pruned_view(pair.0) {
                continue;
            }
            if self.delivered_set.insert(pair.clone()) {
                self.delivered_log.push(pair);
            }
        }
        effects.extend(self.try_advance_round());
        effects.extend(self.pump_log());
        effects
    }

    /// Advances while the current round is disabled or safe and locally finished.
    fn try_advance_round(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        loop {
            let r = self.curr_round;
            let ready = (self.safe.contains(&r) && (self.voted || self.timed_out))
                || self.disabled.contains(&r);
            if !ready {
                break;
            }
            effects.extend(self.enter_round_core(r + 1));
        }
        effects
    }

    pub fn on_comp_report(&mut self, view: View, digest: Digest, sender: PublicKey) -> Vec<Effect> {
        if self.is_pruned_view(view) {
            return Vec::new();
        }
        let entry = self.reports.entry(view).or_default();
        if entry.contains_key(&sender) {
            return Vec::new();
        }
        entry.insert(sender, digest);
        let mut effects = self.retry_pending_echoes();
        effects.extend(self.retry_propose());
        effects
    }

    /// Emits one completion report even when the proposal body was already stored.
    pub fn on_completion_reportable(&mut self, view: View, proposal: ProposalOut) -> Vec<Effect> {
        if self.is_pruned_view(view) {
            return Vec::new();
        }
        if self.reported.contains(&view) {
            return Vec::new();
        }
        self.reported.insert(view);
        let digest = proposal.digest(&self.sid);
        log::debug!("vantage control log: own CompReport for carrier w={}", view);
        self.blocks.insert(view, proposal);
        let name = self.name;
        let mut effects = self.on_comp_report(view, digest.clone(), name);
        effects.push(Effect::BroadcastCompReport(view, digest));
        effects
    }

    pub(crate) fn report_count_for(&self, view: View, digest: &Digest) -> usize {
        if self.is_pruned_view(view) {
            return 0;
        }
        self.reports
            .get(&view)
            .map_or(0, |m| m.values().filter(|d| *d == digest).count())
    }

    /// Validates the view, non-empty recovery entries, digest, and proposal structure.
    fn verify_b_w(&self, view: View, digest: &Digest, proposal: &ProposalOut) -> bool {
        proposal.view() == view
            && !proposal.entries().is_empty()
            && proposal.digest(&self.sid) == *digest
            && proposal.formed(&self.committee)
    }

    fn matching_report_and_echo_authors(&self, w: View, h: &Digest) -> Vec<PublicKey> {
        if self.is_pruned_view(w) {
            return Vec::new();
        }
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

    /// Retries body fetches every eight control rounds from matching report or echo authors.
    fn ensure_fetch(&mut self, w: View, h: &Digest, round: Round) -> Vec<Effect> {
        if self.is_pruned_view(w) || self.blocks.contains_key(&w) {
            return Vec::new();
        }
        let key = (w, h.clone());
        match self.pending_fetch.get(&key) {
            Some(&last) if round.saturating_sub(last) < Self::FETCH_RETRY_ROUNDS => {
                return Vec::new()
            }
            _ => {}
        }
        self.pending_fetch.insert(key, round);
        self.matching_report_and_echo_authors(w, h)
            .into_iter()
            .map(|peer| Effect::ControlFetchTo(peer, w, h.clone()))
            .collect()
    }

    /// Serves one matching held body per requester while the body is above the serve floor.
    pub fn on_control_fetch(&mut self, requester: PublicKey, w: View, h: Digest) -> Vec<Effect> {
        if w < self.min_serve_view {
            return Vec::new();
        }
        if self.fetch_answered.contains(&(w, h.clone(), requester)) {
            return Vec::new();
        }
        let Some(proposal) = self.blocks.get(&w) else {
            return Vec::new();
        };
        if proposal.digest(&self.sid) != h {
            return Vec::new();
        }
        self.fetch_answered.insert((w, h, requester));
        vec![Effect::ControlServeTo(requester, w, proposal.clone())]
    }

    /// Clears the at-most-once marks for a response that never reached the transport.
    ///
    /// Serving is loss-free only while the marks and the send agree: a dropped serve must
    /// release the requester, otherwise its repeat fetch is refused forever and the view
    /// keeps a permanent hole. The drop site carries no digest, so every digest marked for
    /// this `(view, requester)` pair is released; a repeat fetch revalidates the pair.
    pub fn unanswer_fetch(&mut self, requester: &PublicKey, w: View) {
        let released: Vec<(View, Digest, PublicKey)> = self
            .fetch_answered
            .range((w, Digest::default(), PublicKey::default())..)
            .take_while(|(view, _, _)| *view == w)
            .filter(|(_, _, peer)| peer == requester)
            .cloned()
            .collect();
        for key in released {
            self.fetch_answered.remove(&key);
        }
    }

    /// Accepts only a well-formed body matching an outstanding `(view, digest)` request.
    pub fn on_control_serve(&mut self, view: View, proposal: ProposalOut) -> Vec<Effect> {
        if self.is_pruned_view(view) {
            return Vec::new();
        }
        if self.blocks.contains_key(&view) || proposal.view() != view {
            return Vec::new();
        }
        let digest = proposal.digest(&self.sid);
        if !self.pending_fetch.contains_key(&(view, digest.clone())) {
            return Vec::new();
        }
        if proposal.entries().is_empty() || !proposal.formed(&self.committee) {
            return Vec::new();
        }
        self.pending_fetch.remove(&(view, digest));
        self.blocks.insert(view, proposal);
        let mut effects = self.retry_pending_echoes();
        effects.extend(self.pump_log());
        effects
    }

    /// Consumes the delivered log in order and applies each target's first anchor.
    ///
    /// Consumption stops at a missing body. Recovery entries are applied in stored order;
    /// proposal validation has already enforced each entry's view-distance constraint.
    #[allow(clippy::while_let_loop)]
    fn pump_log(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        loop {
            let Some((w, h)) = self.delivered_log.get(self.consume_pos).cloned() else {
                break;
            };
            if self.is_pruned_view(w) {
                self.consume_pos += 1;
                continue;
            }
            let Some(proposal) = self.blocks.get(&w) else {
                effects.extend(self.ensure_fetch(w, &h, self.curr_round));
                break;
            };
            if proposal.digest(&self.sid) != h {
                debug_assert!(
                    false,
                    "control-log digest mismatch for view {w} at position {}",
                    self.consume_pos
                );
                self.consume_pos += 1;
                continue;
            }
            for entry in proposal.entries() {
                let u = entry.target_view();
                if self.anchor_resolved(u) {
                    continue;
                }
                self.anchored.insert(u);
                let (outcome, refs) = Self::derive_anchor(entry);
                log::debug!(
                    "vantage control log: anchor applied for u={} via carrier w={} at control round={}",
                    u,
                    w,
                    self.curr_round
                );
                effects.push(Effect::ApplyAnchor(u, outcome, refs));
            }
            self.consume_pos += 1;
        }
        effects
    }

    fn derive_anchor(entry: &ResolutionEntry) -> (Outcome, Vec<BlockRef>) {
        match entry {
            ResolutionEntry::Full(_, c, t) => (
                Outcome::Full(c.clone(), t.clone()),
                c.iter().chain(t.iter()).cloned().collect(),
            ),
            ResolutionEntry::Core(_, c, t) => (
                Outcome::Core(c.clone()),
                c.iter().chain(t.iter()).cloned().collect(),
            ),
            ResolutionEntry::Skip(_) => (Outcome::Skip, Vec::new()),
        }
    }

    /// Treats pruned views as resolved and otherwise checks the applied-anchor set.
    pub fn is_anchor_resolved(&self, view: View) -> bool {
        self.anchor_resolved(view)
    }

    #[cfg(test)]
    pub(crate) fn delivered_log_for_test(&self) -> &[(View, Digest)] {
        &self.delivered_log
    }

    pub fn curr_round(&self) -> Round {
        self.curr_round
    }

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
