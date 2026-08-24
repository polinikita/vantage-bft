use crate::leader::RoundRobin;
use crate::primary::View;
use crate::vantage::agb::{formed, BatchViewProposal, Manifest, ResolutionEntry, ViewProposal};
use crate::vantage::lanes::LaneManager;
use config::Committee;
use crypto::PublicKey;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Clone, Debug)]
struct TipQuarantine {
    witness: crate::vantage::BlockRef,
    probe_attempt: u32,
    probe_due: View,
}

/// Tracks active views and the contiguous well-formed proposal frontier.
pub struct Frontier {
    name: PublicKey,
    committee: Committee,
    proposers: RoundRobin,
    a_i: View,
    active: BTreeSet<View>,
    fixed_well_formed: BTreeMap<View, bool>,
    proposed: BTreeSet<View>,
    /// A completed-open non-quorum tip suppresses ordinary inclusion for its author.
    quarantined_tips: HashMap<PublicKey, TipQuarantine>,
    /// Counts this party's actual proposal broadcasts, independently of view numbers.
    own_proposal_turn: View,
    min_live_view: View,
}

impl Frontier {
    pub fn new(name: PublicKey, committee: Committee) -> Self {
        let proposers = RoundRobin::new(&committee);
        Self {
            name,
            committee,
            proposers,
            a_i: 0,
            active: BTreeSet::new(),
            fixed_well_formed: BTreeMap::new(),
            proposed: BTreeSet::new(),
            quarantined_tips: HashMap::new(),
            own_proposal_turn: 0,
            min_live_view: 1,
        }
    }

    pub fn a_i(&self) -> View {
        self.a_i
    }

    pub fn is_active(&self, view: View) -> bool {
        self.active.contains(&view)
    }

    pub fn gc_below(&mut self, floor: View) {
        if floor <= self.min_live_view {
            return;
        }
        self.active = self.active.split_off(&floor);
        self.fixed_well_formed = self.fixed_well_formed.split_off(&floor);
        self.proposed = self.proposed.split_off(&floor);
        self.a_i = self.a_i.max(floor.saturating_sub(1));
        self.min_live_view = floor;
    }

    /// Activates `view`, raises the frontier floor, and returns newly active views in order.
    pub fn enter(&mut self, view: View) -> Vec<View> {
        let mut activated = Vec::new();
        if view < self.min_live_view {
            return activated;
        }
        if self.active.insert(view) {
            activated.push(view);
        }
        let floor = view.saturating_sub(1);
        if floor > self.a_i {
            self.a_i = floor;
        }
        loop {
            let next = self.a_i + 1;
            match self.fixed_well_formed.get(&next) {
                Some(true) => {
                    self.a_i = next;
                    if self.active.insert(next) {
                        activated.push(next);
                    }
                }
                _ => break,
            }
        }
        activated
    }

    /// Records a fixed proposal and advances through the contiguous well-formed prefix.
    pub fn record_fixed(&mut self, view: View, well_formed: bool) -> Vec<View> {
        if view < self.min_live_view {
            return Vec::new();
        }
        self.fixed_well_formed.insert(view, well_formed);
        let mut activated = Vec::new();
        loop {
            let next = self.a_i + 1;
            match self.fixed_well_formed.get(&next) {
                Some(true) => {
                    self.a_i = next;
                    if self.active.insert(next) {
                        activated.push(next);
                    }
                }
                _ => break,
            }
        }
        activated
    }

    pub fn next_turn(&self) -> View {
        self.a_i + 1
    }

    pub fn already_proposed(&self, view: View) -> bool {
        self.proposed.contains(&view)
    }

    fn proposer(&self, view: View) -> PublicKey {
        self.proposers.one_based(view)
    }

    pub fn try_propose(
        &mut self,
        lm: &LaneManager,
        m: Option<ResolutionEntry>,
    ) -> Option<ViewProposal> {
        self.propose_view(self.a_i + 1, lm, m)
    }

    /// Proposes at most once and only when this node is the designated proposer.
    pub fn propose_view(
        &mut self,
        view: View,
        lm: &LaneManager,
        m: Option<ResolutionEntry>,
    ) -> Option<ViewProposal> {
        if view < self.min_live_view {
            return None;
        }
        if self.proposed.contains(&view) {
            return None;
        }
        if self.proposer(view) != self.name {
            return None;
        }
        self.proposed.insert(view);
        self.own_proposal_turn = self.own_proposal_turn.saturating_add(1);
        let (c, t) = self.build_manifests(view, lm);
        Some(ViewProposal { view, c, t, m })
    }

    /// Benchmark-only Byzantine proposal used by the mixed-open stress. It
    /// retains the ordinary core, names only this proposer's own current tip,
    /// and deliberately ignores the local quarantine for that tip. The
    /// quarantine state itself is preserved, so ordinary selection resumes as
    /// soon as the finite fault window closes.
    pub fn propose_view_mixed_open(
        &mut self,
        view: View,
        lm: &LaneManager,
    ) -> Option<ViewProposal> {
        if view < self.min_live_view
            || self.proposed.contains(&view)
            || self.proposer(view) != self.name
        {
            return None;
        }
        self.proposed.insert(view);
        self.own_proposal_turn = self.own_proposal_turn.saturating_add(1);
        self.refresh_tip_quarantine(lm);

        let mut seen = HashSet::new();
        let mut c = Manifest::new();
        for author in self.committee.authorities.keys() {
            if let Some(reference) = lm.c_candidate(author) {
                if seen.insert(reference.2.clone()) {
                    c.push(reference);
                }
            }
        }
        let mut t = Manifest::new();
        if let Some(reference) = lm
            .confirmation_candidate(&self.name)
            .or_else(|| lm.t_candidate(&self.name))
        {
            if seen.insert(reference.2.clone()) {
                t.push(reference);
            }
        }
        debug_assert!(formed(&self.committee, view, &c, &t, &None));
        Some(ViewProposal {
            view,
            c,
            t,
            m: None,
        })
    }

    pub fn propose_view_batch(
        &mut self,
        view: View,
        lm: &LaneManager,
        entries: Vec<ResolutionEntry>,
    ) -> Option<BatchViewProposal> {
        if view < self.min_live_view {
            return None;
        }
        if self.proposed.contains(&view) {
            return None;
        }
        if self.proposer(view) != self.name {
            return None;
        }
        self.proposed.insert(view);
        self.own_proposal_turn = self.own_proposal_turn.saturating_add(1);
        let (c, t) = self.build_manifests(view, lm);
        Some(BatchViewProposal {
            view,
            c,
            t,
            m: entries,
        })
    }

    fn refresh_tip_quarantine(&mut self, lm: &LaneManager) {
        self.quarantined_tips.retain(|author, state| {
            if lm.is_exact_q_available(&state.witness) {
                return false;
            }
            let advanced_core = lm.c_candidate(author).is_some_and(|core| {
                core.1 >= state.witness.1 && lm.prefix_contains(&core, &state.witness)
            });
            !advanced_core
        });
    }

    /// Quarantines each tip lacking an exact quorum in a completed-open proposal.
    /// Re-completion preserves the first witness and its retry backoff.
    pub fn quarantine_tips(&mut self, tips: &Manifest, lm: &LaneManager) {
        self.refresh_tip_quarantine(lm);
        for tip in tips {
            if !lm.is_exact_q_available(tip) {
                self.quarantined_tips
                    .entry(tip.0)
                    .or_insert_with(|| TipQuarantine {
                        witness: tip.clone(),
                        probe_attempt: 0,
                        probe_due: self.own_proposal_turn.saturating_add(1),
                    });
            }
        }
    }

    /// Builds deduplicated manifests in committee order, with `C` entries before `T` entries.
    fn build_manifests(&mut self, view: View, lm: &LaneManager) -> (Manifest, Manifest) {
        self.refresh_tip_quarantine(lm);

        let mut seen = HashSet::new();
        let mut c = Manifest::new();
        for author in self.committee.authorities.keys() {
            if let Some(r) = lm.c_candidate(author) {
                if seen.insert(r.2.clone()) {
                    c.push(r);
                }
            }
        }
        let mut t = Manifest::new();
        for author in self.committee.authorities.keys() {
            if self.quarantined_tips.contains_key(author) {
                continue;
            }
            if let Some(r) = lm
                .confirmation_candidate(author)
                .or_else(|| lm.t_candidate(author))
            {
                if seen.insert(r.2.clone()) {
                    t.push(r);
                }
            }
        }
        let probe = self
            .committee
            .authorities
            .keys()
            .enumerate()
            .filter_map(|(committee_index, author)| {
                let quarantine = self.quarantined_tips.get(author)?;
                if quarantine.probe_due > self.own_proposal_turn {
                    return None;
                }
                // Once a prefix has a general claim quorum, name that stable
                // coordinate exactly before chasing a fresher lane head.  Its
                // exact ECHO census is what makes the reference core-eligible.
                let candidate = lm
                    .confirmation_candidate(author)
                    .or_else(|| lm.t_candidate(author))?;
                if seen.contains(&candidate.2) {
                    return None;
                }
                Some((quarantine.probe_due, committee_index, *author, candidate))
            })
            .min_by_key(|(due, committee_index, _, _)| (*due, *committee_index));
        if let Some((_, _, author, candidate)) = probe {
            if seen.insert(candidate.2.clone()) {
                t.push(candidate);
                let quarantine = self
                    .quarantined_tips
                    .get_mut(&author)
                    .expect("selected quarantine remains present");
                let gap = 1u64
                    .checked_shl(quarantine.probe_attempt.min(63))
                    .unwrap_or(u64::MAX);
                quarantine.probe_due = self.own_proposal_turn.saturating_add(gap);
                quarantine.probe_attempt = quarantine.probe_attempt.saturating_add(1);
            }
        }
        // A probe is selected after ordinary tips, but Formed requires canonical
        // author order regardless of which path supplied an entry.
        t.sort_by_key(|r| r.0);
        debug_assert!(
            formed(&self.committee, view, &c, &t, &None),
            "own construction must always be Formed_v"
        );
        (c, t)
    }

    #[cfg(test)]
    pub(crate) fn quarantine_for_test(
        &self,
        author: &PublicKey,
    ) -> Option<(crate::vantage::BlockRef, u32, View)> {
        self.quarantined_tips
            .get(author)
            .map(|state| (state.witness.clone(), state.probe_attempt, state.probe_due))
    }
}
