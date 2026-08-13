use crate::leader::RoundRobin;
use crate::primary::View;
use crate::vantage::agb::{formed, BatchViewProposal, Manifest, ResolutionEntry, ViewProposal};
use crate::vantage::lanes::LaneManager;
use config::Committee;
use crypto::PublicKey;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Tracks active views and the contiguous well-formed proposal frontier.
pub struct Frontier {
    name: PublicKey,
    committee: Committee,
    proposers: RoundRobin,
    a_i: View,
    active: BTreeSet<View>,
    fixed_well_formed: BTreeMap<View, bool>,
    proposed: BTreeSet<View>,
    /// A non-quorum tip that contributed to a local READY-mix quarantines its
    /// author until that prefix (or a containing descendant) becomes core.
    quarantined_tips: HashMap<PublicKey, crate::vantage::BlockRef>,
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
        let (c, t) = self.build_manifests(view, lm);
        Some(ViewProposal { view, c, t, m })
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
        let (c, t) = self.build_manifests(view, lm);
        Some(BatchViewProposal {
            view,
            c,
            t,
            m: entries,
        })
    }

    fn refresh_tip_quarantine(&mut self, lm: &LaneManager) {
        let quorum = self.committee.quorum_threshold();
        self.quarantined_tips.retain(|author, blocked| {
            if lm.is_q_available(blocked, quorum) {
                return false;
            }
            let advanced_core = lm
                .c_candidate(author)
                .is_some_and(|core| core.1 >= blocked.1 && lm.prefix_contains(&core, blocked));
            !advanced_core
        });
    }

    /// Quarantines each non-quorum tip that contributed to a local READY-mix.
    /// A later proposal may include that author again once the witnessed prefix
    /// reaches a quorum or a containing core prefix advances beyond it.
    pub fn quarantine_tips(&mut self, tips: &Manifest, lm: &LaneManager) {
        self.refresh_tip_quarantine(lm);
        let quorum = self.committee.quorum_threshold();
        for tip in tips {
            if !lm.is_q_available(tip, quorum) {
                self.quarantined_tips
                    .entry(tip.0)
                    .or_insert_with(|| tip.clone());
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
            if let Some(r) = lm.t_candidate(author) {
                if seen.insert(r.2.clone()) {
                    t.push(r);
                }
            }
        }
        debug_assert!(
            formed(&self.committee, view, &c, &t, &None),
            "own construction must always be Formed_v"
        );
        (c, t)
    }
}
