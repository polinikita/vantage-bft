// PHASE4-SPEC.md §4 -- responsive proposal frontier, genesis bootstrap, R1 trigger.
//
// `Frontier` owns `a_i` (the responsive proposal frontier), which views are formally
// entered/active, and the R1 "should we propose next" check. It does *not* duplicate
// `AgbEngine`'s per-view `fixed` proposal storage -- it only needs, per view, whether
// the (already-fixed-by-`AgbEngine`) proposal was well-formed, reported via
// `Effect::Fixed` and recorded here as a single bit (`record_fixed`).

use crate::primary::View;
use crate::vantage::agb::{formed, proposer, Manifest, ResolutionEntry, ViewProposal};
use crate::vantage::lanes::LaneManager;
use config::Committee;
use crypto::PublicKey;
use std::collections::{HashMap, HashSet};

pub struct Frontier {
    name: PublicKey,
    committee: Committee,
    /// `a_i`: genesis convention starts every party at 0.
    a_i: View,
    /// Views that are "active" (R2's positive gate may run) -- via the proposal-chain
    /// advance reaching them, or via `enter(v)` (Phase 4: only ever v = 1).
    active: HashSet<View>,
    /// Per-view well-formedness of the (sticky) fixed proposal, as reported by
    /// `AgbEngine::on_propose` via `Effect::Fixed`.
    fixed_well_formed: HashMap<View, bool>,
    /// Views we have already emitted our own proposal for (R1 "not yet proposed").
    proposed: HashSet<View>,
}

impl Frontier {
    pub fn new(name: PublicKey, committee: Committee) -> Self {
        Self {
            name,
            committee,
            a_i: 0,
            active: HashSet::new(),
            fixed_well_formed: HashMap::new(),
            proposed: HashSet::new(),
        }
    }

    pub fn a_i(&self) -> View {
        self.a_i
    }

    pub fn is_active(&self, view: View) -> bool {
        self.active.contains(&view)
    }

    /// §4 "`enter(v)` also activates", extended by PHASE5-SPEC.md W5(c)'s formal-entry
    /// floor: entry to `v` also floors `a_i` to `max(a_i, v-1)` and re-runs the
    /// contiguous well-formed-prefix advance from the new floor (symmetric with
    /// `record_fixed`'s own loop) -- raising the floor can newly make an
    /// already-buffered fixed proposal *above* the old floor part of a contiguous run,
    /// and can newly satisfy R1's `a_i >= v-1` trigger for `v` itself even though no
    /// proposal for `v-1` (or anything below the floor) has actually been verified yet
    /// -- entry is a liveness floor, deliberately independent of the completion path.
    /// Returns every view newly activated by this call, in increasing order (same shape
    /// as `record_fixed`) -- `view` itself is always included unless already active.
    pub fn enter(&mut self, view: View) -> Vec<View> {
        let mut activated = Vec::new();
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

    /// Record the well-formedness outcome of `view`'s now-fixed proposal and advance
    /// `a_i` through the resulting contiguous well-formed prefix ("frontier advance:
    /// when the party has a well-formed fixed proposal ... for every view up to u, the
    /// frontier is u"). Returns every view newly activated by this call, in increasing
    /// order ("activate(v) fires exactly when processing the fixed proposal advances
    /// the frontier to v").
    pub fn record_fixed(&mut self, view: View, well_formed: bool) -> Vec<View> {
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

    /// Peek at `a_i + 1` (the only view this party could newly be entitled to propose
    /// as a direct consequence of the frontier's current value) without mutating
    /// anything -- lets the caller (PHASE6-SPEC.md §4's `Resolver`) decide whether a
    /// recovery attempt is worth computing BEFORE calling `try_propose`, since it needs
    /// to know the target view `w` up front.
    pub fn next_turn(&self) -> View {
        self.a_i + 1
    }

    pub fn already_proposed(&self, view: View) -> bool {
        self.proposed.contains(&view)
    }

    /// R1 trigger (§4): `p_i == proposer(v) ∧ a_i ≥ v−1 ∧ not yet proposed for v`,
    /// evaluated for `view = a_i + 1` -- the only view this party could newly be
    /// entitled to propose as a direct consequence of the frontier's current value.
    /// Call once at genesis bootstrap (checks view 1) and after every `record_fixed`
    /// advance (checks the new `a_i + 1`). PHASE6-SPEC.md §4 extension: `m`, computed
    /// by the caller's `Resolver` (data-only `None`, or a recovery entry) -- this
    /// method's own gate (proposed-once, proposer-turn) is unaffected either way.
    /// Thin wrapper over `propose_view` for the `a_i + 1` case (the frontier-advance
    /// path); the paper's `omega_i^+` early-wish trigger reaches other owned views
    /// through `propose_view` directly.
    pub fn try_propose(
        &mut self,
        lm: &LaneManager,
        m: Option<ResolutionEntry>,
    ) -> Option<ViewProposal> {
        self.propose_view(self.a_i + 1, lm, m)
    }

    /// R1's trigger, generalized to an arbitrary owned `view` (paper: "p_i proposes any
    /// view v it owns and hasn't proposed yet with `v <= max(a_i + 1, omega_i^+)`").
    /// `view = a_i + 1` is the `try_propose` case; `view > a_i + 1` is the passive
    /// early-wish proposal -- reachable only via the caller's `omega_i^+` bound, and
    /// only ever buffered (not activated) downstream until the frontier actually
    /// reaches it (automatic in the existing echo-stage code, unaffected by this
    /// method). Same gate as `try_propose`: not-yet-proposed, and it must actually be
    /// this party's turn for `view`.
    pub fn propose_view(
        &mut self,
        view: View,
        lm: &LaneManager,
        m: Option<ResolutionEntry>,
    ) -> Option<ViewProposal> {
        if self.proposed.contains(&view) {
            return None;
        }
        if proposer(&self.committee, view) != self.name {
            return None;
        }
        self.proposed.insert(view);
        let (c, t) = self.build_manifests(view, lm);
        Some(ViewProposal { view, c, t, m })
    }

    /// Construct C/T from the N5 registers (§3.2), processing authors in canonical
    /// (committee) order and skipping any hash already used by an earlier index
    /// (defensive `Formed_v` dedup, module plan §11). PHASE6-SPEC.md §4's recovery-turn
    /// M-selection is layered on top by the caller (`resolve.rs`/`VantageCore`), not
    /// here -- this always produces the data-only (M=None) pair.
    fn build_manifests(&self, view: View, lm: &LaneManager) -> (Manifest, Manifest) {
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
