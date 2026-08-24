//! Extracts target-local values justified by first-hand AGB evidence.
//!
//! Scheduling and agreement belong to [`super::direct_resolution`]. This
//! module deliberately has no carrier state, retry timer, alternation bit, or
//! global resolution cursor beyond the output-derived scan watermark.

use crate::primary::View;
use crate::vantage::agb::{AgbEngine, ResolutionEntry};
use crate::vantage::Thresholds;

/// Deterministically derives resolver candidates from one party's AGB state.
pub struct ResolutionEvidence {
    f_plus_1_parties: usize,
    two_f_plus_1_parties: usize,
    resolved_watermark: View,
}

impl ResolutionEvidence {
    pub fn new(n: usize, _delta_ms: u64) -> Self {
        let thresholds = Thresholds::from_party_count(n);
        Self {
            f_plus_1_parties: thresholds.f_plus_1_parties,
            two_f_plus_1_parties: thresholds.two_f_plus_1_parties,
            resolved_watermark: 1,
        }
    }

    pub fn resolved_watermark(&self) -> View {
        self.resolved_watermark
    }

    pub fn gc_floor(&self, gc_window: View) -> View {
        self.resolved_watermark.saturating_sub(gc_window).max(1)
    }

    /// Marks every target through `view` as terminal.
    pub fn note_resolved_through(&mut self, view: View) {
        self.resolved_watermark = self.resolved_watermark.max(view.saturating_add(1));
    }

    pub fn gc_below(&mut self, floor: View) {
        self.resolved_watermark = self.resolved_watermark.max(floor);
    }

    fn canonical_key(entry: &ResolutionEntry) -> (bool, Vec<u8>, u8) {
        match entry {
            ResolutionEntry::Full(_, c, t) => {
                (false, bincode::serialize(&(c, t)).expect("serializes"), 0)
            }
            ResolutionEntry::Core(_, c, t) => {
                (false, bincode::serialize(&(c, t)).expect("serializes"), 1)
            }
            ResolutionEntry::Skip(_) => (true, Vec::new(), 2),
        }
    }

    /// Returns candidates in canonical payload order, with full before core and skip last.
    ///
    /// Every candidate requires `2f + 1` ready-stage statements. Full and core
    /// candidates additionally require their respective `f + 1` ECHO predicates.
    pub fn justified_candidates(&self, agb: &AgbEngine, target: View) -> Vec<ResolutionEntry> {
        if agb.ready_stage_total(target) < self.two_f_plus_1_parties {
            return Vec::new();
        }

        let mut candidates = Vec::new();
        let payloads = agb.candidate_payloads(target);
        let core_ready_clause =
            agb.ready_stage_non_grade1_count(target) >= self.two_f_plus_1_parties;
        for (core, tip) in &payloads {
            if agb.echo_grade1_count_for(target, core, tip) >= self.f_plus_1_parties {
                candidates.push(ResolutionEntry::Full(target, core.clone(), tip.clone()));
            }
            if core_ready_clause
                && agb.echo_any_grade_count_for(target, core, tip) >= self.f_plus_1_parties
            {
                candidates.push(ResolutionEntry::Core(target, core.clone(), tip.clone()));
            }
        }
        if agb.noready_count(target) >= self.two_f_plus_1_parties {
            candidates.push(ResolutionEntry::Skip(target));
        }
        candidates
            .sort_by(|left, right| Self::canonical_key(left).cmp(&Self::canonical_key(right)));
        candidates
    }
}
