use crate::primary::View;
use crate::vantage::agb::{AgbEngine, ResolutionEntry};
use crate::vantage::Thresholds;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Selects justified recovery entries for proposer turns.
pub struct Resolver {
    f_plus_1_parties: usize,
    two_f_plus_1_parties: usize,
    /// Alternates qualifying data-only and recovery turns.
    next_is_recovery: bool,
    /// Next candidate per target, stored by value so canonical list reordering is safe.
    candidate_pointer: BTreeMap<View, ResolutionEntry>,
    /// Last observed recovery attempt per target.
    in_flight: BTreeMap<View, Instant>,
    /// Finite suppression interval that prevents one missed attempt from blocking recovery.
    expiry: Duration,
    /// Alternates batched and single-entry attempts for the oldest target.
    alternation_target: Option<View>,
    alternation_force_single: bool,
    resolved_watermark: View,
}

impl Resolver {
    /// Creates a resolver with a `12 * delta_ms` in-flight expiry.
    pub fn new(n: usize, delta_ms: u64) -> Self {
        let thresholds = Thresholds::from_party_count(n);
        Self {
            f_plus_1_parties: thresholds.f_plus_1_parties,
            two_f_plus_1_parties: thresholds.two_f_plus_1_parties,
            next_is_recovery: false,
            candidate_pointer: BTreeMap::new(),
            alternation_target: None,
            alternation_force_single: false,
            in_flight: BTreeMap::new(),
            expiry: Duration::from_millis(12 * delta_ms),
            resolved_watermark: 1,
        }
    }

    fn is_in_flight(&self, u: View, now: Instant) -> bool {
        self.in_flight
            .get(&u)
            .is_some_and(|t| now.saturating_duration_since(*t) < self.expiry)
    }

    pub fn note_carrier_report(&mut self, u: View, now: Instant) {
        if u < self.resolved_watermark {
            return;
        }
        self.in_flight.insert(u, now);
    }

    pub fn resolved_watermark(&self) -> View {
        self.resolved_watermark
    }

    pub fn gc_floor(&self, gc_window: View) -> View {
        self.resolved_watermark.saturating_sub(gc_window).max(1)
    }

    /// Marks every view through `view` as terminal.
    pub fn note_resolved_through(&mut self, view: View) {
        let next = view.saturating_add(1);
        if next > self.resolved_watermark {
            self.resolved_watermark = next;
        }
    }

    pub fn gc_below(&mut self, floor: View) {
        self.candidate_pointer = self.candidate_pointer.split_off(&floor);
        self.in_flight = self.in_flight.split_off(&floor);
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
    /// Every candidate requires `2f + 1` ready-stage statements. Full and core candidates
    /// additionally require their respective `f + 1` echo predicates.
    pub fn justified_candidates(&self, agb: &AgbEngine, u: View) -> Vec<ResolutionEntry> {
        let mut out = Vec::new();
        if agb.ready_stage_total(u) < self.two_f_plus_1_parties {
            return out;
        }
        let payloads = agb.candidate_payloads(u);
        let core_ready_clause = agb.ready_stage_non_grade1_count(u) >= self.two_f_plus_1_parties;
        for (c, t) in &payloads {
            if agb.echo_grade1_count_for(u, c, t) >= self.f_plus_1_parties {
                out.push(ResolutionEntry::Full(u, c.clone(), t.clone()));
            }
            if core_ready_clause && agb.echo_any_grade_count_for(u, c, t) >= self.f_plus_1_parties {
                out.push(ResolutionEntry::Core(u, c.clone(), t.clone()));
            }
        }
        if agb.noready_count(u) >= self.two_f_plus_1_parties {
            out.push(ResolutionEntry::Skip(u));
        }
        out.sort_by(|a, b| Self::canonical_key(a).cmp(&Self::canonical_key(b)));
        out
    }

    fn pick_and_advance(
        &mut self,
        target: View,
        candidates: &[ResolutionEntry],
    ) -> ResolutionEntry {
        let len = candidates.len();
        let start = match self.candidate_pointer.get(&target) {
            Some(prev) => candidates.iter().position(|c| c == prev).unwrap_or(0),
            None => 0,
        };
        let pick = candidates[start].clone();
        let next = candidates[(start + 1) % len].clone();
        self.candidate_pointer.insert(target, next);
        pick
    }

    /// Selects the oldest qualifying target no later than `w - 3`.
    fn decide_head(
        &mut self,
        agb: &AgbEngine,
        w: View,
        now: Instant,
        resolved: &impl Fn(View) -> bool,
    ) -> Option<(View, ResolutionEntry)> {
        let scan_limit = w.saturating_sub(3);
        while self.resolved_watermark <= scan_limit && resolved(self.resolved_watermark) {
            self.resolved_watermark += 1;
        }
        let mut u = self.resolved_watermark;
        let (u1, candidates) = loop {
            if u > scan_limit {
                return None;
            }
            if resolved(u) {
                u += 1;
                continue;
            }
            let candidates = self.justified_candidates(agb, u);
            if candidates.is_empty() {
                u += 1;
                continue;
            }
            if self.is_in_flight(u, now) {
                u += 1;
                continue;
            }
            break (u, candidates);
        };
        if !self.next_is_recovery {
            self.next_is_recovery = true;
            return None;
        }
        self.next_is_recovery = false;
        let entry = self.pick_and_advance(u1, &candidates);
        self.in_flight.insert(u1, now);
        Some((u1, entry))
    }

    pub fn decide(
        &mut self,
        agb: &AgbEngine,
        w: View,
        now: Instant,
        resolved: impl Fn(View) -> bool,
    ) -> Option<ResolutionEntry> {
        let (u, entry) = self.decide_head(agb, w, now, &resolved)?;
        log::debug!(
            "vantage resolver: recovery target u={} attached at carrier turn w={}",
            u,
            w
        );
        Some(entry)
    }

    fn f_cap(&self) -> usize {
        self.f_plus_1_parties.saturating_sub(1).max(1)
    }

    fn skip_justified(&self, agb: &AgbEngine, u: View) -> bool {
        agb.noready_count(u) >= self.two_f_plus_1_parties
    }

    /// Returns no entries for a data-only turn or up to `f` recovery entries.
    ///
    /// Multi-entry results contain only skip entries. Every other attempt for the same
    /// oldest target is forced to one entry so a refused additional entry cannot block it.
    pub fn decide_prefix(
        &mut self,
        agb: &AgbEngine,
        w: View,
        now: Instant,
        resolved: impl Fn(View) -> bool,
    ) -> Vec<ResolutionEntry> {
        let scan_limit = w.saturating_sub(3);
        let Some((u1, u1_entry)) = self.decide_head(agb, w, now, &resolved) else {
            return Vec::new();
        };

        if self.alternation_target != Some(u1) {
            self.alternation_target = Some(u1);
            self.alternation_force_single = false;
        }
        let force_single = self.alternation_force_single;
        self.alternation_force_single = !self.alternation_force_single;

        if force_single || u1_entry != ResolutionEntry::Skip(u1) {
            log::debug!(
                "vantage resolver: recovery target u={} attached at carrier turn w={} (batch size 1)",
                u1,
                w
            );
            return vec![u1_entry];
        }

        let cap = self.f_cap();
        let mut out = vec![u1_entry];
        let mut u = u1 + 1;
        while u <= scan_limit && out.len() < cap {
            if resolved(u) {
                u += 1;
                continue;
            }
            if agb.ready_stage_total(u) < self.two_f_plus_1_parties {
                u += 1;
                continue;
            }
            if !self.skip_justified(agb, u) {
                break;
            }
            if self.is_in_flight(u, now) {
                break;
            }
            out.push(ResolutionEntry::Skip(u));
            self.in_flight.insert(u, now);
            u += 1;
        }
        log::debug!(
            "vantage resolver: recovery target u={} attached at carrier turn w={} (batch size {})",
            u1,
            w,
            out.len()
        );
        out
    }

    #[cfg(test)]
    pub(crate) fn next_is_recovery_for_test(&self) -> bool {
        self.next_is_recovery
    }

    #[cfg(test)]
    pub(crate) fn alternation_state_for_test(&self) -> (Option<View>, bool) {
        (self.alternation_target, self.alternation_force_single)
    }

    #[cfg(test)]
    pub(crate) fn pointer_for_test(&self, target: View) -> Option<ResolutionEntry> {
        self.candidate_pointer.get(&target).cloned()
    }
}
