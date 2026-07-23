// PHASE6-SPEC.md §4 -- proposer recovery turns (extends R1 / `Frontier::try_propose`).
//
// `Resolver` owns the two pieces of persistent proposer state the spec names: a
// next-turn bit (data-only vs recovery) and a per-target candidate pointer. It computes
// justified candidates entirely by querying `AgbEngine`'s existing per-view first-hand
// censuses (the query accessors added alongside §2/§3) -- no parallel counting state
// (reuse rule).

use crate::primary::View;
use crate::vantage::agb::{AgbEngine, ResolutionEntry};
use std::collections::HashMap;

pub struct Resolver {
    f_plus_1_parties: usize,
    two_f_plus_1_parties: usize,
    /// The next-turn bit: `false` = the next qualifying turn is data-only, `true` = the
    /// next qualifying turn is a recovery attempt. Initially data-only (module plan
    /// §7/§4's literal phrasing). Only ever consulted/flipped when at least one target
    /// qualifies (§4 step 2/3) -- otherwise left untouched.
    next_is_recovery: bool,
    /// Per-target candidate pointer: the last candidate proposed for this target,
    /// named by VALUE (never a list index -- §4: "the pointer ... names a candidate,
    /// never a list index"), so it survives the canonical justified list changing
    /// shape/order between attempts (new evidence can only ever grow the set, never
    /// remove an already-qualified entry, but new smaller-order entries can shift
    /// positions).
    candidate_pointer: HashMap<View, ResolutionEntry>,
}

impl Resolver {
    /// `n` = committee size (`Committee::size()`); the AGB engine's own
    /// `f_plus_1_parties` formula, duplicated here (each component derives its own
    /// committee-based threshold constants, same pattern as `AgbEngine`/`Pacemaker` --
    /// this is a threshold constant, not counting state, so it doesn't violate the
    /// reuse rule).
    pub fn new(n: usize) -> Self {
        Self {
            f_plus_1_parties: (n - 1) / 3 + 1,
            two_f_plus_1_parties: 2 * ((n - 1) / 3) + 1,
            next_is_recovery: false,
            candidate_pointer: HashMap::new(),
        }
    }

    /// §4's canonical sort key: payloads sorted lexicographically by `bincode(C,T)`,
    /// Full before Core per payload, Skip last (regardless of payload bytes -- the
    /// leading `bool` component dominates the comparison).
    fn canonical_key(entry: &ResolutionEntry) -> (bool, Vec<u8>, u8) {
        match entry {
            ResolutionEntry::Full(_, c, t) => (false, bincode::serialize(&(c, t)).expect("serializes"), 0),
            ResolutionEntry::Core(_, c, t) => (false, bincode::serialize(&(c, t)).expect("serializes"), 1),
            ResolutionEntry::Skip(_) => (true, Vec::new(), 2),
        }
    }

    /// §4 step 1: every candidate justified for target view `u`, in canonical order.
    /// Prerequisite for ANY candidate: `>= 2f+1` (party count) counted ready-stage
    /// statements for `u` (any kind, noready included) -- an older view with no
    /// evidence at all never blocks a later target (empty result here is exactly that
    /// "no evidence" case, handled identically whether the prerequisite fails or every
    /// individual candidate check fails).
    pub fn justified_candidates(&self, agb: &AgbEngine, u: View) -> Vec<ResolutionEntry> {
        let mut out = Vec::new();
        if agb.ready_stage_total(u) < self.two_f_plus_1_parties {
            return out;
        }
        let payloads = agb.candidate_payloads(u);
        // Core's second clause ("some 2f+1-subset ... containing NO grade-1
        // proposal-ready") is payload-independent -- compute once.
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

    /// The per-target pointer's current pick from `candidates` (non-empty, sorted
    /// canonically): the stored pointer's value if still present in the list (found by
    /// exact equality), else the first candidate (covers both "never attempted this
    /// target before" and the defensive fallback if a previously-pointed candidate
    /// somehow isn't present -- unreachable under the monotonic-evidence model, but
    /// harmless). Advances the stored pointer to the cyclically-next candidate for the
    /// FOLLOWING attempt.
    fn pick_and_advance(&mut self, target: View, candidates: &[ResolutionEntry]) -> ResolutionEntry {
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

    /// §4's full per-turn decision: scan unsealed/un-anchor-resolved views `u <= w-3`
    /// ascending (`resolved` folds in both "sealed at the AGB layer" and, once §6
    /// lands, "already anchor-resolved"), skipping any view whose justified set is
    /// empty (never blocks a later target). Returns `None` for a data-only proposal
    /// (either no target qualifies at all -- bit left untouched -- or the bit selected
    /// data-only this turn at the first qualifying target, which still flips the bit);
    /// `Some(entry)` for a recovery proposal targeting the first qualifying view.
    pub fn decide(&mut self, agb: &AgbEngine, w: View, resolved: impl Fn(View) -> bool) -> Option<ResolutionEntry> {
        for u in 1..=w.saturating_sub(3) {
            if resolved(u) {
                continue;
            }
            let candidates = self.justified_candidates(agb, u);
            if candidates.is_empty() {
                continue; // no-evidence view never blocks a later target
            }
            // A qualifying target was found -- the bit decides, then flips (§4 step 3).
            if !self.next_is_recovery {
                self.next_is_recovery = true;
                return None;
            }
            self.next_is_recovery = false;
            let entry = self.pick_and_advance(u, &candidates);
            // PHASE7-PREP-NOTES.md Finding A: diagnostic-only observational log (no
            // behavior change) -- every recovery attempt actually attached to a
            // proposal, so a run's log can show how many carrier views ever attempt a
            // given target and how far apart (in view number / wall clock) they are.
            log::info!("vantage resolver: recovery target u={} attached at carrier turn w={}", u, w);
            return Some(entry);
        }
        None // no target qualifies at all -- bit unchanged (§4 step 2)
    }

    #[cfg(test)]
    pub(crate) fn next_is_recovery_for_test(&self) -> bool {
        self.next_is_recovery
    }

    #[cfg(test)]
    pub(crate) fn pointer_for_test(&self, target: View) -> Option<ResolutionEntry> {
        self.candidate_pointer.get(&target).cloned()
    }
}
