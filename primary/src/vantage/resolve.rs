// PHASE6-SPEC.md §4 -- proposer recovery turns (extends R1 / `Frontier::try_propose`).
//
// `Resolver` owns the two pieces of persistent proposer state the spec names: a
// next-turn bit (data-only vs recovery) and a per-target candidate pointer. It computes
// justified candidates entirely by querying `AgbEngine`'s existing per-view first-hand
// censuses (the query accessors added alongside §2/§3) -- no parallel counting state
// (reuse rule).

use crate::primary::View;
use crate::vantage::agb::{AgbEngine, ResolutionEntry};
use crate::vantage::Thresholds;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

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
    candidate_pointer: BTreeMap<View, ResolutionEntry>,
    /// D7-1 (PHASE7-PREP-NOTES.md; coordinator-sanctioned WITH this time bound;
    /// Finding A's root-cause fix): the last time this party either (a) itself minted
    /// a recovery attempt for `u`, or (b) observed (via `note_carrier_report`, at the
    /// first genuine completion of some carrier `w` with `M_w` targeting `u`) evidence
    /// that an attempt for `u` is already in flight. While the marker is younger than
    /// `expiry`, `decide` treats `u` like an empty-candidate view (never blocks a
    /// later target); once it ages past `expiry` it is eligible again. Time-BOUNDED
    /// deliberately, never open-ended: O3's progress argument only guarantees
    /// anchoring for proposals that complete at EVERY correct party, so a carrier that
    /// completed non-universally could look permanently "in flight" from this party's
    /// view -- open-ended suppression would then extinguish the attempt stream for
    /// `u`, a liveness loss. Bounding by `expiry` keeps attempts infinitely-often in
    /// the limit (this only throttles the mint rate, never which entries are ever
    /// chosen), so the liveness argument survives; the paper author should still rule
    /// on this.
    in_flight: BTreeMap<View, Instant>,
    /// 12Δ, per the coordinator's ruling (D7-1).
    expiry: Duration,
    /// Fable perf audit: a monotone lower bound on where an unresolved view can be, so
    /// `decide` doesn't rescan the whole `1..=w-3` prefix on every own-proposer turn.
    /// Sound because "resolved" (`AgbEngine::is_sealed(u) || ControlLog::
    /// is_anchor_resolved(u)`, the predicate the caller always passes as `resolved`) is
    /// sticky and never regresses: `AgbEngine::is_sealed` only flips a view's `sealed`
    /// field `None -> Some` (first submission wins; `submit_anchor`/the fastseal path
    /// leave an already-`Some` field untouched), and no entry is ever removed from
    /// `AgbEngine`'s per-view map, so it can never un-seal; `ControlLog::
    /// is_anchor_resolved` only ever inserts into the `anchored` set (never removes),
    /// so it can never un-anchor. Once `resolved(u)` is witnessed true for some `u`, it
    /// is true for every later call -- advancing past `u` here only ever skips a view
    /// this or an earlier call already confirmed resolved, never one whose resolved-
    /// ness was merely assumed. Starts at 1 (nothing resolved yet); only ever advances,
    /// lazily, at the top of `decide`.
    resolved_watermark: View,
}

impl Resolver {
    /// `n` = committee size (`Committee::size()`); thresholds derived via the shared
    /// `Thresholds` type, same as every other Vantage component (`AgbEngine`/
    /// `Pacemaker`/`ControlLog`). `delta_ms` -- D7-1's 12Δ expiry (same Δ every other
    /// Vantage timing constant derives from).
    pub fn new(n: usize, delta_ms: u64) -> Self {
        let thresholds = Thresholds::from_party_count(n);
        Self {
            f_plus_1_parties: thresholds.f_plus_1_parties,
            two_f_plus_1_parties: thresholds.two_f_plus_1_parties,
            next_is_recovery: false,
            candidate_pointer: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            expiry: Duration::from_millis(12 * delta_ms),
            resolved_watermark: 1,
        }
    }

    /// D7-1: is `u` currently suppressed (an in-flight marker younger than `expiry`)?
    fn is_in_flight(&self, u: View, now: Instant) -> bool {
        self.in_flight
            .get(&u)
            .is_some_and(|t| now.saturating_duration_since(*t) < self.expiry)
    }

    /// D7-1: record fresh in-flight evidence for `u` at `now` -- called both by
    /// `decide` (our own attempt, immediately) and by the caller when this party
    /// observes (via `Effect::CompletionReportable`) a carrier -- ours or another
    /// party's -- whose `M` targets `u`, at that carrier's first genuine completion.
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

    pub fn gc_below(&mut self, floor: View) {
        self.candidate_pointer = self.candidate_pointer.split_off(&floor);
        self.in_flight = self.in_flight.split_off(&floor);
    }

    /// §4's canonical sort key: payloads sorted lexicographically by `bincode(C,T)`,
    /// Full before Core per payload, Skip last (regardless of payload bytes -- the
    /// leading `bool` component dominates the comparison).
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

    /// §4's full per-turn decision: scan unsealed/un-anchor-resolved views `u <= w-3`
    /// ascending (`resolved` folds in both "sealed at the AGB layer" and, once §6
    /// lands, "already anchor-resolved"), skipping any view whose justified set is
    /// empty (never blocks a later target) OR whose D7-1 in-flight marker hasn't yet
    /// expired (same "never blocks a later target" treatment). Returns `None` for a
    /// data-only proposal (either no target qualifies at all -- bit left untouched --
    /// or the bit selected data-only this turn at the first qualifying target, which
    /// still flips the bit); `Some(entry)` for a recovery proposal targeting the first
    /// qualifying view. `now` -- D7-1's in-flight age check and marker refresh.
    pub fn decide(
        &mut self,
        agb: &AgbEngine,
        w: View,
        now: Instant,
        resolved: impl Fn(View) -> bool,
    ) -> Option<ResolutionEntry> {
        let scan_limit = w.saturating_sub(3);
        // Advance the watermark over the (possibly newly-grown) contiguous resolved
        // prefix before scanning -- sound by the monotonicity argument on the field
        // itself; identical result to scanning from 1, since every `u` skipped here was
        // just witnessed `resolved(u) == true`.
        while self.resolved_watermark <= scan_limit && resolved(self.resolved_watermark) {
            self.resolved_watermark += 1;
        }
        for u in self.resolved_watermark..=scan_limit {
            if resolved(u) {
                continue;
            }
            let candidates = self.justified_candidates(agb, u);
            if candidates.is_empty() {
                continue; // no-evidence view never blocks a later target
            }
            if self.is_in_flight(u, now) {
                continue; // D7-1: suppressed, not yet expired -- never blocks a later target
            }
            // A qualifying target was found -- the bit decides, then flips (§4 step 3).
            if !self.next_is_recovery {
                self.next_is_recovery = true;
                return None;
            }
            self.next_is_recovery = false;
            let entry = self.pick_and_advance(u, &candidates);
            // D7-1: our own attempt is itself in-flight evidence, immediately.
            self.in_flight.insert(u, now);
            // PHASE7-PREP-NOTES.md Finding A: diagnostic-only observational log (no
            // behavior change) -- every recovery attempt actually attached to a
            // proposal, so a run's log can show how many carrier views ever attempt a
            // given target and how far apart (in view number / wall clock) they are.
            log::info!(
                "vantage resolver: recovery target u={} attached at carrier turn w={}",
                u,
                w
            );
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
