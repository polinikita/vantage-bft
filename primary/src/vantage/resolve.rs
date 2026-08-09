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
    /// signature-free.tex's "Batched resolution entries" paragraph: D7-2's
    /// alternation state -- per FIXED oldest (first-qualifying) target, every OTHER
    /// recovery attempt must carry the single-entry vector (`k=1`), never the full
    /// prefix. Rationale: a batched proposal is refused whole if ANY coordinate is
    /// refused, so without this, a rider coordinate cycling through refused
    /// candidates could starve the oldest target forever. `alternation_target` is
    /// the first-qualifying target this toggle currently pertains to (`None` until
    /// `decide_prefix`'s first call); `alternation_force_single` is whether the
    /// NEXT attempt for that SAME target must be truncated to `k=1` -- flips after
    /// every attempt, and resets (to "next attempt is full") whenever the observed
    /// first-qualifying target changes (the oldest target got resolved, or a new,
    /// even-older target somehow became the first-qualifying one).
    alternation_target: Option<View>,
    alternation_force_single: bool,
    /// Fable perf audit: a monotone lower bound on where an unresolved view can be, so
    /// `decide` doesn't rescan the whole `1..=w-3` prefix on every own-proposer turn.
    /// Sound because "resolved" (`AgbEngine::is_sealed(u) || ControlLog::
    /// is_anchor_resolved(u)`, the predicate the caller always passes as `resolved`) is
    /// sticky and never regresses: `AgbEngine::is_sealed` only flips a view's `sealed`
    /// field `None -> Some` (first submission wins; `submit_anchor`/the fastseal path
    /// leave an already-`Some` field untouched); `ControlLog::is_anchor_resolved` only
    /// ever inserts into the `anchored` set.
    ///
    /// CORRECTION: this comment used to justify stickiness with "no entry is ever removed
    /// from `AgbEngine`'s per-view map" and "(never removes)" from `anchored`. Both became
    /// FALSE when view GC landed -- `AgbEngine::gc_below` and `ControlLog::gc_below` both
    /// `split_off` those very collections. Stickiness survives for a different reason:
    /// both predicates report `true` for a PRUNED view (`is_pruned`/`anchor_resolved`), so
    /// removal flips them `false -> true` at worst, never `true -> false`. That is sound
    /// here specifically because the floor is `resolved_watermark - window`, so a pruned
    /// `u` is one this party already witnessed resolved; do not generalise the
    /// "pruned means resolved" shortcut to predicates that gate a REMOTE claim (see
    /// `AgbEngine::meta_ok`/`compute_origin`, which must not and no longer do).
    /// Once `resolved(u)` is witnessed true for some `u`, it
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
            alternation_target: None,
            alternation_force_single: false,
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

    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §10: verified checkpoint state has been installed
    /// through `view`, so every view at or below it is terminally decided.
    ///
    /// That is the same fact `decide_head`'s scan establishes one view at a time, reached
    /// directly instead. Stating it matters twice over. It stops the resolver proposing
    /// recovery for a range whose outcome is already committed -- `decide_head` starts its
    /// scan at the watermark, so an un-advanced watermark would keep targeting installed
    /// views forever, since their AGB state was pruned and `resolved(u)` can no longer be
    /// witnessed. And because the GC floor IS `resolved_watermark - window`, this is what
    /// lets that range's AGB, control, frontier, digest-statement and timer state be
    /// discarded at all: an installed node that never advanced its watermark would keep
    /// every view it skipped over, which on the straggler this mechanism exists to rescue
    /// is precisely the state it cannot afford.
    ///
    /// Monotonic, so a stale or overlapping install cannot undo resolution progress made
    /// while its blocks were being fetched.
    pub fn note_installed_through(&mut self, view: View) {
        let next = view.saturating_add(1);
        if next > self.resolved_watermark {
            self.resolved_watermark = next;
        }
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

    /// §4's coordinate-1 core, shared by `decide` (which returns exactly this) and
    /// `decide_prefix` (which may extend past it): scan unsealed/un-anchor-resolved
    /// views `u <= w-3` ascending (`resolved` folds in both "sealed at the AGB layer"
    /// and "already anchor-resolved"), skipping any view whose justified set is empty
    /// (never blocks a later target) OR whose D7-1 in-flight marker hasn't yet
    /// expired (same "never blocks a later target" treatment), until a genuinely
    /// qualifying view is found. The data-only/recovery bit then decides (§4 step
    /// 2/3): `None` for a data-only turn (either no target qualifies at all -- bit
    /// left untouched -- or the bit selected data-only this turn at the first
    /// qualifying target, which still flips the bit); otherwise the bit flips to
    /// data-only for next time, `pick_and_advance` chooses this target's candidate by
    /// its cyclic per-target pointer, and the target is marked in-flight immediately
    /// (D7-1: our own attempt is itself in-flight evidence). `now` -- D7-1's
    /// in-flight age check and marker refresh.
    fn decide_head(
        &mut self,
        agb: &AgbEngine,
        w: View,
        now: Instant,
        resolved: &impl Fn(View) -> bool,
    ) -> Option<(View, ResolutionEntry)> {
        let scan_limit = w.saturating_sub(3);
        // Advance the watermark over the (possibly newly-grown) contiguous resolved
        // prefix before scanning -- sound by the monotonicity argument on the field
        // itself; identical result to scanning from 1, since every `u` skipped here was
        // just witnessed `resolved(u) == true`.
        while self.resolved_watermark <= scan_limit && resolved(self.resolved_watermark) {
            self.resolved_watermark += 1;
        }
        let mut u = self.resolved_watermark;
        let (u1, candidates) = loop {
            if u > scan_limit {
                return None; // no target qualifies at all -- bit unchanged (§4 step 2)
            }
            if resolved(u) {
                u += 1;
                continue;
            }
            let candidates = self.justified_candidates(agb, u);
            if candidates.is_empty() {
                u += 1;
                continue; // no-evidence view never blocks a later target
            }
            if self.is_in_flight(u, now) {
                u += 1;
                continue; // D7-1: suppressed, not yet expired -- never blocks a later target
            }
            break (u, candidates);
        };
        // A qualifying target was found -- the bit decides, then flips (§4 step 3).
        if !self.next_is_recovery {
            self.next_is_recovery = true;
            return None;
        }
        self.next_is_recovery = false;
        let entry = self.pick_and_advance(u1, &candidates);
        // D7-1: our own attempt is itself in-flight evidence, immediately.
        self.in_flight.insert(u1, now);
        Some((u1, entry))
    }

    /// §4's full per-turn decision, single-entry shape: `decide_head` alone, with a
    /// diagnostic log line. `None` for a data-only proposal; `Some(entry)` for a
    /// recovery proposal targeting the first qualifying view.
    pub fn decide(
        &mut self,
        agb: &AgbEngine,
        w: View,
        now: Instant,
        resolved: impl Fn(View) -> bool,
    ) -> Option<ResolutionEntry> {
        let (u, entry) = self.decide_head(agb, w, now, &resolved)?;
        // PHASE7-PREP-NOTES.md Finding A: diagnostic-only observational log (no
        // behavior change) -- every recovery attempt actually attached to a
        // proposal, so a run's log can show how many carrier views ever attempt a
        // given target and how far apart (in view number / wall clock) they are.
        log::info!(
            "vantage resolver: recovery target u={} attached at carrier turn w={}",
            u,
            w
        );
        Some(entry)
    }

    /// The vector cap, `f` -- derived from the committee (never a config knob),
    /// floored at 1 so the single-entry case is always representable. Matches
    /// `agb::batch_cap`'s identical formula (kept independently computed here since
    /// `Resolver` only ever holds derived thresholds, not a `Committee` handle).
    fn f_cap(&self) -> usize {
        self.f_plus_1_parties.saturating_sub(1).max(1)
    }

    /// signature-free.tex 704fb29, par:batched-anchors ("the audit narrows the
    /// previously added recovery batching rule"): is `u` skip-justified specifically
    /// (a first-hand `2f+1` no-ready census) -- the only candidate shape a batch
    /// coordinate beyond `u_1` may ever carry (`agb::formed_batch` rejects a
    /// full/core coordinate outright). Equivalent to `justified_candidates`'s own
    /// `Skip` clause, factored out so the prefix scan below can check it without
    /// building the full (possibly full/core-inclusive) candidate list per target.
    fn skip_justified(&self, agb: &AgbEngine, u: View) -> bool {
        agb.noready_count(u) >= self.two_f_plus_1_parties
    }

    /// signature-free.tex's "Batched resolution entries" paragraph, narrowed by the
    /// 704fb29 audit (par:batched-anchors): `decide`'s prefix-scanning
    /// generalization, and the only recovery-turn entry point production wiring
    /// calls (unconditional protocol behavior, not a flag). Returns 0 entries
    /// (data-only turn, or no target qualifies at all -- exactly `decide_head`'s own
    /// `None` cases), or `1..=f` entries for a recovery turn.
    ///
    /// Coordinate 1 (the first-qualifying target `u_1`) is `decide_head` itself --
    /// same scan, same data-only/recovery bit consumption, same cyclic
    /// `pick_and_advance` choice a lone single-entry attempt would use (a full/core
    /// pick is exactly as likely here as on any other turn).
    ///
    /// What may extend past `u_1` is narrowed by the same audit: the vector wire
    /// shape is skip-only ("a vector with a full or core entry is malformed; those
    /// outcomes use one general entry"), so a multi-entry batch can only ever be
    /// attempted when `u_1`'s OWN picked candidate is `Skip` -- otherwise (a
    /// full/core pick, or this being a `force_single` turn per the D7-2 alternation
    /// below) this attempt is `u_1`'s ordinary single-entry attempt, full stop,
    /// exactly what `decide` itself would have produced. When `u_1` picked `Skip`
    /// and this is a "may extend" turn, coordinates 2..k extend rightward, "the
    /// maximal consecutive prefix ... for which every target justifies skip":
    /// resolved views are transparently skipped through (never block or end the run,
    /// mirroring coordinate 1's own scan); a genuinely no-evidence view is ALSO
    /// skipped through ("an older no-evidence view does not prevent an attempt for a
    /// later target") since it justifies nothing at all yet, not even non-skip; but a
    /// view with enough ready-stage evidence to justify something OTHER than skip
    /// (or nothing skip-specific) is treated as "the first other target" and ends the
    /// run there, as does the first in-flight-suppressed view or `scan_limit`/the `f`
    /// cap.
    ///
    /// D7-2 alternation (`alternation_target`/`alternation_force_single`, see their
    /// own doc comments): per fixed `u_1`, every OTHER attempt is truncated to the
    /// single first entry -- applied BEFORE the extension is even attempted (a
    /// `force_single` turn never looks past `u_1`), so it never affects which
    /// candidate `u_1` itself picks, only whether riders may ride along.
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

        // D7-2: per-fixed-first-target alternation between a "may extend" turn and a
        // forced single-entry turn -- resets whenever the observed first-qualifying
        // target changes.
        if self.alternation_target != Some(u1) {
            self.alternation_target = Some(u1);
            self.alternation_force_single = false;
        }
        let force_single = self.alternation_force_single;
        self.alternation_force_single = !self.alternation_force_single;

        // A batch can only ever be headed by a Skip pick -- `formed_batch` rejects a
        // full/core coordinate outright, so there is no valid vector to build around
        // anything else. This attempt is then just u_1's ordinary single-entry
        // attempt, exactly what `decide` itself would have produced.
        if force_single || u1_entry != ResolutionEntry::Skip(u1) {
            log::info!(
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
                continue; // transparently skipped through, mirrors coordinate 1's scan
            }
            if agb.ready_stage_total(u) < self.two_f_plus_1_parties {
                u += 1;
                continue; // no evidence at all yet -- never blocks a later target
            }
            if !self.skip_justified(agb, u) {
                break; // has evidence, but does not justify skip -- "the first other target"
            }
            if self.is_in_flight(u, now) {
                break; // mirrors the pre-704fb29 prefix's own "stop" treatment for in-flight
            }
            out.push(ResolutionEntry::Skip(u));
            // D7-1: this rider is ALSO itself in-flight evidence, immediately --
            // deliberately not touched via `pick_and_advance`/`candidate_pointer`:
            // riding along in an opportunistic batch is independent of `u`'s own
            // per-target candidate cycle (its `justified_candidates` may include a
            // full/core entry the pointer is presently on; the batch always uses
            // `Skip` specifically, regardless of that unrelated cycle's state).
            self.in_flight.insert(u, now);
            u += 1;
        }
        log::info!(
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
