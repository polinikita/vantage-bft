// PHASE4-SPEC.md §§2-8 -- Direct-AGB per-view engine (M = ∅ throughout): wire types
// (§2), `Formed_v`/`proposer(v)` (§3), R2 echo (§5), R3 ready (§6), R4
// completion/direct-seal + the try-seal arbiter (§7), the fast seal + optimistic lock
// (§8). Effect-returning like Phase 3's `LaneManager`/`Repairer` -- no direct
// network/timer I/O, so tests can drive it without a live node (§12).

use crate::primary::View;
use crate::vantage::block::{self, BlockRef};
use crate::vantage::lanes::LaneManager;
use crate::vantage::repair::Repairer;
use crate::vantage::{Effect, Thresholds};
use config::{Committee, Stake};
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// §2: entries in strictly increasing author order; ≤1 entry per author.
pub type Manifest = Vec<BlockRef>;

/// PHASE6-SPEC.md §1: an entry in a proposal's resolution field `M`, targeting an
/// earlier, still-open view `u` (the `View` field in every variant). `Full`/`Core`
/// both carry `(u, C_u, T_u)` -- `Core` "retains T for identity/compat checks" (§1)
/// even though its semantic content is only `C_u`; `Skip` carries no manifests.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ResolutionEntry {
    Full(View, Manifest, Manifest),
    Core(View, Manifest, Manifest),
    Skip(View),
}

impl ResolutionEntry {
    pub fn target_view(&self) -> View {
        match self {
            ResolutionEntry::Full(u, _, _)
            | ResolutionEntry::Core(u, _, _)
            | ResolutionEntry::Skip(u) => *u,
        }
    }
}

/// §2 `ViewProposal { view, c, t, m }` (PHASE6-SPEC.md §1 adds `m`; M structurally
/// absent -- always `None` -- through Phase 5).
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ViewProposal {
    pub view: View,
    pub c: Manifest,
    pub t: Manifest,
    pub m: Option<ResolutionEntry>,
}

impl ViewProposal {
    /// §2: `proposal_digest = blake3("view-proposal" || sid || bincode(ViewProposal))`.
    pub fn digest(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("ViewProposal always serializes");
        block::domain_hash(b"view-proposal", sid, &bytes)
    }
}

/// §2 `Echo { proposal, grade, sender }` (the origin annotation `o` is empty for M = ∅,
/// so not carried).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Echo {
    pub proposal: ViewProposal,
    /// 0 or 1 (`debug_assert`ed by callers that construct one; not itself a typed
    /// bool to match the spec's own "0|1" phrasing and keep the wire shape a plain
    /// byte).
    pub grade: u8,
    pub sender: PublicKey,
    /// PHASE5-SPEC.md §2/W4: the sender's own-wish watermark, piggybacked outside the
    /// message's immutable identity (`proposal_digest`-based counting never reads this
    /// field). D5-3: `AgbEngine` constructs this as `0` (a placeholder -- the engine is
    /// deliberately watermark-free); `VantageCore` overwrites it with
    /// `Pacemaker::own_watermark()` at serialization time, immediately before sending.
    pub wish: View,
    /// PHASE6-SPEC.md §3 (`Ann`): 0/1, `None` for skip entries or empty `M`. Set once
    /// at emission from the sender's OWN E_i(u) (its own already-emitted echo-stage
    /// statement for M's target view `u`), immutable, and -- like `wish` -- OUTSIDE
    /// counting identity: two counted echoes for the same `(view, digest)` may carry
    /// different `origin` bits, and both are individually tallied by R3's `ReadyOK`.
    pub origin: Option<u8>,
}

/// §6 `Ready`'s grade: `One` if a quorum of the counted echoes at emission were
/// grade-1, `Zero` if a quorum were grade-0, else `Mix`.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ReadyGrade {
    Zero,
    One,
    Mix,
}

/// §2 `Ready { proposal, grade, sender }`.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Ready {
    pub proposal: ViewProposal,
    pub grade: ReadyGrade,
    pub sender: PublicKey,
    /// See `Echo::wish`'s doc comment -- same piggyback convention (W4/D5-3).
    pub wish: View,
}

/// §7/§9's terminal per-view result: `gfull(C,T)`, `gcore(C)`, or `gskip` (the last is
/// implemented per the module plan but never produced by Direct-AGB -- unreachable in
/// Phase 4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Full(Manifest, Manifest),
    Core(Manifest),
    Skip,
}

/// §10: which deadline an `Effect::ArmTimer` names. `PartialOrd`/`Ord` (D7-4,
/// PHASE7-PREP-NOTES.md: the timer-queue min-heap fix) carry no protocol meaning --
/// only needed so `(Instant, View, TimerKind)` tuples are orderable for the heap; ties
/// on `Instant` are broken arbitrarily by variant declaration order, which is fine
/// since firing order among same-deadline entries was never specified either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimerKind {
    /// R2's `min(t + Δ, e_i + θE)` fallback deadline (armed once `ρ_i` is known).
    EchoFallback,
    /// R2's absolute `e_i + θE` deadline.
    EchoAbsolute,
    /// R3's absolute `e_i + θR` deadline.
    ReadyAbsolute,
}

/// PHASE5-SPEC.md W3: which response stage is about to be emitted, at a
/// `two_response_wish_target` call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseStage {
    Echo,
    Ready,
}

/// §3 `Formed_v(C, T)`, extended by PHASE6-SPEC.md §1 for `M`: each of C and T has ≤1
/// entry per author and is sorted strictly increasing by author; every hash across
/// C ∪ T is distinct; every entry has height ≥ 1 and an author with stake in the
/// committee. `M` (`view`'s own resolution field): empty, or exactly one entry
/// targeting `u` with `1 <= u <= view - 3`, whose own manifests (if any) satisfy the
/// same syntactic bounds -- checked only against each other (the entry's own C_u ∪ T_u
/// distinctness), never against the carrying C ∪ T (§1: "the paper bounds only the
/// entry's own manifests syntactically; cross-checks are semantic, not Formed").
pub fn formed(
    committee: &Committee,
    view: View,
    c: &Manifest,
    t: &Manifest,
    m: &Option<ResolutionEntry>,
) -> bool {
    fn strictly_sorted_and_staked(committee: &Committee, m: &Manifest) -> bool {
        let mut last: Option<PublicKey> = None;
        for (author, height, _digest) in m {
            if *height < 1 {
                return false;
            }
            if committee.stake(author) == 0 {
                return false;
            }
            if let Some(prev) = last {
                if *author <= prev {
                    return false; // strictly increasing author order (also rejects
                                  // duplicate authors within the same manifest)
                }
            }
            last = Some(*author);
        }
        true
    }
    fn distinct_hashes(m1: &Manifest, m2: &Manifest) -> bool {
        let mut hashes = std::collections::HashSet::new();
        for (_, _, h) in m1.iter().chain(m2.iter()) {
            if !hashes.insert(h.clone()) {
                return false;
            }
        }
        true
    }
    if !strictly_sorted_and_staked(committee, c) || !strictly_sorted_and_staked(committee, t) {
        return false;
    }
    if !distinct_hashes(c, t) {
        return false; // duplicate hash across C ∪ T
    }
    if let Some(entry) = m {
        let u = entry.target_view();
        if u < 1 || u > view.saturating_sub(3) {
            return false;
        }
        match entry {
            ResolutionEntry::Full(_, c_u, t_u) | ResolutionEntry::Core(_, c_u, t_u) => {
                if !strictly_sorted_and_staked(committee, c_u)
                    || !strictly_sorted_and_staked(committee, t_u)
                {
                    return false;
                }
                if !distinct_hashes(c_u, t_u) {
                    return false;
                }
            }
            ResolutionEntry::Skip(_) => {}
        }
    }
    true
}

/// PHASE6-SPEC.md §1 `AuxRefs(M)`: the non-skip entry's manifests (empty for `None`/
/// `Skip`) -- authorized alongside the carrying proposal's own C/T, both on fixing (§5
/// `on_propose`) and on completion (§7 `recheck_completion_and_direct`).
fn aux_refs(m: &Option<ResolutionEntry>) -> Vec<BlockRef> {
    match m {
        Some(ResolutionEntry::Full(_, c, t)) | Some(ResolutionEntry::Core(_, c, t)) => {
            c.iter().chain(t.iter()).cloned().collect()
        }
        _ => Vec::new(),
    }
}

/// §3 D4-2: `proposer(v)` = round-robin over the committee's authorities in their
/// canonical sorted order (`Committee::authorities` is a `BTreeMap`, so iteration order
/// already is that canonical order) -- index `(v - 1) mod n`.
pub fn proposer(committee: &Committee, view: View) -> PublicKey {
    debug_assert!(view >= 1, "proposer(v) is only defined for v >= 1");
    let names: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
    let n = names.len() as u64;
    names[((view.saturating_sub(1)) % n) as usize]
}

#[derive(Clone, Debug)]
enum Fixed {
    Unset,
    Reject,
    /// The `ViewProposal` is `Arc`-wrapped purely as an internal ownership
    /// optimization (Efficiency Item 3): every clone below is a refcount bump, never
    /// a deep copy of `c`/`t`/`m`. Content, digest, and every comparison/query over it
    /// are unchanged; the wrapper never crosses into a wire type (`Echo`/`Ready`
    /// still carry an owned `ViewProposal`, materialized via `(*arc).clone()` at the
    /// point an effect is actually built).
    Proposal(Arc<ViewProposal>, Digest),
}

#[derive(Clone, Debug)]
enum EchoStatement {
    /// A counted proposal echo: the proposal (`Arc`-wrapped, see `Fixed::Proposal`),
    /// its digest, its grade (0 or 1), and its origin bit (PHASE6-SPEC.md §3 `Ann`;
    /// `None` for skip entries/empty M).
    Graded(Arc<ViewProposal>, Digest, u8, Option<u8>),
    Skip,
}

#[derive(Clone, Debug)]
enum ReadyStatement {
    /// A counted proposal ready (`Arc`-wrapped, see `Fixed::Proposal`).
    Graded(Arc<ViewProposal>, Digest, ReadyGrade),
    /// PHASE6-SPEC.md D6-5: a counted no-ready -- Phase 4/5 recorded only that the
    /// one-shot ready-stage slot was used, never the content; §4's justification needs
    /// the content (a first-hand noready census per view), so it is stored now.
    NoReady,
}

/// §8's fast-seal lock, `L_i(v, B)`.
#[derive(Clone, Debug)]
struct Lock {
    proposal: ViewProposal,
    digest: Digest,
    /// "A lock may be born inactive; once inactive it never reactivates."
    active: bool,
}

#[derive(Debug)]
struct ViewState {
    fixed: Fixed,
    echo_sent: bool,
    ready_sent: bool,
    completed: Option<(Manifest, Manifest)>,
    directed: Option<Outcome>,
    sealed: Option<Outcome>,
    fastsealed: bool,
    active: bool,
    entered: bool,
    entry_instant: Option<Instant>,
    first_proposal_instant: Option<Instant>,
    echo_statements: HashMap<PublicKey, EchoStatement>,
    ready_statements: HashMap<PublicKey, ReadyStatement>,
    lock: Option<Lock>,
    /// Efficiency Item 1: memoizes `ViewProposal::digest` per distinct payload
    /// actually observed for this view (content-keyed, via `ViewProposal`'s derived
    /// `Eq` -- NOT the digest itself, which would be circular). Echo/ready messages
    /// from different senders routinely carry byte-identical `ViewProposal`s for the
    /// same view, but each arrives as its own freshly deserialized value, so a
    /// per-object cache (e.g. a `OnceCell` field on `ViewProposal`) cannot dedup
    /// across them -- only a per-view, content-keyed cache can. In practice this
    /// holds at most a handful of entries (quorum-intersection bounds the number of
    /// distinct payloads that can ever be justified for one view); worst case under
    /// Byzantine senders it is bounded by `n`, same order as `echo_statements`
    /// itself.
    digest_cache: Vec<(Arc<ViewProposal>, Digest)>,
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            fixed: Fixed::Unset,
            echo_sent: false,
            ready_sent: false,
            completed: None,
            directed: None,
            sealed: None,
            fastsealed: false,
            active: false,
            entered: false,
            entry_instant: None,
            first_proposal_instant: None,
            echo_statements: HashMap::new(),
            ready_statements: HashMap::new(),
            lock: None,
            digest_cache: Vec::new(),
        }
    }
}

/// The Direct-AGB per-view engine (PHASE4-SPEC.md §§5-8). One instance per node,
/// internally keyed by `View`. Every public method returns the `Effect`s the caller
/// (`vantage::node::VantageCore`, or a test) must execute; this struct never touches
/// the network, a store, or a real clock itself (callers supply `now: Instant`).
pub struct AgbEngine {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    delta: Duration,
    n: usize,
    /// D4-3: fast-seal thresholds count *parties*, not stake.
    f_plus_1_parties: usize,
    quorum: Stake,
    views: HashMap<View, ViewState>,
    /// Efficiency Item 2: exactly the views `recheck_all` would find by scanning
    /// `views` for `active && !echo_sent && matches!(fixed, Fixed::Proposal(..))`.
    /// Maintained incrementally at the only three sites that can change this
    /// membership (`activate`, `on_propose`, and every `echo_sent = true` site) --
    /// see those call sites for the exact insert/remove reasoning.
    pending_gate: std::collections::HashSet<View>,
    /// PHASE6-SPEC.md §9 gate amendment: per-view seal-route counters.
    /// `None` in most unit tests, which don't assert on metrics.
    metrics: Option<Arc<Metrics>>,
}

impl AgbEngine {
    pub fn new(name: PublicKey, committee: Committee, sid: Digest, delta_ms: u64) -> Self {
        let n = committee.size();
        let f_plus_1_parties = Thresholds::from_party_count(n).f_plus_1_parties;
        let quorum = committee.quorum_threshold();
        Self {
            name,
            committee,
            sid,
            delta: Duration::from_millis(delta_ms),
            n,
            f_plus_1_parties,
            quorum,
            views: HashMap::new(),
            pending_gate: std::collections::HashSet::new(),
            metrics: None,
        }
    }

    /// Attach §6.4-style counters (production wiring only -- most unit tests skip
    /// this).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// theta_E: absolute echo fallback deadline. Paper (signature-free.tex, timeout
    /// display, commit b362084): theta_E = 3*Delta.
    pub fn theta_echo(&self) -> Duration {
        self.delta * 3
    }

    /// theta_R: absolute ready deadline. Paper (b362084): theta_R = 4*Delta. (The
    /// control-round timer in control.rs is a separate constant with its own paper
    /// requirement -- see `ControlLog::control_round_timeout` -- not derived from
    /// theta_R.)
    pub fn theta_ready(&self) -> Duration {
        self.delta * 4
    }

    pub fn proposer(&self, view: View) -> PublicKey {
        proposer(&self.committee, view)
    }

    /// PHASE5-SPEC.md W3's two-response wish trigger: consulted at every response-
    /// emission site, immediately before pushing that response's broadcast effect. A
    /// pure query over already-recorded `echo_sent`/`ready_sent` one-shot flags -- it
    /// never itself touches `Pacemaker` (D5-3's module separation; the caller,
    /// `VantageCore`, turns a `Some` result into `Pacemaker::raise_own_wish` via
    /// `Effect::RaiseWish`, pushed immediately before the response effect so it is
    /// always processed first).
    ///
    /// W1: responses for views <= 0 are fixed genesis responses, treated as already
    /// sent by every party -- the only place this boundary is ever consulted is the
    /// `Echo` stage's `u - 1` reference when `u = 1` (`Ready`'s `u + 1` reference is
    /// always >= 2, since ready-stage responses only exist for real views >= 1).
    fn two_response_wish_target(&self, view: View, stage: ResponseStage) -> Option<View> {
        match stage {
            ResponseStage::Echo => {
                let prev = view.saturating_sub(1);
                let prev_ready_sent =
                    prev == 0 || self.views.get(&prev).is_some_and(|s| s.ready_sent);
                prev_ready_sent.then(|| view + 2)
            }
            ResponseStage::Ready => {
                let next = view + 1;
                self.views
                    .get(&next)
                    .is_some_and(|s| s.echo_sent)
                    .then(|| view + 3)
            }
        }
    }

    /// `two_response_wish_target`, wrapped as an `Effect::RaiseWish` ready to be pushed
    /// immediately before the corresponding response broadcast effect (or an empty
    /// iterator, so callers can `effects.extend(...)` unconditionally).
    fn wish_effect(&self, view: View, stage: ResponseStage) -> Option<Effect> {
        self.two_response_wish_target(view, stage)
            .map(Effect::RaiseWish)
    }

    fn state_mut(&mut self, view: View) -> &mut ViewState {
        self.views.entry(view).or_default()
    }

    /// Efficiency Item 1: `ViewProposal::digest` is a pure function of the
    /// proposal's content plus `self.sid`. Rather than recomputing it (full bincode
    /// serialize + blake3) on every `on_echo`/`on_ready` -- up to n-1 times per view
    /// for byte-identical content arriving in separate messages -- memoize it in
    /// `view`'s `digest_cache`, keyed by structural equality (`ViewProposal`'s
    /// derived `Eq`) rather than by the digest itself (which would be circular).
    /// Returns the SAME `Digest` value `proposal.digest(&self.sid)` would have
    /// returned -- only the second and later calls for an equal payload skip the
    /// hash. Also returns an `Arc` around the (possibly newly cached) proposal so
    /// callers can store it in `Fixed`/`EchoStatement`/`ReadyStatement` (Efficiency
    /// Item 3) as a refcount bump instead of a deep clone, and so repeated identical
    /// content shares one allocation.
    fn canonical_proposal(
        &mut self,
        view: View,
        proposal: ViewProposal,
    ) -> (Arc<ViewProposal>, Digest) {
        if let Some(state) = self.views.get(&view) {
            if let Some((cached, digest)) = state.digest_cache.iter().find(|(p, _)| **p == proposal)
            {
                return (Arc::clone(cached), digest.clone());
            }
        }
        let digest = proposal.digest(&self.sid);
        let arc = Arc::new(proposal);
        self.state_mut(view)
            .digest_cache
            .push((Arc::clone(&arc), digest.clone()));
        (arc, digest)
    }

    // ---------------------------------------------------------- PHASE6-SPEC.md §4
    // query accessors over the existing per-view censuses (reuse rule: `resolve.rs`'s
    // justification computation reads these; no parallel counting state anywhere).

    /// Whether `view` has entered AT ALL (a target with genuinely no state yet is a
    /// "no-evidence view" that never blocks a later target, per §4's scanning rule).
    pub fn has_any_state(&self, view: View) -> bool {
        self.views.contains_key(&view)
    }

    /// Counted echo statements for `view` matching `pred` (0 if `view` has no state
    /// yet). Shared shape behind every `echo_*_count*` query below -- these query
    /// accessors differ only in `pred`, never in how the per-view census is read.
    fn echo_count(&self, view: View, pred: impl Fn(&EchoStatement) -> bool) -> usize {
        self.views.get(&view).map_or(0, |s| {
            s.echo_statements.values().filter(|stmt| pred(stmt)).count()
        })
    }

    /// Counted ready-stage statements for `view` matching `pred` (0 if `view` has no
    /// state yet). Shared shape behind every `ready_stage_*`/`noready_count` query
    /// below.
    fn ready_count(&self, view: View, pred: impl Fn(&ReadyStatement) -> bool) -> usize {
        self.views.get(&view).map_or(0, |s| {
            s.ready_statements
                .values()
                .filter(|stmt| pred(stmt))
                .count()
        })
    }

    /// Total counted ready-stage statements for `view` (any kind, noready included) --
    /// §4's prerequisite for ANY candidate: `>= 2f+1` (party count) of these.
    pub fn ready_stage_total(&self, view: View) -> usize {
        self.views
            .get(&view)
            .map_or(0, |s| s.ready_statements.len())
    }

    /// Counted ready-stage statements for `view` that are NOT grade-1 proposal-readies
    /// (noready + grade-0/mix readies) -- §4's `Core` justification second clause.
    pub fn ready_stage_non_grade1_count(&self, view: View) -> usize {
        self.ready_count(view, |stmt| {
            !matches!(stmt, ReadyStatement::Graded(_, _, ReadyGrade::One))
        })
    }

    /// Counted noready statements for `view` -- §4's `Skip` justification (`>= 2f+1`).
    pub fn noready_count(&self, view: View) -> usize {
        self.ready_count(view, |stmt| matches!(stmt, ReadyStatement::NoReady))
    }

    /// Counted grade-1 echoes for `view` naming exactly payload `(c,t)` -- §4's `Full`
    /// justification (`>= f+1`).
    pub fn echo_grade1_count_for(&self, view: View, c: &Manifest, t: &Manifest) -> usize {
        self.echo_count(
            view,
            |stmt| matches!(stmt, EchoStatement::Graded(p, _, 1, _) if p.c == *c && p.t == *t),
        )
    }

    /// Counted echoes (any grade) for `view` naming exactly payload `(c,t)` -- §4's
    /// `Core` justification (`>= f+1`).
    pub fn echo_any_grade_count_for(&self, view: View, c: &Manifest, t: &Manifest) -> usize {
        self.echo_count(
            view,
            |stmt| matches!(stmt, EchoStatement::Graded(p, _, _, _) if p.c == *c && p.t == *t),
        )
    }

    /// Every distinct payload named by a counted (graded) echo for `view` -- the
    /// candidate-payload enumeration §4's justification tests `Full`/`Core` against
    /// (at most 2 can ever be justified, by quorum-intersection, but this simply
    /// returns whatever the first-hand census currently contains).
    pub fn candidate_payloads(&self, view: View) -> Vec<(Manifest, Manifest)> {
        let Some(state) = self.views.get(&view) else {
            return Vec::new();
        };
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for stmt in state.echo_statements.values() {
            if let EchoStatement::Graded(p, _, _, _) = stmt {
                let key = (p.c.clone(), p.t.clone());
                if seen.insert(key.clone()) {
                    out.push(key);
                }
            }
        }
        out
    }

    /// PHASE6-SPEC.md §4: whether `view` is already sealed at the AGB layer (the
    /// try-seal arbiter's terminal result) -- part of "unsealed, un-anchor-resolved"
    /// (the caller, `resolve.rs`, also folds in the anchor-resolved predicate once §6
    /// lands).
    pub fn is_sealed(&self, view: View) -> bool {
        self.views.get(&view).is_some_and(|s| s.sealed.is_some())
    }

    /// D7-4 (PHASE7-PREP-NOTES.md): read-only mirror of the exact guard
    /// `on_echo_fallback_timer`/`on_echo_absolute_timer` already check internally --
    /// used by the timer-queue's lazy stale-discard at pop time, so a superseded timer
    /// (its echo already sent organically) is dropped without ever constructing/
    /// dispatching the handler call, instead of dispatching into a guard that would
    /// have returned an empty `Vec` anyway. Same value, same meaning, no `&mut self`.
    pub fn echo_sent(&self, view: View) -> bool {
        self.views.get(&view).is_some_and(|s| s.echo_sent)
    }

    /// D7-4: read-only mirror of `on_ready_timer`'s guard, same reasoning as
    /// `echo_sent` above.
    pub fn ready_sent(&self, view: View) -> bool {
        self.views.get(&view).is_some_and(|s| s.ready_sent)
    }

    /// PHASE6-SPEC.md §6: submit an anchor-derived outcome `X_u` to the SAME try-seal
    /// arbiter direct/fastseal submissions use (reuse rule) -- first submission for
    /// `view` wins and emits `Effect::Sealed`; a later, compatible submission is a
    /// no-op (`debug_assert`ed compatible by `outcomes_compatible`, same as ever).
    pub fn submit_anchor(&mut self, view: View, outcome: Outcome) -> Vec<Effect> {
        let mut effects = Vec::new();
        let route = match &outcome {
            Outcome::Full(..) => "anchor_full",
            Outcome::Core(..) => "anchor_core",
            Outcome::Skip => "anchor_skip",
        };
        self.try_seal(view, outcome, route, &mut effects);
        effects
    }

    #[cfg(test)]
    pub(crate) fn completed_for_test(&self, view: View) -> Option<(Manifest, Manifest)> {
        self.views.get(&view).and_then(|s| s.completed.clone())
    }

    #[cfg(test)]
    pub(crate) fn sealed_for_test(&self, view: View) -> Option<Outcome> {
        self.views.get(&view).and_then(|s| s.sealed.clone())
    }

    #[cfg(test)]
    pub(crate) fn lock_active_for_test(&self, view: View) -> Option<bool> {
        self.views
            .get(&view)
            .and_then(|s| s.lock.as_ref())
            .map(|l| l.active)
    }

    #[cfg(test)]
    pub(crate) fn directed_for_test(&self, view: View) -> Option<Outcome> {
        self.views.get(&view).and_then(|s| s.directed.clone())
    }

    // ---------------------------------------------------------------- §4 wrapper API

    /// §4: formal entry into `view` (Phase 4: only ever called for v = 1, at genesis
    /// boot; Phase 5's WISH pacemaker calls this for every view once its formal-entry
    /// target reaches it, W5). Arms the absolute echo/ready fallback deadlines and marks
    /// the view active ("`enter(v)` also activates"); re-checks the positive gate in
    /// case a proposal was already fixed (buffered) before entry. Entry is strictly
    /// increasing locally (W5): a view already entered never re-enters.
    ///
    /// W5(b) / PHASE4-NOTES.md §12's recorded carry-over: Phase 4 only ever entered a
    /// view before any proposal for it could possibly have arrived, so `on_propose` was
    /// the only site that ever needed to arm `EchoFallback` (at the moment `rho_i(v)`
    /// first becomes known). Phase 5 can enter a view *after* its proposal already
    /// arrived (a view-change/re-entry via WISH) -- if so, and the echo is still
    /// pending, arm `EchoFallback` here too, from the already-known
    /// `first_proposal_instant` (`rho_i(v)`), using the exact same
    /// `min(max(e_i, rho_i) + Delta, e_i + theta_E)` formula `on_propose` uses (here
    /// `e_i(v) = now`, since entry is happening this instant).
    pub fn enter(
        &mut self,
        view: View,
        now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.state_mut(view).entered {
            return effects;
        }
        let theta_echo = self.theta_echo();
        let theta_ready = self.theta_ready();
        {
            let s = self.state_mut(view);
            s.entered = true;
            s.entry_instant = Some(now);
        }
        effects.push(Effect::ArmTimer(
            view,
            TimerKind::EchoAbsolute,
            now + theta_echo,
        ));
        effects.push(Effect::ArmTimer(
            view,
            TimerKind::ReadyAbsolute,
            now + theta_ready,
        ));

        let (fixed_proposal, echo_sent, first_proposal_instant) = {
            let s = self.state_mut(view);
            (
                matches!(s.fixed, Fixed::Proposal(_, _)),
                s.echo_sent,
                s.first_proposal_instant,
            )
        };
        if fixed_proposal && !echo_sent {
            if let Some(rho) = first_proposal_instant {
                let t = std::cmp::max(now, rho);
                let deadline = std::cmp::min(t + self.delta, now + theta_echo);
                effects.push(Effect::ArmTimer(view, TimerKind::EchoFallback, deadline));
            }
        }

        effects.extend(self.activate(view, now, lm, rep));
        effects
    }

    /// §4: `activate(v)` -- called by the caller once `Frontier` determines `v` is
    /// newly active (either via the proposal-chain advance or via `enter(v)`, which
    /// calls this directly). Idempotent.
    pub fn activate(
        &mut self,
        view: View,
        now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        if self.state_mut(view).active {
            return Vec::new();
        }
        self.state_mut(view).active = true;
        // Efficiency Item 2 transition (a): `active` just became true. If a
        // proposal is already fixed and the echo hasn't been sent, this view now
        // matches `recheck_all`'s scan predicate -- record it. (If `recheck_gate`
        // below immediately sends the echo, the `echo_sent` transition removes it
        // again before this function returns, same net effect as before.)
        let s = self.state_mut(view);
        if matches!(s.fixed, Fixed::Proposal(..)) && !s.echo_sent {
            self.pending_gate.insert(view);
        }
        self.recheck_gate(view, now, lm, rep)
    }

    // ------------------------------------------------------------------------- R1/R2

    /// §5's first bullet: the first direct `VantagePropose` from `proposer(v)` (sender
    /// is checked against `self.proposer(view)` -- D4's declared-sender trust). Sticky:
    /// only the first-ever direct proposal (well-formed or not) sets `fixed`; later
    /// ones are ignored. Authorizes every reference named by C and T on acceptance, and
    /// reports the well-formedness outcome via `Effect::Fixed` for `Frontier` (§4).
    pub fn on_propose(
        &mut self,
        sender: PublicKey,
        proposal: ViewProposal,
        now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let view = proposal.view;
        let mut effects = Vec::new();
        if sender != self.proposer(view) {
            return effects; // only a *direct* proposal from proposer(v) can ever fix
        }
        let theta_echo = self.theta_echo();
        if let Some(entry) = self.state_mut(view).entry_instant {
            if now > entry + theta_echo {
                return effects; // "a proposal delivered after that deadline is ignored"
            }
        }
        if !matches!(self.state_mut(view).fixed, Fixed::Unset) {
            return effects; // sticky: first direct proposal ever seen already resolved this
        }
        self.state_mut(view)
            .first_proposal_instant
            .get_or_insert(now);

        if !formed(&self.committee, view, &proposal.c, &proposal.t, &proposal.m) {
            self.state_mut(view).fixed = Fixed::Reject;
            effects.push(Effect::Fixed(view, false));
            return effects;
        }

        let (proposal, digest) = self.canonical_proposal(view, proposal);
        self.state_mut(view).fixed = Fixed::Proposal(Arc::clone(&proposal), digest.clone());
        // Efficiency Item 2 transition (b): `fixed` just became `Proposal`. If the
        // view is already active and the echo hasn't been sent, it now matches
        // `recheck_all`'s scan predicate -- record it (see `activate`'s matching
        // comment; the direct `recheck_gate` call below may immediately remove it
        // again via the `echo_sent` transition, same net effect as before).
        if self.state_mut(view).active && !self.state_mut(view).echo_sent {
            self.pending_gate.insert(view);
        }
        for r in proposal
            .c
            .iter()
            .chain(proposal.t.iter())
            .chain(aux_refs(&proposal.m).iter())
        {
            effects.extend(rep.authorize(r.clone()));
        }
        effects.push(Effect::Fixed(view, true));

        if let Some(entry) = self.state_mut(view).entry_instant {
            let t = std::cmp::max(entry, now);
            let deadline = std::cmp::min(t + self.delta, entry + theta_echo);
            effects.push(Effect::ArmTimer(view, TimerKind::EchoFallback, deadline));
        }

        effects.extend(self.recheck_gate(view, now, lm, rep));
        effects
    }

    /// R2's positive gate, re-evaluated whenever local state that could satisfy it
    /// changes (ack counts, payload arrivals, block cached, activation) -- call for
    /// every currently pending, active view after any such event.
    pub fn recheck_all(
        &mut self,
        now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        // Efficiency Item 2: `pending_gate` is maintained incrementally (see its
        // field doc and the `activate`/`on_propose`/`echo_sent`-site comments) to
        // always equal exactly what the old full `self.views` scan below would have
        // found:
        //   self.views.iter().filter(|(_, s)| s.active && !s.echo_sent
        //       && matches!(s.fixed, Fixed::Proposal(_, _))).map(|(v, _)| *v)
        // `active`, `fixed`, and `echo_sent` are each one-shot/monotonic (active and
        // fixed only ever become true/set once; echo_sent only ever flips false ->
        // true), so the three transition sites are exhaustive. Iteration order over
        // a `HashSet` is just as unspecified as it was over the `HashMap` here
        // before -- still fine, since each view's `recheck_gate` reads/writes only
        // that view's own `ViewState` and pushes only that view's effects, so the
        // per-view effect outcomes (and hence the concatenated `effects` content,
        // modulo which-view's-chunk-comes-first -- already unordered before) are
        // independent of processing order.
        let views: Vec<View> = self.pending_gate.iter().copied().collect();
        for view in views {
            effects.extend(self.recheck_gate(view, now, lm, rep));
        }
        effects
    }

    fn recheck_gate(
        &mut self,
        view: View,
        _now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        let (active, echo_sent, proposal_digest) = {
            let s = self.state_mut(view);
            // Efficiency Item 3: `p.clone()` on an `Arc<ViewProposal>` is a refcount
            // bump (this used to deep-clone the whole `ViewProposal`, incl. its C/T
            // Vecs, on every call reaching here while a proposal is already fixed --
            // including calls that immediately bail out below because `echo_sent` is
            // already true).
            let pd = match &s.fixed {
                Fixed::Proposal(p, d) => Some((Arc::clone(p), d.clone())),
                _ => None,
            };
            (s.active, s.echo_sent, pd)
        };
        if !active || echo_sent {
            return effects;
        }
        let Some((proposal, digest)) = proposal_digest else {
            return effects;
        };
        if !self.positive_gate_holds(&proposal, lm) {
            return effects;
        }
        // PHASE7-PREP-NOTES.md Delta=1000 investigation: diagnostic-only observational
        // log (no behavior change) -- the organic grade-1 (positive-gate) echo path.
        log::info!("vantage agb: organic grade-1 echo view={}", view);
        // Record the fast-seal lock immediately before sending our own matching echo.
        self.record_lock(view, &proposal, &digest);
        self.state_mut(view).echo_sent = true;
        self.pending_gate.remove(&view); // Efficiency Item 2 transition (c)
        let origin = self.compute_origin(&proposal.m);
        self.count_echo_statement(
            view,
            self.name,
            EchoStatement::Graded(Arc::clone(&proposal), digest, 1, origin),
        );
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        effects.push(Effect::BroadcastEcho(Echo {
            // The wire type still carries an owned `ViewProposal`: exactly one deep
            // clone here (same total deep-clone count as before this file's
            // efficiency changes -- the deep clone above simply moved from the
            // now-Arc'd census entry to this required-owned wire value).
            proposal: (*proposal).clone(),
            grade: 1,
            sender: self.name,
            wish: 0, // D5-3: stamped by `VantageCore` at serialization time
            origin,
        }));
        // D6-4: release evaluation runs BEFORE R3's ready recheck on this same newly
        // counted echo-stage response; the all-n fastseal trigger stays after.
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        effects
    }

    /// R2's positive gate predicate: `CoreOK_i(C) ∧ TipOK_i(C,T) ∧ MetaOK_i(w,M)`
    /// (PHASE6-SPEC.md §2 adds the `MetaOK` conjunct to what Phase 4 called
    /// `positive_gate_holds`).
    fn positive_gate_holds(&self, proposal: &ViewProposal, lm: &mut LaneManager) -> bool {
        if !Self::core_ok(&proposal.c, lm) {
            return false;
        }
        if !proposal.t.iter().all(|r| lm.author_ok(r)) {
            return false;
        }
        if !Self::tip_ok(&proposal.c, &proposal.t, lm) {
            return false;
        }
        self.meta_ok(&proposal.m, lm)
    }

    /// `CoreOK_i(C)`: every C entry is `author_ok`.
    fn core_ok(c: &Manifest, lm: &LaneManager) -> bool {
        c.iter().all(|r| lm.author_ok(r))
    }

    /// The tip-anchoring pairing walk (part of `TipOK_i(C,T)`): every T entry, if
    /// paired by author with a C entry, must strictly extend it, hold its own prefix,
    /// and have that prefix pass through the paired C entry. Factored out so
    /// PHASE6-SPEC.md §2's `MetaOK` can reuse it against a resolution entry's own
    /// `(C_u, T_u)` instead of the carrying proposal's `(C,T)`.
    fn tip_ok(c: &Manifest, t: &Manifest, lm: &mut LaneManager) -> bool {
        for t_ref in t {
            if let Some(c_ref) = c.iter().find(|c_ref| c_ref.0 == t_ref.0) {
                if t_ref.1 <= c_ref.1 {
                    return false; // equal-height (or shorter) tip excluded
                }
                if !lm.holds_prefix(t_ref) {
                    return false; // counted acks never substitute for a paired tip
                }
                if !lm.prefix_contains(t_ref, c_ref) {
                    return false;
                }
            }
        }
        true
    }

    /// PHASE6-SPEC.md §2: `MetaOK_i(w, M)`, evaluated at echo decision time (both the
    /// positive gate and the Δ-fallback echo, per the wiring described there). `∅ →
    /// true`. For one entry targeting `u`: see the three-bullet checklist in the spec
    /// (own target responses already emitted; the fast-seal lock rule; the
    /// outcome-specific payload/availability/tip-anchoring checks). Persistent: while
    /// our own `E_i(u)`/`R_i(u)` are still pending this returns `false` for the current
    /// attempt, and the caller (the positive gate, retried via `recheck_all`, or the
    /// fallback deadline, which simply falls through to echo-skip) is expected to
    /// re-evaluate on the next state change -- see `dispatch_inbound`'s/`Node::dispatch`'s
    /// blanket `recheck_all` retry after every response arm (a pending view `w`'s
    /// `MetaOK` depends on THIS party's own echo/ready for a *different*, earlier view
    /// `u`, which the existing Ack/BlockCached-triggered `recheck_all` call sites never
    /// covered).
    fn meta_ok(&self, m: &Option<ResolutionEntry>, lm: &mut LaneManager) -> bool {
        let Some(entry) = m else {
            return true;
        };
        let u = entry.target_view();
        let Some(state_u) = self.views.get(&u) else {
            return false; // no state at all for u yet -- E_i(u)/R_i(u) certainly pending
        };
        // 1. both own target responses already emitted.
        let Some(own_echo) = state_u.echo_statements.get(&self.name) else {
            return false;
        };
        let Some(own_ready) = state_u.ready_statements.get(&self.name) else {
            return false;
        };
        // 2. lock rule: an active fastLock_u only lets the exact matching Full entry
        // through.
        if let Some(lock) = &state_u.lock {
            if lock.active {
                match entry {
                    ResolutionEntry::Full(_, c, t)
                        if lock.proposal.c == *c && lock.proposal.t == *t => {}
                    _ => return false,
                }
            }
        }
        // 3. outcome-specific.
        match entry {
            ResolutionEntry::Full(_, c_u, t_u) => {
                // The spec's bullet 3 constrains own R_i(u), not own E_i(u), for
                // Full/Core -- bullet 1 already required E_i(u) to merely EXIST.
                let _ = own_echo;
                match own_ready {
                    ReadyStatement::Graded(p, _, grade) => {
                        if *grade == ReadyGrade::Zero {
                            return false; // grade-0 proposal-ready
                        }
                        if p.c != *c_u || p.t != *t_u {
                            return false; // proposal-ready naming a payload != (C_u,T_u)
                        }
                    }
                    ReadyStatement::NoReady => {}
                }
                if !c_u.iter().all(|r| lm.locally_available(r)) {
                    return false;
                }
                if !t_u.iter().all(|r| lm.locally_available(r)) {
                    return false;
                }
                Self::tip_ok(c_u, t_u, lm)
            }
            ResolutionEntry::Core(_, c_u, t_u) => {
                match own_ready {
                    ReadyStatement::Graded(p, _, grade) => {
                        if *grade == ReadyGrade::One {
                            return false; // grade-1 proposal-ready
                        }
                        if p.c != *c_u || p.t != *t_u {
                            return false; // proposal-ready for a different payload
                        }
                    }
                    ReadyStatement::NoReady => {}
                }
                let _ = own_echo;
                c_u.iter().all(|r| lm.locally_available(r))
            }
            ResolutionEntry::Skip(_) => {
                let _ = own_echo;
                matches!(own_ready, ReadyStatement::NoReady)
            }
        }
    }

    /// PHASE6-SPEC.md §3 `Ann`: this party's own origin bit for a carrying proposal's
    /// `M` entry, computed from its own already-emitted `E_i(u)` at emission time.
    fn compute_origin(&self, m: &Option<ResolutionEntry>) -> Option<u8> {
        let entry = m.as_ref()?;
        let u = entry.target_view();
        let own_echo = self
            .views
            .get(&u)
            .and_then(|s| s.echo_statements.get(&self.name));
        let is_one = match entry {
            ResolutionEntry::Full(_, c, t) => {
                matches!(own_echo, Some(EchoStatement::Graded(p, _, 1, _)) if p.c == *c && p.t == *t)
            }
            ResolutionEntry::Core(_, c, t) => {
                matches!(own_echo, Some(EchoStatement::Graded(p, _, _, _)) if p.c == *c && p.t == *t)
            }
            ResolutionEntry::Skip(_) => return None,
        };
        Some(if is_one { 1 } else { 0 })
    }

    /// R2 fallback's `min(t + Δ, e_i + θE)` deadline: if echo is still pending and
    /// `fixed = B`, broadcast a grade-0 echo (if `CoreOK_i(C) ∧ MetaOK_i(w,M)` holds --
    /// PHASE6-SPEC.md §2: "Phase 4's fallback checked CoreOK only -- correct for M=∅,
    /// must change now") or an echo-skip.
    pub fn on_echo_fallback_timer(
        &mut self,
        view: View,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        let (echo_sent, fixed) = {
            let s = self.state_mut(view);
            (s.echo_sent, s.fixed.clone())
        };
        if echo_sent {
            return effects;
        }
        let Fixed::Proposal(proposal, digest) = fixed else {
            return effects; // fixed is still ⊥ or Reject -- defer to the absolute deadline
        };
        self.state_mut(view).echo_sent = true;
        self.pending_gate.remove(&view); // Efficiency Item 2 transition (c)
                                         // PHASE7-PREP-NOTES.md Delta=1000 investigation: diagnostic-only observational
                                         // log (no behavior change) -- the Delta-scaled fallback (grade-0) echo path.
        log::info!("vantage agb: FALLBACK grade-0 echo view={}", view);
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        if Self::core_ok(&proposal.c, lm) && self.meta_ok(&proposal.m, lm) {
            let origin = self.compute_origin(&proposal.m);
            self.count_echo_statement(
                view,
                self.name,
                EchoStatement::Graded(Arc::clone(&proposal), digest, 0, origin),
            );
            effects.push(Effect::BroadcastEcho(Echo {
                // See `recheck_gate`'s matching comment: one deep clone here, same
                // total count as before Efficiency Item 3.
                proposal: (*proposal).clone(),
                grade: 0,
                sender: self.name,
                wish: 0, // D5-3: stamped by `VantageCore` at serialization time
                origin,
            }));
        } else {
            self.count_echo_statement(view, self.name, EchoStatement::Skip);
            effects.push(Effect::BroadcastEchoSkip(view));
        }
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        effects
    }

    /// R2's absolute `e_i + θE` deadline: if echo is still pending (either no active
    /// well-formed fixed proposal, or `MetaOK` never became true in time), broadcast an
    /// echo-skip.
    pub fn on_echo_absolute_timer(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.state_mut(view).echo_sent {
            return effects;
        }
        self.state_mut(view).echo_sent = true;
        self.pending_gate.remove(&view); // Efficiency Item 2 transition (c)
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        self.count_echo_statement(view, self.name, EchoStatement::Skip);
        effects.push(Effect::BroadcastEchoSkip(view));
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        effects
    }

    /// A counted `VantageEcho`. N9 hygiene: `grade` must be exactly 0 or 1 (§2's
    /// "0|1") -- a malformed grade byte is dropped outright, never counted (folding it
    /// into the grade-0 tally would silently treat a malformed message as a legal
    /// one). The `origin` bit travels verbatim (it's the SENDER's own annotation, never
    /// recomputed here).
    pub fn on_echo(&mut self, echo: Echo, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if echo.grade > 1 {
            return effects;
        }
        let view = echo.proposal.view;
        let sender = echo.sender;
        let grade = echo.grade;
        let origin = echo.origin;
        // Efficiency Item 1: reuse the per-view digest cache instead of always
        // recomputing `echo.proposal.digest(&self.sid)`.
        let (proposal, digest) = self.canonical_proposal(view, echo.proposal);
        if !self.count_echo_statement(
            view,
            sender,
            EchoStatement::Graded(proposal, digest, grade, origin),
        ) {
            return effects;
        }
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        effects
    }

    /// A counted `VantageEchoSkip`.
    pub fn on_echo_skip(&mut self, view: View, sender: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        if !self.count_echo_statement(view, sender, EchoStatement::Skip) {
            return effects;
        }
        // Names no B, so it never contributes to R3's per-B echo tally -- only to the
        // fast-seal non-matching count.
        self.recheck_lock_release(view);
        effects.extend(self.recheck_fastseal_trigger(view));
        effects
    }

    /// First-hand echo-stage dedup: at most one counted statement per (view, sender),
    /// ever -- the first one received wins. Returns whether this call was the one that
    /// counted (i.e. this sender had no prior statement for `view`).
    fn count_echo_statement(
        &mut self,
        view: View,
        sender: PublicKey,
        statement: EchoStatement,
    ) -> bool {
        let state = self.state_mut(view);
        if state.echo_statements.contains_key(&sender) {
            return false;
        }
        state.echo_statements.insert(sender, statement);
        true
    }

    fn nonmatching_echo_count(&self, view: View, locked_digest: &Digest) -> usize {
        self.echo_count(view, |stmt| match stmt {
            EchoStatement::Graded(_, d, g, _) => !(*g == 1 && d == locked_digest),
            EchoStatement::Skip => true,
        })
    }

    fn matching_echo_count(&self, view: View, locked_digest: &Digest) -> usize {
        self.echo_count(view, |stmt| matches!(stmt, EchoStatement::Graded(_, d, g, _) if *g == 1 && d == locked_digest))
    }

    /// First-hand ready-stage dedup: at most one counted statement per (view, sender),
    /// ever -- mirrors `count_echo_statement`. Returns whether this call newly counted.
    fn count_ready_statement(
        &mut self,
        view: View,
        sender: PublicKey,
        statement: ReadyStatement,
    ) -> bool {
        let state = self.state_mut(view);
        if state.ready_statements.contains_key(&sender) {
            return false;
        }
        state.ready_statements.insert(sender, statement);
        true
    }

    // --------------------------------------------------------------------------- R3

    /// R3's trigger: on every counted-echo change, if some B has Q = 2f+1 counted
    /// proposal echoes (any grades, identity by proposal_digest) AND PHASE6-SPEC.md
    /// §3's `ReadyOK` holds for it, broadcast a ready for it (grade computed over all
    /// echoes counted at emission). One ready-stage statement per view, ever -- no
    /// entry/fixed-proposal/own-echo guard.
    fn recheck_ready(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.state_mut(view).ready_sent {
            return effects;
        }
        // Efficiency Item 3: the tally's proposal slot is `Arc<ViewProposal>` -- both
        // `or_insert_with` below (once per distinct digest re-derived on every call)
        // and the `.clone()` calls further down are now refcount bumps, not deep
        // clones of `c`/`t`/`m`.
        let mut tallies: HashMap<Digest, (Arc<ViewProposal>, Stake, Stake, usize)> = HashMap::new();
        if let Some(state) = self.views.get(&view) {
            for (sender, stmt) in &state.echo_statements {
                if let EchoStatement::Graded(p, d, g, origin) = stmt {
                    let stake = self.committee.stake(sender);
                    let entry = tallies
                        .entry(d.clone())
                        .or_insert_with(|| (Arc::clone(p), 0, 0, 0));
                    if *g == 1 {
                        entry.1 += stake;
                    } else {
                        entry.2 += stake;
                    }
                    if *origin == Some(1) {
                        entry.3 += 1;
                    }
                }
            }
        }
        for (digest, (proposal, g1, g0, origin_ones)) in tallies {
            if g1 + g0 < self.quorum {
                continue;
            }
            // PHASE6-SPEC.md §3 `ReadyOK`: for a full/core resolution entry, require
            // >= f+1 (party count) counted proposal echoes for THIS proposal with
            // origin = 1; skip/empty always passes.
            let ready_ok = match &proposal.m {
                Some(ResolutionEntry::Full(..)) | Some(ResolutionEntry::Core(..)) => {
                    origin_ones >= self.f_plus_1_parties
                }
                _ => true,
            };
            if !ready_ok {
                continue;
            }
            let grade = if g1 >= self.quorum {
                ReadyGrade::One
            } else if g0 >= self.quorum {
                ReadyGrade::Zero
            } else {
                ReadyGrade::Mix
            };
            let name = self.name;
            self.state_mut(view).ready_sent = true;
            self.count_ready_statement(
                view,
                name,
                ReadyStatement::Graded(Arc::clone(&proposal), digest, grade),
            );
            effects.extend(self.wish_effect(view, ResponseStage::Ready));
            effects.push(Effect::BroadcastReady(Ready {
                // The wire type still carries an owned `ViewProposal`: exactly one
                // deep clone here, same total deep-clone count as before Efficiency
                // Item 3 (previously the census `.clone()` above was the deep clone
                // and this value was moved; now the census clone is free and this is
                // the one remaining deep clone).
                proposal: (*proposal).clone(),
                grade,
                sender: self.name,
                wish: 0, // D5-3: stamped by `VantageCore` at serialization time
            }));
            effects.extend(self.recheck_completion_and_direct(view, rep));
            break; // one ready-stage statement per view, ever
        }
        effects
    }

    /// R3's absolute `e_i + θR` deadline: if we still haven't gone ready by now,
    /// broadcast a no-ready.
    pub fn on_ready_timer(&mut self, view: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.state_mut(view).ready_sent {
            return effects;
        }
        self.state_mut(view).ready_sent = true;
        self.count_ready_statement(view, self.name, ReadyStatement::NoReady);
        effects.extend(self.wish_effect(view, ResponseStage::Ready));
        effects.push(Effect::BroadcastNoReady(view));
        effects
    }

    /// A counted `VantageReady`.
    pub fn on_ready(&mut self, ready: Ready, rep: &mut Repairer) -> Vec<Effect> {
        let view = ready.proposal.view;
        let sender = ready.sender;
        let grade = ready.grade;
        // Efficiency Item 1: reuse the per-view digest cache instead of always
        // recomputing `ready.proposal.digest(&self.sid)`.
        let (proposal, digest) = self.canonical_proposal(view, ready.proposal);
        if !self.count_ready_statement(
            view,
            sender,
            ReadyStatement::Graded(proposal, digest, grade),
        ) {
            return Vec::new();
        }
        self.recheck_completion_and_direct(view, rep)
    }

    /// A counted `VantageNoReady`. PHASE6-SPEC.md D6-5: Phase 4/5 accepted it on the
    /// wire but discarded the content -- now stored one-per-author in the ready-stage
    /// census (§4's justification reads it). Names no B, so it never feeds
    /// completion/direct.
    pub fn on_noready(&mut self, view: View, sender: PublicKey) -> Vec<Effect> {
        self.count_ready_statement(view, sender, ReadyStatement::NoReady);
        Vec::new()
    }

    // --------------------------------------------------------------------------- R4

    /// R4: for each B named by counted proposal-ready statements, (a) completion at
    /// ≥Q readies of any grade (once, ever, hands (C,T) to the cursor as `gopen`), and
    /// (b) the direct result at ≥Q grade-1 (`gfull`) or ≥Q grade-0 (`gcore`) readies,
    /// submitted to the try-seal arbiter. Ready counting continues after completion --
    /// a late homogeneous quorum still produces the direct result.
    fn recheck_completion_and_direct(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        // Efficiency Item 3: see `recheck_ready`'s matching comment -- `Arc::clone`
        // instead of a deep `ViewProposal` clone on every re-scan.
        let mut tallies: HashMap<Digest, (Arc<ViewProposal>, Stake, Stake, Stake)> = HashMap::new();
        if let Some(state) = self.views.get(&view) {
            for (sender, stmt) in &state.ready_statements {
                if let ReadyStatement::Graded(proposal, digest, grade) = stmt {
                    let stake = self.committee.stake(sender);
                    let entry = tallies
                        .entry(digest.clone())
                        .or_insert_with(|| (Arc::clone(proposal), 0, 0, 0));
                    entry.1 += stake;
                    match grade {
                        ReadyGrade::One => entry.2 += stake,
                        ReadyGrade::Zero => entry.3 += stake,
                        ReadyGrade::Mix => {}
                    }
                }
            }
        }
        for (_digest, (proposal, any_stake, g1_stake, g0_stake)) in tallies {
            if any_stake >= self.quorum && self.state_mut(view).completed.is_none() {
                let c = proposal.c.clone();
                let t = proposal.t.clone();
                self.state_mut(view).completed = Some((c.clone(), t.clone()));
                for r in c.iter().chain(aux_refs(&proposal.m).iter()) {
                    effects.extend(rep.authorize(r.clone()));
                }
                // PHASE6-SPEC.md §5: the FIRST genuine R4 completion with M != ∅
                // triggers a completion report (fast-seal alone never does -- fastseal
                // only ever produces `directed`/`sealed`, never `completed`, so this
                // site -- and only this site -- is the right hook). `Effect::
                // CompletionReportable` carries an owned `ViewProposal` (a downstream
                // effect consumer, not internal state), so this one deep clone is
                // required and unchanged from before -- it only ever runs once per
                // view, on the transition into `completed`.
                if proposal.m.is_some() {
                    effects.push(Effect::CompletionReportable(view, (*proposal).clone()));
                }
                effects.push(Effect::Completed(view, c, t));
            }
            if self.state_mut(view).directed.is_none() {
                if g1_stake >= self.quorum {
                    let outcome = Outcome::Full(proposal.c.clone(), proposal.t.clone());
                    self.state_mut(view).directed = Some(outcome.clone());
                    self.try_seal(view, outcome, "direct_full", &mut effects);
                } else if g0_stake >= self.quorum {
                    let outcome = Outcome::Core(proposal.c.clone());
                    self.state_mut(view).directed = Some(outcome.clone());
                    self.try_seal(view, outcome, "direct_core", &mut effects);
                }
            }
        }
        effects
    }

    /// Try-seal arbiter (§7, caller-owned per the paper's framing, implemented here per
    /// the module plan): first submission wins and emits the terminal `Effect::Sealed`;
    /// every later submission for the same view is ignored (`debug_assert`ed
    /// compatible, per the paper's compatibility guarantee). PHASE6-SPEC.md §9 gate
    /// amendment: `route` names which of the (at most 6) ways a view can ever be
    /// sealed produced THIS submission (`fast_full`/`direct_full`/`direct_core`/
    /// `anchor_full`/`anchor_core`/`anchor_skip`) -- passed in by each of the 4 call
    /// sites rather than inferred from `outcome` here, since `Outcome::Full` alone can
    /// arrive via three different routes (fast seal, the direct grade-1 quorum, or an
    /// anchor). Only the FIRST-acceptance submission (the one that actually wins the
    /// arbiter) increments the counter -- a later, merely-compatible submission is not
    /// itself a distinct "route" this view was sealed by.
    fn try_seal(
        &mut self,
        view: View,
        outcome: Outcome,
        route: &'static str,
        effects: &mut Vec<Effect>,
    ) {
        let state = self.state_mut(view);
        match &state.sealed {
            None => {
                state.sealed = Some(outcome.clone());
                effects.push(Effect::Sealed(view, outcome));
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_seals.with_label_values(&[route]).inc();
                }
            }
            Some(existing) => {
                debug_assert!(
                    Self::outcomes_compatible(existing, &outcome),
                    "try-seal arbiter: incompatible outcomes submitted for view {}: {:?} vs {:?}",
                    view,
                    existing,
                    outcome
                );
            }
        }
    }

    fn outcomes_compatible(a: &Outcome, b: &Outcome) -> bool {
        match (a, b) {
            (Outcome::Full(c1, t1), Outcome::Full(c2, t2)) => c1 == c2 && t1 == t2,
            (Outcome::Core(c1), Outcome::Core(c2)) => c1 == c2,
            (Outcome::Full(c1, _), Outcome::Core(c2))
            | (Outcome::Core(c2), Outcome::Full(c1, _)) => c1 == c2,
            (Outcome::Skip, Outcome::Skip) => true,
            _ => false,
        }
    }

    // --------------------------------------------------------------------------- §8

    /// Records `L_i(v, B)` immediately before sending our own matching (grade-1, for
    /// exactly B) echo. Born inactive if ≥ f+1 parties already have non-matching
    /// echo-stage statements counted; otherwise born active. Recorded once per view.
    fn record_lock(&mut self, view: View, proposal: &ViewProposal, digest: &Digest) {
        if self.state_mut(view).lock.is_some() {
            return;
        }
        let nonmatching = self.nonmatching_echo_count(view, digest);
        let active = nonmatching < self.f_plus_1_parties;
        self.state_mut(view).lock = Some(Lock {
            proposal: proposal.clone(),
            digest: digest.clone(),
            active,
        });
    }

    /// PHASE6-SPEC.md D6-4, the release half of what Phase 4 called
    /// `recheck_fastseal`: deactivate the lock (sticky) once ≥ f+1 parties are counted
    /// as non-matching. Split out so every echo-count call site can run this BEFORE
    /// R3's ready recheck on the very same newly counted response -- the paper's
    /// coherence convention: never emit a grade-0/different-payload ready while a
    /// contradictory lock is still active (`MetaOK`'s lock rule reads `lock.active`
    /// too, so this ordering also keeps `MetaOK` itself coherent with the same-instant
    /// echo count).
    fn recheck_lock_release(&mut self, view: View) {
        let Some(lock) = self.views.get(&view).and_then(|s| s.lock.clone()) else {
            return;
        };
        if !lock.active {
            return;
        }
        let nonmatching = self.nonmatching_echo_count(view, &lock.digest);
        if nonmatching >= self.f_plus_1_parties {
            if let Some(l) = self.views.get_mut(&view).and_then(|s| s.lock.as_mut()) {
                l.active = false;
            }
        }
    }

    /// PHASE6-SPEC.md D6-4, the trigger half: once matching responses are counted from
    /// all n parties (and the lock is still active), emit `fastseal(v) -> gfull(C,T)`
    /// (once) via the arbiter. Runs AFTER R3's ready recheck at every call site (only
    /// the release half needed reordering).
    fn recheck_fastseal_trigger(&mut self, view: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        let Some(lock) = self.views.get(&view).and_then(|s| s.lock.clone()) else {
            return effects;
        };
        if !lock.active {
            return effects;
        }
        let already = self.views.get(&view).is_some_and(|s| s.fastsealed);
        if already {
            return effects;
        }
        let matching = self.matching_echo_count(view, &lock.digest);
        if matching == self.n {
            self.state_mut(view).fastsealed = true;
            let outcome = Outcome::Full(lock.proposal.c.clone(), lock.proposal.t.clone());
            self.try_seal(view, outcome, "fast_full", &mut effects);
        }
        effects
    }
}
