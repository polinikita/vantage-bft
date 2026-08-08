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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
///
/// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): the vector
/// (`k >= 2` entries) case of `M` travels on a SEPARATE wire type, `BatchViewProposal`
/// (below), rather than generalizing `m` here to a `Vec` -- an `Option`'s wire
/// encoding differs from a `Vec`'s even when both are "empty"/"one element", so
/// keeping the two logical shapes on two wire types keeps each one's own encoding
/// simple and stable. See `ProposalOut` for the internal (never-itself-serialized)
/// abstraction `AgbEngine`/`control::ControlLog` use to treat both shapes uniformly.
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

/// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): the vector form of `M`, `m.len() in
/// 2..=f` (`f` derived from the committee -- see `formed_batch`), strictly
/// increasing target views. Carried on its own wire messages
/// (`VantageProposeBatch`/`VantageEchoBatch`/.../`ControlServeBatch`), appended
/// last in `PrimaryMessage`, NEVER on `ViewProposal`'s own fields -- see that type's
/// doc comment for why. Domain-separated digest (`"view-proposal-batch"`, distinct
/// from `ViewProposal::digest`'s `"view-proposal"`) purely for hygiene -- the two
/// types' bincode shapes already differ, so an actual collision is not the concern.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BatchViewProposal {
    pub view: View,
    pub c: Manifest,
    pub t: Manifest,
    pub m: Vec<ResolutionEntry>,
}

impl BatchViewProposal {
    pub fn digest(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("BatchViewProposal always serializes");
        block::domain_hash(b"view-proposal-batch", sid, &bytes)
    }
}

/// PHASE7: normalizes the two wire shapes `M` can travel on into one type that
/// `AgbEngine`'s internal per-view state and every M-touching query operate over
/// generically -- itself never serialized (each variant's own payload IS the wire
/// type; this enum is purely an internal/`Effect`-payload abstraction, the same role
/// `Fixed`/`EchoStatement` already play). `entries()` is the uniform view every
/// per-entry check (`meta_ok`, `compute_origin`, `ReadyOK`, anchor application) reads:
/// 0 or 1 entries for `Single`, `2..=f` for `Batch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposalOut {
    Single(ViewProposal),
    Batch(BatchViewProposal),
}

impl ProposalOut {
    pub fn view(&self) -> View {
        match self {
            Self::Single(p) => p.view,
            Self::Batch(p) => p.view,
        }
    }

    pub fn c(&self) -> &Manifest {
        match self {
            Self::Single(p) => &p.c,
            Self::Batch(p) => &p.c,
        }
    }

    pub fn t(&self) -> &Manifest {
        match self {
            Self::Single(p) => &p.t,
            Self::Batch(p) => &p.t,
        }
    }

    /// The 0/1 (`Single`) or `2..=f` (`Batch`) resolution entries this proposal
    /// carries, in canonical (strictly increasing target) order.
    pub fn entries(&self) -> &[ResolutionEntry] {
        match self {
            Self::Single(p) => p.m.as_slice(),
            Self::Batch(p) => &p.m,
        }
    }

    pub fn digest(&self, sid: &Digest) -> Digest {
        match self {
            Self::Single(p) => p.digest(sid),
            Self::Batch(p) => p.digest(sid),
        }
    }

    /// `formed`/`formed_batch`, dispatched by shape.
    pub fn formed(&self, committee: &Committee) -> bool {
        match self {
            Self::Single(p) => formed(committee, p.view, &p.c, &p.t, &p.m),
            Self::Batch(p) => formed_batch(committee, p.view, &p.c, &p.t, &p.m),
        }
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
    /// AVAIL-ECHO-SPEC.md (`Parameters::echo_avail_claims`): this sender's positional
    /// availability claims against `proposal`'s own reference vector -- a bit per lane
    /// instead of `VantageAvail`'s explicit `(a,k,h)` tuples.
    ///
    /// THIRD field of the kind `wish` and `origin` established, and outside counting
    /// identity for the same reason: `proposal_digest` never reads it, and two echoes
    /// counted as the same statement may carry different claims. `AgbEngine` constructs
    /// this as `None` (the engine is deliberately availability-free, exactly as it is
    /// watermark-free -- see `wish`); `VantageCore` fills it in at serialization time
    /// from `LaneManager::build_avail_claim`, immediately before sending, and only when
    /// the flag is on.
    #[serde(default)]
    pub avail: Option<crate::vantage::claim::AvailClaim>,
}

/// signature-free.tex §8.3 "Digest-named AGB statements" (`Parameters::
/// digest_statements`): the digest-named counterpart of `Echo` -- the wire tuple
/// `(view, hash(B_v), grade, origin bit, sender)` the paragraph specifies, minus the
/// by-value proposal itself. Carried on its own wire message, `VantageEchoDigest`,
/// constructed only via `Echo::to_digest` at the emission boundary
/// (`VantageCore::execute`) -- `AgbEngine` itself never builds or reads one; see
/// `DigestStatements`'s own module doc comment for the reception-side translation
/// layer this type feeds.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoDigest {
    pub view: View,
    pub digest: Digest,
    pub grade: u8,
    pub sender: PublicKey,
    pub wish: View,
    pub origin: Option<u8>,
    /// AVAIL-ECHO-SPEC.md §6.5: the claim rides the digest-named encoding too, because
    /// that is what production actually sends (`digest_statements` defaults to true) --
    /// attaching claims only to by-value echoes would leave them unused in every real
    /// run. The lane indices address the BODY's reference vector, so a receiver holding
    /// only the digest cannot resolve them yet; `DigestStatements` already buffers such a
    /// statement until a verified body arrives (`buffered_echo` -> `known_bodies`), and
    /// the claim is carried along that same path rather than through a second stash.
    #[serde(default)]
    pub avail: Option<crate::vantage::claim::AvailClaim>,
}

impl Echo {
    /// The compact, digest-named encoding of this exact statement -- `Parameters::
    /// digest_statements`'s emission-side translation, applied ONLY at the wire-
    /// serialization boundary, never inside `AgbEngine`: the engine keeps
    /// constructing a full by-value `Echo` exactly as before (`build_echo_out`), and
    /// this is purely an alternate wire ENCODING of that same, unchanged value.
    /// `sid` is the same session id `AgbEngine` derives every proposal digest
    /// against (`AgbEngine::sid`), so `self.proposal.digest(sid)` here is byte-
    /// identical to whatever digest a receiver -- by-value or digest-named -- would
    /// independently compute for the same content.
    pub fn to_digest(&self, sid: &Digest) -> EchoDigest {
        EchoDigest {
            view: self.proposal.view,
            digest: self.proposal.digest(sid),
            grade: self.grade,
            sender: self.sender,
            wish: self.wish,
            origin: self.origin,
            // Carried through, like `wish`/`origin`: this is an alternate ENCODING of the
            // same statement, so dropping the claim here would silently disable the
            // optimization in exactly the configuration production runs.
            avail: self.avail.clone(),
        }
    }
}

/// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): `EchoBatch`, the batch-proposal echo.
/// PHASE8 (signature-free.tex 704fb29, par:batched-anchors): carries no `Ann`/origin
/// field at all -- `formed_batch` now requires every `proposal.m` entry to be `Skip`,
/// and a skip entry's origin bit is always `None` (`Echo::origin`'s own doc comment),
/// so a per-position origin vector here would carry zero bits of real information.
/// (An earlier revision of this type carried `origin: Vec<Option<u8>>`, PHASE7's
/// vector generalization of `Echo::origin`, back when a batch could still contain a
/// full/core coordinate.)
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoBatch {
    pub proposal: BatchViewProposal,
    pub grade: u8,
    pub sender: PublicKey,
    pub wish: View,
}

/// PHASE7: normalizes `Echo`/`EchoBatch` for `AgbEngine`'s internal counting (never
/// itself serialized -- see `ProposalOut`'s identical role for the propose message).
#[derive(Clone, Debug)]
pub enum EchoOut {
    Single(Echo),
    Batch(EchoBatch),
}

impl EchoOut {
    pub fn proposal_view(&self) -> View {
        match self {
            Self::Single(e) => e.proposal.view,
            Self::Batch(e) => e.proposal.view,
        }
    }

    pub fn grade(&self) -> u8 {
        match self {
            Self::Single(e) => e.grade,
            Self::Batch(e) => e.grade,
        }
    }

    pub fn sender(&self) -> PublicKey {
        match self {
            Self::Single(e) => e.sender,
            Self::Batch(e) => e.sender,
        }
    }

    pub fn wish(&self) -> View {
        match self {
            Self::Single(e) => e.wish,
            Self::Batch(e) => e.wish,
        }
    }

    pub fn set_wish(&mut self, wish: View) {
        match self {
            Self::Single(e) => e.wish = wish,
            Self::Batch(e) => e.wish = wish,
        }
    }

    /// The per-entry `Ann` bits, aligned 1:1 with `self.proposal().entries()` --
    /// `Echo::origin`'s single `Option<u8>` naturally covers the `Single` shape's 0/1
    /// entries (`None` when `M` is empty is indistinguishable from `None` for a
    /// one-entry skip, but the two cases are equivalent for every reader: `ReadyOK`
    /// treats a skip position exactly like an absent one, always passing). PHASE8:
    /// always empty for `Batch` -- every batch entry is `Skip` (`formed_batch`), whose
    /// origin is always `None`, so there is no per-position bit left to carry;
    /// `recheck_ready`'s tally loop is bounded by this Vec's own length, so an empty
    /// result here simply contributes zero origin-one counts, which is correct (a
    /// batch's `ReadyOK` never reads them -- every position is `Skip`, which always
    /// passes independent of origin).
    pub fn origin_vec(&self) -> Vec<Option<u8>> {
        match self {
            Self::Single(e) => {
                if e.proposal.m.is_some() {
                    vec![e.origin]
                } else {
                    Vec::new()
                }
            }
            Self::Batch(_) => Vec::new(),
        }
    }

    pub fn into_proposal_out(self) -> ProposalOut {
        match self {
            Self::Single(e) => ProposalOut::Single(e.proposal),
            Self::Batch(e) => ProposalOut::Batch(e.proposal),
        }
    }
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

/// signature-free.tex §8.3 "Digest-named AGB statements": the digest-named
/// counterpart of `Ready`, mirroring `EchoDigest` exactly (see its own doc comment) --
/// no origin bit (the paragraph's "origin bit, for an ECHO" is echo-only).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ReadyDigest {
    pub view: View,
    pub digest: Digest,
    pub grade: ReadyGrade,
    pub sender: PublicKey,
    pub wish: View,
}

impl Ready {
    /// `Echo::to_digest`'s counterpart for `Ready` -- see that method's doc comment.
    pub fn to_digest(&self, sid: &Digest) -> ReadyDigest {
        ReadyDigest {
            view: self.proposal.view,
            digest: self.proposal.digest(sid),
            grade: self.grade,
            sender: self.sender,
            wish: self.wish,
        }
    }
}

/// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): `Ready`'s counterpart for a
/// `BatchViewProposal` -- `grade` is unaffected by `M`'s plurality (it grades the
/// carrying `(C,T)` payload alone, exactly as today), so only `proposal`'s type
/// differs.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ReadyBatch {
    pub proposal: BatchViewProposal,
    pub grade: ReadyGrade,
    pub sender: PublicKey,
    pub wish: View,
}

/// PHASE7: normalizes `Ready`/`ReadyBatch`, mirroring `EchoOut`.
#[derive(Clone, Debug)]
pub enum ReadyOut {
    Single(Ready),
    Batch(ReadyBatch),
}

impl ReadyOut {
    pub fn sender(&self) -> PublicKey {
        match self {
            Self::Single(r) => r.sender,
            Self::Batch(r) => r.sender,
        }
    }

    pub fn wish(&self) -> View {
        match self {
            Self::Single(r) => r.wish,
            Self::Batch(r) => r.wish,
        }
    }

    pub fn set_wish(&mut self, wish: View) {
        match self {
            Self::Single(r) => r.wish = wish,
            Self::Batch(r) => r.wish = wish,
        }
    }

    pub fn proposal_view(&self) -> View {
        match self {
            Self::Single(r) => r.proposal.view,
            Self::Batch(r) => r.proposal.view,
        }
    }

    pub fn grade(&self) -> ReadyGrade {
        match self {
            Self::Single(r) => r.grade,
            Self::Batch(r) => r.grade,
        }
    }

    pub fn into_proposal_out(self) -> ProposalOut {
        match self {
            Self::Single(r) => ProposalOut::Single(r.proposal),
            Self::Batch(r) => ProposalOut::Batch(r.proposal),
        }
    }
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
    if !strictly_sorted_and_staked(committee, c) || !strictly_sorted_and_staked(committee, t) {
        return false;
    }
    if !distinct_hashes(c, t) {
        return false; // duplicate hash across C ∪ T
    }
    if let Some(entry) = m {
        if !formed_entry(committee, view, entry) {
            return false;
        }
    }
    true
}

/// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph) / PHASE8
/// (signature-free.tex 704fb29, par:batched-anchors, "The audit narrows the
/// previously added recovery batching rule"): well-formedness for the vector-`M`
/// wire shape (`BatchViewProposal`). The
/// carrying `C`/`T` bounds are identical to `formed`'s own (shared helpers below);
/// additionally: `m.len()` is `2..=f` (`f` derived from the committee, never a config
/// knob -- a Byzantine sender misusing this wire shape for the 0/1-entry case, which
/// belongs on `ViewProposal`/`formed`, is rejected outright, so there is exactly one
/// canonical wire representation per logical `M`), EVERY entry is `Skip` --
/// manifest-free, skip-only ("a vector with a full or core entry is malformed; those
/// outcomes use one general entry" -- narrowed from PHASE7's original full/core-
/// capable vector, which the paper's own audit found put `f` independent manifest
/// pairs in one statement, breaking the proved `O(n*lambda)` by-value statement
/// bound) -- and targets are STRICTLY increasing (load-bearing for §6's "apply a
/// batched anchor's entries in increasing target order" -- enforced here so a FIXED
/// batch proposal already guarantees `pump_log`'s in-order iteration is the paper's
/// rule, not merely this implementation's convention).
pub fn formed_batch(
    committee: &Committee,
    view: View,
    c: &Manifest,
    t: &Manifest,
    m: &[ResolutionEntry],
) -> bool {
    if !strictly_sorted_and_staked(committee, c) || !strictly_sorted_and_staked(committee, t) {
        return false;
    }
    if !distinct_hashes(c, t) {
        return false;
    }
    let f_cap = batch_cap(committee);
    if m.len() < 2 || m.len() > f_cap {
        return false;
    }
    let mut prev: Option<View> = None;
    for entry in m {
        // PHASE8: "a vector with a full or core entry is malformed" -- the vector
        // form is skip-only; full/core outcomes always use the single general entry
        // on `ViewProposal`/`formed`.
        if !matches!(entry, ResolutionEntry::Skip(_)) {
            return false;
        }
        if !formed_entry(committee, view, entry) {
            return false;
        }
        let u = entry.target_view();
        if let Some(p) = prev {
            if u <= p {
                return false; // strictly increasing targets
            }
        }
        prev = Some(u);
    }
    true
}

/// The vector cap, `f` -- derived from the committee, floored at 1 so a single-entry
/// proposal is always representable regardless of committee size. Shared by
/// `formed_batch` (receiver-side validation) and `Resolver::decide_prefix`
/// (proposer-side construction), so both sides agree on the same bound.
pub fn batch_cap(committee: &Committee) -> usize {
    Thresholds::from_party_count(committee.size())
        .f_plus_1_parties
        .saturating_sub(1)
        .max(1)
}

/// Shared by `formed`/`formed_batch`: one resolution entry's own bounds (`1 <= u <=
/// view - 3`, its own `C_u`/`T_u` sorted/staked/distinct) -- PHASE6-SPEC.md §1's
/// per-entry checklist, unchanged by batching (a view enters a batch only if it
/// qualifies exactly as a lone entry would).
fn formed_entry(committee: &Committee, view: View, entry: &ResolutionEntry) -> bool {
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
    true
}

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

/// PHASE6-SPEC.md §1 `AuxRefs(M)`: the non-skip entries' manifests (empty for no
/// entries) -- authorized alongside the carrying proposal's own C/T, both on fixing
/// (§5 `on_propose`) and on completion (§7 `recheck_completion_and_direct`).
/// PHASE7: generalized from a single optional entry to a slice, uniformly covering
/// `ProposalOut::entries()`'s 0/1/many shapes.
fn aux_refs_entries(entries: &[ResolutionEntry]) -> Vec<BlockRef> {
    entries
        .iter()
        .flat_map(|entry| match entry {
            ResolutionEntry::Full(_, c, t) | ResolutionEntry::Core(_, c, t) => {
                c.iter().chain(t.iter()).cloned().collect::<Vec<_>>()
            }
            ResolutionEntry::Skip(_) => Vec::new(),
        })
        .collect()
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
    /// The proposal is `Arc`-wrapped purely as an internal ownership optimization
    /// (Efficiency Item 3): every clone below is a refcount bump, never a deep copy
    /// of `c`/`t`/`m`. Content, digest, and every comparison/query over it are
    /// unchanged; the wrapper never crosses into a wire type (`Echo`/`Ready`/their
    /// `*Batch` counterparts still carry an owned proposal, materialized via
    /// `(*arc).clone()` at the point an effect is actually built). PHASE7: `Arc<
    /// ProposalOut>`, not `Arc<ViewProposal>` -- generalized to cover the batch
    /// shape too (see `ProposalOut`'s own doc comment; `Single` degenerates to
    /// exactly today's behavior).
    Proposal(Arc<ProposalOut>, Digest),
}

#[derive(Clone, Debug)]
enum EchoStatement {
    /// A counted proposal echo: the proposal (`Arc`-wrapped, see `Fixed::Proposal`),
    /// its digest, its grade (0 or 1), and its per-entry origin bits (PHASE6-SPEC.md
    /// §3 `Ann`, generalized by PHASE7 from a single `Option<u8>` to a `Vec`, one
    /// per `proposal.entries()` position -- `None` for a skip entry, aligned 1:1).
    Graded(Arc<ProposalOut>, Digest, u8, Vec<Option<u8>>),
    Skip,
}

#[derive(Clone, Debug)]
enum ReadyStatement {
    /// A counted proposal ready (`Arc`-wrapped, see `Fixed::Proposal`).
    Graded(Arc<ProposalOut>, Digest, ReadyGrade),
    /// PHASE6-SPEC.md D6-5: a counted no-ready -- Phase 4/5 recorded only that the
    /// one-shot ready-stage slot was used, never the content; §4's justification needs
    /// the content (a first-hand noready census per view), so it is stored now.
    NoReady,
}

/// §8's fast-seal lock, `L_i(v, B)`.
#[derive(Clone, Debug)]
struct Lock {
    proposal: ProposalOut,
    digest: Digest,
    /// "A lock may be born inactive; once inactive it never reactivates."
    active: bool,
}

/// signature-free.tex's "Grounded post-ready skip" -- the resolution-stance paragraph
/// beginning "Vantage also maintains a persistent per-target resolution stance": a
/// correct party's persistent per-target stance `z_i(u)`, unconditional protocol
/// state (every party maintains one for every target, always). `Free` is the initial
/// value; `TryMetaOK` may claim `Free -> NonSkip` (before echoing a carrier with a
/// non-skip entry for this target); the post-ready skip-vote rule may claim `Free ->
/// SkipVoted` -- see `AgbEngine::try_meta_ok`/`recheck_skip_vote_trigger`.
/// `pub(crate)` purely so `#[cfg(test)]` accessors elsewhere in this module can
/// return it to sibling test modules; never re-exported via `vantage::mod`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum Stance {
    #[default]
    Free,
    NonSkip,
    SkipVoted,
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
    digest_cache: Vec<(Arc<ProposalOut>, Digest)>,
    /// This view's persistent resolution stance `z_i(u)` (see `Stance`,
    /// `AgbEngine::try_meta_ok`, `AgbEngine::recheck_skip_vote_trigger`). Read when
    /// this `View` is a resolution TARGET `u`; written by both `try_meta_ok` (Free ->
    /// NonSkip, on this same target) and the skip-vote rule (Free -> SkipVoted).
    stance: Stance,
    /// This carrying view's persistent accepted-metadata latch -- the concrete
    /// realization of Direct AGB's abstract, persistent `AuxOK_i(w,M)` predicate
    /// (`try_meta_ok`'s doc comment). Read/written when this `View` is a CARRYING view
    /// `w`. Never cleared once set.
    aux_accepted: bool,
    /// PHASE8: first-hand counted `SkipVote(u)` statements for this view as a
    /// resolution TARGET `u`, deduped by sender (mirrors `echo_statements`/
    /// `ready_statements`'s "first counted wins" census).
    skip_vote_statements: HashSet<PublicKey>,
    /// PHASE8: one-shot latch -- have we already submitted `skip-seal(u) -> gskip` to
    /// the try-seal arbiter via the vote-quorum route (mirrors `fastsealed`'s identical
    /// role for the fast-seal route).
    skip_sealed: bool,
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
            stance: Stance::Free,
            aux_accepted: false,
            skip_vote_statements: HashSet::new(),
            skip_sealed: false,
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
    /// `Q = 2f+1`, party count -- the grounded skip-vote/skip-seal quorum. Shares
    /// D4-3's fast-seal convention (party count, not stake): both mechanisms live in
    /// the same "fast- and skip-seal wrappers" algorithm box, both are caller-side
    /// quorum-intersection arguments over first-hand per-author censuses (lem:fast-
    /// seal / lem:skip-seal), neither is the stake-weighted `quorum` field below that
    /// R3/R4 use to certify a VALUE.
    two_f_plus_1_parties: usize,
    quorum: Stake,
    views: BTreeMap<View, ViewState>,
    /// Efficiency Item 2: exactly the views `recheck_all` would find by scanning
    /// `views` for `active && !echo_sent && matches!(fixed, Fixed::Proposal(..))`.
    /// Maintained incrementally at the only three sites that can change this
    /// membership (`activate`, `on_propose`, and every `echo_sent = true` site) --
    /// see those call sites for the exact insert/remove reasoning.
    pending_gate: BTreeSet<View>,
    /// n=100 straggler fix (2026-08-08): where `recheck_all`'s budgeted scan resumes
    /// -- the successor of the last view the previous call gate-checked, in ring
    /// order over `pending_gate`. Only consulted when the set exceeds
    /// `RECHECK_BUDGET`; a stale value (an echoed or pruned view) is harmless, since
    /// `BTreeSet::range` keys off the bound, not membership.
    recheck_cursor: View,
    /// Lowest view for which this engine may still create/read per-view state. Views
    /// below this have crossed the node-level GC floor and are treated as already
    /// resolved for late-message/timer purposes.
    min_live_view: View,
    /// PHASE6-SPEC.md §9 gate amendment: per-view seal-route counters.
    /// `None` in most unit tests, which don't assert on metrics.
    metrics: Option<Arc<Metrics>>,
}

/// n=100 straggler fix (2026-08-08): per-call ceiling on how many `pending_gate`
/// views one `recheck_all` invocation gate-checks. Sized so the budget never binds
/// on a healthy node (whose set holds 0-2 views) and caps a straggler's per-call
/// cost at ~budget x O(n) census lookups (tens of microseconds at n=100) while
/// still rotating over even a 10k-view backlog within ~160 triggers -- triggers
/// arrive per inbound response, so sub-second in practice.
const RECHECK_BUDGET: usize = 64;

/// n=100 straggler fix (2026-08-08): ceiling on `DigestStatements::pending_fetch`. It
/// was unbounded, cleared only on a successful serve/local-fix or by a `gc_below` whose
/// floor cannot advance on a stalled node, while `retry_fetches` re-fans every overdue
/// entry once a second -- making total fetches quadratic in stall duration (measured
/// 84,386 on a straggler vs 2,004 healthy). Sized well above the number of views a
/// healthy node ever has outstanding, so it binds only on a node that has genuinely
/// stopped resolving. See `ensure_fetch` for why eviction takes the HIGHEST views.
pub(crate) const MAX_PENDING_FETCH: usize = 1_024;

/// Buffered authors asked on a pair's FIRST body fetch, doubling per retry.
///
/// `ensure_fetch` used to ask EVERY buffered author of the pair, every retry, forever.
/// Measured on the 2026-08-08 n=100 netem run, on a node whose core was 96% busy:
/// **433,656 `VantageBodyFetch` sent against 53 on a healthy node** -- 153 pending pairs x
/// ~37 targets x 76 retry rounds (`fetch_retry_interval` is `delta_ms * 8` = 1.6s). Those
/// sends are executed on the core thread at a measured ~50 us each, which accounts for
/// roughly 22s of the node's +29.8s `effect_execution` excess.
///
/// And they are overwhelmingly wasted: this module's own note at `ensure_fetch` records a
/// network-wide body-fetch answer rate of **7.8%**, against 85.2% for header repair,
/// because a stalled node's fetches name views the rest of the committee has already
/// pruned. Asking 37 peers for something ~92% of them cannot answer is the wrong shape.
///
/// Mirrors the staged escalation `Repairer` already uses for header repair (see
/// `repair::FanoutState`): start narrow, widen only if the narrow ask goes unanswered.
pub(crate) const FETCH_WIDTH_START: usize = 2;

/// Retries after which a pair is ABANDONED rather than asked again.
///
/// The width doubling alone does not bound anything: after ~5 retries it reaches full
/// width and a pair that can never be answered keeps paying full fan-out for the rest of
/// the run -- which is exactly the measured state. What bounds it is giving up.
///
/// Dropping a pair is free, and this module already relies on that: see `ensure_fetch`'s
/// eviction note -- `on_echo_digest`/`on_ready_digest` re-create the pair on the next
/// statement arrival for that view, and `buffered_echo`/`buffered_ready` retain the
/// statement regardless. So abandoning is not "never fetch this again": it is "stop
/// spending until something tells us the view is still live". A view that has genuinely
/// gone quiet is one the committee has moved past, and fetching its body cannot help.
///
/// 4 attempts at widths 2/4/8/16 is 30 messages per pair, against the 2,812 measured.
const MAX_FETCH_ATTEMPTS: u32 = 4;

/// Per-pair body-fetch progress: when it was last asked, how wide to ask next, and how
/// many times it has been asked. See `FETCH_WIDTH_START` / `MAX_FETCH_ATTEMPTS`.
#[derive(Clone, Copy, Debug)]
struct FetchState {
    last: Instant,
    next_width: usize,
    attempts: u32,
}

/// The <= `budget` views one budgeted `recheck_all` call scans: ring order over
/// `pending`, starting at `cursor` and wrapping to the smallest view once the
/// upper range is exhausted. Returns the whole set (ascending) whenever it fits
/// within `budget` -- the pre-budget behavior, byte-identical. Pure; the caller
/// advances its cursor to `last scanned + 1`.
pub(crate) fn recheck_window(pending: &BTreeSet<View>, cursor: View, budget: usize) -> Vec<View> {
    if pending.len() <= budget {
        return pending.iter().copied().collect();
    }
    let mut window: Vec<View> = pending.range(cursor..).take(budget).copied().collect();
    if window.len() < budget {
        let remaining = budget - window.len();
        window.extend(pending.range(..cursor).take(remaining).copied());
    }
    window
}

impl AgbEngine {
    pub fn new(name: PublicKey, committee: Committee, sid: Digest, delta_ms: u64) -> Self {
        let n = committee.size();
        let thresholds = Thresholds::from_party_count(n);
        let quorum = committee.quorum_threshold();
        Self {
            name,
            committee,
            sid,
            delta: Duration::from_millis(delta_ms),
            n,
            f_plus_1_parties: thresholds.f_plus_1_parties,
            two_f_plus_1_parties: thresholds.two_f_plus_1_parties,
            quorum,
            views: BTreeMap::new(),
            pending_gate: BTreeSet::new(),
            recheck_cursor: 1,
            min_live_view: 1,
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
                let prev_ready_sent = prev == 0
                    || self.is_pruned(prev)
                    || self.views.get(&prev).is_some_and(|s| s.ready_sent);
                prev_ready_sent.then(|| view + 2)
            }
            ResponseStage::Ready => {
                let next = view + 1;
                (self.is_pruned(next) || self.views.get(&next).is_some_and(|s| s.echo_sent))
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

    fn is_pruned(&self, view: View) -> bool {
        view < self.min_live_view
    }

    pub fn gc_below(&mut self, floor: View) {
        if floor <= self.min_live_view {
            return;
        }
        self.views = self.views.split_off(&floor);
        self.pending_gate = self.pending_gate.split_off(&floor);
        self.min_live_view = floor;
    }

    /// Current `pending_gate` size -- exported as `vantage_pending_gate_len` by
    /// `VantageCore::sample_metrics` (see that gauge's doc comment for why this is
    /// the straggler death-spiral confirmation signal).
    pub fn pending_gate_len(&self) -> usize {
        self.pending_gate.len()
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
        proposal: ProposalOut,
    ) -> (Arc<ProposalOut>, Digest) {
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
        self.is_pruned(view) || self.views.contains_key(&view)
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
            |stmt| matches!(stmt, EchoStatement::Graded(p, _, 1, _) if p.c() == c && p.t() == t),
        )
    }

    /// Counted echoes (any grade) for `view` naming exactly payload `(c,t)` -- §4's
    /// `Core` justification (`>= f+1`).
    pub fn echo_any_grade_count_for(&self, view: View, c: &Manifest, t: &Manifest) -> usize {
        self.echo_count(
            view,
            |stmt| matches!(stmt, EchoStatement::Graded(p, _, _, _) if p.c() == c && p.t() == t),
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
                let key = (p.c().clone(), p.t().clone());
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
        self.is_pruned(view) || self.views.get(&view).is_some_and(|s| s.sealed.is_some())
    }

    /// D7-4 (PHASE7-PREP-NOTES.md): read-only mirror of the exact guard
    /// `on_echo_fallback_timer`/`on_echo_absolute_timer` already check internally --
    /// used by the timer-queue's lazy stale-discard at pop time, so a superseded timer
    /// (its echo already sent organically) is dropped without ever constructing/
    /// dispatching the handler call, instead of dispatching into a guard that would
    /// have returned an empty `Vec` anyway. Same value, same meaning, no `&mut self`.
    pub fn echo_sent(&self, view: View) -> bool {
        self.is_pruned(view) || self.views.get(&view).is_some_and(|s| s.echo_sent)
    }

    /// D7-4: read-only mirror of `on_ready_timer`'s guard, same reasoning as
    /// `echo_sent` above.
    pub fn ready_sent(&self, view: View) -> bool {
        self.is_pruned(view) || self.views.get(&view).is_some_and(|s| s.ready_sent)
    }

    /// signature-free.tex §8.3 "Digest-named AGB statements": read-only accessor for
    /// the session id every proposal digest is computed against -- lets the
    /// wire-serialization boundary (`VantageCore::execute`'s emission-side
    /// translation) and `DigestStatements` (reception-side body verification) derive
    /// the SAME digest this engine itself would, without duplicating `sid`
    /// elsewhere. Read-only: exposes existing state, changes no rule/threshold/
    /// one-shot, same class as `is_sealed`/`echo_sent`/`ready_sent` above.
    pub fn sid(&self) -> &Digest {
        &self.sid
    }

    /// Read-only accessor mirroring `sid()` -- the committee `DigestStatements` needs
    /// for `formed`'s well-formedness check on a served body.
    pub fn committee(&self) -> &Committee {
        &self.committee
    }

    /// signature-free.tex §8.3 "Digest-named AGB statements": read-only query of
    /// this view's fixed proposal (received BY VALUE, via the ordinary propose path
    /// -- `on_propose_any`'s sticky `Fixed::Proposal` write) and its digest, or
    /// `None` for `Unset`/`Reject`/a pruned view. Lets `DigestStatements` recognize
    /// when a locally fixed proposal already matches a digest a remote digest-named
    /// statement just named, without duplicating any of `on_propose_any`'s own
    /// sticky-fixing/validation logic, and lets it answer a peer's body-fetch from
    /// this same state. Read-only: exposes existing state exactly like `is_sealed`/
    /// `echo_sent`/`ready_sent` do; changes no rule, threshold, or one-shot.
    /// CRITICAL for the paragraph's provenance guarantee: this is the ONLY signal
    /// `DigestStatements` ever treats as "directly received from the proposer" --
    /// nothing in that type ever calls `on_propose`/`on_propose_batch` itself (a
    /// served body is verified and drained without ever touching `Fixed`), so a
    /// served body can never cause THIS accessor to start returning `Some` for it.
    pub fn fixed_proposal(&self, view: View) -> Option<(Arc<ProposalOut>, Digest)> {
        if self.is_pruned(view) {
            return None;
        }
        match self.views.get(&view).map(|s| &s.fixed) {
            Some(Fixed::Proposal(p, d)) => Some((Arc::clone(p), d.clone())),
            _ => None,
        }
    }

    /// PHASE6-SPEC.md §6: submit an anchor-derived outcome `X_u` to the SAME try-seal
    /// arbiter direct/fastseal submissions use (reuse rule) -- first submission for
    /// `view` wins and emits `Effect::Sealed`; a later, compatible submission is a
    /// no-op (`debug_assert`ed compatible by `outcomes_compatible`, same as ever).
    pub fn submit_anchor(&mut self, view: View, outcome: Outcome) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
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

    /// PHASE8: this view's persistent resolution stance `z_i(u)` -- `Free` both for a
    /// genuinely untouched view and for a pruned one (matching every other query
    /// accessor's "pruned reads as the default/already-resolved value" convention).
    #[cfg(test)]
    pub(crate) fn stance_for_test(&self, view: View) -> Stance {
        self.views.get(&view).map_or(Stance::Free, |s| s.stance)
    }

    /// PHASE8: first-hand counted `SkipVote(u)` statements -- exposed for tests that
    /// want to assert the census directly rather than only through `Effect::Sealed`.
    #[cfg(test)]
    pub(crate) fn skip_vote_count_for_test(&self, view: View) -> usize {
        self.views
            .get(&view)
            .map_or(0, |s| s.skip_vote_statements.len())
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
        if self.is_pruned(view) {
            return effects;
        }
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

        effects.extend(self.activate(view, lm, rep));
        effects
    }

    /// §4: `activate(v)` -- called by the caller once `Frontier` determines `v` is
    /// newly active (either via the proposal-chain advance or via `enter(v)`, which
    /// calls this directly). Idempotent.
    pub fn activate(
        &mut self,
        view: View,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        if self.is_pruned(view) {
            return Vec::new();
        }
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
        self.recheck_gate(view, lm, rep)
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
        self.on_propose_any(sender, ProposalOut::Single(proposal), now, lm, rep)
    }

    /// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): the `BatchViewProposal` counterpart of
    /// `on_propose` -- same D4 declared-sender trust, same sticky-`fixed` semantics,
    /// delegated to the same shared `on_propose_any` core.
    pub fn on_propose_batch(
        &mut self,
        sender: PublicKey,
        proposal: BatchViewProposal,
        now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        self.on_propose_any(sender, ProposalOut::Batch(proposal), now, lm, rep)
    }

    fn on_propose_any(
        &mut self,
        sender: PublicKey,
        proposal: ProposalOut,
        now: Instant,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let view = proposal.view();
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
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

        if !proposal.formed(&self.committee) {
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
            .c()
            .iter()
            .chain(proposal.t().iter())
            .chain(aux_refs_entries(proposal.entries()).iter())
        {
            effects.extend(rep.authorize(r.clone()));
        }
        effects.push(Effect::Fixed(view, true));

        if let Some(entry) = self.state_mut(view).entry_instant {
            let t = std::cmp::max(entry, now);
            let deadline = std::cmp::min(t + self.delta, entry + theta_echo);
            effects.push(Effect::ArmTimer(view, TimerKind::EchoFallback, deadline));
        }

        effects.extend(self.recheck_gate(view, lm, rep));
        effects
    }

    /// R2's positive gate, re-evaluated whenever local state that could satisfy it
    /// changes (ack counts, payload arrivals, block cached, activation) -- call for
    /// every currently pending, active view after any such event.
    pub fn recheck_all(&mut self, lm: &mut LaneManager, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        // Efficiency Item 2: `pending_gate` is maintained incrementally (see its
        // field doc and the `activate`/`on_propose`/`echo_sent`-site comments) to
        // always equal exactly what the old full `self.views` scan below would have
        // found:
        //   self.views.iter().filter(|(_, s)| s.active && !s.echo_sent
        //       && matches!(s.fixed, Fixed::Proposal(_, _))).map(|(v, _)| *v)
        // `active`, `fixed`, and `echo_sent` are each one-shot/monotonic (active and
        // fixed only ever become true/set once; echo_sent only ever flips false ->
        // true), so the three transition sites are exhaustive.
        //
        // Iteration order over a `HashSet` is just as unspecified as it was over the
        // `HashMap` here before, and is still fine -- but NOT because each view's
        // recheck is self-contained. It is not: `try_meta_ok` writes a DIFFERENT
        // view's state (`state_mut(*u).stance = NonSkip` for each resolution-entry
        // target `u`) and `meta_ok_entry` reads target views' echo/ready census, lock
        // and seal. What makes the order immaterial is the shape of that one
        // cross-view write: it fires only on `stance == Free`, is one-shot, and
        // `stance_excludes` rejects only on `SkipVoted` or a sealed `Skip` -- so
        // `Free -> NonSkip` can only ever REMOVE an exclusion, never add one. Order
        // can therefore change which view's gate passes first within a scan, but not
        // whether a view eventually passes; anything still un-echoed stays in
        // `pending_gate` and is rechecked on the next trigger. The same bound is what
        // makes `VantageCore::execute`'s coalescing (one scan per effect drain rather
        // than one per credited availability ref) safe.
        //
        // n=100 straggler fix (2026-08-08): the scan is additionally BUDGETED to
        // `RECHECK_BUDGET` views per call, resumed round-robin across calls via
        // `recheck_cursor` (see `recheck_window`). Safety rests on the exact sentence
        // already doing the work above: "anything still un-echoed stays in
        // `pending_gate` and is rechecked on the next trigger" -- the budget only
        // ever DEFERS a recheck to a later trigger, never skips one, and triggers
        // arrive per inbound response and per effect drain. Healthy nodes hold 0-2
        // pending views, so the budget never binds there; it binds exactly on a
        // straggler whose gates fail wholesale (its `pending_gate` grows with its
        // view gap), where the pre-budget full scan -- re-run per inbound message --
        // cost O(gap x n) each and collapsed intake to ~10% (the measured ~500x
        // per-message cost explosion; see `vantage_pending_gate_len`).
        let views = recheck_window(&self.pending_gate, self.recheck_cursor, RECHECK_BUDGET);
        if let Some(last) = views.last() {
            self.recheck_cursor = last.saturating_add(1);
        }
        for view in views {
            effects.extend(self.recheck_gate(view, lm, rep));
        }
        effects
    }

    fn recheck_gate(
        &mut self,
        view: View,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
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
        let origin = self.compute_origin(proposal.entries());
        self.count_echo_statement(
            view,
            self.name,
            EchoStatement::Graded(Arc::clone(&proposal), digest, 1, origin.clone()),
        );
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        // The wire type still carries an owned proposal: exactly one deep clone here
        // (same total deep-clone count as before this file's efficiency changes --
        // the deep clone above simply moved from the now-Arc'd census entry to this
        // required-owned wire value).
        effects.push(Effect::BroadcastEcho(
            self.build_echo_out(&proposal, 1, origin),
        ));
        // D6-4: release evaluation runs BEFORE R3's ready recheck on this same newly
        // counted echo-stage response; the all-n fastseal trigger stays after.
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        // PHASE8: this is a GRADED echo, never an ECHO-SKIP, so `echo_skip_count`
        // cannot have changed here -- the skip-vote trigger's echo-skip-quorum conjunct
        // is unaffected and this call is a guaranteed no-op. Kept anyway, mirroring
        // `recheck_fastseal_trigger`'s own call-site set exactly, so every echo-census-
        // changing site uniformly rechecks every echo-driven trigger.
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// PHASE7: builds the outbound `Echo`/`EchoBatch` (wrapped in `EchoOut`) for a
    /// just-decided echo of `proposal`, dispatched by its own shape -- shared by the
    /// organic (positive-gate), fallback, and echo-skip-adjacent (well-formed but
    /// fallback) emission sites. `origin`'s length always matches
    /// `proposal.entries().len()` for `Single` (0 or 1) -- PHASE8: `EchoBatch` no
    /// longer has an origin field at all (every batch entry is `Skip`, whose origin is
    /// always `None`), so `origin` is simply dropped on that path; callers still
    /// compute it generically via `compute_origin(proposal.entries())` either way (a
    /// harmless, always-`None` computation for a batch), rather than branching by
    /// shape before calling in.
    fn build_echo_out(
        &self,
        proposal: &Arc<ProposalOut>,
        grade: u8,
        origin: Vec<Option<u8>>,
    ) -> EchoOut {
        match proposal.as_ref() {
            ProposalOut::Single(p) => EchoOut::Single(Echo {
                proposal: p.clone(),
                grade,
                sender: self.name,
                wish: 0, // D5-3: stamped by `VantageCore` at serialization time
                origin: origin.into_iter().next().flatten(),
                // AVAIL-ECHO-SPEC.md: like `wish`, stamped by `VantageCore` at
                // serialization time -- the engine is availability-free.
                avail: None,
            }),
            ProposalOut::Batch(p) => EchoOut::Batch(EchoBatch {
                proposal: p.clone(),
                grade,
                sender: self.name,
                wish: 0,
            }),
        }
    }

    /// PHASE7: `build_echo_out`'s `Ready`/`ReadyBatch` counterpart -- `grade` is
    /// unaffected by `M`'s plurality, so this is a pure shape dispatch.
    fn build_ready_out(&self, proposal: &Arc<ProposalOut>, grade: ReadyGrade) -> ReadyOut {
        match proposal.as_ref() {
            ProposalOut::Single(p) => ReadyOut::Single(Ready {
                proposal: p.clone(),
                grade,
                sender: self.name,
                wish: 0, // D5-3: stamped by `VantageCore` at serialization time
            }),
            ProposalOut::Batch(p) => ReadyOut::Batch(ReadyBatch {
                proposal: p.clone(),
                grade,
                sender: self.name,
                wish: 0,
            }),
        }
    }

    /// R2's positive gate predicate: `CoreOK_i(C) ∧ TipOK_i(C,T) ∧ TryMetaOK_i(w,M)`
    /// (PHASE6-SPEC.md §2 adds the `MetaOK` conjunct to what Phase 4 called
    /// `positive_gate_holds`; PHASE8 renames the conjunct `TryMetaOK` -- see
    /// `try_meta_ok`'s doc comment). `&mut self` since PHASE8: `try_meta_ok` may
    /// atomically claim this carrying view's persistent stances/latch on success.
    fn positive_gate_holds(&mut self, proposal: &ProposalOut, lm: &mut LaneManager) -> bool {
        if !Self::core_ok(proposal.c(), lm) {
            return false;
        }
        if !proposal.t().iter().all(|r| lm.author_ok(r)) {
            return false;
        }
        if !Self::tip_ok(proposal.c(), proposal.t(), lm) {
            return false;
        }
        self.try_meta_ok(proposal.view(), proposal.entries(), lm)
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
        // Index C by author ONCE instead of scanning it per T entry: both manifests
        // hold one entry per authority, so the old `c.iter().find(..)` inside the T loop
        // made this O(n^2) per call -- and `recheck_all` calls it for every pending
        // view, on the single-threaded core, on every credited availability ref.
        let by_author: HashMap<_, _> = c.iter().map(|c_ref| (c_ref.0, c_ref)).collect();
        for t_ref in t {
            if let Some(c_ref) = by_author.get(&t_ref.0).copied() {
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
    /// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): the CONJUNCTION of `meta_ok_entry`
    /// over every entry -- a party echoes the carrying proposal only if EVERY entry
    /// passes the same single-entry predicate it always has. Degenerates exactly to
    /// today's behavior for 0/1 entries (`entries.iter().all(..)` over an empty or
    /// singleton slice).
    fn meta_ok(&self, entries: &[ResolutionEntry], lm: &mut LaneManager) -> bool {
        entries.iter().all(|entry| self.meta_ok_entry(entry, lm))
    }

    fn meta_ok_entry(&self, entry: &ResolutionEntry, lm: &mut LaneManager) -> bool {
        let u = entry.target_view();
        if self.is_pruned(u) {
            // SAFETY: a pruned `u` means we dropped the very evidence MetaOK is defined
            // over (our own E_i(u)/R_i(u), the active fast-seal lock, and `C_u`/`T_u`'s
            // local availability), so we cannot honestly evaluate it. This branch
            // originally returned `true`, which made every party whose GC floor had
            // passed `u` endorse a carrier's resolution entry for `u` unconditionally --
            // and `formed` bounds only `1 <= u <= w-3` (there is no lower bound on the
            // target), so a Byzantine proposer of a live carrier view could name an
            // arbitrarily old `u` with fabricated `(C_u,T_u)` and collect those free
            // endorsements from everyone past the floor.
            //
            // Declining is the safe direction: echo-skip is a legitimate outcome, and a
            // carrier targeting a view we have ALREADY resolved (pruning implies
            // resolved -- `gc_floor` is `resolved_watermark - window`) is moot for us
            // regardless. The cost is that a proposer lagging more than the GC window
            // behind is echo-skipped by up-to-date parties, and its carrier resolves via
            // the skip/recovery route instead of completing directly.
            return false;
        }
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
                        if lock.proposal.c() == c && lock.proposal.t() == t => {}
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
                        if p.c() != c_u || p.t() != t_u {
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
                        if p.c() != c_u || p.t() != t_u {
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

    /// signature-free.tex's "Grounded post-ready skip": `TryMetaOK_i(w, M)`, the
    /// metadata-acceptance transition R2 invokes in place of the pre-existing
    /// `MetaOK` conjunct -- the SOLE gate through which a carrier ECHO for nonempty
    /// `M` is ever decided (`positive_gate_holds` and `on_echo_fallback_timer` are its
    /// only two callers, both of which unconditionally proceed to build+push the
    /// ECHO/EchoSkip effect immediately after this returns, in the same synchronous
    /// call -- there is no intervening yield point in this single-threaded engine, so
    /// "atomically claim the stance, then emit" is simply "claim it in this function,
    /// return, and let the caller's very next statements run").
    ///
    /// Realizes two things the abstract Direct AGB contract requires of
    /// `AuxOK_i(w,M)` (persistence) that the raw per-entry conjunction (`meta_ok`)
    /// cannot provide by itself:
    ///   - the resolution-stance exclusion (`stance_excludes`): a non-skip entry for
    ///     target `u` additionally fails when this party's stance for `u` is already
    ///     `SkipVoted`, or it has already learned a terminal `gskip` for `u` (skip
    ///     entries never consult or affect the stance -- "the ordinary all-NO-READY
    ///     resolver path remains available after an unsuccessful skip-vote attempt");
    ///   - the persistent accepted-metadata latch `aux_accepted`, keyed by the
    ///     CARRYING view `w`: once every entry has passed once, further evaluation for
    ///     the SAME `w` short-circuits true without re-deriving anything. This matters
    ///     because the per-entry stance/terminal-outcome checks are the one place this
    ///     predicate is not itself monotone in time (a free stance that is still free
    ///     right now could, in principle, later become `SkipVoted`) -- exactly the
    ///     paper's "a free stance that could later change is not itself the abstract
    ///     predicate" remark. The latch is the named artifact later lemmas' proofs
    ///     point to ("by its accepted AuxOK latch, p_i emitted its target-view echo
    ///     before accepting the entry") -- structurally, `echo_sent`'s own one-shot
    ///     guard at both call sites already prevents this engine from ever reaching a
    ///     SECOND real evaluation for a `w` that already succeeded, so the latch's
    ///     short-circuit is not reachable via any current call path; it is still
    ///     implemented as the paper's own explicit, persistent predicate rather than
    ///     relying on that structural accident, so a future additional call site can
    ///     never regress persistence silently.
    ///
    /// On success, atomically (before returning, so before the caller's next
    /// statement -- the carrier ECHO): claims the at-most-one non-skip entry's target
    /// stance `Free -> NonSkip` (a no-op if already `NonSkip` -- "a later non-skip
    /// carrier may reuse that stance"), and sets `aux_accepted` for `w`.
    ///
    /// The stance-exclusion and the plain per-entry conjunction are independent,
    /// side-effect-free boolean checks evaluated before any commit below, so
    /// evaluating all entries' stance-exclusion first and then the existing `meta_ok`
    /// conjunction (rather than interleaving the two per entry, as the displayed
    /// pseudocode's single per-entry loop does) computes the identical AND of the same
    /// conjuncts -- a pure reassociation, not a behavior change.
    fn try_meta_ok(&mut self, w: View, entries: &[ResolutionEntry], lm: &mut LaneManager) -> bool {
        if entries.is_empty() {
            return true;
        }
        if self.views.get(&w).is_some_and(|s| s.aux_accepted) {
            return true;
        }
        if entries.iter().any(|entry| self.stance_excludes(entry)) {
            return false;
        }
        if !self.meta_ok(entries, lm) {
            return false;
        }
        for entry in entries {
            if let ResolutionEntry::Full(u, ..) | ResolutionEntry::Core(u, ..) = entry {
                let target = self.state_mut(*u);
                if target.stance == Stance::Free {
                    target.stance = Stance::NonSkip;
                }
            }
        }
        self.state_mut(w).aux_accepted = true;
        true
    }

    /// True (reject) iff `entry` is non-skip and this party's stance for its target
    /// is already `SkipVoted`, or it has already learned a terminal `gskip` for that
    /// target. Skip entries are never excluded here -- "skip entries neither require
    /// nor change the stance". A pruned target is handled by `meta_ok_entry`'s own
    /// pruned branch (declines outright), so this returns `false` (no ADDITIONAL
    /// exclusion) rather than duplicating that decline.
    fn stance_excludes(&self, entry: &ResolutionEntry) -> bool {
        let u = match entry {
            ResolutionEntry::Full(u, ..) | ResolutionEntry::Core(u, ..) => *u,
            ResolutionEntry::Skip(_) => return false,
        };
        if self.is_pruned(u) {
            return false;
        }
        self.views.get(&u).is_some_and(|s| {
            s.stance == Stance::SkipVoted || matches!(s.sealed, Some(Outcome::Skip))
        })
    }

    /// First-hand counted `ECHO-SKIP(u)` responses -- the grounding census the
    /// skip-vote rule requires a `2f+1` quorum of (`par:skip-seal`). Reuses the
    /// existing echo-stage census (`echo_statements`, already deduped one-per-author,
    /// first-hand-only -- counted only via `count_echo_statement`) rather than adding
    /// a parallel tally, per the reuse rule the rest of this module already follows
    /// for `noready_count`/`echo_grade1_count_for`/etc.
    fn echo_skip_count(&self, view: View) -> usize {
        self.echo_count(view, |stmt| matches!(stmt, EchoStatement::Skip))
    }

    /// The grounded post-ready skip vote's Upon-rule (par:skip-seal) -- re-evaluated
    /// at every site that can newly satisfy it: every echo-stage-census-changing site
    /// (mirroring `recheck_fastseal_trigger`'s own call sites exactly -- "process
    /// every enabled optimistic-lock release" before this rule is already guaranteed
    /// there, since `recheck_lock_release` always runs first at each of them) plus
    /// `on_ready_timer`, the one and only site where this party's own `R_i(u)`
    /// becomes `NoReady`.
    ///
    /// On success: persists the stance BEFORE broadcasting (`Free -> SkipVoted` is set
    /// before `Effect::BroadcastSkipVote` is pushed, matching "persist-before-send");
    /// self-counts our own vote immediately, mirroring the existing echo/ready
    /// self-counting convention (`recheck_gate`/`on_ready_timer` etc. count `self.name`
    /// in the same census a remote statement would land in); then rechecks the
    /// seal-quorum trigger, since counting our own vote can itself complete the
    /// quorum.
    fn recheck_skip_vote_trigger(&mut self, u: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(u) {
            return effects;
        }
        let ready_for_vote = self.views.get(&u).is_some_and(|s| {
            s.stance == Stance::Free
                && matches!(
                    s.ready_statements.get(&self.name),
                    Some(ReadyStatement::NoReady)
                )
                && !matches!(s.sealed, Some(Outcome::Full(..)) | Some(Outcome::Core(..)))
        });
        if !ready_for_vote || self.echo_skip_count(u) < self.two_f_plus_1_parties {
            return effects;
        }
        self.state_mut(u).stance = Stance::SkipVoted;
        let name = self.name;
        self.count_skip_vote_statement(u, name);
        if let Some(metrics) = &self.metrics {
            metrics.vantage_skip_votes_sent.inc();
        }
        effects.push(Effect::BroadcastSkipVote(u));
        effects.extend(self.recheck_skip_seal_trigger(u));
        effects
    }

    /// First-hand skip-vote dedup: at most one counted statement per (view, sender),
    /// ever -- mirrors `count_echo_statement`/`count_ready_statement`. Returns
    /// whether this call newly counted (`sender` had no prior statement for `view`)
    /// -- "count only the first directly received statement per author".
    fn count_skip_vote_statement(&mut self, view: View, sender: PublicKey) -> bool {
        if self.is_pruned(view) {
            return false;
        }
        self.state_mut(view).skip_vote_statements.insert(sender)
    }

    /// The skip-seal wrapper's Upon-rule -- upon counting a first-hand `Q = 2f+1`
    /// quorum of `SkipVote(u)` statements, submit `gskip` to the SAME caller-side
    /// try-seal arbiter the fast-seal/direct/anchor routes use (route `"vote_skip"`).
    /// No completion, no direct-seal, no completion report, R1--R4 unchanged.
    /// Idempotent with a later anchor submission for the same view -- `try_seal`
    /// itself provides that (first submission wins; a later compatible one is a
    /// no-op).
    fn recheck_skip_seal_trigger(&mut self, u: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(u) {
            return effects;
        }
        if self.views.get(&u).is_some_and(|s| s.skip_sealed) {
            return effects;
        }
        let count = self
            .views
            .get(&u)
            .map_or(0, |s| s.skip_vote_statements.len());
        if count < self.two_f_plus_1_parties {
            return effects;
        }
        self.state_mut(u).skip_sealed = true;
        self.try_seal(u, Outcome::Skip, "vote_skip", &mut effects);
        effects
    }

    /// A counted `VantageSkipVote`.
    pub fn on_skip_vote(&mut self, view: View, sender: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if !self.count_skip_vote_statement(view, sender) {
            return effects;
        }
        if let Some(metrics) = &self.metrics {
            metrics.vantage_skip_votes_received.inc();
        }
        effects.extend(self.recheck_skip_seal_trigger(view));
        effects
    }

    /// PHASE6-SPEC.md §3 `Ann`, generalized by PHASE7 to a vector: this party's own
    /// origin bit for EACH of the carrying proposal's `M` entries, each computed
    /// exactly as `compute_origin_entry` does for a lone entry -- degenerates
    /// exactly to today's single-bit behavior for 0/1 entries.
    fn compute_origin(&self, entries: &[ResolutionEntry]) -> Vec<Option<u8>> {
        entries
            .iter()
            .map(|entry| self.compute_origin_entry(entry))
            .collect()
    }

    /// This party's own origin bit for ONE `M` entry, computed from its own
    /// already-emitted `E_i(u)` at emission time.
    fn compute_origin_entry(&self, entry: &ResolutionEntry) -> Option<u8> {
        let u = entry.target_view();
        // SAFETY: deliberately NO pruned-view shortcut. This function used to return
        // `Some(1)` for a pruned Full/Core target -- a wire-visible claim ("I myself
        // emitted a matching grade-1 echo for exactly this `(C_u,T_u)`") that peers
        // count verbatim toward `ReadyOK`'s `origin_ones >= f+1` threshold in
        // `recheck_completion_and_direct`, asserted with no evidence whatsoever and even
        // when our own E_i(u) had named a different payload. Since `views` holds no entry
        // for a pruned `u`, falling through leaves `own_echo` as `None`, so the bit comes
        // out `Some(0)` -- the truthful answer, and the one that simply does not
        // contribute to the threshold. `meta_ok` already declines pruned targets, so a
        // remote carrier never reaches here; this honest default is defence in depth.
        let own_echo = self
            .views
            .get(&u)
            .and_then(|s| s.echo_statements.get(&self.name));
        let is_one = match entry {
            ResolutionEntry::Full(_, c, t) => {
                matches!(own_echo, Some(EchoStatement::Graded(p, _, 1, _)) if p.c() == c && p.t() == t)
            }
            ResolutionEntry::Core(_, c, t) => {
                matches!(own_echo, Some(EchoStatement::Graded(p, _, _, _)) if p.c() == c && p.t() == t)
            }
            ResolutionEntry::Skip(_) => return None,
        };
        Some(if is_one { 1 } else { 0 })
    }

    /// R2 fallback's `min(t + Δ, e_i + θE)` deadline: if echo is still pending and
    /// `fixed = B`, broadcast a grade-0 echo (if `CoreOK_i(C) ∧ TryMetaOK_i(w,M)` holds
    /// -- PHASE6-SPEC.md §2: "Phase 4's fallback checked CoreOK only -- correct for
    /// M=∅, must change now"; PHASE8 renames the conjunct `TryMetaOK`, see
    /// `try_meta_ok`'s doc comment) or an echo-skip.
    pub fn on_echo_fallback_timer(
        &mut self,
        view: View,
        lm: &mut LaneManager,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
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
        if Self::core_ok(proposal.c(), lm) && self.try_meta_ok(view, proposal.entries(), lm) {
            let origin = self.compute_origin(proposal.entries());
            self.count_echo_statement(
                view,
                self.name,
                EchoStatement::Graded(Arc::clone(&proposal), digest, 0, origin.clone()),
            );
            // See `recheck_gate`'s matching comment: one deep clone here, same total
            // count as before Efficiency Item 3.
            effects.push(Effect::BroadcastEcho(
                self.build_echo_out(&proposal, 0, origin),
            ));
        } else {
            self.count_echo_statement(view, self.name, EchoStatement::Skip);
            effects.push(Effect::BroadcastEchoSkip(view));
        }
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        // PHASE8: this branch's own echo may be Graded (no-op for the reason
        // `recheck_gate`'s matching comment gives) or Skip (this DOES grow
        // `echo_skip_count` -- either way, rechecking here is correct and mirrors
        // `recheck_fastseal_trigger`'s own call-site set).
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// R2's absolute `e_i + θE` deadline: if echo is still pending (either no active
    /// well-formed fixed proposal, or `MetaOK` never became true in time), broadcast an
    /// echo-skip.
    pub fn on_echo_absolute_timer(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
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
        // PHASE8: this IS our own ECHO-SKIP for `view` -- grows `echo_skip_count(view)`.
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// A counted `VantageEcho`. N9 hygiene: `grade` must be exactly 0 or 1 (§2's
    /// "0|1") -- a malformed grade byte is dropped outright, never counted (folding it
    /// into the grade-0 tally would silently treat a malformed message as a legal
    /// one). The `origin` bit travels verbatim (it's the SENDER's own annotation, never
    /// recomputed here).
    pub fn on_echo(&mut self, echo: Echo, rep: &mut Repairer) -> Vec<Effect> {
        // AVAIL-ECHO-SPEC.md: surface the piggybacked availability claim as an effect
        // before the echo is consumed. Emitted UNCONDITIONALLY of any local flag, exactly
        // as `digest_statements` reception is (see that flag's own doc comment): a peer
        // may send claims whether or not this party emits them, and refusing to count a
        // first-hand statement we received would be a liveness bug, not a safety one.
        // Resolution is positional and pure -- `manifest_refs` plus bit tests, no
        // `BlockCache` -- so the engine stays free of availability state; the linkage
        // check and the crediting itself happen in `VantageCore::execute`.
        let mut effects = Vec::new();
        if let Some(claim) = &echo.avail {
            let refs = crate::vantage::claim::manifest_refs(&echo.proposal);
            let at_tip: std::collections::HashSet<Digest> = refs
                .iter()
                .enumerate()
                .filter(|(j, _)| claim.is_at_tip(*j))
                .map(|(_, r)| r.2.clone())
                .collect();
            let resolved: Vec<(BlockRef, bool)> = claim
                .resolve(&refs)
                .into_iter()
                .map(|r| {
                    let tip = at_tip.contains(&r.2);
                    (r, tip)
                })
                .collect();
            if !resolved.is_empty() {
                effects.push(Effect::AvailClaimed(echo.sender, resolved));
            }
        }
        effects.extend(self.on_echo_any(EchoOut::Single(echo), rep));
        effects
    }

    /// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): the `EchoBatch` counterpart of
    /// `on_echo`, delegated to the same shared `on_echo_any` core.
    pub fn on_echo_batch(&mut self, echo: EchoBatch, rep: &mut Repairer) -> Vec<Effect> {
        self.on_echo_any(EchoOut::Batch(echo), rep)
    }

    fn on_echo_any(&mut self, echo: EchoOut, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if echo.grade() > 1 {
            return effects;
        }
        let view = echo.proposal_view();
        if self.is_pruned(view) {
            return effects;
        }
        let sender = echo.sender();
        let grade = echo.grade();
        let origin = echo.origin_vec();
        // Efficiency Item 1: reuse the per-view digest cache instead of always
        // recomputing the proposal's digest.
        let (proposal, digest) = self.canonical_proposal(view, echo.into_proposal_out());
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
        // PHASE8: a remote statement here may itself be an ECHO-SKIP (grows
        // `echo_skip_count`) or Graded (a guaranteed no-op for this trigger) --
        // rechecking unconditionally mirrors `recheck_fastseal_trigger`'s own
        // call-site set, which does not distinguish the two either.
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// A counted `VantageEchoSkip`.
    pub fn on_echo_skip(&mut self, view: View, sender: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if !self.count_echo_statement(view, sender, EchoStatement::Skip) {
            return effects;
        }
        // Names no B, so it never contributes to R3's per-B echo tally -- only to the
        // fast-seal non-matching count.
        self.recheck_lock_release(view);
        effects.extend(self.recheck_fastseal_trigger(view));
        // PHASE8: this IS a (remote) ECHO-SKIP for `view` -- grows `echo_skip_count`.
        effects.extend(self.recheck_skip_vote_trigger(view));
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
        if self.is_pruned(view) {
            return false;
        }
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
        if self.is_pruned(view) {
            return false;
        }
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
        if self.is_pruned(view) {
            return effects;
        }
        if self.state_mut(view).ready_sent {
            return effects;
        }
        // Efficiency Item 3: the tally's proposal slot is `Arc<ProposalOut>` -- both
        // `or_insert_with` below (once per distinct digest re-derived on every call)
        // and the `.clone()` calls further down are now refcount bumps, not deep
        // clones of `c`/`t`/`m`. PHASE7: the 4th slot generalizes from a single
        // origin-ones `usize` to a `Vec<usize>`, one counter per `M` position,
        // monotonically incremented as more echoes are counted -- never shrinks, so
        // `ReadyOK`'s AND-over-positions below only ever becomes true as the counted
        // set grows (the same monotonicity today's single-counter version had).
        let mut tallies: HashMap<Digest, (Arc<ProposalOut>, Stake, Stake, Vec<usize>)> =
            HashMap::new();
        if let Some(state) = self.views.get(&view) {
            for (sender, stmt) in &state.echo_statements {
                if let EchoStatement::Graded(p, d, g, origin) = stmt {
                    let stake = self.committee.stake(sender);
                    let n_entries = p.entries().len();
                    let entry = tallies
                        .entry(d.clone())
                        .or_insert_with(|| (Arc::clone(p), 0, 0, vec![0; n_entries]));
                    if *g == 1 {
                        entry.1 += stake;
                    } else {
                        entry.2 += stake;
                    }
                    for (i, bit) in origin.iter().enumerate() {
                        if *bit == Some(1) {
                            entry.3[i] += 1;
                        }
                    }
                }
            }
        }
        for (digest, (proposal, g1, g0, origin_ones)) in tallies {
            if g1 + g0 < self.quorum {
                continue;
            }
            // PHASE6-SPEC.md §3 `ReadyOK`, generalized by PHASE7 to a per-position
            // guard: for EACH full/core entry, require >= f+1 (party count) counted
            // proposal echoes for THIS proposal with origin = 1 AT THAT ENTRY'S
            // POSITION; skip entries (and, trivially, an empty `M`) always pass.
            // Degenerates exactly to today's single check for 0/1 entries. PHASE8:
            // `formed_batch` now requires every `Batch` entry to be `Skip`, so the
            // `Full`/`Core` arm below is reachable only through `ProposalOut::Single`
            // (0/1 entries) -- a runtime invariant enforced there, not something the
            // type system encodes, so the per-position generality here is kept rather
            // than special-cased.
            let ready_ok = proposal
                .entries()
                .iter()
                .enumerate()
                .all(|(i, entry)| match entry {
                    ResolutionEntry::Full(..) | ResolutionEntry::Core(..) => {
                        origin_ones[i] >= self.f_plus_1_parties
                    }
                    ResolutionEntry::Skip(_) => true,
                });
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
            // The wire type still carries an owned proposal: exactly one deep clone
            // here, same total deep-clone count as before Efficiency Item 3
            // (previously the census `.clone()` above was the deep clone and this
            // value was moved; now the census clone is free and this is the one
            // remaining deep clone).
            effects.push(Effect::BroadcastReady(
                self.build_ready_out(&proposal, grade),
            ));
            effects.extend(self.recheck_completion_and_direct(view, rep));
            break; // one ready-stage statement per view, ever
        }
        effects
    }

    /// R3's absolute `e_i + θR` deadline: if we still haven't gone ready by now,
    /// broadcast a no-ready.
    pub fn on_ready_timer(&mut self, view: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if self.state_mut(view).ready_sent {
            return effects;
        }
        self.state_mut(view).ready_sent = true;
        self.count_ready_statement(view, self.name, ReadyStatement::NoReady);
        effects.extend(self.wish_effect(view, ResponseStage::Ready));
        effects.push(Effect::BroadcastNoReady(view));
        // PHASE8: this is the ONE and only site where our own R_i(view) becomes
        // NoReady -- "after durably emitting NO-READY(u)" (par:skip-seal). The
        // echo-skip-quorum conjunct cannot have changed here, but the own-noready
        // conjunct just did, so the vote may now be immediately due if the quorum was
        // already counted.
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// A counted `VantageReady`.
    pub fn on_ready(&mut self, ready: Ready, rep: &mut Repairer) -> Vec<Effect> {
        self.on_ready_any(ReadyOut::Single(ready), rep)
    }

    /// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph): the `ReadyBatch` counterpart of
    /// `on_ready`, delegated to the same shared `on_ready_any` core.
    pub fn on_ready_batch(&mut self, ready: ReadyBatch, rep: &mut Repairer) -> Vec<Effect> {
        self.on_ready_any(ReadyOut::Batch(ready), rep)
    }

    fn on_ready_any(&mut self, ready: ReadyOut, rep: &mut Repairer) -> Vec<Effect> {
        let view = ready.proposal_view();
        if self.is_pruned(view) {
            return Vec::new();
        }
        let sender = ready.sender();
        let grade = ready.grade();
        // Efficiency Item 1: reuse the per-view digest cache instead of always
        // recomputing the proposal's digest.
        let (proposal, digest) = self.canonical_proposal(view, ready.into_proposal_out());
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
        if self.is_pruned(view) {
            return Vec::new();
        }
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
        if self.is_pruned(view) {
            return effects;
        }
        // Efficiency Item 3: see `recheck_ready`'s matching comment -- `Arc::clone`
        // instead of a deep proposal clone on every re-scan.
        let mut tallies: HashMap<Digest, (Arc<ProposalOut>, Stake, Stake, Stake)> = HashMap::new();
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
                let c = proposal.c().clone();
                let t = proposal.t().clone();
                self.state_mut(view).completed = Some((c.clone(), t.clone()));
                for r in c.iter().chain(aux_refs_entries(proposal.entries()).iter()) {
                    effects.extend(rep.authorize(r.clone()));
                }
                // PHASE6-SPEC.md §5: the FIRST genuine R4 completion with M != ∅
                // triggers a completion report (fast-seal alone never does -- fastseal
                // only ever produces `directed`/`sealed`, never `completed`, so this
                // site -- and only this site -- is the right hook). `Effect::
                // CompletionReportable` carries an owned proposal (a downstream
                // effect consumer, not internal state), so this one deep clone is
                // required and unchanged from before -- it only ever runs once per
                // view, on the transition into `completed`.
                if !proposal.entries().is_empty() {
                    effects.push(Effect::CompletionReportable(view, (*proposal).clone()));
                }
                effects.push(Effect::Completed(view, c, t));
            }
            if self.state_mut(view).directed.is_none() {
                if g1_stake >= self.quorum {
                    let outcome = Outcome::Full(proposal.c().clone(), proposal.t().clone());
                    self.state_mut(view).directed = Some(outcome.clone());
                    self.try_seal(view, outcome, "direct_full", &mut effects);
                } else if g0_stake >= self.quorum {
                    let outcome = Outcome::Core(proposal.c().clone());
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
        if self.is_pruned(view) {
            return;
        }
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
    fn record_lock(&mut self, view: View, proposal: &ProposalOut, digest: &Digest) {
        if self.is_pruned(view) {
            return;
        }
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
            let outcome = Outcome::Full(lock.proposal.c().clone(), lock.proposal.t().clone());
            self.try_seal(view, outcome, "fast_full", &mut effects);
        }
        effects
    }
}

// =====================================================================================
// signature-free.tex §8.3 "Digest-named AGB statements" (`Parameters::
// digest_statements`) -- the reception-side translation layer. `AgbEngine` above is
// UNCHANGED by this section: no by-value rule, threshold, or one-shot in it was
// touched to add this (the only edits to `AgbEngine` itself are the three read-only
// accessors just above -- `sid`/`committee`/`fixed_proposal` -- which expose existing
// state and mutate nothing). `DigestStatements` is a thin adapter that sits strictly
// BETWEEN the wire and the engine: it never maintains a parallel counted census, and
// it is the only place a digest-named statement is ever constructed or consumed.
//
// CROSS-ENCODING ONE-SHOT (the module's central correctness property): a sender's
// digest-named and by-value statement for the same (view, statement kind) occupy the
// SAME one-shot slot because this type never counts anything itself -- the moment a
// body is verified, a buffered statement is drained by resynthesizing the exact
// by-value `Echo`/`Ready` value and calling `AgbEngine::on_echo`/`on_ready` UNCHANGED.
// Those methods' own `count_echo_statement`/`count_ready_statement` (first received
// wins, keyed by (view, sender) only -- never by encoding) is the SAME dedup a real
// by-value statement goes through; a digest-named statement that resolves immediately
// (body already held) is fed through the identical call, with no buffering at all.
// So whichever encoding from a given sender reaches that call FIRST wins the slot,
// and the second is a guaranteed no-op there -- exactly the by-value protocol's
// existing single-slot guarantee, now shared across two wire encodings instead of one.
//
// THRESHOLD DISCIPLINE (no READY/completion/seal before the body is held): automatic
// by construction, not by a separate check -- a buffered statement is kept ENTIRELY
// in `buffered_echo`/`buffered_ready` below, never touching `AgbEngine::on_echo`/
// `on_ready` (and therefore never touching any census, tally, or threshold check
// inside them) until `drain` resynthesizes and feeds it, which happens only once a
// verified body is in hand. Nothing in this type counts toward anything on its own.
//
// BOUND (`<= n digests per view`): `buffered_echo`/`buffered_ready` are keyed
// (View -> PublicKey -> ..), one entry per (view, sender) -- first-hand dedup,
// mirroring `AgbEngine`'s own one-shot rule exactly (`Entry::or_insert` on an
// already-populated sender is a no-op). There are only `n` possible senders, so each
// map holds at most `n` entries per view regardless of how many DISTINCT
// (Byzantine-fabricated) digests are named across them -- and since `known_bodies`/
// `pending_fetch` are keyed by exactly the digests those <= n statements name, both
// hold at most `n` entries per view too (in the worst case every sender names its own
// distinct digest; in the honest-majority case there is exactly one).
// =====================================================================================

/// clippy::type_complexity: named factor-outs for `DigestStatements`'s two buffered-
/// statement maps -- the payload a sender's one-shot buffered ECHO/READY digest
/// statement carries (the digest it named, plus its own grade/origin and, since
/// AVAIL-ECHO-SPEC.md, its availability claim), keyed `View -> PublicKey -> ..` in the
/// struct itself.
///
/// The claim MUST be buffered alongside the rest: its lane indices address the BODY's
/// reference vector, so a statement that arrives before its body cannot be resolved yet,
/// and dropping the claim here would silently lose every acknowledgment that raced its
/// proposal -- the common case for a node that is behind, which is exactly when
/// availability matters. Carried along the existing `buffered_echo -> known_bodies` path
/// rather than through a second stash of its own.
type BufferedEcho = (
    Digest,
    u8,
    Option<u8>,
    Option<crate::vantage::claim::AvailClaim>,
);
type BufferedReady = (Digest, ReadyGrade);

/// The translation layer's per-view/per-(view,digest) state -- see the module
/// doc comment above for the correctness argument. All new per-view state lives in
/// `BTreeMap`/`BTreeSet`s keyed by `View` (or a `(View, ..)` tuple), covered by the
/// standing `split_off`-based GC (`gc_below`) -- no `retain`, same discipline as
/// `AgbEngine`/`control::ControlLog`'s own GC.
pub struct DigestStatements {
    /// Per view, per sender: this sender's one-shot buffered ECHO digest statement --
    /// the digest it named, its grade, and its origin bit -- not yet drained because
    /// no verified body matching that digest was held at arrival time.
    buffered_echo: BTreeMap<View, BTreeMap<PublicKey, BufferedEcho>>,
    /// Per view, per sender: this sender's one-shot buffered READY digest statement.
    buffered_ready: BTreeMap<View, BTreeMap<PublicKey, BufferedReady>>,
    /// Verified `(view, digest)` bodies -- populated once, from EITHER a matching
    /// `AgbEngine::fixed_proposal` (received by value, via the ordinary propose path)
    /// or an accepted `VantageBodyServe` -- so a LATER digest statement naming an
    /// already-verified pair is fed immediately, with no repeated fetch/verify.
    known_bodies: BTreeMap<(View, Digest), Arc<ViewProposal>>,
    /// Outstanding fetches this party has issued, mapped to the `Instant` of the last
    /// fan-out. Mirrors `control::ControlLog::pending_fetch`'s identical role, with
    /// an `Instant` clock standing in for that type's `Round` one -- an AGB view has
    /// no per-view round counter of its own to retry against.
    pending_fetch: BTreeMap<(View, Digest), FetchState>,
    /// Per-requester serve dedup on the answering side: at most one `VantageBodyServe`
    /// per (view, digest, requester), ever. Mirrors `control::ControlLog::
    /// fetch_answered` exactly.
    fetch_answered: BTreeSet<(View, Digest, PublicKey)>,
    min_live_view: View,
    /// Re-fan an unanswered fetch after this long. Mirrors `control::ControlLog::
    /// FETCH_RETRY_ROUNDS` / `simpleit::engine::CutEngine::FETCH_RETRY_ROUNDS` (both
    /// "8" of their own retry clock's units) -- stated directly as a `Duration` (8
    /// base delay units, Δ) since AGB views carry no round counter of their own.
    fetch_retry_interval: Duration,
    /// `None` in most unit tests, which don't assert on metrics -- mirrors `AgbEngine`/
    /// `Repairer`'s own optional-handle convention.
    metrics: Option<Arc<Metrics>>,
}

impl DigestStatements {
    pub fn new(delta_ms: u64) -> Self {
        Self {
            buffered_echo: BTreeMap::new(),
            buffered_ready: BTreeMap::new(),
            known_bodies: BTreeMap::new(),
            pending_fetch: BTreeMap::new(),
            fetch_answered: BTreeSet::new(),
            min_live_view: 1,
            fetch_retry_interval: Duration::from_millis(delta_ms) * 8,
            metrics: None,
        }
    }

    /// Attach counters (production wiring only -- most unit tests skip this).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn is_pruned(&self, view: View) -> bool {
        view < self.min_live_view
    }

    /// GC: prune every per-view/per-(view,digest) map below `floor`. Mirrors
    /// `AgbEngine::gc_below`/`control::ControlLog::gc_below`'s identical `split_off`
    /// shape -- `Digest::default()`/`PublicKey::default()` are the same "smallest
    /// possible key at this view" sentinels `ControlLog::gc_below` already uses to
    /// `split_off` a `BTreeMap`/`BTreeSet` keyed by a `(View, ..)` tuple.
    pub fn gc_below(&mut self, floor: View) {
        if floor <= self.min_live_view {
            return;
        }
        self.buffered_echo = self.buffered_echo.split_off(&floor);
        self.buffered_ready = self.buffered_ready.split_off(&floor);
        self.known_bodies = self.known_bodies.split_off(&(floor, Digest::default()));
        self.pending_fetch = self.pending_fetch.split_off(&(floor, Digest::default()));
        self.fetch_answered =
            self.fetch_answered
                .split_off(&(floor, Digest::default(), PublicKey::default()));
        self.min_live_view = floor;
    }

    // ---------------------------------------------------------- resolving a body

    /// A verified body for `(view, digest)`, if this party holds one -- either
    /// already memoized in `known_bodies` (from an earlier verification, by value or
    /// via serve) or newly recognized against `AgbEngine::fixed_proposal` (received
    /// by value, via the ordinary propose path), in which case it is memoized into
    /// `known_bodies` here too, so a LATER `VantageBodyFetch` for the same pair
    /// answers from this same lookup without re-deriving anything (see
    /// `on_body_fetch`, which calls this same accessor).
    ///
    /// Only `ProposalOut::Single` can ever match: a `Batch` fixed proposal's digest
    /// lives in a disjoint, domain-separated hash space (`ProposalOut::digest`), so a
    /// `Single`-shaped `EchoDigest`/`ReadyDigest` can never legitimately name it --
    /// digest-named statements are a `ViewProposal` (Single)-only encoding by
    /// construction (see this module's own doc comment; PHASE7's batched `M` is an
    /// orthogonal optimization, out of scope here).
    fn resolve_body(
        &mut self,
        view: View,
        digest: &Digest,
        agb: &AgbEngine,
    ) -> Option<Arc<ViewProposal>> {
        let key = (view, digest.clone());
        if let Some(p) = self.known_bodies.get(&key) {
            return Some(Arc::clone(p));
        }
        let (fixed, fixed_digest) = agb.fixed_proposal(view)?;
        if fixed_digest != *digest {
            return None;
        }
        let ProposalOut::Single(vp) = fixed.as_ref() else {
            return None;
        };
        let arc = Arc::new(vp.clone());
        self.known_bodies.insert(key, Arc::clone(&arc));
        Some(arc)
    }

    // ---------------------------------------------------------------------- draining

    /// Body for `(view, digest)` just became verified-held -- drain every statement
    /// buffered under EXACTLY this pair through the UNCHANGED by-value `on_echo`/
    /// `on_ready` (the cross-encoding one-shot property -- see this module's own doc
    /// comment). Buffered entries naming a DIFFERENT digest for the same view are
    /// left untouched (still pending their own body). Clears the matching
    /// `pending_fetch` entry, if any.
    fn drain(
        &mut self,
        view: View,
        digest: &Digest,
        body: &Arc<ViewProposal>,
        agb: &mut AgbEngine,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.pending_fetch.remove(&(view, digest.clone()));

        let mut echo_bucket_empty = false;
        if let Some(senders) = self.buffered_echo.get_mut(&view) {
            #[allow(clippy::type_complexity)]
            let due: Vec<(
                PublicKey,
                u8,
                Option<u8>,
                Option<crate::vantage::claim::AvailClaim>,
            )> = senders
                .iter()
                .filter(|(_, (d, _, _, _))| d == digest)
                .map(|(s, (_, g, o, a))| (*s, *g, *o, a.clone()))
                .collect();
            for (sender, grade, origin, avail) in due {
                senders.remove(&sender);
                // `wish` is irrelevant here: it was already absorbed at reception
                // time (`VantageCore::dispatch_inbound`'s `Inbound::EchoDigest` arm
                // calls `Pacemaker::on_wish` unconditionally, whether or not this
                // statement ends up buffered), and `AgbEngine::on_echo`/`on_ready`
                // never read the field anyway (see `on_echo_any`'s own body).
                let echo = Echo {
                    proposal: (**body).clone(),
                    grade,
                    sender,
                    wish: 0,
                    origin,
                    // The claim was buffered with the statement precisely so it survives
                    // to here: its lane indices address THIS body's reference vector.
                    avail,
                };
                effects.extend(agb.on_echo(echo, rep));
            }
            echo_bucket_empty = senders.is_empty();
        }
        if echo_bucket_empty {
            self.buffered_echo.remove(&view);
        }

        let mut ready_bucket_empty = false;
        if let Some(senders) = self.buffered_ready.get_mut(&view) {
            let due: Vec<(PublicKey, ReadyGrade)> = senders
                .iter()
                .filter(|(_, (d, _))| d == digest)
                .map(|(s, (_, g))| (*s, *g))
                .collect();
            for (sender, grade) in due {
                senders.remove(&sender);
                let ready = Ready {
                    proposal: (**body).clone(),
                    grade,
                    sender,
                    wish: 0,
                };
                effects.extend(agb.on_ready(ready, rep));
            }
            ready_bucket_empty = senders.is_empty();
        }
        if ready_bucket_empty {
            self.buffered_ready.remove(&view);
        }
        effects
    }

    // ------------------------------------------------------------------- buffering

    /// Records `sender`'s first-hand digest-named ECHO for `view` (one-shot: mirrors
    /// `AgbEngine::count_echo_statement`'s own per-(view,sender) dedup exactly --
    /// `Entry::or_insert` never overwrites an already-present sender), then ensures a
    /// fetch is outstanding for `(view, digest)`.
    ///
    /// Takes the whole `EchoDigest` rather than its fields one by one: adding the
    /// availability claim pushed the scalar form to 8 parameters, and every one of them
    /// already travels together on the message.
    fn buffer_echo(&mut self, msg: EchoDigest, now: Instant) -> Vec<Effect> {
        let EchoDigest {
            view,
            digest,
            grade,
            sender,
            origin,
            avail,
            ..
        } = msg;
        self.buffered_echo
            .entry(view)
            .or_default()
            .entry(sender)
            .or_insert_with(|| (digest.clone(), grade, origin, avail));
        self.ensure_fetch(view, digest, now)
    }

    fn buffer_ready(
        &mut self,
        view: View,
        digest: Digest,
        sender: PublicKey,
        grade: ReadyGrade,
        now: Instant,
    ) -> Vec<Effect> {
        self.buffered_ready
            .entry(view)
            .or_default()
            .entry(sender)
            .or_insert_with(|| (digest.clone(), grade));
        self.ensure_fetch(view, digest, now)
    }

    // ---------------------------------------------------------------- fetch/serve

    /// The union of senders currently buffered under `(view, digest)` in EITHER
    /// census -- "the statement authors buffered for that (view, digest)" (the
    /// paragraph's own retention guarantee: a correct author of a matching ECHO or
    /// READY necessarily holds the exact body it named, having validated it before
    /// ever naming it).
    fn fetch_targets(&self, view: View, digest: &Digest) -> Vec<PublicKey> {
        let mut targets: BTreeSet<PublicKey> = BTreeSet::new();
        if let Some(senders) = self.buffered_echo.get(&view) {
            targets.extend(
                senders
                    .iter()
                    .filter(|(_, (d, _, _, _))| d == digest)
                    .map(|(s, _)| *s),
            );
        }
        if let Some(senders) = self.buffered_ready.get(&view) {
            targets.extend(
                senders
                    .iter()
                    .filter(|(_, (d, _))| d == digest)
                    .map(|(s, _)| *s),
            );
        }
        targets.into_iter().collect()
    }

    /// Fan a `VantageBodyFetch` out to every currently-buffered author of `(view,
    /// digest)`, at most once per `fetch_retry_interval` for that exact pair. Mirrors
    /// `control::ControlLog::ensure_fetch` exactly: the backoff is a latch on the
    /// PAIR, not on the target set, so a sender that buffers AFTER the first attempt
    /// is still asked once the next retry re-derives the (by-then-larger) target set
    /// fresh.
    fn ensure_fetch(&mut self, view: View, digest: Digest, now: Instant) -> Vec<Effect> {
        if self.is_pruned(view) {
            return Vec::new();
        }
        let key = (view, digest.clone());
        // Width and attempt count carried forward from any previous attempt on this pair.
        let mut width = FETCH_WIDTH_START;
        let mut attempts = 0;
        match self.pending_fetch.get(&key) {
            Some(state)
                if now.saturating_duration_since(state.last) < self.fetch_retry_interval =>
            {
                return Vec::new();
            }
            Some(state) => {
                // Give up rather than keep paying full fan-out for a pair nobody answers.
                // Safe to forget entirely -- see `MAX_FETCH_ATTEMPTS`.
                if state.attempts >= MAX_FETCH_ATTEMPTS {
                    self.pending_fetch.remove(&key);
                    if let Some(metrics) = &self.metrics {
                        metrics.vantage_body_fetch_abandoned_total.inc();
                    }
                    return Vec::new();
                }
                width = state.next_width;
                attempts = state.attempts;
            }
            None => {}
        }
        // n=100 straggler fix (2026-08-08): bound `pending_fetch`. It is cleared only by
        // a successful serve/local-fix or by `gc_below`, whose floor is
        // `resolved_watermark - gc_window` -- so on a node whose resolution has stalled
        // it never prunes, and `retry_fetches` re-fans EVERY overdue entry off the 1s
        // tick. Total fetches sent are therefore QUADRATIC in stall duration: measured
        // 84,386 on a straggler versus 2,004 healthy (42x), and its own fetches name
        // views the rest of the network has already pruned, so they are answered empty
        // (network-wide answer rate 7.8% for body fetch vs 85.2% for header repair).
        //
        // Evicting the HIGHEST views is the right direction, not the lowest: resolution
        // is strictly sequential, so the lowest pending view is the one actually
        // blocking progress and the far-ahead ones are useless until it clears.
        // Dropping one is free -- `on_echo_digest`/`on_ready_digest` re-create the pair
        // on the next statement arrival, and `buffered_echo`/`buffered_ready` retain the
        // statement regardless -- so this only stops budget being spent on views the
        // node cannot use yet.
        if self.pending_fetch.len() >= MAX_PENDING_FETCH {
            while self.pending_fetch.len() >= MAX_PENDING_FETCH {
                let Some((highest, _)) = self.pending_fetch.iter().next_back() else {
                    break;
                };
                let highest = highest.clone();
                // Never evict the pair we are about to insert in favour of itself.
                if highest <= key {
                    break;
                }
                self.pending_fetch.remove(&highest);
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_body_fetch_evicted_total.inc();
                }
            }
            if self.pending_fetch.len() >= MAX_PENDING_FETCH {
                // Everything pending is at or below `key`, i.e. all of it is more
                // urgent. Skip this fetch rather than displace a blocking one.
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_body_fetch_evicted_total.inc();
                }
                return Vec::new();
            }
        }
        self.pending_fetch.insert(
            key,
            FetchState {
                last: now,
                next_width: width.saturating_mul(2),
                attempts: attempts + 1,
            },
        );
        // `fetch_targets` is deterministic (a `BTreeSet` of public keys), so truncating
        // takes a STABLE prefix: a retry that widens strictly adds peers rather than
        // re-rolling the set, and every peer asked at width w is still asked at 2w.
        let mut targets = self.fetch_targets(view, &digest);
        targets.truncate(width);
        if let Some(metrics) = &self.metrics {
            for _ in &targets {
                metrics.vantage_body_fetches_sent.inc();
            }
        }
        targets
            .into_iter()
            .map(|peer| Effect::BodyFetchTo(peer, view, digest.clone()))
            .collect()
    }

    /// An inbound digest-named ECHO (`VantageEchoDigest`) -- handled unconditionally
    /// regardless of this party's OWN `digest_statements` setting (reception always
    /// handles both encodings; see the module doc comment). N9 hygiene mirrors
    /// `AgbEngine::on_echo`'s own: a malformed grade byte (not 0/1) is dropped
    /// outright, never buffered or counted.
    ///
    /// If the named body is already held+verified, synthesizes the by-value `Echo`
    /// and feeds it straight into the UNCHANGED `AgbEngine::on_echo` -- occupying the
    /// SAME one-shot slot a by-value `VantageEcho` from this sender would. Otherwise
    /// buffers it and ensures a fetch is outstanding. Nothing here ever touches
    /// `AgbEngine`'s own census before the body is verified.
    pub fn on_echo_digest(
        &mut self,
        msg: EchoDigest,
        now: Instant,
        agb: &mut AgbEngine,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        if self.is_pruned(msg.view) || msg.grade > 1 {
            return Vec::new();
        }
        if let Some(body) = self.resolve_body(msg.view, &msg.digest, agb) {
            let echo = Echo {
                proposal: (*body).clone(),
                grade: msg.grade,
                sender: msg.sender,
                wish: msg.wish,
                origin: msg.origin,
                avail: msg.avail.clone(),
            };
            return agb.on_echo(echo, rep);
        }
        self.buffer_echo(msg, now)
    }

    /// An inbound digest-named READY (`VantageReadyDigest`) -- `on_echo_digest`'s
    /// counterpart, same discipline.
    pub fn on_ready_digest(
        &mut self,
        msg: ReadyDigest,
        now: Instant,
        agb: &mut AgbEngine,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        if self.is_pruned(msg.view) {
            return Vec::new();
        }
        if let Some(body) = self.resolve_body(msg.view, &msg.digest, agb) {
            let ready = Ready {
                proposal: (*body).clone(),
                grade: msg.grade,
                sender: msg.sender,
                wish: msg.wish,
            };
            return agb.on_ready(ready, rep);
        }
        self.buffer_ready(msg.view, msg.digest, msg.sender, msg.grade, now)
    }

    /// `Effect::Fixed(view, true)`'s hook: a proposal was just fixed BY VALUE (the
    /// ordinary propose path) -- drain any digest statements already buffered for
    /// its digest. A no-op if nothing was ever buffered for this view, or if
    /// `view`'s fixed proposal is `Reject`/absent/`Batch`-shaped (`resolve_body`'s
    /// own scope).
    pub fn on_local_fixed(
        &mut self,
        view: View,
        agb: &mut AgbEngine,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let Some((_, digest)) = agb.fixed_proposal(view) else {
            return Vec::new();
        };
        let Some(body) = self.resolve_body(view, &digest, agb) else {
            return Vec::new();
        };
        self.drain(view, &digest, &body, agb, rep)
    }

    /// A peer's `VantageBodyFetch(view, digest, requester)` -- answer with our own
    /// held, fixed `ViewProposal` if it matches exactly this digest and we haven't
    /// already answered this requester for this pair. Mirrors `control::ControlLog::
    /// on_control_fetch` (per-requester dedup; the held, verified body is the only
    /// real gate). Serves ONLY from `AgbEngine::fixed_proposal` (a body received by
    /// value) -- never from `known_bodies` (a body this party itself only ever
    /// fetched): the paragraph's retention guarantee is stated over ECHO/READY
    /// AUTHORS, and a party that never itself echoes/readies a view (which a served,
    /// non-`Fixed` body can never cause -- see `fixed_proposal`'s own doc comment)
    /// can never legitimately be selected as anyone's fetch TARGET either
    /// (`fetch_targets` draws only from buffered STATEMENT senders), so this
    /// narrower serve source answers every fetch that could ever legitimately reach
    /// this party.
    pub fn on_body_fetch(
        &mut self,
        requester: PublicKey,
        view: View,
        digest: Digest,
        agb: &AgbEngine,
    ) -> Vec<Effect> {
        if self.is_pruned(view) {
            return Vec::new();
        }
        let key = (view, digest.clone(), requester);
        if self.fetch_answered.contains(&key) {
            return Vec::new();
        }
        let Some((fixed, fixed_digest)) = agb.fixed_proposal(view) else {
            return Vec::new();
        };
        if fixed_digest != digest {
            return Vec::new();
        }
        let ProposalOut::Single(vp) = fixed.as_ref() else {
            return Vec::new();
        };
        self.fetch_answered.insert(key);
        if let Some(metrics) = &self.metrics {
            metrics.vantage_bodies_served.inc();
        }
        vec![Effect::BodyServeTo(requester, view, vp.clone())]
    }

    /// A peer's `VantageBodyServe(view, proposal)` response -- accept only a body
    /// matching an outstanding fetch of OUR OWN for exactly `(view, digest(proposal))`
    /// (the same "hash-matching a REQUESTED pair, not merely well-formed" discipline
    /// `control::ControlLog::on_control_serve`'s P6-2 fix applies -- an unsolicited or
    /// wrong-digest serve changes no state) AND well-formed (`formed`). On
    /// acceptance: memoizes it into `known_bodies` and drains every statement
    /// buffered for this exact pair through the by-value path -- WITHOUT ever calling
    /// `AgbEngine::on_propose`, so a served body creates no proposal provenance (the
    /// paragraph's own "served bytes recover the body but create neither proposal
    /// provenance nor an ECHO or READY" -- no `rho_i`/direct-receipt state is ever
    /// set; `AgbEngine::fixed_proposal` stays `None` for this view unless a genuine
    /// by-value propose independently arrives). A mismatched or malformed serve is
    /// simply dropped; the outstanding `pending_fetch` entry is left untouched, so
    /// the next periodic retry re-asks (a different holder, or the same one again).
    pub fn on_body_serve(
        &mut self,
        view: View,
        proposal: ViewProposal,
        agb: &mut AgbEngine,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        if self.is_pruned(view) || proposal.view != view {
            return Vec::new();
        }
        let digest = proposal.digest(agb.sid());
        if !self.pending_fetch.contains_key(&(view, digest.clone())) {
            return Vec::new(); // unsolicited, or answers a pair we never asked for
        }
        if !formed(
            agb.committee(),
            proposal.view,
            &proposal.c,
            &proposal.t,
            &proposal.m,
        ) {
            return Vec::new();
        }
        let body = Arc::new(proposal);
        self.known_bodies
            .insert((view, digest.clone()), Arc::clone(&body));
        self.drain(view, &digest, &body, agb, rep)
    }

    /// Periodic retry -- mirrors `VantageCore::run`'s 1s `metrics_tick`, which
    /// already drives `collect_internal_garbage`/`sample_metrics` off the same
    /// cadence: re-fan every outstanding fetch whose backoff has elapsed.
    pub fn retry_fetches(&mut self, now: Instant) -> Vec<Effect> {
        let due: Vec<(View, Digest)> = self
            .pending_fetch
            .iter()
            .filter(|(_, state)| {
                now.saturating_duration_since(state.last) >= self.fetch_retry_interval
            })
            .map(|(k, _)| k.clone())
            .collect();
        let mut effects = Vec::new();
        for (view, digest) in due {
            effects.extend(self.ensure_fetch(view, digest, now));
        }
        effects
    }

    #[cfg(test)]
    pub(crate) fn buffered_echo_count_for_test(&self, view: View) -> usize {
        self.buffered_echo.get(&view).map_or(0, |m| m.len())
    }

    #[cfg(test)]
    pub(crate) fn buffered_ready_count_for_test(&self, view: View) -> usize {
        self.buffered_ready.get(&view).map_or(0, |m| m.len())
    }

    /// Is any fetch pending for `view`? Lets the cap test assert on WHICH end eviction
    /// takes without exposing the map itself.
    #[cfg(test)]
    pub(crate) fn fetch_targets_len_for_test(&self, view: View, digest: &Digest) -> usize {
        self.fetch_targets(view, digest).len()
    }

    #[cfg(test)]
    pub(crate) fn has_pending_fetch_for_test(&self, view: View) -> bool {
        self.pending_fetch.keys().any(|(v, _)| *v == view)
    }

    #[cfg(test)]
    pub(crate) fn pending_fetch_count_for_test(&self) -> usize {
        self.pending_fetch.len()
    }

    /// Outstanding body fetches -- exported as `vantage_pending_body_fetch_len` by
    /// `VantageCore::sample_metrics`. The production counterpart of
    /// `pending_fetch_count_for_test`, which was the only way to see this before.
    pub fn pending_fetch_len(&self) -> usize {
        self.pending_fetch.len()
    }

    #[cfg(test)]
    pub(crate) fn fetch_answered_count_for_test(&self) -> usize {
        self.fetch_answered.len()
    }

    #[cfg(test)]
    pub(crate) fn known_body_for_test(&self, view: View, digest: &Digest) -> bool {
        self.known_bodies.contains_key(&(view, digest.clone()))
    }
}

#[cfg(test)]
mod recheck_window_tests {
    use super::*;

    fn set(views: &[View]) -> BTreeSet<View> {
        views.iter().copied().collect()
    }

    #[test]
    fn within_budget_returns_the_whole_set_ascending() {
        let pending = set(&[9, 3, 7]);
        assert_eq!(recheck_window(&pending, 8, 3), vec![3, 7, 9]);
        // Cursor is irrelevant whenever the set fits -- pre-budget behavior.
        assert_eq!(recheck_window(&pending, 0, 64), vec![3, 7, 9]);
    }

    #[test]
    fn over_budget_takes_exactly_budget_from_the_cursor() {
        let pending = set(&[1, 2, 3, 4, 5, 6]);
        assert_eq!(recheck_window(&pending, 3, 2), vec![3, 4]);
    }

    #[test]
    fn wraps_past_the_end_to_the_smallest_views() {
        let pending = set(&[1, 2, 3, 4, 5, 6]);
        assert_eq!(recheck_window(&pending, 5, 4), vec![5, 6, 1, 2]);
    }

    #[test]
    fn a_cursor_on_a_missing_view_starts_at_the_next_present_one() {
        let pending = set(&[10, 20, 30, 40]);
        assert_eq!(recheck_window(&pending, 21, 2), vec![30, 40]);
    }

    #[test]
    fn successive_windows_cover_every_view() {
        // Simulate `recheck_all`'s cursor advancement: every view must be scanned
        // within ceil(len/budget) consecutive calls, with no view starved.
        let pending: BTreeSet<View> = (1..=10).collect();
        let mut cursor = 4;
        let mut seen = BTreeSet::new();
        for _ in 0..4 {
            let window = recheck_window(&pending, cursor, 3);
            assert_eq!(window.len(), 3);
            cursor = window.last().unwrap().saturating_add(1);
            seen.extend(window);
        }
        assert_eq!(seen, pending, "4 windows of 3 must cover all 10 views");
    }
}
