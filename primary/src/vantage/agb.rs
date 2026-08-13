// Direct-AGB wire types and the per-view state machine.

use crate::leader::{one_based_authority, RoundRobin};
use crate::primary::View;
use crate::vantage::block::{self, BlockRef};
use crate::vantage::lanes::LaneManager;
use crate::vantage::repair::Repairer;
use crate::vantage::{Effect, Thresholds};
use config::{Committee, Stake};
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Entries are ordered strictly by author, with at most one entry per author.
pub type Manifest = Vec<BlockRef>;

/// A resolution entry targets an earlier open view.
/// Full and core entries carry both manifests; skip entries carry neither.
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

/// Wire proposal with at most one resolution entry.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ViewProposal {
    pub view: View,
    pub c: Manifest,
    pub t: Manifest,
    pub m: Option<ResolutionEntry>,
}

impl ViewProposal {
    /// Uses `blake3("view-proposal" || sid || bincode(ViewProposal))`.
    pub fn digest(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("ViewProposal always serializes");
        block::domain_hash(b"view-proposal", sid, &bytes)
    }
}

/// Wire proposal with 2..=f skip entries in strictly increasing target order.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct BatchViewProposal {
    pub view: View,
    pub c: Manifest,
    pub t: Manifest,
    pub m: Vec<ResolutionEntry>,
}

impl BatchViewProposal {
    /// Uses `blake3("view-proposal-batch" || sid || bincode(BatchViewProposal))`.
    pub fn digest(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("BatchViewProposal always serializes");
        block::domain_hash(b"view-proposal-batch", sid, &bytes)
    }
}

/// This internal proposal representation is not serialized.
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

    /// Returns resolution entries in strictly increasing target order.
    /// Single proposals contain at most one entry; batches contain 2..=f entries.
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

    pub fn formed(&self, committee: &Committee) -> bool {
        match self {
            Self::Single(p) => formed(committee, p.view, &p.c, &p.t, &p.m),
            Self::Batch(p) => formed_batch(committee, p.view, &p.c, &p.t, &p.m),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Echo {
    pub proposal: ViewProposal,
    /// The wire grade is 0 or 1.
    pub grade: u8,
    pub sender: PublicKey,
    /// The sender's wish watermark, which is excluded from proposal identity.
    pub wish: View,
    /// The origin bit is `None` for an empty resolution or a skip entry.
    pub origin: Option<u8>,
    /// Claims are positional over the proposal's reference vector.
    #[serde(default)]
    pub avail: Option<crate::vantage::claim::AvailClaim>,
}

/// A digest-named echo omits the proposal body but preserves the statement fields.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoDigest {
    pub view: View,
    pub digest: Digest,
    pub grade: u8,
    pub sender: PublicKey,
    pub wish: View,
    pub origin: Option<u8>,
    /// The claim remains buffered until the referenced proposal body is verified.
    #[serde(default)]
    pub avail: Option<crate::vantage::claim::AvailClaim>,
}

impl Echo {
    /// Converts this echo to the digest-named wire representation.
    pub fn to_digest(&self, sid: &Digest) -> EchoDigest {
        EchoDigest {
            view: self.proposal.view,
            digest: self.proposal.digest(sid),
            grade: self.grade,
            sender: self.sender,
            wish: self.wish,
            origin: self.origin,
            // Preserve the claim across body retrieval.
            avail: self.avail.clone(),
        }
    }
}

/// A batch echo omits origin bits because all batch entries are skips.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoBatch {
    pub proposal: BatchViewProposal,
    pub grade: u8,
    pub sender: PublicKey,
    pub wish: View,
}

/// This internal echo representation is not serialized.
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

/// `One` requires a quorum of grade-1 echoes and `Zero` requires a quorum of
/// grade-0 echoes; all other quorum combinations are `Mix`.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ReadyGrade {
    Zero,
    One,
    Mix,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Ready {
    pub proposal: ViewProposal,
    pub grade: ReadyGrade,
    pub sender: PublicKey,
    pub wish: View,
}

/// A digest-named ready omits the proposal body and has no origin bit.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ReadyDigest {
    pub view: View,
    pub digest: Digest,
    pub grade: ReadyGrade,
    pub sender: PublicKey,
    pub wish: View,
}

impl Ready {
    /// Converts this ready to the digest-named wire representation.
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ReadyBatch {
    pub proposal: BatchViewProposal,
    pub grade: ReadyGrade,
    pub sender: PublicKey,
    pub wish: View,
}

/// This internal ready representation is not serialized.
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Full(Manifest, Manifest),
    Core(Manifest),
    Skip,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimerKind {
    /// The fallback deadline is `min(t + Δ, e_i + θE)` after direct proposal receipt.
    EchoFallback,
    /// The absolute echo deadline is `e_i + θE`.
    EchoAbsolute,
    /// The absolute ready deadline is `e_i + θR`.
    ReadyAbsolute,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseStage {
    Echo,
    Ready,
}

/// Validates manifest ordering, stake membership, hash uniqueness, and resolution bounds.
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
        return false; // Hashes must be unique across C and T.
    }
    if let Some(entry) = m {
        if !formed_entry(committee, view, entry) {
            return false;
        }
    }
    true
}

/// Validates a skip-only batch with 2..=f entries in strictly increasing target order.
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
        // Batch resolution entries are skip-only.
        if !matches!(entry, ResolutionEntry::Skip(_)) {
            return false;
        }
        if !formed_entry(committee, view, entry) {
            return false;
        }
        let u = entry.target_view();
        if let Some(p) = prev {
            if u <= p {
                return false; // Batch targets must increase strictly.
            }
        }
        prev = Some(u);
    }
    true
}

/// Returns `max(1, f)` for the committee.
pub fn batch_cap(committee: &Committee) -> usize {
    Thresholds::from_party_count(committee.size())
        .f_plus_1_parties
        .saturating_sub(1)
        .max(1)
}

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
                return false; // Authors must increase strictly.
            }
        }
        last = Some(*author);
    }
    true
}

fn distinct_hashes(m1: &Manifest, m2: &Manifest) -> bool {
    let mut hashes = std::collections::HashSet::with_capacity(m1.len() + m2.len());
    for (_, _, h) in m1.iter().chain(m2.iter()) {
        if !hashes.insert(h) {
            return false;
        }
    }
    true
}

/// Returns manifests from non-skip resolution entries.
/// These references are authorized with the carrying proposal.
fn aux_refs_entries(entries: &[ResolutionEntry]) -> impl Iterator<Item = &BlockRef> {
    entries.iter().flat_map(|entry| {
        let (c, t): (&[BlockRef], &[BlockRef]) = match entry {
            ResolutionEntry::Full(_, c, t) | ResolutionEntry::Core(_, c, t) => (c, t),
            ResolutionEntry::Skip(_) => (&[], &[]),
        };
        c.iter().chain(t)
    })
}

/// Returns the round-robin proposer using the committee's sorted authority order.
pub fn proposer(committee: &Committee, view: View) -> PublicKey {
    debug_assert!(view >= 1, "proposer(v) is only defined for v >= 1");
    one_based_authority(committee, view)
}

#[derive(Clone, Debug)]
enum Fixed {
    Unset,
    Reject,
    Proposal(Arc<ProposalOut>, Digest),
}

#[derive(Clone, Debug)]
enum EchoStatement {
    Graded(Arc<ProposalOut>, Digest, u8, Vec<Option<u8>>),
    Skip,
}

#[derive(Clone, Debug)]
struct EchoTally {
    proposal: Arc<ProposalOut>,
    grade_one: Stake,
    grade_zero: Stake,
    grade_one_parties: usize,
    grade_zero_parties: usize,
    origin_ones: Vec<usize>,
}

#[derive(Clone, Debug)]
struct ReadyTally {
    proposal: Arc<ProposalOut>,
    any: Stake,
    grade_one: Stake,
    grade_zero: Stake,
}

#[derive(Clone, Debug)]
enum ReadyStatement {
    Graded(Arc<ProposalOut>, Digest, ReadyGrade),
    NoReady,
}

#[derive(Clone, Debug)]
struct Lock {
    proposal: ProposalOut,
    digest: Digest,
    /// Once inactive, the lock remains inactive.
    active: bool,
}

/// The resolution stance persists for each target view.
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
    /// The initial READY-stage response has been emitted.
    ready_sent: bool,
    /// The local initial response was READY-mix and may still refine to a
    /// homogeneous grade. Provisional mix cannot authorize resolution metadata.
    ready_mix_open: bool,
    completed: Option<(Manifest, Manifest)>,
    directed: Option<Outcome>,
    sealed: Option<Outcome>,
    fastsealed: bool,
    active: bool,
    entered: bool,
    entry_instant: Option<Instant>,
    first_proposal_instant: Option<Instant>,
    echo_statements: HashMap<PublicKey, EchoStatement>,
    echo_tallies: HashMap<Digest, EchoTally>,
    echo_skip_parties: usize,
    ready_statements: HashMap<PublicKey, ReadyStatement>,
    ready_tallies: HashMap<Digest, ReadyTally>,
    ready_non_grade_one_parties: usize,
    noready_parties: usize,
    lock: Option<Lock>,
    /// Caches canonical proposal bodies and digests by content within this view.
    digest_cache: Vec<(Arc<ProposalOut>, Digest)>,
    stance: Stance,
    /// Once accepted, auxiliary metadata remains accepted for the carrying view.
    aux_accepted: bool,
    /// Stores at most one skip vote per sender.
    skip_vote_statements: HashSet<PublicKey>,
    /// Prevents duplicate skip-seal submissions.
    skip_sealed: bool,
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            fixed: Fixed::Unset,
            echo_sent: false,
            ready_sent: false,
            ready_mix_open: false,
            completed: None,
            directed: None,
            sealed: None,
            fastsealed: false,
            active: false,
            entered: false,
            entry_instant: None,
            first_proposal_instant: None,
            echo_statements: HashMap::new(),
            echo_tallies: HashMap::new(),
            echo_skip_parties: 0,
            ready_statements: HashMap::new(),
            ready_tallies: HashMap::new(),
            ready_non_grade_one_parties: 0,
            noready_parties: 0,
            lock: None,
            digest_cache: Vec::new(),
            stance: Stance::Free,
            aux_accepted: false,
            skip_vote_statements: HashSet::new(),
            skip_sealed: false,
        }
    }
}

pub struct AgbEngine {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    delta: Duration,
    n: usize,
    f_plus_1_parties: usize,
    /// Skip quorums count parties: `Q = 2f + 1`.
    two_f_plus_1_parties: usize,
    quorum: Stake,
    proposers: RoundRobin,
    views: BTreeMap<View, ViewState>,
    pending_gate: BTreeSet<View>,
    recheck_cursor: View,
    /// Views below this floor are treated as pruned.
    min_live_view: View,
    metrics: Option<Arc<Metrics>>,
}

const RECHECK_BUDGET: usize = 64;

pub(crate) const MAX_PENDING_FETCH: usize = 1_024;

pub(crate) const FETCH_WIDTH_START: usize = 2;

const MAX_FETCH_ATTEMPTS: u32 = 4;

#[derive(Clone, Copy, Debug)]
struct FetchState {
    last: Instant,
    next_width: usize,
    attempts: u32,
}

/// Returns at most `budget` pending views from `cursor` upward, then from the smallest view.
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
        let proposers = RoundRobin::new(&committee);
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
            proposers,
            views: BTreeMap::new(),
            pending_gate: BTreeSet::new(),
            recheck_cursor: 1,
            min_live_view: 1,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn theta_echo(&self) -> Duration {
        self.delta * 3
    }

    pub fn theta_ready(&self) -> Duration {
        self.delta * 4
    }

    pub fn proposer(&self, view: View) -> PublicKey {
        self.proposers.one_based(view)
    }

    /// Computes the wish target immediately before an echo or ready broadcast.
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

    pub fn pending_gate_len(&self) -> usize {
        self.pending_gate.len()
    }

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

    /// Returns true for pruned views and views with local state.
    pub fn has_any_state(&self, view: View) -> bool {
        self.is_pruned(view) || self.views.contains_key(&view)
    }

    /// Counts all ready-stage statements, including no-ready statements.
    pub fn ready_stage_total(&self, view: View) -> usize {
        self.views
            .get(&view)
            .map_or(0, |s| s.ready_statements.len())
    }

    /// Counts no-ready, grade-0, and mixed ready statements.
    pub fn ready_stage_non_grade1_count(&self, view: View) -> usize {
        self.views
            .get(&view)
            .map_or(0, |s| s.ready_non_grade_one_parties)
    }

    pub fn noready_count(&self, view: View) -> usize {
        self.views.get(&view).map_or(0, |s| s.noready_parties)
    }

    /// Counts grade-1 echoes for the exact `(c, t)` payload.
    pub fn echo_grade1_count_for(&self, view: View, c: &Manifest, t: &Manifest) -> usize {
        self.views.get(&view).map_or(0, |state| {
            state
                .echo_tallies
                .values()
                .filter(|tally| tally.proposal.c() == c && tally.proposal.t() == t)
                .map(|tally| tally.grade_one_parties)
                .sum()
        })
    }

    /// Counts echoes of any grade for the exact `(c, t)` payload.
    pub fn echo_any_grade_count_for(&self, view: View, c: &Manifest, t: &Manifest) -> usize {
        self.views.get(&view).map_or(0, |state| {
            state
                .echo_tallies
                .values()
                .filter(|tally| tally.proposal.c() == c && tally.proposal.t() == t)
                .map(|tally| tally.grade_one_parties + tally.grade_zero_parties)
                .sum()
        })
    }

    pub fn candidate_payloads(&self, view: View) -> Vec<(Manifest, Manifest)> {
        let Some(state) = self.views.get(&view) else {
            return Vec::new();
        };
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for tally in state.echo_tallies.values() {
            let key = (tally.proposal.c().clone(), tally.proposal.t().clone());
            if seen.insert(key.clone()) {
                out.push(key);
            }
        }
        out
    }

    /// Returns true for pruned views and views with a terminal AGB result.
    pub fn is_sealed(&self, view: View) -> bool {
        self.is_pruned(view) || self.views.get(&view).is_some_and(|s| s.sealed.is_some())
    }

    pub fn echo_sent(&self, view: View) -> bool {
        self.is_pruned(view) || self.views.get(&view).is_some_and(|s| s.echo_sent)
    }

    pub fn ready_sent(&self, view: View) -> bool {
        self.is_pruned(view) || self.views.get(&view).is_some_and(|s| s.ready_sent)
    }

    /// Returns true once the initial READY response cannot refine further.
    pub fn ready_finalized(&self, view: View) -> bool {
        self.is_pruned(view)
            || self
                .views
                .get(&view)
                .is_some_and(|s| s.ready_sent && !s.ready_mix_open)
    }

    #[cfg(test)]
    pub fn ready_mix_open_for_test(&self, view: View) -> bool {
        self.views
            .get(&view)
            .is_some_and(|state| state.ready_mix_open)
    }

    pub fn sid(&self) -> &Digest {
        &self.sid
    }

    pub fn committee(&self) -> &Committee {
        &self.committee
    }

    /// Returns a proposal fixed through the by-value proposal path.
    /// Returns `None` for pruned, rejected, or unset views.
    pub fn fixed_proposal(&self, view: View) -> Option<(Arc<ProposalOut>, Digest)> {
        if self.is_pruned(view) {
            return None;
        }
        match self.views.get(&view).map(|s| &s.fixed) {
            Some(Fixed::Proposal(p, d)) => Some((Arc::clone(p), d.clone())),
            _ => None,
        }
    }

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

    #[cfg(test)]
    pub(crate) fn stance_for_test(&self, view: View) -> Stance {
        self.views.get(&view).map_or(Stance::Free, |s| s.stance)
    }

    #[cfg(test)]
    pub(crate) fn skip_vote_count_for_test(&self, view: View) -> usize {
        self.views
            .get(&view)
            .map_or(0, |s| s.skip_vote_statements.len())
    }

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
        let s = self.state_mut(view);
        if matches!(s.fixed, Fixed::Proposal(..)) && !s.echo_sent {
            self.pending_gate.insert(view);
        }
        self.recheck_gate(view, lm, rep)
    }

    /// The caller supplies the authenticated sender identity.
    /// Only the first timely proposal from the designated proposer can fix the view.
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
            return effects; // Only the designated proposer can fix a proposal.
        }
        let theta_echo = self.theta_echo();
        if let Some(entry) = self.state_mut(view).entry_instant {
            if now > entry + theta_echo {
                return effects; // Ignore proposals received after the echo deadline.
            }
        }
        if !matches!(self.state_mut(view).fixed, Fixed::Unset) {
            return effects; // The first direct proposal fixes the view.
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
        if self.state_mut(view).active && !self.state_mut(view).echo_sent {
            self.pending_gate.insert(view);
        }
        for r in proposal
            .c()
            .iter()
            .chain(proposal.t().iter())
            .chain(aux_refs_entries(proposal.entries()))
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

    pub fn recheck_all(&mut self, lm: &mut LaneManager, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
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
        log::debug!("vantage agb: organic grade-1 echo view={}", view);
        // Record the fast-seal lock before broadcasting the matching echo.
        self.record_lock(view, &proposal, &digest);
        self.state_mut(view).echo_sent = true;
        self.pending_gate.remove(&view);
        let origin = self.compute_origin(proposal.entries());
        self.count_echo_statement(
            view,
            self.name,
            EchoStatement::Graded(Arc::clone(&proposal), digest, 1, origin.clone()),
        );
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        effects.push(Effect::BroadcastEcho(
            self.build_echo_out(&proposal, 1, origin),
        ));
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

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
                wish: 0, // Replaced with the sender's watermark during serialization.
                origin: origin.into_iter().next().flatten(),
                // Serialization inserts availability claims.
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

    fn build_ready_out(&self, proposal: &Arc<ProposalOut>, grade: ReadyGrade) -> ReadyOut {
        match proposal.as_ref() {
            ProposalOut::Single(p) => ReadyOut::Single(Ready {
                proposal: p.clone(),
                grade,
                sender: self.name,
                wish: 0, // Replaced with the sender's watermark during serialization.
            }),
            ProposalOut::Batch(p) => ReadyOut::Batch(ReadyBatch {
                proposal: p.clone(),
                grade,
                sender: self.name,
                wish: 0,
            }),
        }
    }

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

    fn core_ok(c: &Manifest, lm: &LaneManager) -> bool {
        c.iter().all(|r| lm.author_ok(r))
    }

    /// A paired tip must strictly extend its core entry, have a local prefix,
    /// and include the core entry in that prefix.
    fn tip_ok(c: &Manifest, t: &Manifest, lm: &mut LaneManager) -> bool {
        let by_author: HashMap<_, _> = c.iter().map(|c_ref| (c_ref.0, c_ref)).collect();
        for t_ref in t {
            if let Some(c_ref) = by_author.get(&t_ref.0).copied() {
                if t_ref.1 <= c_ref.1 {
                    return false; // A paired tip must be higher than its core entry.
                }
                if !lm.holds_prefix(t_ref) {
                    return false; // Acknowledgements do not substitute for the prefix.
                }
                if !lm.prefix_contains(t_ref, c_ref) {
                    return false;
                }
            }
        }
        true
    }

    /// Evaluates resolution metadata before positive and fallback echoes.
    fn meta_ok(&self, entries: &[ResolutionEntry], lm: &mut LaneManager) -> bool {
        entries.iter().all(|entry| self.meta_ok_entry(entry, lm))
    }

    fn meta_ok_entry(&self, entry: &ResolutionEntry, lm: &mut LaneManager) -> bool {
        let u = entry.target_view();
        if self.is_pruned(u) {
            // A pruned target lacks the evidence required to evaluate MetaOK.
            return false;
        }
        let Some(state_u) = self.views.get(&u) else {
            return false; // Metadata requires local echo and ready state.
        };
        let Some(own_echo) = state_u.echo_statements.get(&self.name) else {
            return false;
        };
        let Some(own_ready) = state_u.ready_statements.get(&self.name) else {
            return false;
        };
        if state_u.ready_mix_open {
            // A provisional MIX may still refine to the grade incompatible
            // with this entry. It becomes resolution evidence only after it
            // refines or is finalized by closure/deadline.
            return false;
        }
        if let Some(lock) = &state_u.lock {
            if lock.active {
                match entry {
                    ResolutionEntry::Full(_, c, t)
                        if lock.proposal.c() == c && lock.proposal.t() == t => {}
                    _ => return false,
                }
            }
        }
        match entry {
            ResolutionEntry::Full(_, c_u, t_u) => {
                // Full and core entries constrain the local ready state;
                // the local echo only needs to exist.
                let _ = own_echo;
                match own_ready {
                    ReadyStatement::Graded(p, _, grade) => {
                        if *grade == ReadyGrade::Zero {
                            return false;
                        }
                        if p.c() != c_u || p.t() != t_u {
                            return false;
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
                            return false;
                        }
                        if p.c() != c_u || p.t() != t_u {
                            return false;
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

    /// Rejects non-skip entries after a skip vote or terminal skip for the
    /// target view.
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

    /// Counts echo-skip responses toward the `2f + 1` skip-vote threshold.
    fn echo_skip_count(&self, view: View) -> usize {
        self.views.get(&view).map_or(0, |s| s.echo_skip_parties)
    }

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

    /// Counts only the first skip vote from each sender.
    fn count_skip_vote_statement(&mut self, view: View, sender: PublicKey) -> bool {
        if self.is_pruned(view) {
            return false;
        }
        self.state_mut(view).skip_vote_statements.insert(sender)
    }

    /// Submits skip after `2f + 1` distinct skip votes.
    /// The submission uses the same terminal arbiter as other seal routes.
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

    fn compute_origin(&self, entries: &[ResolutionEntry]) -> Vec<Option<u8>> {
        entries
            .iter()
            .map(|entry| self.compute_origin_entry(entry))
            .collect()
    }

    /// Computes the local origin bit from the emitted echo for the target view.
    fn compute_origin_entry(&self, entry: &ResolutionEntry) -> Option<u8> {
        let u = entry.target_view();
        // Pruning does not bypass this check.
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
            return effects; // Wait for the absolute deadline without a fixed proposal.
        };
        self.state_mut(view).echo_sent = true;
        self.pending_gate.remove(&view);
        log::debug!("vantage agb: fallback grade-0 echo view={}", view);
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        if Self::core_ok(proposal.c(), lm) && self.try_meta_ok(view, proposal.entries(), lm) {
            let origin = self.compute_origin(proposal.entries());
            self.count_echo_statement(
                view,
                self.name,
                EchoStatement::Graded(Arc::clone(&proposal), digest, 0, origin.clone()),
            );
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
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// Emits echo-skip at `e_i + θE` if no echo has been emitted.
    pub fn on_echo_absolute_timer(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if self.state_mut(view).echo_sent {
            return effects;
        }
        self.state_mut(view).echo_sent = true;
        self.pending_gate.remove(&view);
        effects.extend(self.wish_effect(view, ResponseStage::Echo));
        self.count_echo_statement(view, self.name, EchoStatement::Skip);
        effects.push(Effect::BroadcastEchoSkip(view));
        self.recheck_lock_release(view);
        effects.extend(self.recheck_ready(view, rep));
        effects.extend(self.recheck_fastseal_trigger(view));
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    pub fn on_echo(&mut self, echo: Echo, rep: &mut Repairer) -> Vec<Effect> {
        // Apply availability claims before counting the echo.
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
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    pub fn on_echo_skip(&mut self, view: View, sender: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if !self.count_echo_statement(view, sender, EchoStatement::Skip) {
            return effects;
        }
        // Echo-skip affects only the fast-seal nonmatching count.
        self.recheck_lock_release(view);
        self.finalize_mix_if_closed(view, false);
        effects.extend(self.recheck_fastseal_trigger(view));
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    /// Counts only the first echo-stage statement from each sender.
    fn count_echo_statement(
        &mut self,
        view: View,
        sender: PublicKey,
        statement: EchoStatement,
    ) -> bool {
        if self.is_pruned(view) {
            return false;
        }
        let stake = self.committee.stake(&sender);
        let state = self.state_mut(view);
        if state.echo_statements.contains_key(&sender) {
            return false;
        }
        match &statement {
            EchoStatement::Graded(proposal, digest, grade, origin) => {
                let tally = state
                    .echo_tallies
                    .entry(digest.clone())
                    .or_insert_with(|| EchoTally {
                        proposal: Arc::clone(proposal),
                        grade_one: 0,
                        grade_zero: 0,
                        grade_one_parties: 0,
                        grade_zero_parties: 0,
                        origin_ones: vec![0; proposal.entries().len()],
                    });
                if *grade == 1 {
                    tally.grade_one += stake;
                    tally.grade_one_parties += 1;
                } else {
                    tally.grade_zero += stake;
                    tally.grade_zero_parties += 1;
                }
                for (i, bit) in origin.iter().enumerate() {
                    if *bit == Some(1) {
                        tally.origin_ones[i] += 1;
                    }
                }
            }
            EchoStatement::Skip => state.echo_skip_parties += 1,
        }
        state.echo_statements.insert(sender, statement);
        true
    }

    fn nonmatching_echo_count(&self, view: View, locked_digest: &Digest) -> usize {
        let Some(state) = self.views.get(&view) else {
            return 0;
        };
        let matching = state
            .echo_tallies
            .get(locked_digest)
            .map_or(0, |tally| tally.grade_one_parties);
        state.echo_statements.len().saturating_sub(matching)
    }

    fn matching_echo_count(&self, view: View, locked_digest: &Digest) -> usize {
        self.views
            .get(&view)
            .and_then(|state| state.echo_tallies.get(locked_digest))
            .map_or(0, |tally| tally.grade_one_parties)
    }

    /// Counts one initial ready-stage statement per sender and, after an
    /// initial READY-mix, at most one same-proposal homogeneous refinement.
    fn count_ready_statement(
        &mut self,
        view: View,
        sender: PublicKey,
        statement: ReadyStatement,
    ) -> bool {
        if self.is_pruned(view) {
            return false;
        }
        let stake = self.committee.stake(&sender);
        let state = self.state_mut(view);
        if let Some(existing) = state.ready_statements.get(&sender) {
            let valid_refinement = matches!(
                (existing, &statement),
                (
                    ReadyStatement::Graded(_, old_digest, ReadyGrade::Mix),
                    ReadyStatement::Graded(_, new_digest, ReadyGrade::Zero | ReadyGrade::One)
                ) if old_digest == new_digest
            );
            if !valid_refinement {
                return false;
            }

            let ReadyStatement::Graded(_, digest, grade) = &statement else {
                unreachable!("a valid READY refinement is graded")
            };
            let Some(tally) = state.ready_tallies.get_mut(digest) else {
                debug_assert!(false, "READY-mix refinement lost its initial tally");
                return false;
            };
            match grade {
                ReadyGrade::One => tally.grade_one += stake,
                ReadyGrade::Zero => tally.grade_zero += stake,
                ReadyGrade::Mix => unreachable!("READY-mix cannot refine to READY-mix"),
            }
            // `any` and the historical non-grade-1 census already counted this
            // author when its initial READY-mix arrived.
            state.ready_statements.insert(sender, statement);
            return true;
        }
        match &statement {
            ReadyStatement::Graded(proposal, digest, grade) => {
                let tally = state
                    .ready_tallies
                    .entry(digest.clone())
                    .or_insert_with(|| ReadyTally {
                        proposal: Arc::clone(proposal),
                        any: 0,
                        grade_one: 0,
                        grade_zero: 0,
                    });
                tally.any += stake;
                match grade {
                    ReadyGrade::One => tally.grade_one += stake,
                    ReadyGrade::Zero => {
                        tally.grade_zero += stake;
                        state.ready_non_grade_one_parties += 1;
                    }
                    ReadyGrade::Mix => state.ready_non_grade_one_parties += 1,
                }
            }
            ReadyStatement::NoReady => {
                state.ready_non_grade_one_parties += 1;
                state.noready_parties += 1;
            }
        }
        state.ready_statements.insert(sender, statement);
        true
    }

    fn recheck_ready(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if !self.state_mut(view).ready_sent {
            if let Some((digest, proposal, grade)) = self.ready_candidate(view) {
                effects.extend(self.emit_initial_ready(view, digest, proposal, grade, rep));
            }
            return effects;
        }
        if let Some((digest, proposal, grade)) = self.ready_refinement_candidate(view) {
            effects.extend(self.emit_ready_refinement(view, digest, proposal, grade, rep));
        } else {
            self.finalize_mix_if_closed(view, false);
        }
        effects
    }

    /// Selects the first guarded ECHO quorum immediately, retaining the paper's
    /// existing READY-mix completion latency.
    fn ready_candidate(&self, view: View) -> Option<(Digest, Arc<ProposalOut>, ReadyGrade)> {
        self.views.get(&view).and_then(|state| {
            state.echo_tallies.iter().find_map(|(digest, tally)| {
                if tally.grade_one + tally.grade_zero < self.quorum {
                    return None;
                }
                let ready_guard = tally
                    .proposal
                    .entries()
                    .iter()
                    .enumerate()
                    .all(|(i, entry)| match entry {
                        ResolutionEntry::Full(..) | ResolutionEntry::Core(..) => {
                            tally.origin_ones[i] >= self.f_plus_1_parties
                        }
                        ResolutionEntry::Skip(_) => true,
                    });
                if !ready_guard {
                    return None;
                }
                let grade = if tally.grade_one >= self.quorum {
                    ReadyGrade::One
                } else if tally.grade_zero >= self.quorum {
                    ReadyGrade::Zero
                } else {
                    ReadyGrade::Mix
                };
                Some((digest.clone(), Arc::clone(&tally.proposal), grade))
            })
        })
    }

    /// Returns a homogeneous refinement only for this party's provisional
    /// READY-mix proposal.
    fn ready_refinement_candidate(
        &self,
        view: View,
    ) -> Option<(Digest, Arc<ProposalOut>, ReadyGrade)> {
        let state = self.views.get(&view)?;
        if !state.ready_mix_open {
            return None;
        }
        let ReadyStatement::Graded(proposal, digest, ReadyGrade::Mix) =
            state.ready_statements.get(&self.name)?
        else {
            return None;
        };
        let tally = state.echo_tallies.get(digest)?;
        let grade = if tally.grade_one >= self.quorum {
            ReadyGrade::One
        } else if tally.grade_zero >= self.quorum {
            ReadyGrade::Zero
        } else {
            return None;
        };
        Some((digest.clone(), Arc::clone(proposal), grade))
    }

    fn finalize_mix_if_closed(&mut self, view: View, deadline: bool) {
        let close = self.views.get(&view).is_some_and(|state| {
            state.ready_mix_open && (deadline || state.echo_statements.len() == self.n)
        });
        if close {
            self.state_mut(view).ready_mix_open = false;
        }
    }

    fn emit_initial_ready(
        &mut self,
        view: View,
        digest: Digest,
        proposal: Arc<ProposalOut>,
        grade: ReadyGrade,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        let name = self.name;
        {
            let state = self.state_mut(view);
            state.ready_sent = true;
            state.ready_mix_open = grade == ReadyGrade::Mix;
        }
        let tally_digest = digest.clone();
        self.count_ready_statement(
            view,
            name,
            ReadyStatement::Graded(Arc::clone(&proposal), digest, grade),
        );
        if grade == ReadyGrade::Mix && !proposal.t().is_empty() {
            effects.push(Effect::QuarantineTips(proposal.t().clone()));
        }
        effects.extend(self.wish_effect(view, ResponseStage::Ready));
        effects.push(Effect::BroadcastReady(
            self.build_ready_out(&proposal, grade),
        ));
        effects.extend(self.recheck_completion_and_direct(view, &tally_digest, rep));
        self.finalize_mix_if_closed(view, false);
        effects
    }

    fn emit_ready_refinement(
        &mut self,
        view: View,
        digest: Digest,
        proposal: Arc<ProposalOut>,
        grade: ReadyGrade,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        debug_assert!(matches!(grade, ReadyGrade::Zero | ReadyGrade::One));
        let accepted = self.count_ready_statement(
            view,
            self.name,
            ReadyStatement::Graded(Arc::clone(&proposal), digest.clone(), grade),
        );
        debug_assert!(accepted, "local READY-mix refinement must be admissible");
        if !accepted {
            return Vec::new();
        }
        self.state_mut(view).ready_mix_open = false;
        let mut effects = vec![Effect::BroadcastReady(
            self.build_ready_out(&proposal, grade),
        )];
        effects.extend(self.recheck_completion_and_direct(view, &digest, rep));
        effects
    }

    /// Closes a provisional READY-mix, or emits NO-READY if no guarded ECHO
    /// quorum produced an initial response.
    pub fn on_ready_timer(&mut self, view: View, rep: &mut Repairer) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        if self.state_mut(view).ready_sent {
            if let Some((digest, proposal, grade)) = self.ready_refinement_candidate(view) {
                return self.emit_ready_refinement(view, digest, proposal, grade, rep);
            }
            self.finalize_mix_if_closed(view, true);
            return effects;
        }
        if let Some((digest, proposal, grade)) = self.ready_candidate(view) {
            let effects = self.emit_initial_ready(view, digest, proposal, grade, rep);
            self.finalize_mix_if_closed(view, true);
            return effects;
        }
        self.state_mut(view).ready_sent = true;
        self.count_ready_statement(view, self.name, ReadyStatement::NoReady);
        effects.extend(self.wish_effect(view, ResponseStage::Ready));
        effects.push(Effect::BroadcastNoReady(view));
        // This is the only transition of the local ready state to NoReady.
        effects.extend(self.recheck_skip_vote_trigger(view));
        effects
    }

    pub fn on_ready(&mut self, ready: Ready, rep: &mut Repairer) -> Vec<Effect> {
        self.on_ready_any(ReadyOut::Single(ready), rep)
    }

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
        let (proposal, digest) = self.canonical_proposal(view, ready.into_proposal_out());
        let tally_digest = digest.clone();
        if !self.count_ready_statement(
            view,
            sender,
            ReadyStatement::Graded(proposal, digest, grade),
        ) {
            return Vec::new();
        }
        self.recheck_completion_and_direct(view, &tally_digest, rep)
    }

    pub fn on_noready(&mut self, view: View, sender: PublicKey) -> Vec<Effect> {
        if self.is_pruned(view) {
            return Vec::new();
        }
        self.count_ready_statement(view, sender, ReadyStatement::NoReady);
        Vec::new()
    }

    fn recheck_completion_and_direct(
        &mut self,
        view: View,
        digest: &Digest,
        rep: &mut Repairer,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.is_pruned(view) {
            return effects;
        }
        let Some((proposal, any_stake, g1_stake, g0_stake)) = self
            .views
            .get(&view)
            .and_then(|state| state.ready_tallies.get(digest))
            .map(|tally| {
                (
                    Arc::clone(&tally.proposal),
                    tally.any,
                    tally.grade_one,
                    tally.grade_zero,
                )
            })
        else {
            return effects;
        };
        if any_stake >= self.quorum && self.state_mut(view).completed.is_none() {
            let c = proposal.c().clone();
            let t = proposal.t().clone();
            self.state_mut(view).completed = Some((c.clone(), t.clone()));
            for r in c.iter().chain(aux_refs_entries(proposal.entries())) {
                effects.extend(rep.authorize(r.clone()));
            }
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
        effects
    }

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
        if let Some(existing) = &state.sealed {
            debug_assert!(
                Self::outcomes_compatible(existing, &outcome),
                "try-seal arbiter: incompatible outcomes submitted for view {}: {:?} vs {:?}",
                view,
                existing,
                outcome
            );
            return;
        }
        #[cfg(feature = "pipeline-tracing")]
        let proposal_start = state.first_proposal_instant;
        state.sealed = Some(outcome.clone());
        effects.push(Effect::Sealed(view, outcome));
        if let Some(metrics) = &self.metrics {
            metrics.vantage_seals.with_label_values(&[route]).inc();
            #[cfg(feature = "pipeline-tracing")]
            if let Some(start) = proposal_start {
                metrics
                    .pipeline
                    .vantage_proposal_to_seal_latency
                    .observe(start.elapsed());
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

    /// Records the fast-seal lock before sending the matching grade-1 echo.
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

    /// Deactivates the lock after `f + 1` nonmatching statements.
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

    /// Seals the locked proposal after matching statements from all parties.
    /// The lock must still be active.
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

type BufferedEcho = (
    Digest,
    u8,
    Option<u8>,
    Option<crate::vantage::claim::AvailClaim>,
);
type BufferedReady = (Digest, ReadyGrade);

/// Buffers digest-named statements until their proposal bodies are verified.
pub struct DigestStatements {
    /// Stores the first buffered echo digest statement from each sender for each view.
    /// Statements remain buffered until a matching body is verified.
    buffered_echo: BTreeMap<View, BTreeMap<PublicKey, BufferedEcho>>,
    /// Stores one effective buffered ready per sender. A same-digest
    /// homogeneous refinement replaces an earlier READY-mix before body fetch.
    buffered_ready: BTreeMap<View, BTreeMap<PublicKey, BufferedReady>>,
    /// Caches verified proposal bodies by `(view, digest)`.
    known_bodies: BTreeMap<(View, Digest), Arc<ViewProposal>>,
    pending_fetch: BTreeMap<(View, Digest), FetchState>,
    /// Serves each `(view, digest, requester)` tuple at most once.
    fetch_answered: BTreeSet<(View, Digest, PublicKey)>,
    min_live_view: View,
    fetch_retry_interval: Duration,
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

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn is_pruned(&self, view: View) -> bool {
        view < self.min_live_view
    }

    /// Prunes all digest-statement state below `floor`.
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

    /// Resolves only verified single-proposal bodies matching `(view, digest)`.
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
                let echo = Echo {
                    proposal: (**body).clone(),
                    grade,
                    sender,
                    wish: 0,
                    origin,
                    // Resolve the claim only after its proposal body is available.
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
        let inserted = match self.buffered_echo.entry(view).or_default().entry(sender) {
            Entry::Vacant(entry) => {
                entry.insert((digest.clone(), grade, origin, avail));
                true
            }
            Entry::Occupied(_) => false,
        };
        if inserted {
            self.ensure_fetch(view, digest, now)
        } else {
            Vec::new()
        }
    }

    fn buffer_ready(
        &mut self,
        view: View,
        digest: Digest,
        sender: PublicKey,
        grade: ReadyGrade,
        now: Instant,
    ) -> Vec<Effect> {
        let accepted = match self.buffered_ready.entry(view).or_default().entry(sender) {
            Entry::Vacant(entry) => {
                entry.insert((digest.clone(), grade));
                true
            }
            Entry::Occupied(mut entry) => {
                let (old_digest, old_grade) = entry.get();
                if *old_digest == digest
                    && *old_grade == ReadyGrade::Mix
                    && matches!(grade, ReadyGrade::Zero | ReadyGrade::One)
                {
                    entry.insert((digest.clone(), grade));
                    true
                } else {
                    false
                }
            }
        };
        if accepted {
            self.ensure_fetch(view, digest, now)
        } else {
            Vec::new()
        }
    }

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

    /// Returns whether a buffered statement authorizes this body pair.
    fn has_buffered_statement_for(&self, view: View, digest: &Digest) -> bool {
        self.buffered_echo
            .get(&view)
            .is_some_and(|senders| senders.values().any(|(d, _, _, _)| d == digest))
            || self
                .buffered_ready
                .get(&view)
                .is_some_and(|senders| senders.values().any(|(d, _)| d == digest))
    }

    /// Fetches a body from buffered authors at most once per retry interval.
    fn ensure_fetch(&mut self, view: View, digest: Digest, now: Instant) -> Vec<Effect> {
        if self.is_pruned(view) {
            return Vec::new();
        }
        let key = (view, digest.clone());
        // Retries preserve the previous width and attempt count.
        let mut width = FETCH_WIDTH_START;
        let mut attempts = 0;
        match self.pending_fetch.get(&key) {
            Some(state)
                if now.saturating_duration_since(state.last) < self.fetch_retry_interval =>
            {
                return Vec::new();
            }
            Some(state) => {
                // Stop fetching after `MAX_FETCH_ATTEMPTS`.
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
        if self.pending_fetch.len() >= MAX_PENDING_FETCH {
            while self.pending_fetch.len() >= MAX_PENDING_FETCH {
                let Some((highest, _)) = self.pending_fetch.iter().next_back() else {
                    break;
                };
                let highest = highest.clone();
                // Do not evict the pair being inserted.
                if highest <= key {
                    break;
                }
                self.pending_fetch.remove(&highest);
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_body_fetch_evicted_total.inc();
                }
            }
            if self.pending_fetch.len() >= MAX_PENDING_FETCH {
                // Lower pending views take priority over this pair.
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
        // Deterministic ordering makes each wider retry retain earlier targets.
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

    /// Counts a digest-named echo only after its proposal body is verified.
    /// Reception accepts both by-value and digest-named encodings.
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

    /// Counts a digest-named ready only after its proposal body is verified.
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

    /// Drains buffered digest statements after a by-value proposal is fixed.
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

    /// Serves only a matching locally fixed body.
    /// Each requester receives a body pair at most once.
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

    /// Accepts only well-formed bodies requested or named by a buffered statement.
    /// Accepted bodies do not create proposal provenance.
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
        let key = (view, digest.clone());
        let relevant =
            self.pending_fetch.contains_key(&key) || self.has_buffered_statement_for(view, &digest);
        if !relevant {
            return Vec::new(); // Reject unsolicited or irrelevant bodies.
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

    /// Retries body fetches whose backoff interval has elapsed.
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

    /// Returns the number of outstanding body fetches.
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
