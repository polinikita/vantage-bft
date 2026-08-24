//! Concurrent, per-target agreement for open Vantage outcomes.
//!
//! Each unresolved AGB view owns an independent WISH/IT-HS instance.  A fresh
//! value is checked directly against that target's local AGB state immediately
//! before ECHO.  A quorum of fresh ECHOs, including `f + 1` first-hand origins
//! for a non-skip value, permits a target-local validity witness.  Witnesses
//! use the usual `f + 1` relay and `n - f` delivery thresholds, so a delivered
//! value becomes valid at every correct party before it enters KEY1.  This
//! stable `Backed` predicate is reused during view change.  No later AGB
//! proposal, carrier quorum, or global resolver height lies on this path.

use crate::leader::one_based_authority;
use crate::primary::View;
use crate::vantage::agb::{formed_resolution_entry, ResolutionEntry};
use crate::vantage::block;
use crate::vantage::Thresholds;
use config::Committee;
use crypto::{Digest, PublicKey};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

pub type DirectResolverView = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DirectResolutionTimerKind {
    /// Retransmit the initial WISH until this locally justified target enters.
    Entry,
    /// Abandon a view whose primary has not delivered a proposal.
    Proposal,
    /// Abandon a proposed view whose agreement phases did not finish.
    View,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionWish {
    pub target: View,
    pub view: DirectResolverView,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionSuggest {
    pub target: View,
    pub view: DirectResolverView,
    pub sender: PublicKey,
    pub key3_view: DirectResolverView,
    pub key3_value: Digest,
    pub key2_view: DirectResolverView,
    pub key2_value: Digest,
    pub prev_key2: DirectResolverView,
    /// A positive suggestion carries the bounded resolver value it names.
    pub entry: Option<ResolutionEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionProof {
    pub target: View,
    pub view: DirectResolverView,
    pub sender: PublicKey,
    pub key1_view: DirectResolverView,
    pub key1_value: Digest,
    pub prev_key1: DirectResolverView,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionWitness {
    pub target: View,
    pub view: DirectResolverView,
    pub value: Digest,
    /// The bounded value is attached so a witness relay never depends on a
    /// Byzantine proposer serving it again.
    pub entry: ResolutionEntry,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionProposal {
    pub target: View,
    pub view: DirectResolverView,
    pub key_view: DirectResolverView,
    pub value: Digest,
    pub entry: ResolutionEntry,
    pub sender: PublicKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DirectResolutionPhase {
    Echo,
    Key1,
    Key2,
    Key3,
    Lock,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionStatement {
    pub target: View,
    pub view: DirectResolverView,
    pub value: Digest,
    pub phase: DirectResolutionPhase,
    /// Only a fresh non-skip ECHO carries an origin bit.
    pub origin: Option<u8>,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionDone {
    pub target: View,
    pub value: Digest,
    pub entry: ResolutionEntry,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionValueFetch {
    pub target: View,
    pub value: Digest,
    pub requester: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectResolutionValueServe {
    pub target: View,
    pub value: Digest,
    pub entry: ResolutionEntry,
}

/// The caller's result for a target-local external-validity request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectResolutionVote {
    Reject,
    Accept { origin: Option<u8> },
}

/// Effects are deliberately local to this module until the experimental path
/// is selected by the Vantage runtime.
#[derive(Clone, Debug)]
pub enum DirectResolutionEffect {
    BroadcastWish(DirectResolutionWish),
    SuggestTo(PublicKey, DirectResolutionSuggest),
    BroadcastProof(DirectResolutionProof),
    BroadcastWitness(DirectResolutionWitness),
    BroadcastProposal(DirectResolutionProposal),
    BroadcastStatement(DirectResolutionStatement),
    BroadcastDone(DirectResolutionDone),
    DoneTo(PublicKey, DirectResolutionDone),
    ValueFetchTo(PublicKey, DirectResolutionValueFetch),
    ValueServeTo(PublicKey, DirectResolutionValueServe),
    ArmTimer(View, DirectResolverView, DirectResolutionTimerKind, Instant),
    ValidateVote {
        target: View,
        view: DirectResolverView,
        value: Digest,
        entry: ResolutionEntry,
        fresh: bool,
    },
    Decide(ResolutionEntry),
}

#[derive(Clone, Debug)]
struct StatementRecord {
    value: Digest,
    origin: Option<u8>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum VoteStatus {
    #[default]
    Unknown,
    Requested,
    Rejected,
    Accepted,
}

#[derive(Default)]
struct DirectViewState {
    suggestions: HashMap<PublicKey, DirectResolutionSuggest>,
    proofs: HashMap<PublicKey, DirectResolutionProof>,
    proposal: Option<DirectResolutionProposal>,
    backing_sent: Option<Digest>,
    backing_witnesses: HashMap<PublicKey, Digest>,
    vote_status: VoteStatus,
    vote_origin: Option<u8>,
    sent: HashMap<DirectResolutionPhase, Digest>,
    statements: HashMap<DirectResolutionPhase, HashMap<PublicKey, StatementRecord>>,
}

struct DirectInstance {
    own_wish: DirectResolverView,
    entered_through: DirectResolverView,
    current_view: DirectResolverView,
    wishes: HashMap<PublicKey, DirectResolverView>,
    views: BTreeMap<DirectResolverView, DirectViewState>,
    candidates: Vec<ResolutionEntry>,

    key1_view: DirectResolverView,
    key1_value: Digest,
    prev_key1: DirectResolverView,
    key2_view: DirectResolverView,
    key2_value: Digest,
    prev_key2: DirectResolverView,
    key3_view: DirectResolverView,
    key3_value: Digest,
    lock_view: DirectResolverView,
    lock_value: Digest,

    done_sent: Option<Digest>,
    done: HashMap<PublicKey, Digest>,
}

impl DirectInstance {
    fn new(target: View) -> Self {
        debug_assert!(target > 0);
        Self {
            own_wish: 0,
            entered_through: 0,
            current_view: 0,
            wishes: HashMap::new(),
            views: BTreeMap::new(),
            candidates: Vec::new(),
            key1_view: 0,
            key1_value: Digest::default(),
            prev_key1: 0,
            key2_view: 0,
            key2_value: Digest::default(),
            prev_key2: 0,
            key3_view: 0,
            key3_value: Digest::default(),
            lock_view: 0,
            lock_value: Digest::default(),
            done_sent: None,
            done: HashMap::new(),
        }
    }
}

/// Independent WISH/IT-HS resolver instances keyed by the AGB target view.
pub struct DirectResolver {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    delta: Duration,
    f_plus_1: usize,
    quorum: usize,
    instances: BTreeMap<View, DirectInstance>,
    values: BTreeMap<(View, Digest), ResolutionEntry>,
    decisions: BTreeMap<View, (Digest, ResolutionEntry)>,
    /// Targets sealed by any Vantage path. Retained until the sequence GC floor
    /// passes them so stale traffic cannot recreate dead instances.
    terminals: BTreeSet<View>,
    pending_fetch: BTreeMap<View, BTreeSet<Digest>>,
    fetch_requested: BTreeMap<View, BTreeSet<(Digest, PublicKey)>>,
    fetch_answered: BTreeMap<View, BTreeSet<(Digest, PublicKey)>>,
}

impl DirectResolver {
    pub fn new(name: PublicKey, committee: Committee, sid: Digest, delta_ms: u64) -> Self {
        let thresholds = Thresholds::from_party_count(committee.size());
        Self {
            name,
            committee,
            sid,
            delta: Duration::from_millis(delta_ms),
            f_plus_1: thresholds.f_plus_1_parties,
            quorum: thresholds.n_minus_f_parties,
            instances: BTreeMap::new(),
            values: BTreeMap::new(),
            decisions: BTreeMap::new(),
            terminals: BTreeSet::new(),
            pending_fetch: BTreeMap::new(),
            fetch_requested: BTreeMap::new(),
            fetch_answered: BTreeMap::new(),
        }
    }

    pub fn resolver_timeout(&self) -> Duration {
        self.delta * 11
    }

    /// From the first correct entry, all correct parties enter within `2Delta`,
    /// their suggestions reach a correct primary within one more delay, and
    /// its proposal reaches every correct party within another.  The extra
    /// delay avoids relying on a deadline tie in the runtime scheduler.
    pub fn proposal_timeout(&self) -> Duration {
        self.delta * 5
    }

    pub fn resolution_leader(&self, target: View, view: DirectResolverView) -> PublicKey {
        // View 1 deliberately starts after the unresolved AGB proposer.  A
        // faulty proposer is therefore not guaranteed the first resolver turn;
        // subsequent views still visit every committee member in order.
        one_based_authority(&self.committee, target.saturating_add(view))
    }

    pub fn value_digest(&self, entry: &ResolutionEntry) -> Digest {
        let bytes = bincode::serialize(entry).expect("ResolutionEntry always serializes");
        block::domain_hash(b"vantage-direct-resolution-value", &self.sid, &bytes)
    }

    fn is_member(&self, sender: &PublicKey) -> bool {
        self.committee.stake(sender) > 0
    }

    fn valid_entry(&self, target: View, entry: &ResolutionEntry) -> bool {
        entry.target_view() == target && formed_resolution_entry(&self.committee, entry)
    }

    fn remember_entry(&mut self, target: View, entry: ResolutionEntry) -> Option<Digest> {
        if !self.valid_entry(target, &entry) {
            return None;
        }
        let value = self.value_digest(&entry);
        self.values.entry((target, value.clone())).or_insert(entry);
        Some(value)
    }

    fn entry(&self, target: View, value: &Digest) -> Option<&ResolutionEntry> {
        self.values.get(&(target, value.clone()))
    }

    fn canonical_entry_key(entry: &ResolutionEntry) -> Vec<u8> {
        bincode::serialize(entry).expect("ResolutionEntry always serializes")
    }

    /// Adds locally justified values and starts the target's WISH instance.
    pub fn update_candidates(
        &mut self,
        target: View,
        candidates: impl IntoIterator<Item = ResolutionEntry>,
    ) -> Vec<DirectResolutionEffect> {
        if target == 0 || self.decisions.contains_key(&target) || self.terminals.contains(&target) {
            return Vec::new();
        }
        let mut accepted = Vec::new();
        for entry in candidates {
            if !self.valid_entry(target, &entry) {
                continue;
            }
            self.remember_entry(target, entry.clone());
            accepted.push(entry);
        }
        if accepted.is_empty() {
            return Vec::new();
        }
        let instance = self
            .instances
            .entry(target)
            .or_insert_with(|| DirectInstance::new(target));
        for entry in accepted {
            if !instance.candidates.contains(&entry) {
                instance.candidates.push(entry);
            }
        }
        instance.candidates.sort_by_key(Self::canonical_entry_key);
        let mut effects = self.raise_own_wish(target, 1);
        effects.extend(self.recheck_wishes(target));
        effects
    }

    /// Phase traffic may race one resolver view ahead of local WISH entry,
    /// but a Byzantine member cannot allocate arbitrary future view maps.
    fn admits_phase_view(&self, target: View, view: DirectResolverView) -> bool {
        self.instances.get(&target).is_some_and(|instance| {
            view > 0
                && view
                    <= instance
                        .own_wish
                        .max(instance.entered_through)
                        .saturating_add(1)
        })
    }

    fn raise_own_wish(
        &mut self,
        target: View,
        view: DirectResolverView,
    ) -> Vec<DirectResolutionEffect> {
        let retry_delay = self.proposal_timeout();
        let Some(instance) = self.instances.get_mut(&target) else {
            return Vec::new();
        };
        if view <= instance.own_wish {
            return Vec::new();
        }
        instance.own_wish = view;
        instance.wishes.insert(self.name, view);
        let waiting_for_initial_entry = instance.current_view == 0;
        let mut effects = vec![DirectResolutionEffect::BroadcastWish(
            DirectResolutionWish {
                target,
                view,
                sender: self.name,
            },
        )];
        if waiting_for_initial_entry {
            effects.push(DirectResolutionEffect::ArmTimer(
                target,
                view,
                DirectResolutionTimerKind::Entry,
                Instant::now() + retry_delay,
            ));
        }
        effects
    }

    pub fn on_wish(&mut self, wish: DirectResolutionWish) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&wish.sender) || wish.target == 0 || wish.view == 0 {
            return Vec::new();
        }
        if let Some((value, entry)) = self.decisions.get(&wish.target) {
            return vec![DirectResolutionEffect::DoneTo(
                wish.sender,
                DirectResolutionDone {
                    target: wish.target,
                    value: value.clone(),
                    entry: entry.clone(),
                    sender: self.name,
                },
            )];
        }
        if self.terminals.contains(&wish.target) {
            return Vec::new();
        }
        // Only local AGB evidence may allocate a target.  Correct parties
        // retransmit their initial WISH until all peers have created the same
        // locally justified instance, so dropping an early unsolicited WISH
        // does not weaken liveness.
        let Some(instance) = self.instances.get_mut(&wish.target) else {
            return Vec::new();
        };
        let slot = instance.wishes.entry(wish.sender).or_default();
        if wish.view <= *slot {
            return Vec::new();
        }
        *slot = wish.view;
        self.recheck_wishes(wish.target)
    }

    fn kth_largest(
        values: impl Iterator<Item = DirectResolverView>,
        k: usize,
    ) -> DirectResolverView {
        let mut values: Vec<_> = values.collect();
        values.sort_unstable_by(|a, b| b.cmp(a));
        values.get(k.saturating_sub(1)).copied().unwrap_or(0)
    }

    fn recheck_wishes(&mut self, target: View) -> Vec<DirectResolutionEffect> {
        let Some(instance) = self.instances.get(&target) else {
            return Vec::new();
        };
        let amplify = Self::kth_largest(instance.wishes.values().copied(), self.f_plus_1);
        let mut effects = self.raise_own_wish(target, amplify);
        let Some(instance) = self.instances.get(&target) else {
            return effects;
        };
        let enter = Self::kth_largest(instance.wishes.values().copied(), self.quorum);
        let start = instance.entered_through.saturating_add(1);
        if enter < start {
            return effects;
        }
        for view in start..=enter {
            effects.extend(self.enter_view(target, view));
        }
        effects
    }

    fn enter_view(
        &mut self,
        target: View,
        view: DirectResolverView,
    ) -> Vec<DirectResolutionEffect> {
        let proposal_timeout = self.proposal_timeout();
        let view_timeout = self.resolver_timeout();
        let (
            key3_view,
            key3_value,
            key2_view,
            key2_value,
            prev_key2,
            key1_view,
            key1_value,
            prev_key1,
        ) = {
            let Some(instance) = self.instances.get_mut(&target) else {
                return Vec::new();
            };
            if view <= instance.entered_through {
                return Vec::new();
            }
            instance.entered_through = view;
            instance.current_view = view;
            instance.views.entry(view).or_default();
            (
                instance.key3_view,
                instance.key3_value.clone(),
                instance.key2_view,
                instance.key2_value.clone(),
                instance.prev_key2,
                instance.key1_view,
                instance.key1_value.clone(),
                instance.prev_key1,
            )
        };
        let leader = self.resolution_leader(target, view);
        let entry = (key3_view > 0)
            .then(|| self.entry(target, &key3_value).cloned())
            .flatten();
        let suggest = DirectResolutionSuggest {
            target,
            view,
            sender: self.name,
            key3_view,
            key3_value,
            key2_view,
            key2_value,
            prev_key2,
            entry,
        };
        let proof = DirectResolutionProof {
            target,
            view,
            sender: self.name,
            key1_view,
            key1_value,
            prev_key1,
        };
        let instance = self.instances.get_mut(&target).unwrap();
        let state = instance.views.get_mut(&view).unwrap();
        state.suggestions.insert(self.name, suggest.clone());
        state.proofs.insert(self.name, proof.clone());
        let now = Instant::now();
        let mut effects = vec![
            DirectResolutionEffect::ArmTimer(
                target,
                view,
                DirectResolutionTimerKind::Proposal,
                now + proposal_timeout,
            ),
            DirectResolutionEffect::ArmTimer(
                target,
                view,
                DirectResolutionTimerKind::View,
                now + view_timeout,
            ),
            DirectResolutionEffect::BroadcastProof(proof),
        ];
        if leader != self.name {
            effects.push(DirectResolutionEffect::SuggestTo(leader, suggest));
        }
        effects.extend(self.try_primary_propose(target, view));
        effects.extend(self.try_echo(target, view));
        effects.extend(self.advance_view(target, view));
        effects
    }

    pub fn on_timer(
        &mut self,
        target: View,
        view: DirectResolverView,
        kind: DirectResolutionTimerKind,
    ) -> Vec<DirectResolutionEffect> {
        let Some(instance) = self.instances.get(&target) else {
            return Vec::new();
        };
        if kind == DirectResolutionTimerKind::Entry {
            if instance.current_view != 0 || instance.own_wish != view {
                return Vec::new();
            }
            return vec![
                DirectResolutionEffect::BroadcastWish(DirectResolutionWish {
                    target,
                    view,
                    sender: self.name,
                }),
                DirectResolutionEffect::ArmTimer(
                    target,
                    view,
                    DirectResolutionTimerKind::Entry,
                    Instant::now() + self.proposal_timeout(),
                ),
            ];
        }
        if instance.current_view != view {
            return Vec::new();
        }
        if kind == DirectResolutionTimerKind::Proposal
            && instance
                .views
                .get(&view)
                .is_some_and(|state| state.proposal.is_some())
        {
            return Vec::new();
        }
        let mut effects = self.raise_own_wish(target, view.saturating_add(1));
        effects.extend(self.recheck_wishes(target));
        effects
    }

    pub fn on_suggest(&mut self, suggest: DirectResolutionSuggest) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&suggest.sender)
            || suggest.target == 0
            || suggest.view == 0
            || self.decisions.contains_key(&suggest.target)
            || !self.instances.contains_key(&suggest.target)
        {
            return Vec::new();
        }
        if !self.admits_phase_view(suggest.target, suggest.view)
            || self
                .instances
                .get(&suggest.target)
                .is_some_and(|instance| suggest.view < instance.current_view)
        {
            return Vec::new();
        }
        if self
            .instances
            .get(&suggest.target)
            .and_then(|instance| instance.views.get(&suggest.view))
            .is_some_and(|state| state.suggestions.contains_key(&suggest.sender))
        {
            return Vec::new();
        }
        if suggest.key3_view == 0 {
            if suggest.entry.is_some() {
                return Vec::new();
            }
        } else {
            let Some(entry) = suggest.entry.clone() else {
                return Vec::new();
            };
            let Some(value) = self.remember_entry(suggest.target, entry) else {
                return Vec::new();
            };
            if value != suggest.key3_value {
                return Vec::new();
            }
        }
        let instance = self.instances.get_mut(&suggest.target).unwrap();
        let state = instance.views.entry(suggest.view).or_default();
        state.suggestions.insert(suggest.sender, suggest.clone());
        self.try_primary_propose(suggest.target, suggest.view)
    }

    pub fn on_proof(&mut self, proof: DirectResolutionProof) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&proof.sender)
            || proof.target == 0
            || proof.view == 0
            || self.decisions.contains_key(&proof.target)
            || !self.instances.contains_key(&proof.target)
        {
            return Vec::new();
        }
        if !self.admits_phase_view(proof.target, proof.view)
            || self
                .instances
                .get(&proof.target)
                .is_some_and(|instance| proof.view < instance.current_view)
        {
            return Vec::new();
        }
        let instance = self.instances.get_mut(&proof.target).unwrap();
        let state = instance.views.entry(proof.view).or_default();
        if state.proofs.contains_key(&proof.sender) {
            return Vec::new();
        }
        state.proofs.insert(proof.sender, proof.clone());
        self.try_echo(proof.target, proof.view)
    }

    fn accept_key(
        &self,
        target: View,
        view: DirectResolverView,
        key: DirectResolverView,
        value: &Digest,
    ) -> bool {
        if key == 0 {
            return true;
        }
        let Some(state) = self
            .instances
            .get(&target)
            .and_then(|instance| instance.views.get(&view))
        else {
            return false;
        };
        state
            .suggestions
            .values()
            .filter(|suggest| {
                suggest.prev_key2 < suggest.key2_view
                    && suggest.key2_view < view
                    && (key <= suggest.prev_key2
                        || (key <= suggest.key2_view && &suggest.key2_value == value))
            })
            .count()
            >= self.f_plus_1
    }

    fn backing_winner(
        &self,
        target: View,
        view: DirectResolverView,
        threshold: usize,
    ) -> Option<Digest> {
        let witnesses = &self
            .instances
            .get(&target)?
            .views
            .get(&view)?
            .backing_witnesses;
        let mut counts: BTreeMap<Digest, usize> = BTreeMap::new();
        for value in witnesses.values() {
            *counts.entry(value.clone()).or_default() += 1;
        }
        counts
            .into_iter()
            .find_map(|(value, count)| (count >= threshold).then_some(value))
    }

    fn is_backed(&self, target: View, value: &Digest) -> bool {
        self.instances.get(&target).is_some_and(|instance| {
            instance.views.values().any(|state| {
                state
                    .backing_witnesses
                    .values()
                    .filter(|witnessed| *witnessed == value)
                    .count()
                    >= self.quorum
            })
        })
    }

    fn emit_backing_witness(
        &mut self,
        target: View,
        view: DirectResolverView,
        value: Digest,
    ) -> Vec<DirectResolutionEffect> {
        let Some(entry) = self.entry(target, &value).cloned() else {
            return Vec::new();
        };
        let Some(state) = self
            .instances
            .get_mut(&target)
            .and_then(|instance| instance.views.get_mut(&view))
        else {
            return Vec::new();
        };
        if state.backing_sent.is_some() {
            return Vec::new();
        }
        state.backing_sent = Some(value.clone());
        state.backing_witnesses.insert(self.name, value.clone());
        vec![DirectResolutionEffect::BroadcastWitness(
            DirectResolutionWitness {
                target,
                view,
                value,
                entry,
                sender: self.name,
            },
        )]
    }

    fn recheck_backing(
        &mut self,
        target: View,
        view: DirectResolverView,
    ) -> Vec<DirectResolutionEffect> {
        let mut effects = Vec::new();
        if let Some(value) = self.backing_winner(target, view, self.f_plus_1) {
            effects.extend(self.emit_backing_witness(target, view, value));
        }
        if self.backing_winner(target, view, self.quorum).is_some()
            && self
                .instances
                .get(&target)
                .is_some_and(|instance| instance.current_view == view)
        {
            effects.extend(self.advance_view(target, view));
        }
        effects
    }

    pub fn on_witness(&mut self, witness: DirectResolutionWitness) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&witness.sender)
            || witness.target == 0
            || witness.view == 0
            || self.decisions.contains_key(&witness.target)
            || self.value_digest(&witness.entry) != witness.value
            || !self.instances.contains_key(&witness.target)
        {
            return Vec::new();
        }
        if !self.admits_phase_view(witness.target, witness.view) {
            return Vec::new();
        }
        if self
            .instances
            .get(&witness.target)
            .and_then(|instance| instance.views.get(&witness.view))
            .is_some_and(|state| state.backing_witnesses.contains_key(&witness.sender))
        {
            return Vec::new();
        }
        if self.remember_entry(witness.target, witness.entry).is_none() {
            return Vec::new();
        }
        let state = self
            .instances
            .get_mut(&witness.target)
            .unwrap()
            .views
            .entry(witness.view)
            .or_default();
        state
            .backing_witnesses
            .insert(witness.sender, witness.value);
        let mut effects = self.recheck_backing(witness.target, witness.view);
        effects.extend(self.retry_target(witness.target));
        effects
    }

    fn accepted_suggestions(
        &self,
        target: View,
        view: DirectResolverView,
    ) -> Vec<DirectResolutionSuggest> {
        let Some(state) = self
            .instances
            .get(&target)
            .and_then(|instance| instance.views.get(&view))
        else {
            return Vec::new();
        };
        state
            .suggestions
            .values()
            .filter(|suggest| {
                if suggest.key3_view == 0 {
                    return true;
                }
                suggest.key3_view < view
                    && self.accept_key(target, view, suggest.key3_view, &suggest.key3_value)
                    && self.is_backed(target, &suggest.key3_value)
                    && self
                        .entry(target, &suggest.key3_value)
                        .is_some_and(|entry| self.valid_entry(target, entry))
            })
            .cloned()
            .collect()
    }

    fn try_primary_propose(
        &mut self,
        target: View,
        view: DirectResolverView,
    ) -> Vec<DirectResolutionEffect> {
        let Some(instance) = self.instances.get(&target) else {
            return Vec::new();
        };
        if view != instance.current_view
            || self.resolution_leader(target, view) != self.name
            || instance
                .views
                .get(&view)
                .is_some_and(|state| state.proposal.is_some())
        {
            return Vec::new();
        }
        let mut accepted = self.accepted_suggestions(target, view);
        if accepted.len() < self.quorum {
            return Vec::new();
        }
        accepted.sort_by(|a, b| {
            a.key3_view
                .cmp(&b.key3_view)
                .then_with(|| a.key3_value.cmp(&b.key3_value))
        });
        let max = accepted.last().unwrap();
        let (key_view, entry) = if max.key3_view > 0 {
            let Some(entry) = self.entry(target, &max.key3_value).cloned() else {
                return Vec::new();
            };
            (max.key3_view, entry)
        } else {
            let candidates = &instance.candidates;
            if candidates.is_empty() {
                return Vec::new();
            }
            let index = usize::try_from(view.saturating_sub(1)).unwrap_or(0) % candidates.len();
            (0, candidates[index].clone())
        };
        let value = self.value_digest(&entry);
        let proposal = DirectResolutionProposal {
            target,
            view,
            key_view,
            value,
            entry,
            sender: self.name,
        };
        let mut effects = vec![DirectResolutionEffect::BroadcastProposal(proposal.clone())];
        effects.extend(self.on_proposal(proposal));
        effects
    }

    pub fn on_proposal(
        &mut self,
        proposal: DirectResolutionProposal,
    ) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&proposal.sender)
            || proposal.target == 0
            || proposal.view == 0
            || proposal.key_view >= proposal.view
            || proposal.sender != self.resolution_leader(proposal.target, proposal.view)
            || self.decisions.contains_key(&proposal.target)
            || !self.instances.contains_key(&proposal.target)
        {
            return Vec::new();
        }
        if !self.admits_phase_view(proposal.target, proposal.view)
            || self
                .instances
                .get(&proposal.target)
                .is_some_and(|instance| proposal.view < instance.current_view)
        {
            return Vec::new();
        }
        if self
            .instances
            .get(&proposal.target)
            .and_then(|instance| instance.views.get(&proposal.view))
            .is_some_and(|state| state.proposal.is_some())
        {
            return Vec::new();
        }
        let Some(value) = self.remember_entry(proposal.target, proposal.entry.clone()) else {
            return Vec::new();
        };
        if value != proposal.value {
            return Vec::new();
        }
        let instance = self.instances.get_mut(&proposal.target).unwrap();
        let state = instance.views.entry(proposal.view).or_default();
        state.proposal = Some(proposal.clone());
        self.try_echo(proposal.target, proposal.view)
    }

    fn open_lock(&self, target: View, view: DirectResolverView) -> bool {
        let Some(instance) = self.instances.get(&target) else {
            return false;
        };
        let Some(state) = instance.views.get(&view) else {
            return false;
        };
        state
            .proofs
            .values()
            .filter(|proof| {
                proof.prev_key1 < proof.key1_view
                    && proof.key1_view < view
                    && (instance.lock_view <= proof.prev_key1
                        || (instance.lock_view <= proof.key1_view
                            && proof.key1_value != instance.lock_value))
            })
            .count()
            >= self.f_plus_1
    }

    fn try_echo(&mut self, target: View, view: DirectResolverView) -> Vec<DirectResolutionEffect> {
        let Some(instance) = self.instances.get(&target) else {
            return Vec::new();
        };
        if view != instance.current_view {
            return Vec::new();
        }
        let Some(state) = instance.views.get(&view) else {
            return Vec::new();
        };
        if state.sent.contains_key(&DirectResolutionPhase::Echo) {
            return Vec::new();
        }
        let Some(proposal) = state.proposal.clone() else {
            return Vec::new();
        };
        if proposal.key_view > 0 && !self.is_backed(target, &proposal.value) {
            return Vec::new();
        }
        let lock_ok = instance.lock_view == 0
            || instance.lock_value == proposal.value
            || (view > proposal.key_view
                && proposal.key_view >= instance.lock_view
                && self.open_lock(target, view));
        if !lock_ok {
            return Vec::new();
        }
        match state.vote_status {
            VoteStatus::Accepted => {
                return self.emit_statement(
                    target,
                    view,
                    DirectResolutionPhase::Echo,
                    proposal.value,
                    state.vote_origin,
                );
            }
            VoteStatus::Requested | VoteStatus::Rejected => return Vec::new(),
            VoteStatus::Unknown => {}
        }
        self.instances
            .get_mut(&target)
            .unwrap()
            .views
            .get_mut(&view)
            .unwrap()
            .vote_status = VoteStatus::Requested;
        vec![DirectResolutionEffect::ValidateVote {
            target,
            view,
            value: proposal.value,
            entry: proposal.entry,
            fresh: proposal.key_view == 0,
        }]
    }

    pub fn on_vote(
        &mut self,
        target: View,
        view: DirectResolverView,
        value: Digest,
        vote: DirectResolutionVote,
    ) -> Vec<DirectResolutionEffect> {
        let Some(state) = self
            .instances
            .get_mut(&target)
            .and_then(|instance| instance.views.get_mut(&view))
        else {
            return Vec::new();
        };
        let proposal_matches = state
            .proposal
            .as_ref()
            .is_some_and(|proposal| proposal.value == value);
        if state.vote_status != VoteStatus::Requested || !proposal_matches {
            return Vec::new();
        }
        match vote {
            DirectResolutionVote::Reject => {
                state.vote_status = VoteStatus::Rejected;
                Vec::new()
            }
            DirectResolutionVote::Accept { origin } => {
                if origin.is_some_and(|bit| bit > 1) {
                    state.vote_status = VoteStatus::Rejected;
                    return Vec::new();
                }
                state.vote_status = VoteStatus::Accepted;
                state.vote_origin = origin;
                self.try_echo(target, view)
            }
        }
    }

    /// Retries only previously rejected external-validity checks.  Callers use
    /// this after AGB or lane state changes, never in a direct-message loop.
    pub fn retry_external_validity(&mut self) -> Vec<DirectResolutionEffect> {
        let coordinates: Vec<_> = self
            .instances
            .iter_mut()
            .filter_map(|(target, instance)| {
                let view = instance.current_view;
                let state = instance.views.get_mut(&view)?;
                if state.vote_status != VoteStatus::Rejected {
                    return None;
                }
                state.vote_status = VoteStatus::Unknown;
                Some((*target, view))
            })
            .collect();
        coordinates
            .into_iter()
            .flat_map(|(target, view)| self.try_echo(target, view))
            .collect()
    }

    fn emit_statement(
        &mut self,
        target: View,
        view: DirectResolverView,
        phase: DirectResolutionPhase,
        value: Digest,
        origin: Option<u8>,
    ) -> Vec<DirectResolutionEffect> {
        let Some(instance) = self.instances.get_mut(&target) else {
            return Vec::new();
        };
        let state = instance.views.entry(view).or_default();
        if state.sent.contains_key(&phase) {
            return Vec::new();
        }
        let origin = (phase == DirectResolutionPhase::Echo)
            .then_some(origin)
            .flatten();
        state.sent.insert(phase, value.clone());
        state.statements.entry(phase).or_default().insert(
            self.name,
            StatementRecord {
                value: value.clone(),
                origin,
            },
        );
        vec![DirectResolutionEffect::BroadcastStatement(
            DirectResolutionStatement {
                target,
                view,
                value,
                phase,
                origin,
                sender: self.name,
            },
        )]
    }

    pub fn on_statement(
        &mut self,
        statement: DirectResolutionStatement,
    ) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&statement.sender)
            || statement.target == 0
            || statement.view == 0
            || statement.origin.is_some_and(|bit| bit > 1)
            || (statement.phase != DirectResolutionPhase::Echo && statement.origin.is_some())
            || self.decisions.contains_key(&statement.target)
            || !self.instances.contains_key(&statement.target)
        {
            return Vec::new();
        }
        if !self.admits_phase_view(statement.target, statement.view) {
            return Vec::new();
        }
        let current_view = self
            .instances
            .get(&statement.target)
            .map_or(0, |instance| instance.current_view);
        let instance = self.instances.get_mut(&statement.target).unwrap();
        let state = instance.views.entry(statement.view).or_default();
        let phase = state.statements.entry(statement.phase).or_default();
        if phase.contains_key(&statement.sender) {
            return Vec::new();
        }
        phase.insert(
            statement.sender,
            StatementRecord {
                value: statement.value,
                origin: statement.origin,
            },
        );
        // Late KEY1 statements remain useful because Backed is a stable
        // target-local predicate used by later views.  Other old-view phases
        // are retained but cannot advance an obsolete view.
        let mut effects = self.try_primary_propose(statement.target, current_view);
        effects.extend(self.try_echo(statement.target, current_view));
        if statement.view == current_view {
            effects.extend(self.advance_view(statement.target, statement.view));
        }
        effects
    }

    fn phase_winner(
        &self,
        target: View,
        view: DirectResolverView,
        phase: DirectResolutionPhase,
        threshold: usize,
    ) -> Option<Digest> {
        let statements = self
            .instances
            .get(&target)?
            .views
            .get(&view)?
            .statements
            .get(&phase)?;
        let mut counts: BTreeMap<Digest, usize> = BTreeMap::new();
        for statement in statements.values() {
            *counts.entry(statement.value.clone()).or_default() += 1;
        }
        counts
            .into_iter()
            .find_map(|(value, count)| (count >= threshold).then_some(value))
    }

    fn phase_authors(
        &self,
        target: View,
        view: DirectResolverView,
        phase: DirectResolutionPhase,
        value: &Digest,
    ) -> Vec<PublicKey> {
        self.instances
            .get(&target)
            .and_then(|instance| instance.views.get(&view))
            .and_then(|state| state.statements.get(&phase))
            .into_iter()
            .flat_map(|statements| statements.iter())
            .filter_map(|(sender, statement)| (&statement.value == value).then_some(*sender))
            .collect()
    }

    fn fresh_origin_ok(&self, target: View, view: DirectResolverView, value: &Digest) -> bool {
        let Some(instance) = self.instances.get(&target) else {
            return false;
        };
        let Some(state) = instance.views.get(&view) else {
            return false;
        };
        let Some(proposal) = state.proposal.as_ref() else {
            return false;
        };
        if proposal.value != *value {
            return false;
        }
        if proposal.key_view > 0 || matches!(proposal.entry, ResolutionEntry::Skip(_)) {
            return true;
        }
        state
            .statements
            .get(&DirectResolutionPhase::Echo)
            .map_or(0, |statements| {
                statements
                    .values()
                    .filter(|statement| statement.value == *value && statement.origin == Some(1))
                    .count()
            })
            >= self.f_plus_1
    }

    fn ensure_value_fetch(
        &mut self,
        target: View,
        value: Digest,
        peers: Vec<PublicKey>,
    ) -> Vec<DirectResolutionEffect> {
        if self.entry(target, &value).is_some() {
            return Vec::new();
        }
        self.pending_fetch
            .entry(target)
            .or_default()
            .insert(value.clone());
        peers
            .into_iter()
            .filter(|peer| {
                self.fetch_requested
                    .entry(target)
                    .or_default()
                    .insert((value.clone(), *peer))
            })
            .map(|peer| {
                DirectResolutionEffect::ValueFetchTo(
                    peer,
                    DirectResolutionValueFetch {
                        target,
                        value: value.clone(),
                        requester: self.name,
                    },
                )
            })
            .collect()
    }

    fn advance_view(
        &mut self,
        target: View,
        view: DirectResolverView,
    ) -> Vec<DirectResolutionEffect> {
        if self
            .instances
            .get(&target)
            .is_none_or(|instance| view != instance.current_view)
        {
            return Vec::new();
        }
        let mut effects = Vec::new();
        loop {
            let mut progressed = false;
            for (from, to) in [
                (DirectResolutionPhase::Echo, DirectResolutionPhase::Key1),
                (DirectResolutionPhase::Key1, DirectResolutionPhase::Key2),
                (DirectResolutionPhase::Key2, DirectResolutionPhase::Key3),
                (DirectResolutionPhase::Key3, DirectResolutionPhase::Lock),
            ] {
                let Some(value) = self.phase_winner(target, view, from, self.quorum) else {
                    continue;
                };
                let already = self
                    .instances
                    .get(&target)
                    .and_then(|instance| instance.views.get(&view))
                    .is_some_and(|state| state.sent.contains_key(&to));
                if already {
                    continue;
                }
                if self
                    .entry(target, &value)
                    .is_none_or(|entry| !self.valid_entry(target, entry))
                {
                    let peers = self.phase_authors(target, view, from, &value);
                    effects.extend(self.ensure_value_fetch(target, value, peers));
                    return effects;
                }
                if from == DirectResolutionPhase::Echo {
                    if !self.fresh_origin_ok(target, view, &value) {
                        continue;
                    }
                    let proposal_is_fresh = self
                        .instances
                        .get(&target)
                        .and_then(|instance| instance.views.get(&view))
                        .and_then(|state| state.proposal.as_ref())
                        .is_some_and(|proposal| proposal.key_view == 0);
                    if proposal_is_fresh && !self.is_backed(target, &value) {
                        effects.extend(self.emit_backing_witness(target, view, value.clone()));
                        effects.extend(self.recheck_backing(target, view));
                        continue;
                    }
                }
                if let Some(instance) = self.instances.get_mut(&target) {
                    match to {
                        DirectResolutionPhase::Key1 => {
                            if instance.key1_value != value {
                                instance.prev_key1 = instance.key1_view;
                                instance.key1_value = value.clone();
                            }
                            instance.key1_view = view;
                        }
                        DirectResolutionPhase::Key2 => {
                            if instance.key2_value != value {
                                instance.prev_key2 = instance.key2_view;
                                instance.key2_value = value.clone();
                            }
                            instance.key2_view = view;
                        }
                        DirectResolutionPhase::Key3 => {
                            instance.key3_view = view;
                            instance.key3_value = value.clone();
                        }
                        DirectResolutionPhase::Lock => {
                            instance.lock_view = view;
                            instance.lock_value = value.clone();
                        }
                        DirectResolutionPhase::Echo => unreachable!(),
                    }
                }
                effects.extend(self.emit_statement(target, view, to, value, None));
                progressed = true;
            }

            if let Some(value) =
                self.phase_winner(target, view, DirectResolutionPhase::Lock, self.quorum)
            {
                let sent = self
                    .instances
                    .get(&target)
                    .and_then(|instance| instance.done_sent.clone());
                if sent.is_none() {
                    let Some(entry) = self.entry(target, &value).cloned() else {
                        let peers =
                            self.phase_authors(target, view, DirectResolutionPhase::Lock, &value);
                        effects.extend(self.ensure_value_fetch(target, value, peers));
                        return effects;
                    };
                    effects.extend(self.emit_done(target, value, entry));
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        effects.extend(self.recheck_done(target));
        effects
    }

    fn emit_done(
        &mut self,
        target: View,
        value: Digest,
        entry: ResolutionEntry,
    ) -> Vec<DirectResolutionEffect> {
        if !self.valid_entry(target, &entry) || self.value_digest(&entry) != value {
            return Vec::new();
        }
        let Some(instance) = self.instances.get_mut(&target) else {
            return Vec::new();
        };
        if instance.done_sent.is_some() {
            return Vec::new();
        }
        instance.done_sent = Some(value.clone());
        instance.done.insert(self.name, value.clone());
        vec![DirectResolutionEffect::BroadcastDone(
            DirectResolutionDone {
                target,
                value,
                entry,
                sender: self.name,
            },
        )]
    }

    pub fn on_done(&mut self, done: DirectResolutionDone) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&done.sender)
            || done.target == 0
            || self.value_digest(&done.entry) != done.value
            || !self.valid_entry(done.target, &done.entry)
        {
            return Vec::new();
        }
        if self.decisions.contains_key(&done.target) || self.terminals.contains(&done.target) {
            return Vec::new();
        }
        if !self.instances.contains_key(&done.target) {
            return Vec::new();
        }
        if self
            .instances
            .get(&done.target)
            .is_some_and(|instance| instance.done.contains_key(&done.sender))
        {
            return Vec::new();
        }
        self.remember_entry(done.target, done.entry);
        let instance = self.instances.get_mut(&done.target).unwrap();
        instance.done.insert(done.sender, done.value);
        self.recheck_done(done.target)
    }

    fn done_winner(&self, target: View, threshold: usize) -> Option<Digest> {
        let mut counts: BTreeMap<Digest, usize> = BTreeMap::new();
        for value in self.instances.get(&target)?.done.values() {
            *counts.entry(value.clone()).or_default() += 1;
        }
        counts
            .into_iter()
            .find_map(|(value, count)| (count >= threshold).then_some(value))
    }

    fn recheck_done(&mut self, target: View) -> Vec<DirectResolutionEffect> {
        let mut effects = Vec::new();
        if self
            .instances
            .get(&target)
            .and_then(|instance| instance.done_sent.clone())
            .is_none()
        {
            if let Some(value) = self.done_winner(target, self.f_plus_1) {
                if let Some(entry) = self.entry(target, &value).cloned() {
                    effects.extend(self.emit_done(target, value, entry));
                }
            }
        }
        if let Some(value) = self.done_winner(target, self.quorum) {
            if let Some(entry) = self.entry(target, &value).cloned() {
                self.decisions.insert(target, (value, entry.clone()));
                self.instances.remove(&target);
                self.pending_fetch.remove(&target);
                self.fetch_requested.remove(&target);
                self.fetch_answered.remove(&target);
                effects.push(DirectResolutionEffect::Decide(entry));
            }
        }
        effects
    }

    pub fn on_value_fetch(
        &mut self,
        fetch: DirectResolutionValueFetch,
    ) -> Vec<DirectResolutionEffect> {
        if !self.is_member(&fetch.requester) || fetch.target == 0 {
            return Vec::new();
        }
        let Some(entry) = self.entry(fetch.target, &fetch.value).cloned() else {
            return Vec::new();
        };
        let key = (fetch.value.clone(), fetch.requester);
        // `ValueServeTo` uses the reliable sender. Once queued, the transport
        // retains it across reconnects, so this at-most-once mark is final.
        if !self
            .fetch_answered
            .entry(fetch.target)
            .or_default()
            .insert(key)
        {
            return Vec::new();
        }
        vec![DirectResolutionEffect::ValueServeTo(
            fetch.requester,
            DirectResolutionValueServe {
                target: fetch.target,
                value: fetch.value,
                entry,
            },
        )]
    }

    pub fn on_value_serve(
        &mut self,
        serve: DirectResolutionValueServe,
    ) -> Vec<DirectResolutionEffect> {
        if self.value_digest(&serve.entry) != serve.value
            || !self.valid_entry(serve.target, &serve.entry)
            || !self
                .pending_fetch
                .get_mut(&serve.target)
                .is_some_and(|values| values.remove(&serve.value))
        {
            return Vec::new();
        }
        if self
            .pending_fetch
            .get(&serve.target)
            .is_some_and(BTreeSet::is_empty)
        {
            self.pending_fetch.remove(&serve.target);
        }
        self.remember_entry(serve.target, serve.entry);
        self.retry_target(serve.target)
    }

    fn retry_target(&mut self, target: View) -> Vec<DirectResolutionEffect> {
        let Some(view) = self
            .instances
            .get(&target)
            .map(|instance| instance.current_view)
        else {
            return Vec::new();
        };
        let mut effects = self.try_primary_propose(target, view);
        effects.extend(self.try_echo(target, view));
        effects.extend(self.advance_view(target, view));
        effects.extend(self.recheck_done(target));
        effects
    }

    pub fn is_decided(&self, target: View) -> bool {
        self.decisions.contains_key(&target)
    }

    pub fn decision(&self, target: View) -> Option<&ResolutionEntry> {
        self.decisions.get(&target).map(|(_, entry)| entry)
    }

    /// Stops redundant resolver work after another compatible Vantage path
    /// has already sealed the target.  The ordered sequence/checkpoint path is
    /// responsible for lagging-node recovery in this case.
    pub fn note_terminal(&mut self, target: View) {
        self.terminals.insert(target);
        self.instances.remove(&target);
        self.pending_fetch.remove(&target);
        self.fetch_requested.remove(&target);
        self.fetch_answered.remove(&target);
    }

    pub fn current_view(&self, target: View) -> DirectResolverView {
        self.instances
            .get(&target)
            .map_or(0, |instance| instance.current_view)
    }

    pub fn max_current_view(&self) -> DirectResolverView {
        self.instances
            .values()
            .map(|instance| instance.current_view)
            .max()
            .unwrap_or(0)
    }

    pub fn active_len(&self) -> usize {
        self.instances.len()
    }

    /// Drops target-local resolver history below the caller's retained
    /// sequence floor.  Decisions below that floor are recovered through the
    /// existing sequence checkpoint path, not by replaying ancient DONEs.
    pub fn gc_below(&mut self, floor: View) {
        if floor == 0 {
            return;
        }
        self.instances = self.instances.split_off(&floor);
        self.values = self.values.split_off(&(floor, Digest::default()));
        self.decisions = self.decisions.split_off(&floor);
        self.terminals = self.terminals.split_off(&floor);
        self.pending_fetch = self.pending_fetch.split_off(&floor);
        self.fetch_requested = self.fetch_requested.split_off(&floor);
        self.fetch_answered = self.fetch_answered.split_off(&floor);
    }

    #[cfg(test)]
    pub(crate) fn backed_for_test(&self, target: View, value: &Digest) -> bool {
        self.is_backed(target, value)
    }

    #[cfg(test)]
    pub(crate) fn buffered_views_for_test(&self, target: View) -> usize {
        self.instances
            .get(&target)
            .map_or(0, |instance| instance.views.len())
    }
}
