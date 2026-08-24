//! On-demand agreement for resolution-bearing Vantage proposals.
//!
//! A completed carrier first gathers stable, first-hand witnesses.  A quorum
//! makes its hash eligible and activates a WISH-synchronized resolver height.
//! Each resolver view then follows the IT-HS path: suggestions and lock-opening
//! proofs, a leader proposal, ECHO, KEY1--KEY3, LOCK, and DONE.  A DONE quorum
//! appends one nonempty block to the hash chain and applies its carrier anchors.
//!
//! Witnesses, carrier bodies, and decided chain blocks are intentionally
//! retained until a future certified resolver checkpoint can replace them.

use crate::leader::one_based_authority;
use crate::primary::View;
use crate::vantage::agb::{Outcome, ProposalOut, ResolutionEntry};
use crate::vantage::block::{self, BlockRef};
use crate::vantage::{Effect, Thresholds};
use config::Committee;
use crypto::{Digest, PublicKey};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

pub type ResolutionHeight = u64;
pub type ResolverView = u64;

/// The default cap keeps one resolution value bounded while allowing burst drain.
pub const DEFAULT_RESOLUTION_BATCH_CAP: usize = 16;

#[cfg(feature = "benchmark")]
fn recovery_epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock precedes UNIX epoch")
        .as_millis()
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AnchorRef {
    pub view: View,
    pub digest: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResolutionBlock {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub anchors: Vec<AnchorRef>,
}

impl ResolutionBlock {
    pub fn digest(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("ResolutionBlock always serializes");
        block::domain_hash(b"vantage-resolution-block", sid, &bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionWitness {
    pub carrier_view: View,
    pub carrier_digest: Digest,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionWish {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub view: ResolverView,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionSuggest {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub view: ResolverView,
    pub sender: PublicKey,
    pub key3_view: ResolverView,
    pub key3_value: Digest,
    pub key2_view: ResolverView,
    pub key2_value: Digest,
    pub prev_key2: ResolverView,
    pub block: Option<ResolutionBlock>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionProof {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub view: ResolverView,
    pub sender: PublicKey,
    pub key1_view: ResolverView,
    pub key1_value: Digest,
    pub prev_key1: ResolverView,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionProposal {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub view: ResolverView,
    pub key_view: ResolverView,
    pub value: Digest,
    pub block: ResolutionBlock,
    pub sender: PublicKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ResolutionPhase {
    Echo,
    Key1,
    Key2,
    Key3,
    Lock,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionStatement {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub view: ResolverView,
    pub value: Digest,
    pub phase: ResolutionPhase,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionDone {
    pub height: ResolutionHeight,
    pub parent: Digest,
    pub value: Digest,
    pub block: ResolutionBlock,
    pub sender: PublicKey,
}

/// Retained solely so old bincode enum variants keep their layout and indices.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LegacyControlProposal {
    pub round: u64,
    pub parent: u64,
    pub value: Option<(View, Digest)>,
}

#[derive(Default)]
struct ResolverViewState {
    suggestions: HashMap<PublicKey, ResolutionSuggest>,
    proofs: HashMap<PublicKey, ResolutionProof>,
    proposal: Option<ResolutionProposal>,
    sent: HashMap<ResolutionPhase, Digest>,
    statements: HashMap<ResolutionPhase, HashMap<PublicKey, Digest>>,
}

struct ActiveHeight {
    height: ResolutionHeight,
    parent: Digest,
    own_wish: ResolverView,
    entered_through: ResolverView,
    current_view: ResolverView,
    wishes: HashMap<PublicKey, ResolverView>,
    views: BTreeMap<ResolverView, ResolverViewState>,

    key1_view: ResolverView,
    key1_value: Digest,
    prev_key1: ResolverView,
    key2_view: ResolverView,
    key2_value: Digest,
    prev_key2: ResolverView,
    key3_view: ResolverView,
    key3_value: Digest,
    lock_view: ResolverView,
    lock_value: Digest,

    done_sent: Option<Digest>,
    done: HashMap<PublicKey, Digest>,
}

impl ActiveHeight {
    fn new(height: ResolutionHeight, parent: Digest) -> Self {
        Self {
            height,
            parent,
            own_wish: 0,
            entered_through: 0,
            current_view: 0,
            wishes: HashMap::new(),
            views: BTreeMap::new(),
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

/// On-demand, hash-chained resolver with stable witnesses and an IT-HS core.
pub struct ResolutionChain {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    delta: Duration,
    batch_cap: usize,
    f_plus_1: usize,
    quorum: usize,

    witness_statements: BTreeMap<View, HashMap<PublicKey, Digest>>,
    witnessed: BTreeSet<View>,
    carrier_bodies: BTreeMap<(View, Digest), ProposalOut>,
    eligible: BTreeSet<AnchorRef>,
    decided_anchors: BTreeSet<AnchorRef>,
    anchored_targets: BTreeSet<View>,
    /// Targets sealed locally by any data-plane path. This is only a batching
    /// priority hint; `anchored_targets` retains its proof-facing meaning.
    locally_sealed_targets: BTreeSet<View>,

    decided_height: ResolutionHeight,
    head: Digest,
    decided_blocks: BTreeMap<ResolutionHeight, ResolutionBlock>,
    resolution_blocks: HashMap<Digest, ResolutionBlock>,
    active: Option<ActiveHeight>,
    future_height_hints: BTreeMap<PublicKey, ResolutionHeight>,
    decision_request_pending: Option<ResolutionHeight>,

    pending_carrier_fetch: BTreeSet<(View, Digest)>,
    carrier_fetch_requested: BTreeMap<(View, Digest), BTreeSet<PublicKey>>,
    carrier_fetch_answered: BTreeSet<(View, Digest, PublicKey)>,
    pending_block_fetch: BTreeMap<ResolutionHeight, BTreeSet<Digest>>,
    resolved_target_floor: View,
}

impl ResolutionChain {
    pub fn new(name: PublicKey, committee: Committee, sid: Digest, delta_ms: u64) -> Self {
        Self::new_with_batch_cap(name, committee, sid, delta_ms, DEFAULT_RESOLUTION_BATCH_CAP)
    }

    pub fn new_with_batch_cap(
        name: PublicKey,
        committee: Committee,
        sid: Digest,
        delta_ms: u64,
        batch_cap: usize,
    ) -> Self {
        assert!(batch_cap > 0, "resolution batch cap must be positive");
        let thresholds = Thresholds::from_party_count(committee.size());
        let genesis = block::domain_hash(b"vantage-resolution-genesis", &sid, b"genesis");
        Self {
            name,
            committee,
            sid,
            delta: Duration::from_millis(delta_ms),
            batch_cap,
            f_plus_1: thresholds.f_plus_1_parties,
            quorum: thresholds.n_minus_f_parties,
            witness_statements: BTreeMap::new(),
            witnessed: BTreeSet::new(),
            carrier_bodies: BTreeMap::new(),
            eligible: BTreeSet::new(),
            decided_anchors: BTreeSet::new(),
            anchored_targets: BTreeSet::new(),
            locally_sealed_targets: BTreeSet::new(),
            decided_height: 0,
            head: genesis,
            decided_blocks: BTreeMap::new(),
            resolution_blocks: HashMap::new(),
            active: None,
            future_height_hints: BTreeMap::new(),
            decision_request_pending: None,
            pending_carrier_fetch: BTreeSet::new(),
            carrier_fetch_requested: BTreeMap::new(),
            carrier_fetch_answered: BTreeSet::new(),
            pending_block_fetch: BTreeMap::new(),
            resolved_target_floor: 1,
        }
    }

    /// There is deliberately no genesis-started or empty resolver round.
    pub fn genesis(&mut self) -> Vec<Effect> {
        Vec::new()
    }

    pub fn resolver_timeout(&self) -> Duration {
        self.delta * 11
    }

    pub fn resolution_leader(&self, height: ResolutionHeight, view: ResolverView) -> PublicKey {
        one_based_authority(
            &self.committee,
            height.saturating_add(view).saturating_sub(1),
        )
    }

    fn is_member(&self, sender: &PublicKey) -> bool {
        self.committee.stake(sender) > 0
    }

    fn verify_carrier(&self, view: View, digest: &Digest, proposal: &ProposalOut) -> bool {
        proposal.view() == view
            && !proposal.entries().is_empty()
            && proposal.digest(&self.sid) == *digest
            && proposal.formed(&self.committee)
    }

    fn held_carrier(&self, anchor: &AnchorRef) -> bool {
        self.carrier_bodies
            .get(&(anchor.view, anchor.digest.clone()))
            .is_some_and(|body| self.verify_carrier(anchor.view, &anchor.digest, body))
    }

    fn matching_witness_count(&self, view: View, digest: &Digest) -> usize {
        self.witness_statements
            .get(&view)
            .map_or(0, |m| m.values().filter(|d| *d == digest).count())
    }

    fn matching_witness_authors(&self, view: View, digest: &Digest) -> Vec<PublicKey> {
        self.witness_statements
            .get(&view)
            .into_iter()
            .flat_map(|m| m.iter())
            .filter_map(|(sender, d)| (d == digest).then_some(*sender))
            .collect()
    }

    fn emit_witness(&mut self, view: View, digest: Digest) -> Vec<Effect> {
        if self.witnessed.contains(&view) {
            return Vec::new();
        }
        let key = (view, digest.clone());
        let Some(body) = self.carrier_bodies.get(&key) else {
            return Vec::new();
        };
        if !self.verify_carrier(view, &digest, body) {
            return Vec::new();
        }
        self.witnessed.insert(view);
        self.witness_statements
            .entry(view)
            .or_default()
            .insert(self.name, digest.clone());
        vec![Effect::BroadcastResolutionWitness(ResolutionWitness {
            carrier_view: view,
            carrier_digest: digest,
            sender: self.name,
        })]
    }

    pub fn on_completion_reportable(&mut self, view: View, proposal: ProposalOut) -> Vec<Effect> {
        if proposal.view() != view || proposal.entries().is_empty() {
            return Vec::new();
        }
        let digest = proposal.digest(&self.sid);
        if !self.verify_carrier(view, &digest, &proposal) {
            return Vec::new();
        }
        self.carrier_bodies
            .entry((view, digest.clone()))
            .or_insert(proposal);
        let mut effects = self.emit_witness(view, digest.clone());
        effects.extend(self.recheck_witness(view, digest));
        effects
    }

    pub fn on_resolution_witness(&mut self, witness: ResolutionWitness) -> Vec<Effect> {
        if !self.is_member(&witness.sender) {
            return Vec::new();
        }
        let statements = self
            .witness_statements
            .entry(witness.carrier_view)
            .or_default();
        if statements.contains_key(&witness.sender) {
            return Vec::new();
        }
        statements.insert(witness.sender, witness.carrier_digest.clone());
        self.recheck_witness(witness.carrier_view, witness.carrier_digest)
    }

    fn recheck_witness(&mut self, view: View, digest: Digest) -> Vec<Effect> {
        let mut effects = Vec::new();
        let anchor = AnchorRef {
            view,
            digest: digest.clone(),
        };
        if !self.held_carrier(&anchor) {
            effects.extend(self.ensure_carrier_fetch(view, digest));
            return effects;
        }
        let count = self.matching_witness_count(view, &digest);
        if count >= self.f_plus_1 && !self.witnessed.contains(&view) {
            effects.extend(self.emit_witness(view, digest.clone()));
        }
        if self.matching_witness_count(view, &digest) >= self.quorum && self.eligible.insert(anchor)
        {
            let targets: Vec<_> = self
                .carrier_bodies
                .get(&(view, digest.clone()))
                .expect("held carrier checked above")
                .entries()
                .iter()
                .map(ResolutionEntry::target_view)
                .collect();
            #[cfg(feature = "benchmark")]
            log::info!(
                "VANTAGE_RECOVERY_EVENT kind=resolver_eligible view={} epoch_ms={} height={} targets={} pending={}",
                view,
                recovery_epoch_ms(),
                self.active
                    .as_ref()
                    .map_or(self.decided_height + 1, |active| active.height),
                targets.len(),
                self.pending_anchors().len()
            );
            effects.push(Effect::ResolutionCarrierEligible(targets));
            effects.extend(self.activate_if_pending());
            effects.extend(self.retry_active());
        }
        effects
    }

    fn ensure_carrier_fetch(&mut self, view: View, digest: Digest) -> Vec<Effect> {
        if self.carrier_bodies.contains_key(&(view, digest.clone())) {
            return Vec::new();
        }
        self.pending_carrier_fetch.insert((view, digest.clone()));
        self.matching_witness_authors(view, &digest)
            .into_iter()
            .filter(|peer| {
                self.carrier_fetch_requested
                    .entry((view, digest.clone()))
                    .or_default()
                    .insert(*peer)
            })
            .map(|peer| Effect::ResolutionCarrierFetchTo(peer, view, digest.clone()))
            .collect()
    }

    pub fn on_carrier_fetch(
        &mut self,
        requester: PublicKey,
        view: View,
        digest: Digest,
    ) -> Vec<Effect> {
        if !self.is_member(&requester) {
            return Vec::new();
        }
        let key = (view, digest.clone(), requester);
        if self.carrier_fetch_answered.contains(&key) {
            return Vec::new();
        }
        let Some(body) = self.carrier_bodies.get(&(view, digest.clone())) else {
            return Vec::new();
        };
        if !self.verify_carrier(view, &digest, body) {
            return Vec::new();
        }
        // The runtime dispatches this effect through its reliable sender.  It
        // is therefore safe to mark the request answered before emitting the
        // at-most-once serve effect.
        self.carrier_fetch_answered.insert(key);
        vec![Effect::ResolutionCarrierServeTo(
            requester,
            view,
            body.clone(),
        )]
    }

    pub fn on_carrier_serve(&mut self, view: View, proposal: ProposalOut) -> Vec<Effect> {
        if proposal.view() != view {
            return Vec::new();
        }
        let digest = proposal.digest(&self.sid);
        if !self.pending_carrier_fetch.contains(&(view, digest.clone()))
            || !self.verify_carrier(view, &digest, &proposal)
        {
            return Vec::new();
        }
        self.pending_carrier_fetch.remove(&(view, digest.clone()));
        self.carrier_fetch_requested.remove(&(view, digest.clone()));
        self.carrier_bodies.insert((view, digest.clone()), proposal);
        self.recheck_witness(view, digest)
    }

    fn pending_anchors(&self) -> Vec<AnchorRef> {
        self.eligible
            .iter()
            .filter(|a| !self.decided_anchors.contains(*a))
            .cloned()
            .collect()
    }

    fn unresolved_targets(&self, anchor: &AnchorRef) -> BTreeSet<View> {
        self.carrier_bodies
            .get(&(anchor.view, anchor.digest.clone()))
            .into_iter()
            .flat_map(ProposalOut::entries)
            .map(ResolutionEntry::target_view)
            .filter(|target| {
                !self.is_anchor_resolved(*target) && !self.locally_sealed_targets.contains(target)
            })
            .collect()
    }

    /// Selects a bounded, deterministic block without sacrificing anchor
    /// fairness. The oldest anchor is always retained; remaining slots first
    /// add distinct unresolved targets and then fill in lexicographic order.
    fn select_pending_anchors(&self) -> Vec<AnchorRef> {
        let pending = self.pending_anchors();
        let Some(oldest) = pending.first().cloned() else {
            return Vec::new();
        };
        if pending.len() <= self.batch_cap {
            return pending;
        }

        let mut selected = vec![oldest.clone()];
        let mut selected_set = BTreeSet::from([oldest.clone()]);
        let mut covered = self.unresolved_targets(&oldest);

        for anchor in pending.iter().skip(1) {
            if selected.len() == self.batch_cap {
                break;
            }
            let targets = self.unresolved_targets(anchor);
            if targets.iter().any(|target| !covered.contains(target)) {
                covered.extend(targets);
                selected.push(anchor.clone());
                selected_set.insert(anchor.clone());
            }
        }

        if selected.len() < self.batch_cap {
            for anchor in &pending {
                if selected.len() == self.batch_cap {
                    break;
                }
                if selected_set.insert(anchor.clone()) {
                    selected.push(anchor.clone());
                }
            }
        }
        selected.sort();
        debug_assert_eq!(selected.len(), self.batch_cap);
        debug_assert!(selected.contains(&oldest));
        selected
    }

    fn activate_if_pending(&mut self) -> Vec<Effect> {
        if self.pending_anchors().is_empty() {
            return Vec::new();
        }
        if self.active.is_none() {
            self.active = Some(ActiveHeight::new(
                self.decided_height + 1,
                self.head.clone(),
            ));
        }
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.own_wish > 0)
        {
            return Vec::new();
        }
        let mut effects = self.raise_own_wish(1);
        effects.extend(self.recheck_wishes());
        effects
    }

    fn ensure_active_coordinate(&mut self, height: ResolutionHeight, parent: &Digest) -> bool {
        if height != self.decided_height + 1 || parent != &self.head {
            return false;
        }
        if self.active.is_none() {
            self.active = Some(ActiveHeight::new(height, parent.clone()));
        }
        self.active
            .as_ref()
            .is_some_and(|a| a.height == height && &a.parent == parent)
    }

    fn raise_own_wish(&mut self, view: ResolverView) -> Vec<Effect> {
        let Some(active) = self.active.as_mut() else {
            return Vec::new();
        };
        if view <= active.own_wish {
            return Vec::new();
        }
        active.own_wish = view;
        active.wishes.insert(self.name, view);
        vec![Effect::BroadcastResolutionWish(ResolutionWish {
            height: active.height,
            parent: active.parent.clone(),
            view,
            sender: self.name,
        })]
    }

    pub fn on_resolution_wish(&mut self, wish: ResolutionWish) -> Vec<Effect> {
        if !self.is_member(&wish.sender) || wish.view == 0 {
            return Vec::new();
        }
        if wish.height <= self.decided_height {
            return self.on_decision_request(wish.height, wish.sender);
        }
        if wish.height > self.decided_height + 1 {
            return self.record_future_height_hint(wish.sender, wish.height.saturating_sub(1));
        }
        if !self.ensure_active_coordinate(wish.height, &wish.parent) {
            return Vec::new();
        }
        let active = self.active.as_mut().unwrap();
        let slot = active.wishes.entry(wish.sender).or_default();
        if wish.view <= *slot {
            return Vec::new();
        }
        *slot = wish.view;
        self.recheck_wishes()
    }

    fn kth_largest(values: impl Iterator<Item = ResolverView>, k: usize) -> ResolverView {
        let mut values: Vec<_> = values.collect();
        values.sort_unstable_by(|a, b| b.cmp(a));
        values.get(k.saturating_sub(1)).copied().unwrap_or(0)
    }

    fn record_future_height_hint(
        &mut self,
        sender: PublicKey,
        decided_through: ResolutionHeight,
    ) -> Vec<Effect> {
        let high_watermark = self.future_height_hints.entry(sender).or_default();
        *high_watermark = (*high_watermark).max(decided_through);
        self.maybe_request_missing_decision()
    }

    fn maybe_request_missing_decision(&mut self) -> Vec<Effect> {
        let missing = self.decided_height.saturating_add(1);
        let supported =
            Self::kth_largest(self.future_height_hints.values().copied(), self.f_plus_1);
        if supported < missing || self.decision_request_pending == Some(missing) {
            return Vec::new();
        }
        self.decision_request_pending = Some(missing);
        vec![Effect::BroadcastResolutionDecisionRequest(
            missing, self.name,
        )]
    }

    fn recheck_wishes(&mut self) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        let amplify = Self::kth_largest(active.wishes.values().copied(), self.f_plus_1);
        let mut effects = self.raise_own_wish(amplify);
        let Some(active) = self.active.as_ref() else {
            return effects;
        };
        let enter = Self::kth_largest(active.wishes.values().copied(), self.quorum);
        let start = active.entered_through.saturating_add(1);
        if enter < start {
            return effects;
        }
        for view in start..=enter {
            effects.extend(self.enter_view(view));
        }
        effects
    }

    fn enter_view(&mut self, view: ResolverView) -> Vec<Effect> {
        let timeout = self.resolver_timeout();
        let (
            height,
            parent,
            key3_view,
            key3_value,
            key2_view,
            key2_value,
            prev_key2,
            key1_view,
            key1_value,
            prev_key1,
        ) = {
            let Some(active) = self.active.as_mut() else {
                return Vec::new();
            };
            if view <= active.entered_through {
                return Vec::new();
            }
            active.entered_through = view;
            active.current_view = view;
            active.views.entry(view).or_default();
            (
                active.height,
                active.parent.clone(),
                active.key3_view,
                active.key3_value.clone(),
                active.key2_view,
                active.key2_value.clone(),
                active.prev_key2,
                active.key1_view,
                active.key1_value.clone(),
                active.prev_key1,
            )
        };
        let leader = self.resolution_leader(height, view);
        #[cfg(feature = "benchmark")]
        log::info!(
            "VANTAGE_RECOVERY_EVENT kind=resolver_enter view={} epoch_ms={} height={} pending={}",
            view,
            recovery_epoch_ms(),
            height,
            self.pending_anchors().len()
        );
        let block = (key3_view > 0)
            .then(|| self.resolution_blocks.get(&key3_value).cloned())
            .flatten();
        let suggest = ResolutionSuggest {
            height,
            parent: parent.clone(),
            view,
            sender: self.name,
            key3_view,
            key3_value,
            key2_view,
            key2_value,
            prev_key2,
            block,
        };
        let proof = ResolutionProof {
            height,
            parent,
            view,
            sender: self.name,
            key1_view,
            key1_value,
            prev_key1,
        };
        let deadline = Instant::now() + timeout;
        let active = self.active.as_mut().unwrap();
        active
            .views
            .get_mut(&view)
            .unwrap()
            .suggestions
            .insert(self.name, suggest.clone());
        active
            .views
            .get_mut(&view)
            .unwrap()
            .proofs
            .insert(self.name, proof.clone());
        let mut effects = vec![
            Effect::ArmResolutionTimer(active.height, view, deadline),
            Effect::BroadcastResolutionProof(proof),
        ];
        if leader != self.name {
            effects.push(Effect::ResolutionSuggestTo(leader, suggest));
        }
        effects.extend(self.try_primary_propose(view));
        effects.extend(self.try_echo(view));
        effects.extend(self.advance_view(view));
        effects
    }

    pub fn on_resolution_timer(
        &mut self,
        height: ResolutionHeight,
        view: ResolverView,
    ) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        if active.height != height || active.current_view != view {
            return Vec::new();
        }
        #[cfg(feature = "benchmark")]
        log::info!(
            "VANTAGE_RECOVERY_EVENT kind=resolver_timeout view={} epoch_ms={} height={} pending={}",
            view,
            recovery_epoch_ms(),
            height,
            self.pending_anchors().len()
        );
        let mut effects = self.raise_own_wish(view.saturating_add(1));
        effects.extend(self.recheck_wishes());
        effects
    }

    fn structurally_valid_block(&self, block: &ResolutionBlock) -> bool {
        block.height == self.decided_height + 1
            && block.parent == self.head
            && !block.anchors.is_empty()
            && block.anchors.len() <= self.batch_cap
            && block.anchors.windows(2).all(|w| w[0] < w[1])
            && block
                .anchors
                .iter()
                .all(|a| !self.decided_anchors.contains(a))
    }

    fn valid_block(&self, block: &ResolutionBlock) -> bool {
        self.structurally_valid_block(block)
            && block
                .anchors
                .iter()
                .all(|a| self.eligible.contains(a) && self.held_carrier(a))
    }

    fn remember_block(&mut self, block: ResolutionBlock) -> Option<Digest> {
        if block.height != self.decided_height + 1 || block.parent != self.head {
            return None;
        }
        let digest = block.digest(&self.sid);
        self.resolution_blocks
            .entry(digest.clone())
            .or_insert(block);
        Some(digest)
    }

    pub fn on_resolution_suggest(&mut self, suggest: ResolutionSuggest) -> Vec<Effect> {
        if !self.is_member(&suggest.sender)
            || suggest.view == 0
            || !self.ensure_active_coordinate(suggest.height, &suggest.parent)
            || self
                .active
                .as_ref()
                .is_some_and(|active| suggest.view < active.current_view)
        {
            return Vec::new();
        }
        if let Some(block) = suggest.block.clone() {
            if block.digest(&self.sid) != suggest.key3_value {
                return Vec::new();
            }
            self.remember_block(block);
        }
        let state = self
            .active
            .as_mut()
            .unwrap()
            .views
            .entry(suggest.view)
            .or_default();
        if state.suggestions.contains_key(&suggest.sender) {
            return Vec::new();
        }
        state.suggestions.insert(suggest.sender, suggest.clone());
        self.try_primary_propose(suggest.view)
    }

    pub fn on_resolution_proof(&mut self, proof: ResolutionProof) -> Vec<Effect> {
        if !self.is_member(&proof.sender)
            || proof.view == 0
            || !self.ensure_active_coordinate(proof.height, &proof.parent)
            || self
                .active
                .as_ref()
                .is_some_and(|active| proof.view < active.current_view)
        {
            return Vec::new();
        }
        let state = self
            .active
            .as_mut()
            .unwrap()
            .views
            .entry(proof.view)
            .or_default();
        if state.proofs.contains_key(&proof.sender) {
            return Vec::new();
        }
        state.proofs.insert(proof.sender, proof.clone());
        self.try_echo(proof.view)
    }

    /// Checks the IT-HS second-key proof that makes a positive KEY3 suggestion usable.
    fn accept_key(&self, view: ResolverView, key: ResolverView, value: &Digest) -> bool {
        if key == 0 {
            return true;
        }
        let Some(state) = self.active.as_ref().and_then(|a| a.views.get(&view)) else {
            return false;
        };
        state
            .suggestions
            .values()
            .filter(|s| {
                s.prev_key2 < s.key2_view
                    && s.key2_view < view
                    && (key <= s.prev_key2 || (key <= s.key2_view && &s.key2_value == value))
            })
            .count()
            >= self.f_plus_1
    }

    fn accepted_suggestions(&self, view: ResolverView) -> Vec<ResolutionSuggest> {
        let Some(state) = self.active.as_ref().and_then(|a| a.views.get(&view)) else {
            return Vec::new();
        };
        state
            .suggestions
            .values()
            .filter(|s| {
                if s.key3_view == 0 {
                    return true;
                }
                if s.key3_view >= view || !self.accept_key(view, s.key3_view, &s.key3_value) {
                    return false;
                }
                self.resolution_blocks
                    .get(&s.key3_value)
                    .is_some_and(|b| self.valid_block(b))
            })
            .cloned()
            .collect()
    }

    fn try_primary_propose(&mut self, view: ResolverView) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        if view != active.current_view
            || self.resolution_leader(active.height, view) != self.name
            || active
                .views
                .get(&view)
                .is_some_and(|state| state.proposal.is_some())
        {
            return Vec::new();
        }
        let mut accepted = self.accepted_suggestions(view);
        if accepted.len() < self.quorum {
            return Vec::new();
        }
        accepted.sort_by(|a, b| {
            a.key3_view
                .cmp(&b.key3_view)
                .then_with(|| a.key3_value.cmp(&b.key3_value))
        });
        let max = accepted.last().cloned().unwrap();
        let (key_view, block) = if max.key3_view > 0 {
            let Some(block) = self.resolution_blocks.get(&max.key3_value).cloned() else {
                return Vec::new();
            };
            if !self.valid_block(&block) {
                return Vec::new();
            }
            (max.key3_view, block)
        } else {
            let anchors = self.select_pending_anchors();
            if anchors.is_empty() {
                return Vec::new();
            }
            (
                0,
                ResolutionBlock {
                    height: active.height,
                    parent: active.parent.clone(),
                    anchors,
                },
            )
        };
        let value = block.digest(&self.sid);
        self.resolution_blocks.insert(value.clone(), block.clone());
        let proposal = ResolutionProposal {
            height: active.height,
            parent: active.parent.clone(),
            view,
            key_view,
            value,
            block,
            sender: self.name,
        };
        let mut effects = vec![Effect::BroadcastResolutionProposal(proposal.clone())];
        effects.extend(self.on_resolution_proposal(proposal));
        effects
    }

    pub fn on_resolution_proposal(&mut self, proposal: ResolutionProposal) -> Vec<Effect> {
        if !self.is_member(&proposal.sender)
            || proposal.view == 0
            || !self.ensure_active_coordinate(proposal.height, &proposal.parent)
            || self
                .active
                .as_ref()
                .is_some_and(|active| proposal.view < active.current_view)
            || proposal.sender != self.resolution_leader(proposal.height, proposal.view)
            || proposal.block.digest(&self.sid) != proposal.value
            || !self.structurally_valid_block(&proposal.block)
        {
            return Vec::new();
        }
        self.remember_block(proposal.block.clone());
        let state = self
            .active
            .as_mut()
            .unwrap()
            .views
            .entry(proposal.view)
            .or_default();
        if state.proposal.is_some() {
            return Vec::new();
        }
        state.proposal = Some(proposal.clone());
        self.try_echo(proposal.view)
    }

    /// Checks the IT-HS first-key proof that permits moving away from a local lock.
    fn open_lock(&self, view: ResolverView) -> bool {
        let Some(active) = self.active.as_ref() else {
            return false;
        };
        let Some(state) = active.views.get(&view) else {
            return false;
        };
        state
            .proofs
            .values()
            .filter(|p| {
                p.prev_key1 < p.key1_view
                    && p.key1_view < view
                    && (active.lock_view <= p.prev_key1
                        || (active.lock_view <= p.key1_view && p.key1_value != active.lock_value))
            })
            .count()
            >= self.f_plus_1
    }

    fn try_echo(&mut self, view: ResolverView) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        if view != active.current_view {
            return Vec::new();
        }
        let Some(state) = active.views.get(&view) else {
            return Vec::new();
        };
        if state.sent.contains_key(&ResolutionPhase::Echo) {
            return Vec::new();
        }
        let Some(proposal) = state.proposal.clone() else {
            return Vec::new();
        };
        if !self.valid_block(&proposal.block) {
            return Vec::new();
        }
        let lock_ok = active.lock_view == 0
            || active.lock_value == proposal.value
            || (view > proposal.key_view
                && proposal.key_view >= active.lock_view
                && self.open_lock(view));
        if !lock_ok {
            return Vec::new();
        }
        self.emit_statement(view, ResolutionPhase::Echo, proposal.value)
    }

    fn emit_statement(
        &mut self,
        view: ResolverView,
        phase: ResolutionPhase,
        value: Digest,
    ) -> Vec<Effect> {
        let Some(active) = self.active.as_mut() else {
            return Vec::new();
        };
        let state = active.views.entry(view).or_default();
        if state.sent.contains_key(&phase) {
            return Vec::new();
        }
        state.sent.insert(phase, value.clone());
        state
            .statements
            .entry(phase)
            .or_default()
            .insert(self.name, value.clone());
        vec![Effect::BroadcastResolutionStatement(ResolutionStatement {
            height: active.height,
            parent: active.parent.clone(),
            view,
            value,
            phase,
            sender: self.name,
        })]
    }

    pub fn on_resolution_statement(&mut self, statement: ResolutionStatement) -> Vec<Effect> {
        if !self.is_member(&statement.sender)
            || statement.view == 0
            || !self.ensure_active_coordinate(statement.height, &statement.parent)
            || self
                .active
                .as_ref()
                .is_some_and(|active| statement.view < active.current_view)
        {
            return Vec::new();
        }
        let state = self
            .active
            .as_mut()
            .unwrap()
            .views
            .entry(statement.view)
            .or_default();
        let phase = state.statements.entry(statement.phase).or_default();
        if phase.contains_key(&statement.sender) {
            return Vec::new();
        }
        phase.insert(statement.sender, statement.value.clone());
        self.advance_view(statement.view)
    }

    fn phase_winner(
        &self,
        view: ResolverView,
        phase: ResolutionPhase,
        threshold: usize,
    ) -> Option<Digest> {
        let statements = self
            .active
            .as_ref()?
            .views
            .get(&view)?
            .statements
            .get(&phase)?;
        let mut counts: BTreeMap<Digest, usize> = BTreeMap::new();
        for value in statements.values() {
            *counts.entry(value.clone()).or_default() += 1;
        }
        counts
            .into_iter()
            .find_map(|(value, count)| (count >= threshold).then_some(value))
    }

    fn phase_authors(
        &self,
        view: ResolverView,
        phase: ResolutionPhase,
        value: &Digest,
    ) -> Vec<PublicKey> {
        self.active
            .as_ref()
            .and_then(|a| a.views.get(&view))
            .and_then(|s| s.statements.get(&phase))
            .into_iter()
            .flat_map(|m| m.iter())
            .filter_map(|(sender, v)| (v == value).then_some(*sender))
            .collect()
    }

    fn ensure_resolution_block(
        &mut self,
        height: ResolutionHeight,
        value: Digest,
        peers: Vec<PublicKey>,
    ) -> Vec<Effect> {
        if self.resolution_blocks.contains_key(&value)
            || !self
                .pending_block_fetch
                .entry(height)
                .or_default()
                .insert(value.clone())
        {
            return Vec::new();
        }
        peers
            .into_iter()
            .map(|peer| Effect::ResolutionBlockFetchTo(peer, height, value.clone()))
            .collect()
    }

    fn advance_view(&mut self, view: ResolverView) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        if view != active.current_view {
            return Vec::new();
        }
        let mut effects = Vec::new();
        loop {
            let mut progressed = false;
            for (from, to) in [
                (ResolutionPhase::Echo, ResolutionPhase::Key1),
                (ResolutionPhase::Key1, ResolutionPhase::Key2),
                (ResolutionPhase::Key2, ResolutionPhase::Key3),
                (ResolutionPhase::Key3, ResolutionPhase::Lock),
            ] {
                let Some(value) = self.phase_winner(view, from, self.quorum) else {
                    continue;
                };
                let already = self
                    .active
                    .as_ref()
                    .and_then(|a| a.views.get(&view))
                    .is_some_and(|s| s.sent.contains_key(&to));
                if already {
                    continue;
                }
                let valid = self
                    .resolution_blocks
                    .get(&value)
                    .is_some_and(|b| self.valid_block(b));
                if !valid {
                    let peers = self.phase_authors(view, from, &value);
                    effects.extend(self.ensure_resolution_block(
                        self.decided_height + 1,
                        value,
                        peers,
                    ));
                    return effects;
                }
                if let Some(active) = self.active.as_mut() {
                    match to {
                        ResolutionPhase::Key1 => {
                            // IT-HS advances `prev` only when the carried value
                            // changes; repeated support for one value only
                            // raises that value's key view.
                            if active.key1_value != value {
                                active.prev_key1 = active.key1_view;
                                active.key1_value = value.clone();
                            }
                            active.key1_view = view;
                        }
                        ResolutionPhase::Key2 => {
                            // Keep the same conditional-previous-key rule for
                            // the AcceptKey proof chain.
                            if active.key2_value != value {
                                active.prev_key2 = active.key2_view;
                                active.key2_value = value.clone();
                            }
                            active.key2_view = view;
                        }
                        ResolutionPhase::Key3 => {
                            active.key3_view = view;
                            active.key3_value = value.clone();
                        }
                        ResolutionPhase::Lock => {
                            active.lock_view = view;
                            active.lock_value = value.clone();
                        }
                        ResolutionPhase::Echo => unreachable!(),
                    }
                }
                effects.extend(self.emit_statement(view, to, value));
                progressed = true;
            }

            if let Some(value) = self.phase_winner(view, ResolutionPhase::Lock, self.quorum) {
                let sent = self.active.as_ref().and_then(|a| a.done_sent.clone());
                if sent.is_none() {
                    let Some(block) = self.resolution_blocks.get(&value).cloned() else {
                        let peers = self.phase_authors(view, ResolutionPhase::Lock, &value);
                        effects.extend(self.ensure_resolution_block(
                            self.decided_height + 1,
                            value,
                            peers,
                        ));
                        return effects;
                    };
                    if self.valid_block(&block) {
                        effects.extend(self.emit_done(value, block));
                        progressed = true;
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        effects.extend(self.recheck_done());
        effects
    }

    fn emit_done(&mut self, value: Digest, block: ResolutionBlock) -> Vec<Effect> {
        let Some(active) = self.active.as_mut() else {
            return Vec::new();
        };
        if active.done_sent.is_some() || block.digest(&self.sid) != value {
            return Vec::new();
        }
        active.done_sent = Some(value.clone());
        active.done.insert(self.name, value.clone());
        vec![Effect::BroadcastResolutionDone(ResolutionDone {
            height: active.height,
            parent: active.parent.clone(),
            value,
            block,
            sender: self.name,
        })]
    }

    pub fn on_resolution_done(&mut self, done: ResolutionDone) -> Vec<Effect> {
        if !self.is_member(&done.sender)
            || done.height != done.block.height
            || done.parent != done.block.parent
            || done.block.digest(&self.sid) != done.value
        {
            return Vec::new();
        }
        if done.height > self.decided_height + 1 {
            return self.record_future_height_hint(done.sender, done.height);
        }
        if !self.ensure_active_coordinate(done.height, &done.parent) {
            return Vec::new();
        }
        self.remember_block(done.block.clone());
        let active = self.active.as_mut().unwrap();
        if active.done.contains_key(&done.sender) {
            return Vec::new();
        }
        active.done.insert(done.sender, done.value);
        self.recheck_done()
    }

    fn done_winner(&self, threshold: usize) -> Option<Digest> {
        let mut counts: BTreeMap<Digest, usize> = BTreeMap::new();
        for value in self.active.as_ref()?.done.values() {
            *counts.entry(value.clone()).or_default() += 1;
        }
        counts
            .into_iter()
            .find_map(|(value, count)| (count >= threshold).then_some(value))
    }

    fn recheck_done(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self
            .active
            .as_ref()
            .and_then(|a| a.done_sent.clone())
            .is_none()
        {
            if let Some(value) = self.done_winner(self.f_plus_1) {
                if let Some(block) = self.resolution_blocks.get(&value).cloned() {
                    if self.valid_block(&block) {
                        effects.extend(self.emit_done(value, block));
                    }
                }
            }
        }
        if let Some(value) = self.done_winner(self.quorum) {
            if let Some(block) = self.resolution_blocks.get(&value).cloned() {
                if self.valid_block(&block) {
                    effects.extend(self.decide(value, block));
                }
            }
        }
        effects
    }

    fn decide(&mut self, value: Digest, block: ResolutionBlock) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        let resolver_view = active.current_view;
        if block.height != active.height
            || block.parent != active.parent
            || block.digest(&self.sid) != value
            || !self.valid_block(&block)
        {
            return Vec::new();
        }
        let pending_before = self.pending_anchors().len();
        let mut effects = Vec::new();
        let mut applied_targets = 0usize;
        for anchor in &block.anchors {
            self.decided_anchors.insert(anchor.clone());
            let Some(carrier) = self
                .carrier_bodies
                .get(&(anchor.view, anchor.digest.clone()))
                .cloned()
            else {
                log::error!(
                    "valid resolution block lost retained carrier body for view {} and digest {:?}",
                    anchor.view,
                    anchor.digest
                );
                debug_assert!(
                    false,
                    "valid resolution block must retain every carrier body"
                );
                continue;
            };
            for entry in carrier.entries() {
                let target = entry.target_view();
                if self.is_anchor_resolved(target) {
                    continue;
                }
                self.anchored_targets.insert(target);
                applied_targets += 1;
                let (outcome, refs) = Self::derive_anchor(entry);
                effects.push(Effect::ApplyAnchor(target, outcome, refs));
            }
        }
        self.decided_height = block.height;
        self.head = value;
        self.decided_blocks.insert(block.height, block.clone());
        let pending_after = self.pending_anchors().len();
        #[cfg(feature = "benchmark")]
        log::info!(
            "VANTAGE_RECOVERY_EVENT kind=resolver_decide view={} epoch_ms={} height={} anchors={} applied_targets={} pending_before={} pending_after={} beta={}",
            resolver_view,
            recovery_epoch_ms(),
            block.height,
            block.anchors.len(),
            applied_targets,
            pending_before,
            pending_after,
            self.batch_cap
        );
        self.active = None;
        self.resolution_blocks.clear();
        self.pending_block_fetch = self
            .pending_block_fetch
            .split_off(&self.decided_height.saturating_add(1));
        self.decision_request_pending = None;
        effects.extend(self.maybe_request_missing_decision());
        effects.extend(self.activate_if_pending());
        effects
    }

    fn derive_anchor(entry: &ResolutionEntry) -> (Outcome, Vec<BlockRef>) {
        match entry {
            ResolutionEntry::Full(_, c, t) => (
                Outcome::Full(c.clone(), t.clone()),
                c.iter().chain(t.iter()).cloned().collect(),
            ),
            ResolutionEntry::Core(_, c, t) => (
                Outcome::Core(c.clone()),
                c.iter().chain(t.iter()).cloned().collect(),
            ),
            ResolutionEntry::Skip(_) => (Outcome::Skip, Vec::new()),
        }
    }

    pub fn on_resolution_block_fetch(
        &self,
        requester: PublicKey,
        height: ResolutionHeight,
        digest: Digest,
    ) -> Vec<Effect> {
        if !self.is_member(&requester) {
            return Vec::new();
        }
        let Some(block) = self
            .resolution_blocks
            .get(&digest)
            .or_else(|| self.decided_blocks.get(&height))
        else {
            return Vec::new();
        };
        if block.height != height || block.digest(&self.sid) != digest {
            return Vec::new();
        }
        vec![Effect::ResolutionBlockServeTo(requester, block.clone())]
    }

    pub fn on_resolution_block_serve(&mut self, block: ResolutionBlock) -> Vec<Effect> {
        let digest = block.digest(&self.sid);
        if !self.structurally_valid_block(&block) {
            return Vec::new();
        }
        let requested = self
            .pending_block_fetch
            .get_mut(&block.height)
            .is_some_and(|digests| digests.remove(&digest));
        if !requested {
            return Vec::new();
        }
        if self
            .pending_block_fetch
            .get(&block.height)
            .is_some_and(BTreeSet::is_empty)
        {
            self.pending_block_fetch.remove(&block.height);
        }
        self.resolution_blocks.insert(digest, block);
        self.retry_active()
    }

    pub fn on_decision_request(
        &self,
        height: ResolutionHeight,
        requester: PublicKey,
    ) -> Vec<Effect> {
        if !self.is_member(&requester) {
            return Vec::new();
        }
        let Some(block) = self.decided_blocks.get(&height) else {
            return Vec::new();
        };
        let value = block.digest(&self.sid);
        vec![Effect::ResolutionDoneTo(
            requester,
            ResolutionDone {
                height,
                parent: block.parent.clone(),
                value,
                block: block.clone(),
                sender: self.name,
            },
        )]
    }

    fn retry_active(&mut self) -> Vec<Effect> {
        let Some(active) = self.active.as_ref() else {
            return Vec::new();
        };
        let view = active.current_view;
        let mut effects = Vec::new();
        if view > 0 {
            effects.extend(self.try_primary_propose(view));
            effects.extend(self.try_echo(view));
            effects.extend(self.advance_view(view));
        }
        effects.extend(self.recheck_done());
        effects
    }

    /// Records that data-plane targets below `floor` are already resolved.
    ///
    /// Resolver evidence is deliberately retained: the formal protocol has no
    /// certified checkpoint that can replace an in-flight resolution block.
    pub fn advance_resolved_target_floor(&mut self, floor: View) {
        if floor <= self.resolved_target_floor {
            return;
        }
        self.anchored_targets = self.anchored_targets.split_off(&floor);
        self.locally_sealed_targets = self.locally_sealed_targets.split_off(&floor);
        self.resolved_target_floor = floor;
    }

    /// Records a target sealed by any data-plane path so fresh resolver blocks
    /// can prioritize anchors that still add useful outcomes.
    pub fn note_target_resolved(&mut self, view: View) {
        if view >= self.resolved_target_floor {
            self.locally_sealed_targets.insert(view);
        }
    }

    pub fn is_anchor_resolved(&self, view: View) -> bool {
        view < self.resolved_target_floor || self.anchored_targets.contains(&view)
    }

    pub fn current_resolver_view(&self) -> ResolverView {
        self.active.as_ref().map_or(0, |a| a.current_view)
    }

    pub fn decided_height(&self) -> ResolutionHeight {
        self.decided_height
    }

    pub fn pending_anchor_count(&self) -> usize {
        self.pending_anchors().len()
    }

    pub fn batch_cap(&self) -> usize {
        self.batch_cap
    }

    pub(crate) fn unresolved_target_count(&self, block: &ResolutionBlock) -> usize {
        block
            .anchors
            .iter()
            .flat_map(|anchor| self.unresolved_targets(anchor))
            .collect::<BTreeSet<_>>()
            .len()
    }

    pub fn head(&self) -> &Digest {
        &self.head
    }

    #[cfg(test)]
    pub(crate) fn is_eligible_for_test(&self, view: View, digest: &Digest) -> bool {
        self.eligible.contains(&AnchorRef {
            view,
            digest: digest.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn active_coordinate_for_test(
        &self,
    ) -> Option<(ResolutionHeight, Digest, ResolverView)> {
        self.active
            .as_ref()
            .map(|a| (a.height, a.parent.clone(), a.current_view))
    }

    #[cfg(test)]
    pub(crate) fn decided_block_for_test(
        &self,
        height: ResolutionHeight,
    ) -> Option<&ResolutionBlock> {
        self.decided_blocks.get(&height)
    }

    #[cfg(test)]
    pub(crate) fn held_carrier_for_test(&self, view: View, digest: &Digest) -> bool {
        self.carrier_bodies.contains_key(&(view, digest.clone()))
    }
}
