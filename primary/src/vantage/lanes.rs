use crate::messages::{Ack, Header};
use crate::primary::Height;
use crate::vantage::block::{self, block_ok, BlockRef};
use crate::vantage::claim::{batch_manifest_refs, manifest_refs, AvailClaim, ClaimRef};
use crate::vantage::Effect;
use config::{Committee, Stake, WorkerId};
use crypto::{Digest, PublicKey};
use metrics::{Metrics, UtilizationTimer};
use parking_lot::Mutex;
use prometheus::IntCounter;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;
use store::Store;

#[derive(Clone)]
pub struct BlockEntry {
    pub block: Header,
    /// The block arrived from its encoded author through the publish path.
    pub direct: bool,
    /// The block arrived through the repair path without publish provenance.
    pub repaired: bool,
    /// Retained blocks remain available for repair service.
    pub retained: bool,
    /// Every worker batch referenced by this block is present locally.
    pub payload_ok: bool,
    /// A successful direct-and-payload prefix check; this flag is monotonic.
    pub direct_prefix_verified: bool,
    /// A successful chain-validity prefix check; this flag is monotonic.
    pub chain_verified: bool,
    /// The exact cached header passed `block_ok`; this flag is monotonic.
    pub block_ok_verified: bool,
}

impl BlockEntry {
    /// Checks the author and expected height used as the walk's termination measure.
    fn pinned_at(&self, author: PublicKey, expected_height: Height) -> bool {
        self.block.author == author && self.block.height == expected_height
    }
}

#[derive(Default)]
/// Digest-keyed block cache that can represent multiple digests at one author and height.
pub struct BlockCache {
    by_digest: HashMap<Digest, BlockEntry>,
    by_author: HashMap<PublicKey, BTreeMap<Height, HashSet<Digest>>>,
    walk_steps_chain: u64,
    walk_steps_direct: u64,
    /// Failure counts indexed as missing block, coordinate mismatch, and validity gate.
    walk_fail_chain: [u64; 3],
    walk_fail_direct: [u64; 3],
    /// Missing exact coordinates awaiting repair authorization.
    missing_parents: BTreeSet<BlockRef>,
}

enum DirectPrefixCheck {
    Verified,
    Gate(Digest),
    Failed,
}

enum DirectPubCheck {
    Confirmed,
    BlockedOnGate(Digest),
    Failed,
}

/// Maximum distinct missing coordinates retained for reporting.
const MISSING_PARENTS_CAP: usize = 64;

impl BlockCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, h: &Digest) -> Option<&BlockEntry> {
        self.by_digest.get(h)
    }

    /// Returns monotonic `(chain, direct)` walk-step counts.
    pub fn walk_steps(&self) -> (u64, u64) {
        (self.walk_steps_chain, self.walk_steps_direct)
    }

    /// Returns monotonic `(chain, direct)` failure counts in missing, coordinate, gate order.
    pub fn walk_failures(&self) -> ([u64; 3], [u64; 3]) {
        (self.walk_fail_chain, self.walk_fail_direct)
    }

    fn note_missing_parent(&mut self, author: PublicKey, height: Height, digest: Digest) {
        if self.missing_parents.len() >= MISSING_PARENTS_CAP {
            return;
        }
        self.missing_parents.insert((author, height, digest));
    }

    /// Removes and returns at most `cap` missing exact coordinates.
    pub fn take_missing_parents(&mut self, cap: usize) -> Vec<BlockRef> {
        let mut out = Vec::new();
        while out.len() < cap {
            let Some(r) = self.missing_parents.pop_first() else {
                break;
            };
            out.push(r);
        }
        out
    }

    pub fn len(&self) -> usize {
        self.by_digest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_digest.is_empty()
    }

    /// Deletes blocks below `height` and returns the number deleted.
    ///
    /// The caller must ensure no correct local or remote party can require those blocks;
    /// a floor derived only from local progress is unsafe.
    pub fn evict_author_below(&mut self, author: &PublicKey, height: Height) -> usize {
        let Some(by_height) = self.by_author.get_mut(author) else {
            return 0;
        };
        let keep = by_height.split_off(&height);
        let below = std::mem::replace(by_height, keep);
        let mut dropped = 0;
        for (_, digests) in below {
            for d in digests {
                if self.by_digest.remove(&d).is_some() {
                    dropped += 1;
                }
            }
        }
        if by_height.is_empty() {
            self.by_author.remove(author);
        }
        dropped
    }

    pub fn contains(&self, h: &Digest) -> bool {
        self.by_digest.contains_key(h)
    }

    /// Inserts a block and monotonically merges its provenance and validation flags.
    ///
    /// `block_ok_verified` may be true only after this exact header passes `block_ok`.
    pub fn upsert(
        &mut self,
        block: Header,
        direct: bool,
        repaired: bool,
        payload_ok: bool,
        block_ok_verified: bool,
    ) {
        let digest = block.id.clone();
        let author = block.author;
        let height = block.height;
        self.by_author
            .entry(author)
            .or_default()
            .entry(height)
            .or_default()
            .insert(digest.clone());
        match self.by_digest.entry(digest) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(BlockEntry {
                    block,
                    direct,
                    repaired,
                    retained: false,
                    payload_ok,
                    direct_prefix_verified: false,
                    chain_verified: false,
                    block_ok_verified,
                });
            }
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                entry.direct |= direct;
                entry.repaired |= repaired;
                entry.payload_ok |= payload_ok;
                entry.block_ok_verified |= block_ok_verified;
                entry.block = block;
            }
        }
    }

    /// Restores a verified, retained anchor for this node's own lane.
    ///
    /// The caller must verify the session, author, frontier coordinate, and `block_ok` first.
    pub(crate) fn seed_own_anchor(&mut self, block: Header) {
        let digest = block.id.clone();
        self.upsert(block, true, false, true, true);
        if let Some(entry) = self.by_digest.get_mut(&digest) {
            entry.chain_verified = true;
            entry.direct_prefix_verified = true;
            entry.retained = true;
        }
    }

    /// Seeds a checkpoint-certified anchor without claiming local payload storage.
    pub(crate) fn seed_recovered_own_anchor(&mut self, block: Header) {
        let digest = block.id.clone();
        self.upsert(block, false, true, false, true);
        if let Some(entry) = self.by_digest.get_mut(&digest) {
            entry.chain_verified = true;
            entry.direct_prefix_verified = true;
            entry.retained = true;
        }
    }

    /// Returns true only when this call changes `h` to retained.
    pub fn mark_retained(&mut self, h: &Digest) -> bool {
        match self.by_digest.get_mut(h) {
            Some(entry) if !entry.retained => {
                entry.retained = true;
                true
            }
            _ => false,
        }
    }

    /// Returns true only when this call records payload availability for `h`.
    pub fn set_payload_ok(&mut self, h: &Digest, ok: bool) -> bool {
        match self.by_digest.get_mut(h) {
            Some(entry) if ok && !entry.payload_ok => {
                entry.payload_ok = true;
                true
            }
            _ => false,
        }
    }

    fn direct_gate_ready(&self, h: &Digest) -> bool {
        self.by_digest
            .get(h)
            .is_some_and(|entry| entry.direct && entry.payload_ok)
    }

    /// Returns one cached block at the exact author and height.
    pub fn author_block_at(&self, author: &PublicKey, height: Height) -> Option<&Header> {
        let digest = self.by_author.get(author)?.get(&height)?.iter().next()?;
        self.by_digest.get(digest).map(|e| &e.block)
    }

    /// Returns the smallest cached height for `author`.
    pub fn earliest_height(&self, author: &PublicKey) -> Option<Height> {
        self.by_author.get(author)?.keys().next().copied()
    }

    pub fn author_refs(&self, author: &PublicKey) -> Vec<BlockRef> {
        self.by_author
            .get(author)
            .map(|heights| {
                heights
                    .iter()
                    .flat_map(|(height, digests)| {
                        digests.iter().map(move |d| (*author, *height, d.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Verifies one author, consecutive heights, direct provenance, and payload availability.
    ///
    /// Expected height decreases on every step, so malformed parent cycles terminate.
    /// Only successful prefixes are memoized.
    pub fn direct_prefix_ok(&mut self, genesis: &Digest, h: &Digest) -> bool {
        matches!(
            self.direct_prefix_check(genesis, h),
            DirectPrefixCheck::Verified
        )
    }

    fn direct_prefix_check(&mut self, genesis: &Digest, h: &Digest) -> DirectPrefixCheck {
        let Some(start) = self.by_digest.get(h) else {
            return DirectPrefixCheck::Failed;
        };
        if start.direct_prefix_verified {
            return DirectPrefixCheck::Verified;
        }
        let author = start.block.author;
        let mut expected_height = start.block.height;
        let mut cur = h.clone();
        let mut newly_walked: Vec<Digest> = Vec::new();
        let mut steps: u64 = 0;
        loop {
            steps += 1;
            if &cur == genesis {
                if expected_height != 0 {
                    self.walk_steps_direct += steps;
                    self.walk_fail_direct[1] += 1;
                    return DirectPrefixCheck::Failed;
                }
                break;
            }
            if expected_height == 0 {
                self.walk_steps_direct += steps;
                self.walk_fail_direct[1] += 1;
                return DirectPrefixCheck::Failed;
            }
            let Some(entry) = self.by_digest.get(&cur) else {
                self.walk_steps_direct += steps;
                self.walk_fail_direct[0] += 1;
                self.note_missing_parent(author, expected_height, cur);
                return DirectPrefixCheck::Failed;
            };
            if !entry.pinned_at(author, expected_height) {
                self.walk_steps_direct += steps;
                self.walk_fail_direct[1] += 1;
                return DirectPrefixCheck::Failed;
            }
            if entry.direct_prefix_verified {
                break;
            }
            if !(entry.direct && entry.payload_ok) {
                self.walk_steps_direct += steps;
                self.walk_fail_direct[2] += 1;
                return DirectPrefixCheck::Gate(cur);
            }
            newly_walked.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
        self.walk_steps_direct += steps;
        for d in &newly_walked {
            if let Some(e) = self.by_digest.get_mut(d) {
                e.direct_prefix_verified = true;
            }
        }
        DirectPrefixCheck::Verified
    }

    /// Verifies one author, consecutive heights, validated blocks, and the genesis link.
    ///
    /// Expected height decreases on every step, and only successful prefixes are memoized.
    pub fn verified_prefix_through_genesis(
        &mut self,
        _committee: &Committee,
        _sid: &Digest,
        _max_block_payload: usize,
        genesis: &Digest,
        h: &Digest,
    ) -> bool {
        let Some(start) = self.by_digest.get(h) else {
            return false;
        };
        if start.chain_verified {
            return true;
        }
        let author = start.block.author;
        let mut expected_height = start.block.height;
        let mut cur = h.clone();
        let mut newly_walked: Vec<Digest> = Vec::new();
        let mut steps: u64 = 0;
        loop {
            steps += 1;
            if &cur == genesis {
                if expected_height != 0 {
                    self.walk_steps_chain += steps;
                    self.walk_fail_chain[1] += 1;
                    return false;
                }
                break;
            }
            if expected_height == 0 {
                self.walk_steps_chain += steps;
                self.walk_fail_chain[1] += 1;
                return false;
            }
            let Some(entry) = self.by_digest.get(&cur) else {
                self.walk_steps_chain += steps;
                self.walk_fail_chain[0] += 1;
                self.note_missing_parent(author, expected_height, cur);
                return false;
            };
            if !entry.pinned_at(author, expected_height) {
                self.walk_steps_chain += steps;
                self.walk_fail_chain[1] += 1;
                return false;
            }
            if !entry.block_ok_verified {
                self.walk_steps_chain += steps;
                self.walk_fail_chain[2] += 1;
                return false;
            }
            if entry.chain_verified {
                break;
            }
            newly_walked.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
        self.walk_steps_chain += steps;
        for d in &newly_walked {
            if let Some(e) = self.by_digest.get_mut(d) {
                e.chain_verified = true;
            }
        }
        true
    }

    /// Returns a validated digest chain in ascending height order, including genesis.
    pub fn collect_verified_chain(
        &self,
        committee: &Committee,
        sid: &Digest,
        max_block_payload: usize,
        genesis: &Digest,
        h: &Digest,
    ) -> Option<Vec<Digest>> {
        let start = self.by_digest.get(h)?;
        let author = start.block.author;
        let mut expected_height = start.block.height;
        let mut cur = h.clone();
        let mut chain = Vec::new();
        loop {
            if &cur == genesis {
                if expected_height != 0 {
                    return None;
                }
                chain.push(genesis.clone());
                chain.reverse();
                return Some(chain);
            }
            if expected_height == 0 {
                return None;
            }
            let entry = self.by_digest.get(&cur)?;
            if !entry.pinned_at(author, expected_height) {
                return None;
            }
            if !block_ok(&entry.block, committee, sid, max_block_payload) {
                return None;
            }
            chain.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
    }

    /// Returns the validated suffix above the exact stop coordinate in ascending order.
    pub fn collect_verified_suffix(
        &self,
        _committee: &Committee,
        _sid: &Digest,
        _max_block_payload: usize,
        stop_height: Height,
        stop_digest: &Digest,
        h: &Digest,
    ) -> Option<Vec<Digest>> {
        match self.classify_suffix(
            _committee,
            _sid,
            _max_block_payload,
            stop_height,
            stop_digest,
            h,
        ) {
            SuffixWalk::Ready(chain) => Some(chain),
            SuffixWalk::Pending | SuffixWalk::Forked => None,
        }
    }

    /// Distinguishes missing validation data from ancestry that contradicts the stop coordinate.
    pub fn classify_suffix(
        &self,
        _committee: &Committee,
        _sid: &Digest,
        _max_block_payload: usize,
        stop_height: Height,
        stop_digest: &Digest,
        h: &Digest,
    ) -> SuffixWalk {
        let Some(start) = self.by_digest.get(h) else {
            return SuffixWalk::Pending;
        };
        let author = start.block.author;
        let mut expected_height = start.block.height;
        let mut cur = h.clone();
        let mut chain = Vec::new();
        loop {
            if expected_height == stop_height {
                if &cur != stop_digest {
                    return SuffixWalk::Forked;
                }
                chain.reverse();
                return SuffixWalk::Ready(chain);
            }
            if expected_height == 0 {
                return SuffixWalk::Forked;
            }
            let Some(entry) = self.by_digest.get(&cur) else {
                return SuffixWalk::Pending;
            };
            if !entry.pinned_at(author, expected_height) {
                return SuffixWalk::Pending;
            }
            if !entry.block_ok_verified {
                return SuffixWalk::Pending;
            }
            chain.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
    }

    /// Derives an exact verified ancestor of a proposal anchor.
    pub(crate) fn resolve_verified_ancestor(
        &self,
        anchor: &BlockRef,
        delta: Height,
    ) -> AncestorWalk {
        if delta == 0 || delta >= anchor.1 {
            return AncestorWalk::Forked;
        }
        let target_height = anchor.1 - delta;
        let author = anchor.0;
        let mut expected_height = anchor.1;
        let mut cur = anchor.2.clone();
        loop {
            let Some(entry) = self.by_digest.get(&cur) else {
                return AncestorWalk::Pending;
            };
            if !entry.pinned_at(author, expected_height) {
                return AncestorWalk::Forked;
            }
            if !entry.block_ok_verified {
                return AncestorWalk::Pending;
            }
            if expected_height == target_height {
                return AncestorWalk::Ready((author, target_height, cur));
            }
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of validating ancestry above an exact watermark coordinate.
pub enum SuffixWalk {
    /// Validated digests in ascending height order.
    Ready(Vec<Digest>),
    /// A required block or validation result is unavailable.
    Pending,
    /// The target ancestry does not contain the watermark coordinate.
    Forked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AncestorWalk {
    Ready(BlockRef),
    Pending,
    Forked,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
/// A claim that the sender holds `author` through the exact `(height, head)` coordinate.
pub struct AvailEntry {
    pub author: PublicKey,
    pub height: Height,
    pub head: Digest,
}

pub type SharedBlocks = Arc<Mutex<BlockCache>>;

fn min_digest() -> Digest {
    Digest([0; 32])
}

fn max_digest() -> Digest {
    Digest([u8::MAX; 32])
}

fn author_lower_bound(author: PublicKey) -> BlockRef {
    author_lower_bound_from(author, 0)
}

fn author_lower_bound_from(author: PublicKey, height: Height) -> BlockRef {
    (author, height, min_digest())
}

fn author_upper_bound(author: PublicKey) -> BlockRef {
    (author, u64::MAX, max_digest())
}

/// Selects the greatest height and the smallest digest at that height.
fn newest_indexed(index: &BTreeSet<BlockRef>, author: PublicKey) -> Option<BlockRef> {
    let mut current_height = None;
    let mut best_at_height = None;
    for r in index
        .range(author_lower_bound(author)..=author_upper_bound(author))
        .rev()
    {
        if current_height.is_some_and(|height| height != r.1) {
            break;
        }
        current_height.get_or_insert(r.1);
        best_at_height = Some(r.clone());
    }
    best_at_height
}

fn set_candidate(
    candidates: &mut HashMap<PublicKey, BlockRef>,
    author: PublicKey,
    value: Option<BlockRef>,
) {
    match value {
        Some(r) => {
            candidates.insert(author, r);
        }
        None => {
            candidates.remove(&author);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AckThreshold {
    Validity,
    Quorum,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckAvailability {
    pub reference: BlockRef,
    pub threshold: AckThreshold,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckAggregationResult {
    pub accepted: bool,
    pub availability: Option<AckAvailability>,
}

pub struct AckAggregator {
    committee: Committee,
    members: HashSet<PublicKey>,
    /// Distinct committee senders counted for each exact reference.
    senders: HashMap<BlockRef, HashSet<PublicKey>>,
    weights: HashMap<BlockRef, Stake>,
    /// Highest emitted threshold; quorum entries permanently retire working state.
    emitted: HashMap<BlockRef, AckThreshold>,
}

impl AckAggregator {
    pub fn new(committee: Committee) -> Self {
        let members = committee.authorities.keys().cloned().collect();
        Self {
            committee,
            members,
            senders: HashMap::new(),
            weights: HashMap::new(),
            emitted: HashMap::new(),
        }
    }

    /// Counts each committee sender once and emits each crossed threshold once.
    pub fn record_ack(&mut self, sender: PublicKey, reference: BlockRef) -> AckAggregationResult {
        if !self.members.contains(&sender) {
            return AckAggregationResult {
                accepted: false,
                availability: None,
            };
        }
        if self.emitted.get(&reference) == Some(&AckThreshold::Quorum) {
            return AckAggregationResult {
                accepted: true,
                availability: None,
            };
        }
        if !self
            .senders
            .entry(reference.clone())
            .or_default()
            .insert(sender)
        {
            return AckAggregationResult {
                accepted: true,
                availability: None,
            };
        }

        let stake = self.committee.stake(&sender);
        let weight = self.weights.entry(reference.clone()).or_insert(0);
        *weight += stake;
        let crossed = if *weight >= self.committee.quorum_threshold() {
            Some(AckThreshold::Quorum)
        } else if *weight >= self.committee.validity_threshold() {
            Some(AckThreshold::Validity)
        } else {
            None
        };

        let Some(threshold) = crossed else {
            return AckAggregationResult {
                accepted: true,
                availability: None,
            };
        };
        if self
            .emitted
            .get(&reference)
            .is_some_and(|old| *old >= threshold)
        {
            return AckAggregationResult {
                accepted: true,
                availability: None,
            };
        }
        self.emitted.insert(reference.clone(), threshold);
        if threshold == AckThreshold::Quorum {
            // Quorum is terminal; `emitted` prevents later acknowledgments from recreating state.
            self.senders.remove(&reference);
            self.weights.remove(&reference);
        }
        AckAggregationResult {
            accepted: true,
            availability: Some(AckAvailability {
                reference,
                threshold,
            }),
        }
    }

    pub(crate) fn will_count(&self, sender: PublicKey, reference: &BlockRef) -> bool {
        self.members.contains(&sender)
            && self.emitted.get(reference) != Some(&AckThreshold::Quorum)
            && !self
                .senders
                .get(reference)
                .is_some_and(|senders| senders.contains(&sender))
    }

    pub fn senders_tracked(&self) -> usize {
        self.senders.len()
    }

    pub fn refs_retired(&self) -> usize {
        self.emitted.len()
    }

    pub(crate) fn is_at_quorum(&self, reference: &BlockRef) -> bool {
        self.emitted.get(reference) == Some(&AckThreshold::Quorum)
    }

    pub(crate) fn reset_author(&mut self, author: PublicKey) {
        self.senders.retain(|r, _| r.0 != author);
        self.weights.retain(|r, _| r.0 != author);
        self.emitted.retain(|r, _| r.0 != author);
    }
}

pub type SharedAckAggregator = Arc<Mutex<AckAggregator>>;

pub(crate) fn aggregate_received_ack(
    aggregator: &SharedAckAggregator,
    metrics: Option<&Metrics>,
    ack: &Ack,
) -> Option<AckAvailability> {
    let result = aggregator.lock().record_ack(ack.sender, ack.reference());
    if !result.accepted {
        if let Some(metrics) = metrics {
            metrics.vantage_rejected_nonmember_total.inc();
        }
        return None;
    }
    if let Some(metrics) = metrics {
        metrics.vantage_acks_received.inc();
    }
    result.availability
}

pub struct LaneManager {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    store: Store,
    blocks: SharedBlocks,

    /// Highest acknowledgment threshold for each exact reference.
    ack_availability: HashMap<BlockRef, AckThreshold>,
    /// Exact references already acknowledged by this node.
    acked: HashSet<BlockRef>,
    pending_direct: BTreeSet<BlockRef>,
    /// Current direct-prefix blocker for each deferred reference.
    pending_direct_blocked_by: BTreeMap<BlockRef, Digest>,
    /// Inverse index of `pending_direct_blocked_by`.
    pending_direct_waiters_by_blocker: HashMap<Digest, BTreeSet<BlockRef>>,
    direct_prefix_blocker_by_digest: HashMap<Digest, Digest>,
    /// Last pending reference checked for each author.
    refresh_scan_after: HashMap<PublicKey, BlockRef>,
    direct_pub_refs: BTreeSet<BlockRef>,
    /// Locally direct references with a general quorum of prefix claims.
    quorum_claim_refs: BTreeSet<BlockRef>,
    /// Locally direct references with a quorum of exact-position claims.
    quorum_direct_refs: BTreeSet<BlockRef>,

    c_candidate: HashMap<PublicKey, BlockRef>,
    confirmation_candidate: HashMap<PublicKey, BlockRef>,
    t_candidate: HashMap<PublicKey, BlockRef>,

    /// This node's lane tip, or `(0, genesis)` before its first block.
    own_frontier: (Height, Digest),

    /// This node's highest contiguous direct prefix for each author.
    own_avail_watermark: HashMap<PublicKey, (Height, Digest)>,
    avail_dirty: bool,
    avail: crate::vantage::avail::AvailResolver,

    /// Greatest height with at least a validity-threshold availability mark.
    avail_watermark_high: HashMap<PublicKey, Height>,

    metrics: Option<Arc<Metrics>>,

    wt_store_probe: Option<IntCounter>,

    /// Restored anchor available for one boot-time broadcast.
    seeded_anchor: Option<Header>,
}

const OWN_FRONTIER_KEY: &[u8] = b"vantage/own_frontier";

const OWN_FRONTIER_HEADER_KEY: &[u8] = b"vantage/own_frontier_header";

/// Maximum pending direct references checked per refresh call.
const REFRESH_WALK_BUDGET: usize = 8;

#[derive(Serialize, Deserialize)]
struct PersistedFrontier {
    sid: Digest,
    height: Height,
    digest: Digest,
}

#[derive(Serialize, Deserialize)]
struct PersistedFrontierHeader {
    sid: Digest,
    header: Header,
}

impl LaneManager {
    pub fn new(
        name: PublicKey,
        committee: Committee,
        max_block_payload: usize,
        store: Store,
    ) -> Self {
        Self::with_shared_blocks(
            name,
            committee,
            max_block_payload,
            store,
            Arc::new(Mutex::new(BlockCache::new())),
        )
    }

    pub fn with_shared_blocks(
        name: PublicKey,
        committee: Committee,
        max_block_payload: usize,
        store: Store,
        blocks: SharedBlocks,
    ) -> Self {
        let sid = block::session_id(&committee);
        let genesis = block::genesis_digest(&sid);
        let avail = crate::vantage::avail::AvailResolver::new(
            committee.clone(),
            sid.clone(),
            genesis.clone(),
            max_block_payload,
            blocks.clone(),
        );
        Self {
            name,
            committee,
            sid,
            genesis: genesis.clone(),
            max_block_payload,
            store,
            blocks,
            ack_availability: HashMap::new(),
            acked: HashSet::new(),
            pending_direct: BTreeSet::new(),
            pending_direct_blocked_by: BTreeMap::new(),
            pending_direct_waiters_by_blocker: HashMap::new(),
            direct_prefix_blocker_by_digest: HashMap::new(),
            refresh_scan_after: HashMap::new(),
            direct_pub_refs: BTreeSet::new(),
            quorum_claim_refs: BTreeSet::new(),
            quorum_direct_refs: BTreeSet::new(),
            c_candidate: HashMap::new(),
            confirmation_candidate: HashMap::new(),
            t_candidate: HashMap::new(),
            own_frontier: (0, genesis),
            own_avail_watermark: HashMap::new(),
            avail_dirty: false,
            avail,
            avail_watermark_high: HashMap::new(),
            metrics: None,
            wt_store_probe: None,
            seeded_anchor: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn sid(&self) -> &Digest {
        &self.sid
    }

    pub fn genesis(&self) -> &Digest {
        &self.genesis
    }

    pub fn block_cache_len(&self) -> usize {
        self.blocks.lock().len()
    }

    pub fn blocks_handle(&self) -> SharedBlocks {
        self.blocks.clone()
    }

    #[cfg(test)]
    pub(crate) fn pending_avail_index_for_test(
        &self,
    ) -> std::collections::HashSet<(PublicKey, PublicKey)> {
        self.avail.pending_avail_index_for_test()
    }

    #[cfg(test)]
    pub(crate) fn pending_avail_keys_for_test(
        &self,
    ) -> std::collections::HashSet<(PublicKey, PublicKey)> {
        self.avail.pending_avail_keys_for_test()
    }

    #[cfg(test)]
    pub(crate) fn store_for_test(&self) -> Store {
        self.store.clone()
    }

    async fn persist_own_frontier(&mut self, header: &Header) {
        let (height, digest) = self.own_frontier.clone();
        let record = PersistedFrontier {
            sid: self.sid.clone(),
            height,
            digest,
        };
        match bincode::serialize(&record) {
            Ok(bytes) => self.store.write(OWN_FRONTIER_KEY.to_vec(), bytes).await,
            Err(e) => log::error!("vantage lanes: cannot serialize own lane frontier: {e}"),
        }
        let record = PersistedFrontierHeader {
            sid: self.sid.clone(),
            header: header.clone(),
        };
        match bincode::serialize(&record) {
            Ok(bytes) => {
                self.store
                    .write(OWN_FRONTIER_HEADER_KEY.to_vec(), bytes)
                    .await
            }
            Err(e) => log::error!("vantage lanes: cannot serialize own frontier header: {e}"),
        }
    }

    /// Restores only a frontier from the current protocol session.
    pub async fn restore_own_frontier(&mut self) {
        let bytes = match self.store.read(OWN_FRONTIER_KEY.to_vec()).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(e) => {
                log::error!("vantage lanes: cannot read own lane frontier: {e}");
                return;
            }
        };
        let record: PersistedFrontier = match bincode::deserialize(&bytes) {
            Ok(record) => record,
            Err(e) => {
                log::error!("vantage lanes: cannot decode own lane frontier: {e}");
                return;
            }
        };
        if record.sid != self.sid {
            log::warn!(
                "vantage lanes: persisted lane frontier belongs to another session; \
                 starting this session's lane at genesis"
            );
            return;
        }
        if record.height <= self.own_frontier.0 {
            return;
        }
        log::info!(
            "vantage lanes: restored own lane frontier at height={} (was {})",
            record.height,
            self.own_frontier.0
        );
        self.own_frontier = (record.height, record.digest);
        self.seed_own_anchor_from_store().await;
    }

    /// Replaces uncommitted local lane state with a checkpoint-certified tip.
    pub async fn recover_own_frontier(&mut self, header: Header) -> bool {
        if header.author != self.name
            || header.height == 0
            || !block_ok(&header, &self.committee, &self.sid, self.max_block_payload)
        {
            return false;
        }
        let recovered = (header.height, header.id.clone());
        if self.own_frontier == recovered {
            return false;
        }

        let mut stale_digests = HashSet::new();
        for r in self
            .pending_direct
            .iter()
            .chain(self.pending_direct_blocked_by.keys())
            .chain(self.direct_pub_refs.iter())
            .chain(self.quorum_claim_refs.iter())
            .chain(self.quorum_direct_refs.iter())
            .chain(self.ack_availability.keys())
            .chain(self.acked.iter())
        {
            if r.0 == self.name {
                stale_digests.insert(r.2.clone());
            }
        }

        self.pending_direct.retain(|r| r.0 != self.name);
        self.pending_direct_blocked_by
            .retain(|r, _| r.0 != self.name);
        self.pending_direct_waiters_by_blocker.retain(|_, refs| {
            refs.retain(|r| r.0 != self.name);
            !refs.is_empty()
        });
        self.direct_prefix_blocker_by_digest
            .retain(|digest, _| !stale_digests.contains(digest));
        self.direct_pub_refs.retain(|r| r.0 != self.name);
        self.quorum_claim_refs.retain(|r| r.0 != self.name);
        self.quorum_direct_refs.retain(|r| r.0 != self.name);
        self.ack_availability.retain(|r, _| r.0 != self.name);
        self.acked.retain(|r| r.0 != self.name);
        self.refresh_scan_after.remove(&self.name);
        self.c_candidate.remove(&self.name);
        self.confirmation_candidate.remove(&self.name);
        self.t_candidate.remove(&self.name);

        let anchor = (self.name, header.height, header.id.clone());
        self.blocks.lock().seed_recovered_own_anchor(header.clone());
        self.avail.reset_author(self.name, &anchor);
        self.ack_availability
            .insert(anchor.clone(), AckThreshold::Quorum);
        self.avail.note_threshold(&anchor, AckThreshold::Quorum);
        self.acked.insert(anchor.clone());
        self.direct_pub_refs.insert(anchor.clone());
        self.quorum_claim_refs.insert(anchor.clone());
        self.quorum_direct_refs.insert(anchor.clone());
        self.c_candidate.insert(self.name, anchor.clone());
        self.own_avail_watermark
            .insert(self.name, (anchor.1, anchor.2.clone()));
        self.avail_watermark_high.insert(self.name, anchor.1);
        self.avail_dirty = true;
        self.own_frontier = recovered;
        self.seeded_anchor = None;
        self.persist_own_frontier(&header).await;
        log::info!(
            "vantage lanes: reconciled own lane to committed height={}",
            header.height
        );
        true
    }

    async fn seed_own_anchor_from_store(&mut self) {
        let bytes = match self.store.read(OWN_FRONTIER_HEADER_KEY.to_vec()).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(e) => {
                log::error!("vantage lanes: cannot read own frontier header: {e}");
                return;
            }
        };
        let record: PersistedFrontierHeader = match bincode::deserialize(&bytes) {
            Ok(record) => record,
            Err(e) => {
                log::error!("vantage lanes: cannot decode own frontier header: {e}");
                return;
            }
        };
        if record.sid != self.sid {
            return;
        }
        let header = record.header;
        if header.author != self.name
            || (header.height, &header.id) != (self.own_frontier.0, &self.own_frontier.1)
        {
            log::warn!(
                "vantage lanes: persisted frontier header does not match the restored \
                 frontier (author={} height={}); cache left unseeded",
                header.author,
                header.height,
            );
            return;
        }
        if !block_ok(&header, &self.committee, &self.sid, self.max_block_payload) {
            log::error!("vantage lanes: persisted frontier header fails block_ok; not seeded");
            return;
        }
        let r = (header.author, header.height, header.id.clone());
        log::info!(
            "vantage lanes: seeded own lane anchor at height={} into the block cache",
            header.height
        );
        self.blocks.lock().seed_own_anchor(header.clone());
        self.seeded_anchor = Some(header);
        self.pending_direct.insert(r);
    }

    /// Takes the restored anchor at most once.
    pub fn take_seeded_anchor(&mut self) -> Option<Header> {
        self.seeded_anchor.take()
    }

    fn enqueue_pending_direct(&mut self, r: BlockRef) {
        if self.acked.contains(&r) || self.pending_direct_blocked_by.contains_key(&r) {
            return;
        }
        if let Some(blocker) = self.inherited_direct_prefix_blocker(&r) {
            self.park_pending_direct_on(&r, blocker);
            return;
        }
        self.pending_direct.insert(r);
    }

    fn block_pending_direct_on(&mut self, r: &BlockRef, blocker: Digest) {
        self.pending_direct.remove(r);
        self.park_pending_direct_on(r, blocker);
    }

    fn park_pending_direct_on(&mut self, r: &BlockRef, blocker: Digest) {
        if self.acked.contains(r) {
            return;
        }
        if let Some(old) = self
            .pending_direct_blocked_by
            .insert(r.clone(), blocker.clone())
        {
            let remove_old =
                if let Some(waiters) = self.pending_direct_waiters_by_blocker.get_mut(&old) {
                    waiters.remove(r);
                    waiters.is_empty()
                } else {
                    false
                };
            if remove_old {
                self.pending_direct_waiters_by_blocker.remove(&old);
            }
        }
        self.pending_direct_waiters_by_blocker
            .entry(blocker.clone())
            .or_default()
            .insert(r.clone());
        self.direct_prefix_blocker_by_digest
            .insert(r.2.clone(), blocker);
    }

    fn inherited_direct_prefix_blocker(&mut self, r: &BlockRef) -> Option<Digest> {
        let blocks = self.blocks.lock();
        let entry = blocks.get(&r.2)?;
        let parent = entry.block.parent_cert.header_digest.clone();
        let blocker = self.direct_prefix_blocker_by_digest.get(&parent).cloned()?;
        if !blocks.direct_gate_ready(&blocker) {
            return Some(blocker);
        }
        drop(blocks);
        self.direct_prefix_blocker_by_digest.remove(&parent);
        None
    }

    fn note_direct_prefix_self_blocker(&mut self, blocker: &Digest) {
        self.direct_prefix_blocker_by_digest
            .insert(blocker.clone(), blocker.clone());
    }

    fn clear_direct_prefix_blocker(&mut self, blocker: &Digest) {
        if self
            .direct_prefix_blocker_by_digest
            .get(blocker)
            .is_some_and(|mapped| mapped == blocker)
        {
            self.direct_prefix_blocker_by_digest.remove(blocker);
        }
    }

    fn wake_pending_direct_blocker(&mut self, blocker: &Digest) -> BTreeSet<PublicKey> {
        let mut authors = BTreeSet::new();
        self.clear_direct_prefix_blocker(blocker);
        let Some(waiters) = self.pending_direct_waiters_by_blocker.remove(blocker) else {
            return authors;
        };
        for r in waiters {
            if self.pending_direct_blocked_by.remove(&r).is_some() {
                self.direct_prefix_blocker_by_digest.remove(&r.2);
                if !self.acked.contains(&r) {
                    authors.insert(r.0);
                    self.pending_direct.insert(r);
                }
            }
        }
        authors
    }

    fn refresh_woken_pending_direct(&mut self, blocker: &Digest) -> Vec<Effect> {
        let authors = self.wake_pending_direct_blocker(blocker);
        let mut effects = Vec::new();
        for author in authors {
            effects.extend(self.refresh_author(author));
        }
        effects
    }

    pub fn take_missing_parents(&mut self, cap: usize) -> Vec<BlockRef> {
        self.blocks.lock().take_missing_parents(cap)
    }

    /// Persists the new lane tip before returning its broadcast effect.
    pub async fn publish_own(
        &mut self,
        payload: BTreeMap<Digest, WorkerId>,
    ) -> (Header, Vec<Effect>) {
        let (height, prev) = self.own_frontier.clone();
        let next_height = height + 1;
        let header = Header::new_vantage(self.name, next_height, payload, prev, self.sid.clone());
        self.own_frontier = (next_height, header.id.clone());
        self.persist_own_frontier(&header).await;
        if let Some(metrics) = &self.metrics {
            metrics.vantage_blocks_published.inc();
        }
        let mut effects = self.process_publish_inner(self.name, header.clone()).await;
        effects.push(Effect::BroadcastPublish(header.clone()));
        (header, effects)
    }

    /// Accepts only session-valid blocks that pass `block_ok`.
    pub async fn process_publish(&mut self, sender: PublicKey, header: Header) -> Vec<Effect> {
        self.process_publish_inner(sender, header).await
    }

    async fn process_publish_inner(&mut self, sender: PublicKey, header: Header) -> Vec<Effect> {
        let mut effects = Vec::new();

        if header.sid.as_ref() != Some(&self.sid) {
            return effects;
        }
        if !block_ok(&header, &self.committee, &self.sid, self.max_block_payload) {
            return effects;
        }

        // Direct provenance requires the authenticated sender to equal the encoded author.
        let direct = sender == header.author;
        let missing_payload = self.missing_payload(&header).await;
        let payload_ok = missing_payload.is_empty();
        let digest = header.id.clone();

        let gate_ready = {
            let mut blocks = self.blocks.lock();
            blocks.upsert(header.clone(), direct, false, payload_ok, true);
            blocks.direct_gate_ready(&digest)
        };
        if direct && payload_ok {
            let r = (header.author, header.height, digest.clone());
            self.enqueue_pending_direct(r);
        } else if direct {
            self.note_direct_prefix_self_blocker(&digest);
        }
        effects.push(Effect::BlockCached(digest.clone()));
        if header.author != self.name {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_blocks_received.inc();
            }
        }

        if direct && !missing_payload.is_empty() {
            effects.push(Effect::SyncBatches(
                header.author,
                digest.clone(),
                missing_payload,
            ));
        }

        effects.extend(self.refresh_author(header.author));
        if gate_ready {
            effects.extend(self.refresh_woken_pending_direct(&digest));
        }
        effects
    }

    /// Returns payload entries absent under the `[digest || worker_id_le]` store key.
    pub(crate) async fn missing_payload(&mut self, header: &Header) -> Vec<(Digest, WorkerId)> {
        if header.author == self.name {
            return Vec::new();
        }
        let entries: Vec<_> = header.payload.iter().collect();
        let keys: Vec<_> = entries
            .iter()
            .map(|(digest, worker_id)| [digest.as_ref(), &worker_id.to_le_bytes()].concat())
            .collect();
        let found = {
            let _wait = self.metrics.as_ref().map(|metrics| {
                UtilizationTimer::from_counter(
                    self.wt_store_probe
                        .get_or_insert_with(|| {
                            metrics.core_wait_timer.with_label_values(&["store_probe"])
                        })
                        .clone(),
                )
            });
            self.store.read_many(keys).await
        };
        entries
            .iter()
            .enumerate()
            .filter(|(i, _)| found.get(*i).map(Option::is_none).unwrap_or(true))
            .map(|(_, (digest, worker_id))| ((*digest).clone(), **worker_id))
            .collect()
    }

    pub fn set_payload_ready(&mut self, digest: &Digest) -> Vec<Effect> {
        let (direct_ready, gate_ready) = {
            let mut blocks = self.blocks.lock();
            blocks.set_payload_ok(digest, true);
            (
                blocks.get(digest).and_then(|e| {
                    (e.direct && e.payload_ok)
                        .then(|| (e.block.author, e.block.height, digest.clone()))
                }),
                blocks.direct_gate_ready(digest),
            )
        };
        let mut effects = Vec::new();
        if let Some(r) = direct_ready {
            let author = r.0;
            self.enqueue_pending_direct(r);
            effects.extend(self.refresh_author(author));
        }
        if gate_ready {
            effects.extend(self.refresh_woken_pending_direct(digest));
        }
        effects
    }

    /// Checks a rotating bounded subset of pending direct references for `author`.
    fn refresh_author(&mut self, author: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        let lower = author_lower_bound(author);
        let upper = author_upper_bound(author);
        let mut refs = Vec::with_capacity(REFRESH_WALK_BUDGET);
        if let Some(after) = self.refresh_scan_after.get(&author) {
            refs.extend(
                self.pending_direct
                    .range((Excluded(after.clone()), Included(upper.clone())))
                    .take(REFRESH_WALK_BUDGET)
                    .cloned(),
            );
            if refs.len() < REFRESH_WALK_BUDGET {
                refs.extend(
                    self.pending_direct
                        .range((Included(lower), Included(after.clone())))
                        .take(REFRESH_WALK_BUDGET - refs.len())
                        .cloned(),
                );
            }
        } else {
            refs.extend(
                self.pending_direct
                    .range(lower..=upper)
                    .take(REFRESH_WALK_BUDGET)
                    .cloned(),
            );
        }
        if let Some(last) = refs.last() {
            self.refresh_scan_after.insert(author, last.clone());
        } else {
            self.refresh_scan_after.remove(&author);
        }
        let mut registers_changed = false;
        for r in &refs {
            if self.acked.contains(r) {
                self.pending_direct.remove(r);
                continue;
            }
            match self.direct_pub_check(r) {
                DirectPubCheck::Confirmed => {}
                DirectPubCheck::BlockedOnGate(blocker) => {
                    self.block_pending_direct_on(r, blocker);
                    continue;
                }
                DirectPubCheck::Failed => continue,
            }
            self.pending_direct.remove(r);
            let r = r.clone();
            self.direct_prefix_blocker_by_digest.remove(&r.2);
            self.on_direct_pub_confirmed(&r, &mut effects);
            registers_changed = true;
        }
        if registers_changed {
            self.refresh_registers(author);
        }
        effects
    }

    fn on_direct_pub_confirmed(&mut self, r: &BlockRef, effects: &mut Vec<Effect>) {
        self.retain_prefix(r);
        self.acked.insert(r.clone());
        let ack = Ack::new(r.0, r.1, r.2.clone(), self.name);
        effects.push(Effect::BroadcastAck(ack));
        if let Some(metrics) = &self.metrics {
            metrics.vantage_acks_sent.inc();
        }
        self.record_direct_pub(r);
    }

    /// Retains the verified prefix through `r`; retained prefixes are prefix-closed.
    fn retain_prefix(&mut self, r: &BlockRef) {
        let newly_retained_bytes = {
            let mut blocks = self.blocks.lock();
            Self::retain_prefix_locked(&mut blocks, &self.genesis, r)
        };
        self.record_retained_bytes(newly_retained_bytes);
    }

    fn retain_prefix_locked(blocks: &mut BlockCache, genesis: &Digest, r: &BlockRef) -> u64 {
        let mut cur = r.2.clone();
        let mut expected_height = r.1;
        let mut newly_retained_bytes: u64 = 0;
        loop {
            if &cur == genesis || expected_height == 0 {
                break;
            }
            let Some(entry) = blocks.get(&cur) else {
                break;
            };
            if entry.block.height != expected_height || entry.retained {
                break;
            }
            let next = entry.block.parent_cert.header_digest.clone();
            let size = bincode::serialized_size(&entry.block).unwrap_or(0);
            blocks.mark_retained(&cur);
            newly_retained_bytes += size;
            cur = next;
            expected_height -= 1;
        }
        newly_retained_bytes
    }

    fn record_retained_bytes(&self, newly_retained_bytes: u64) {
        if newly_retained_bytes > 0 {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_retained_bytes.inc_by(newly_retained_bytes);
            }
        }
    }

    /// Records only monotonic availability thresholds for an exact reference.
    pub fn process_ack_availability(&mut self, availability: AckAvailability) -> Vec<Effect> {
        self.process_availability(availability, true)
    }

    /// Records an ECHO-derived threshold without promoting a core candidate.
    /// Exact-position quorum promotion is tracked independently by `note_claim`;
    /// a quorum containing digest-free ancestor claims is only eventually common.
    pub fn process_claim_availability(&mut self, availability: AckAvailability) -> Vec<Effect> {
        self.process_availability(availability, false)
    }

    fn process_availability(
        &mut self,
        availability: AckAvailability,
        promote_core: bool,
    ) -> Vec<Effect> {
        let r = availability.reference;
        let threshold = availability.threshold;
        if self
            .ack_availability
            .get(&r)
            .is_some_and(|old| *old >= threshold)
        {
            return Vec::new();
        }
        self.ack_availability.insert(r.clone(), threshold);
        self.avail.note_threshold(&r, threshold);
        let high = self.avail_watermark_high.entry(r.0).or_insert(0);
        if r.1 > *high {
            *high = r.1;
        }
        if threshold >= AckThreshold::Quorum && self.direct_pub_refs.contains(&r) {
            let learned_claim_quorum = self.quorum_claim_refs.insert(r.clone());
            let learned_core_quorum = promote_core && self.quorum_direct_refs.insert(r.clone());
            if learned_claim_quorum || learned_core_quorum {
                self.refresh_registers(r.0);
            }
        }
        Vec::new()
    }

    /// Returns whether the exact reference has accumulated at least stake `q`.
    pub fn is_q_available(&self, r: &BlockRef, q: Stake) -> bool {
        match self.ack_availability.get(r) {
            Some(AckThreshold::Quorum) => q <= self.committee.quorum_threshold(),
            Some(AckThreshold::Validity) => q <= self.committee.validity_threshold(),
            None => false,
        }
    }

    /// Exact-position quorum evidence is bounded-common and core-eligible.
    pub fn is_exact_q_available(&self, r: &BlockRef) -> bool {
        self.avail.is_exact_quorum(r)
    }

    /// Requires an exact coordinate and a direct, payload-ready prefix through genesis.
    pub fn direct_pub(&self, r: &BlockRef) -> bool {
        matches!(self.direct_pub_check(r), DirectPubCheck::Confirmed)
    }

    fn direct_pub_check(&self, r: &BlockRef) -> DirectPubCheck {
        let mut blocks = self.blocks.lock();
        if !blocks
            .get(&r.2)
            .is_some_and(|entry| entry.block.author == r.0 && entry.block.height == r.1)
        {
            return DirectPubCheck::Failed;
        }
        if let Some(blocker) = self.direct_prefix_blocker_by_digest.get(&r.2) {
            if !blocks.direct_gate_ready(blocker) {
                return DirectPubCheck::BlockedOnGate(blocker.clone());
            }
        }
        if !blocks.verified_prefix_through_genesis(
            &self.committee,
            &self.sid,
            self.max_block_payload,
            &self.genesis,
            &r.2,
        ) {
            return DirectPubCheck::Failed;
        }
        match blocks.direct_prefix_check(&self.genesis, &r.2) {
            DirectPrefixCheck::Verified => DirectPubCheck::Confirmed,
            DirectPrefixCheck::Gate(blocker) => DirectPubCheck::BlockedOnGate(blocker),
            DirectPrefixCheck::Failed => DirectPubCheck::Failed,
        }
    }

    /// Returns true for a locally held valid prefix or a validity-threshold certificate.
    pub fn locally_available(&mut self, r: &BlockRef) -> bool {
        self.holds_prefix(r) || self.is_q_available(r, self.committee.validity_threshold())
    }

    /// Returns true for direct publication or a validity-threshold certificate.
    pub fn author_ok(&self, r: &BlockRef) -> bool {
        self.is_q_available(r, self.committee.validity_threshold()) || self.direct_pub(r)
    }

    /// Verifies and retains the exact reference's valid prefix when locally held.
    pub fn holds_prefix(&mut self, r: &BlockRef) -> bool {
        let (verified, newly_retained_bytes) = {
            let mut blocks = self.blocks.lock();
            let exact = blocks
                .get(&r.2)
                .is_some_and(|e| e.block.author == r.0 && e.block.height == r.1);
            if !exact {
                (false, 0)
            } else {
                let verified = blocks.verified_prefix_through_genesis(
                    &self.committee,
                    &self.sid,
                    self.max_block_payload,
                    &self.genesis,
                    &r.2,
                );
                let retained = if verified {
                    Self::retain_prefix_locked(&mut blocks, &self.genesis, r)
                } else {
                    0
                };
                (verified, retained)
            }
        };
        self.record_retained_bytes(newly_retained_bytes);
        verified
    }

    pub fn c_candidate(&self, author: &PublicKey) -> Option<BlockRef> {
        self.c_candidate.get(author).cloned()
    }

    pub fn t_candidate(&self, author: &PublicKey) -> Option<BlockRef> {
        self.t_candidate.get(author).cloned()
    }

    pub fn confirmation_candidate(&self, author: &PublicKey) -> Option<BlockRef> {
        self.confirmation_candidate.get(author).cloned()
    }

    fn record_direct_pub(&mut self, r: &BlockRef) {
        self.direct_pub_refs.insert(r.clone());
        if self.is_q_available(r, self.committee.quorum_threshold()) {
            self.quorum_claim_refs.insert(r.clone());
        }
        if self.avail.is_exact_quorum(r) {
            self.quorum_direct_refs.insert(r.clone());
        }
        let advances = match self.own_avail_watermark.get(&r.0) {
            Some((h, _)) => r.1 > *h,
            None => true,
        };
        if advances {
            self.own_avail_watermark.insert(r.0, (r.1, r.2.clone()));
            self.avail_dirty = true;
        }
    }

    /// Returns the full watermark vector once after any local watermark advances.
    pub fn take_avail_flush(&mut self) -> Option<Vec<AvailEntry>> {
        if !self.avail_dirty {
            return None;
        }
        self.avail_dirty = false;
        Some(
            self.own_avail_watermark
                .iter()
                .map(|(author, (height, head))| AvailEntry {
                    author: *author,
                    height: *height,
                    head: head.clone(),
                })
                .collect(),
        )
    }

    /// Returns this node's contiguous direct-prefix height for `author`, or zero.
    pub fn own_direct_frontier(&self, author: &PublicKey) -> Height {
        self.own_avail_watermark
            .get(author)
            .map(|(h, _)| *h)
            .unwrap_or(0)
    }

    /// Claims a proposal tip only when its exact coordinate is held and validated.
    pub fn build_avail_claim(&self, proposal: &crate::vantage::agb::ViewProposal) -> AvailClaim {
        let refs = manifest_refs(proposal);
        self.build_avail_claim_for_refs(&refs)
    }

    /// Builds claims for a skip-only batch proposal's `C || T` reference vector.
    pub fn build_batch_avail_claim(
        &self,
        proposal: &crate::vantage::agb::BatchViewProposal,
    ) -> AvailClaim {
        let refs = batch_manifest_refs(proposal);
        self.build_avail_claim_for_refs(&refs)
    }

    fn build_avail_claim_for_refs(&self, refs: &[&BlockRef]) -> AvailClaim {
        let mut claim = AvailClaim::with_capacity(refs.len());
        for (j, r) in refs.iter().enumerate() {
            if self.direct_pub_refs.contains(*r) {
                claim.set_at_tip(j);
                continue;
            }
            if let Some(ancestor) = self.greatest_direct_ancestor(r) {
                claim.push_short(j, r.1 - ancestor.1);
            }
        }
        claim
    }

    /// Returns the greatest directly published strict ancestor on `anchor`'s exact branch.
    fn greatest_direct_ancestor(&self, anchor: &BlockRef) -> Option<BlockRef> {
        self.direct_pub_refs
            .range(author_lower_bound(anchor.0)..=author_upper_bound(anchor.0))
            .rev()
            .filter(|candidate| candidate.1 < anchor.1)
            .find(|candidate| self.prefix_contains(anchor, candidate))
            .cloned()
    }

    pub fn avail_high(&self, author: &PublicKey) -> Height {
        self.avail_watermark_high.get(author).copied().unwrap_or(0)
    }

    pub fn own_tip_height(&self) -> Height {
        self.own_frontier.0
    }

    /// Returns the earliest cached height, or one because height zero is implicit genesis.
    pub fn earliest_authored_height(&self, author: &PublicKey) -> Height {
        self.blocks.lock().earliest_height(author).unwrap_or(1)
    }

    pub fn author_block_at(&self, author: &PublicKey, height: Height) -> Option<Header> {
        self.blocks.lock().author_block_at(author, height).cloned()
    }

    pub fn author_of(&self, digest: &Digest) -> Option<PublicKey> {
        self.blocks.lock().get(digest).map(|e| e.block.author)
    }

    /// Resolves height claims to exact locally validated references before crediting them.
    pub fn resolve_watermark(
        &mut self,
        sender: PublicKey,
        entries: &[AvailEntry],
    ) -> Vec<BlockRef> {
        self.avail.resolve_watermark(sender, entries)
    }

    pub fn note_claim(&mut self, sender: PublicKey, claims: &[ClaimRef]) -> Vec<BlockRef> {
        let credits = self.avail.note_claim(sender, claims);
        let mut changed = HashSet::new();
        for r in credits.newly_exact_quorum {
            if self.direct_pub_refs.contains(&r) && self.quorum_direct_refs.insert(r.clone()) {
                changed.insert(r.0);
            }
        }
        for author in changed {
            self.refresh_registers(author);
        }
        credits.references
    }

    pub fn claim_avail_height(&self, author: &PublicKey) -> Height {
        self.avail.avail_height(author)
    }

    pub fn retry_pending_avail(&mut self, digest: &Digest) -> Vec<(PublicKey, BlockRef)> {
        self.avail.retry_pending_avail(digest)
    }

    /// Refreshes candidates using greatest height and smallest-digest tie-breaking.
    fn refresh_registers(&mut self, author: PublicKey) {
        let c = newest_indexed(&self.quorum_direct_refs, author);
        set_candidate(&mut self.c_candidate, author, c.clone());

        let confirmation = self.least_confirmation_candidate(author, c.as_ref());
        set_candidate(&mut self.confirmation_candidate, author, confirmation);
        let t = self.newest_t_candidate(author, c.as_ref());
        set_candidate(&mut self.t_candidate, author, t);
    }

    /// Selects the least-height generally quorum-known prefix above `C` that
    /// still needs an exact-position ECHO census.  Holding this coordinate
    /// stable prevents continuous publication from making confirmation chase
    /// a fresh head forever.
    fn least_confirmation_candidate(
        &self,
        author: PublicKey,
        c: Option<&BlockRef>,
    ) -> Option<BlockRef> {
        let min_height = c.map_or(0, |c_ref| c_ref.1.saturating_add(1));
        for r in self
            .quorum_claim_refs
            .range(author_lower_bound_from(author, min_height)..=author_upper_bound(author))
        {
            if self.quorum_direct_refs.contains(r) {
                continue;
            }
            let qualifies = match c {
                Some(c_ref) => r.1 > c_ref.1 && self.prefix_contains(r, c_ref),
                None => true,
            };
            if qualifies {
                return Some(r.clone());
            }
        }
        None
    }

    fn newest_t_candidate(&self, author: PublicKey, c: Option<&BlockRef>) -> Option<BlockRef> {
        let min_height = c.map_or(0, |c_ref| c_ref.1.saturating_add(1));
        let mut current_height = None;
        let mut best_at_height = None;
        for r in self
            .direct_pub_refs
            .range(author_lower_bound_from(author, min_height)..=author_upper_bound(author))
            .rev()
        {
            if current_height.is_some_and(|height| height != r.1) {
                if best_at_height.is_some() {
                    return best_at_height;
                }
                current_height = Some(r.1);
            } else if current_height.is_none() {
                current_height = Some(r.1);
            }

            let qualifies = match c {
                Some(c_ref) => r.1 > c_ref.1 && self.prefix_contains(r, c_ref),
                None => true,
            };
            if qualifies {
                best_at_height = Some(r.clone());
            }
        }
        best_at_height
    }

    /// Returns whether `r` contains `target` at its exact height and digest.
    ///
    /// The walk pins the author and decreases expected height on every step.
    pub fn prefix_contains(&self, r: &BlockRef, target: &BlockRef) -> bool {
        if r.0 != target.0 || target.1 > r.1 {
            return false;
        }
        let blocks = self.blocks.lock();
        let author = r.0;
        let mut cur = r.2.clone();
        let mut expected_height = r.1;
        loop {
            if cur == self.genesis || expected_height == 0 {
                return false;
            }
            let Some(entry) = blocks.get(&cur) else {
                return false;
            };
            if entry.block.author != author {
                return false;
            }
            if entry.block.height != expected_height {
                return false;
            }
            if !entry.block_ok_verified {
                return false;
            }
            if expected_height == target.1 {
                return cur == target.2;
            }
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
    }
}
