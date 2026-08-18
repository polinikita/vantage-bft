use crate::primary::Height;
use crate::vantage::claim::ClaimRef;
use crate::vantage::index::{ByAuthor, ByPair, CommitteeIndex, Slot, SlotSet};
use crate::vantage::lanes::{AckAggregator, AckThreshold, AncestorWalk, AvailEntry, SharedBlocks};
use crate::vantage::BlockRef;
use config::Committee;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;

/// Maximum remembered quorum references per author.
pub(crate) const AT_QUORUM_HEIGHTS: usize = 1_024;

/// Maximum remembered verified heights per author in the shared chain segment.
const CHAIN_SEGMENT_HEIGHTS: Height = 256;

/// Maximum remembered anchor-height span per author for derived relative targets.
///
/// Anchors are proposal manifest entries, so only the newest heights are claimed against;
/// a narrow span keeps the count bound below out of reach of honest traffic.
const RELATIVE_ANCHOR_HEIGHTS: Height = 32;

/// Maximum remembered relative targets per author, whatever their anchor span.
const RELATIVE_TARGETS: usize = 1_024;

/// Derived ancestors keyed by anchor height, anchor digest, and claimed distance.
type RelativeTargets = BTreeMap<(Height, Digest, Height), BlockRef>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PendingRelativeClaim {
    sender: PublicKey,
    anchor: BlockRef,
    delta: Height,
}

/// One contiguous run of verified coordinates on a single fork of one author's chain.
///
/// Every coordinate is appended from a walk that verified its parent link, so two runs
/// that agree at one height agree at every lower height they both cover: one shared
/// coordinate decides whether a walk extends this run or replaces it.
#[derive(Default)]
struct ChainSegment {
    heights: BTreeMap<Height, Digest>,
}

impl ChainSegment {
    fn digest_at(&self, height: Height) -> Option<&Digest> {
        self.heights.get(&height)
    }

    /// Returns the digests over `(floor_height, height]` when this run is the exact fork
    /// named by both endpoints.
    ///
    /// Both endpoints are compared: a sender whose floor sits on another fork, or whose
    /// head is another fork's block, must fall back to its own walk.
    fn suffix(
        &self,
        floor_height: Height,
        floor_digest: &Digest,
        height: Height,
        head: &Digest,
    ) -> Option<Vec<Digest>> {
        if self.digest_at(floor_height) != Some(floor_digest)
            || self.digest_at(height) != Some(head)
        {
            return None;
        }
        let suffix: Vec<Digest> = self
            .heights
            .range((Excluded(floor_height), Included(height)))
            .map(|(_, digest)| digest.clone())
            .collect();
        // A gap inside the run would skip a link the walk validates.
        (suffix.len() as Height == height - floor_height).then_some(suffix)
    }

    /// Returns the digest `target_height` below the exact coordinate `(height, digest)`.
    fn ancestor(&self, height: Height, digest: &Digest, target_height: Height) -> Option<&Digest> {
        if self.digest_at(height) != Some(digest) {
            return None;
        }
        let target = self.digest_at(target_height)?;
        // Every intermediate link must be present for the same reason.
        let span = self.heights.range(target_height..=height).count() as Height;
        (span == height - target_height + 1).then_some(target)
    }

    /// Returns the highest height held both by this run and by `[lo, hi]`.
    fn shared_height(&self, lo: Height, hi: Height) -> Option<Height> {
        let base = *self.heights.keys().next()?;
        let top = *self.heights.keys().next_back()?;
        let shared = top.min(hi);
        (base.max(lo) <= shared).then_some(shared)
    }

    /// Drops every coordinate further than `CHAIN_SEGMENT_HEIGHTS` below the run's top.
    fn prune_below_window(&mut self) {
        let Some(cut) = self
            .heights
            .keys()
            .next_back()
            .and_then(|top| top.checked_sub(CHAIN_SEGMENT_HEIGHTS))
        else {
            return;
        };
        let keep = self.heights.split_off(&cut);
        self.heights = keep;
    }
}

#[derive(Default)]
pub(crate) struct ClaimCredits {
    pub(crate) references: Vec<BlockRef>,
    pub(crate) newly_exact_quorum: Vec<BlockRef>,
}

pub struct AvailResolver {
    committee: Committee,
    index: Arc<CommitteeIndex>,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// Credited prefix and fork digest for each `(sender, author)` pair.
    credited_floor: ByPair<(Height, Digest)>,
    /// Latest unresolved watermark for each `(sender, author)` pair.
    pending_avail: ByPair<AvailEntry>,
    /// Exact author-indexed mirror of the `pending_avail` keys.
    pending_avail_by_author: ByAuthor<SlotSet>,
    /// Tuple-specific unresolved digest-free ECHO claims.
    pending_relative: HashSet<PendingRelativeClaim>,
    /// Exact author-indexed mirror of `pending_relative`.
    pending_relative_by_author: HashMap<PublicKey, HashSet<PendingRelativeClaim>>,
    /// Recent quorum references keyed by height and digest to distinguish forks.
    at_quorum: ByAuthor<BTreeSet<(Height, Digest)>>,

    /// Highest verified claim by each sender for each author.
    claimed: ByPair<(Height, Digest)>,
    /// Exact-position claims have bounded commonality and alone may promote `C`.
    exact_claims: AckAggregator,

    /// One verified run per author, shared by every sender claiming that lane.
    ///
    /// Positional claims name the same targets for all senders, so the segment between a
    /// sender's floor and its claim is walked once per author and sliced afterwards.
    chain_segments: ByAuthor<ChainSegment>,
    /// Derived ancestors per author for anchors the shared run does not cover.
    relative_targets: ByAuthor<RelativeTargets>,

    metrics: Option<Arc<Metrics>>,

    #[cfg(test)]
    segment_walks: u64,
}

impl AvailResolver {
    pub fn new(
        committee: Committee,
        sid: Digest,
        genesis: Digest,
        max_block_payload: usize,
        blocks: SharedBlocks,
    ) -> Self {
        let exact_claims = AckAggregator::new(committee.clone());
        let index = CommitteeIndex::new(&committee);
        Self {
            committee,
            sid,
            genesis,
            max_block_payload,
            blocks,
            credited_floor: ByPair::new(index.clone()),
            pending_avail: ByPair::new(index.clone()),
            pending_avail_by_author: ByAuthor::new(index.clone()),
            pending_relative: HashSet::new(),
            pending_relative_by_author: HashMap::new(),
            at_quorum: ByAuthor::new(index.clone()),
            claimed: ByPair::new(index.clone()),
            exact_claims,
            chain_segments: ByAuthor::new(index.clone()),
            relative_targets: ByAuthor::new(index.clone()),
            index,
            metrics: None,
            #[cfg(test)]
            segment_walks: 0,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Resets one author to a checkpoint-certified lane tip.
    pub fn reset_author(&mut self, author: PublicKey, anchor: &BlockRef) {
        let lane = self.index.slot(&author);
        self.credited_floor.clear_author(&lane);
        for position in 0..self.index.size() {
            let sender = self.index.at(position);
            self.credited_floor
                .insert(&sender, &lane, (anchor.1, anchor.2.clone()));
        }
        self.pending_avail.clear_author(&lane);
        self.pending_avail_by_author.clear(&lane);
        self.pending_relative
            .retain(|pending| pending.anchor.0 != author);
        self.pending_relative_by_author.remove(&author);
        *self.at_quorum.entry(&lane) = [(anchor.1, anchor.2.clone())].into();
        self.claimed.clear_author(&lane);
        self.exact_claims.reset_author(author);
        // The anchor replaces this lane's history, so nothing remembered below it may still
        // answer a claim.
        self.chain_segments.clear(&lane);
        self.relative_targets.clear(&lane);
    }

    /// Records monotonic positional claims and returns newly credited exact references.
    /// Digest-free ancestor claims remain pending until the anchor walk is locally verified.
    pub(crate) fn note_claim(&mut self, sender: PublicKey, claims: &[ClaimRef]) -> ClaimCredits {
        if !self.index.is_member(&sender) {
            return ClaimCredits::default();
        }
        let sender_slot = self.index.slot(&sender);
        let mut out = ClaimCredits::default();
        for claim in claims {
            match claim {
                ClaimRef::Exact(r) => {
                    let newly_counted = self.exact_claims.will_count(sender, r);
                    if self.note_exact_claim(sender, r.clone()) {
                        out.newly_exact_quorum.push(r.clone());
                    }
                    let mut credited = self.credit_claim_prefix(&sender_slot, r.clone());
                    // A direct prefix claim covers every ancestor.  A forked or
                    // lower exact tuple still counts on its own even when the
                    // monotone prefix cursor cannot backfill that branch.
                    if newly_counted && !credited.contains(r) && !self.is_at_quorum(r) {
                        credited.push(r.clone());
                    }
                    out.references.extend(credited);
                }
                ClaimRef::Ancestor { anchor, delta } => {
                    if *delta == 0 || *delta >= anchor.1 {
                        continue;
                    }
                    match self.resolve_relative(anchor, *delta) {
                        AncestorWalk::Ready(r) => {
                            out.references
                                .extend(self.credit_relative_target(&sender_slot, r));
                        }
                        AncestorWalk::Pending => {
                            self.remember_relative(sender, anchor.clone(), *delta);
                        }
                        AncestorWalk::Forked => {}
                    }
                }
            }
        }
        out
    }

    /// Counts one exact-position claim and reports the first quorum crossing.
    pub(crate) fn note_exact_claim(&mut self, sender: PublicKey, r: BlockRef) -> bool {
        self.exact_claims
            .record_ack(sender, r)
            .availability
            .is_some_and(|availability| availability.threshold == AckThreshold::Quorum)
    }

    pub(crate) fn is_exact_quorum(&self, r: &BlockRef) -> bool {
        self.exact_claims.is_at_quorum(r)
    }

    /// Derives an ancestor from the shared run or the per-anchor memo, walking only when
    /// neither covers the tuple.
    fn resolve_relative(&mut self, anchor: &BlockRef, delta: Height) -> AncestorWalk {
        // The walk answers both of these as forked, so no remembered coordinate -- least of
        // all height zero, which every fork shares -- may answer in its place.
        if delta > 0 && delta < anchor.1 {
            if let Some(r) = self.cached_relative(anchor, delta) {
                return AncestorWalk::Ready(r);
            }
        }
        let walked = self.blocks.lock().resolve_verified_ancestor(anchor, delta);
        if let AncestorWalk::Ready(r) = &walked {
            self.memoize_relative(anchor, delta, r.clone());
        }
        walked
    }

    fn cached_relative(&self, anchor: &BlockRef, delta: Height) -> Option<BlockRef> {
        let target_height = anchor.1 - delta;
        let lane = self.index.slot(&anchor.0);
        let from_segment = self
            .chain_segments
            .get(&lane)
            .and_then(|segment| segment.ancestor(anchor.1, &anchor.2, target_height))
            .map(|digest| (anchor.0, target_height, digest.clone()));
        from_segment.or_else(|| {
            self.relative_targets
                .get(&lane)?
                .get(&(anchor.1, anchor.2.clone(), delta))
                .cloned()
        })
    }

    /// Remembers one derived ancestor for the exact `(anchor, delta)` tuple.
    fn memoize_relative(&mut self, anchor: &BlockRef, delta: Height, target: BlockRef) {
        let lane = self.index.slot(&anchor.0);
        let memo = self.relative_targets.entry(&lane);
        memo.insert((anchor.1, anchor.2.clone(), delta), target);
        // Anchor height orders the memo, so one split bounds how far back anchors are kept.
        if let Some(cut) = memo
            .last_key_value()
            .map(|((height, _, _), _)| *height)
            .and_then(|top| top.checked_sub(RELATIVE_ANCHOR_HEIGHTS))
        {
            let keep = memo.split_off(&(cut, Digest([0u8; 32]), 0));
            *memo = keep;
        }
        // That window cannot bound how many distinct distances one anchor attracts, so the
        // count is the hard bound; dropping the memo only costs later walks.
        if memo.len() > RELATIVE_TARGETS {
            memo.clear();
        }
    }

    fn credit_claim_prefix(&mut self, sender: &Slot, r: BlockRef) -> Vec<BlockRef> {
        let (author, height, digest) = (r.0, r.1, r.2.clone());
        let lane = self.index.slot(&author);
        if self
            .claimed
            .get(sender, &lane)
            .is_none_or(|(h, _)| *h < height)
        {
            self.claimed.insert(sender, &lane, (height, digest.clone()));
        }
        self.resolve_one(
            sender,
            &lane,
            &AvailEntry {
                author,
                height,
                head: digest,
            },
        )
    }

    fn remember_relative(&mut self, sender: PublicKey, anchor: BlockRef, delta: Height) {
        let author = anchor.0;
        let pending = PendingRelativeClaim {
            sender,
            anchor,
            delta,
        };
        if !self.pending_relative.insert(pending.clone()) {
            return;
        }
        self.pending_relative_by_author
            .entry(author)
            .or_default()
            .insert(pending);
    }

    fn credit_relative_target(&mut self, sender: &Slot, r: BlockRef) -> Vec<BlockRef> {
        let mut credited = self.credit_claim_prefix(sender, r.clone());
        // The prefix cursor is only an optimization and may already point to a
        // different fork.  The exact derived tuple must still reach the
        // first-hand aggregator.
        if !credited.contains(&r) && !self.is_at_quorum(&r) {
            credited.push(r);
        }
        credited
    }

    fn remove_relative(&mut self, pending: &PendingRelativeClaim) {
        self.pending_relative.remove(pending);
        let author = pending.anchor.0;
        if let Some(claims) = self.pending_relative_by_author.get_mut(&author) {
            claims.remove(pending);
            if claims.is_empty() {
                self.pending_relative_by_author.remove(&author);
            }
        }
    }

    /// Returns the greatest height supported by quorum stake for `author`.
    pub fn avail_height(&self, author: &PublicKey) -> Height {
        let lane = self.index.slot(author);
        let mut by_height: Vec<(Height, config::Stake)> = self
            .claimed
            .row(&lane)
            .map(|(sender, (h, _))| (*h, self.index.stake_of(&sender)))
            .collect();
        by_height.sort_unstable_by_key(|(h, _)| std::cmp::Reverse(*h));
        let mut acc: config::Stake = 0;
        for (h, stake) in by_height {
            acc += stake;
            if acc >= self.index.quorum_threshold() {
                return h;
            }
        }
        0
    }

    #[cfg(test)]
    pub(crate) fn claimed_len_for_test(&self) -> usize {
        self.claimed.pairs().len()
    }

    /// Remembers terminal quorum references and prunes only the optimization cache.
    pub fn note_threshold(&mut self, r: &BlockRef, threshold: AckThreshold) {
        if threshold != AckThreshold::Quorum {
            return;
        }
        let lane = self.index.slot(&r.0);
        let per_author = self.at_quorum.entry(&lane);
        per_author.insert((r.1, r.2.clone()));
        if per_author.len() > AT_QUORUM_HEIGHTS {
            if let Some(&(cut, _)) = per_author.iter().nth(per_author.len() - AT_QUORUM_HEIGHTS) {
                let keep = per_author.split_off(&(cut, Digest([0u8; 32])));
                *per_author = keep;
            }
        }
    }

    fn is_at_quorum(&self, r: &BlockRef) -> bool {
        self.lane_at_quorum(&self.index.slot(&r.0), r.1, &r.2)
    }

    fn lane_at_quorum(&self, lane: &Slot, height: Height, digest: &Digest) -> bool {
        self.at_quorum
            .get(lane)
            .is_some_and(|set| set.contains(&(height, digest.clone())))
    }

    pub fn resolve_watermark(
        &mut self,
        sender: PublicKey,
        entries: &[AvailEntry],
    ) -> Vec<BlockRef> {
        let sender = self.index.slot(&sender);
        let mut refs = Vec::new();
        for entry in entries {
            let lane = self.index.slot(&entry.author);
            refs.extend(self.resolve_one(&sender, &lane, entry));
        }
        refs
    }

    /// Retries only unresolved watermarks for the author of the newly cached block.
    pub fn retry_pending_avail(&mut self, digest: &Digest) -> Vec<(PublicKey, BlockRef)> {
        let author = {
            let blocks = self.blocks.lock();
            blocks.get(digest).map(|e| e.block.author)
        };
        let Some(author) = author else {
            return Vec::new();
        };
        let lane = self.index.slot(&author);
        let senders: Vec<Slot> = self
            .pending_avail_by_author
            .get(&lane)
            .map(|senders| senders.iter(&self.index).collect())
            .unwrap_or_default();
        let mut out = Vec::new();
        for sender in senders {
            let Some(entry) = self.pending_avail.get(&sender, &lane).cloned() else {
                continue;
            };
            for r in self.resolve_one(&sender, &lane, &entry) {
                out.push((sender.key(), r));
            }
        }
        let relative_claims: Vec<PendingRelativeClaim> = self
            .pending_relative_by_author
            .get(&author)
            .map(|claims| claims.iter().cloned().collect())
            .unwrap_or_default();
        for pending in relative_claims {
            if !self.pending_relative.contains(&pending) {
                continue;
            }
            match self.resolve_relative(&pending.anchor, pending.delta) {
                AncestorWalk::Ready(r) => {
                    self.remove_relative(&pending);
                    let sender = self.index.slot(&pending.sender);
                    for r in self.credit_relative_target(&sender, r) {
                        out.push((pending.sender, r));
                    }
                }
                AncestorWalk::Pending => {}
                AncestorWalk::Forked => self.remove_relative(&pending),
            }
        }
        out
    }

    fn resolve_one(&mut self, sender: &Slot, lane: &Slot, entry: &AvailEntry) -> Vec<BlockRef> {
        let floor = self.credited_floor.get(sender, lane);
        // The floor probe compares heights before cloning anything, and it also covers the
        // never-credited key: that floor is genesis, where an entry at height zero claims
        // nothing to walk.
        let floor_height = floor.map_or(0, |(height, _)| *height);
        if entry.height <= floor_height {
            return Vec::new();
        }
        let floor_digest = match floor {
            Some((_, digest)) => digest.clone(),
            None => self.genesis.clone(),
        };
        let segment = self.verified_segment(lane, entry, floor_height, &floor_digest);
        match segment {
            Some(suffix) => {
                let mut refs = Vec::with_capacity(suffix.len());
                for (i, d) in suffix.iter().enumerate() {
                    let height = floor_height + 1 + i as Height;
                    if self.lane_at_quorum(lane, height, d) {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_avail_credit_skipped_total.inc();
                        }
                    } else {
                        refs.push((entry.author, height, d.clone()));
                    }
                }
                if let Some(last) = suffix.last() {
                    self.credited_floor.insert(
                        sender,
                        lane,
                        (floor_height + suffix.len() as Height, last.clone()),
                    );
                }
                self.pending_avail.remove(sender, lane);
                self.pending_avail_by_author.entry(lane).remove(sender);
                refs
            }
            None => {
                self.pending_avail_by_author.entry(lane).insert(sender);
                self.pending_avail.insert(sender, lane, entry.clone());
                let head = (entry.author, entry.height, entry.head.clone());
                if self.lane_at_quorum(lane, head.1, &head.2) {
                    if let Some(metrics) = &self.metrics {
                        metrics.vantage_avail_credit_skipped_total.inc();
                    }
                    Vec::new()
                } else {
                    vec![head]
                }
            }
        }
    }

    /// Returns the verified suffix above the floor, sliced from the author's shared run when
    /// it covers the same fork and walked under the blocks lock otherwise.
    fn verified_segment(
        &mut self,
        lane: &Slot,
        entry: &AvailEntry,
        floor_height: Height,
        floor_digest: &Digest,
    ) -> Option<Vec<Digest>> {
        if let Some(suffix) = self.chain_segments.get(lane).and_then(|segment| {
            segment.suffix(floor_height, floor_digest, entry.height, &entry.head)
        }) {
            return Some(suffix);
        }
        #[cfg(test)]
        {
            self.segment_walks += 1;
        }
        let walked = {
            let blocks = self.blocks.lock();
            blocks.collect_verified_suffix(
                &self.committee,
                &self.sid,
                self.max_block_payload,
                floor_height,
                floor_digest,
                &entry.head,
            )
        };
        if let Some(suffix) = &walked {
            self.memoize_segment(lane, floor_height, floor_digest, suffix);
        }
        walked
    }

    /// Records a walked run so later senders on the same fork are served by slicing.
    ///
    /// One shared coordinate decides the merge: agreement there means the two runs agree
    /// wherever they overlap, and anything else -- another fork, or a gap between the two
    /// spans -- replaces the run rather than merging into it.
    fn memoize_segment(
        &mut self,
        lane: &Slot,
        floor_height: Height,
        floor_digest: &Digest,
        suffix: &[Digest],
    ) {
        if suffix.is_empty() {
            return;
        }
        let walked_at = |height: Height| -> &Digest {
            if height == floor_height {
                floor_digest
            } else {
                &suffix[(height - floor_height - 1) as usize]
            }
        };
        let walked_top = floor_height + suffix.len() as Height;
        let segment = self.chain_segments.entry(lane);
        let agrees = segment
            .shared_height(floor_height, walked_top)
            .is_some_and(|height| segment.digest_at(height) == Some(walked_at(height)));
        if !agrees {
            segment.heights.clear();
        }
        segment.heights.insert(floor_height, floor_digest.clone());
        for (i, digest) in suffix.iter().enumerate() {
            segment
                .heights
                .insert(floor_height + 1 + i as Height, digest.clone());
        }
        // The run grows only here, so one ordered split after every extension is the whole
        // memory bound: no scan over senders or their floors.
        segment.prune_below_window();
    }

    #[cfg(test)]
    pub(crate) fn segment_walks_for_test(&self) -> u64 {
        self.segment_walks
    }

    #[cfg(test)]
    pub(crate) fn at_quorum_len_for_test(&self, author: &PublicKey) -> usize {
        self.at_quorum
            .get(&self.index.slot(author))
            .map_or(0, |s| s.len())
    }

    #[cfg(test)]
    pub(crate) fn is_at_quorum_for_test(&self, r: &BlockRef) -> bool {
        self.is_at_quorum(r)
    }

    #[cfg(test)]
    pub(crate) fn pending_avail_index_for_test(&self) -> HashSet<(PublicKey, PublicKey)> {
        self.pending_avail_by_author
            .iter()
            .flat_map(|(author, senders)| {
                senders
                    .iter(&self.index)
                    .map(move |sender| (sender.key(), author.key()))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn pending_avail_keys_for_test(&self) -> HashSet<(PublicKey, PublicKey)> {
        self.pending_avail.pairs().into_iter().collect()
    }

    #[cfg(test)]
    pub(crate) fn pending_relative_len_for_test(&self) -> usize {
        self.pending_relative.len()
    }
}
