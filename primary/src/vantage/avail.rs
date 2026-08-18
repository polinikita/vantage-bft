use crate::primary::Height;
use crate::vantage::claim::ClaimRef;
use crate::vantage::lanes::{AckAggregator, AckThreshold, AncestorWalk, AvailEntry, SharedBlocks};
use crate::vantage::BlockRef;
use config::Committee;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Maximum remembered quorum references per author.
pub(crate) const AT_QUORUM_HEIGHTS: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PendingRelativeClaim {
    sender: PublicKey,
    anchor: BlockRef,
    delta: Height,
}

#[derive(Default)]
pub(crate) struct ClaimCredits {
    pub(crate) references: Vec<BlockRef>,
    pub(crate) newly_exact_quorum: Vec<BlockRef>,
}

pub struct AvailResolver {
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// Credited prefix and fork digest for each `(sender, author)` pair.
    credited_floor: HashMap<(PublicKey, PublicKey), (Height, Digest)>,
    /// Latest unresolved watermark for each `(sender, author)` pair.
    pending_avail: HashMap<(PublicKey, PublicKey), AvailEntry>,
    /// Exact author-indexed mirror of the `pending_avail` keys.
    pending_avail_by_author: HashMap<PublicKey, HashSet<PublicKey>>,
    /// Tuple-specific unresolved digest-free ECHO claims.
    pending_relative: HashSet<PendingRelativeClaim>,
    /// Exact author-indexed mirror of `pending_relative`.
    pending_relative_by_author: HashMap<PublicKey, HashSet<PendingRelativeClaim>>,
    /// Recent quorum references keyed by height and digest to distinguish forks.
    at_quorum: HashMap<PublicKey, BTreeSet<(Height, Digest)>>,

    /// Highest verified claim by each sender for each author.
    claimed: HashMap<PublicKey, HashMap<PublicKey, (Height, Digest)>>,
    /// Exact-position claims have bounded commonality and alone may promote `C`.
    exact_claims: AckAggregator,

    metrics: Option<Arc<Metrics>>,
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
        Self {
            committee,
            sid,
            genesis,
            max_block_payload,
            blocks,
            credited_floor: HashMap::new(),
            pending_avail: HashMap::new(),
            pending_avail_by_author: HashMap::new(),
            pending_relative: HashSet::new(),
            pending_relative_by_author: HashMap::new(),
            at_quorum: HashMap::new(),
            claimed: HashMap::new(),
            exact_claims,
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Resets one author to a checkpoint-certified lane tip.
    pub fn reset_author(&mut self, author: PublicKey, anchor: &BlockRef) {
        self.credited_floor.retain(|(_, a), _| *a != author);
        let senders: Vec<_> = self.committee.authorities.keys().copied().collect();
        for sender in senders {
            self.credited_floor
                .insert((sender, author), (anchor.1, anchor.2.clone()));
        }
        self.pending_avail.retain(|(_, a), _| *a != author);
        self.pending_avail_by_author.remove(&author);
        self.pending_relative
            .retain(|pending| pending.anchor.0 != author);
        self.pending_relative_by_author.remove(&author);
        self.at_quorum
            .insert(author, [(anchor.1, anchor.2.clone())].into());
        self.claimed.remove(&author);
        self.exact_claims.reset_author(author);
    }

    /// Records monotonic positional claims and returns newly credited exact references.
    /// Digest-free ancestor claims remain pending until the anchor walk is locally verified.
    pub(crate) fn note_claim(&mut self, sender: PublicKey, claims: &[ClaimRef]) -> ClaimCredits {
        if !self.committee.authorities.contains_key(&sender) {
            return ClaimCredits::default();
        }
        let mut out = ClaimCredits::default();
        for claim in claims {
            match claim {
                ClaimRef::Exact(r) => {
                    let newly_counted = self.exact_claims.will_count(sender, r);
                    if self.note_exact_claim(sender, r.clone()) {
                        out.newly_exact_quorum.push(r.clone());
                    }
                    let mut credited = self.credit_claim_prefix(sender, r.clone());
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
                                .extend(self.credit_relative_target(sender, r));
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

    fn resolve_relative(&self, anchor: &BlockRef, delta: Height) -> AncestorWalk {
        self.blocks.lock().resolve_verified_ancestor(anchor, delta)
    }

    fn credit_claim_prefix(&mut self, sender: PublicKey, r: BlockRef) -> Vec<BlockRef> {
        let (author, height, digest) = (r.0, r.1, r.2.clone());
        let per_author = self.claimed.entry(author).or_default();
        if per_author.get(&sender).is_none_or(|(h, _)| *h < height) {
            per_author.insert(sender, (height, digest.clone()));
        }
        self.resolve_one(
            sender,
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

    fn credit_relative_target(&mut self, sender: PublicKey, r: BlockRef) -> Vec<BlockRef> {
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
        let Some(per_author) = self.claimed.get(author) else {
            return 0;
        };
        let mut by_height: Vec<(Height, config::Stake)> = per_author
            .iter()
            .map(|(s, (h, _))| (*h, self.committee.stake(s)))
            .collect();
        by_height.sort_unstable_by_key(|(h, _)| std::cmp::Reverse(*h));
        let mut acc: config::Stake = 0;
        for (h, stake) in by_height {
            acc += stake;
            if acc >= self.committee.quorum_threshold() {
                return h;
            }
        }
        0
    }

    #[cfg(test)]
    pub(crate) fn claimed_len_for_test(&self) -> usize {
        self.claimed.values().map(|m| m.len()).sum()
    }

    /// Remembers terminal quorum references and prunes only the optimization cache.
    pub fn note_threshold(&mut self, r: &BlockRef, threshold: AckThreshold) {
        if threshold != AckThreshold::Quorum {
            return;
        }
        let per_author = self.at_quorum.entry(r.0).or_default();
        per_author.insert((r.1, r.2.clone()));
        if per_author.len() > AT_QUORUM_HEIGHTS {
            if let Some(&(cut, _)) = per_author.iter().nth(per_author.len() - AT_QUORUM_HEIGHTS) {
                let keep = per_author.split_off(&(cut, Digest([0u8; 32])));
                *per_author = keep;
            }
        }
    }

    fn is_at_quorum(&self, r: &BlockRef) -> bool {
        self.at_quorum
            .get(&r.0)
            .is_some_and(|set| set.contains(&(r.1, r.2.clone())))
    }

    pub fn resolve_watermark(
        &mut self,
        sender: PublicKey,
        entries: &[AvailEntry],
    ) -> Vec<BlockRef> {
        let mut refs = Vec::new();
        for entry in entries {
            refs.extend(self.resolve_one(sender, entry));
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
        let keys: Vec<(PublicKey, PublicKey)> = self
            .pending_avail_by_author
            .get(&author)
            .map(|senders| senders.iter().map(|sender| (*sender, author)).collect())
            .unwrap_or_default();
        let mut out = Vec::new();
        for key in keys {
            let sender = key.0;
            let Some(entry) = self.pending_avail.get(&key).cloned() else {
                continue;
            };
            for r in self.resolve_one(sender, &entry) {
                out.push((sender, r));
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
                    for r in self.credit_relative_target(pending.sender, r) {
                        out.push((pending.sender, r));
                    }
                }
                AncestorWalk::Pending => {}
                AncestorWalk::Forked => self.remove_relative(&pending),
            }
        }
        out
    }

    fn resolve_one(&mut self, sender: PublicKey, entry: &AvailEntry) -> Vec<BlockRef> {
        let key = (sender, entry.author);
        let floor = self.credited_floor.get(&key);
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
        let segment = {
            let blocks = self.blocks.lock();
            blocks.collect_verified_suffix(
                &self.committee,
                &self.sid,
                self.max_block_payload,
                floor_height,
                &floor_digest,
                &entry.head,
            )
        };
        match segment {
            Some(suffix) => {
                let mut refs = Vec::with_capacity(suffix.len());
                for (i, d) in suffix.iter().enumerate() {
                    let r = (entry.author, floor_height + 1 + i as Height, d.clone());
                    if self.is_at_quorum(&r) {
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_avail_credit_skipped_total.inc();
                        }
                    } else {
                        refs.push(r);
                    }
                }
                if let Some(last) = suffix.last() {
                    self.credited_floor
                        .insert(key, (floor_height + suffix.len() as Height, last.clone()));
                }
                self.pending_avail.remove(&key);
                if let Some(senders) = self.pending_avail_by_author.get_mut(&key.1) {
                    senders.remove(&key.0);
                    if senders.is_empty() {
                        self.pending_avail_by_author.remove(&key.1);
                    }
                }
                refs
            }
            None => {
                self.pending_avail_by_author
                    .entry(key.1)
                    .or_default()
                    .insert(key.0);
                self.pending_avail.insert(key, entry.clone());
                let head = (entry.author, entry.height, entry.head.clone());
                if self.is_at_quorum(&head) {
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

    #[cfg(test)]
    pub(crate) fn at_quorum_len_for_test(&self, author: &PublicKey) -> usize {
        self.at_quorum.get(author).map_or(0, |s| s.len())
    }

    #[cfg(test)]
    pub(crate) fn is_at_quorum_for_test(&self, r: &BlockRef) -> bool {
        self.is_at_quorum(r)
    }

    #[cfg(test)]
    pub(crate) fn pending_avail_index_for_test(&self) -> HashSet<(PublicKey, PublicKey)> {
        self.pending_avail_by_author
            .iter()
            .flat_map(|(author, senders)| senders.iter().map(move |s| (*s, *author)))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn pending_avail_keys_for_test(&self) -> HashSet<(PublicKey, PublicKey)> {
        self.pending_avail.keys().copied().collect()
    }

    #[cfg(test)]
    pub(crate) fn pending_relative_len_for_test(&self) -> usize {
        self.pending_relative.len()
    }
}
