// Availability-watermark resolution, extracted from `LaneManager` so it can run OFF the
// core thread.
//
// WHY THIS IS A SEPARATE TYPE (2026-08-07). Measured at n=100 over a 122.6s window, on a
// single-threaded core that saturates at 122.6s:
//
//     healthy node   inbound_dispatch 60.4s + effect_execution  2.8s = 63.2s = 52% of a core
//     dying node     inbound_dispatch 80.6s + effect_execution 35.0s = 115.6s = 94%
//
// Only ~2x headroom exists even when healthy, so any node that picks up extra work crosses
// into saturation and never returns. Ack crediting is the largest single term: 190,292
// credited refs/s per node, 96.3 per avail message (a watermark carries one entry per
// author, so n per message), which at the measured 2.06us per credited ref is 48.1s = 39%
// of one core against the 49% total that `inbound_dispatch` costs.
//
// That volume is NOT waste. 100 senders x 100 authors x ~20 blocks/s is ~200,000 facts/s,
// matching the measured 190,292 within 5%, and `resolve_one` already early-returns before
// taking any lock when a watermark carries nothing new. So the work cannot be deleted --
// it is the true information rate of all-to-all availability. It can only be moved.
//
// What makes moving it sound is that this whole path is a FUNNEL: it consumes ~190k
// per-(sender, author, height) facts and emits only monotone threshold marks -- one
// `AckAvailability` per ref that crosses f+1 or 2f+1, i.e. ~2 per block rather than ~n. At
// n=100 that is roughly 4,000 marks/s against 190,000 credits/s, a ~47x reduction in what
// the core has to touch. The core already has an `Inbound::AckAvailability` arm, so it
// consumes exactly this shape today.
//
// State split. Everything here is either private to resolution or already shared:
//   - private: `credited_floor`, `pending_avail`, `pending_avail_by_author`, `at_quorum`
//   - already `Arc`-shared with the core: `BlockCache`, `AckAggregator`
//   - immutable for the run: committee, sid, genesis, max_block_payload
// `LaneManager::ack_availability` deliberately does NOT move: the core consumes marks from
// it (`is_q_available`), and this type tracks quorum in its own `at_quorum` set instead --
// it produces the marks, so it already knows which refs have crossed.

use crate::primary::Height;
use crate::vantage::lanes::{AckThreshold, AvailEntry, SharedBlocks};
use crate::vantage::BlockRef;
use config::Committee;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Per-author cap on the remembered at-quorum set. Bounds it at O(n x this) -- about 5 MB at
/// n=100 -- against the unbounded growth the first version had (~720 MB/hour per node).
/// Generous on purpose: the entries that matter are recent (the retry re-credit and
/// late-sender credits both concern the current tip region), so a large window costs little
/// and forgetting an old entry only costs one redundant credit.
pub(crate) const AT_QUORUM_HEIGHTS: usize = 1_024;

pub struct AvailResolver {
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// Per `(sender, author)`: the height up to which `sender`'s watermark has already been
    /// credited for `author`'s lane, with the digest at that height. The digest pins the
    /// FORK, so a sender is only ever credited along the chain it actually declared.
    credited_floor: HashMap<(PublicKey, PublicKey), (Height, Digest)>,
    /// Watermark entries whose segment below the head did not fully resolve, kept so
    /// `retry_pending_avail` can re-attempt once the missing blocks arrive. Latest-wins per
    /// `(sender, author)`, so bounded by O(n^2).
    pending_avail: HashMap<(PublicKey, PublicKey), AvailEntry>,
    /// Strict mirror of `pending_avail`'s key set, indexed by AUTHOR, so a newly cached
    /// block retries only the senders waiting on that author instead of scanning the whole
    /// map. Drift here silently stops a sender's watermark from ever resolving, which is why
    /// a test pins it as an exact mirror.
    pending_avail_by_author: HashMap<PublicKey, HashSet<PublicKey>>,
    /// Refs already at the terminal `Quorum` threshold, from the marks this resolver itself
    /// emitted. Lets a credit be dropped before it is even built: `record_ack` returns no
    /// availability past quorum, so all n senders credit the same block but only the first
    /// 2f+1 can change anything -- and `retry_pending_avail` re-credits a stuck head ref once
    /// per arriving block, unboundedly, until this cuts it off.
    ///
    /// BOUNDED per author to the most recent `AT_QUORUM_HEIGHTS` heights. The first version of
    /// this was a flat `HashSet<BlockRef>` that only ever grew -- ~2,000 refs/s at n=100 x
    /// ~100 B, about 720 MB/hour per node, i.e. a fresh instance of exactly the leak class
    /// this file's own header complains about. Pruning is safe because the set is a pure
    /// OPTIMIZATION: forgetting an entry costs one redundant credit (work), never correctness.
    ///
    /// Kept keyed by `(Height, Digest)` rather than collapsed to a per-author high-water
    /// height, even though quorum at height h normally implies quorum below it. Under an
    /// equivocating author two forks can sit at the same height, and a height-only mark would
    /// let fork A's quorum suppress crediting for fork B -- which is liveness-only, but it is
    /// a Byzantine-triggerable liveness bug, and the digest costs 32 bytes to avoid.
    at_quorum: HashMap<PublicKey, BTreeSet<(Height, Digest)>>,

    /// AVAIL-ECHO-SPEC.md step 4: the positional-claim front-end's state, keyed
    /// `author -> sender -> (height, anchor digest)` -- the highest verified prefix each
    /// sender has claimed on that author's lane.
    ///
    /// This is the SAME information `credited_floor` holds, transposed: that map is
    /// `(sender, author) -> height` and exists to avoid re-crediting; this one is indexed
    /// author-first because the question it answers is per-lane ("what stake has claimed
    /// at least h?"), which is an order statistic over senders. Bounded at O(n^2) by
    /// construction -- one entry per (author, sender) pair, overwritten in place.
    claimed: HashMap<PublicKey, HashMap<PublicKey, (Height, Digest)>>,

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
        Self {
            committee,
            sid,
            genesis,
            max_block_payload,
            blocks,
            credited_floor: HashMap::new(),
            pending_avail: HashMap::new(),
            pending_avail_by_author: HashMap::new(),
            at_quorum: HashMap::new(),
            claimed: HashMap::new(),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Record that `r` reached `threshold`, so future credits for it can be skipped once it
    /// is terminal. Fed from the marks this resolver emits, which is why it needs no access
    /// to the core's own `ack_availability`.
    ///
    /// Prunes this author's set to the most recent `AT_QUORUM_HEIGHTS` heights. See the field
    /// for why forgetting an entry is safe (it costs a redundant credit, never correctness)
    /// and why the digest stays in the key.
    /// AVAIL-ECHO-SPEC.md step 4: record `sender`'s positional claims, carried on an AGB
    /// echo for `proposal`. Returns the refs newly credited, for the caller to feed the
    /// shared `AckAggregator` exactly as `resolve_watermark`'s output is fed.
    ///
    /// Per claimed lane the sender asserts possession of the whole verified prefix through
    /// `(author, height)` on the chain ending at that claim's digest. Two checks before it
    /// counts, both of which a Byzantine sender can fail without harming anyone else:
    ///
    ///   1. MONOTONICITY. A claim at or below what this sender already claimed for that
    ///      author is dropped. Availability is a prefix fact, so a sender's own claims can
    ///      only advance; letting one regress would re-credit the same prefix forever, which
    ///      is the unbounded-recredit shape `at_quorum` exists to stop on the other path.
    ///   2. LINKAGE, for short claims only. The sender could not prove its prefix lies on
    ///      `chain(named)` without holding that chain (spec §4), so it sends its own head
    ///      digest and we check it here against OUR copy: the block it names must be held,
    ///      verified, and sit at exactly that coordinate. An at-tip claim needs no such
    ///      check -- its digest IS the proposal's, which `proposal_digest` already commits
    ///      to. When we cannot verify a short claim (we lack the block), we DROP rather
    ///      than stash: the sender re-claims on the next view's echo at no extra cost,
    ///      which is the simplification that lets `pending_avail` disappear.
    pub fn note_claim(
        &mut self,
        sender: PublicKey,
        proposal: &crate::vantage::agb::ViewProposal,
        claim: &crate::vantage::claim::AvailClaim,
    ) -> Vec<BlockRef> {
        if !self.committee.authorities.contains_key(&sender) {
            return Vec::new();
        }
        let refs = crate::vantage::claim::manifest_refs(proposal);
        let at_tip: HashSet<Digest> = refs
            .iter()
            .enumerate()
            .filter(|(j, _)| claim.is_at_tip(*j))
            .map(|(_, r)| r.2.clone())
            .collect();
        let mut out = Vec::new();
        let blocks = self.blocks.lock();
        for r in claim.resolve(&refs) {
            let (author, height, digest) = (r.0, r.1, r.2.clone());
            // (1) monotone per (author, sender).
            let per_author = self.claimed.entry(author).or_default();
            if per_author.get(&sender).is_some_and(|(h, _)| *h >= height) {
                continue;
            }
            // (2) linkage, short claims only (an at-tip digest came from the proposal).
            if !at_tip.contains(&digest) {
                let verifiable = blocks.get(&digest).is_some_and(|e| {
                    e.block.author == author && e.block.height == height && e.block_ok_verified
                });
                if !verifiable {
                    continue;
                }
            }
            per_author.insert(sender, (height, digest.clone()));
            out.push((author, height, digest));
        }
        out
    }

    /// The greatest height on `author`'s lane that a quorum of stake has claimed, i.e. the
    /// availability watermark the positional front-end computes. `0` if no quorum exists.
    ///
    /// A stake-weighted order statistic over at most `n` claims: sort descending by claimed
    /// height and walk until accumulated stake reaches `quorum_threshold`. `O(n log n)` per
    /// call against the old path's per-ref hash work -- and called once per lane per view
    /// rather than once per (sender, block).
    ///
    /// Monotone in the claims, because each sender's entry is itself monotone (see
    /// `note_claim`), which is what makes it a safe replacement for `credited_floor`.
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

    /// Live claim-state size, for the bound test: total `(author, sender)` entries.
    #[cfg(test)]
    pub(crate) fn claimed_len_for_test(&self) -> usize {
        self.claimed.values().map(|m| m.len()).sum()
    }

    pub fn note_threshold(&mut self, r: &BlockRef, threshold: AckThreshold) {
        if threshold != AckThreshold::Quorum {
            return;
        }
        let per_author = self.at_quorum.entry(r.0).or_default();
        per_author.insert((r.1, r.2.clone()));
        // `split_off` keeps the recent tail and drops the old head, the same GC discipline
        // used elsewhere in this crate -- no `retain`, no full scan.
        if per_author.len() > AT_QUORUM_HEIGHTS {
            if let Some(&(cut, _)) = per_author.iter().nth(per_author.len() - AT_QUORUM_HEIGHTS) {
                let keep = per_author.split_off(&(cut, Digest([0u8; 32])));
                *per_author = keep;
            }
        }
    }

    /// Whether `r` is known to have reached the terminal `Quorum` threshold.
    fn is_at_quorum(&self, r: &BlockRef) -> bool {
        self.at_quorum
            .get(&r.0)
            .is_some_and(|set| set.contains(&(r.1, r.2.clone())))
    }

    /// N5 ack-watermark front-end: resolve every entry in one peer's watermark message.
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

    /// Re-attempt every `(sender, author)` watermark entry pending on `digest`'s author, now
    /// that `digest` has just been cached. Returns `(sender, ref)` pairs so the caller can
    /// credit each under the correct declaring sender.
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
        out
    }

    /// Resolve one entry against `sender`'s current credited floor for `entry.author`.
    ///
    /// Monotone: an entry at or below the floor is ignored, before any lock is taken -- pure
    /// liveness, a stale resend costs nothing. On success the credited refs and the new floor
    /// come from the WALK's own result (`collect_verified_suffix` re-derives every height
    /// from the actual cached chain, never from the declared height), so a lying declared
    /// height can only make this a no-op, never advance the floor past what was verified. On
    /// failure the head ref alone is credited -- exactly as a direct ack for that tuple
    /// would be -- and the entry is stashed for retry.
    fn resolve_one(&mut self, sender: PublicKey, entry: &AvailEntry) -> Vec<BlockRef> {
        let key = (sender, entry.author);
        let (floor_height, floor_digest) = self
            .credited_floor
            .get(&key)
            .cloned()
            .unwrap_or_else(|| (0, self.genesis.clone()));
        if entry.height <= floor_height {
            return Vec::new();
        }
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

    /// Remembered at-quorum entries for `author`, for the bound test.
    #[cfg(test)]
    pub(crate) fn at_quorum_len_for_test(&self, author: &PublicKey) -> usize {
        self.at_quorum.get(author).map_or(0, |s| s.len())
    }

    #[cfg(test)]
    pub(crate) fn is_at_quorum_for_test(&self, r: &BlockRef) -> bool {
        self.is_at_quorum(r)
    }

    /// The `pending_avail` index's own key set, for the test that pins it as a strict mirror
    /// of `pending_avail`. A drifted index would silently stop retrying a stashed entry.
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
}
