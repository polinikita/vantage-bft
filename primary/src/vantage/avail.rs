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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    at_quorum: HashSet<BlockRef>,

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
            at_quorum: HashSet::new(),
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
    pub fn note_threshold(&mut self, r: &BlockRef, threshold: AckThreshold) {
        if threshold == AckThreshold::Quorum {
            self.at_quorum.insert(r.clone());
        }
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
                    if self.at_quorum.contains(&r) {
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
                if self.at_quorum.contains(&head) {
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
