// PHASE3-SPEC.md §3.3 -- Authorize walk + serving (N6-N8).
//
// `Repairer` owns the request-out / answer-in bookkeeping (`authorized`, `requested`,
// `pendingReq`, `answered`) and shares `lanes::BlockCache` with `LaneManager` (see
// lanes.rs's module doc for why). Shape follows `HeaderWaiter`+`Helper`
// (request-out / answer-in split) with the paper's differences (D2): requests fan out
// to every other primary at most once per peer, ESCALATING in batches off the node's
// 1s tick rather than all at once (see `fan_out`); serving keeps `pendingReq` for
// blocks not yet held instead of dropping unknown digests (Autobahn's `Helper` drops
// them, helper.rs:77 -- we must not, N7).

use crate::messages::Header;
use crate::primary::Height;
use crate::vantage::block::block_ok;
use crate::vantage::lanes::SharedBlocks;
use crate::vantage::{BlockRef, Effect};
use config::Committee;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Peers asked in the FIRST round for a newly-missing digest. The full fan-out is still
/// reached by escalation (`fan_out`), so this only sets how much traffic the common case
/// costs -- and in the common case the block is held by nearly everyone, so a handful of
/// peers answers immediately.
pub(crate) const FANOUT_FIRST: usize = 4;

/// Digests escalated per `retry_requests` call. Without a budget the escalation pass is
/// itself an unbounded per-tick sweep -- the exact shape that made `on_block_available`,
/// `retry_pending_avail` and `try_serve` pathological at n=100.
pub(crate) const FANOUT_ESCALATE_BUDGET: usize = 256;

/// Per-digest fan-out progress. See `fan_out` for why coverage is staged.
struct FanoutState {
    /// Where in `peers` this digest's fan-out begins, derived from the digest itself.
    /// Without this every node asks `peers[0..k]` for every digest, so peer 0 serves the
    /// whole committee's repair load while peer n-1 serves none. Digest-derived (not
    /// random) keeps it deterministic and reproducible across runs.
    start: usize,
    /// How many peers, counting forward from `start` (wrapping), have been asked.
    asked: usize,
    /// Size of the NEXT batch. Doubling reaches all n-1 peers in O(log n) ticks, so the
    /// worst-case time-to-full-coverage stays small while the common case stays cheap.
    next_width: usize,
}

pub struct Repairer {
    committee: Committee,
    /// The other primaries' keys, resolved ONCE at construction. `Committee::
    /// others_primaries` builds a fresh `Vec<(PublicKey, PrimaryAddresses)>` per call
    /// (~12.7 KB at n=100, addresses included, none of which the repair fan-out
    /// wants), and `settle`'s miss branch used to call it on every miss. The committee
    /// is immutable for a run, so this is the same list, allocated never instead of
    /// per call.
    peers: Vec<PublicKey>,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// N6: every `(a, k, h)` ever passed to `authorize`.
    authorized: HashSet<BlockRef>,
    /// PHASE6-SPEC.md §9 gate amendment, R1 (D6-7 continued): every `BlockRef` for which
    /// `settle` has ever returned `true` (verified-through-genesis + retained). Both of
    /// those facts are monotone/sticky (N8: retention never discards; the chain a digest
    /// names is immutable), so membership here is permanent -- once settled, always
    /// settled. `settle` short-circuits at the top for any ref already in this set,
    /// making repeat/recursive `settle` calls amortized O(1) once the whole prefix below
    /// a tip has been walked once.
    settled: HashSet<BlockRef>,
    /// PHASE6-SPEC.md §9 gate amendment, R1: `authorized \ settled` -- the only refs
    /// `on_block_available` re-attempts on a new block arrival (replacing the previous
    /// re-settle-everything sweep). A ref leaves this set (into `settled`) exactly when
    /// `settle` succeeds for it; it never leaves any other way, so this is always exactly
    /// `authorized \ settled` without needing to recompute the set difference.
    pending_settle: HashSet<BlockRef>,
    /// n=100 straggler fix (2026-08-08): `digest -> refs whose walk stalled on it`.
    /// This is what makes `on_block_available` cost O(refs waiting for THIS digest)
    /// instead of O(`pending_settle`) -- see that method's own doc comment for the
    /// 612M-`settle`-call measurement that motivated it.
    blocked_on: HashMap<Digest, HashSet<BlockRef>>,
    /// The inverse of `blocked_on`, so re-blocking a ref on a deeper digest can drop
    /// its stale bucket entry. Together they maintain "each pending ref sits in at
    /// most one bucket".
    blocked_at: HashMap<BlockRef, Digest>,
    /// Mirrors `vantage_repair_settle_calls_total` so tests can assert on it without a
    /// metrics handle (`metrics` is `None` in most unit tests).
    settle_calls: u64,
    /// N6: `(peer, h)` we have sent `request(h)` to, ever -- at most one, no retries.
    requested: HashSet<(PublicKey, Digest)>,
    /// N6/P1-2: every hash we have ever requested (union of `requested`'s second
    /// component) -- gates `on_serve`, since the paper's serve clause fires only "for a
    /// requested hash". Without this an unsolicited-but-hash-correct serve could inject
    /// unbounded valid blocks of a peer's own lane into the shared cache without us
    /// ever asking, outside §6.3's documented (attacker-cost-proportional) exposure.
    requested_hashes: HashSet<Digest>,
    /// n=100 recovery fix (2026-08-07): per-digest fan-out progress, for digests whose
    /// coverage is not yet complete. Removed on arrival (`on_block_available`) and when
    /// nothing waits on the digest any more (`retry_requests`), so it is bounded by the
    /// genuinely-outstanding set rather than by history.
    fanout: HashMap<Digest, FanoutState>,
    /// Escalation order for `fanout`, keyed by the missing block's HEIGHT so the lowest
    /// escalates first. Not FIFO, and the difference matters: repair is parallel (the
    /// failing n=100 nodes had 6,328-51,851 digests outstanding at once) while output is
    /// strictly serial -- `Cursor::pump` only ever advances `next_view` and breaks on the
    /// first `expand` miss, and `AgbEngine::ensure_fetch` says it outright: "resolution is
    /// strictly sequential, so the lowest pending view is the one actually blocking
    /// progress and the far-ahead ones are useless until it clears". Escalating in arrival
    /// order therefore spends the escalation budget on digests the node provably cannot use
    /// yet, while the one digest that would unblock the cursor waits its turn behind tens
    /// of thousands of them. `ensure_fetch` already acts on this (it evicts the HIGHEST
    /// views); `Repairer` had no notion of priority at all.
    ///
    /// Entries whose digest is no longer in `fanout` are skipped and dropped, so this is
    /// self-cleaning without a separate eviction pass.
    fanout_queue: BTreeSet<(Height, Digest)>,
    /// N7: `(requester, h)` recorded on a direct `request(h)`, even before we hold `h`.
    /// n=100 straggler fix (2026-08-08): indexed BY DIGEST rather than a flat
    /// `HashSet<(PublicKey, Digest)>`. `try_serve` ran a linear scan of the whole set on
    /// every retained block, and nothing removes an entry except being served -- so on a
    /// straggler (measured |pending_req| ~ 10.4k) that scan ran inside
    /// `inbound_dispatch` once per settled frame. Same un-indexed-sweep shape as
    /// `on_block_available`'s, in a third place.
    pending_req: HashMap<Digest, HashSet<PublicKey>>,
    /// N7: `(requester, h)` we have already served -- at most one answer, ever.
    answered: HashSet<(PublicKey, Digest)>,

    /// §6.4 counters; `None` in most unit tests, which don't assert on metrics.
    metrics: Option<Arc<Metrics>>,
}

impl Repairer {
    pub fn new(
        name: PublicKey,
        committee: Committee,
        sid: Digest,
        genesis: Digest,
        max_block_payload: usize,
        blocks: SharedBlocks,
    ) -> Self {
        Self {
            // `name` itself is no longer stored: its only reader was `settle`'s
            // `others_primaries(&self.name)` call, which `peers` now replaces.
            peers: committee
                .others_primaries(&name)
                .into_iter()
                .map(|(pk, _)| pk)
                .collect(),
            committee,
            sid,
            genesis,
            max_block_payload,
            blocks,
            authorized: HashSet::new(),
            settled: HashSet::new(),
            pending_settle: HashSet::new(),
            blocked_on: HashMap::new(),
            blocked_at: HashMap::new(),
            settle_calls: 0,
            requested: HashSet::new(),
            requested_hashes: HashSet::new(),
            fanout: HashMap::new(),
            fanout_queue: BTreeSet::new(),
            pending_req: HashMap::new(),
            answered: HashSet::new(),
            metrics: None,
        }
    }

    /// Attach §6.4 counters (production wiring only -- most unit tests skip this).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// §4 entry point, N6. Idempotent: re-authorizing a tuple already in `authorized`
    /// never re-emits a request (guarded by `requested`) but does re-attempt to settle
    /// it (harmless/idempotent, and O(1) if it's already `settled`) in case cached state
    /// advanced since the last attempt.
    pub fn authorize(&mut self, r: BlockRef) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.note_authorized(r.clone());
        self.settle(r, &mut effects);
        effects
    }

    /// PHASE6-SPEC.md §9 gate amendment, R1(a): record `r` in `authorized`, and in
    /// `pending_settle` too unless it's already `settled` (permanent membership there
    /// means it can never regress back to pending).
    fn note_authorized(&mut self, r: BlockRef) {
        self.authorized.insert(r.clone());
        if !self.settled.contains(&r) {
            self.pending_settle.insert(r);
        }
    }

    /// `settle` succeeded for `r`: move it from `pending_settle` into the permanent
    /// `settled` set.
    fn mark_settled(&mut self, r: BlockRef) {
        self.settled.insert(r.clone());
        self.pending_settle.remove(&r);
    }

    /// Whenever a cached block matches an authorized exact coordinate (arriving via
    /// publish *or* serve), the walk advances -- called by the caller after any block
    /// becomes cached (`Effect::BlockCached`, from either `LaneManager` or `on_serve`
    /// below).
    ///
    /// PHASE6-SPEC.md §9 gate amendment, R1(c): iterates only `pending_settle` (=
    /// `authorized \ settled`), not the whole `authorized` set. This still gets
    /// retention propagation right: when ancestors arrive one at a time, the newly-
    /// arrived ancestor's OWN `settle` call (a `pending_settle` member, since nothing
    /// settles before its own walk succeeds) completes and is memoized into `settled`;
    /// every *descendant* still in `pending_settle` (blocked earlier, waiting on exactly
    /// this ancestor) then short-circuits into that memoized `true` at the top of
    /// `settle`.
    ///
    /// n=100 straggler fix (2026-08-08), THE dominant cost: this used to ignore
    /// `digest` and re-walk ALL of `pending_settle` on every newly cached block. A
    /// straggler holds thousands of unsettled refs (measured `pending_settle_len` =
    /// 4,967, and nothing GCs it), so the sweep was O(P) per arrival and produced
    /// **612,424,724 `settle` calls** against 60,262 received blocks -- a ratio of
    /// 10,163, i.e. ~2xP, the 2x being the double sweep this commit also removes. That
    /// alone was 80.3s of `effect_execution` versus 2.57s on a healthy node, which
    /// saturated the single core and pinned the 1000-slot inbound queue.
    ///
    /// Now indexed: a ref that blocks records the digest it blocked ON (`blocked_on`),
    /// and an arrival wakes only the refs actually waiting for it. A digest nobody
    /// waits on costs one hash lookup. The correctness argument is the same one the
    /// paragraph above already makes -- a ref that fails to settle stays pending and is
    /// retried when the digest it NOW blocks on arrives -- with the bucket standing in
    /// for the full set. `blocked_on` is taken by value so a re-blocked ref reinserts
    /// itself under its new digest, which keeps the index self-cleaning with no
    /// separate eviction pass.
    pub fn on_block_available(&mut self, digest: Digest) -> Vec<Effect> {
        let mut effects = Vec::new();
        // The digest is in hand, so no further fan-out round is ever needed for it. The
        // matching `fanout_queue` entry is left to be skipped-and-dropped by
        // `retry_requests` rather than searched for here -- this runs once per cached
        // block, and one hash removal is the whole budget it deserves.
        self.fanout.remove(&digest);
        let Some(waiting) = self.blocked_on.remove(&digest) else {
            return effects;
        };
        for r in waiting {
            // Skip refs that have since settled or been GC'd out of `pending_settle`;
            // `settle` would short-circuit anyway, but this avoids the call entirely.
            if self.pending_settle.contains(&r) {
                self.blocked_at.remove(&r);
                self.settle(r, &mut effects);
            } else {
                self.blocked_at.remove(&r);
            }
        }
        effects
    }

    /// Record that every ref in `refs` (the walk's origin plus every ancestor it walked
    /// past) is blocked on `h`. Moving a ref between buckets drops its stale entry
    /// first, so a ref is in at most one bucket at a time -- the invariant that keeps
    /// `blocked_on` from accumulating duplicates across re-blocks.
    fn record_blocked(&mut self, refs: &[BlockRef], h: &Digest) {
        for r in refs {
            if let Some(prev) = self.blocked_at.get(r) {
                if prev == h {
                    continue;
                }
                if let Some(bucket) = self.blocked_on.get_mut(prev) {
                    bucket.remove(r);
                    if bucket.is_empty() {
                        self.blocked_on.remove(prev);
                    }
                }
            }
            self.blocked_at.insert(r.clone(), h.clone());
            self.blocked_on
                .entry(h.clone())
                .or_default()
                .insert(r.clone());
        }
    }

    /// Emit the next batch of `request(h)` for a digest we are missing.
    ///
    /// n=100 recovery fix (2026-08-07). This used to ask ALL n-1 peers the first time a
    /// digest was missed. On the failing n=100 run every stalled node's
    /// `vantage_repairs_requested` was an exact multiple of 99 -- node 72 sent 5,133,249
    /// = 51,851 distinct digests x 99 peers. The ANSWERS are what killed it: 99 copies of
    /// each body arrived, overflowed the bounded bulk inbound queue
    /// (`vantage_bulk_inbound_dropped_total` 663,546 versus 186 healthy), and the copy the
    /// node actually needed was among the drops -- so the digest stayed missing and the
    /// backlog grew. Stalled nodes received MORE wire messages than healthy ones (2.57M
    /// vs 2.08M) while committing nothing: repair was manufacturing the congestion that
    /// stopped the repair landing.
    ///
    /// Coverage is staged instead -- `FANOUT_FIRST` peers now, doubling per tick until all
    /// n-1 are asked. That leaves N6's guarantee intact because the guarantee is about
    /// EVENTUAL coverage, and it does have to be FULL coverage rather than a quorum: the
    /// holder set is only guaranteed f+1 *stake*, so in the worst case exactly one of its
    /// members is correct and which one is unknown. Any bounded subset can miss it;
    /// asking everyone eventually cannot.
    fn fan_out(&mut self, h: &Digest, effects: &mut Vec<Effect>) {
        let n = self.peers.len();
        if n == 0 {
            return;
        }
        let entry = self.fanout.entry(h.clone()).or_insert_with(|| FanoutState {
            start: Self::fanout_start(h, n),
            asked: 0,
            next_width: FANOUT_FIRST,
        });
        if entry.asked >= n {
            return;
        }
        let (start, from) = (entry.start, entry.asked);
        let take = entry.next_width.min(n - from);
        entry.asked = from + take;
        entry.next_width = entry.next_width.saturating_mul(2);
        for k in 0..take {
            let peer = self.peers[(start + from + k) % n];
            if self.requested.insert((peer, h.clone())) {
                effects.push(Effect::RequestTo(peer, h.clone()));
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repairs_requested.inc();
                }
            }
        }
    }

    /// Where in `peers` a digest's fan-out starts. Spreading by digest matters at n=100:
    /// with a fixed start, every node's first `FANOUT_FIRST` requests land on the same few
    /// peers, concentrating the committee's whole repair-serve load onto them while the
    /// rest serve nothing.
    fn fanout_start(h: &Digest, n: usize) -> usize {
        // A digest is already a hash, so any 8 bytes of it are uniform -- no re-hashing.
        let mut acc = [0u8; 8];
        acc.copy_from_slice(&h.0[..8]);
        (u64::from_le_bytes(acc) % n as u64) as usize
    }

    /// Widen the fan-out for digests still outstanding. Driven by the node's existing 1s
    /// tick (`node.rs`'s `metrics_tick`, next to `AgbEngine::retry_fetches`), budgeted to
    /// `FANOUT_ESCALATE_BUDGET` digests per call so the escalation pass cannot itself
    /// become the unbounded per-tick sweep this whole class of bug is made of, and FIFO so
    /// the longest-outstanding digest escalates first.
    pub fn retry_requests(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        let budget = FANOUT_ESCALATE_BUDGET.min(self.fanout_queue.len());
        // Escalated entries are re-inserted, so draining in place would re-visit them
        // within the same call (they sort at or below the cursor). Collect the batch first.
        let batch: Vec<(Height, Digest)> = self.fanout_queue.iter().take(budget).cloned().collect();
        for key in batch {
            self.fanout_queue.remove(&key);
            let (_, h) = &key;
            // Arrived already: `on_block_available` dropped the `fanout` entry.
            if !self.fanout.contains_key(h) {
                continue;
            }
            // Nothing waits on it any more (whatever did has settled, or was dropped), so
            // stop spending requests on it.
            if !self.blocked_on.contains_key(h) {
                self.fanout.remove(h);
                continue;
            }
            self.fan_out(&key.1.clone(), &mut effects);
            if let Some(metrics) = &self.metrics {
                metrics.vantage_repair_fanout_escalations_total.inc();
            }
            if self
                .fanout
                .get(&key.1)
                .is_some_and(|s| s.asked < self.peers.len())
            {
                self.fanout_queue.insert(key);
            } else {
                // Fully covered: N6's fan-out obligation for `h` is discharged, so the
                // per-digest state is dead weight from here on. Dropping it is safe --
                // `requested_hashes` (never pruned) is what gates `on_serve`, and
                // `requested` (never pruned) is what prevents a re-ask.
                self.fanout.remove(&key.1);
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_repair_fanout_pending
                .set(self.fanout.len() as i64);
        }
        effects
    }

    /// On `serve(h, b)` **for a requested `h`** (N6 -- this gate is normative, not
    /// incidental: an unsolicited-but-hash-correct serve is ignored, same as a
    /// corrupted one, so a peer cannot bulk-inject unbounded valid blocks of its own
    /// lane into our cache without us ever asking): if `h` was requested and `b` is
    /// well-formed (`hash(b) == h` is implicit -- we compute `h` from `b` ourselves
    /// below -- `BlockOK`, size caps), cache it as repaired data *without* a provenance
    /// mark, coordinate-independent (cached even if `b`'s encoded author/height differ
    /// from whatever coordinate we were hoping this digest would satisfy -- only an
    /// exact coordinate match advances a walk). A failed check leaves no trace: the
    /// hash stays un-obtained.
    pub fn on_serve(&mut self, block: Header) -> Vec<Effect> {
        if !self.requested_hashes.contains(&block.id) {
            return Vec::new();
        }
        // NOTE (2026-08-08): a duplicate serve of an already-cached-and-verified digest
        // must NOT be short-circuited here, even though its repair work is redundant.
        // `VantageCore::serve_effects` gates the PAYLOAD re-probe on this function
        // emitting `Effect::BlockCached` (node.rs:2282-2290), and the payload state can
        // legitimately have advanced between two serves of the same header -- the
        // second probe asks only for what is still missing and merges into the existing
        // pending set. Pinned by `duplicate_accepted_serve_merges_pending_payload_
        // without_duplicate_waiters`. The duplicate-serve amplification this creates
        // (~n-1 identical serves per requested digest, each driving a `pending_settle`
        // sweep) is real and is addressed where it belongs -- by making the sweep
        // digest-indexed rather than O(pending_settle) -- not by dropping serves.
        if !block_ok(&block, &self.committee, &self.sid, self.max_block_payload) {
            return Vec::new();
        }
        let digest = block.id.clone();
        {
            let mut blocks = self.blocks.lock();
            // `block_ok` just passed above for this exact block -- memoize it.
            blocks.upsert(block, false, true, false, true);
        }
        // `payload_ok` is left false here -- serving/retention are chain-hash
        // authenticity concerns (D1's clause (ii)), not the payload-possession clause
        // (D1 (iii)), which only ever gates *acking*, never repair/retention. A
        // caller that also wants the batch bytes synced can still do so separately;
        // Phase 3's repair walk itself only ever needs the chain of headers.
        // This inline sweep is deliberately KEPT, though it duplicates the one
        // `VantageCore::execute`'s `Effect::BlockCached` arm performs (the failing
        // n=100 run's `settle_calls / blocks_received` = 10,163 against
        // `pending_settle_len` = 4,967 is exactly that 2x). Removing it was tried and
        // reverted: now that `on_block_available` is digest-indexed, the duplicate
        // costs 2 x O(refs waiting on THIS digest) rather than 2 x O(pending_settle),
        // so the 2x is negligible -- while dropping it changes `Repairer`'s contract
        // (`on_serve` would no longer settle anything on its own, only signal that the
        // caller should), which every standalone use of this type would have to know.
        // Not worth a contract change for a constant factor on an operation that is no
        // longer hot.
        let mut effects = vec![Effect::BlockCached(digest.clone())];
        effects.extend(self.on_block_available(digest));
        effects
    }

    /// On a direct `request(h)` from `p_j` (N7): record `(j, h)` in `pendingReq` if not
    /// already answered -- even when the block is not held yet -- then try-serve.
    pub fn on_request(&mut self, requester: PublicKey, h: Digest) -> Vec<Effect> {
        if !self.answered.contains(&(requester, h.clone())) {
            self.pending_req
                .entry(h.clone())
                .or_default()
                .insert(requester);
        }
        let mut effects = Vec::new();
        self.try_serve(&h, &mut effects);
        effects
    }

    /// The Authorize walk (§3.3 N6): match a cached block at the exact coordinate,
    /// walk into the parent, or fan out `request(h)`. Returns whether this reference's
    /// prefix is verified through genesis *right now* (used only to decide whether to
    /// retain+try-serve at this level; the return value itself is not otherwise
    /// consumed by the top-level caller, though it IS consumed by the direct caller
    /// below to decide whether to retain/settle).
    ///
    /// PHASE6-SPEC.md §9 gate amendment, R1(b): short-circuits at the top for any `r`
    /// already in `settled` -- the walk then stops at the first settled ancestor
    /// instead of re-walking to genesis, making repeat calls (whether from a fresh
    /// `authorize` on an overlapping lane, or from `on_block_available`'s loop) amortized
    /// O(1) per already-settled tip. Only a genuinely new, not-yet-settled tail of the
    /// chain is ever walked.
    ///
    /// Fable perf audit: rewritten from recursion (depth = length of the contiguous
    /// cached-but-unsettled suffix -- an adversarial deep chain, cached all at once and
    /// then authorized only at the tip, could overflow the stack; the same class
    /// `control::mark_safe` and the height-bounded lane walks in `lanes.rs` were already
    /// made iterative to avoid) into an explicit descend-then-ascend loop that produces
    /// the IDENTICAL result. The original recursion walks parent-ward on the way down
    /// and settles/serves on the way back up (post-order: deepest ancestor first, `r`
    /// itself last); this rewrite reproduces that exactly in two phases instead of via
    /// the call stack:
    ///   - Descend: walk from `r` toward genesis, exactly mirroring each recursive
    ///     frame's own logic in the same order -- the `settled` short-circuit, the
    ///     height-0 base case, the cached-block lookup (now consulting
    ///     `BlockEntry::block_ok_verified` instead of recomputing `block_ok`, per the
    ///     memoization above), the missing-block request fan-out, and the height==1
    ///     genesis-link check -- pushing each ref that has a verified parent step ahead
    ///     of it onto `frames` (and calling `note_authorized` on that parent) in the same
    ///     place the recursive call used to recurse, and terminating with the same
    ///     `verified` value the recursion would have returned from that point (`true` at
    ///     an already-settled ancestor, genesis, or a matching height-1 genesis link;
    ///     `false` at a missing block or a broken genesis link).
    ///   - Ascend: pop `frames` in LIFO order (deepest-pushed first -- the same order
    ///     the recursive unwind visits frames) and, only if `verified`, call
    ///     `retain_and_serve`/`mark_settled` for each, in that order -- identical to the
    ///     recursion, which does exactly this each time it unwinds through a level with
    ///     `parent_verified == true`, and does neither once `parent_verified` is
    ///     `false` (propagated unchanged through every remaining level).
    ///
    /// Net effect: identical `settled`/`requested_hashes`/`requested` mutations,
    /// identical `Effect`s in identical order, identical return value -- just O(1) stack
    /// depth instead of O(chain length).
    fn settle(&mut self, r: BlockRef, effects: &mut Vec<Effect>) -> bool {
        self.settle_calls += 1;
        if let Some(metrics) = &self.metrics {
            metrics.vantage_repair_settle_calls_total.inc();
        }
        let mut cur = r;
        let mut frames: Vec<BlockRef> = Vec::new();
        let verified = loop {
            if self.settled.contains(&cur) {
                break true;
            }
            let (author, height, h) = cur.clone();
            if height == 0 {
                break true; // implicit genesis base case; trivial, not memoized
            }

            let cached = {
                let blocks = self.blocks.lock();
                blocks.get(&h).and_then(|entry| {
                    let b = &entry.block;
                    if b.author == author
                        && b.height == height
                        && b.id == h
                        && entry.block_ok_verified
                    {
                        Some(b.clone())
                    } else {
                        None
                    }
                })
            };

            let Some(block) = cached else {
                // n=100 straggler fix (2026-08-08): the fan-out loop is GATED on
                // `requested_hashes` rather than run unconditionally. Equivalence is
                // exact, not approximate: `requested_hashes` is inserted only here,
                // `requested` only just below, neither is ever removed from, and
                // `committee` is immutable -- so `requested_hashes.contains(h)` holds
                // iff `requested` already contains `(p, h)` for EVERY p in
                // `others_primaries`, i.e. iff the loop below would emit nothing. Same
                // effects, same order, same state.
                //
                // What it removes: `on_block_available` re-walks all of
                // `pending_settle` on EVERY block arrival (and again from `on_serve`),
                // so a node missing k blocks re-ran this loop once per miss per
                // arrival while emitting nothing at all. Measured on the failing
                // n=100 run: ~1,920 missing blocks x ~13k arrivals ~= 25M misses x 99
                // peers ~= 2.5B iterations, each paying a `Digest` clone, a SipHash of
                // a 64-byte key, a probe into a 190k-entry set, and -- via
                // `others_primaries` -- a fresh ~12.7 KB `Vec` allocation. That
                // accounted for ~100s of the 103.85s `effect_execution` (healthy:
                // 2.99s), which saturated the single core, pinned the 1000-slot
                // inbound queue, and cut organic block intake to ~10%. The 190,080
                // messages themselves cost ~0.4s -- the loop, not the traffic, was the
                // bottleneck.
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repair_fanout_loops_total.inc();
                }
                // n=100 recovery fix (2026-08-07): the FIRST round only. `requested_
                // hashes` still gates entry, so a repeated miss on the same digest stays
                // O(1) here (the property the 2026-08-08 gate introduced and which
                // `on_block_available`'s walks depend on); widening coverage is
                // `retry_requests`' job, off the node's 1s tick.
                if self.requested_hashes.insert(h.clone()) {
                    self.fanout_queue.insert((height, h.clone()));
                    self.fan_out(&h, effects);
                }
                // Index the blockage so `on_block_available(h)` wakes exactly these
                // refs. `frames` holds every descendant this walk passed through on the
                // way down and `cur` is the missing block itself -- all of them are
                // waiting on `h`, and all of them are in `pending_settle` (`cur` via the
                // `note_authorized` on the previous iteration, the origin via the
                // caller's own `authorize`). Recording the whole stalled sub-chain, not
                // just `cur`, is what preserves the old sweep's behaviour: when `h`
                // lands, whichever of them is tried first walks the now-complete chain
                // and settles all of it, and the rest short-circuit on `settled`.
                let mut waiting = frames.clone();
                waiting.push(cur.clone());
                self.record_blocked(&waiting, &h);
                break false;
            };

            let parent_h = block.parent_cert.header_digest.clone();
            if height == 1 {
                if parent_h != self.genesis {
                    // Never verifies (a malformed/forged genesis link) -- documented
                    // residual: the walk simply never completes for this coordinate.
                    break false;
                }
                frames.push(cur);
                break true;
            }

            let parent_ref = (author, height - 1, parent_h);
            self.note_authorized(parent_ref.clone());
            frames.push(cur);
            cur = parent_ref;
        };

        if verified {
            while let Some(frame) = frames.pop() {
                self.retain_and_serve(&frame, effects);
                self.mark_settled(frame);
            }
        }
        verified
    }

    fn retain_and_serve(&mut self, r: &BlockRef, effects: &mut Vec<Effect>) {
        let h = r.2.clone();
        {
            let mut blocks = self.blocks.lock();
            blocks.mark_retained(&h);
        }
        self.try_serve(&h, effects);
    }

    /// Try-serve (N7): if a retained block matches `h`, answer each pending
    /// `(j, h)` not yet answered with `serve(h, b)`, marking `(j, h)` answered.
    fn try_serve(&mut self, h: &Digest, effects: &mut Vec<Effect>) {
        let block = {
            let blocks = self.blocks.lock();
            match blocks.get(h) {
                Some(entry) if entry.retained => Some(entry.block.clone()),
                _ => None,
            }
        };
        let Some(block) = block else {
            return;
        };
        let pending: Vec<PublicKey> = self
            .pending_req
            .get(h)
            .map(|peers| {
                peers
                    .iter()
                    .copied()
                    .filter(|peer| !self.answered.contains(&(*peer, h.clone())))
                    .collect()
            })
            .unwrap_or_default();
        for peer in pending {
            self.answered.insert((peer, h.clone()));
            // Paper: "remove it from pendingReq" -- `answered` already makes the
            // (peer, h) pair permanently inert, but do the removal too so the set
            // doesn't grow forever.
            if let Some(peers) = self.pending_req.get_mut(h) {
                peers.remove(&peer);
                if peers.is_empty() {
                    self.pending_req.remove(h);
                }
            }
            effects.push(Effect::ServeTo(peer, block.clone()));
            if let Some(metrics) = &self.metrics {
                metrics.vantage_repairs_served.inc();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn requested_count(&self) -> usize {
        self.requested.len()
    }

    /// Current `pending_settle` size -- exported as `vantage_pending_settle_len` by
    /// `VantageCore::sample_metrics`. See that gauge's doc comment for why this is the
    /// number to watch: it is the `P` in `on_block_available`'s O(P) per-block sweep,
    /// and nothing GCs it.
    pub fn pending_settle_len(&self) -> usize {
        self.pending_settle.len()
    }

    /// Test accessors for the two sets whose relationship makes `settle`'s fan-out gate
    /// sound -- see `requested_hashes_set_implies_coverage_complete_or_escalating`.
    #[cfg(test)]
    pub(crate) fn was_requested_hash(&self, h: &Digest) -> bool {
        self.requested_hashes.contains(h)
    }

    /// How many peers have been asked for `h` so far, or `None` once coverage is complete
    /// and the per-digest state has been dropped. Together with `is_escalating_for_test`
    /// this pins the property the fan-out gate now rests on: a gated digest is either
    /// fully covered or still scheduled to widen.
    #[cfg(test)]
    pub(crate) fn fanout_asked_for_test(&self, h: &Digest) -> Option<usize> {
        self.fanout.get(h).map(|s| s.asked)
    }

    /// Whether `h` is still queued for a further fan-out round.
    #[cfg(test)]
    pub(crate) fn is_escalating_for_test(&self, h: &Digest) -> bool {
        self.fanout_queue.iter().any(|(_, d)| d == h)
    }

    /// How many refs are waiting on `h` -- lets tests assert that a re-blocked ref
    /// LEAVES its previous bucket, the invariant that keeps `blocked_on` from growing
    /// into the very set the digest index exists to stop scanning.
    #[cfg(test)]
    pub(crate) fn blocked_on_len_for_test(&self, h: &Digest) -> usize {
        self.blocked_on.get(h).map_or(0, |s| s.len())
    }

    /// Total `settle` calls, mirroring `vantage_repair_settle_calls_total` for tests
    /// that assert an arrival did NO settling at all (the metrics handle is `None` in
    /// most unit tests, so the counter itself is unreadable there).
    #[cfg(test)]
    pub(crate) fn settle_calls_for_test(&self) -> u64 {
        self.settle_calls
    }

    #[cfg(test)]
    pub(crate) fn was_requested(&self, peer: &PublicKey, h: &Digest) -> bool {
        self.requested.contains(&(*peer, h.clone()))
    }

    #[cfg(test)]
    pub(crate) fn is_settled(&self, r: &BlockRef) -> bool {
        self.settled.contains(r)
    }

    #[cfg(test)]
    pub(crate) fn is_pending_settle(&self, r: &BlockRef) -> bool {
        self.pending_settle.contains(r)
    }

    #[cfg(test)]
    pub(crate) fn blocks_for_test(
        &self,
    ) -> parking_lot::MutexGuard<'_, crate::vantage::lanes::BlockCache> {
        self.blocks.lock()
    }
}
