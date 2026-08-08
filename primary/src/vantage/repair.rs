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

/// Starting per-tick ceiling on `request(h)` emission, across BOTH first rounds and
/// escalations. Adapts at runtime -- see `adapt_recovery_ceiling`.
///
/// The per-mechanism caps that already existed (fan-out width, escalation count, body-fetch
/// cap of 1024, 8 concurrent resume episodes) each bound their own mechanism and nothing
/// bounds their SUM -- so a node in deep recovery runs all of them at once, which is
/// precisely the load that saturated the core. This bounds the dominant term: measured on
/// the 2026-08-07 runs, repair requests outweighed lane-resume requests by more than 100x
/// (223,122 versus 1,328 on a degraded node, and 4.1M versus 534 before the fan-out was
/// bounded), so bounding repair is what bounding "recovery" mostly means.
pub(crate) const RECOVERY_EMIT_START: usize = 2_048;

/// Floor for the adaptive ceiling: the rate a node keeps even while it is visibly drowning,
/// so congestion control can never starve recovery to a stop.
pub(crate) const RECOVERY_EMIT_MIN: usize = 256;

/// Cap for the adaptive ceiling. Still ~30x below the ~4.7M requests a single stalled node
/// emitted before the fan-out was bounded, so this is a bound, not a licence.
pub(crate) const RECOVERY_EMIT_MAX: usize = 65_536;

/// Core inbound-queue depth above which the node counts as congested, for the ESCALATION
/// WIDTH cap only (`fan_out`). The queue is 1000 slots; measured on the 2026-08-07 n=100 run,
/// degraded nodes sat pinned at exactly 1000 while healthy nodes sat at 9-17, so anything
/// near the cap is unambiguous.
///
/// This use is correctly attributed and stays at "merely busy": widening ONE digest to ~49
/// peers invites ~49 copies of the same block onto the main queue, and those duplicates are
/// entirely repair's own doing. Backing off narrows a self-inflicted amplification.
pub(crate) const CORE_QUEUE_CONGESTED: usize = 256;

/// Core inbound-queue depth at which `adapt_recovery_ceiling` backs off -- deliberately just
/// under the 1000-slot cap, NOT at "merely busy" like `CORE_QUEUE_CONGESTED` above.
///
/// The emit ceiling read `CORE_QUEUE_CONGESTED` for one run and that was a control-loop
/// MISATTRIBUTION, measured on the 2026-08-07 n=100 run at `adc6048`. All four degraded nodes
/// sat at `emit_ceiling` = `RECOVERY_EMIT_MIN` deferring 38-61% of their repair demand
/// (deferred 282-299/s against requested 192-245/s) while `bulk_inbound_dropped_total` was
/// ZERO on all 100 nodes -- nothing was overflowing anywhere. The main queue was indeed deep,
/// but from availability CREDITING (128,942 refs/s, ~= n x (2f+1) x block_rate), which repair
/// cannot influence; repair itself was 193 requests/s, ~0.15% of the node's load. So the loop
/// throttled the one thing a lagging node needs, in response to load that would not decrease
/// however far it backed off: cutting repair 60% freed nothing and left a 5,000-block gap
/// unable to close. Same failure as the fixed 2048/tick budget this AIMD replaced, at 256 --
/// 8x worse than the bug it was written to fix.
///
/// Why the two thresholds differ: the width cap throttles DUPLICATE answers for one digest
/// (self-inflicted, worth suppressing early); the emit ceiling throttles DISTINCT blocks the
/// node is missing (each needed exactly once, and suppressing them prevents recovery). At
/// ~900 the queue is genuinely about to shed CONSENSUS traffic, which is worth backing off
/// for whatever the cause. Below that, absorption is bounded by `RECOVERY_IN_FLIGHT_MAX`,
/// which is closed-loop -- a slot frees only when an answer lands or is retired -- and so
/// self-limits on the real answer rate instead of guessing at it.
pub(crate) const CORE_QUEUE_NEAR_OVERFLOW: usize = 900;

/// Maximum requests outstanding (emitted, digest not yet in hand) at any moment.
///
/// A RATE limit alone does not bound inbound: the 2026-08-07 run had 3,420 outstanding
/// digests asked of ~49 peers each = ~167,000 invited answers, every one of which costs a
/// `block_ok` verify, a cache insert and a settle walk on the single-threaded core. That is
/// what pinned `core_queue_length` at its 1000-slot cap while `bulk_inbound_dropped` stayed
/// at 0 -- the flood was on the main queue, and it was self-invited. This is the window that
/// bounds it, in the same sense as a TCP congestion window: answers cannot exceed what was
/// asked for, so capping outstanding asks caps inbound.
pub(crate) const RECOVERY_IN_FLIGHT_MAX: usize = 512;

/// Ticks (1s each -- `retry_requests` is driven by the node's own 1s tick) after which an
/// unanswered round's in-flight slots are RECLAIMED and the digest is escalated to fresh
/// peers.
///
/// Without this the window is an absorbing state. A slot is freed only when the digest
/// arrives or the digest retires, and retirement requires emission progress, which requires
/// `room > 0` -- so `RECOVERY_IN_FLIGHT_MAX` asks that will never be answered halt repair on
/// that node permanently. That is reachable WITHOUT an adversary: `Effect::RequestTo` is sent
/// as `HeadersRequest`, which `Inbound::is_bulk` routes to the bulk queue and `try_send`
/// DROPS silently when full (node.rs) -- and N6 forbids ever re-asking that peer, so each
/// drop permanently burns a `(peer, digest)` pair. A window without a retransmission timer
/// latches on loss, and loss here is by design.
///
/// Reclaiming a slot is NOT a retransmission: `requested`/`requested_hashes` are untouched,
/// so N6's "at most one request per (peer, digest) ever" still holds exactly. Only the
/// window accounting is corrected, which lets escalation reach peers it has not yet asked.
pub(crate) const ASK_TIMEOUT_TICKS: u64 = 4;

/// Peers a single digest may be asked of before escalation stops widening. Escalating to
/// near-full coverage (the measured ~49) invites 49 copies of one block; if the first
/// `ESCALATE_WIDTH_MAX` peers -- chosen holder-first -- did not answer, the bottleneck is
/// almost certainly local intake, and asking more peers makes it worse. N6's eventual
/// guarantee is preserved because the cap lifts as soon as the node is no longer congested.
pub(crate) const ESCALATE_WIDTH_MAX: usize = 8;

/// Per-digest fan-out progress. See `fan_out` for why coverage is staged.
struct FanoutState {
    /// The missing block's coordinate, kept so each round can re-consult `holders` for
    /// peers known to hold this author's lane at this height.
    author: PublicKey,
    height: Height,
    /// Where in `peers` this digest's fill order begins, derived from the digest itself.
    /// Used only for peers we have NO holder information about: without it every node
    /// would fall back to `peers[0..k]` for every digest, so peer 0 absorbs the whole
    /// committee's repair load while peer n-1 serves none. Digest-derived rather than
    /// random, so it stays deterministic and reproducible across runs.
    start: usize,
    /// How many distinct peers have been asked for this digest so far.
    asked: usize,
    /// Size of the NEXT batch. Doubling reaches all n-1 peers in O(log n) ticks, so the
    /// worst-case time-to-full-coverage stays small while the common case stays cheap.
    next_width: usize,
    /// How many of this digest's asks are CURRENTLY counted in `Repairer::in_flight`.
    /// Distinct from `asked`, which is monotone coverage bookkeeping and must never shrink
    /// (N6). The two diverge the moment `ASK_TIMEOUT_TICKS` reclaims a round's slots: the
    /// window forgets those asks, coverage does not. Keeping them separate is what makes
    /// the release exact -- releasing `asked` after a reclaim would double-release and
    /// silently WIDEN the window instead of holding it.
    in_flight_asks: usize,
    /// `Repairer::ticks` when this digest last had a round go out, for the timeout.
    asked_at: u64,
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
    /// `author -> peer -> highest height that peer has confirmed holding of author's lane`.
    ///
    /// Fed by `note_holder` from the same availability credits `LaneManager` already
    /// consumes, so it costs no new messages -- it is information the node receives and
    /// then threw away. `fan_out` uses it to pick the FIRST round of peers, which is what
    /// decides whether a bounded fan-out actually finds the block.
    ///
    /// Motivation, measured on the 2026-08-07 n=100 run AFTER bounding the fan-out: the
    /// still-degraded nodes reported `vantage_repair_fanout_escalations_total` 6,101 against
    /// `vantage_repair_fanout_pending` 6,109 -- essentially EVERY outstanding digest needed
    /// a round beyond the first four peers. The width was not the problem; the choice was.
    /// Peers were picked by hashing the digest, which spreads serve load perfectly and
    /// ignores the only thing that matters: whether the peer has the data.
    ///
    /// Bounded O(n^2) -- 100 x 100 x 40 B = ~400 KB at n=100 -- and monotone per entry, so
    /// it needs no GC.
    holders: HashMap<PublicKey, HashMap<PublicKey, Height>>,
    /// Requests still permitted this tick, refilled to `emit_ceiling` by `retry_requests`.
    /// Checked BEFORE `requested`/`requested_hashes` are written, never after: those two are
    /// permanent one-shot records, so denying a request after recording it would mean the
    /// peer is never asked again -- a liveness bug wearing a congestion control's clothing.
    /// A denied digest keeps its `fanout` state and its queue entry, so the next tick simply
    /// picks it up.
    emit_budget: usize,
    /// The adaptive per-tick ceiling. See `adapt_recovery_ceiling` for why this is not a
    /// constant: a fixed 2048 throttled the one node that most needed to recover, while that
    /// node was reporting ZERO congestion.
    emit_ceiling: usize,
    /// `vantage_bulk_inbound_dropped_total` as of the previous tick, so the ceiling can react
    /// to the DELTA. The absolute counter is useless here -- it never decreases, so a node
    /// that dropped messages once would be throttled for the rest of the run.
    last_bulk_drops: u64,
    /// Core inbound-queue depth as of the last tick, supplied by the caller. This -- not
    /// bulk drops -- is the load-bearing congestion signal: on the 2026-08-07 run degraded
    /// nodes were pinned at the 1000-slot cap while `bulk_inbound_dropped` read 0, so an
    /// AIMD driven by drops alone would have doubled its ceiling while the node drowned.
    core_queue_len: usize,
    /// Monotone count of `retry_requests` calls -- the node's own 1s tick, used as the clock
    /// for `ASK_TIMEOUT_TICKS`. A tick count rather than an `Instant` deliberately: it keeps
    /// `Repairer` free of wall-clock state, so every timeout test is deterministic and needs
    /// no sleeping.
    ticks: u64,
    /// Requests emitted whose digest has not yet arrived. Bounded by
    /// `RECOVERY_IN_FLIGHT_MAX`; see that constant for why a rate limit alone is not enough.
    in_flight: usize,
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
            holders: HashMap::new(),
            emit_budget: RECOVERY_EMIT_START,
            emit_ceiling: RECOVERY_EMIT_START,
            last_bulk_drops: 0,
            core_queue_len: 0,
            ticks: 0,
            in_flight: 0,
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
        //
        // Releasing `in_flight` here is what keeps the window from latching shut: every
        // request counted into it is released either by the digest arriving (here) or by the
        // digest being retired in `retry_requests`. Miss either path and the node stops
        // asking for anything, forever.
        if let Some(state) = self.fanout.remove(&digest) {
            self.in_flight = self.in_flight.saturating_sub(state.in_flight_asks);
        }
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
    ///
    /// TARGET CHOICE (2026-08-07, second iteration): each round prefers peers KNOWN to hold
    /// this author's lane at this height (`holders`, fed by the availability credits the node
    /// already receives), highest confirmed height first, and only falls back to the
    /// digest-spread rotation for peers we know nothing about. Bounding the width without
    /// this fixed the flood but not the recovery: the still-degraded n=100 nodes showed
    /// `escalations` 6,101 against `fanout_pending` 6,109, i.e. essentially every digest
    /// needed a round beyond the first four. Four peers is enough -- four peers chosen by
    /// hashing the digest is not.
    fn fan_out(&mut self, h: &Digest, effects: &mut Vec<Effect>) {
        let n = self.peers.len();
        if n == 0 {
            return;
        }
        // Read everything the round needs, then drop the borrow: the target choice below
        // consults `holders`/`requested`, which are other fields of `self`.
        let (author, height, start, asked, take) = {
            let Some(entry) = self.fanout.get_mut(h) else {
                return; // never created without a coordinate; see `begin_fanout`
            };
            if entry.asked >= n {
                return;
            }
            // Cap how wide one digest's coverage may get. Escalating to near-full coverage
            // (~49 peers measured) invites 49 copies of one block into a queue that is
            // already the bottleneck. The cap lifts once the node is uncongested, so N6's
            // eventual coverage is delayed, never abandoned.
            let width_cap = if self.core_queue_len >= CORE_QUEUE_CONGESTED {
                ESCALATE_WIDTH_MAX.min(n)
            } else {
                n
            };
            if entry.asked >= width_cap {
                return;
            }
            let take = entry.next_width.min(width_cap - entry.asked);
            (entry.author, entry.height, entry.start, entry.asked, take)
        };

        // Peers that have confirmed this exact lane height, best-informed first, skipping
        // any already asked (N6: at most one request per (peer, digest), ever).
        let mut targets: Vec<PublicKey> = Vec::with_capacity(take);
        for peer in self.likely_holders(&author, height, n, h) {
            if targets.len() >= take {
                break;
            }
            if !self.requested.contains(&(peer, h.clone())) {
                targets.push(peer);
            }
        }
        // Fill the rest from the digest-spread rotation. Scans at most n peers, so a round
        // stays O(n) even when most of the committee has already been asked.
        let mut scanned = 0;
        while targets.len() < take && scanned < n {
            let peer = self.peers[(start + asked + scanned) % n];
            if !self.requested.contains(&(peer, h.clone())) && !targets.contains(&peer) {
                targets.push(peer);
            }
            scanned += 1;
        }

        // Truncate to what the tick's recovery budget AND the in-flight window still allow.
        // Done here, before any `requested`/`requested_hashes` write, for the reason those
        // fields' comments give. The window is what actually bounds INBOUND: an answer can
        // only arrive for something we asked for, so capping outstanding asks caps arrivals.
        let now_tick = self.ticks;
        let room = RECOVERY_IN_FLIGHT_MAX.saturating_sub(self.in_flight);
        let wanted = targets.len();
        targets.truncate(self.emit_budget.min(room));
        let emitted = targets.len();
        if emitted == 0 && (self.emit_budget == 0 || room == 0) {
            if let Some(metrics) = &self.metrics {
                // Count the REQUESTS deferred, not the deferral events. Per-event counting
                // made the metric unreadable: "deferred 2/s" against "requested 92/s" could
                // mean 2 requests held back or ~90, a 45x range, so the deferred FRACTION --
                // the only thing the number is for -- was unknowable. `wanted` is what this
                // round would have emitted before the truncate.
                metrics
                    .vantage_repair_budget_deferred_total
                    .inc_by(wanted as u64);
            }
            return; // try again next tick; state and queue entry are intact
        }
        self.emit_budget -= emitted;
        self.in_flight += emitted;
        for peer in targets {
            if self.requested.insert((peer, h.clone())) {
                // Only now is the digest genuinely "requested", which is what `on_serve`'s
                // P1-2 gate means. Setting it earlier would let a digest we never asked for
                // accept an unsolicited serve.
                self.requested_hashes.insert(h.clone());
                effects.push(Effect::RequestTo(peer, h.clone()));
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repairs_requested.inc();
                }
            }
        }
        if let Some(entry) = self.fanout.get_mut(h) {
            // Count what was actually asked, not what was budgeted. If the fill loop found
            // nobody new, every peer has already been asked, so jump straight to full
            // coverage -- that is what retires the digest in `retry_requests` instead of
            // leaving it queued forever emitting nothing.
            // `emitted == 0` here means the fill loop found nobody new, i.e. every peer has
            // already been asked -- so jump to full coverage, which is what retires the digest
            // in `retry_requests` instead of leaving it queued forever emitting nothing.
            //
            // Believed unreachable: `asked` always equals |requested INTERSECT (.,h)|, and
            // all-peers-asked forces the `entry.asked >= n` early return above. Asserted
            // rather than trusted because if it IS reachable the silent jump inflates the
            // later `in_flight_asks` release into a window that quietly WIDENS -- a bug that
            // would present as excess inbound, nowhere near this line.
            debug_assert!(
                emitted > 0,
                "fan_out emitted nothing while asked ({}) < n ({}): `asked` has drifted from \
                 the `requested` set",
                entry.asked,
                n
            );
            entry.asked = if emitted == 0 {
                n
            } else {
                entry.asked + emitted
            };
            entry.in_flight_asks += emitted;
            entry.asked_at = now_tick;
            // Widen for the NEXT round -- here, where a round actually went out, and never
            // on a deferred one. Doubling before the budget/window truncate above meant a
            // digest starved through k ticks jumped to ~FANOUT_FIRST * 2^k the instant room
            // opened, so the staged escalation this whole mechanism exists to provide was
            // erased exactly when recovering from congestion -- the moment a wide fan-out is
            // least affordable. `saturating_mul` keeps the arithmetic safe; `width_cap` and
            // `n` bound what is actually asked.
            entry.next_width = entry.next_width.saturating_mul(2);
        }
    }

    /// Create the per-digest fan-out state for a newly-missing block and emit its first
    /// round. Separate from `fan_out` because only the `settle` miss branch knows the
    /// coordinate; later rounds read it back from `FanoutState`.
    fn begin_fanout(
        &mut self,
        h: &Digest,
        author: PublicKey,
        height: Height,
        effects: &mut Vec<Effect>,
    ) {
        let n = self.peers.len();
        if n == 0 {
            return;
        }
        self.fanout.entry(h.clone()).or_insert_with(|| FanoutState {
            author,
            height,
            start: Self::fanout_start(h, n),
            asked: 0,
            next_width: FANOUT_FIRST,
            in_flight_asks: 0,
            asked_at: 0,
        });
        self.fan_out(h, effects);
    }

    /// Move the per-tick recovery ceiling toward what the node can actually absorb.
    ///
    /// A FIXED ceiling was a regression, measured on the 2026-08-07 n=100 run at 43f4f0c:
    /// node 96 reported `vantage_repair_budget_deferred_total` 5,425 and 9,224 across two
    /// attempts (healthy nodes: 0) while `vantage_bulk_inbound_dropped_total` was ZERO. There
    /// was no congestion; the node had headroom and the constant refused it work anyway. Its
    /// cursor ended 481 and 452 views behind, failing the 200-view quality gate that the
    /// previous build had passed at worst 74. Capping recovery unconditionally simply
    /// exchanged the original failure (too much recovery traffic -> core collapse) for its
    /// mirror image (too little -> recovery never catches up).
    ///
    /// So the ceiling is conditional, AIMD on the same signal that diagnosed the original
    /// bug: halve on any new bulk-inbound drop, double on a clean tick. A node that is
    /// drowning backs off; a node with headroom -- which is exactly the node that needs to
    /// recover -- is allowed to use it. `RECOVERY_EMIT_MIN` keeps a drowning node making
    /// some progress rather than none; `RECOVERY_EMIT_MAX` keeps a clean node from
    /// rediscovering the unbounded behaviour.
    ///
    /// The DELTA matters, not the absolute counter: drops never decrease, so reacting to the
    /// total would throttle a node forever after one bad moment.
    ///
    /// CONGESTION SIGNAL, third and current iteration (2026-08-07, `adc6048` run). The signal
    /// must be one this controller's actuator can actually MOVE. Its actuator is "how many
    /// `request(h)` to emit", so the only load it induces on itself is the answers it invites.
    /// History of getting this wrong twice:
    ///   1. Bulk-inbound drops alone -- blind in the pre-fan-out-cap era, when a x99 request
    ///      flood pinned the MAIN queue at 1000 while the bulk queue never dropped anything.
    ///   2. Core inbound-queue depth at 256 -- the mirror error. See
    ///      `CORE_QUEUE_NEAR_OVERFLOW`: the main queue is dominated by availability crediting
    ///      (~= n x (2f+1) x block_rate; 128,942 refs/s at n=100), which repair cannot
    ///      influence, so the loop throttled recovery in response to load that was not its
    ///      own and would not decrease however far it backed off.
    ///
    /// Note (1) and (2) are NOT symmetric mistakes about which queue to read: they are the
    /// same mistake about ATTRIBUTION. Neither signal isolated repair's own contribution.
    ///
    /// So: bulk drops are the primary signal (the fleet, including us, is shedding service
    /// traffic -- back off regardless of cause), the core queue is a backstop only at ~900
    /// where consensus traffic is about to be shed, and absorption proper is left to
    /// `RECOVERY_IN_FLIGHT_MAX`, which is closed-loop: a slot frees only when an answer
    /// lands or is retired, so it self-limits on the real answer rate instead of guessing.
    ///
    /// What is deliberately NOT used: bulk-queue DEPTH. `Inbound::is_bulk` covers only
    /// inbound service REQUESTS (`HeadersRequest`/`ControlFetch`/`BodyFetch`/`LaneResume`/
    /// `ResumeHello`) -- serve RESPONSES were moved to the main queue on purpose, so bulk
    /// depth measures what PEERS ask of us, not what we can absorb. Reading it here would be
    /// a third misattribution of exactly the same kind.
    fn adapt_recovery_ceiling(&mut self) {
        let congested = self.core_queue_len >= CORE_QUEUE_NEAR_OVERFLOW;
        let new_drops = match &self.metrics {
            Some(metrics) => {
                let now = metrics.vantage_bulk_inbound_dropped_total.get();
                let d = now.saturating_sub(self.last_bulk_drops);
                self.last_bulk_drops = now;
                d
            }
            None => 0,
        };
        self.emit_ceiling = if congested || new_drops > 0 {
            (self.emit_ceiling / 2).max(RECOVERY_EMIT_MIN)
        } else {
            self.emit_ceiling.saturating_mul(2).min(RECOVERY_EMIT_MAX)
        };
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_repair_emit_ceiling
                .set(self.emit_ceiling as i64);
            metrics.vantage_repair_in_flight.set(self.in_flight as i64);
            // WHY the ceiling moved, not just where it landed. A gauge cannot answer that,
            // and the ambiguity cost a full debugging cycle: on the 2026-08-08 n=50/200k
            // netem run the failed nodes reported `emit_ceiling` = 256 (the floor) with
            // BOTH inputs at zero in ABSOLUTE terms -- `core_queue_peak` 0 and
            // `bulk_inbound_dropped_total` 0. Those three readings cannot all be true, and
            // with only a gauge there is no way to say which is lying. Counting the causes
            // makes it self-reporting: halved_by_queue + halved_by_drops must equal the
            // number of halvings, and `raised` is the denominator that separates "the
            // controller keeps halving" from "the controller stopped running and the gauge
            // is stale" -- opposite bugs with opposite fixes.
            //
            // Both causes are counted when both hold, so the sum can exceed the halving
            // count; that is deliberate, since the interesting question is which signal is
            // ever active at all, not how a tie was broken.
            if congested || new_drops > 0 {
                if congested {
                    metrics.vantage_repair_ceiling_halved_by_queue.inc();
                }
                if new_drops > 0 {
                    metrics.vantage_repair_ceiling_halved_by_drops.inc();
                }
            } else {
                metrics.vantage_repair_ceiling_raised.inc();
            }
        }
    }

    /// Report the core inbound queue depth, for `adapt_recovery_ceiling`'s near-overflow
    /// BACKSTOP (`CORE_QUEUE_NEAR_OVERFLOW`, ~900 of 1000) and for `fan_out`'s escalation
    /// width cap (`CORE_QUEUE_CONGESTED`, 256). It is NOT the emit ceiling's primary
    /// congestion signal -- repair's own emission cannot move this queue. Called from the
    /// same 1s tick that samples the gauge.
    pub fn observe_core_queue(&mut self, len: usize) {
        self.core_queue_len = len;
    }

    /// Record that `peer` has confirmed holding `author`'s lane up to `height`. Monotone:
    /// a lower height never regresses an entry.
    ///
    /// Called for every availability credit the node already processes (`credit_refs`), so
    /// this is free in wire terms. See `holders` for why the first fan-out round needs it.
    pub fn note_holder(&mut self, peer: PublicKey, author: PublicKey, height: Height) {
        let entry = self
            .holders
            .entry(author)
            .or_default()
            .entry(peer)
            .or_insert(0);
        if height > *entry {
            *entry = height;
        }
    }

    /// The height below which EVERY peer has confirmed holding `author`'s lane, or `None`
    /// if any peer has not been heard from about this author at all.
    ///
    /// This is the safe floor for evicting `author`'s blocks from the shared cache: if all
    /// n-1 peers have credited height >= h, no correct peer can still need h from us, so
    /// dropping it cannot starve anyone's repair. `None` -- not zero -- when the picture is
    /// incomplete, so a missing peer blocks eviction rather than authorising it.
    ///
    /// A single lagging peer therefore pins this floor and memory keeps growing. That is
    /// deliberate: the alternative, evicting above the floor to force progress, trades a
    /// bounded and observable memory problem for an unbounded liveness one -- it would
    /// recreate exactly the starved-repair failure this module was rewritten to fix.
    /// `vantage_block_cache_evict_blocked` reports when this is happening.
    pub fn universally_held_below(&self, author: &PublicKey) -> Option<Height> {
        let by_peer = self.holders.get(author)?;
        if by_peer.len() < self.peers.len() {
            return None; // not every peer has been heard from about this lane
        }
        by_peer.values().copied().min()
    }

    /// Every author this node has holder information for -- the eviction driver's work list.
    pub fn known_lane_authors(&self) -> Vec<PublicKey> {
        self.holders.keys().copied().collect()
    }

    /// Peers that have confirmed holding `author`'s lane at or above `height`, capped at
    /// `want`. Ordered by descending confirmed height, so the peers furthest ahead -- and
    /// therefore least likely to have pruned the block -- are asked first.
    fn likely_holders(
        &self,
        author: &PublicKey,
        height: Height,
        want: usize,
        h: &Digest,
    ) -> Vec<PublicKey> {
        let Some(by_peer) = self.holders.get(author) else {
            return Vec::new();
        };
        let mut candidates: Vec<(Height, PublicKey)> = by_peer
            .iter()
            .filter(|(_, &h)| h >= height)
            .map(|(p, &h)| (h, *p))
            .collect();
        // Descending height, then by a PER-DIGEST permutation of the key so the order is
        // deterministic across nodes and runs (a HashMap iteration order would not be)
        // WITHOUT being the same order for every digest.
        //
        // A plain key sort reintroduced exactly the pathology `fanout_start` exists to
        // prevent: equal-height holders are the common case (every caught-up peer reports the
        // same tip), so a global key order made the lowest-keyed peers absorb the committee's
        // entire first-round serve load while the highest-keyed served none. Mixing the
        // digest in spreads that per digest at identical cost and keeps full determinism --
        // every node computes the same order for the same digest.
        candidates.sort_unstable_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| Self::holder_rank(&a.1, h).cmp(&Self::holder_rank(&b.1, h)))
        });
        candidates.into_iter().take(want).map(|(_, p)| p).collect()
    }

    /// A per-digest ordering key for a holder, so equal-height holders are not ranked the
    /// same way for every digest. Both inputs are already hashes, so mixing eight bytes of
    /// each is enough -- no re-hashing, and identical on every node for a given (peer,
    /// digest) pair, which is what keeps the fan-out reproducible.
    fn holder_rank(peer: &PublicKey, h: &Digest) -> u64 {
        let mut pk = [0u8; 8];
        let mut dg = [0u8; 8];
        pk.copy_from_slice(&peer.0[..8]);
        dg.copy_from_slice(&h.0[..8]);
        u64::from_le_bytes(pk) ^ u64::from_le_bytes(dg)
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
        self.ticks += 1;
        // RECLAIM timed-out rounds BEFORE refilling and escalating, so the freed slots are
        // usable in this very tick rather than the next one. Without this the in-flight
        // window is an absorbing state -- see `ASK_TIMEOUT_TICKS` for the two ways a node
        // reaches `room == 0` with asks that will never be answered, one of which needs no
        // adversary at all (the harness's own bulk-queue `try_send` drops).
        //
        // Coverage bookkeeping (`asked`, `requested`, `requested_hashes`) is deliberately
        // untouched: this corrects window accounting only, so N6's one-request-per-(peer,
        // digest)-ever still holds exactly and escalation resumes toward peers not yet asked.
        let now_tick = self.ticks;
        let mut reclaimed = 0usize;
        for (_, state) in self.fanout.iter_mut() {
            if state.in_flight_asks > 0
                && now_tick.saturating_sub(state.asked_at) >= ASK_TIMEOUT_TICKS
            {
                reclaimed += state.in_flight_asks;
                state.in_flight_asks = 0;
            }
        }
        if reclaimed > 0 {
            self.in_flight = self.in_flight.saturating_sub(reclaimed);
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_repair_asks_reclaimed_total
                    .inc_by(reclaimed as u64);
            }
        }
        // Refill the tick's recovery allowance. This is the ONE place it is refilled, so the
        // ceiling applies to the hot-path first rounds too, not just to escalations.
        self.adapt_recovery_ceiling();
        self.emit_budget = self.emit_ceiling;
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
            // stop spending requests on it. Release its window slots -- every request counted
            // into `in_flight` must be released on exactly one of the two retirement paths,
            // or the window latches shut and the node stops asking for anything forever.
            if !self.blocked_on.contains_key(h) {
                if let Some(state) = self.fanout.remove(h) {
                    self.in_flight = self.in_flight.saturating_sub(state.in_flight_asks);
                }
                continue;
            }
            let before = effects.len();
            self.fan_out(&key.1.clone(), &mut effects);
            if effects.len() > before {
                // Only count a round that actually went out. Under the congestion width cap
                // a queued digest is visited every tick and emits nothing, which would
                // otherwise inflate this counter into meaninglessness.
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repair_fanout_escalations_total.inc();
                }
            }
            if self
                .fanout
                .get(&key.1)
                .is_some_and(|s| s.asked < self.peers.len())
            {
                // Still short of full coverage -- requeue. Note this includes a digest parked
                // at the congestion width cap: it stays queued, emitting nothing, and resumes
                // widening once the queue drains.
                self.fanout_queue.insert(key);
            } else {
                // Fully covered: N6's fan-out obligation for `h` is discharged, so the
                // per-digest state is dead weight from here on. Dropping it is safe --
                // `requested_hashes` (never pruned) is what gates `on_serve`, and
                // `requested` (never pruned) is what prevents a re-ask.
                if let Some(state) = self.fanout.remove(&key.1) {
                    self.in_flight = self.in_flight.saturating_sub(state.in_flight_asks);
                }
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
                // "Already being asked for" is now `fanout` OR `requested_hashes`, not
                // `requested_hashes` alone: with a recovery budget, a digest can be known
                // and queued before any request has actually gone out, and
                // `requested_hashes` is deliberately only set once one has (see `fan_out`).
                // Either way a repeated miss on the same digest stays O(1) here, which is
                // what `on_block_available`'s walks depend on.
                if !self.requested_hashes.contains(&h) && !self.fanout.contains_key(&h) {
                    self.fanout_queue.insert((height, h.clone()));
                    self.begin_fanout(&h, author, height, effects);
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

    /// Outstanding-request count, for the window test.
    #[cfg(test)]
    pub(crate) fn in_flight_for_test(&self) -> usize {
        self.in_flight
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
