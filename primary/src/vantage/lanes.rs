// PHASE3-SPEC.md §3.2 -- lane state, first-hand availability, ACK trigger (N1-N5).
//
// `LaneManager` owns the per-author block index, the ack bookkeeping, and the C/T
// candidate registers. It shares its block cache (`BlockCache`, behind `SharedBlocks`)
// with `repair::Repairer`, which also writes into it (repaired/served blocks) and
// reads it (to decide what it may serve). This is a deliberate simplification of the
// spec's "one task each" framing (§3.2/§3.3) into "two tasks, one shared chain cache,
// two disjoint bookkeeping sets" -- documented in PHASE3-NOTES.md.

use crate::messages::{Ack, Header};
use crate::primary::Height;
use crate::vantage::block::{self, block_ok, BlockRef};
use crate::vantage::Effect;
use config::{Committee, Stake, WorkerId};
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use store::Store;

/// Per-block bookkeeping (§3.2 `BlockEntry`).
#[derive(Clone)]
pub struct BlockEntry {
    pub block: Header,
    /// N1/N2: received via an authentic `publish` from the encoded author (channel
    /// sender == author), directly or as an upgrade of previously-repaired bytes.
    pub direct: bool,
    /// N6: arrived (at least once) only via `serve`, without provenance.
    pub repaired: bool,
    /// N8: must be held + served forever once set. Sticky (never cleared).
    pub retained: bool,
    /// D1: this block's own referenced worker batches are locally present.
    pub payload_ok: bool,
    /// PHASE6-SPEC.md §9 gate amendment (D6-7, performance-only, deterministic-
    /// equivalent): memoized result of `direct_prefix_ok`'s walk down to genesis for
    /// THIS exact digest -- the original PHASE3-SPEC.md §3.2 design. Once true, never
    /// reset (the underlying chain a digest names is immutable once cached, and
    /// `direct`/`payload_ok` are themselves monotonic -- see `direct_prefix_ok`'s own
    /// doc comment for why this is a sound memoization, not just an optimization).
    /// Only ever set to `true`; a failed check never caches `false`, so a later retry
    /// (once e.g. `payload_ok` flips) always re-walks from scratch.
    pub direct_prefix_verified: bool,
    /// PHASE6-SPEC.md §9 gate amendment (D6-7 continued -- profiled AFTER the original
    /// (a)/(b) fixes still left the capacity probe short): memoized result of
    /// `verified_prefix_through_genesis`'s OWN walk down to genesis for THIS exact
    /// digest -- a DIFFERENT, previously-unmemoized check from `direct_prefix_verified`
    /// above (chain/`BlockOK` validity alone, no `direct`/`payload_ok` requirement) that
    /// `direct_pub`/`holds_prefix` each call on every single invocation. Sound for the
    /// identical reason `direct_prefix_verified` is: only a successful walk all the way
    /// to genesis (or to an already-verified ancestor) ever sets it, a cached block's
    /// own content is immutable, and `BlockOK` is a pure function of that content -- so
    /// nothing can ever make an already-verified chain stop verifying.
    pub chain_verified: bool,
    /// Fable perf audit: memoized result of `block::block_ok(&self.block, ...)` for
    /// THIS exact digest. Set exactly once, in `BlockCache::upsert`, and ONLY by a
    /// caller that has ALREADY run a genuine, passing `block_ok` check on this exact
    /// `Header` (both current call sites -- `LaneManager::process_publish_inner` and
    /// `Repairer::on_serve` -- check `block_ok` first and return early without ever
    /// calling `upsert` if it fails, so `upsert` is never reached with a failing
    /// header). Sound to trust forever after that: the cache is digest-keyed and a
    /// cached entry's `block` content is immutable once inserted (a later `upsert` for
    /// the SAME digest is, by construction, byte-identical content -- see `upsert`'s own
    /// doc comment), and `block_ok` is a pure function of that content plus the
    /// (per-session-constant) committee/sid/size-cap arguments -- so a digest that
    /// passed once can never later fail. Read-side chain walks
    /// (`verified_prefix_through_genesis`, `collect_verified_suffix`, and
    /// `repair::Repairer::settle`) consult this flag instead of re-running the header's
    /// `blake3` self-consistency check on every visit.
    pub block_ok_verified: bool,
}

impl BlockEntry {
    /// Author-pin + consecutive-height check shared by every author-pinned chain
    /// walk in this file (`direct_prefix_ok`/`verified_prefix_through_genesis`/
    /// `collect_verified_chain`/`collect_verified_suffix`): `true` iff this entry
    /// belongs to `author` and sits at exactly `expected_height` (§1 "one author
    /// index" + "consecutive heights" -- see `direct_prefix_ok`'s doc comment for
    /// why a Byzantine parent-pointer graft/cycle needs both checks to terminate
    /// the walk safely).
    fn pinned_at(&self, author: PublicKey, expected_height: Height) -> bool {
        self.block.author == author && self.block.height == expected_height
    }
}

/// Shared block cache: every block this node has ever obtained, keyed by its digest
/// (`Header.id`, which folds `sid` -- §3.1). Forks are representable (multiple digests
/// per (author, height)).
#[derive(Default)]
pub struct BlockCache {
    by_digest: HashMap<Digest, BlockEntry>,
    by_author: HashMap<PublicKey, BTreeMap<Height, HashSet<Digest>>>,
}

impl BlockCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, h: &Digest) -> Option<&BlockEntry> {
        self.by_digest.get(h)
    }

    pub fn contains(&self, h: &Digest) -> bool {
        self.by_digest.contains_key(h)
    }

    /// Insert a freshly-seen block, or upgrade an existing entry's provenance/payload
    /// flags. `direct`/`repaired`/`payload_ok`/`block_ok_verified` are OR-merged (N2: "a
    /// later publish may upgrade bytes previously cached via repair"; all flags besides
    /// the block body itself are monotonic/sticky, matching N8's "no discard").
    /// `block_ok_verified` must be `true` only when the caller has already run a
    /// genuine, passing `block::block_ok` check on this exact `block` (see
    /// `BlockEntry::block_ok_verified`'s doc comment) -- both current call sites satisfy
    /// this by construction.
    ///
    /// Fable perf audit: the valuable part on an EXISTING entry is OR-merging the
    /// monotonic flags (esp. `block_ok_verified`, so readers never re-run `block_ok`).
    /// The `block` field is still overwritten last-writer-wins, exactly as before this
    /// audit -- NOT skipped: although the digest key pins everything `Header::digest()`
    /// folds and everything `block_ok` checks, the one field it pins neither
    /// (`parent_cert.author`) is Byzantine-forgeable, so two same-`id` headers can
    /// differ there; keeping the overwrite preserves the pre-audit last-writer-wins
    /// bytes exactly (that field is dead on every vantage read path, so this is purely
    /// about staying byte-identical, not correctness). The overwrite is a cheap move of
    /// an already-owned `Header`, not a clone.
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
                entry.block = block; // last-writer-wins, as before the audit (see doc)
            }
        }
    }

    /// Returns `true` iff this call is the one that newly retained `h` (idempotent).
    pub fn mark_retained(&mut self, h: &Digest) -> bool {
        match self.by_digest.get_mut(h) {
            Some(entry) if !entry.retained => {
                entry.retained = true;
                true
            }
            _ => false,
        }
    }

    /// Returns `true` iff this call is the one that newly marked payload presence.
    pub fn set_payload_ok(&mut self, h: &Digest, ok: bool) -> bool {
        match self.by_digest.get_mut(h) {
            Some(entry) if ok && !entry.payload_ok => {
                entry.payload_ok = true;
                true
            }
            _ => false,
        }
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

    /// `direct_prefix_ok(a, k, h)` (§3.2): direct mark + `payload_ok` on this block, and
    /// the same holds recursively for the parent, down to genesis (D1's "whole
    /// prefix").
    ///
    /// Walks by expected height (strictly decreasing each step, checked against each
    /// visited block's *own* height field) rather than purely by digest pointer: a
    /// Byzantine pair of blocks can reference each other as "parent" with no hash
    /// preimage cost at all (each is individually self-consistent -- `BlockOK` only
    /// checks a block against *its own* `parent_cert.height`, never against the real
    /// height of whatever the pointer resolves to), which would otherwise make a
    /// pointer-only walk spin forever. Tracking height gives a strictly-decreasing
    /// termination measure independent of any adversarial digest choice, and doubles as
    /// this function's enforcement of "consecutive heights" (part of "valid lane
    /// prefix", §1). Also pins the walk's author to the starting block's own author and
    /// rejects any visited block authored by someone else (§1's "one author index" --
    /// otherwise a Byzantine author can graft their block onto a *different* author's
    /// genuine, already-verified chain: `BlockOK` never checks a parent pointer's target
    /// against the child's own author, only the child's internal height arithmetic).
    /// PHASE6-SPEC.md §9 gate amendment (D6-7): incrementally memoized via
    /// `BlockEntry::direct_prefix_verified` -- the walk stops as soon as it reaches an
    /// ancestor already known verified (trusting it, exactly as the original PHASE3-
    /// SPEC.md §3.2 design intended), instead of re-walking all the way to genesis on
    /// every call. Sound because: (a) only a SUCCESSFUL full walk to genesis (or to an
    /// already-verified ancestor) ever sets the flag, never a failed one, so a
    /// negative result is never memoized and a later retry (e.g. once `payload_ok`
    /// flips true) still re-walks from scratch; (b) `direct`/`payload_ok` are
    /// themselves monotonic (only ever OR-merged upward, §3.2 N2/N8), so a chain that
    /// verified once can never stop verifying; (c) a cached digest's own `block`/
    /// `parent_cert` content is immutable once inserted. Every other check (author
    /// pinning, height arithmetic, `direct && payload_ok`) is unchanged from the
    /// original per-visited-node semantics.
    pub fn direct_prefix_ok(&mut self, genesis: &Digest, h: &Digest) -> bool {
        let Some(start) = self.by_digest.get(h) else {
            return false;
        };
        if start.direct_prefix_verified {
            return true;
        }
        let author = start.block.author;
        let mut expected_height = start.block.height;
        let mut cur = h.clone();
        let mut newly_walked: Vec<Digest> = Vec::new();
        loop {
            if &cur == genesis {
                if expected_height != 0 {
                    return false;
                }
                break;
            }
            if expected_height == 0 {
                return false; // ran out of height before reaching genesis
            }
            let Some(entry) = self.by_digest.get(&cur) else {
                return false;
            };
            if !entry.pinned_at(author, expected_height) {
                return false; // cross-author graft (§1 "one author index") or height gap
            }
            if !(entry.direct && entry.payload_ok) {
                return false;
            }
            if entry.direct_prefix_verified {
                break; // this ancestor (and everything below it) already verified
            }
            newly_walked.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
        for d in &newly_walked {
            if let Some(e) = self.by_digest.get_mut(d) {
                e.direct_prefix_verified = true;
            }
        }
        true
    }

    /// A "valid lane prefix" (§1 last row): one author, consecutive heights, matching
    /// predecessor hashes, every block `BlockOK`, back to genesis. See
    /// `direct_prefix_ok`'s doc comment for why this walks by expected height (and
    /// pinned author) rather than purely by digest pointer (termination against
    /// adversarial reference cycles/grafts, and the "one author"/"consecutive heights"
    /// checks themselves).
    ///
    /// PHASE6-SPEC.md §9 gate amendment (D6-7 continued): incrementally memoized via
    /// `BlockEntry::chain_verified`, mirroring `direct_prefix_ok`'s exact pattern --
    /// stops at the first already-verified ancestor instead of re-walking to genesis on
    /// every call (this used to delegate to `collect_verified_chain`'s genesis-anew
    /// walk on EVERY invocation; profiling the capacity probe after the (a)/(b) fixes
    /// showed this specific, separate walk -- called by `direct_pub`/`holds_prefix` on
    /// every C/T/T-pairing check -- as the new dominant cost). `collect_verified_chain`
    /// itself is unchanged (still used by nothing else; kept for the shape/doc
    /// parallel with `collect_verified_suffix`, and in case a future caller needs the
    /// actual hash sequence again).
    ///
    /// Fable perf audit: the per-visited-block `block::block_ok` re-check now consults
    /// `BlockEntry::block_ok_verified` instead of recomputing (every cached entry has
    /// this memo set true at admission time -- see that field's doc comment), so
    /// `_committee`/`_sid`/`_max_block_payload` are no longer read in this body; kept as
    /// parameters (renamed, not removed) to leave `collect_verified_suffix`'s identical
    /// signature shape and this function's own call sites (`direct_pub`, `holds_prefix`)
    /// untouched.
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
        loop {
            if &cur == genesis {
                if expected_height != 0 {
                    return false;
                }
                break;
            }
            if expected_height == 0 {
                return false;
            }
            let Some(entry) = self.by_digest.get(&cur) else {
                return false;
            };
            if !entry.pinned_at(author, expected_height) {
                return false; // cross-author graft (§1 "one author index") or height gap
            }
            if !entry.block_ok_verified {
                return false;
            }
            if entry.chain_verified {
                break; // this ancestor (and everything below it) already verified
            }
            newly_walked.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
        for d in &newly_walked {
            if let Some(e) = self.by_digest.get_mut(d) {
                e.chain_verified = true;
            }
        }
        true
    }

    /// PHASE4-SPEC.md §9: like `verified_prefix_through_genesis`, but returns the actual
    /// verified chain (genesis first, `h` last) instead of a bare bool -- the output
    /// cursor's `Expand_D` needs the hashes themselves, not just a validity bit.
    /// `verified_prefix_through_genesis` is NOT a thin wrapper over this (this doc used
    /// to say it was): the D6-7 gate amendment below gave it its own `chain_verified`
    /// memoization and its own copy of the walk with an early-break this function
    /// doesn't need (this one has no memo to stop at -- every call walks all the way to
    /// genesis). No caller in this workspace currently needs the actual hash sequence
    /// (only `collect_verified_suffix`'s incremental sibling is called, by
    /// `cursor.rs`) -- kept as public API per the original PHASE4-SPEC.md #9 design for
    /// whichever future caller needs it.
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
                return None; // cross-author graft (§1 "one author index") or height gap
            }
            if !block_ok(&entry.block, committee, sid, max_block_payload) {
                return None;
            }
            chain.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
    }

    /// PHASE6-SPEC.md §9 gate amendment (D6-7, performance-only, deterministic-
    /// equivalent): like `collect_verified_chain`, but stops at a caller-supplied
    /// watermark `(stop_height, stop_digest)` instead of walking all the way to
    /// genesis every time -- the cursor's per-lane emitted-height watermark records
    /// exactly this (the last block of this author's lane already verified+emitted),
    /// so re-verifying everything below it on every subsequent seal is pure waste.
    /// Returns just the NEW suffix, ascending, excluding the watermark itself. `None`
    /// if the walk breaks before reaching the watermark (missing/invalid block) OR if
    /// the chain reaches `stop_height` on a DIFFERENT digest than `stop_digest` (a
    /// fork below the watermark -- Byzantine-unreachable under this protocol's own
    /// safety properties, since the cursor only ever advances along a single agreed
    /// chain per author, but checked defensively rather than assumed).
    ///
    /// Fable perf audit: the per-visited-block `block::block_ok` re-check now consults
    /// `BlockEntry::block_ok_verified` instead of recomputing (see that field's doc
    /// comment); `_committee`/`_sid`/`_max_block_payload` are no longer read in this
    /// body but stay in the signature (renamed, not removed) so `cursor.rs`'s call site
    /// -- outside this change's scope -- is untouched.
    pub fn collect_verified_suffix(
        &self,
        _committee: &Committee,
        _sid: &Digest,
        _max_block_payload: usize,
        stop_height: Height,
        stop_digest: &Digest,
        h: &Digest,
    ) -> Option<Vec<Digest>> {
        let start = self.by_digest.get(h)?;
        let author = start.block.author;
        let mut expected_height = start.block.height;
        let mut cur = h.clone();
        let mut chain = Vec::new();
        loop {
            if expected_height == stop_height {
                if &cur != stop_digest {
                    return None; // fork below the watermark
                }
                chain.reverse();
                return Some(chain);
            }
            if expected_height == 0 {
                return None; // ran out of height before reaching the watermark
            }
            let entry = self.by_digest.get(&cur)?;
            if !entry.pinned_at(author, expected_height) {
                return None; // cross-author graft (§1 "one author index") or height gap
            }
            if !entry.block_ok_verified {
                return None;
            }
            chain.push(cur.clone());
            cur = entry.block.parent_cert.header_digest.clone();
            expected_height -= 1;
        }
    }
}

/// One entry of a periodic per-lane availability watermark (optional, flag-gated
/// replacement for per-block ACKs -- `Parameters::ack_watermarks`): "for `author`, the
/// declaring party holds `author`'s lane through (`height`, `head`)". Digest-bound,
/// never height-only -- see `LaneManager::resolve_watermark`'s doc comment for why
/// crediting must always resolve to an exact `BlockRef` before touching the shared
/// `AckAggregator` (the same invariant a per-block ack already satisfies via
/// `Ack::reference`). Derives mirror `Ack`'s own wire derives (`messages::Ack`).
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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

/// Greatest height, ties broken by lexicographically smallest digest (§2 N5 "newest"),
/// read directly from an `(author, height, digest)` BTree index.
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
        // Reverse BTree iteration sees larger digests first for a fixed height.
        // Overwrite through the height group so the smallest digest wins ties.
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

/// First-hand ACK accumulator for Vantage data dissemination.
///
/// This intentionally sits outside `LaneManager`'s hot protocol state: it keeps the
/// per-sender dedup sets needed to implement the paper's first-hand ACK counting, and
/// emits only monotone f+1 / 2f+1 availability marks to the core.
pub struct AckAggregator {
    committee: Committee,
    members: HashSet<PublicKey>,
    senders: HashMap<BlockRef, HashSet<PublicKey>>,
    weights: HashMap<BlockRef, Stake>,
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

    pub fn record_ack(&mut self, sender: PublicKey, reference: BlockRef) -> AckAggregationResult {
        if !self.members.contains(&sender) {
            return AckAggregationResult {
                accepted: false,
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
        AckAggregationResult {
            accepted: true,
            availability: Some(AckAvailability {
                reference,
                threshold,
            }),
        }
    }
}

pub type SharedAckAggregator = Arc<Mutex<AckAggregator>>;

pub struct LaneManager {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    store: Store,
    blocks: SharedBlocks,

    /// ACK-derived availability marks per exact tuple. First-hand sender counting lives
    /// in `AckAggregator`; the core consumes only these monotone threshold facts.
    ack_availability: HashMap<BlockRef, AckThreshold>,
    /// N3: tuples we have already broadcast our own ack for (at most once, ever).
    acked: HashSet<BlockRef>,
    /// Direct, payload-ready tuples whose prefix has not yet been confirmed
    /// `DirectPub`. A missing parent/payload can make a descendant become valid later;
    /// this keeps retries to that monotone frontier instead of every cached block.
    pending_direct: BTreeSet<BlockRef>,
    /// Tuples already confirmed as `DirectPub`. Ordered by `(author, height, digest)` so
    /// N5's newest selection and GC can walk/prune by key rather than scanning all block
    /// cache entries.
    direct_pub_refs: BTreeSet<BlockRef>,
    /// Confirmed `DirectPub` tuples that have reached quorum ack stake.
    quorum_direct_refs: BTreeSet<BlockRef>,

    /// N5 registers.
    c_candidate: HashMap<PublicKey, BlockRef>,
    t_candidate: HashMap<PublicKey, BlockRef>,

    /// Our own lane frontier: (height, digest of that block, or genesis at height 0).
    own_frontier: (Height, Digest),

    /// Optional ack-watermark front-end (flag-gated at the core level; see
    /// `Parameters::ack_watermarks`). This party's own greatest DIRECT-PREFIX height
    /// per author, and the digest at that height -- "the greatest h such that every
    /// height <= h of that author's lane is DirectPub at this party". Advanced
    /// incrementally, exactly where N3's DirectPub confirmation already fires
    /// (`record_direct_pub`), never by rescanning. Bounded by committee size (one
    /// entry per author) -- plain `HashMap`, no GC needed. This bookkeeping is
    /// unconditional (LaneManager itself doesn't know the flag) and inert unless
    /// `take_avail_flush` is ever called by the core.
    own_avail_watermark: HashMap<PublicKey, (Height, Digest)>,
    /// Set whenever `own_avail_watermark` advances; cleared by `take_avail_flush`.
    avail_dirty: bool,
    /// Per (sender, author) credited floor for INCOMING watermarks: the height up to
    /// which `sender`'s watermark has already been credited for `author`'s lane.
    /// Bounded by O(n^2) (sender x author pairs, n = committee size) -- plain
    /// `HashMap`, no GC needed.
    credited_floor: HashMap<(PublicKey, PublicKey), (Height, Digest)>,
    /// Per (sender, author) latest-wins pending slot: a watermark entry whose head
    /// resolved (attested, credited) but whose segment below the head did not fully
    /// resolve locally yet -- retried by `retry_pending_avail` once a new block is
    /// cached. Bounded by O(n^2), same as `credited_floor`, no GC needed.
    pending_avail: HashMap<(PublicKey, PublicKey), AvailEntry>,

    /// §6.4 counters; `None` in most unit tests, which don't assert on metrics.
    metrics: Option<Arc<Metrics>>,
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
            direct_pub_refs: BTreeSet::new(),
            quorum_direct_refs: BTreeSet::new(),
            c_candidate: HashMap::new(),
            t_candidate: HashMap::new(),
            own_frontier: (0, genesis),
            own_avail_watermark: HashMap::new(),
            avail_dirty: false,
            credited_floor: HashMap::new(),
            pending_avail: HashMap::new(),
            metrics: None,
        }
    }

    /// Attach §6.4 counters (production wiring only -- most unit tests skip this).
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

    pub fn blocks_handle(&self) -> SharedBlocks {
        self.blocks.clone()
    }

    /// N1: create and self-publish our own next block. Height advances immediately on
    /// self-creation -- lanes are ack-independent (no certificate wait, unlike
    /// Autobahn's `last_parent` gate at proposer.rs:241). Self-delivery counts: we
    /// process our own block as a direct publication; `VantageCore` feeds the resulting
    /// local ACK back through `AckAggregator`.
    pub async fn publish_own(
        &mut self,
        payload: BTreeMap<Digest, WorkerId>,
    ) -> (Header, Vec<Effect>) {
        let (height, prev) = self.own_frontier.clone();
        let next_height = height + 1;
        let header = Header::new_vantage(self.name, next_height, payload, prev, self.sid.clone());
        self.own_frontier = (next_height, header.id.clone());
        if let Some(metrics) = &self.metrics {
            metrics.vantage_blocks_published.inc();
        }
        let mut effects = self.process_publish_inner(self.name, header.clone()).await;
        effects.push(Effect::BroadcastPublish(header.clone()));
        (header, effects)
    }

    /// N1/N2: handle an incoming `publish` (or relayed-publish) message. `sender` is
    /// the network channel's sender (D4-trusted); direct iff it equals the block's own
    /// encoded author.
    pub async fn process_publish(&mut self, sender: PublicKey, header: Header) -> Vec<Effect> {
        self.process_publish_inner(sender, header).await
    }

    async fn process_publish_inner(&mut self, sender: PublicKey, header: Header) -> Vec<Effect> {
        let mut effects = Vec::new();

        // N9: session hygiene + malformed messages are rejected before storing or
        // counting -- no state change.
        if header.sid.as_ref() != Some(&self.sid) {
            return effects;
        }
        if !block_ok(&header, &self.committee, &self.sid, self.max_block_payload) {
            return effects;
        }

        let direct = sender == header.author; // N1
        let payload_ok = self.payload_present(&header).await;
        let digest = header.id.clone();

        {
            let mut blocks = self.blocks.lock().unwrap();
            // `block_ok` just passed above for this exact header -- memoize it.
            blocks.upsert(header.clone(), direct, false, payload_ok, true);
        }
        if direct && payload_ok {
            let r = (header.author, header.height, digest.clone());
            // Only track tuples we have NOT already acked. `refresh_author` removes a ref
            // from `pending_direct` inside its `!acked && direct_pub` branch, so an
            // already-acked ref re-inserted here could never be evicted again -- one
            // permanently pinned entry per re-delivered publish, and `pending_direct` is
            // scanned on every `refresh_author`. That defeats this set's whole purpose
            // ("under steady honest traffic the pending set contains only the freshly-
            // arrived tip"), and `LaneManager` has no GC to mop it up.
            if !self.acked.contains(&r) {
                self.pending_direct.insert(r);
            }
        }
        effects.push(Effect::BlockCached(digest.clone()));
        if header.author != self.name {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_blocks_received.inc();
            }
        }

        if direct && !payload_ok {
            let missing: Vec<(Digest, WorkerId)> = header
                .payload
                .iter()
                .map(|(d, w)| (d.clone(), *w))
                .collect();
            effects.push(Effect::SyncBatches(header.author, digest.clone(), missing));
        }

        effects.extend(self.refresh_author(header.author));
        effects
    }

    /// D1's payload gate, reusing the exact key shape
    /// `synchronizer::Synchronizer::missing_payload`/`payload_receiver::PayloadReceiver`
    /// already use (`[digest || worker_id LE]`, written on `OthersBatch`). We don't
    /// store the payload of our own workers under that key (mirroring
    /// `missing_payload`'s early return for `header.author == self.name`) -- our own
    /// blocks are always payload-ok since the `OurBatch` digests we proposed with are
    ///, by construction, digests our own workers already sealed.
    async fn payload_present(&mut self, header: &Header) -> bool {
        if header.author == self.name {
            return true;
        }
        for (digest, worker_id) in &header.payload {
            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
            if self.store.read(key).await.unwrap_or(None).is_none() {
                return false;
            }
        }
        true
    }

    /// Call once a previously-missing block's worker batches have arrived (production:
    /// after `store.notify_read` resolves following a `SyncBatches` effect; tests:
    /// after writing the payload marker directly). Re-runs the N3 ack check.
    pub fn set_payload_ready(&mut self, digest: &Digest) -> Vec<Effect> {
        let direct_ready = {
            let mut blocks = self.blocks.lock().unwrap();
            blocks.set_payload_ok(digest, true);
            blocks.get(digest).and_then(|e| {
                (e.direct && e.payload_ok).then(|| (e.block.author, e.block.height, digest.clone()))
            })
        };
        match direct_ready {
            Some(r) => {
                // Same already-acked guard as `process_publish` -- see its comment.
                if !self.acked.contains(&r) {
                    self.pending_direct.insert(r.clone());
                }
                self.refresh_author(r.0)
            }
            None => Vec::new(),
        }
    }

    /// Re-run the N3 ack trigger over direct, payload-ready tuples of `author` that have
    /// not yet been confirmed as `DirectPub`, then refresh N5 registers from the indexed
    /// direct/quorum sets. This is intentionally not a full block-cache scan: under
    /// steady honest traffic the pending set contains only the freshly-arrived tip.
    ///
    /// Deterministic and idempotent: `acked`/registers only ever grow/replace with a
    /// "newer" (§2 N5) reference, never regress.
    fn refresh_author(&mut self, author: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        let refs: Vec<BlockRef> = self
            .pending_direct
            .range(author_lower_bound(author)..=author_upper_bound(author))
            .cloned()
            .collect();
        let mut registers_changed = false;
        for r in refs {
            if !self.acked.contains(&r) && self.direct_pub(&r) {
                self.pending_direct.remove(&r);
                self.on_direct_pub_confirmed(&r, &mut effects);
                registers_changed = true;
            }
        }
        if registers_changed {
            self.refresh_registers(author);
        }
        effects
    }

    /// Note: does *not* itself call `refresh_registers` -- `refresh_author` (the only
    /// caller) always does so once after processing this batch, covering every tuple
    /// confirmed in the same pass.
    fn on_direct_pub_confirmed(&mut self, r: &BlockRef, effects: &mut Vec<Effect>) {
        // N8(i): retain the whole valid lane prefix through h.
        self.retain_prefix(r);
        self.acked.insert(r.clone());
        let ack = Ack::new(r.0, r.1, r.2.clone(), self.name);
        effects.push(Effect::BroadcastAck(ack));
        if let Some(metrics) = &self.metrics {
            metrics.vantage_acks_sent.inc();
        }
        self.record_direct_pub(r);
    }

    /// Callers only ever reach this after `verified_prefix_through_genesis` (which
    /// bounds itself by strictly-decreasing expected height) already proved the chain
    /// from `r` to genesis is real and cycle-free; this walk still tracks expected
    /// height itself too, defensively, so it can never hang even if that invariant were
    /// ever violated by a future change.
    ///
    /// Fable perf audit: early-breaks the walk at the first block that's already
    /// `retained`, instead of continuing to genesis (and re-serializing every already-
    /// retained ancestor along the way just to have `mark_retained` report "not new")
    /// every single call. Sound by the retention invariant this codebase already
    /// relies on elsewhere (mirroring the `chain_verified`/`direct_prefix_verified`/
    /// `settled` memoization pattern): retention is prefix-closed -- whenever a block is
    /// retained, its ENTIRE ancestor chain down to genesis is already retained too. This
    /// holds inductively because `mark_retained` is only ever reached in two places, and
    /// both preserve it: (a) this same function, which -- absent the early break --
    /// walks a block's *whole* verified prefix down to genesis (or down to an ancestor
    /// that, by the same induction, already has ITS prefix retained) in one call; (b)
    /// `Repairer::settle`'s post-order ascend, which retains a chain's ancestors
    /// strictly before the chain's own tip in the same call (see that function's own
    /// doc comment). So the first already-retained block this walk meets is guaranteed
    /// to already have its own full prefix retained -- stopping there, rather than
    /// re-walking it, changes neither the final retained SET nor the total newly-
    /// retained byte count: every block below the break point would have contributed
    /// `size` only to have `mark_retained` return `false` (already retained) and discard
    /// it, exactly as skipping it here discards nothing new.
    ///
    /// Also computes `serialized_size` only for a block this call is about to newly
    /// retain: the `!entry.retained` state (checked, and not yet the early-break
    /// condition, at this point in the loop) combined with this function holding the
    /// cache's lock for its whole duration guarantees `mark_retained` below always
    /// newly retains here, so sizing every visited block up front (including ones later
    /// discarded) is no longer necessary.
    fn retain_prefix(&mut self, r: &BlockRef) {
        let mut cur = r.2.clone();
        let mut expected_height = r.1;
        let mut newly_retained_bytes: u64 = 0;
        {
            let mut blocks = self.blocks.lock().unwrap();
            loop {
                if cur == self.genesis || expected_height == 0 {
                    break;
                }
                let Some(entry) = blocks.get(&cur) else {
                    break; // defensive; direct_pub already established this exists
                };
                if entry.block.height != expected_height {
                    break; // defensive; height mismatch means this isn't the real chain
                }
                if entry.retained {
                    break; // prefix-closed: everything below here is already retained
                }
                let next = entry.block.parent_cert.header_digest.clone();
                let size = bincode::serialized_size(&entry.block).unwrap_or(0);
                blocks.mark_retained(&cur);
                newly_retained_bytes += size;
                cur = next;
                expected_height -= 1;
            }
        }
        if newly_retained_bytes > 0 {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_retained_bytes.inc_by(newly_retained_bytes);
            }
        }
    }

    /// Consume a compact ACK-derived availability mark from `AckAggregator`.
    pub fn process_ack_availability(&mut self, availability: AckAvailability) -> Vec<Effect> {
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
        if threshold >= AckThreshold::Quorum
            && self.direct_pub_refs.contains(&r)
            && self.quorum_direct_refs.insert(r.clone())
        {
            self.refresh_registers(r.0);
        }
        Vec::new()
    }

    /// §4 query: `is_q_available(ref, q)`, `q` typically `committee.validity_threshold()`
    /// (f+1) or `committee.quorum_threshold()` (2f+1).
    pub fn is_q_available(&self, r: &BlockRef, q: Stake) -> bool {
        match self.ack_availability.get(r) {
            Some(AckThreshold::Quorum) => q <= self.committee.quorum_threshold(),
            Some(AckThreshold::Validity) => q <= self.committee.validity_threshold(),
            None => false,
        }
    }

    fn exact_coordinate(&self, r: &BlockRef) -> bool {
        let blocks = self.blocks.lock().unwrap();
        blocks
            .get(&r.2)
            .is_some_and(|e| e.block.author == r.0 && e.block.height == r.1)
    }

    /// `DirectPub_i(a,k,h)` (§1 D1 / §2 N1-N3): whole prefix is both chain-valid
    /// (`BlockOK` all through) and directly-published-with-payload all through.
    pub fn direct_pub(&self, r: &BlockRef) -> bool {
        if !self.exact_coordinate(r) {
            return false;
        }
        let mut blocks = self.blocks.lock().unwrap();
        blocks.verified_prefix_through_genesis(
            &self.committee,
            &self.sid,
            self.max_block_payload,
            &self.genesis,
            &r.2,
        ) && blocks.direct_prefix_ok(&self.genesis, &r.2)
    }

    /// §4 query: `locally_available(ref)` = holds the valid lane prefix, or
    /// (f+1)-available.
    pub fn locally_available(&mut self, r: &BlockRef) -> bool {
        self.holds_prefix(r) || self.is_q_available(r, self.committee.validity_threshold())
    }

    /// §4 query: `author_ok(ref)` = `DirectPub_i` or (f+1)-available. `DirectPub_i`
    /// already implies retention (N3 retains on the same transition that triggers the
    /// ack, via `refresh_author`/`on_direct_pub_confirmed`), so unlike `holds_prefix`
    /// this doesn't need its own retain-on-success side effect.
    pub fn author_ok(&self, r: &BlockRef) -> bool {
        self.direct_pub(r) || self.is_q_available(r, self.committee.validity_threshold())
    }

    /// §4 query: `holds_prefix(ref)` -- we hold a verified (chain-valid) prefix through
    /// this exact reference, regardless of provenance (direct or repaired). N8(ii): a
    /// local-availability check that succeeds *because* we hold the prefix must retain
    /// it from now on -- unlike `direct_pub`'s success (already retained via N3, since
    /// `refresh_author` runs on every relevant mutation), a chain-valid-but-not-yet-
    /// `DirectPub` prefix (e.g. payload still missing somewhere up the chain) is not
    /// otherwise retained anywhere, so this query must retain it itself.
    pub fn holds_prefix(&mut self, r: &BlockRef) -> bool {
        if !self.exact_coordinate(r) {
            return false;
        }
        let verified = {
            let mut blocks = self.blocks.lock().unwrap();
            blocks.verified_prefix_through_genesis(
                &self.committee,
                &self.sid,
                self.max_block_payload,
                &self.genesis,
                &r.2,
            )
        };
        if verified {
            self.retain_prefix(r);
        }
        verified
    }

    pub fn c_candidate(&self, author: &PublicKey) -> Option<BlockRef> {
        self.c_candidate.get(author).cloned()
    }

    pub fn t_candidate(&self, author: &PublicKey) -> Option<BlockRef> {
        self.t_candidate.get(author).cloned()
    }

    fn record_direct_pub(&mut self, r: &BlockRef) {
        self.direct_pub_refs.insert(r.clone());
        if self.is_q_available(r, self.committee.quorum_threshold()) {
            self.quorum_direct_refs.insert(r.clone());
        }
        // Ack-watermark front-end (see `own_avail_watermark`'s doc comment): advance
        // this author's own DIRECT-PREFIX watermark. Absent a Byzantine fork,
        // DirectPub confirmations for a fixed author are always recorded in strictly
        // consecutive height order: `direct_prefix_ok`'s own chain walk requires every
        // ancestor's `direct && payload_ok` flags, which -- by the same recursive
        // argument applied to the ancestor -- means the ancestor's own DirectPub
        // confirmation was already recorded first (this method is only ever reached
        // from `refresh_author`'s ascending-`BTreeSet` range walk over `pending_direct`
        // for one author, via `on_direct_pub_confirmed`). Tracking the greatest seen
        // height regardless of exact contiguity (`>`, not `== + 1`) is a defensive
        // choice under an equivocating author who gets two different DirectPub digests
        // recorded at the SAME height at this party (last-recorded wins): harmless
        // liveness-only degradation, never a soundness issue, since the RECEIVE side
        // (`resolve_watermark`) always re-verifies the exact digest chain before
        // crediting anything -- this value is only ever a candidate to ADVERTISE, not
        // something trusted on its own.
        let advances = match self.own_avail_watermark.get(&r.0) {
            Some((h, _)) => r.1 > *h,
            None => true,
        };
        if advances {
            self.own_avail_watermark.insert(r.0, (r.1, r.2.clone()));
            self.avail_dirty = true;
        }
    }

    /// Full-vector-when-dirty ack-watermark flush (flag-gated at the core level -- see
    /// `Parameters::ack_watermarks`): if this party's own watermark has advanced since
    /// the last flush, clear the dirty bit and return the FULL current vector (every
    /// author with a recorded watermark); else `None`. Deliberately not a delta: the
    /// result is monotone and idempotent on the receive side (`resolve_watermark`'s own
    /// credited floor silently re-ignores an already-covered or stale entry), so a
    /// duplicate or slightly-stale full vector is always harmless, and an idle lane
    /// (nothing new to report) goes silent instead of re-broadcasting forever.
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

    /// Resolves a peer's ack-watermark vector into the exact `BlockRef`s this party's
    /// own cache lets it credit -- see this module's own header comment for why
    /// crediting must always resolve to a specific `(author, height, digest)` before
    /// touching `AckAggregator`: a height-only credit would let an equivocating
    /// author's fork reach quorum with zero correct holders of the counted digest.
    /// `sender` is the watermark's declaring party -- D4-trusted the same way an
    /// `Ack::sender` already is; the caller (`VantageCore`/`SimpleItCore::
    /// dispatch_inbound`) has already checked committee membership before reaching
    /// here.
    pub fn resolve_watermark(&mut self, sender: PublicKey, entries: &[AvailEntry]) -> Vec<BlockRef> {
        let mut refs = Vec::new();
        for entry in entries {
            refs.extend(self.resolve_one(sender, entry));
        }
        refs
    }

    /// Re-attempt every `(sender, author)` watermark entry pending on `digest`'s
    /// author, now that `digest` has just been cached -- hook: both cores'
    /// `Effect::BlockCached` handling (mirroring `Repairer::on_block_available`'s
    /// identical "retry on new cache content" role for repair, and `refresh_author`'s
    /// own retry-on-new-content role for direct acks). Returns `(sender, ref)` pairs so
    /// the caller can credit each through the shared aggregator under the correct
    /// declaring sender.
    pub fn retry_pending_avail(&mut self, digest: &Digest) -> Vec<(PublicKey, BlockRef)> {
        let author = {
            let blocks = self.blocks.lock().unwrap();
            blocks.get(digest).map(|e| e.block.author)
        };
        let Some(author) = author else {
            return Vec::new();
        };
        let keys: Vec<(PublicKey, PublicKey)> = self
            .pending_avail
            .keys()
            .filter(|(_, a)| *a == author)
            .cloned()
            .collect();
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

    /// Per-entry core of `resolve_watermark`/`retry_pending_avail`: resolves `entry`
    /// against `sender`'s current credited floor for `entry.author`.
    ///
    /// Monotone: an entry whose DECLARED height is at or below the floor is ignored --
    /// pure liveness (a stale/duplicate resend costs nothing).
    ///
    /// On a successful resolve, the credited refs and the new floor are derived from
    /// the WALK's own result (`BlockCache::collect_verified_suffix`, which re-derives
    /// every height from the ACTUAL cached chain, never from the caller's declared
    /// `entry.height`) -- so a lying declared height can never advance the floor past
    /// what was genuinely verified; it can only make this call a harmless no-op or
    /// degrade to the head-alone case below.
    ///
    /// On failure (the segment below the head does not fully resolve -- including the
    /// head itself not being cached at all, per `collect_verified_suffix`'s own
    /// contract), the head ref alone is credited, EXACTLY as a direct ack for that
    /// exact `(author, height, digest)` tuple would be (the declaring party attests
    /// holding it), using the caller's DECLARED `entry.height` -- the same trust model
    /// `Ack::reference` already applies to a wire-declared height (see `messages::Ack`'s
    /// doc comment): if the declared height doesn't match the digest's real cached
    /// height, the resulting ref is simply inert downstream (`exact_coordinate`'s
    /// pinned-height check rejects it the same way a wrong-height Ack already would),
    /// never unsound. The floor is NOT advanced past what's already recorded, and the
    /// entry is stashed (latest-wins, keyed `(sender, author)`) for
    /// `retry_pending_avail` to retry later.
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
            let blocks = self.blocks.lock().unwrap();
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
                    refs.push((entry.author, floor_height + 1 + i as Height, d.clone()));
                }
                if let Some(last) = suffix.last() {
                    self.credited_floor.insert(
                        key,
                        (floor_height + suffix.len() as Height, last.clone()),
                    );
                }
                self.pending_avail.remove(&key);
                refs
            }
            None => {
                self.pending_avail.insert(key, entry.clone());
                vec![(entry.author, entry.height, entry.head.clone())]
            }
        }
    }

    /// N5: refresh both registers for `author` from the monotone direct/quorum indexes.
    /// This preserves `newest` (greatest height, smallest digest tie-break) without
    /// walking every historical block ref on each ack.
    fn refresh_registers(&mut self, author: PublicKey) {
        let c = newest_indexed(&self.quorum_direct_refs, author);
        set_candidate(&mut self.c_candidate, author, c.clone());

        let t = self.newest_t_candidate(author, c.as_ref());
        set_candidate(&mut self.t_candidate, author, t);
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
                // Reverse BTree iteration sees larger digests first for a fixed height.
                // Overwrite through the height group so the smallest digest wins ties.
                best_at_height = Some(r.clone());
            }
        }
        best_at_height
    }

    /// Does `r`'s ancestor chain pass through `target`'s exact (height, digest) as a
    /// (non-strict-here; strictness is enforced by the caller's `r.1 > c_ref.1`) ancestor?
    /// Also realizes the fork rule: a branch that never passes through `target` (e.g. a
    /// sibling fork) returns `false`, so it can never be picked as T against that C.
    /// Height-bounded walk (see `direct_prefix_ok`'s doc comment): tracks `r`'s own
    /// height, decrementing by exactly one per step and rejecting any mismatch against
    /// the block actually found there, so an adversarial reference cycle can only ever
    /// cost `r.1` iterations, never hang.
    ///
    /// `pub` (PHASE4-SPEC.md §5): the AGB engine's R2 tip-anchoring check
    /// (`TipOK_i(C,T)`) needs this same "does r's chain pass through target" query
    /// against arbitrary manifest entries from *received* proposals, not just this
    /// node's own N5 registers -- extended in place per the reuse rule rather than
    /// duplicated.
    pub fn prefix_contains(&self, r: &BlockRef, target: &BlockRef) -> bool {
        let blocks = self.blocks.lock().unwrap();
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
                return false; // defense-in-depth: enforce "one author" here too (§1)
            }
            if entry.block.height != expected_height {
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
