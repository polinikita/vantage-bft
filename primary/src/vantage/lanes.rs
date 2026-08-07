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
use crate::vantage::claim::{manifest_refs, AvailClaim};
use crate::vantage::Effect;
use config::{Committee, Stake, WorkerId};
use crypto::{Digest, PublicKey};
use metrics::{Metrics, UtilizationTimer};
use parking_lot::Mutex;
use prometheus::IntCounter;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
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

    /// Entries held, exported as `vantage_block_cache_len`. Measured at 4,286 bytes of
    /// per-node RSS growth per entry, growing at exactly the committee's block rate
    /// (`local-dryrun/rss-growth.sh`), which is why `evict_author_below` exists.
    pub fn len(&self) -> usize {
        self.by_digest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_digest.is_empty()
    }

    /// Drop every cached block of `author` strictly below `height`. Returns how many
    /// entries went away.
    ///
    /// MEMORY (2026-08-07): this type kept "every block this node has ever obtained" with no
    /// eviction of any kind, which after the `AckAggregator` retirement was the dominant
    /// remaining leak -- 2.504 MB/s/node at n=30, ~4,286 B per entry, with `len()` climbing
    /// at exactly the committee block rate (584/s = 30 nodes x 20 blocks/s at a 50ms header
    /// delay). Extrapolated to n=100 that is OOM against 8 GiB in roughly 8-10 minutes.
    ///
    /// SAFETY IS THE CALLER'S: this method is a mechanical delete and will happily drop a
    /// block the node still needs. `height` must be a floor below which no correct party --
    /// local or remote -- can still require the data. `LaneManager::evict_universally_held`
    /// derives it from "every peer has confirmed holding this lane at or above h", which
    /// makes serving requests below it unnecessary by construction. Do not call this with a
    /// floor derived from local progress alone: N8 forbids discarding retained blocks
    /// precisely because peers may still ask for them, and starving a peer's repair is the
    /// failure this whole line of work exists to fix.
    pub fn evict_author_below(&mut self, author: &PublicKey, height: Height) -> usize {
        let Some(by_height) = self.by_author.get_mut(author) else {
            return 0;
        };
        // `split_off` keeps `height..` in place and hands back everything below it, the same
        // GC discipline `AgbEngine`/`ControlLog` use -- no `retain`, no full-map scan.
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

    /// Mechanism A (sender-side lane resume, `vantage::resume`): this author's own
    /// cached block at exactly `height`, if any. An author's own lane carries at
    /// most one digest per height absent a Byzantine fork (`upsert`'s own doc
    /// comment on fork representability); the first indexed digest is taken. Every
    /// call site (`LaneManager::author_block_at`, reached only from
    /// `VantageCore`/`SimpleItCore`'s `Inbound::LaneResume` handling after already
    /// checking `author == self.name`) only ever asks about ITS OWN lane, which this
    /// protocol never lets fork at the party serving it.
    pub fn author_block_at(&self, author: &PublicKey, height: Height) -> Option<&Header> {
        let digest = self.by_author.get(author)?.get(&height)?.iter().next()?;
        self.by_digest.get(digest).map(|e| &e.block)
    }

    /// Mechanism A: the smallest height this party holds ANY cached block for
    /// `author` at. Currently always `Some(1)` once `author` has published anything
    /// -- N8 retention never discards (`BlockEntry::retained`'s doc comment: "must be
    /// held + served forever once set") -- and `None` before that; kept as a real
    /// query over `by_author`'s own index (not a hardcoded `1`) so the resume-serve
    /// clamp this feeds (`LaneManager::earliest_authored_height`) stays correct if
    /// height-based eviction is ever added to this cache.
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
            // NOTE (2026-08-07): unlike `collect_verified_suffix` and
            // `verified_prefix_through_genesis`, this walk re-runs `block_ok` rather than
            // consulting `BlockEntry::block_ok_verified`. Left as-is deliberately: this
            // function has NO production callers -- `expand`/the gate amendment replaced it
            // with the suffix/prefix walks -- so memoizing it would be a change to dead code
            // and would imply a hot-path win that does not exist.
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
    /// Per-ref first-hand sender dedup set. RETIRED once the ref reaches `Quorum` -- see
    /// `record_ack`'s doc comment for the 13.4 MB/s leak that made this necessary.
    senders: HashMap<BlockRef, HashSet<PublicKey>>,
    /// Per-ref accumulated stake. Retired alongside `senders`, for the same reason.
    weights: HashMap<BlockRef, Stake>,
    /// The highest threshold already emitted per ref. Outlives `senders`/`weights` and
    /// doubles as the "retired" marker: `Quorum` here means the other two maps have been
    /// dropped on purpose, not that they were never populated.
    ///
    /// STILL UNBOUNDED, deliberately: one entry is ~73 B against `senders`' ~4.2 KB, so
    /// retiring the other two cuts growth ~59x (13.4 -> ~0.23 MB/s), but this map alone
    /// still grows about 0.8 GB/hour at n=100. Bounding it needs a safe floor below which
    /// a re-emitted availability mark is known harmless, which is the same policy question
    /// as `Repairer`'s GC floor -- not decidable here.
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

    /// Record a first-hand ack and report whether it crossed a new availability threshold.
    ///
    /// MEMORY (2026-08-07): once a ref reaches `Quorum` -- the top threshold -- no further
    /// ack for it can change any output, so its `senders` set and `weights` entry are dead
    /// and are dropped. Keeping them was the dominant memory leak at n=100: measured RSS
    /// growth of 13.43 MB/s per node, 1.19 -> 2.73 GiB across a 123s window, i.e. OOM
    /// against an 8 GiB box in about 7 minutes. The run only survived because it was short.
    ///
    /// The arithmetic, from that run's own counters: 403,418 blocks received x ~100 senders
    /// each = 40.3M acks (`vantage_avail_credited_refs` read 39.3M), and `senders` held one
    /// `HashSet<PublicKey>` of ~97 entries -- about 4.2 KB with capacity rounding -- per
    /// block ever seen, forever. 403,418 x 4.3 KB = 1.73 GB, or 14.1 MB/s over the window.
    ///
    /// Correctness of the drop: a later ack for a retired ref is short-circuited by the
    /// `emitted == Quorum` check below, BEFORE `senders` is touched, so it neither
    /// re-allocates the set nor re-counts stake nor re-emits a mark. Without that check the
    /// drop would be a bug -- the set would be recreated by the very next ack and the
    /// weight would restart from one sender's stake.
    pub fn record_ack(&mut self, sender: PublicKey, reference: BlockRef) -> AckAggregationResult {
        if !self.members.contains(&sender) {
            return AckAggregationResult {
                accepted: false,
                availability: None,
            };
        }
        // Retired: `Quorum` is terminal, so nothing this ack could do would be observable.
        // Must precede every `senders`/`weights` access -- see the doc comment above.
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
            // Terminal threshold reached: retire the per-ref working state. The `emitted`
            // entry left behind is what makes every subsequent ack a single hash lookup.
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

    /// Live per-ref working-state size (`senders`), exported as
    /// `vantage_ack_senders_tracked` so the retirement above is observable: it should sit
    /// near the count of refs still BELOW quorum, not grow with every block ever seen.
    pub fn senders_tracked(&self) -> usize {
        self.senders.len()
    }

    /// Refs that have reached a threshold and been retired (`emitted`). Exported as
    /// `vantage_ack_refs_retired` -- still unbounded by design, see the field's comment,
    /// so this is the series that shows the remaining ~0.23 MB/s.
    pub fn refs_retired(&self) -> usize {
        self.emitted.len()
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
    /// MOVED (2026-08-07): watermark resolution now lives in `avail::AvailResolver`, so it
    /// can be lifted off the core thread -- it consumes ~190k per-(sender, author, height)
    /// facts per second at n=100 and emits only ~4k monotone threshold marks, a ~47x funnel.
    /// Held here for now so the call sites and tests are unchanged; the threading move is the
    /// next step. See that module's header for the measurements.
    avail: crate::vantage::avail::AvailResolver,
    /// Per (sender, author) latest-wins pending slot: a watermark entry whose head
    /// resolved (attested, credited) but whose segment below the head did not fully
    /// resolve locally yet -- retried by `retry_pending_avail` once a new block is
    /// cached. Bounded by O(n^2), same as `credited_floor`, no GC needed.
    /// n=100 straggler fix (2026-08-08): `author -> senders with a stashed entry for that
    /// author`. `retry_pending_avail` used to scan ALL of `pending_avail` and filter by
    /// author on every newly cached block; this makes it O(senders waiting on THIS
    /// author). The same un-indexed-sweep shape as `Repairer::on_block_available`'s, in
    /// a second place -- measured 426M scan iterations on an n=100 straggler. Kept as a
    /// strict mirror of `pending_avail`'s key set: every insert/remove below updates
    /// both, and an emptied bucket is dropped so the index cannot leak authors.

    /// Mechanism A (sender-side lane resume, `vantage::resume`) requester-side
    /// trigger input: per-author max height that has reached at least an
    /// (f+1)-availability mark (`AckThreshold::Validity`), maintained in
    /// `process_ack_availability` -- the SAME mark-consumption site
    /// `ack_availability`/`quorum_direct_refs` are already updated from, just also
    /// tracking the plain running max height per author. Deliberately NOT
    /// necessarily a contiguous prefix (an ack-availability mark is per EXACT tuple,
    /// so a higher height can cross the threshold before a lower one does under
    /// network asynchrony) -- this is exactly `avail(a)` in the design doc, "the
    /// highest height with an (f+1)-availability mark for lane a", compared against
    /// `own_avail_watermark`'s own CONTIGUOUS frontier below to detect a gap.
    /// Bounded by committee size, same as `own_avail_watermark`, no GC needed.
    avail_watermark_high: HashMap<PublicKey, Height>,

    /// §6.4 counters; `None` in most unit tests, which don't assert on metrics.
    metrics: Option<Arc<Metrics>>,

    /// Cached `core_wait_timer{proc="store_probe"}` handle -- resolved on first use,
    /// then reused, exactly like `VantageCore::cached_utilization_timer`'s caches.
    /// Times `missing_payload`'s single store round-trip, the one genuine block on
    /// the VantageCore thread inside this module.
    wt_store_probe: Option<IntCounter>,
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
        // Built before the struct literal: it borrows the same committee/sid/genesis/blocks
        // the literal then moves.
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
            direct_pub_refs: BTreeSet::new(),
            quorum_direct_refs: BTreeSet::new(),
            c_candidate: HashMap::new(),
            t_candidate: HashMap::new(),
            own_frontier: (0, genesis),
            own_avail_watermark: HashMap::new(),
            avail_dirty: false,
            avail,
            avail_watermark_high: HashMap::new(),
            metrics: None,
            wt_store_probe: None,
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

    /// Shared block-cache size, for `vantage_block_cache_len`. Takes the same lock the
    /// hot paths do, so it is sampled once per second from `sample_metrics`, never per
    /// message.
    pub fn block_cache_len(&self) -> usize {
        self.blocks.lock().len()
    }

    /// Evict cached blocks of `author` that every peer has confirmed holding, keeping the
    /// most recent `keep` heights as slack. Returns entries dropped.
    ///
    /// `floor` comes from `Repairer::universally_held_below`, i.e. "all n-1 peers have
    /// credited this lane at or above this height", which is what makes the drop safe --
    /// see `BlockCache::evict_author_below` for why the caller owns that argument. `keep`
    /// exists because the floor is derived from credits that can lag the node's own
    /// position slightly; it costs a few blocks per lane and removes any dependence on
    /// credit timing.
    pub fn evict_universally_held(
        &mut self,
        author: &PublicKey,
        floor: Height,
        keep: Height,
    ) -> usize {
        let cut = floor.saturating_sub(keep);
        if cut == 0 {
            return 0;
        }
        self.blocks.lock().evict_author_below(author, cut)
    }

    pub fn blocks_handle(&self) -> SharedBlocks {
        self.blocks.clone()
    }

    /// The `pending_avail` index's own key set, for the test that pins it as a strict
    /// mirror of `pending_avail`. A drifted index would silently stop retrying a stashed
    /// avail entry -- the sender's watermark would then never resolve.
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
        let missing_payload = self.missing_payload(&header).await;
        let payload_ok = missing_payload.is_empty();
        let digest = header.id.clone();

        {
            let mut blocks = self.blocks.lock();
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

        if direct && !missing_payload.is_empty() {
            effects.push(Effect::SyncBatches(
                header.author,
                digest.clone(),
                missing_payload,
            ));
        }

        effects.extend(self.refresh_author(header.author));
        effects
    }

    /// Returns the payload entries not present in the local worker store, using the
    /// exact key shape `synchronizer::Synchronizer::missing_payload`/
    /// `payload_receiver::PayloadReceiver` use (`[digest || worker_id LE]`, written
    /// on `OthersBatch`). We don't store the payload of our own workers under that key
    /// (mirroring `missing_payload`'s early return for `header.author == self.name`) --
    /// our own blocks are always payload-ready since the `OurBatch` digests we proposed
    /// with are, by construction, digests our own workers already sealed.
    pub(crate) async fn missing_payload(&mut self, header: &Header) -> Vec<(Digest, WorkerId)> {
        if header.author == self.name {
            return Vec::new();
        }
        // ONE store round-trip for the whole payload, not one per entry. The previous
        // per-entry `store.read(..).await` serialized up to `max_block_payload` (16)
        // round-trips through the single store actor -- on the VantageCore thread, for
        // every inbound and every served block -- each queued behind the batch-write
        // stream sharing that channel. `read_many` preserves input order, so results
        // zip 1:1 with `header.payload`.
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
            // `found.len() == keys.len()` by construction; `unwrap_or(true)` is a
            // defensive "treat as missing", never taken.
            .filter(|(i, _)| found.get(*i).map(Option::is_none).unwrap_or(true))
            .map(|(_, (digest, worker_id))| ((*digest).clone(), **worker_id))
            .collect()
    }

    /// Call once a previously-missing block's worker batches have arrived (production:
    /// after `store.notify_read` resolves following a `SyncBatches` effect; tests:
    /// after writing the payload marker directly). Re-runs the N3 ack check.
    pub fn set_payload_ready(&mut self, digest: &Digest) -> Vec<Effect> {
        let direct_ready = {
            let mut blocks = self.blocks.lock();
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
            let mut blocks = self.blocks.lock();
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
        // Tell the resolver, so it can stop crediting this ref once the threshold is
        // terminal. `Quorum` admits no further change, and all n senders credit the same
        // block while only the first 2f+1 matter -- plus `retry_pending_avail` re-credits a
        // stuck head ref once per arriving block until this cuts it off. The resolver keeps
        // its own set rather than reading `ack_availability`, so it stays independent of core
        // state and can move off the core thread.
        self.avail.note_threshold(&r, threshold);
        // Mechanism A (`vantage::resume`): every mark reaching this point is, by
        // construction, at least `Validity` (f+1) -- `AckAggregator::record_ack`
        // never emits anything weaker -- so this is unconditional, not gated on
        // `threshold`. A running max, not an insert-if-absent: marks for the same
        // author can arrive out of height order (this is a per-EXACT-tuple fact, not
        // a prefix), so only a genuinely higher height may advance it.
        let high = self.avail_watermark_high.entry(r.0).or_insert(0);
        if r.1 > *high {
            *high = r.1;
        }
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
        let blocks = self.blocks.lock();
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
        let mut blocks = self.blocks.lock();
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
            let mut blocks = self.blocks.lock();
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

    /// Mechanism A (`vantage::resume`) requester-side trigger input: `frontier(a)` in
    /// the design doc -- this party's own held CONTIGUOUS direct-verified prefix
    /// height for lane `author`. Reuses `own_avail_watermark` -- the ack-watermark
    /// front-end's identical per-author bookkeeping, unconditional regardless of
    /// `Parameters::ack_watermarks` (see that field's own doc comment) -- rather than
    /// duplicating a second frontier tracker. `0` (genesis) if nothing of `author`'s
    /// has ever confirmed DirectPub at this party.
    pub fn own_direct_frontier(&self, author: &PublicKey) -> Height {
        self.own_avail_watermark
            .get(author)
            .map(|(h, _)| *h)
            .unwrap_or(0)
    }

    /// AVAIL-ECHO-SPEC.md step 3: this party's own availability claims against
    /// `proposal`, for piggybacking on the AGB echo it is about to send.
    ///
    /// One pass over `manifest_refs`, reading only `own_avail_watermark` (the contiguous
    /// DirectPub prefix per author, maintained unconditionally -- see
    /// `own_direct_frontier`) plus one `BlockCache` lookup per at-tip candidate. That
    /// makes it O(|refs|) with no chain walk, against `resolve_watermark`'s
    /// `collect_verified_suffix` per entry -- the whole point is that the sender already
    /// knows what it holds.
    ///
    /// Three outcomes per lane, exactly as the spec's §4 states:
    ///   - own watermark reaches the named height AND the named digest is held, verified,
    ///     at that exact coordinate  => at-tip bit. Holding the named digest is what makes
    ///     this a claim about the RIGHT chain; an author that forked has two digests at one
    ///     height and only one of them is the proposal's.
    ///   - own watermark is short but nonzero => `ShortClaim` at our own frontier, carrying
    ///     OUR head digest. We cannot prove here that our prefix lies on `chain(named)`
    ///     without holding that chain, so the receiver re-checks the linkage against its
    ///     own copy (`AvailResolver::note_claim`). Emitting it is what keeps the ack rate
    ///     from collapsing under ragged frontiers.
    ///   - nothing held for that author => no claim. Silence is not a negative
    ///     acknowledgment; it just carries no information.
    ///
    /// Claiming ABOVE the named height is deliberately inexpressible: there is no digest
    /// in the proposal to anchor it, and availability only ever has to certify what a
    /// proposal names (spec §3, consequence 2).
    pub fn build_avail_claim(&self, proposal: &crate::vantage::agb::ViewProposal) -> AvailClaim {
        let refs = manifest_refs(proposal);
        let mut claim = AvailClaim::with_capacity(refs.len());
        let blocks = self.blocks.lock();
        for (j, r) in refs.iter().enumerate() {
            let (author, height, digest) = (r.0, r.1, &r.2);
            let Some((own_h, own_head)) = self.own_avail_watermark.get(&author) else {
                continue;
            };
            if *own_h >= height {
                let holds_named = blocks.get(digest).is_some_and(|e| {
                    e.block.author == author
                        && e.block.height == height
                        && e.block.id == *digest
                        && e.block_ok_verified
                });
                if holds_named {
                    claim.set_at_tip(j);
                }
                // else: our lane for this author is a DIFFERENT chain at that height (the
                // author equivocated). Claiming either endpoint would be a claim about a
                // chain we do not hold, so we say nothing.
            } else if *own_h > 0 {
                claim.push_short(j, height - *own_h, own_head.clone());
            }
        }
        claim
    }

    /// Mechanism A requester-side trigger input: `avail(a)` in the design doc -- see
    /// `avail_watermark_high`'s own doc comment. `0` if no (f+1) mark has ever been
    /// recorded for `author`.
    pub fn avail_high(&self, author: &PublicKey) -> Height {
        self.avail_watermark_high.get(author).copied().unwrap_or(0)
    }

    /// Mechanism A serve-side upper bound: this party's own current lane tip height
    /// (`own_frontier`'s pre-existing role for `publish_own`). Only meaningful as
    /// "the requested lane's own tip" when `self.name` IS that lane's author --
    /// `VantageCore`/`SimpleItCore`'s `Inbound::LaneResume` handling only ever calls
    /// this after already checking exactly that.
    pub fn own_tip_height(&self) -> Height {
        self.own_frontier.0
    }

    /// Mechanism A serve-side clamp floor, delegating to `BlockCache::
    /// earliest_height`; `1` (the lowest real block height -- height 0 is the
    /// implicit, never-transmitted genesis) when nothing has been cached for
    /// `author` yet, so a request naming `from <= 1` (or `0`) clamps up to the
    /// earliest block that could ever legitimately be served.
    pub fn earliest_authored_height(&self, author: &PublicKey) -> Height {
        self.blocks.lock().earliest_height(author).unwrap_or(1)
    }

    /// Mechanism A serve-side lookup, delegating to `BlockCache::author_block_at`.
    pub fn author_block_at(&self, author: &PublicKey, height: Height) -> Option<Header> {
        self.blocks.lock().author_block_at(author, height).cloned()
    }

    /// Mechanism A receipt-continuation: the cached author of `digest`'s block, if
    /// held. `on_payload_ready`'s delayed DirectPub transition (a header whose bytes
    /// already arrived but whose payload was still syncing) is keyed by header
    /// digest alone, unlike `Inbound::Publish`'s direct access to `header.author` --
    /// this is the one extra lookup that lets the SAME "did frontier(author) just
    /// advance" continuation check run at that call site too.
    pub fn author_of(&self, digest: &Digest) -> Option<PublicKey> {
        self.blocks.lock().get(digest).map(|e| e.block.author)
    }

    /// Resolves a peer's ack-watermark vector into the exact `BlockRef`s this party's
    /// own cache lets it credit -- see this module's own header comment for why
    /// crediting must always resolve to a specific `(author, height, digest)` before
    /// touching `AckAggregator`: a height-only credit would let an equivocating
    /// author's fork reach quorum with zero correct holders of the counted digest.
    /// `sender` is the watermark's declaring party -- D4-trusted the same way an
    /// `Ack::sender` already is; the caller (`VantageCore`/`SimpleItCore::
    /// dispatch_inbound`) has already checked committee membership before reaching
    /// N5 ack-watermark front-end. Delegates to `avail::AvailResolver` -- see that module
    /// for why resolution is a separate type (it is a ~47x funnel and belongs off the core
    /// thread).
    pub fn resolve_watermark(
        &mut self,
        sender: PublicKey,
        entries: &[AvailEntry],
    ) -> Vec<BlockRef> {
        self.avail.resolve_watermark(sender, entries)
    }

    /// Re-attempt every `(sender, author)` watermark entry pending on `digest`'s author.
    pub fn retry_pending_avail(&mut self, digest: &Digest) -> Vec<(PublicKey, BlockRef)> {
        self.avail.retry_pending_avail(digest)
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
