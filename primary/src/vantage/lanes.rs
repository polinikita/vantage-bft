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
use std::collections::{BTreeMap, HashMap, HashSet};
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
    /// flags. `direct`/`repaired`/`payload_ok` are OR-merged (N2: "a later publish may
    /// upgrade bytes previously cached via repair"; all flags besides the block body
    /// itself are monotonic/sticky, matching N8's "no discard").
    pub fn upsert(&mut self, block: Header, direct: bool, repaired: bool, payload_ok: bool) {
        let digest = block.id.clone();
        let author = block.author;
        let height = block.height;
        self.by_author
            .entry(author)
            .or_default()
            .entry(height)
            .or_default()
            .insert(digest.clone());
        let entry = self.by_digest.entry(digest).or_insert_with(|| BlockEntry {
            block: block.clone(),
            direct: false,
            repaired: false,
            retained: false,
            payload_ok: false,
            direct_prefix_verified: false,
            chain_verified: false,
        });
        entry.block = block;
        entry.direct |= direct;
        entry.repaired |= repaired;
        entry.payload_ok |= payload_ok;
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
                    .flat_map(|(height, digests)| digests.iter().map(move |d| (*author, *height, d.clone())))
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
            if entry.block.author != author {
                return false; // cross-author graft (§1 "one author index")
            }
            if entry.block.height != expected_height {
                return false;
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
    pub fn verified_prefix_through_genesis(
        &mut self,
        committee: &Committee,
        sid: &Digest,
        max_block_payload: usize,
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
            if entry.block.author != author {
                return false; // cross-author graft (§1 "one author index")
            }
            if entry.block.height != expected_height {
                return false;
            }
            if !block_ok(&entry.block, committee, sid, max_block_payload) {
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
    /// `verified_prefix_through_genesis` is now a thin wrapper over this (no logic
    /// duplication between the two).
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
            if entry.block.author != author {
                return None; // cross-author graft (§1 "one author index")
            }
            if entry.block.height != expected_height {
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
    pub fn collect_verified_suffix(
        &self,
        committee: &Committee,
        sid: &Digest,
        max_block_payload: usize,
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
            if entry.block.author != author {
                return None;
            }
            if entry.block.height != expected_height {
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
}

pub type SharedBlocks = Arc<Mutex<BlockCache>>;

/// Greatest height, ties broken by lexicographically smallest digest (§2 N5 "newest").
fn newest(refs: Vec<BlockRef>) -> Option<BlockRef> {
    refs.into_iter().max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.2.cmp(&a.2)))
}

pub struct LaneManager {
    name: PublicKey,
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    store: Store,
    blocks: SharedBlocks,

    /// N4: first-hand ack senders per exact tuple.
    ack_senders: HashMap<BlockRef, HashSet<PublicKey>>,
    /// N3: tuples we have already broadcast our own ack for (at most once, ever).
    acked: HashSet<BlockRef>,

    /// N5 registers.
    c_candidate: HashMap<PublicKey, BlockRef>,
    t_candidate: HashMap<PublicKey, BlockRef>,

    /// Our own lane frontier: (height, digest of that block, or genesis at height 0).
    own_frontier: (Height, Digest),

    /// §6.4 counters; `None` in most unit tests, which don't assert on metrics.
    metrics: Option<Arc<Metrics>>,
}

impl LaneManager {
    pub fn new(name: PublicKey, committee: Committee, max_block_payload: usize, store: Store) -> Self {
        Self::with_shared_blocks(name, committee, max_block_payload, store, Arc::new(Mutex::new(BlockCache::new())))
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
            ack_senders: HashMap::new(),
            acked: HashSet::new(),
            c_candidate: HashMap::new(),
            t_candidate: HashMap::new(),
            own_frontier: (0, genesis),
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
    /// process our own block as a direct publication and count our own ack.
    pub async fn publish_own(&mut self, payload: BTreeMap<Digest, WorkerId>) -> (Header, Vec<Effect>) {
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
            blocks.upsert(header.clone(), direct, false, payload_ok);
        }
        effects.push(Effect::BlockCached(digest.clone()));
        if header.author != self.name {
            if let Some(metrics) = &self.metrics {
                metrics.vantage_blocks_received.inc();
            }
        }

        if direct && !payload_ok {
            let missing: Vec<(Digest, WorkerId)> = header.payload.iter().map(|(d, w)| (d.clone(), *w)).collect();
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
        let author = {
            let mut blocks = self.blocks.lock().unwrap();
            blocks.set_payload_ok(digest, true);
            blocks.get(digest).map(|e| e.block.author)
        };
        match author {
            Some(author) => self.refresh_author(author),
            None => Vec::new(),
        }
    }

    /// Re-run the N3 ack trigger and N5 registers over every known tuple of `author`.
    /// Deterministic and idempotent: `acked`/registers only ever grow/replace with a
    /// "newer" (§2 N5) reference, never regress.
    fn refresh_author(&mut self, author: PublicKey) -> Vec<Effect> {
        let mut effects = Vec::new();
        let refs: Vec<BlockRef> = {
            let blocks = self.blocks.lock().unwrap();
            blocks.author_refs(&author)
        };
        for r in refs {
            if !self.acked.contains(&r) && self.direct_pub(&r) {
                self.on_direct_pub_confirmed(&r, &mut effects);
            }
        }
        self.recompute_registers(author);
        effects
    }

    /// Note: does *not* itself call `recompute_registers` -- `refresh_author` (the only
    /// caller) always does so once, after this loop, covering every tuple confirmed in
    /// the same pass.
    fn on_direct_pub_confirmed(&mut self, r: &BlockRef, effects: &mut Vec<Effect>) {
        // N8(i): retain the whole valid lane prefix through h.
        self.retain_prefix(r);
        self.acked.insert(r.clone());
        let ack = Ack::new(r.0, r.1, r.2.clone(), self.name);
        effects.push(Effect::BroadcastAck(ack));
        if let Some(metrics) = &self.metrics {
            metrics.vantage_acks_sent.inc();
        }
        // N1: self-delivery counts -- record our own ack immediately.
        self.record_ack(self.name, r.clone());
    }

    /// Callers only ever reach this after `verified_prefix_through_genesis` (which
    /// bounds itself by strictly-decreasing expected height) already proved the chain
    /// from `r` to genesis is real and cycle-free; this walk still tracks expected
    /// height itself too, defensively, so it can never hang even if that invariant were
    /// ever violated by a future change.
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
                let next = entry.block.parent_cert.header_digest.clone();
                let size = bincode::serialized_size(&entry.block).unwrap_or(0);
                if blocks.mark_retained(&cur) {
                    newly_retained_bytes += size;
                }
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

    /// N4: count a first-hand ack. Callers must have already confirmed `sender` is the
    /// message's own channel sender (network dispatch, not re-derived here) and that
    /// the enclosing session is ours -- `Ack` itself carries no `sid` (§6.1 D5), acks
    /// bind the session transitively through the digest, which folds `sid`.
    pub fn process_ack(&mut self, sender: PublicKey, r: BlockRef) -> Vec<Effect> {
        self.record_ack(sender, r.clone());
        if let Some(metrics) = &self.metrics {
            metrics.vantage_acks_received.inc();
        }
        self.recompute_registers(r.0);
        Vec::new()
    }

    fn record_ack(&mut self, sender: PublicKey, r: BlockRef) {
        self.ack_senders.entry(r).or_default().insert(sender);
    }

    pub fn ack_stake(&self, r: &BlockRef) -> Stake {
        self.ack_senders
            .get(r)
            .map(|senders| senders.iter().map(|pk| self.committee.stake(pk)).sum())
            .unwrap_or(0)
    }

    /// §4 query: `is_q_available(ref, q)`, `q` typically `committee.validity_threshold()`
    /// (f+1) or `committee.quorum_threshold()` (2f+1).
    pub fn is_q_available(&self, r: &BlockRef, q: Stake) -> bool {
        self.ack_stake(r) >= q
    }

    fn exact_coordinate(&self, r: &BlockRef) -> bool {
        let blocks = self.blocks.lock().unwrap();
        blocks.get(&r.2).map_or(false, |e| e.block.author == r.0 && e.block.height == r.1)
    }

    /// `DirectPub_i(a,k,h)` (§1 D1 / §2 N1-N3): whole prefix is both chain-valid
    /// (`BlockOK` all through) and directly-published-with-payload all through.
    pub fn direct_pub(&self, r: &BlockRef) -> bool {
        if !self.exact_coordinate(r) {
            return false;
        }
        let mut blocks = self.blocks.lock().unwrap();
        blocks.verified_prefix_through_genesis(&self.committee, &self.sid, self.max_block_payload, &self.genesis, &r.2)
            && blocks.direct_prefix_ok(&self.genesis, &r.2)
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
            blocks.verified_prefix_through_genesis(&self.committee, &self.sid, self.max_block_payload, &self.genesis, &r.2)
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

    /// N5: recompute both registers for `author` from current first-hand state.
    fn recompute_registers(&mut self, author: PublicKey) {
        let refs: Vec<BlockRef> = {
            let blocks = self.blocks.lock().unwrap();
            blocks.author_refs(&author)
        };
        let quorum = self.committee.quorum_threshold();

        let c_candidates: Vec<BlockRef> = refs
            .iter()
            .filter(|r| self.direct_pub(r) && self.ack_stake(r) >= quorum)
            .cloned()
            .collect();
        let c = newest(c_candidates);

        let t_candidates: Vec<BlockRef> = refs
            .iter()
            .filter(|r| self.direct_pub(r))
            .filter(|r| match &c {
                Some(c_ref) => r.1 > c_ref.1 && self.prefix_contains(r, c_ref),
                None => true,
            })
            .cloned()
            .collect();
        let t = newest(t_candidates);

        match c {
            Some(c) => {
                self.c_candidate.insert(author, c);
            }
            None => {
                self.c_candidate.remove(&author);
            }
        }
        match t {
            Some(t) => {
                self.t_candidate.insert(author, t);
            }
            None => {
                self.t_candidate.remove(&author);
            }
        }
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
