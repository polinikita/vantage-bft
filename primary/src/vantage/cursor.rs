// PHASE4-SPEC.md §9 -- output cursor: deterministic linearization of AGB per-view
// outcomes into the committed block log, plus the Phase-2 Committed metric reuse.

use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::agb::{Manifest, Outcome};
use crate::vantage::lanes::SharedBlocks;
use crate::vantage::Effect;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default, Clone)]
struct ViewInput {
    completed: Option<(Manifest, Manifest)>,
    sealed: Option<Outcome>,
}

/// §9's cursor: views processed in strictly increasing order; `output` = `D`, the set
/// of block hashes already output (initialized `{genesis_digest}`).
pub struct Cursor {
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// The lowest view not yet fully advanced past.
    next_view: View,
    output: HashSet<Digest>,
    /// The committed block log in emission order (for equality checks / eventual
    /// application use).
    output_log: Vec<Digest>,
    /// Views whose core prefix `K` has already been emitted (either at a
    /// completed-but-open step, or inline while sealing).
    core_emitted: BTreeSet<View>,
    pending: BTreeMap<View, ViewInput>,
    /// PHASE6-SPEC.md §9 gate amendment (D6-7, performance-only, deterministic-
    /// equivalent): per-author "last emitted" watermark (height + digest), replacing
    /// `expand`'s genesis-anew `collect_verified_chain` walk at every seal with an
    /// incremental `collect_verified_suffix` walk from the new target back to the
    /// already-emitted point. Absent entry means "nothing emitted yet for this
    /// author" (implicitly `(0, genesis)`).
    watermarks: HashMap<PublicKey, (Height, Digest)>,
}

impl Cursor {
    pub fn new(
        committee: Committee,
        sid: Digest,
        genesis: Digest,
        max_block_payload: usize,
        blocks: SharedBlocks,
    ) -> Self {
        let mut output = HashSet::new();
        output.insert(genesis.clone());
        Self {
            committee,
            sid,
            genesis,
            max_block_payload,
            blocks,
            next_view: 1,
            output,
            output_log: Vec::new(),
            core_emitted: BTreeSet::new(),
            pending: BTreeMap::new(),
            watermarks: HashMap::new(),
        }
    }

    pub fn output_log(&self) -> &[Digest] {
        &self.output_log
    }

    pub fn next_view(&self) -> View {
        self.next_view
    }

    pub fn gc_below(&mut self, floor: View) {
        let floor = floor.min(self.next_view);
        self.pending = self.pending.split_off(&floor);
        self.core_emitted = self.core_emitted.split_off(&floor);
    }

    /// R4's `complete(v) -> B`: the core becomes irrevocable at a still-`gopen` view.
    /// A late/duplicate input for a view the cursor has already advanced past is
    /// idempotent-ignored -- re-inserting a `pending` entry for it would never be
    /// processed or removed (nothing ever revisits a view below `next_view`), which
    /// would otherwise leak one entry per such late arrival forever.
    pub fn on_completed(&mut self, view: View, c: Manifest, t: Manifest) -> Vec<Effect> {
        if view < self.next_view {
            return Vec::new();
        }
        self.pending.entry(view).or_default().completed = Some((c, t));
        self.pump()
    }

    /// The try-seal arbiter's terminal result for `view`. See `on_completed`'s doc
    /// comment for why views below `next_view` are ignored rather than buffered.
    pub fn on_sealed(&mut self, view: View, outcome: Outcome) -> Vec<Effect> {
        if view < self.next_view {
            return Vec::new();
        }
        self.pending.entry(view).or_default().sealed = Some(outcome);
        self.pump()
    }

    /// Re-attempt progress -- call after any `BlockCached` wakeup (a previously
    /// missing/unverified prefix may now be available).
    pub fn retry(&mut self) -> Vec<Effect> {
        self.pump()
    }

    fn pump(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        while let Some(input) = self.pending.get(&self.next_view) {
            let (completed, sealed) = (input.completed.clone(), input.sealed.clone());

            // Locally completed but open: emit K, do not advance (tip stays open).
            if !self.core_emitted.contains(&self.next_view) {
                if let Some((c, _t)) = &completed {
                    if let Some(hashes) = self.expand(c) {
                        self.core_emitted.insert(self.next_view);
                        effects.extend(self.emit(hashes));
                    }
                }
            }

            let Some(outcome) = sealed else {
                break; // still gopen -- wait for the seal
            };

            match outcome {
                Outcome::Full(c, t) => {
                    if !self.core_emitted.contains(&self.next_view) {
                        let Some(k_hashes) = self.expand(&c) else {
                            break;
                        };
                        self.core_emitted.insert(self.next_view);
                        effects.extend(self.emit(k_hashes));
                    }
                    let Some(t_hashes) = self.expand(&t) else {
                        break;
                    };
                    effects.extend(self.emit(t_hashes));
                    self.advance();
                }
                Outcome::Core(c) => {
                    if !self.core_emitted.contains(&self.next_view) {
                        let Some(k_hashes) = self.expand(&c) else {
                            break;
                        };
                        self.core_emitted.insert(self.next_view);
                        effects.extend(self.emit(k_hashes));
                    }
                    self.advance();
                }
                Outcome::Skip => {
                    // gskip: emit nothing, advance (arm implemented, unreachable in
                    // Phase 4 -- Direct-AGB never produces it).
                    self.advance();
                }
            }
        }
        effects
    }

    fn advance(&mut self) {
        self.pending.remove(&self.next_view);
        self.core_emitted.remove(&self.next_view);
        self.next_view += 1;
    }

    fn emit(&mut self, hashes: Vec<Digest>) -> Vec<Effect> {
        if hashes.is_empty() {
            return Vec::new();
        }
        for h in &hashes {
            self.output.insert(h.clone());
            self.output_log.push(h.clone());
            log::info!("Committed vantage block {}", h);
        }
        let (by_worker, headers) = self.batches_by_worker_and_headers(&hashes);
        let commit_millis = now_millis();
        vec![Effect::NotifyCommitted(
            commit_millis,
            by_worker.into_iter().collect(),
            headers,
        )]
    }

    /// Same `BlockCache` lock/lookup `batches_by_worker` always did, plus (PHASE7-
    /// PREP-NOTES.md, paying down PHASE4-NOTES.md §6's scope cut) collecting each
    /// committed hash's own `Header` for `tx_output` -- the digest's `Header` is
    /// already on hand at this exact call site, per that note's recommendation.
    fn batches_by_worker_and_headers(
        &self,
        hashes: &[Digest],
    ) -> (HashMap<WorkerId, Vec<Digest>>, Vec<Header>) {
        let mut out: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        let mut headers = Vec::with_capacity(hashes.len());
        let blocks = self.blocks.lock().unwrap();
        for h in hashes {
            if let Some(entry) = blocks.get(h) {
                for (digest, worker_id) in &entry.block.payload {
                    out.entry(*worker_id).or_default().push(digest.clone());
                }
                headers.push(entry.block.clone());
            }
        }
        (out, headers)
    }

    /// `Expand_D(X)` (§9): traverse `manifest` in encoded vector order; for each entry,
    /// walk its lane prefix from genesis toward the named frontier (genesis itself
    /// never output), omitting hashes already in `self.output` (D, growing across calls
    /// within the same `pump()` to realize `D + K` for T's expansion) or seen earlier in
    /// the same traversal. Returns `None` (caller must wait for a `BlockCached` wakeup)
    /// if any needed prefix isn't yet fully obtained + verified.
    fn expand(&mut self, manifest: &Manifest) -> Option<Vec<Digest>> {
        let blocks = self.blocks.lock().unwrap();
        // First pass (read-only): resolve every entry's NEW suffix against its
        // author's current watermark. Collected before any watermark is mutated, so a
        // `None` (still-missing/unverified prefix somewhere in the manifest) leaves no
        // partial side effect -- `pump()`'s callers rely on `expand` being all-or-
        // nothing, exactly as the original genesis-anew version was.
        let mut suffixes: Vec<(PublicKey, Height, Vec<Digest>)> =
            Vec::with_capacity(manifest.len());
        for (author, height, digest) in manifest {
            let (stop_height, stop_digest) = self
                .watermarks
                .get(author)
                .cloned()
                .unwrap_or_else(|| (0, self.genesis.clone()));
            if *height <= stop_height {
                continue; // already emitted (or older) through this author's watermark
            }
            let suffix = blocks.collect_verified_suffix(
                &self.committee,
                &self.sid,
                self.max_block_payload,
                stop_height,
                &stop_digest,
                digest,
            )?;
            suffixes.push((*author, *height, suffix));
        }
        drop(blocks);

        let mut out = Vec::new();
        let mut seen: HashSet<Digest> = HashSet::new();
        for (author, height, suffix) in suffixes {
            for h in &suffix {
                if self.output.contains(h) || seen.contains(h) {
                    continue;
                }
                seen.insert(h.clone());
                out.push(h.clone());
            }
            if let Some(last) = suffix.last() {
                self.watermarks.insert(author, (height, last.clone()));
            }
        }
        Some(out)
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}
