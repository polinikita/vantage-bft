// PHASE4-SPEC.md §9 -- output cursor: deterministic linearization of AGB per-view
// outcomes into the committed block log, plus the Phase-2 Committed metric reuse.

use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::agb::{Manifest, Outcome};
use crate::vantage::lanes::SharedBlocks;
use crate::vantage::sequence::SequenceOutcome;
use crate::vantage::Effect;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Why an install was refused. A prefix emitted by an earlier install step remains valid;
/// the step that returns an error emits nothing further.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallError {
    /// Not the view the cursor is waiting on.
    OutOfOrder { expected: View, got: View },
    /// This node already emitted output for `view` that the verified delta contradicts.
    /// Impossible between correct parties under Phase A determinism.
    PrefixMismatch {
        view: View,
        emitted: usize,
        verified: usize,
    },
    /// A digest the delta wants delivered is already in `D`.
    AlreadyOutput { view: View, digest: Digest },
    /// A digest in the delta is not in the block cache, so its header could not be
    /// resolved and its payload would be silently dropped.
    BlocksMissing { view: View, digest: Digest },
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfOrder { expected, got } => {
                write!(
                    f,
                    "install out of order: expected view {expected}, got {got}"
                )
            }
            Self::PrefixMismatch {
                view,
                emitted,
                verified,
            } => write!(
                f,
                "install prefix mismatch at view {view}: {emitted} digests already emitted \
                 locally are not a prefix of the {verified} verified"
            ),
            Self::AlreadyOutput { view, digest } => {
                write!(f, "install at view {view} would re-output {digest}")
            }
            Self::BlocksMissing { view, digest } => {
                write!(f, "install at view {view} is missing block {digest}")
            }
        }
    }
}

#[derive(Default, Clone)]
struct ViewInput {
    completed: Option<(Manifest, Manifest)>,
    sealed: Option<Outcome>,
}

/// Cursor-owned continuation for a chunked checkpoint install.
///
/// Owning the verified bytes matters for two reasons: callers cannot swap the target under
/// a partially emitted view, and continuation never has to clone or re-scan the prefix it
/// already checked. While this is present, ordinary `pump` work for the same view is parked.
struct InstallProgress {
    view: View,
    outcome: SequenceOutcome,
    verified: Arc<[Digest]>,
    /// Local output that existed before installation started and still needs comparison
    /// with the verified prefix. This advances once and is never re-scanned.
    prefix_checked: usize,
    prefix_len: usize,
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
    /// The committed block log in emission order, kept ONLY for tests (cross-node log
    /// equality assertions in `byzantine_tests`/`crash_fault_tests`/`integration_tests`).
    ///
    /// `#[cfg(test)]` because nothing in production ever read it: the real output path is
    /// `Effect::Output`/`tx_output`, and this `Vec` grew by one 32-byte `Digest` per
    /// committed block for the process lifetime with no reader and no pruning.
    #[cfg(test)]
    output_log: Vec<Digest>,
    /// Views whose core prefix `K` has already been emitted (either at a
    /// completed-but-open step, or inline while sealing).
    core_emitted: BTreeSet<View>,
    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §8: digests emitted so far for `next_view`, in
    /// emission order.
    ///
    /// A field rather than a local, because a view's delta is NOT produced in one
    /// `pump()`: a completed-but-open view emits its core prefix `K` and then breaks to
    /// wait for the seal, and those early core blocks belong to the same view's eventual
    /// delta (§3). Cleared only by the terminal advance in `finalize`.
    delta: Vec<Digest>,
    installing: Option<InstallProgress>,
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
            #[cfg(test)]
            output_log: Vec::new(),
            core_emitted: BTreeSet::new(),
            delta: Vec::new(),
            installing: None,
            pending: BTreeMap::new(),
            watermarks: HashMap::new(),
        }
    }

    #[cfg(test)]
    pub fn output_log(&self) -> &[Digest] {
        &self.output_log
    }

    pub fn next_view(&self) -> View {
        self.next_view
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
            // A checkpoint install owns this view until it finalizes or is explicitly
            // abandoned. Ordinary Completed/Sealed inputs remain parked in `pending` so
            // they cannot bypass the install's per-tick digest budget.
            if self
                .installing
                .as_ref()
                .is_some_and(|install| install.view == self.next_view)
            {
                break;
            }
            // Check `sealed` BEFORE cloning `completed`. `completed` is an
            // `Option<(Manifest, Manifest)>` -- up to 2n `(PublicKey, Height, Digest)`
            // entries, ~14 KB at n=100 -- and `pump` is reached from `Cursor::retry` on
            // EVERY `Effect::BlockCached`, i.e. once per received block (287k on the
            // 2026-08-07 n=100 run). Whenever the tip is still gopen the old order
            // cloned all of it and then immediately hit the `break` below, so a wedged
            // cursor turned every arriving block into a large pointless allocation.
            if input.sealed.is_none() {
                // Still gopen. Emit this view's core prefix K if we can (that is
                // independent of the seal), then wait.
                if !self.core_emitted.contains(&self.next_view) {
                    if let Some((c, _t)) = input.completed.clone() {
                        if let Some(hashes) = self.expand(&c) {
                            self.core_emitted.insert(self.next_view);
                            effects.extend(self.emit(hashes));
                        }
                    }
                }
                break;
            }
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
                    effects.push(self.finalize(SequenceOutcome::Full { c, t }));
                }
                Outcome::Core(c) => {
                    if !self.core_emitted.contains(&self.next_view) {
                        let Some(k_hashes) = self.expand(&c) else {
                            break;
                        };
                        self.core_emitted.insert(self.next_view);
                        effects.extend(self.emit(k_hashes));
                    }
                    effects.push(self.finalize(SequenceOutcome::Core { c }));
                }
                Outcome::Skip => {
                    // gskip: emit nothing, advance (arm implemented, unreachable in
                    // Phase 4 -- Direct-AGB never produces it).
                    effects.push(self.finalize(SequenceOutcome::Skip));
                }
            }
        }
        effects
    }

    /// The delta already emitted for the currently open view, in emission order.
    ///
    /// Exposed for the install prefix check: a view can be half-emitted (a completed-but-
    /// open view emits its core prefix `K` and then waits for the seal), and installing a
    /// verified delta over that partial output is only sound if the partial output is a
    /// prefix of it.
    pub fn open_delta(&self) -> &[Digest] {
        &self.delta
    }

    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §10: apply one verified view, atomically.
    ///
    /// Refusals are checked before emitting the current chunk. A previously installed
    /// canonical prefix may remain if a later chunk becomes unavailable; `abort_install`
    /// releases ordinary execution, whose normal expansion deduplicates that prefix.
    ///
    /// The refusals, in the order they can bite:
    ///
    /// 1. `OutOfOrder` -- the cursor advances one view at a time and `finalize` is defined
    ///    against `next_view`, so installing anything else would silently skip or redo.
    /// 2. `PrefixMismatch` -- the locally emitted partial delta is not a prefix of the
    ///    verified one, i.e. this node already output blocks in an order the target
    ///    contradicts. Installing would duplicate or reorder committed output. Under Phase
    ///    A determinism this is impossible between correct parties, which is exactly why it
    ///    is worth checking: it can only fire on a real divergence.
    /// 3. `BlocksMissing` -- a digest in the delta is not cached and block-verified.
    ///    `emit` resolves headers by cache lookup and silently omits what it cannot find,
    ///    so an unchecked install over a partial cache would advance the view while
    ///    dropping the blocks it was supposed to deliver. Silent output loss, not a stall.
    ///
    /// Also refuses `AlreadyOutput`: a fresh digest that is already in `D`. Between correct
    /// parties this cannot happen -- the source's own `expand` deduplicated against an
    /// output set identical to ours, since the heads agree -- so it is the cheap
    /// double-delivery backstop for the case where that assumption is wrong.
    ///
    /// On success the view is finalized through the ORDINARY `finalize` path, so the
    /// sequence head advances through the same code as locally executed views and the
    /// verified-vs-local head comparison still fires at the target.
    ///
    /// `budget` caps digests compared or emitted in this call. Returns
    /// `(effects, finalized, digests_examined)`; `finalized == false` means the budget ran
    /// out and the cursor-owned continuation must be resumed. Arguments after the first
    /// call identify the view only; the cursor keeps the original outcome and delta.
    pub fn install(
        &mut self,
        view: View,
        outcome: SequenceOutcome,
        delta: &[Digest],
        budget: usize,
    ) -> Result<(Vec<Effect>, bool), InstallError> {
        self.install_budgeted(view, outcome, Arc::from(delta.to_vec()), budget)
            .map(|(effects, finalized, _)| (effects, finalized))
    }

    pub fn install_budgeted(
        &mut self,
        view: View,
        outcome: SequenceOutcome,
        delta: Arc<[Digest]>,
        budget: usize,
    ) -> Result<(Vec<Effect>, bool, usize), InstallError> {
        if view != self.next_view {
            return Err(InstallError::OutOfOrder {
                expected: self.next_view,
                got: view,
            });
        }
        if self.installing.is_none() {
            if delta.len() < self.delta.len() {
                return Err(InstallError::PrefixMismatch {
                    view,
                    emitted: self.delta.len(),
                    verified: delta.len(),
                });
            }
            self.installing = Some(InstallProgress {
                view,
                outcome,
                verified: delta,
                prefix_checked: 0,
                prefix_len: self.delta.len(),
            });
        }

        let budget = budget.max(1);
        let mut examined = 0usize;
        if let Some(progress) = self.installing.as_ref() {
            if progress.view != view {
                return Err(InstallError::OutOfOrder {
                    expected: progress.view,
                    got: view,
                });
            }
        }
        let install = self.installing.as_mut().expect("initialized");

        // Compare an already-emitted local prefix incrementally. Re-checking
        // `starts_with(self.delta)` on every continuation made a large view quadratic.
        while install.prefix_checked < install.prefix_len && examined < budget {
            let index = install.prefix_checked;
            if self.delta[index] != install.verified[index] {
                return Err(InstallError::PrefixMismatch {
                    view,
                    emitted: install.prefix_len,
                    verified: install.verified.len(),
                });
            }
            install.prefix_checked += 1;
            examined += 1;
        }
        if install.prefix_checked < install.prefix_len {
            return Ok((Vec::new(), false, examined));
        }

        let start = self.delta.len();
        let end = install.verified.len().min(start + (budget - examined));
        let chunk = install.verified[start..end].to_vec();
        if let Some(d) = chunk.iter().find(|d| self.output.contains(*d)) {
            return Err(InstallError::AlreadyOutput {
                view,
                digest: d.clone(),
            });
        }
        {
            let blocks = self.blocks.lock();
            if let Some(d) = chunk
                .iter()
                .find(|d| !blocks.get(d).is_some_and(|e| e.block_ok_verified))
            {
                return Err(InstallError::BlocksMissing {
                    view,
                    digest: d.clone(),
                });
            }
        }

        let emitted = chunk.len();
        let complete = end == install.verified.len();
        let mut effects = self.emit(chunk);
        examined += emitted;
        if !complete {
            return Ok((effects, false, examined));
        }

        let install = self.installing.take().expect("present until complete");
        self.apply_watermarks(&install.outcome);
        effects.push(self.finalize(install.outcome));
        Ok((effects, true, examined))
    }

    /// Abandon a partial checkpoint view and release parked ordinary execution.
    ///
    /// Any already emitted prefix is canonical and remains in `D`; `pump` therefore emits
    /// only the remainder when the ordinary outcome is available.
    pub fn abort_install(&mut self) -> Vec<Effect> {
        self.installing = None;
        self.pump()
    }

    /// Advance each lane's watermark to the tip its manifest names.
    ///
    /// `expand` maintains these so an ordinary seal walks only the NEW suffix instead of
    /// re-walking from genesis. An install that delivered blocks without moving them would
    /// leave the next ordinary `expand` walking from a stale point across a prefix it may
    /// no longer hold in full -- correct output (`D` still deduplicates) reached by the
    /// pathological path this index exists to remove.
    fn apply_watermarks(&mut self, outcome: &SequenceOutcome) {
        let manifests: [&Manifest; 2] = match outcome {
            SequenceOutcome::Full { c, t } => [c, t],
            SequenceOutcome::Core { c } => [c, c],
            SequenceOutcome::Skip => return,
        };
        for manifest in manifests {
            for (author, height, digest) in manifest {
                let entry = self
                    .watermarks
                    .entry(*author)
                    .or_insert((0, digest.clone()));
                if *height >= entry.0 {
                    *entry = (*height, digest.clone());
                }
            }
        }
    }

    /// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §8: the terminal advance past `next_view`.
    ///
    /// The ONLY caller of `advance`, so a view's accumulated delta is handed over exactly
    /// once and cleared at exactly the moment the view stops being open. The `break`
    /// paths above deliberately do not reach here: a view whose prefix is still missing
    /// keeps its partial delta for the next `pump`.
    fn finalize(&mut self, outcome: SequenceOutcome) -> Effect {
        let view = self.next_view;
        let output_delta = std::mem::take(&mut self.delta);
        self.advance();
        Effect::SequenceFinalized {
            view,
            outcome,
            output_delta,
        }
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
            // Emission order IS the delta order -- `expand` already deduplicated against
            // `output` and within the traversal, so this records exactly what the plan's
            // `delta_v` names.
            self.delta.push(h.clone());
            #[cfg(test)]
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
        let blocks = self.blocks.lock();
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
        let blocks = self.blocks.lock();
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
