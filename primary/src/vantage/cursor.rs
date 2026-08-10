use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::agb::{Manifest, Outcome};
use crate::vantage::lanes::{SharedBlocks, SuffixWalk};
use crate::vantage::sequence::SequenceOutcome;
use crate::vantage::Effect;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Reason a verified sequence installation was refused.
///
/// Output from earlier calls remains valid, and the refusing call emits nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallError {
    /// The cursor is waiting for another view.
    OutOfOrder { expected: View, got: View },
    /// Local output is not a prefix of the verified output.
    PrefixMismatch {
        view: View,
        emitted: usize,
        verified: usize,
    },
    /// Installation would emit a digest already in the committed output set.
    AlreadyOutput { view: View, digest: Digest },
    /// A required digest is not cached and block verified.
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

struct InstallProgress {
    view: View,
    outcome: SequenceOutcome,
    verified: Arc<[Digest]>,
    prefix_checked: usize,
    prefix_len: usize,
}

/// Emits per-view outcomes in strictly increasing view order.
pub struct Cursor {
    committee: Committee,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    next_view: View,
    stall_logged_for: Option<View>,
    forked_reported: HashSet<PublicKey>,
    forked_dropped: u64,
    output: HashSet<Digest>,
    #[cfg(test)]
    output_log: Vec<Digest>,
    core_emitted: BTreeSet<View>,
    /// Output accumulated for the current view in emission order.
    delta: Vec<Digest>,
    installing: Option<InstallProgress>,
    pending: BTreeMap<View, ViewInput>,
    /// Last emitted height and digest for each author.
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
            stall_logged_for: None,
            forked_reported: HashSet::new(),
            forked_dropped: 0,
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

    pub fn forked_dropped(&self) -> u64 {
        self.forked_dropped
    }

    /// Records completion unless the cursor has already advanced past `view`.
    pub fn on_completed(&mut self, view: View, c: Manifest, t: Manifest) -> Vec<Effect> {
        if view < self.next_view {
            return Vec::new();
        }
        self.pending.entry(view).or_default().completed = Some((c, t));
        self.pump()
    }

    /// Records a terminal outcome unless the cursor has already advanced past `view`.
    pub fn on_sealed(&mut self, view: View, outcome: Outcome) -> Vec<Effect> {
        if view < self.next_view {
            return Vec::new();
        }
        self.pending.entry(view).or_default().sealed = Some(outcome);
        self.pump()
    }

    pub fn retry(&mut self) -> Vec<Effect> {
        self.pump()
    }

    fn pump(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        while let Some(input) = self.pending.get(&self.next_view) {
            if self
                .installing
                .as_ref()
                .is_some_and(|install| install.view == self.next_view)
            {
                break;
            }
            if input.sealed.is_none() {
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

            if !self.core_emitted.contains(&self.next_view) {
                if let Some((c, _t)) = &completed {
                    if let Some(hashes) = self.expand(c) {
                        self.core_emitted.insert(self.next_view);
                        effects.extend(self.emit(hashes));
                    }
                }
            }

            let Some(outcome) = sealed else {
                break;
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
                    effects.push(self.finalize(SequenceOutcome::Skip));
                }
            }
        }
        effects
    }

    /// Returns output already emitted for the current open view.
    pub fn open_delta(&self) -> &[Digest] {
        &self.delta
    }

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

    /// Installs one verified view with a per-call digest budget.
    ///
    /// The first call fixes the outcome and digest sequence. Later calls continue that fixed
    /// view. The result reports effects, finalization, and the number of digests examined.
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
        let delivered: Vec<Digest> = install.verified.iter().cloned().collect();
        self.apply_watermarks(&delivered);
        effects.push(self.finalize(install.outcome));
        Ok((effects, true, examined))
    }

    /// Abandons installation; any emitted canonical prefix remains committed.
    pub fn abort_install(&mut self) -> Vec<Effect> {
        self.installing = None;
        self.pump()
    }

    /// Advances watermarks only from delivered blocks, never from manifest intent.
    fn apply_watermarks(&mut self, delivered: &[Digest]) {
        let advances: Vec<(PublicKey, Height, Digest)> = {
            let blocks = self.blocks.lock();
            delivered
                .iter()
                .filter_map(|digest| {
                    blocks
                        .get(digest)
                        .map(|entry| (entry.block.author, entry.block.height, digest.clone()))
                })
                .collect()
        };
        for (author, height, digest) in advances {
            let entry = self.watermarks.entry(author).or_insert((0, digest.clone()));
            if height > entry.0 {
                *entry = (height, digest);
            }
        }
    }

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
            self.delta.push(h.clone());
            #[cfg(test)]
            self.output_log.push(h.clone());
            log::debug!("Committed vantage block {}", h);
        }
        let (by_worker, headers) = self.batches_by_worker_and_headers(&hashes);
        let commit_millis = now_millis();
        vec![Effect::NotifyCommitted(
            commit_millis,
            by_worker.into_iter().collect(),
            headers,
        )]
    }

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

    /// Expands a manifest from emitted watermarks in manifest order.
    ///
    /// A missing suffix stalls the view. A suffix that contradicts emitted ancestry is
    /// dropped without moving that author's watermark.
    fn expand(&mut self, manifest: &Manifest) -> Option<Vec<Digest>> {
        let blocks = self.blocks.lock();
        let mut suffixes: Vec<(PublicKey, Height, Vec<Digest>)> =
            Vec::with_capacity(manifest.len());
        for (author, height, digest) in manifest {
            let (stop_height, stop_digest) = self
                .watermarks
                .get(author)
                .cloned()
                .unwrap_or_else(|| (0, self.genesis.clone()));
            if *height <= stop_height {
                continue;
            }
            let suffix = match blocks.classify_suffix(
                &self.committee,
                &self.sid,
                self.max_block_payload,
                stop_height,
                &stop_digest,
                digest,
            ) {
                SuffixWalk::Ready(suffix) => suffix,
                SuffixWalk::Pending => {
                    if self.stall_logged_for != Some(self.next_view) {
                        self.stall_logged_for = Some(self.next_view);
                        log::warn!(
                            "vantage cursor: waiting at view={} on author={author} \
                             height={height} (watermark height={}) target={digest}",
                            self.next_view,
                            stop_height,
                        );
                    }
                    return None;
                }
                SuffixWalk::Forked => {
                    if self.forked_reported.insert(*author) {
                        log::warn!(
                            "vantage cursor: dropping FORKED manifest entry at view={} \
                             author={author} height={height} (watermark height={}); \
                             its ancestry contradicts delivered output",
                            self.next_view,
                            stop_height,
                        );
                    }
                    self.forked_dropped += 1;
                    continue;
                }
            };
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

/// Returns Unix time in milliseconds.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis() as u64
}
