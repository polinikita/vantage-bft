// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §10 -- Phase C staging: turn a VERIFIED sequence
// target into blocks this node actually holds.
//
// A verified transfer yields per-view `(outcome, delta)`, where the delta is a list of
// block DIGESTS -- the identities of the blocks the target says were output, not the
// blocks themselves. Nothing can be installed until those blocks are in the local cache,
// so this module is the bridge: it turns each view's outcome manifests into fetch work for
// the existing `Repairer`, and reports when a view's whole delta is finally in hand.
//
// The fetch instruction is the OUTCOME's manifests, not the delta. A `Manifest` entry is
// exactly a `BlockRef`, and `Repairer::authorize` already walks the named lane's prefix,
// requesting every missing ancestor with bounded fan-out, a congestion window and worker-
// payload sync on arrival. The delta is the completion test instead: it is the
// authoritative list of what this view must deliver, and checking it against the cache
// works no matter how the blocks arrived, so ordinary dissemination beating repair to them
// costs nothing.
//
// PACING. A target can span hundreds of views across n lanes, and authorizing all of it at
// once grows `Repairer::pending_settle` without bound -- the exact set whose unbounded
// growth produced 612,424,724 settle calls against 60,262 blocks on the 2026-08-07 n=100
// run (repair.rs's `on_block_available` doc comment). So views are admitted in install
// order behind two gates: at most `window_views` in flight, and nothing new admitted while
// repair's own pending set is above `settle_ceiling`. The window keeps one slow lane from
// serializing the whole range; the settle gate is the same feedback idiom
// `RECOVERY_IN_FLIGHT_MAX` already applies to requests, so installation cannot outrun the
// machinery it is driving.

use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::block::BlockRef;
use crate::vantage::lanes::{BlockCache, SharedBlocks};
use crate::vantage::sequence::SequenceOutcome;
use crypto::{Digest, PublicKey};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Views admitted into the fetch window at once. Eight is enough that a single slow lane
/// overlaps with progress on later views, and small enough that the authorized set stays
/// on the order of `8 * n` refs rather than `range * n`.
pub const DEFAULT_WINDOW_VIEWS: usize = 8;

/// `Repairer::pending_settle_len()` above which no further view is admitted. Set well
/// below the 4,967 unsettled refs measured on a straggler, so installation backs off
/// before it reaches the regime that pinned the core.
pub const DEFAULT_SETTLE_CEILING: usize = 2_048;

/// What a rebase concluded about a target after the cursor moved under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseOutcome {
    /// The target still has an installable suffix.
    Continue,
    /// Ordinary dissemination reached the target on its own; nothing left to install.
    Overtaken,
    /// The head local execution derived at the rebase boundary is not the one the verified
    /// chain records. Installing the suffix would splice it onto a divergent history.
    Diverged {
        view: View,
        expected: Digest,
        local: Digest,
    },
}

#[derive(Debug)]
struct StagedView {
    outcome: SequenceOutcome,
    /// The verified output of this view, in emission order. Both the completion test and,
    /// in the install step, the delivery order.
    delta: Arc<[Digest]>,
    /// `outcome`'s manifest entries: what to hand `Repairer::authorize`.
    refs: Vec<BlockRef>,
    admitted: bool,
    /// Number of consecutive deliverable digests already checked. Readiness validation is
    /// resumed from here so a large view is never re-scanned from its first block.
    ready_prefix: usize,
    /// Every digest in `delta` is cached. Latched: the cache only grows within a session
    /// for blocks this range needs, and re-checking a finished view every tick is the
    /// per-tick sweep this module exists to avoid.
    complete: bool,
}

/// A verified target being turned into locally held blocks.
///
/// Holds no borrow of the transfer that produced it -- the transfer is retired as soon as
/// it verifies, and everything needed to install is copied out here.
#[derive(Debug)]
pub struct SequenceInstall {
    /// Highest view the local chain held when the transfer started. Installation covers
    /// `base_view + 1 ..= target_view`.
    base_view: View,
    target_view: View,
    target_head: Digest,
    views: BTreeMap<View, StagedView>,
    /// The verified chain's head at each view in the range, kept after the view itself is
    /// installed or dropped. `rebase` needs the head at a boundary the staged views no
    /// longer cover, so this deliberately outlives `views`.
    heads: BTreeMap<View, Digest>,
    /// Next view to admit into the fetch window.
    next_admit: View,
    /// Next view to install. Never runs ahead of `next_admit`.
    next_install: View,
    window_views: usize,
    settle_ceiling: usize,
}

impl SequenceInstall {
    /// `staged` is the verified per-view output, which must cover `base_view+1..=target`
    /// contiguously; anything outside that range is dropped rather than trusted.
    pub fn new(
        base_view: View,
        target_view: View,
        target_head: Digest,
        staged: Vec<(View, SequenceOutcome, Vec<Digest>)>,
        heads: Vec<(View, Digest)>,
        window_views: usize,
        settle_ceiling: usize,
    ) -> Self {
        let heads: BTreeMap<View, Digest> = heads
            .into_iter()
            .filter(|(view, _)| *view > base_view && *view <= target_view)
            .collect();
        let mut views = BTreeMap::new();
        for (view, outcome, delta) in staged {
            if view <= base_view || view > target_view {
                continue;
            }
            let refs = manifest_refs(&outcome);
            // A Skip carries no manifest and no output, so it is finished on arrival and
            // must never wait on a fetch that will never be needed.
            let complete = delta.is_empty() && refs.is_empty();
            views.insert(
                view,
                StagedView {
                    outcome,
                    delta: delta.into(),
                    refs,
                    admitted: false,
                    ready_prefix: 0,
                    complete,
                },
            );
        }
        Self {
            base_view,
            target_view,
            target_head,
            views,
            heads,
            next_admit: base_view + 1,
            next_install: base_view + 1,
            window_views: window_views.max(1),
            settle_ceiling,
        }
    }

    pub fn target(&self) -> (View, &Digest) {
        (self.target_view, &self.target_head)
    }

    /// Drop the views ordinary execution reached while the blocks were still being fetched.
    ///
    /// `base_view` is where the local chain stood when the TRANSFER started, and a node
    /// that is merely slow rather than stuck keeps committing the whole time. Without this
    /// the first `Cursor::install` would be `OutOfOrder` against a cursor that has moved
    /// on, and a target still perfectly good above the new position would be abandoned.
    ///
    /// `local_head` is the head the local chain derived at `local_view`, and it is
    /// REVALIDATED against the verified chain before any prefix is dropped. Skipping that
    /// check would be the one way this mechanism could install a correct suffix onto a
    /// divergent prefix: the verified chain says what the head at `local_view` must be, and
    /// if the locally derived one differs then everything above it is being spliced onto a
    /// history no correct party shares. The remaining suffix is otherwise unaffected -- the
    /// chain is per-view, so dropping a REVALIDATED prefix weakens nothing.
    ///
    /// A boundary outside the target's range cannot be checked and is not treated as
    /// agreement: below `base_view` the chain says nothing, and at or above `target_view`
    /// the target is spent anyway.
    pub fn rebase(&mut self, local_view: View, local_head: &Digest) -> RebaseOutcome {
        if local_view >= self.next_install {
            // Only meaningful where the verified chain has an opinion.
            if let Some(expected) = self.heads.get(&local_view) {
                if expected != local_head {
                    return RebaseOutcome::Diverged {
                        view: local_view,
                        expected: expected.clone(),
                        local: local_head.clone(),
                    };
                }
            }
        }
        while self.next_install <= local_view && self.next_install <= self.target_view {
            self.views.remove(&self.next_install);
            self.next_install += 1;
        }
        self.next_admit = self.next_admit.max(self.next_install);
        if self.next_install > self.target_view {
            RebaseOutcome::Overtaken
        } else {
            RebaseOutcome::Continue
        }
    }

    pub fn base_view(&self) -> View {
        self.base_view
    }

    /// Contiguity is a precondition for installing anything: the cursor advances one view
    /// at a time, so a hole makes every later view unreachable no matter what arrives.
    /// Checked once, at construction time, rather than discovered mid-install.
    pub fn is_contiguous(&self) -> bool {
        (self.base_view + 1..=self.target_view).all(|v| self.views.contains_key(&v))
    }

    /// The highest `(author, height)` this target needs from each lane.
    ///
    /// For seeding repair's holder index from the checkpoint announcers. That index is
    /// keyed by lane and keeps the maximum height per peer, so one entry per author
    /// carries exactly the same information as every manifest entry would, at `n` updates
    /// instead of `views * n`.
    ///
    /// Sound to attribute to an announcer: a first-hand announcement claims the sender
    /// terminally processed through the target view, and a party cannot terminally process
    /// a view without holding the blocks its manifests name. A lying announcer costs one
    /// misdirected request, never correctness -- the index only biases WHICH peer is asked.
    pub fn lane_tips(&self) -> Vec<(PublicKey, Height)> {
        let mut tips: BTreeMap<PublicKey, Height> = BTreeMap::new();
        for staged in self.views.values() {
            for (author, height, _) in &staged.refs {
                let entry = tips.entry(*author).or_insert(0);
                *entry = (*entry).max(*height);
            }
        }
        tips.into_iter().collect()
    }

    pub fn views_total(&self) -> usize {
        self.views.len()
    }

    pub fn views_complete(&self) -> usize {
        self.views.values().filter(|v| v.complete).count()
    }

    /// Admitted, still waiting on blocks.
    pub fn views_in_flight(&self) -> usize {
        self.views
            .values()
            .filter(|v| v.admitted && !v.complete)
            .count()
    }

    /// Digests this target still needs and cannot yet deliver. Diagnostic only -- an
    /// install that stops making progress is otherwise indistinguishable from a slow one.
    pub fn blocks_awaited(&self, blocks: &SharedBlocks) -> usize {
        let cache = blocks.lock();
        self.views
            .values()
            .filter(|v| !v.complete)
            .flat_map(|v| v.delta.iter())
            .filter(|d| !deliverable(&cache, d))
            .count()
    }

    /// Return a bounded set of cached headers whose worker payload is still missing.
    /// Payload readiness is monotonic, but the initial Synchronize can be lost; callers
    /// use this to retry without requiring another header announcement.
    ///
    /// Each view is scanned from `ready_prefix`, not from its first digest. Everything
    /// below that index was already observed deliverable and `payload_ok` never goes back
    /// to false, so re-examining it can only ever produce misses -- and this runs every
    /// tick against up to `window_views` deltas that are each a whole lane suffix, which is
    /// exactly the per-tick sweep over already-settled state this module exists to avoid.
    pub fn payload_retry_headers(&self, blocks: &SharedBlocks, limit: usize) -> Vec<Header> {
        let cache = blocks.lock();
        let mut out = Vec::new();
        for staged in self.views.values().filter(|v| v.admitted && !v.complete) {
            // Deltas are disjoint across views -- `expand` deduplicates against `D` before
            // a digest is ever recorded -- so no cross-view dedup set is needed here.
            for digest in staged.delta.iter().skip(staged.ready_prefix) {
                if out.len() >= limit {
                    return out;
                }
                let Some(entry) = cache.get(digest) else {
                    continue;
                };
                if !entry.payload_ok {
                    out.push(entry.block.clone());
                }
            }
        }
        out
    }

    /// Re-test at most `budget` new digests and latch views whose whole delta can be
    /// delivered. Each view resumes at its first unchecked digest, avoiding a full scan on
    /// every tick while preserving strict prefix readiness.
    pub fn refresh_budgeted(&mut self, blocks: &SharedBlocks, budget: usize) -> usize {
        let cache = blocks.lock();
        let mut examined = 0usize;
        for staged in self.views.values_mut() {
            if staged.complete || !staged.admitted {
                continue;
            }
            while staged.ready_prefix < staged.delta.len() && examined < budget {
                examined += 1;
                if !deliverable(&cache, &staged.delta[staged.ready_prefix]) {
                    break;
                }
                staged.ready_prefix += 1;
            }
            if staged.ready_prefix == staged.delta.len() {
                staged.complete = true;
            }
            if examined == budget {
                break;
            }
        }
        examined
    }

    #[cfg(test)]
    pub fn refresh(&mut self, blocks: &SharedBlocks) {
        self.refresh_budgeted(blocks, usize::MAX);
    }

    /// Refs to hand `Repairer::authorize`, respecting both gates.
    ///
    /// Returns empty when the window is full, when repair is already congested, or when
    /// the target is fully staged -- all three are ordinary, not errors.
    pub fn admit(&mut self, pending_settle_len: usize) -> Vec<BlockRef> {
        let mut out = Vec::new();
        if pending_settle_len >= self.settle_ceiling {
            return out;
        }
        while self.next_admit <= self.target_view && self.views_in_flight() < self.window_views {
            let view = self.next_admit;
            let Some(staged) = self.views.get_mut(&view) else {
                // A hole: `is_contiguous` already refused this target, so reaching here
                // means the install was started anyway. Stop rather than skip -- skipping
                // would install a gap.
                break;
            };
            self.next_admit += 1;
            if staged.complete {
                continue; // Skip view, or one ordinary dissemination already delivered.
            }
            staged.admitted = true;
            out.extend(staged.refs.iter().cloned());
        }
        out
    }

    /// The next view ready to apply, or `None` while its blocks are still missing.
    /// Strictly in order: a later complete view is never installed ahead of an earlier
    /// incomplete one.
    pub fn installable(&self) -> Option<View> {
        if self.next_install > self.target_view {
            return None;
        }
        let staged = self.views.get(&self.next_install)?;
        staged.complete.then_some(self.next_install)
    }

    /// The verified content of `view`, for the caller to apply.
    pub fn view_output(&self, view: View) -> Option<(&SequenceOutcome, Arc<[Digest]>)> {
        let staged = self.views.get(&view)?;
        Some((&staged.outcome, Arc::clone(&staged.delta)))
    }

    /// Record that `view` has been applied. Panics only on a caller bug (installing out of
    /// order), which would otherwise corrupt the cursor silently.
    pub fn mark_installed(&mut self, view: View) {
        assert_eq!(
            view, self.next_install,
            "sequence install applied out of order"
        );
        self.views.remove(&view);
        self.next_install += 1;
    }

    pub fn is_done(&self) -> bool {
        self.next_install > self.target_view
    }
}

/// Can this block actually be handed to the executor?
///
/// Presence is NOT enough. `Repairer::on_serve` upserts repaired headers with
/// `payload_ok = false` on purpose -- repair is a chain-authenticity concern (D1 clause
/// ii), not a payload-possession one (clause iii) -- and `collect_verified_suffix` checks
/// only `block_ok_verified`. So a header can be cached, chain-verified and still have none
/// of its worker batches locally. `Cursor::emit` would resolve its `Header` fine and
/// `notify_committed` would send `PrimaryWorkerMessage::Committed` for batch digests the
/// worker does not hold.
///
/// The ordinary path has the same shape but not the same exposure: blocks arrive roughly
/// in real time, and the `SyncBatches` emitted on arrival usually resolves before the
/// cursor reaches them. An install pulls a whole backlog at once, so the window is wide and
/// systematic rather than incidental -- thousands of headers whose payloads are all still
/// in flight. Waiting for `payload_ok` costs a stalled view, which `rebase` and the fetch
/// window already handle; not waiting costs committed output the node cannot produce.
fn deliverable(cache: &BlockCache, digest: &Digest) -> bool {
    cache.get(digest).is_some_and(|entry| entry.payload_ok)
}

/// A view's fetch instruction: every manifest entry named by its outcome.
///
/// `Full` names both the core `c` and the terminal `t`; the union is required, because the
/// delta is the expansion of both and a block reachable only through `t` would otherwise
/// never be requested.
fn manifest_refs(outcome: &SequenceOutcome) -> Vec<BlockRef> {
    match outcome {
        SequenceOutcome::Full { c, t } => c.iter().chain(t.iter()).cloned().collect(),
        SequenceOutcome::Core { c } => c.to_vec(),
        SequenceOutcome::Skip => Vec::new(),
    }
}
