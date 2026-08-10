use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::block::BlockRef;
use crate::vantage::lanes::{BlockCache, SharedBlocks};
use crate::vantage::sequence::SequenceOutcome;
use crypto::{Digest, PublicKey};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Maximum number of views admitted for concurrent fetch.
pub const DEFAULT_WINDOW_VIEWS: usize = 64;

/// Retained for configuration compatibility; admission ignores this value.
pub const DEFAULT_SETTLE_CEILING: usize = 2_048;

/// Result of rebasing an installation target onto local progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebaseOutcome {
    /// An installable suffix remains.
    Continue,
    /// Local execution reached or passed the target.
    Overtaken,
    /// The verified and local sequence heads disagree at the rebase boundary.
    Diverged {
        view: View,
        expected: Digest,
        local: Digest,
    },
}

#[derive(Debug)]
struct StagedView {
    outcome: SequenceOutcome,
    /// Verified output digests in emission order.
    delta: Arc<[Digest]>,
    refs: Vec<BlockRef>,
    admitted: bool,
    /// Number of consecutive deliverable digests already checked.
    ready_prefix: usize,
    complete: bool,
}

/// Converts a verified sequence target into ordered, locally deliverable views.
#[derive(Debug)]
pub struct SequenceInstall {
    base_view: View,
    target_view: View,
    target_head: Digest,
    views: BTreeMap<View, StagedView>,
    lane_tips: BTreeMap<PublicKey, BlockRef>,
    /// Verified heads retained for rebase checks after staged views are removed.
    heads: BTreeMap<View, Digest>,
    next_admit: View,
    next_install: View,
    window_views: usize,
}

impl SequenceInstall {
    /// Stages entries in `base_view + 1..=target_view` and drops entries outside that range.
    pub fn new(
        base_view: View,
        target_view: View,
        target_head: Digest,
        staged: Vec<(View, SequenceOutcome, Vec<Digest>)>,
        heads: Vec<(View, Digest)>,
        window_views: usize,
        _settle_ceiling: usize,
    ) -> Self {
        let heads: BTreeMap<View, Digest> = heads
            .into_iter()
            .filter(|(view, _)| *view > base_view && *view <= target_view)
            .collect();
        let mut views = BTreeMap::new();
        let mut lane_tips = BTreeMap::new();
        for (view, outcome, delta) in staged {
            if view <= base_view || view > target_view {
                continue;
            }
            let refs = manifest_refs(&outcome);
            for r in &refs {
                let replace = lane_tips
                    .get(&r.0)
                    .is_none_or(|current: &BlockRef| r.1 > current.1);
                if replace {
                    lane_tips.insert(r.0, r.clone());
                }
            }
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
            lane_tips,
            heads,
            next_admit: base_view + 1,
            next_install: base_view + 1,
            window_views: window_views.max(1),
        }
    }

    pub fn target(&self) -> (View, &Digest) {
        (self.target_view, &self.target_head)
    }

    /// Rebases onto local progress and rejects a conflicting verified head.
    pub fn rebase(&mut self, local_view: View, local_head: &Digest) -> RebaseOutcome {
        if local_view >= self.next_install {
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

    pub fn is_contiguous(&self) -> bool {
        (self.base_view + 1..=self.target_view).all(|v| self.views.contains_key(&v))
    }

    pub fn lane_tips(&self) -> Vec<(PublicKey, Height)> {
        self.lane_tips.values().map(|r| (r.0, r.1)).collect()
    }

    pub fn lane_tip(&self, author: &PublicKey) -> Option<BlockRef> {
        self.lane_tips.get(author).cloned()
    }

    pub fn views_total(&self) -> usize {
        self.views.len()
    }

    pub fn views_complete(&self) -> usize {
        self.views.values().filter(|v| v.complete).count()
    }

    pub fn views_in_flight(&self) -> usize {
        self.views
            .values()
            .filter(|v| v.admitted && !v.complete)
            .count()
    }

    pub fn blocks_awaited(&self, blocks: &SharedBlocks) -> usize {
        let cache = blocks.lock();
        self.views
            .values()
            .filter(|v| v.admitted && !v.complete)
            .flat_map(|v| v.delta.iter())
            .filter(|d| !deliverable(&cache, d))
            .count()
    }

    /// Returns missing headers from at most `scan_limit` staged positions.
    pub fn missing_digests(&self, blocks: &SharedBlocks, scan_limit: usize) -> Vec<Digest> {
        let cache = blocks.lock();
        let mut out = Vec::new();
        let mut examined = 0usize;
        for staged in self.views.values().filter(|v| v.admitted && !v.complete) {
            for digest in staged.delta.iter().skip(staged.ready_prefix) {
                if examined >= scan_limit {
                    return out;
                }
                examined += 1;
                if !deliverable(&cache, digest) {
                    out.push(digest.clone());
                }
            }
        }
        out
    }

    pub fn payload_retry_headers(&self, blocks: &SharedBlocks, limit: usize) -> Vec<Header> {
        let cache = blocks.lock();
        let mut out = Vec::new();
        for staged in self.views.values().filter(|v| v.admitted && !v.complete) {
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

    /// Checks at most `budget` digests and returns the number examined.
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

    /// Admits views in order while the fetch window has capacity.
    pub fn admit(&mut self, _pending_settle_len: usize) -> Vec<BlockRef> {
        let mut out = Vec::new();
        let mut in_flight = self.views_in_flight();
        while self.next_admit <= self.target_view && in_flight < self.window_views {
            let view = self.next_admit;
            let Some(staged) = self.views.get_mut(&view) else {
                break;
            };
            self.next_admit += 1;
            if staged.complete {
                continue;
            }
            staged.admitted = true;
            in_flight += 1;
            out.extend(staged.refs.iter().cloned());
        }
        out
    }

    pub fn installable(&self) -> Option<View> {
        if self.next_install > self.target_view {
            return None;
        }
        let staged = self.views.get(&self.next_install)?;
        staged.complete.then_some(self.next_install)
    }

    /// Returns the verified outcome and output order for a staged view.
    pub fn view_output(&self, view: View) -> Option<(&SequenceOutcome, Arc<[Digest]>)> {
        let staged = self.views.get(&view)?;
        Some((&staged.outcome, Arc::clone(&staged.delta)))
    }

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

/// Readiness requires a cached, block-verified header; payload retrieval is handled later.
fn deliverable(cache: &BlockCache, digest: &Digest) -> bool {
    cache
        .get(digest)
        .is_some_and(|entry| entry.block_ok_verified)
}

fn manifest_refs(outcome: &SequenceOutcome) -> Vec<BlockRef> {
    match outcome {
        SequenceOutcome::Full { c, t } => c.iter().chain(t.iter()).cloned().collect(),
        SequenceOutcome::Core { c } => c.to_vec(),
        SequenceOutcome::Skip => Vec::new(),
    }
}
