use crate::primary::Height;
use crate::vantage::agb::{BatchViewProposal, ViewProposal};
use crate::vantage::BlockRef;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Claims a directly published lane prefix below the proposal tip.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ShortClaim {
    /// Index in `manifest_refs`.
    pub lane: u16,
    /// Distance below the referenced tip.
    pub delta: Height,
}

/// A validated positional claim ready for ancestry resolution and crediting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimRef {
    /// The sender directly received the proposal's exact named reference.
    Exact(BlockRef),
    /// The sender directly received the ancestor `delta` heights below `anchor`.
    Ancestor { anchor: BlockRef, delta: Height },
}

/// Positional availability claims against `manifest_refs`.
///
/// Claims are not part of the echo identity and do not affect echo thresholds.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct AvailClaim {
    /// Bit `j` asserts direct publication of reference `j` at its tip.
    pub at_tip: Vec<u64>,
    /// Claims for shorter verified prefixes.
    pub short: Vec<ShortClaim>,
}

impl AvailClaim {
    pub fn with_capacity(len: usize) -> Self {
        Self {
            at_tip: vec![0u64; len.div_ceil(64)],
            short: Vec::new(),
        }
    }

    pub fn set_at_tip(&mut self, lane: usize) {
        let (w, b) = (lane / 64, lane % 64);
        if w < self.at_tip.len() {
            self.at_tip[w] |= 1u64 << b;
        }
    }

    pub fn is_at_tip(&self, lane: usize) -> bool {
        let (w, b) = (lane / 64, lane % 64);
        self.at_tip
            .get(w)
            .is_some_and(|word| word & (1u64 << b) != 0)
    }

    /// Adds a short claim unless its lane, delta, or exclusivity constraint is invalid.
    pub fn push_short(&mut self, lane: usize, delta: Height) -> bool {
        if delta == 0
            || self.is_at_tip(lane)
            || lane > u16::MAX as usize
            || self.short.iter().any(|claim| claim.lane as usize == lane)
        {
            return false;
        }
        self.short.push(ShortClaim {
            lane: lane as u16,
            delta,
        });
        true
    }

    pub fn claimed(&self) -> usize {
        self.at_tip
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum::<usize>()
            + self.short.len()
    }

    /// Validates positional claims and drops malformed, duplicate, or contradictory entries.
    pub fn statements(&self, refs: &[&BlockRef]) -> Vec<ClaimRef> {
        let words = refs.len().div_ceil(64);
        if self.at_tip.len() != words || self.short.len() > refs.len() {
            return Vec::new();
        }
        if let Some(last) = self.at_tip.last() {
            let used = refs.len() % 64;
            if used != 0 && (*last & (!0u64 << used)) != 0 {
                return Vec::new();
            }
        }
        let mut out = Vec::with_capacity(self.claimed());
        for (j, r) in refs.iter().enumerate() {
            if self.is_at_tip(j) {
                out.push(ClaimRef::Exact((*r).clone()));
            }
        }
        let mut seen_short = HashSet::new();
        for s in &self.short {
            let j = s.lane as usize;
            let Some(r) = refs.get(j) else { continue };
            if self.is_at_tip(j) || !seen_short.insert(j) || s.delta == 0 || s.delta >= r.1 {
                continue;
            }
            out.push(ClaimRef::Ancestor {
                anchor: (*r).clone(),
                delta: s.delta,
            });
        }
        out
    }
}

/// Returns references in the fixed order `C`, `T`, then recovery manifests.
pub fn manifest_refs(proposal: &ViewProposal) -> Vec<&BlockRef> {
    let mut refs: Vec<&BlockRef> = Vec::with_capacity(proposal.c.len() + proposal.t.len());
    refs.extend(proposal.c.iter());
    refs.extend(proposal.t.iter());
    if let Some(m) = &proposal.m {
        use crate::vantage::agb::ResolutionEntry;
        match m {
            ResolutionEntry::Full(_, c, t) | ResolutionEntry::Core(_, c, t) => {
                refs.extend(c.iter());
                refs.extend(t.iter());
            }
            ResolutionEntry::Skip(_) => {}
        }
    }
    refs
}

/// Returns batch-proposal references in the fixed order `C`, then `T`.
/// Batch recovery entries are skips and therefore contribute no lane references.
pub fn batch_manifest_refs(proposal: &BatchViewProposal) -> Vec<&BlockRef> {
    let mut refs = Vec::with_capacity(proposal.c.len() + proposal.t.len());
    refs.extend(proposal.c.iter());
    refs.extend(proposal.t.iter());
    refs
}
