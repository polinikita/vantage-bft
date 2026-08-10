use crate::primary::Height;
use crate::vantage::agb::ViewProposal;
use crate::vantage::BlockRef;
use crypto::Digest;
use serde::{Deserialize, Serialize};

/// Claims a verified lane prefix below the proposal tip.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ShortClaim {
    /// Index in `manifest_refs`.
    pub lane: u16,
    /// Distance below the referenced tip.
    pub delta: Height,
    /// Digest at the claimed height.
    pub head: Digest,
}

/// Positional availability claims against `manifest_refs`.
///
/// Claims are not part of the echo identity and do not affect echo thresholds.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct AvailClaim {
    /// Bit `j` asserts possession of reference `j` at its tip.
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
    pub fn push_short(&mut self, lane: usize, delta: Height, head: Digest) -> bool {
        if delta == 0 || self.is_at_tip(lane) || lane > u16::MAX as usize {
            return false;
        }
        self.short.push(ShortClaim {
            lane: lane as u16,
            delta,
            head,
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

    /// Resolves valid claims and drops malformed or contradictory claims.
    pub fn resolve(&self, refs: &[&BlockRef]) -> Vec<BlockRef> {
        let mut out = Vec::with_capacity(self.claimed());
        for (j, r) in refs.iter().enumerate() {
            if self.is_at_tip(j) {
                out.push((*r).clone());
            }
        }
        for s in &self.short {
            let j = s.lane as usize;
            let Some(r) = refs.get(j) else { continue };
            if self.is_at_tip(j) || s.delta == 0 || s.delta >= r.1 {
                continue;
            }
            out.push((r.0, r.1 - s.delta, s.head.clone()));
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
