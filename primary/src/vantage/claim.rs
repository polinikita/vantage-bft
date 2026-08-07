// AVAIL-ECHO-SPEC.md -- positional availability acknowledgments.
//
// An availability acknowledgment `<ack, a, k, h>` (PHASE3-SPEC.md §3.2, `DirectPub`)
// asserts possession of the WHOLE valid lane prefix named by `h`. The proved algorithm
// puts one explicit `(a,k,h)` tuple on the wire per lane advance; this module encodes the
// same statements POSITIONALLY against a proposal whose references both sides already
// agree on, because `hash(B_v)` commits to them.
//
// WHY: measured on the 2026-08-07 n=100 run at `e46f6e1`, per node,
// `network_bytes_sent_total{type=...}`:
//     VantageAvail  18.330 MB/s   92.2%   <-- explicit tuples
//     Batch          0.527          2.6%
//     Header         0.286          1.4%
//     VantagePropose 0.264          1.3%
//     total         19.880
// against autobahn-optimistic's 4.34 MB/s, whose largest term is `Header` at 72.6% and
// which has no acknowledgment layer at all. Avail messages are 9,258 B against a 2,203 B
// average, so they are 21.8% of messages but 92.2% of bytes. Replacing a
// reference-per-lane with a bit-per-lane on a message already being sent is therefore
// worth ~18 MB/s per node, not a few percent.
//
// The claim predicate is IDENTICAL to `Cursor::expand`'s precondition (AVAIL-ECHO-SPEC
// §6.1): for manifest entry `(A,h,d)` the cursor calls `collect_verified_suffix(.., d)`
// and needs every block of `chain(d)` from its watermark through `h`. So a set bit means
// exactly "I can expand this entry" -- a re-encoding, not an approximation.

use crate::primary::Height;
use crate::vantage::agb::ViewProposal;
use crate::vantage::BlockRef;
use crypto::Digest;
use serde::{Deserialize, Serialize};

/// A sender holds a STRICTLY SHORTER prefix of lane `lane` than the proposal names.
///
/// Ragged lane frontiers make this necessary rather than optional: each manifest lists at
/// most one entry per author (the newest), so a party mid-catch-up would otherwise
/// contribute no acknowledgment at all for that lane -- and the acknowledgment rate would
/// collapse exactly when nodes are recovering, which is when availability matters most.
///
/// `head` anchors the claim to a chain. Without it a height-only claim is ambiguous under
/// an equivocating author (two chains at one height), and the receiver could credit a
/// prefix the sender does not hold. That is liveness-only -- `Cursor` still verifies via
/// `block_ok` before output, so the cost of being wrong is an unnecessary repair, never a
/// safety violation -- but 32 bytes on a rare message is cheap insurance.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ShortClaim {
    /// Index into `manifest_refs(proposal)`.
    pub lane: u16,
    /// `manifest_refs(proposal)[lane].1 - delta` is the height actually held.
    pub delta: Height,
    /// The digest AT the held height, on the chain named by the proposal's entry.
    pub head: Digest,
}

/// Per-lane availability claims against `manifest_refs(proposal)`, riding an AGB echo.
///
/// Lives OUTSIDE the echo's counting identity, exactly like `Echo::wish` (PHASE5 §2/W4)
/// and `Echo::origin` (PHASE6 §3): `proposal_digest` never reads it, and two echoes
/// counted as the same statement may carry different claims. That precedent is what makes
/// this a pure addition rather than a change to any AGB threshold.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct AvailClaim {
    /// Bit `j` (word `j/64`, bit `j%64`) set => the sender holds
    /// `manifest_refs(proposal)[j]` EXACTLY, i.e. delta 0.
    pub at_tip: Vec<u64>,
    /// Lanes held only to a shorter prefix, ascending by `lane`. A lane absent from both
    /// `at_tip` and here carries NO claim.
    pub short: Vec<ShortClaim>,
}

impl AvailClaim {
    /// Bit-vector sized for `len` lanes, no claims set.
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

    /// `(lane, delta, head)` for a lane held only to a shorter prefix. Rejects a lane
    /// already marked at-tip: the two are mutually exclusive claims about one lane, and
    /// admitting both would let one echo assert two different heights.
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

    /// Total lanes claimed, at-tip plus short. Only for metrics and tests.
    pub fn claimed(&self) -> usize {
        self.at_tip
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum::<usize>()
            + self.short.len()
    }

    /// Every claim as the `(author, height, digest)` reference it denotes, resolved
    /// against `refs` (the receiver's own `manifest_refs` of the same proposal).
    ///
    /// Ill-formed claims are DROPPED, not errors: a Byzantine sender may put anything on
    /// the wire, and a dropped claim costs it only its own acknowledgment. Dropped are an
    /// out-of-range lane, a `delta` at or past the entry's height (which would name a
    /// non-positive height -- `Height` is unsigned, so this must be checked before
    /// subtracting), and a lane claimed both at-tip and short.
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

/// The canonical reference vector a claim's lane indices address: `C`, then `T`, then the
/// manifests carried inside `M`.
///
/// All three are needed, and finding that out was the point of AVAIL-ECHO-SPEC §6.1's
/// audit. Manifests reach `Cursor` through three paths, not one:
///   - `agb.rs`'s seal:            `Outcome::Full(proposal.c(), proposal.t())`  -> C and T
///   - `control.rs`'s `derive_anchor`: `ResolutionEntry::Full/Core(u, C_u, T_u)` -> inside M
///   - `cursor.rs`'s gopen path:   `input.completed` -> `expand(&c)`            -> C
///
/// A vector over `C` alone would have silently failed to acknowledge every
/// anchor-resolved view -- precisely the recovery path that matters at n=100.
///
/// Order is fixed and total, and every element is covered by `proposal_digest`, so sender
/// and receiver derive the same indices with no negotiation. `ResolutionEntry::Skip`
/// contributes nothing, which keeps indices stable across variants.
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
