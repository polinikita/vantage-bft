// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §4/§9 -- canonical sequence objects and the local
// record store.
//
// PHASE A (record-only shadow mode) is what lives here: every terminally processed view
// gets exactly one hash-chained `SequenceRecord`, and checkpoint boundaries get a head
// that a later phase may announce. Nothing in this module announces, fetches, installs,
// or touches live AGB -- it only observes what the cursor has already terminally output.
//
// The point of building this first, alone, is that it is decisively testable without any
// protocol change: correct parties that terminally process through view `v` MUST derive
// the identical `H_v`. A mismatch between two healthy nodes at a common boundary is a
// determinism bug in this code or in the cursor, and the plan makes it a release blocker
// (§14 Phase A). Announcing a head derived by divergent code would be actively unsafe,
// so determinism has to be established before anything is put on the wire.

use crate::primary::View;
use crate::vantage::agb::{Manifest, Outcome};
use crate::vantage::block;
use crypto::Digest;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Wire/format version of every object in this module. Bumped as a unit: a record's
/// version is covered by its own hash, so two versions can never collide into the same
/// chain even if their bincode shapes happen to coincide.
pub const SEQUENCE_VERSION: u16 = 1;

// Domain tags. Distinct per object so no encoding of one can ever be reinterpreted as
// another under the same session, matching the discipline in `agb.rs`'s digests.
const TAG_GENESIS: &[u8] = b"vantage-sequence-genesis";
const TAG_RECORD: &[u8] = b"vantage-sequence-record";
const TAG_OUTCOME: &[u8] = b"vantage-sequence-outcome";
const TAG_DELTA: &[u8] = b"vantage-sequence-delta";
const TAG_ITEM: &[u8] = b"vantage-sequence-item";

/// §4.2: the canonical state-transfer-only view result.
///
/// Deliberately commits the TERMINAL result rather than the proposal body. A proposal
/// digest alone cannot distinguish `Full` from `Core` -- both name the same `c` -- and a
/// `Skip` has no proposal at all, so a proposal-keyed encoding would make two different
/// output sequences share a head. Recovery safety must not depend on obtaining
/// historical proposal traffic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceOutcome {
    Full { c: Manifest, t: Manifest },
    Core { c: Manifest },
    Skip,
}

impl SequenceOutcome {
    /// `outcome_digest = H("vantage-sequence-outcome" || sid || v || bincode(outcome))`.
    ///
    /// `(view, self)` is serialized as one tuple, which bincode lays out as the
    /// concatenation of its fields -- byte-identical to the plan's `v || encode(outcome)`
    /// while keeping the length framing bincode gives the variant payload.
    pub fn digest(&self, sid: &Digest, view: View) -> Digest {
        let bytes = bincode::serialize(&(view, self)).expect("SequenceOutcome serializes");
        block::domain_hash(TAG_OUTCOME, sid, &bytes)
    }
}

impl From<&Outcome> for SequenceOutcome {
    fn from(outcome: &Outcome) -> Self {
        match outcome {
            Outcome::Full(c, t) => SequenceOutcome::Full {
                c: c.clone(),
                t: t.clone(),
            },
            Outcome::Core(c) => SequenceOutcome::Core { c: c.clone() },
            Outcome::Skip => SequenceOutcome::Skip,
        }
    }
}

/// §4.1 seed: `D_v[0] = H("vantage-sequence-delta" || sid || v || 0)`.
pub fn delta_seed(sid: &Digest, view: View) -> Digest {
    let bytes = bincode::serialize(&(view, 0u64)).expect("(View, u64) serializes");
    block::domain_hash(TAG_DELTA, sid, &bytes)
}

/// §4.1 step: `D_v[i+1] = H("vantage-sequence-item" || sid || v || i || D_v[i] || item)`.
///
/// An incremental item chain rather than a Merkle tree, so an arbitrarily large delta can
/// be streamed and verified chunk by chunk without buffering one oversized frame or
/// materializing a tree. `index` is bound into every step, so a receiver cannot splice a
/// valid chunk in at the wrong offset.
pub fn delta_step(
    sid: &Digest,
    view: View,
    index: u64,
    previous: &Digest,
    item: &Digest,
) -> Digest {
    let mut bytes = bincode::serialize(&(view, index)).expect("(View, u64) serializes");
    bytes.extend_from_slice(&previous.0);
    bytes.extend_from_slice(&item.0);
    block::domain_hash(TAG_ITEM, sid, &bytes)
}

/// Fold a complete delta into its `(delta_len, delta_head)` commitment.
pub fn delta_commitment(sid: &Digest, view: View, items: &[Digest]) -> (u64, Digest) {
    let mut head = delta_seed(sid, view);
    for (index, item) in items.iter().enumerate() {
        head = delta_step(sid, view, index as u64, &head, item);
    }
    (items.len() as u64, head)
}

/// The session's genesis sequence head `H_0`, below which no record exists.
pub fn genesis_head(sid: &Digest) -> Digest {
    block::domain_hash(TAG_GENESIS, sid, &[])
}

/// §4.3: exactly one record per terminally processed view, including `Skip`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRecord {
    pub version: u16,
    pub view: View,
    pub previous_head: Digest,
    pub outcome_digest: Digest,
    pub delta_len: u64,
    pub delta_head: Digest,
}

impl SequenceRecord {
    /// `H_v = H("vantage-sequence-record" || sid || bincode(record_v))`.
    pub fn head(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("SequenceRecord serializes");
        block::domain_hash(TAG_RECORD, sid, &bytes)
    }
}

/// Why a record was refused. Every variant is a local invariant violation, not a
/// remote-input condition -- Phase A has no remote input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceError {
    /// The cursor finalized a view out of order. §4.3 requires a gapless chain, so
    /// recording this would silently produce a head no other party can reproduce.
    OutOfOrder { expected: View, got: View },
}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequenceError::OutOfOrder { expected, got } => write!(
                f,
                "sequence record out of order: expected view {expected}, got {got}"
            ),
        }
    }
}

/// §9's local store. Phase A keeps records in memory and retains them indefinitely,
/// which matches the plan's version-1 choice (§16) and the paper's current
/// indefinite-retention model; bounded GC needs its own proof and a snapshot rule first.
pub struct SequenceStore {
    sid: Digest,
    /// Checkpoint boundary interval `K` in views. Fixed boundaries (rather than "current
    /// head") are what make `f+1` EXACT matches likely while correct cursors sit a few
    /// views apart -- see §4.4.
    interval: u64,
    head: Digest,
    /// The view the next record must carry; `head` is the head through `next_view - 1`.
    next_view: View,
    records: BTreeMap<View, SequenceRecord>,
    /// Boundary view -> head at that boundary. A later phase announces from here.
    boundaries: BTreeMap<View, Digest>,
}

impl SequenceStore {
    /// `interval` of 0 is treated as 1 (every view is a boundary) rather than panicking
    /// or dividing by zero on a misconfiguration.
    pub fn new(sid: Digest, interval: u64) -> Self {
        Self {
            head: genesis_head(&sid),
            sid,
            interval: interval.max(1),
            next_view: 1,
            records: BTreeMap::new(),
            boundaries: BTreeMap::new(),
        }
    }

    /// Current chain head, `H_0` before any record.
    pub fn head(&self) -> &Digest {
        &self.head
    }

    /// Highest view covered by `head`, or 0 when empty.
    pub fn head_view(&self) -> View {
        self.next_view - 1
    }

    pub fn record_for(&self, view: View) -> Option<&SequenceRecord> {
        self.records.get(&view)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The highest checkpoint boundary this party has passed, if any. Phase A only
    /// exposes it for comparison across nodes and for the head-view gauge.
    pub fn latest_boundary(&self) -> Option<(View, &Digest)> {
        self.boundaries.last_key_value().map(|(v, h)| (*v, h))
    }

    pub fn boundary(&self, view: View) -> Option<&Digest> {
        self.boundaries.get(&view)
    }

    fn is_boundary(&self, view: View) -> bool {
        view.is_multiple_of(self.interval)
    }

    /// Record the terminal result of `view` and extend the chain.
    ///
    /// Returns the new head. Refuses anything but exactly the next view: a gap would
    /// produce a head that no other correct party derives, which is precisely the
    /// divergence Phase A exists to detect, so it must fail loudly rather than be papered
    /// over by skipping ahead.
    pub fn record(
        &mut self,
        view: View,
        outcome: &SequenceOutcome,
        output_delta: &[Digest],
    ) -> Result<&Digest, SequenceError> {
        if view != self.next_view {
            return Err(SequenceError::OutOfOrder {
                expected: self.next_view,
                got: view,
            });
        }
        let (delta_len, delta_head) = delta_commitment(&self.sid, view, output_delta);
        let record = SequenceRecord {
            version: SEQUENCE_VERSION,
            view,
            previous_head: self.head.clone(),
            outcome_digest: outcome.digest(&self.sid, view),
            delta_len,
            delta_head,
        };
        self.head = record.head(&self.sid);
        self.records.insert(view, record);
        self.next_view = view + 1;
        if self.is_boundary(view) {
            self.boundaries.insert(view, self.head.clone());
        }
        Ok(&self.head)
    }
}
