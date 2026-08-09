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
use crypto::{Digest, PublicKey};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

/// Full 64-character hex of a head.
///
/// NOT `Digest`'s `Display`, which is deliberately truncated to 16 base64 characters for
/// log readability. A cross-node divergence check compares head IDENTITY, so it must see
/// every byte -- a truncated head that happens to match would report agreement that does
/// not exist.
pub fn head_hex(head: &Digest) -> String {
    let mut out = String::with_capacity(64);
    for byte in head.0 {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Leading 8 bytes of a head as a signed integer, so a dashboard can GRAPH divergence
/// (a Prometheus label cannot be plotted). Lossy on purpose and never authoritative:
/// `sequence_check.py` compares full hex.
pub fn head_prefix_i64(head: &Digest) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&head.0[..8]);
    i64::from_be_bytes(bytes)
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
    /// Terminal outcome bodies, and the ordered per-view output deltas.
    ///
    /// Retained because section 9's correctness rule is that a party never announces a
    /// checkpoint whose state it cannot actually serve. Holding the record alone would
    /// let this node advertise a head and then fail every transfer against it, which
    /// costs a requester its whole recovery -- the `f+1` argument guarantees one correct
    /// announcer EXISTS, so a correct announcer that cannot serve is a liveness bug.
    /// Version 1 retains indefinitely (section 16); bounded GC needs its own proof.
    outcomes: BTreeMap<View, SequenceOutcome>,
    deltas: BTreeMap<View, Vec<Digest>>,
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
            outcomes: BTreeMap::new(),
            deltas: BTreeMap::new(),
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
        self.outcomes.insert(view, outcome.clone());
        self.deltas.insert(view, output_delta.to_vec());
        self.next_view = view + 1;
        if self.is_boundary(view) {
            self.boundaries.insert(view, self.head.clone());
        }
        Ok(&self.head)
    }
}

// ---------------------------------------------------------------- checkpoint collector

/// §4.4: a first-hand claim that the sender's own terminal output through `view` has head
/// `head`, and that it can serve state back to `serve_floor`.
///
/// NOT a certificate and never forwarded as evidence. It counts only when received
/// first-hand over an authenticated channel from its encoded sender, so a third party
/// must collect its own `f+1`. `serve_floor` is informational and cannot strengthen the
/// claim -- a lying floor costs the requester one failed transfer, not safety.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceAnnouncement {
    pub version: u16,
    pub view: View,
    pub head: Digest,
    pub serve_floor: View,
    pub sender: PublicKey,
}

/// Why an announcement did not count. Every variant is remote input, so all of them are
/// ordinary operation rather than local invariant violations -- none may panic or abort.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IgnoreReason {
    /// The encoded `sender` differs from the authenticated connection's identity. The
    /// authoritative identity is ALWAYS the connection, exactly as on Vantage's other
    /// first-hand paths; accepting the payload's own claim would make announcements
    /// forgeable and destroy the `f+1` argument outright.
    SenderMismatch,
    /// Not a committee member, so it cannot be one of the `f+1`.
    NotAMember,
    /// Unknown object version.
    Version,
    /// Same `(view, head)` from a sender already counted for that view. Counted once.
    Duplicate,
    /// A DIFFERENT head from a sender that already announced this view. Recorded and
    /// never counted -- for either head. A Byzantine party that could have both of its
    /// announcements counted would need only `(f+1)/2` accomplices.
    Equivocation,
    /// Too far above anything the fleet is plausibly at; bounds unbounded future-view
    /// memory from a Byzantine peer.
    TooFarAhead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnouncementOutcome {
    /// Counted toward `(view, head)`. `newly_certified` is true on the announcement that
    /// pushed it to `f+1` -- exactly once per `(view, head)`.
    Counted {
        newly_certified: bool,
    },
    Ignored(IgnoreReason),
}

/// §7.1's head collector: the `f+1` matching first-hand rule.
///
/// For `n = 3f+1`, any `f+1` distinct parties contain at least one correct party. A
/// correct party announces `(v, H_v)` only for output it actually, terminally sequenced
/// and can still serve, so `f+1` identical first-hand announcements mean at least one
/// correct party holds exactly that prefix. The rule is LOCAL: these messages are not a
/// transferable certificate.
pub struct CheckpointCollector {
    threshold: usize,
    max_candidate_views: usize,
    future_view_slack: View,
    /// view -> head -> distinct senders that announced it first-hand.
    candidates: BTreeMap<View, BTreeMap<Digest, BTreeSet<PublicKey>>>,
    /// (view, sender) -> the FIRST head that sender announced for that view.
    first_claim: BTreeMap<(View, PublicKey), Digest>,
    equivocators: BTreeSet<PublicKey>,
    certified: BTreeMap<View, Digest>,
}

impl CheckpointCollector {
    /// `threshold` is `f+1`. Derived by the caller from the committee so this type stays
    /// free of committee plumbing and is trivially testable at any `f`.
    pub fn new(threshold: usize, max_candidate_views: usize, future_view_slack: View) -> Self {
        Self {
            threshold: threshold.max(1),
            max_candidate_views: max_candidate_views.max(1),
            future_view_slack,
            candidates: BTreeMap::new(),
            first_claim: BTreeMap::new(),
            equivocators: BTreeSet::new(),
            certified: BTreeMap::new(),
        }
    }

    /// Count one announcement.
    ///
    /// `authenticated_sender` comes from the connection, never from the payload.
    /// `local_view` is roughly where this node believes the fleet is; it only bounds
    /// future-view memory and can be stale without affecting safety.
    pub fn on_announcement(
        &mut self,
        announcement: &SequenceAnnouncement,
        authenticated_sender: &PublicKey,
        is_member: bool,
        local_view: View,
    ) -> AnnouncementOutcome {
        if announcement.version != SEQUENCE_VERSION {
            return AnnouncementOutcome::Ignored(IgnoreReason::Version);
        }
        if &announcement.sender != authenticated_sender {
            return AnnouncementOutcome::Ignored(IgnoreReason::SenderMismatch);
        }
        if !is_member {
            return AnnouncementOutcome::Ignored(IgnoreReason::NotAMember);
        }
        if announcement.view > local_view.saturating_add(self.future_view_slack) {
            return AnnouncementOutcome::Ignored(IgnoreReason::TooFarAhead);
        }

        let key = (announcement.view, *authenticated_sender);
        match self.first_claim.get(&key) {
            Some(previous) if previous == &announcement.head => {
                return AnnouncementOutcome::Ignored(IgnoreReason::Duplicate);
            }
            Some(_) => {
                // A second, different head for the same view from the same sender. Both
                // are now worthless: we cannot tell which (if either) is honest, and
                // counting either would let one Byzantine party supply two of the f+1.
                self.equivocators.insert(*authenticated_sender);
                self.retract(announcement.view, authenticated_sender);
                return AnnouncementOutcome::Ignored(IgnoreReason::Equivocation);
            }
            None => {}
        }
        if self.equivocators.contains(authenticated_sender) {
            return AnnouncementOutcome::Ignored(IgnoreReason::Equivocation);
        }

        self.first_claim.insert(key, announcement.head.clone());
        let senders = self
            .candidates
            .entry(announcement.view)
            .or_default()
            .entry(announcement.head.clone())
            .or_default();
        senders.insert(*authenticated_sender);
        let reached = senders.len() >= self.threshold;
        self.evict_if_needed();

        let newly_certified = reached
            && self
                .certified
                .insert(announcement.view, announcement.head.clone())
                .is_none();
        AnnouncementOutcome::Counted { newly_certified }
    }

    /// Remove an equivocator's vote for `view` from every head it may have reached.
    ///
    /// Retraction can take a candidate back BELOW threshold, but a `(view, head)` already
    /// promoted to `certified` is deliberately left alone: it was certified by `f+1`
    /// DISTINCT senders, so even discounting this one entirely, `f` remain and at least
    /// one of those was correct only if the threshold still holds. Certification is
    /// therefore re-derived rather than assumed -- see `certified_head`.
    fn retract(&mut self, view: View, sender: &PublicKey) {
        if let Some(heads) = self.candidates.get_mut(&view) {
            for senders in heads.values_mut() {
                senders.remove(sender);
            }
            heads.retain(|_, senders| !senders.is_empty());
        }
        if let Some(head) = self.certified.get(&view).cloned() {
            let still = self
                .candidates
                .get(&view)
                .and_then(|heads| heads.get(&head))
                .map(|senders| senders.len())
                .unwrap_or(0);
            if still < self.threshold {
                self.certified.remove(&view);
            }
        }
    }

    /// Bound retained candidate boundaries. Evicts the LOWEST views: the target is always
    /// the highest certified head above the local one, so old candidates are dead weight.
    fn evict_if_needed(&mut self) {
        while self.candidates.len() > self.max_candidate_views {
            let Some((&lowest, _)) = self.candidates.iter().next() else {
                break;
            };
            self.candidates.remove(&lowest);
            self.first_claim.retain(|(view, _), _| *view != lowest);
        }
    }

    /// The highest certified `(view, head)` strictly above `above`, if any.
    pub fn certified_head(&self, above: View) -> Option<(View, Digest)> {
        self.certified
            .iter()
            .rev()
            .find(|(view, _)| **view > above)
            .map(|(view, head)| (*view, head.clone()))
    }

    /// Distinct senders that announced this exact `(view, head)` first-hand.
    pub fn support(&self, view: View, head: &Digest) -> usize {
        self.candidates
            .get(&view)
            .and_then(|heads| heads.get(head))
            .map(|senders| senders.len())
            .unwrap_or(0)
    }

    /// Matching announcers for a certified target -- the source set §7.2 requests from.
    pub fn announcers(&self, view: View, head: &Digest) -> Vec<PublicKey> {
        self.candidates
            .get(&view)
            .and_then(|heads| heads.get(head))
            .map(|senders| senders.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn equivocator_count(&self) -> usize {
        self.equivocators.len()
    }

    pub fn candidate_view_count(&self) -> usize {
        self.candidates.len()
    }
}

// -------------------------------------------------------------------------- serving

impl SequenceStore {
    /// Lowest view this node can fully serve. Version 1 retains indefinitely, so this is
    /// the lowest recorded view (or `next_view` when empty, i.e. "nothing serveable").
    pub fn serve_floor(&self) -> View {
        self.records
            .keys()
            .next()
            .copied()
            .unwrap_or(self.next_view)
    }

    /// Consecutive records starting at `from`, capped at `max`.
    ///
    /// Returns only a CONTIGUOUS run: a requester verifies records by chaining
    /// `previous_head`, so a response with a hole is worthless to it and silently
    /// skipping the hole would look like a chain that does not link. Stops at the first
    /// gap and lets the requester see a short answer.
    pub fn records_from(&self, from: View, max: usize) -> Vec<SequenceRecord> {
        let mut out = Vec::new();
        let mut view = from;
        while out.len() < max {
            let Some(record) = self.records.get(&view) else {
                break;
            };
            out.push(record.clone());
            view += 1;
        }
        out
    }

    pub fn outcome_for(&self, view: View) -> Option<&SequenceOutcome> {
        self.outcomes.get(&view)
    }

    /// One delta chunk: up to `max` digests from `start`, plus whether it reaches the end.
    ///
    /// `None` when the view is not retained at all, which the caller answers with
    /// `SequenceUnavailable` and its authoritative floor rather than an empty chunk --
    /// section 9 forbids silently clamping a request and calling the transfer complete.
    pub fn delta_chunk(&self, view: View, start: u64, max: usize) -> Option<(Vec<Digest>, bool)> {
        let delta = self.deltas.get(&view)?;
        let start = start as usize;
        if start > delta.len() {
            return None;
        }
        let end = delta.len().min(start.saturating_add(max));
        Some((delta[start..end].to_vec(), end == delta.len()))
    }
}

// ------------------------------------------------------------------------ wire types

/// Sections 6/7.2: ask a matching announcer for the record range that links our local head
/// to a certified target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRequest {
    pub version: u16,
    pub transfer_id: u64,
    pub target_view: View,
    pub target_head: Digest,
    pub from_view: View,
    pub max_records: u32,
    pub requester: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRecordChunk {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub records: Vec<SequenceRecord>,
    pub serve_floor: View,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDeltaRequest {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub view: View,
    pub start_index: u64,
    pub max_items: u32,
    pub requester: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDeltaChunk {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub view: View,
    pub start_index: u64,
    pub items: Vec<Digest>,
    pub complete: bool,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceOutcomeRequest {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub view: View,
    pub requester: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceOutcomeServe {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub view: View,
    pub outcome: SequenceOutcome,
    pub sender: PublicKey,
}

/// Section 9: an explicit "I cannot serve that" carrying the authoritative floor, so the
/// requester can try another matching announcer or a newer checkpoint. Never a silent
/// truncation presented as success.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceUnavailable {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub serve_floor: View,
    pub sender: PublicKey,
}

// ----------------------------------------------------------------------- verification

/// Why downloaded state was rejected. All of these are ordinary remote input: up to `f`
/// of the matching announcers may withhold or corrupt every byte, so none may panic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainError {
    Version,
    /// Not the record we are waiting for. Chunks must arrive in view order because each
    /// record is verified against the running head.
    UnexpectedView {
        expected: View,
        got: View,
    },
    /// `previous_head` does not match the head we have built. The core content binding:
    /// a plausible-looking prefix from a Byzantine peer dies here unless it can complete
    /// the chain to the certified head, which it cannot without a hash collision.
    BrokenLink {
        view: View,
    },
    /// The chain reached the target view but not the certified head.
    HeadMismatch {
        view: View,
    },
    /// More records than the target view calls for.
    PastTarget {
        target: View,
    },
    UnexpectedIndex {
        expected: u64,
        got: u64,
    },
    /// The delta produced a different commitment than its record promised.
    DeltaMismatch {
        view: View,
    },
    /// More delta items than `delta_len`.
    DeltaTooLong {
        view: View,
    },
    /// A `Skip` must have an empty delta (section 7.3 step 3).
    SkipWithDelta {
        view: View,
    },
    /// The served outcome does not hash to the record's `outcome_digest`.
    OutcomeMismatch {
        view: View,
    },
}

/// Section 7.2: verify a record range links our local head to the certified target.
///
/// Verification is incremental and strictly ordered so an arbitrarily long range streams
/// without buffering, and so a corrupt chunk is rejected at the point it breaks rather
/// than after a whole range has been accepted.
pub struct ChainVerifier {
    sid: Digest,
    target_view: View,
    target_head: Digest,
    next_view: View,
    head: Digest,
    verified: BTreeMap<View, SequenceRecord>,
    complete: bool,
}

impl ChainVerifier {
    /// `base_view`/`base_head` are what the requester already holds and trusts -- its own
    /// installed head, or genesis.
    pub fn new(
        sid: Digest,
        base_view: View,
        base_head: Digest,
        target_view: View,
        target_head: Digest,
    ) -> Self {
        Self {
            sid,
            target_view,
            target_head,
            next_view: base_view + 1,
            head: base_head,
            verified: BTreeMap::new(),
            complete: false,
        }
    }

    pub fn next_view(&self) -> View {
        self.next_view
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn verified_record(&self, view: View) -> Option<&SequenceRecord> {
        self.verified.get(&view)
    }

    pub fn verified_len(&self) -> usize {
        self.verified.len()
    }

    /// Absorb one chunk of consecutive records. `Ok(true)` once the chain reaches the
    /// certified head.
    ///
    /// A rejected chunk leaves the verifier UNCHANGED, so a Byzantine source cannot
    /// poison the running head and force the requester to restart: the next source's
    /// copy of the same chunk is still accepted.
    pub fn absorb_records(&mut self, records: &[SequenceRecord]) -> Result<bool, ChainError> {
        let mut view = self.next_view;
        let mut head = self.head.clone();
        let mut staged = Vec::with_capacity(records.len());
        for record in records {
            if record.version != SEQUENCE_VERSION {
                return Err(ChainError::Version);
            }
            if view > self.target_view {
                return Err(ChainError::PastTarget {
                    target: self.target_view,
                });
            }
            if record.view != view {
                return Err(ChainError::UnexpectedView {
                    expected: view,
                    got: record.view,
                });
            }
            if record.previous_head != head {
                return Err(ChainError::BrokenLink { view });
            }
            head = record.head(&self.sid);
            staged.push(record.clone());
            view += 1;
        }
        // Only at the target may the head be compared: an intermediate head is expected
        // to differ from the target.
        let reached = view > self.target_view;
        if reached && head != self.target_head {
            return Err(ChainError::HeadMismatch {
                view: self.target_view,
            });
        }
        for record in staged {
            self.verified.insert(record.view, record);
        }
        self.next_view = view;
        self.head = head;
        self.complete = reached;
        Ok(reached)
    }

    /// Section 7.3 steps 1 and 3: a served outcome must hash to the verified record's
    /// commitment, and a `Skip` must carry an empty delta.
    pub fn check_outcome(&self, view: View, outcome: &SequenceOutcome) -> Result<(), ChainError> {
        let record = self.verified.get(&view).ok_or(ChainError::UnexpectedView {
            expected: self.next_view,
            got: view,
        })?;
        if outcome.digest(&self.sid, view) != record.outcome_digest {
            return Err(ChainError::OutcomeMismatch { view });
        }
        if matches!(outcome, SequenceOutcome::Skip) && record.delta_len != 0 {
            return Err(ChainError::SkipWithDelta { view });
        }
        Ok(())
    }
}

/// Section 7.3 step 2: stream one view's output digests and verify them against the
/// `(delta_len, delta_head)` its verified record committed to.
pub struct DeltaVerifier {
    sid: Digest,
    view: View,
    expected_len: u64,
    expected_head: Digest,
    next_index: u64,
    running: Digest,
    items: Vec<Digest>,
}

impl DeltaVerifier {
    pub fn new(sid: Digest, view: View, record: &SequenceRecord) -> Self {
        Self {
            running: delta_seed(&sid, view),
            sid,
            view,
            expected_len: record.delta_len,
            expected_head: record.delta_head.clone(),
            next_index: 0,
            items: Vec::new(),
        }
    }

    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    pub fn is_complete(&self) -> bool {
        self.next_index == self.expected_len
    }

    /// The verified digests, only once the whole delta checks out.
    pub fn take_items(self) -> Option<Vec<Digest>> {
        self.is_complete().then_some(self.items)
    }

    /// Absorb consecutive items at `start_index`. `Ok(true)` when the delta is complete
    /// AND its head matches. Rejects gaps, wrong offsets, and overlong deltas; leaves the
    /// verifier unchanged on error.
    pub fn absorb(&mut self, start_index: u64, items: &[Digest]) -> Result<bool, ChainError> {
        if start_index != self.next_index {
            return Err(ChainError::UnexpectedIndex {
                expected: self.next_index,
                got: start_index,
            });
        }
        if self.next_index + items.len() as u64 > self.expected_len {
            return Err(ChainError::DeltaTooLong { view: self.view });
        }
        let mut running = self.running.clone();
        let mut index = self.next_index;
        for item in items {
            running = delta_step(&self.sid, self.view, index, &running, item);
            index += 1;
        }
        let complete = index == self.expected_len;
        if complete && running != self.expected_head {
            return Err(ChainError::DeltaMismatch { view: self.view });
        }
        self.running = running;
        self.next_index = index;
        self.items.extend_from_slice(items);
        Ok(complete)
    }
}
