use crate::messages::Header;
use crate::primary::View;
use crate::vantage::agb::{Manifest, Outcome};
use crate::vantage::block;
use crypto::{Digest, PublicKey};
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Wire version shared by every sequence object.
pub const SEQUENCE_VERSION: u16 = 1;

const TAG_GENESIS: &[u8] = b"vantage-sequence-genesis";
const TAG_RECORD: &[u8] = b"vantage-sequence-record";
const TAG_OUTCOME: &[u8] = b"vantage-sequence-outcome";
const TAG_DELTA: &[u8] = b"vantage-sequence-delta";
const TAG_ITEM: &[u8] = b"vantage-sequence-item";

/// Canonical terminal result for one view.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceOutcome {
    Full { c: Manifest, t: Manifest },
    Core { c: Manifest },
    Skip,
}

impl SequenceOutcome {
    /// Hashes the outcome with its session and view.
    pub fn digest(&self, sid: &Digest, view: View) -> Digest {
        let bytes = bincode::serialize(&(view, self)).expect("SequenceOutcome serializes");
        block::domain_hash(TAG_OUTCOME, sid, &bytes)
    }

    /// Returns the number of manifest references used as the response-size unit.
    pub fn manifest_items(&self) -> usize {
        match self {
            Self::Full { c, t } => c.len().saturating_add(t.len()),
            Self::Core { c } => c.len(),
            Self::Skip => 0,
        }
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

/// Returns the initial hash for a view's ordered output delta.
pub fn delta_seed(sid: &Digest, view: View) -> Digest {
    let bytes = bincode::serialize(&(view, 0u64)).expect("(View, u64) serializes");
    block::domain_hash(TAG_DELTA, sid, &bytes)
}

/// Extends a delta hash with the item index, previous hash, and item digest.
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

/// Returns the item count and final hash for a complete delta.
pub fn delta_commitment(sid: &Digest, view: View, items: &[Digest]) -> (u64, Digest) {
    let mut head = delta_seed(sid, view);
    for (index, item) in items.iter().enumerate() {
        head = delta_step(sid, view, index as u64, &head, item);
    }
    (items.len() as u64, head)
}

/// Returns the session-specific head before view 1.
pub fn genesis_head(sid: &Digest) -> Digest {
    block::domain_hash(TAG_GENESIS, sid, &[])
}

/// Returns all 32 digest bytes as 64 hexadecimal characters.
pub fn head_hex(head: &Digest) -> String {
    let mut out = String::with_capacity(64);
    for byte in head.0 {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Commits one terminal view to the preceding sequence head.
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
    pub fn head(&self, sid: &Digest) -> Digest {
        let bytes = bincode::serialize(self).expect("SequenceRecord serializes");
        block::domain_hash(TAG_RECORD, sid, &bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceError {
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

/// Retains the ordered record chain and all bodies needed to serve announced checkpoints.
pub struct SequenceStore {
    sid: Digest,
    interval: u64,
    head: Digest,
    next_view: View,
    records: BTreeMap<View, SequenceRecord>,
    boundaries: BTreeMap<View, Digest>,
    outcomes: BTreeMap<View, SequenceOutcome>,
    deltas: BTreeMap<View, Vec<Digest>>,
    headers: HashMap<Digest, Header>,
}

impl SequenceStore {
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
            headers: HashMap::new(),
        }
    }

    pub fn head(&self) -> &Digest {
        &self.head
    }

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

    pub fn latest_boundary(&self) -> Option<(View, &Digest)> {
        self.boundaries.last_key_value().map(|(v, h)| (*v, h))
    }

    pub fn boundary(&self, view: View) -> Option<&Digest> {
        self.boundaries.get(&view)
    }

    /// Retains verified headers referenced by recorded deltas.
    pub(crate) fn retain_verified_headers(&mut self, headers: impl IntoIterator<Item = Header>) {
        for header in headers {
            self.headers.entry(header.id.clone()).or_insert(header);
        }
    }

    pub(crate) fn retained_header(&self, digest: &Digest) -> Option<&Header> {
        self.headers.get(digest)
    }

    /// Returns up to `limit` newest boundaries in ascending view order.
    pub fn recent_boundaries(&self, limit: usize) -> Vec<(View, Digest)> {
        let mut boundaries: Vec<_> = self
            .boundaries
            .iter()
            .rev()
            .take(limit)
            .map(|(view, head)| (*view, head.clone()))
            .collect();
        boundaries.reverse();
        boundaries
    }

    fn is_boundary(&self, view: View) -> bool {
        view.is_multiple_of(self.interval)
    }

    /// Appends exactly the next view and rejects gaps or reordering.
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

/// Carries a first-hand checkpoint claim from an authenticated committee member.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceAnnouncement {
    pub version: u16,
    pub view: View,
    pub head: Digest,
    pub serve_floor: View,
    pub sender: PublicKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IgnoreReason {
    SenderMismatch,
    NotAMember,
    Version,
    Duplicate,
    Equivocation,
    TooFarAhead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnouncementOutcome {
    Counted {
        /// This is true only for the announcement that first reaches the threshold.
        newly_certified: bool,
    },
    Ignored(IgnoreReason),
}

/// Certifies a head after matching first-hand announcements reach the configured threshold.
pub struct CheckpointCollector {
    threshold: usize,
    max_candidate_views: usize,
    future_view_slack: View,
    candidates: BTreeMap<View, BTreeMap<Digest, BTreeSet<PublicKey>>>,
    first_claim: BTreeMap<(View, PublicKey), Digest>,
    equivocators: BTreeSet<PublicKey>,
    certified: BTreeMap<View, Digest>,
}

impl CheckpointCollector {
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

    /// Counts a claim only when its sender matches the authenticated connection identity.
    ///
    /// A sender that announces two heads for one view contributes to neither head.
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

    /// Retracts an equivocator and removes certification if support falls below the threshold.
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

    fn evict_if_needed(&mut self) {
        while self.candidates.len() > self.max_candidate_views {
            let Some((&lowest, _)) = self.candidates.iter().next() else {
                break;
            };
            self.candidates.remove(&lowest);
            self.first_claim.retain(|(view, _), _| *view != lowest);
        }
    }

    pub fn certified_head(&self, above: View) -> Option<(View, Digest)> {
        self.certified
            .iter()
            .rev()
            .find(|(view, _)| **view > above)
            .map(|(view, head)| (*view, head.clone()))
    }

    pub fn support(&self, view: View, head: &Digest) -> usize {
        self.candidates
            .get(&view)
            .and_then(|heads| heads.get(head))
            .map(|senders| senders.len())
            .unwrap_or(0)
    }

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

impl SequenceStore {
    pub fn serve_floor(&self) -> View {
        self.records
            .keys()
            .next()
            .copied()
            .unwrap_or(self.next_view)
    }

    /// Returns a contiguous record range and stops at the first missing view.
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

    /// Returns a contiguous, bounded outcome range.
    ///
    /// The first retained outcome is returned even when it exceeds `max_items`.
    pub fn outcomes_from(
        &self,
        from: View,
        through: View,
        max_views: usize,
        max_items: usize,
    ) -> Vec<SequenceOutcomeEntry> {
        let mut outcomes = Vec::new();
        let mut view = from;
        let mut remaining = max_items;
        while view <= through && outcomes.len() < max_views {
            let Some(outcome) = self.outcomes.get(&view) else {
                break;
            };
            let items = outcome.manifest_items();
            if !outcomes.is_empty() && items > remaining {
                break;
            }
            outcomes.push(SequenceOutcomeEntry {
                view,
                outcome: outcome.clone(),
            });
            remaining = remaining.saturating_sub(items);
            if view == through {
                break;
            }
            view = view.saturating_add(1);
        }
        outcomes
    }

    /// Returns at most `max` delta items and whether the chunk reaches the delta end.
    pub fn delta_chunk(&self, view: View, start: u64, max: usize) -> Option<(Vec<Digest>, bool)> {
        let delta = self.deltas.get(&view)?;
        let start = start as usize;
        if start > delta.len() {
            return None;
        }
        let end = delta.len().min(start.saturating_add(max));
        Some((delta[start..end].to_vec(), end == delta.len()))
    }

    /// Returns consecutive delta chunks bounded by view count and total item count.
    pub fn delta_entries_from(
        &self,
        from_view: View,
        start_index: u64,
        through: View,
        max_views: usize,
        max_items: usize,
    ) -> Vec<SequenceDeltaEntry> {
        let mut entries = Vec::new();
        let mut view = from_view;
        let mut remaining = max_items;
        while view <= through && entries.len() < max_views && remaining > 0 {
            let Some(delta) = self.deltas.get(&view) else {
                break;
            };
            let start = if view == from_view {
                start_index as usize
            } else {
                0
            };
            if start > delta.len() {
                break;
            }
            let end = delta.len().min(start.saturating_add(remaining));
            let items = delta[start..end].to_vec();
            remaining = remaining.saturating_sub(items.len());
            let complete = end == delta.len();
            entries.push(SequenceDeltaEntry {
                view,
                start_index: start as u64,
                items,
                complete,
            });
            if !complete || view == through {
                break;
            }
            view = view.saturating_add(1);
        }
        entries
    }
}

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
pub struct SequenceDeltaRangeRequest {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub target_view: View,
    pub from_view: View,
    pub start_index: u64,
    pub max_views: u32,
    pub max_items: u32,
    pub requester: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDeltaEntry {
    pub view: View,
    pub start_index: u64,
    pub items: Vec<Digest>,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceDeltaRangeChunk {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub entries: Vec<SequenceDeltaEntry>,
    pub sender: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceOutcomeRequest {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub target_view: View,
    pub from_view: View,
    pub max_views: u32,
    pub max_items: u32,
    pub requester: PublicKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceOutcomeEntry {
    pub view: View,
    pub outcome: SequenceOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceOutcomeServe {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub outcomes: Vec<SequenceOutcomeEntry>,
    pub sender: PublicKey,
}

/// Reports that one source cannot serve the request and includes that source's floor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceUnavailable {
    pub version: u16,
    pub transfer_id: u64,
    pub target_head: Digest,
    pub serve_floor: View,
    pub sender: PublicKey,
}

/// Describes invalid remote sequence data; callers must handle every variant without panicking.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChainError {
    Version,
    UnexpectedView {
        /// The verifier expected this view before `got`.
        expected: View,
        got: View,
    },
    BrokenLink {
        /// The record at this view did not extend the running head.
        view: View,
    },
    HeadMismatch {
        /// The chain reached this view without reaching the certified head.
        view: View,
    },
    PastTarget {
        /// The response contained records after this target view.
        target: View,
    },
    UnexpectedIndex {
        /// The verifier expected this item index before `got`.
        expected: u64,
        got: u64,
    },
    DeltaMismatch {
        /// The completed delta for this view did not match its committed head.
        view: View,
    },
    DeltaTooLong {
        /// The delta for this view exceeded its committed item count.
        view: View,
    },
    SkipWithDelta {
        /// This skipped view committed a non-empty delta.
        view: View,
    },
    OutcomeMismatch {
        /// The outcome for this view did not match its committed digest.
        view: View,
    },
}

/// Verifies an ordered record chain against a trusted base and target head.
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

    /// Accepts consecutive records and leaves verifier state unchanged on failure.
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

    /// Checks an outcome against its verified record and requires an empty delta for `Skip`.
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

/// Verifies consecutive delta items against a record's length and final hash.
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

    pub fn take_items(self) -> Option<Vec<Digest>> {
        self.is_complete().then_some(self.items)
    }

    /// Accepts the next consecutive items and leaves verifier state unchanged on failure.
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequenceWant {
    Records { from_view: View },
    Outcomes { from_view: View },
    Deltas { from_view: View, start_index: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferState {
    FetchingRecords,
    FetchingOutcomes,
    FetchingDeltas,
    Verified,
    Exhausted,
}

// One invalid response is tolerated; the second removes that source from the transfer.
const MAX_INVALID_PER_SOURCE: usize = 2;

/// Downloads one certified target from its matching first-hand announcers.
pub struct SequenceTransfer {
    sid: Digest,
    transfer_id: u64,
    target_view: View,
    target_head: Digest,
    sources: Vec<PublicKey>,
    cursor: usize,
    invalid: BTreeMap<PublicKey, usize>,
    chain: ChainVerifier,
    outcomes: BTreeMap<View, SequenceOutcome>,
    deltas: BTreeMap<View, Vec<Digest>>,
    delta_in_flight: BTreeMap<View, DeltaVerifier>,
    outcome_scan_from: Cell<View>,
    delta_scan_from: Cell<View>,
    state: TransferState,
}

impl SequenceTransfer {
    pub fn new(
        sid: Digest,
        transfer_id: u64,
        base_view: View,
        base_head: Digest,
        target_view: View,
        target_head: Digest,
        sources: Vec<PublicKey>,
    ) -> Self {
        let chain = ChainVerifier::new(
            sid.clone(),
            base_view,
            base_head,
            target_view,
            target_head.clone(),
        );
        let state = if sources.is_empty() {
            TransferState::Exhausted
        } else {
            TransferState::FetchingRecords
        };
        Self {
            sid,
            transfer_id,
            target_view,
            target_head,
            sources,
            cursor: 0,
            invalid: BTreeMap::new(),
            chain,
            outcomes: BTreeMap::new(),
            deltas: BTreeMap::new(),
            delta_in_flight: BTreeMap::new(),
            outcome_scan_from: Cell::new(1),
            delta_scan_from: Cell::new(1),
            state,
        }
    }

    pub fn state(&self) -> TransferState {
        self.state
    }

    pub fn transfer_id(&self) -> u64 {
        self.transfer_id
    }

    pub fn target(&self) -> (View, &Digest) {
        (self.target_view, &self.target_head)
    }

    pub fn is_verified(&self) -> bool {
        self.state == TransferState::Verified
    }

    /// Returns output only after the record chain, outcomes, and deltas are verified.
    pub fn verified_output(&self) -> Option<Vec<(View, &SequenceOutcome, &Vec<Digest>)>> {
        if !self.is_verified() {
            return None;
        }
        Some(
            self.outcomes
                .iter()
                .filter_map(|(view, outcome)| {
                    self.deltas.get(view).map(|delta| (*view, outcome, delta))
                })
                .collect(),
        )
    }

    /// Returns each verified boundary head for safe rebasing against local execution.
    pub fn verified_heads(&self) -> Option<Vec<(View, Digest)>> {
        if !self.is_verified() {
            return None;
        }
        Some(
            (1..=self.target_view)
                .filter_map(|view| {
                    self.chain
                        .verified_record(view)
                        .map(|record| (view, record.head(&self.sid)))
                })
                .collect(),
        )
    }

    /// Returns up to `max` disjoint outstanding wants, nearest first.
    ///
    /// Fetch cost is one chunk per round trip, so a gap spanning many chunks is
    /// round-trip bound. Outcomes and deltas are per-view maps and `first_missing` skips
    /// forward over what has already arrived, so distinct ranges can be in flight at once
    /// and merged in any order. Records stay single: they verify as a chain, and a later
    /// record cannot be checked before its predecessor.
    ///
    /// `outcome_span` and `delta_span` are the per-response view caps the caller will put
    /// on the wire; spacing wants by them keeps the ranges from overlapping.
    pub fn wants(&self, max: usize, outcome_span: View, delta_span: View) -> Vec<SequenceWant> {
        match self.state {
            TransferState::FetchingRecords | TransferState::Verified | TransferState::Exhausted => {
                self.want().into_iter().collect()
            }
            TransferState::FetchingOutcomes => self
                .missing_spaced(&self.outcome_scan_from, &self.outcomes, outcome_span, max)
                .into_iter()
                .map(|from_view| SequenceWant::Outcomes { from_view })
                .collect(),
            TransferState::FetchingDeltas => self
                .missing_spaced(&self.delta_scan_from, &self.deltas, delta_span, max)
                .into_iter()
                .map(|view| SequenceWant::Deltas {
                    from_view: view,
                    start_index: self
                        .delta_in_flight
                        .get(&view)
                        .map(|verifier| verifier.next_index())
                        .unwrap_or(0),
                })
                .collect(),
        }
    }

    /// Missing views spaced at least `span` apart, so their responses cannot overlap.
    fn missing_spaced<T>(
        &self,
        cursor: &Cell<View>,
        have: &BTreeMap<View, T>,
        span: View,
        max: usize,
    ) -> Vec<View> {
        let mut out = Vec::new();
        let Some(first) = self.first_missing(cursor, have) else {
            return out;
        };
        out.push(first);
        let span = span.max(1);
        let mut from = first.saturating_add(span);
        while out.len() < max.max(1) && from <= self.target_view {
            let Some(view) = (from..=self.target_view).find(|view| {
                self.chain.verified_record(*view).is_some() && !have.contains_key(view)
            }) else {
                break;
            };
            out.push(view);
            from = view.saturating_add(span);
        }
        out
    }

    /// Returns up to `max` sources beginning at the current failover cursor.
    pub fn next_sources(&self, max: usize) -> Vec<PublicKey> {
        if self.sources.is_empty() {
            return Vec::new();
        }
        let take = max.max(1).min(self.sources.len());
        (0..take)
            .map(|i| self.sources[(self.cursor + i) % self.sources.len()])
            .collect()
    }

    pub fn want(&self) -> Option<SequenceWant> {
        match self.state {
            TransferState::FetchingRecords => Some(SequenceWant::Records {
                from_view: self.chain.next_view(),
            }),
            TransferState::FetchingOutcomes => self
                .first_missing_outcome()
                .map(|from_view| SequenceWant::Outcomes { from_view }),
            TransferState::FetchingDeltas => {
                self.first_missing_delta().map(|view| SequenceWant::Deltas {
                    from_view: view,
                    start_index: self
                        .delta_in_flight
                        .get(&view)
                        .map(|verifier| verifier.next_index())
                        .unwrap_or(0),
                })
            }
            TransferState::Verified | TransferState::Exhausted => None,
        }
    }

    fn first_missing_outcome(&self) -> Option<View> {
        self.first_missing(&self.outcome_scan_from, &self.outcomes)
    }

    fn first_missing_delta(&self) -> Option<View> {
        self.first_missing(&self.delta_scan_from, &self.deltas)
    }

    /// Advances only across the contiguous prefix with both a verified record and body.
    fn first_missing<T>(&self, cursor: &Cell<View>, have: &BTreeMap<View, T>) -> Option<View> {
        loop {
            let view = cursor.get();
            if view > self.target_view {
                break;
            }
            if self.chain.verified_record(view).is_none() {
                break;
            }
            if !have.contains_key(&view) {
                return Some(view);
            }
            cursor.set(view + 1);
        }
        (cursor.get()..=self.target_view)
            .filter(|v| self.chain.verified_record(*v).is_some())
            .find(|v| !have.contains_key(v))
    }

    /// Accepts responses only from a selected source for this transfer and target head.
    fn accepts(&self, from: &PublicKey, transfer_id: u64, head: &Digest) -> bool {
        transfer_id == self.transfer_id && head == &self.target_head && self.sources.contains(from)
    }

    fn penalize(&mut self, from: &PublicKey) {
        let count = self.invalid.entry(*from).or_insert(0);
        *count += 1;
        if *count >= MAX_INVALID_PER_SOURCE {
            self.drop_source(from);
        }
    }

    pub fn drop_source(&mut self, from: &PublicKey) {
        if let Some(index) = self.sources.iter().position(|s| s == from) {
            self.sources.remove(index);
            if index < self.cursor {
                self.cursor -= 1;
            }
        }
        if self.sources.is_empty() {
            self.state = TransferState::Exhausted;
        } else {
            self.cursor %= self.sources.len();
        }
    }

    pub fn rotate(&mut self) {
        if !self.sources.is_empty() {
            self.cursor = (self.cursor + 1) % self.sources.len();
        }
    }

    /// Ignores duplicate and past-target records before verifying the useful range.
    pub fn on_records(
        &mut self,
        chunk: &SequenceRecordChunk,
        from: &PublicKey,
    ) -> Result<(), ChainError> {
        if self.state != TransferState::FetchingRecords
            || !self.accepts(from, chunk.transfer_id, &chunk.target_head)
        {
            return Ok(());
        }
        let already = self.chain.next_view();
        let target = self.target_view;
        let fresh: Vec<SequenceRecord> = chunk
            .records
            .iter()
            .filter(|r| r.view >= already && r.view <= target)
            .cloned()
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        match self.chain.absorb_records(&fresh) {
            Ok(complete) => {
                if complete {
                    self.state = TransferState::FetchingOutcomes;
                    self.advance_if_done();
                }
                Ok(())
            }
            Err(e) => {
                self.penalize(from);
                Err(e)
            }
        }
    }

    /// Ignores overlapping bodies and penalizes only content that fails its record commitment.
    pub fn on_outcomes(
        &mut self,
        serve: &SequenceOutcomeServe,
        from: &PublicKey,
    ) -> Result<(), ChainError> {
        if self.state != TransferState::FetchingOutcomes
            || !self.accepts(from, serve.transfer_id, &serve.target_head)
        {
            return Ok(());
        }
        for entry in &serve.outcomes {
            if self.outcomes.contains_key(&entry.view)
                || self.chain.verified_record(entry.view).is_none()
            {
                continue;
            }
            if let Err(e) = self.chain.check_outcome(entry.view, &entry.outcome) {
                self.penalize(from);
                return Err(e);
            }
            self.outcomes.insert(entry.view, entry.outcome.clone());
        }
        if self.first_missing_outcome().is_none() {
            self.state = TransferState::FetchingDeltas;
            self.advance_if_done();
        }
        Ok(())
    }

    pub fn on_delta(
        &mut self,
        chunk: &SequenceDeltaChunk,
        from: &PublicKey,
    ) -> Result<(), ChainError> {
        if self.state != TransferState::FetchingDeltas
            || !self.accepts(from, chunk.transfer_id, &chunk.target_head)
        {
            return Ok(());
        }
        let entry = SequenceDeltaEntry {
            view: chunk.view,
            start_index: chunk.start_index,
            items: chunk.items.clone(),
            complete: chunk.complete,
        };
        self.on_delta_entry(&entry, from)
    }

    pub fn on_delta_range(
        &mut self,
        chunk: &SequenceDeltaRangeChunk,
        from: &PublicKey,
    ) -> Result<(), ChainError> {
        if self.state != TransferState::FetchingDeltas
            || !self.accepts(from, chunk.transfer_id, &chunk.target_head)
        {
            return Ok(());
        }
        for entry in &chunk.entries {
            self.on_delta_entry(entry, from)?;
        }
        Ok(())
    }

    /// Ignores unsolicited or duplicate delta data and restarts a view after invalid content.
    fn on_delta_entry(
        &mut self,
        entry: &SequenceDeltaEntry,
        from: &PublicKey,
    ) -> Result<(), ChainError> {
        let Some(record) = self.chain.verified_record(entry.view).cloned() else {
            return Ok(());
        };
        if self.deltas.contains_key(&entry.view) {
            return Ok(());
        }
        if self
            .delta_in_flight
            .get(&entry.view)
            .is_some_and(|verifier| entry.start_index < verifier.next_index())
        {
            return Ok(());
        }
        let verifier = self
            .delta_in_flight
            .entry(entry.view)
            .or_insert_with(|| DeltaVerifier::new(self.sid.clone(), entry.view, &record));
        match verifier.absorb(entry.start_index, &entry.items) {
            Ok(complete) => {
                if complete {
                    let verifier = self.delta_in_flight.remove(&entry.view).expect("in flight");
                    let items = verifier.take_items().expect("complete");
                    self.deltas.insert(entry.view, items);
                    self.advance_if_done();
                }
                Ok(())
            }
            Err(e) => {
                self.delta_in_flight.remove(&entry.view);
                self.penalize(from);
                Err(e)
            }
        }
    }

    /// Drops only the source that reports it cannot serve this transfer.
    pub fn on_unavailable(&mut self, unavailable: &SequenceUnavailable, from: &PublicKey) {
        if unavailable.transfer_id != self.transfer_id
            || unavailable.target_head != self.target_head
        {
            return;
        }
        self.drop_source(from);
    }

    fn advance_if_done(&mut self) {
        if self.chain.is_complete()
            && self.first_missing_outcome().is_none()
            && self.first_missing_delta().is_none()
        {
            self.state = TransferState::Verified;
        }
    }
}
