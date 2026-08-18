use crate::messages::Header;
use crate::primary::Height;
use crate::vantage::block::block_ok;
use crate::vantage::index::{ByPair, ByRef, CommitteeIndex, Slot};
use crate::vantage::lanes::SharedBlocks;
use crate::vantage::{BlockRef, Effect};
use config::Committee;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// Maximum peers requested in the initial fan-out round.
pub(crate) const FANOUT_FIRST: usize = 4;

/// Maximum digests escalated by one `retry_requests` call.
pub(crate) const FANOUT_ESCALATE_BUDGET: usize = 256;

/// Initial request-emission limit per one-second tick.
pub(crate) const RECOVERY_EMIT_START: usize = 2_048;

/// Minimum request-emission limit per one-second tick.
pub(crate) const RECOVERY_EMIT_MIN: usize = 256;

/// Maximum request-emission limit per one-second tick.
pub(crate) const RECOVERY_EMIT_MAX: usize = 65_536;

/// Core queue length that limits congested fan-out width.
pub(crate) const CORE_QUEUE_CONGESTED: usize = 256;

/// Maximum requests counted as in flight.
pub(crate) const RECOVERY_IN_FLIGHT_MAX: usize = 512;

/// One-second ticks before an unanswered round releases its in-flight slots.
pub(crate) const ASK_TIMEOUT_TICKS: u64 = 4;

/// Maximum peers requested per digest while the core queue is congested.
pub(crate) const ESCALATE_WIDTH_MAX: usize = 8;

/// One-second ticks before a fully covered digest starts a new request cycle.
pub(crate) const REFETCH_COOLDOWN_TICKS: u64 = 10;

struct FanoutState {
    author: PublicKey,
    height: Height,
    /// Deterministic start offset for peers without holder information.
    start: usize,
    /// Distinct peers requested during this cycle; this value never decreases.
    asked: usize,
    next_width: usize,
    /// Requests from this digest currently counted in `Repairer::in_flight`.
    in_flight_asks: usize,
    /// One-second tick when the latest round was emitted.
    asked_at: u64,
}

/// Repair state for one exact `(author, height, digest)` reference.
#[derive(Default)]
struct RepairPosition {
    /// Verified through genesis and retained; this flag is permanent.
    settled: bool,
    /// Authorized and not yet settled.
    pending_settle: bool,
    /// The missing digest this reference waits for; each reference waits on at most one.
    blocked_at: Option<Digest>,
}

pub struct Repairer {
    committee: Committee,
    index: Arc<CommitteeIndex>,
    peers: Vec<PublicKey>,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// Settlement state of every reference admitted for repair.
    positions: ByRef<RepairPosition>,
    /// References not yet settled.
    pending_settle: usize,
    /// References waiting for each missing digest.
    blocked_on: HashMap<Digest, HashSet<BlockRef>>,
    settle_calls: u64,
    /// `(peer, digest)` requests emitted at most once per request cycle.
    requested: HashSet<(PublicKey, Digest)>,
    /// Digests requested by this node and therefore permitted through `on_serve`.
    requested_hashes: HashSet<Digest>,
    fanout: HashMap<Digest, FanoutState>,
    /// Outstanding fan-out entries ordered by ascending block height.
    fanout_queue: BTreeSet<(Height, Digest)>,
    /// Highest confirmed height for each `(author, peer)` pair.
    holders: ByPair<Height>,
    /// Earliest one-second tick for each digest's next request cycle.
    refetch_at: HashMap<Digest, u64>,
    emit_budget: usize,
    emit_ceiling: usize,
    last_bulk_drops: u64,
    core_queue_len: usize,
    /// Number of `retry_requests` calls; each call represents one second.
    ticks: u64,
    in_flight: usize,
    /// Requesters waiting for each digest, including digests not yet held.
    pending_req: HashMap<Digest, HashSet<PublicKey>>,
    /// `(requester, digest)` responses emitted at most once.
    answered: HashSet<(PublicKey, Digest)>,

    metrics: Option<Arc<Metrics>>,
    /// Cached handle to `vantage_repair_settle_busy_us`. Settle runs hundreds of thousands
    /// of times a second, so a registry lookup per call would itself distort the measurement.
    ut_settle: Option<prometheus::IntCounter>,
    walk_steps_settle: u64,
}

impl Repairer {
    pub fn new(
        name: PublicKey,
        committee: Committee,
        sid: Digest,
        genesis: Digest,
        max_block_payload: usize,
        blocks: SharedBlocks,
    ) -> Self {
        let index = CommitteeIndex::new(&committee);
        Self {
            peers: committee
                .others_primaries(&name)
                .into_iter()
                .map(|(pk, _)| pk)
                .collect(),
            committee,
            positions: ByRef::new(&index),
            holders: ByPair::new(index.clone()),
            index,
            sid,
            genesis,
            max_block_payload,
            blocks,
            pending_settle: 0,
            blocked_on: HashMap::new(),
            settle_calls: 0,
            ut_settle: None,
            requested: HashSet::new(),
            requested_hashes: HashSet::new(),
            refetch_at: HashMap::new(),
            fanout: HashMap::new(),
            fanout_queue: BTreeSet::new(),
            emit_budget: RECOVERY_EMIT_START,
            emit_ceiling: RECOVERY_EMIT_START,
            last_bulk_drops: 0,
            core_queue_len: 0,
            ticks: 0,
            in_flight: 0,
            pending_req: HashMap::new(),
            answered: HashSet::new(),
            metrics: None,
            walk_steps_settle: 0,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Authorizes an exact block reference and attempts to verify and retain its prefix.
    pub fn authorize(&mut self, r: BlockRef) -> Vec<Effect> {
        let mut effects = Vec::new();
        // One walk covers one author, so the lane resolves once for the authorization and
        // every settlement step below it.
        let lane = self.index.slot(&r.0);
        self.note_authorized(&lane, r.1, &r.2);
        self.settle(&lane, r, &mut effects);
        effects
    }

    /// Permits a sequence-requested digest through `on_serve` without coordinate authorization.
    pub fn expect_sequence_digest(&mut self, digest: Digest) {
        self.requested_hashes.insert(digest);
    }

    fn note_authorized(&mut self, lane: &Slot, height: Height, digest: &Digest) {
        let (position, _) = self.positions.entry(lane, height, digest);
        if !position.settled && !position.pending_settle {
            position.pending_settle = true;
            self.pending_settle += 1;
        }
    }

    fn mark_settled(&mut self, lane: &Slot, height: Height, digest: &Digest) {
        let (position, _) = self.positions.entry(lane, height, digest);
        position.settled = true;
        if position.pending_settle {
            position.pending_settle = false;
            self.pending_settle -= 1;
        }
    }

    /// Retries only references indexed as waiting for `digest`.
    pub fn on_block_available(&mut self, digest: Digest) -> Vec<Effect> {
        let mut effects = Vec::new();
        if let Some(state) = self.fanout.remove(&digest) {
            self.in_flight = self.in_flight.saturating_sub(state.in_flight_asks);
        }
        self.refetch_at.remove(&digest);
        let Some(waiting) = self.blocked_on.remove(&digest) else {
            return effects;
        };
        for r in waiting {
            let lane = self.index.slot(&r.0);
            let mut pending = false;
            if let Some(position) = self.positions.get_mut(&lane, r.1, &r.2) {
                position.blocked_at = None;
                pending = position.pending_settle;
            }
            if pending {
                self.settle(&lane, r, &mut effects);
            }
        }
        effects
    }

    /// Moves each reference to the bucket for its current missing digest.
    ///
    /// One settlement walk pins one author, so every reference here shares `lane`.
    fn record_blocked(&mut self, lane: &Slot, refs: &[BlockRef], h: &Digest) {
        for r in refs {
            let previous = self
                .positions
                .get_mut(lane, r.1, &r.2)
                .and_then(|position| position.blocked_at.replace(h.clone()));
            if let Some(prev) = previous {
                if prev == *h {
                    continue;
                }
                if let Some(bucket) = self.blocked_on.get_mut(&prev) {
                    bucket.remove(r);
                    if bucket.is_empty() {
                        self.blocked_on.remove(&prev);
                    }
                }
            }
            self.blocked_on
                .entry(h.clone())
                .or_default()
                .insert(r.clone());
        }
    }

    fn fan_out(&mut self, h: &Digest, effects: &mut Vec<Effect>) {
        let n = self.peers.len();
        if n == 0 {
            return;
        }
        let (author, height, start, asked, take) = {
            let Some(entry) = self.fanout.get_mut(h) else {
                return;
            };
            if entry.asked >= n {
                return;
            }
            let width_cap = if self.core_queue_len >= CORE_QUEUE_CONGESTED {
                ESCALATE_WIDTH_MAX.min(n)
            } else {
                n
            };
            if entry.asked >= width_cap {
                return;
            }
            let take = entry.next_width.min(width_cap - entry.asked);
            (entry.author, entry.height, entry.start, entry.asked, take)
        };

        let mut targets: Vec<PublicKey> = Vec::with_capacity(take);
        for peer in self.likely_holders(&author, height, n, h) {
            if targets.len() >= take {
                break;
            }
            if !self.requested.contains(&(peer, h.clone())) {
                targets.push(peer);
            }
        }
        let mut scanned = 0;
        while targets.len() < take && scanned < n {
            let peer = self.peers[(start + asked + scanned) % n];
            if !self.requested.contains(&(peer, h.clone())) && !targets.contains(&peer) {
                targets.push(peer);
            }
            scanned += 1;
        }

        let now_tick = self.ticks;
        let room = RECOVERY_IN_FLIGHT_MAX.saturating_sub(self.in_flight);
        let wanted = targets.len();
        targets.truncate(self.emit_budget.min(room));
        let emitted = targets.len();
        if emitted == 0 && (self.emit_budget == 0 || room == 0) {
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_repair_budget_deferred_total
                    .inc_by(wanted as u64);
            }
            // Preserve fan-out state so the next tick retries the same round.
            return;
        }
        self.emit_budget -= emitted;
        self.in_flight += emitted;
        for peer in targets {
            // Record a request only after both emission limits admit it.
            if self.requested.insert((peer, h.clone())) {
                self.requested_hashes.insert(h.clone());
                effects.push(Effect::RequestTo(peer, h.clone()));
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repairs_requested.inc();
                }
            }
        }
        if let Some(entry) = self.fanout.get_mut(h) {
            debug_assert!(
                emitted > 0,
                "fan_out emitted nothing while asked ({}) < n ({}): `asked` has drifted from \
                 the `requested` set",
                entry.asked,
                n
            );
            entry.asked = if emitted == 0 {
                n
            } else {
                entry.asked + emitted
            };
            entry.in_flight_asks += emitted;
            entry.asked_at = now_tick;
            // Increase width only after emitting a round.
            entry.next_width = entry.next_width.saturating_mul(2);
        }
    }

    fn begin_fanout(
        &mut self,
        h: &Digest,
        author: PublicKey,
        height: Height,
        effects: &mut Vec<Effect>,
    ) {
        let n = self.peers.len();
        if n == 0 {
            return;
        }
        self.fanout.entry(h.clone()).or_insert_with(|| FanoutState {
            author,
            height,
            start: Self::fanout_start(h, n),
            asked: 0,
            next_width: FANOUT_FIRST,
            in_flight_asks: 0,
            asked_at: 0,
        });
        self.fan_out(h, effects);
    }

    fn adapt_recovery_ceiling(&mut self) {
        let new_drops = match &self.metrics {
            Some(metrics) => {
                let now = metrics.vantage_bulk_inbound_dropped_total.get();
                let d = now.saturating_sub(self.last_bulk_drops);
                self.last_bulk_drops = now;
                d
            }
            None => 0,
        };
        self.emit_ceiling = if new_drops > 0 {
            (self.emit_ceiling / 2).max(RECOVERY_EMIT_MIN)
        } else {
            self.emit_ceiling.saturating_mul(2).min(RECOVERY_EMIT_MAX)
        };
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_repair_emit_ceiling
                .set(self.emit_ceiling as i64);
            metrics.vantage_repair_in_flight.set(self.in_flight as i64);
            if new_drops > 0 {
                metrics.vantage_repair_ceiling_halved_by_drops.inc();
            } else {
                metrics.vantage_repair_ceiling_raised.inc();
            }
        }
    }

    pub fn observe_core_queue(&mut self, len: usize) {
        self.core_queue_len = len;
    }

    /// Records a monotonic claim that `peer` holds `author` through `height`.
    ///
    /// An unclaimed pair reads as height zero, and every query names a height of at least
    /// one, so an explicit zero claim and no claim select the same peers.
    pub fn note_holder(&mut self, peer: PublicKey, author: PublicKey, height: Height) {
        let peer = self.index.slot(&peer);
        let lane = self.index.slot(&author);
        if height > self.holders.get(&peer, &lane).copied().unwrap_or(0) {
            self.holders.insert(&peer, &lane, height);
        }
    }

    fn likely_holders(
        &self,
        author: &PublicKey,
        height: Height,
        want: usize,
        h: &Digest,
    ) -> Vec<PublicKey> {
        let lane = self.index.slot(author);
        let mut candidates: Vec<(Height, PublicKey)> = self
            .holders
            .row(&lane)
            .filter(|(_, held)| **held >= height)
            .map(|(peer, held)| (*held, peer.key()))
            .collect();
        // Prefer greater confirmed heights, then use a deterministic per-digest order.
        candidates.sort_unstable_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| Self::holder_rank(&a.1, h).cmp(&Self::holder_rank(&b.1, h)))
        });
        candidates.into_iter().take(want).map(|(_, p)| p).collect()
    }

    fn holder_rank(peer: &PublicKey, h: &Digest) -> u64 {
        let mut pk = [0u8; 8];
        let mut dg = [0u8; 8];
        pk.copy_from_slice(&peer.0[..8]);
        dg.copy_from_slice(&h.0[..8]);
        u64::from_le_bytes(pk) ^ u64::from_le_bytes(dg)
    }

    fn fanout_start(h: &Digest, n: usize) -> usize {
        let mut acc = [0u8; 8];
        acc.copy_from_slice(&h.0[..8]);
        (u64::from_le_bytes(acc) % n as u64) as usize
    }

    pub fn retry_requests(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.ticks += 1;
        // Release timed-out slots before selecting this tick's requests.
        let now_tick = self.ticks;
        let mut reclaimed = 0usize;
        for (_, state) in self.fanout.iter_mut() {
            if state.in_flight_asks > 0
                && now_tick.saturating_sub(state.asked_at) >= ASK_TIMEOUT_TICKS
            {
                reclaimed += state.in_flight_asks;
                state.in_flight_asks = 0;
            }
        }
        if reclaimed > 0 {
            self.in_flight = self.in_flight.saturating_sub(reclaimed);
            if let Some(metrics) = &self.metrics {
                metrics
                    .vantage_repair_asks_reclaimed_total
                    .inc_by(reclaimed as u64);
            }
        }
        self.adapt_recovery_ceiling();
        self.emit_budget = self.emit_ceiling;
        let budget = FANOUT_ESCALATE_BUDGET.min(self.fanout_queue.len());
        // Snapshot the batch because emitted entries may be reinserted into the ordered set.
        let batch: Vec<(Height, Digest)> = self.fanout_queue.iter().take(budget).cloned().collect();
        for key in batch {
            self.fanout_queue.remove(&key);
            let (_, h) = &key;
            if !self.fanout.contains_key(h) {
                continue;
            }
            if !self.blocked_on.contains_key(h) {
                if let Some(state) = self.fanout.remove(h) {
                    self.in_flight = self.in_flight.saturating_sub(state.in_flight_asks);
                }
                continue;
            }
            let before = effects.len();
            self.fan_out(&key.1.clone(), &mut effects);
            if effects.len() > before {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repair_fanout_escalations_total.inc();
                }
            }
            if self
                .fanout
                .get(&key.1)
                .is_some_and(|s| s.asked < self.peers.len())
            {
                self.fanout_queue.insert(key);
            } else {
                if let Some(state) = self.fanout.remove(&key.1) {
                    self.in_flight = self.in_flight.saturating_sub(state.in_flight_asks);
                }
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics
                .vantage_repair_fanout_pending
                .set(self.fanout.len() as i64);
        }
        effects
    }

    /// Accepts only requested, valid blocks and caches them without direct-publish provenance.
    ///
    /// A block advances settlement only when its author, height, and digest match the
    /// authorized reference checked by `settle`.
    pub fn on_serve(&mut self, block: Header) -> Vec<Effect> {
        if !self.requested_hashes.contains(&block.id) {
            return Vec::new();
        }
        self.accept_served(block)
    }

    /// Accepts a header authorized by the active sequence request window.
    pub fn on_sequence_serve(&mut self, block: Header) -> Vec<Effect> {
        self.accept_served(block)
    }

    fn accept_served(&mut self, block: Header) -> Vec<Effect> {
        if !block_ok(&block, &self.committee, &self.sid, self.max_block_payload) {
            return Vec::new();
        }
        let digest = block.id.clone();
        {
            let mut blocks = self.blocks.lock();
            blocks.upsert(block, false, true, false, true);
        }
        let mut effects = vec![Effect::BlockCached(digest.clone())];
        effects.extend(self.on_block_available(digest));
        effects
    }

    /// Records a request even when the block is absent, then serves it if retained.
    pub fn on_request(&mut self, requester: PublicKey, h: Digest) -> Vec<Effect> {
        if !self.answered.contains(&(requester, h.clone())) {
            self.pending_req
                .entry(h.clone())
                .or_default()
                .insert(requester);
        }
        let mut effects = Vec::new();
        self.try_serve(&h, &mut effects);
        effects
    }

    /// Clears the at-most-once mark for a response that never reached the transport.
    ///
    /// Serving is loss-free only while the mark and the send agree: a dropped serve must
    /// release the pair, otherwise a repeat request from the same peer is never answered.
    pub(crate) fn unanswer(&mut self, peer: &PublicKey, digest: &Digest) {
        self.answered.remove(&(*peer, digest.clone()));
    }

    pub(crate) fn walk_steps_settle(&self) -> u64 {
        self.walk_steps_settle
    }

    fn settle(&mut self, lane: &Slot, r: BlockRef, effects: &mut Vec<Effect>) -> bool {
        self.settle_calls += 1;
        let _timer = self.metrics.as_ref().map(|metrics| {
            // Settling is reached from several top-level sections, so its time goes to the
            // dedicated cross-cutting counter, never to a `utilization_timer` label.
            let counter = self
                .ut_settle
                .get_or_insert_with(|| metrics.vantage_repair_settle_busy_us.clone())
                .clone();
            metrics.vantage_repair_settle_calls_total.inc();
            metrics::UtilizationTimer::from_counter(counter)
        });
        let mut cur = r;
        let mut frames: Vec<BlockRef> = Vec::new();
        let mut steps: u64 = 0;
        let verified = loop {
            steps += 1;
            if self
                .positions
                .get(lane, cur.1, &cur.2)
                .is_some_and(|position| position.settled)
            {
                break true;
            }
            let (author, height, h) = cur.clone();
            if height == 0 {
                break true;
            }

            let (parent_digest, verified_present) = {
                let blocks = self.blocks.lock();
                let entry = blocks.get(&h);
                let parent = entry.and_then(|entry| {
                    let b = &entry.block;
                    if b.author == author
                        && b.height == height
                        && b.id == h
                        && entry.block_ok_verified
                    {
                        Some(b.parent_cert.header_digest.clone())
                    } else {
                        None
                    }
                });
                let verified = entry.is_some_and(|entry| entry.block_ok_verified);
                (parent, verified)
            };

            let Some(parent_h) = parent_digest else {
                if let Some(metrics) = &self.metrics {
                    metrics.vantage_repair_fanout_loops_total.inc();
                }
                if !self.requested_hashes.contains(&h) && !self.fanout.contains_key(&h) {
                    self.fanout_queue.insert((height, h.clone()));
                    self.begin_fanout(&h, author, height, effects);
                } else if self.requested_hashes.contains(&h) && !self.fanout.contains_key(&h) {
                    let now = self.ticks;
                    let ready = self.refetch_at.get(&h).is_none_or(|&at| now >= at);
                    if !verified_present && ready {
                        self.refetch_at
                            .insert(h.clone(), now + REFETCH_COOLDOWN_TICKS);
                        let peers = self.peers.clone();
                        for peer in peers {
                            self.requested.remove(&(peer, h.clone()));
                        }
                        self.requested_hashes.remove(&h);
                        if let Some(metrics) = &self.metrics {
                            metrics.vantage_repair_refetch_campaigns_total.inc();
                        }
                        log::debug!(
                            "vantage repair: retrying missing block author={author} \
                             height={height}"
                        );
                        self.fanout_queue.insert((height, h.clone()));
                        self.begin_fanout(&h, author, height, effects);
                    }
                }
                frames.push(cur.clone());
                self.record_blocked(lane, &frames, &h);
                break false;
            };

            if height == 1 {
                // Height one must link directly to the configured genesis digest.
                if parent_h != self.genesis {
                    break false;
                }
                frames.push(cur);
                break true;
            }

            self.note_authorized(lane, height - 1, &parent_h);
            frames.push(cur);
            cur = (author, height - 1, parent_h);
        };
        self.walk_steps_settle += steps;

        if verified {
            // Retain ancestors before descendants so retention remains prefix-closed.
            while let Some(frame) = frames.pop() {
                self.retain_and_serve(&frame, effects);
                self.mark_settled(lane, frame.1, &frame.2);
            }
        }
        verified
    }

    fn retain_and_serve(&mut self, r: &BlockRef, effects: &mut Vec<Effect>) {
        let h = r.2.clone();
        let block = {
            let mut blocks = self.blocks.lock();
            blocks.mark_retained(&h);
            blocks
                .get(&h)
                .and_then(|entry| entry.retained.then(|| entry.block.clone()))
        };
        if let Some(block) = block {
            self.serve_pending(&h, block, effects);
        }
    }

    fn try_serve(&mut self, h: &Digest, effects: &mut Vec<Effect>) {
        let block = {
            let blocks = self.blocks.lock();
            match blocks.get(h) {
                Some(entry) if entry.retained => Some(entry.block.clone()),
                _ => None,
            }
        };
        let Some(block) = block else {
            return;
        };
        self.serve_pending(h, block, effects);
    }

    fn serve_pending(&mut self, h: &Digest, block: Header, effects: &mut Vec<Effect>) {
        let pending: Vec<PublicKey> = self
            .pending_req
            .get(h)
            .map(|peers| {
                peers
                    .iter()
                    .copied()
                    .filter(|peer| !self.answered.contains(&(*peer, h.clone())))
                    .collect()
            })
            .unwrap_or_default();
        for peer in pending {
            // Mark the pair before emitting so each requester receives at most one response.
            self.answered.insert((peer, h.clone()));
            if let Some(peers) = self.pending_req.get_mut(h) {
                peers.remove(&peer);
                if peers.is_empty() {
                    self.pending_req.remove(h);
                }
            }
            effects.push(Effect::ServeTo(peer, block.clone()));
            if let Some(metrics) = &self.metrics {
                metrics.vantage_repairs_served.inc();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn requested_count(&self) -> usize {
        self.requested.len()
    }

    pub fn blocks(&self) -> SharedBlocks {
        self.blocks.clone()
    }

    pub fn pending_settle_len(&self) -> usize {
        self.pending_settle
    }

    #[cfg(test)]
    pub(crate) fn was_requested_hash(&self, h: &Digest) -> bool {
        self.requested_hashes.contains(h)
    }

    #[cfg(test)]
    pub(crate) fn fanout_asked_for_test(&self, h: &Digest) -> Option<usize> {
        self.fanout.get(h).map(|s| s.asked)
    }

    #[cfg(test)]
    pub(crate) fn in_flight_for_test(&self) -> usize {
        self.in_flight
    }

    #[cfg(test)]
    pub(crate) fn is_escalating_for_test(&self, h: &Digest) -> bool {
        self.fanout_queue.iter().any(|(_, d)| d == h)
    }

    #[cfg(test)]
    pub(crate) fn blocked_on_len_for_test(&self, h: &Digest) -> usize {
        self.blocked_on.get(h).map_or(0, |s| s.len())
    }

    #[cfg(test)]
    pub(crate) fn settle_calls_for_test(&self) -> u64 {
        self.settle_calls
    }

    #[cfg(test)]
    pub(crate) fn was_requested(&self, peer: &PublicKey, h: &Digest) -> bool {
        self.requested.contains(&(*peer, h.clone()))
    }

    #[cfg(test)]
    pub(crate) fn is_settled(&self, r: &BlockRef) -> bool {
        self.positions
            .get(&self.index.slot(&r.0), r.1, &r.2)
            .is_some_and(|position| position.settled)
    }

    #[cfg(test)]
    pub(crate) fn is_pending_settle(&self, r: &BlockRef) -> bool {
        self.positions
            .get(&self.index.slot(&r.0), r.1, &r.2)
            .is_some_and(|position| position.pending_settle)
    }

    #[cfg(test)]
    pub(crate) fn blocks_for_test(
        &self,
    ) -> parking_lot::MutexGuard<'_, crate::vantage::lanes::BlockCache> {
        self.blocks.lock()
    }
}
