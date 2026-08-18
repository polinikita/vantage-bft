use crate::messages::Header;
use crate::primary::Height;
use crate::vantage::block::block_ok;
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

pub struct Repairer {
    committee: Committee,
    peers: Vec<PublicKey>,
    sid: Digest,
    genesis: Digest,
    max_block_payload: usize,
    blocks: SharedBlocks,

    /// Exact `(author, height, digest)` references admitted for repair.
    authorized: HashSet<BlockRef>,
    /// References verified through genesis and retained; membership is permanent.
    settled: HashSet<BlockRef>,
    /// Authorized references not present in `settled`.
    pending_settle: HashSet<BlockRef>,
    /// References waiting for each missing digest.
    blocked_on: HashMap<Digest, HashSet<BlockRef>>,
    /// Inverse of `blocked_on`; each pending reference belongs to at most one bucket.
    blocked_at: HashMap<BlockRef, Digest>,
    settle_calls: u64,
    /// `(peer, digest)` requests emitted at most once per request cycle.
    requested: HashSet<(PublicKey, Digest)>,
    /// Digests requested by this node and therefore permitted through `on_serve`.
    requested_hashes: HashSet<Digest>,
    fanout: HashMap<Digest, FanoutState>,
    /// Outstanding fan-out entries ordered by ascending block height.
    fanout_queue: BTreeSet<(Height, Digest)>,
    /// Highest confirmed height for each `(author, peer)` pair.
    holders: HashMap<PublicKey, HashMap<PublicKey, Height>>,
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
    /// Cached `utilization_timer{proc="repair_settle"}`. Settle runs hundreds of thousands
    /// of times a second, so a label lookup per call would itself distort the measurement.
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
        Self {
            peers: committee
                .others_primaries(&name)
                .into_iter()
                .map(|(pk, _)| pk)
                .collect(),
            committee,
            sid,
            genesis,
            max_block_payload,
            blocks,
            authorized: HashSet::new(),
            settled: HashSet::new(),
            pending_settle: HashSet::new(),
            blocked_on: HashMap::new(),
            blocked_at: HashMap::new(),
            settle_calls: 0,
            ut_settle: None,
            requested: HashSet::new(),
            requested_hashes: HashSet::new(),
            refetch_at: HashMap::new(),
            fanout: HashMap::new(),
            fanout_queue: BTreeSet::new(),
            holders: HashMap::new(),
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
        self.note_authorized(r.clone());
        self.settle(r, &mut effects);
        effects
    }

    /// Permits a sequence-requested digest through `on_serve` without coordinate authorization.
    pub fn expect_sequence_digest(&mut self, digest: Digest) {
        self.requested_hashes.insert(digest);
    }

    fn note_authorized(&mut self, r: BlockRef) {
        self.authorized.insert(r.clone());
        if !self.settled.contains(&r) {
            self.pending_settle.insert(r);
        }
    }

    fn mark_settled(&mut self, r: BlockRef) {
        self.settled.insert(r.clone());
        self.pending_settle.remove(&r);
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
            if self.pending_settle.contains(&r) {
                self.blocked_at.remove(&r);
                self.settle(r, &mut effects);
            } else {
                self.blocked_at.remove(&r);
            }
        }
        effects
    }

    /// Moves each reference to the bucket for its current missing digest.
    fn record_blocked(&mut self, refs: &[BlockRef], h: &Digest) {
        for r in refs {
            if let Some(prev) = self.blocked_at.get(r) {
                if prev == h {
                    continue;
                }
                if let Some(bucket) = self.blocked_on.get_mut(prev) {
                    bucket.remove(r);
                    if bucket.is_empty() {
                        self.blocked_on.remove(prev);
                    }
                }
            }
            self.blocked_at.insert(r.clone(), h.clone());
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
    pub fn note_holder(&mut self, peer: PublicKey, author: PublicKey, height: Height) {
        let entry = self
            .holders
            .entry(author)
            .or_default()
            .entry(peer)
            .or_insert(0);
        if height > *entry {
            *entry = height;
        }
    }

    fn likely_holders(
        &self,
        author: &PublicKey,
        height: Height,
        want: usize,
        h: &Digest,
    ) -> Vec<PublicKey> {
        let Some(by_peer) = self.holders.get(author) else {
            return Vec::new();
        };
        let mut candidates: Vec<(Height, PublicKey)> = by_peer
            .iter()
            .filter(|(_, &h)| h >= height)
            .map(|(p, &h)| (h, *p))
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

    pub(crate) fn walk_steps_settle(&self) -> u64 {
        self.walk_steps_settle
    }

    fn settle(&mut self, r: BlockRef, effects: &mut Vec<Effect>) -> bool {
        self.settle_calls += 1;
        let _timer = self.metrics.as_ref().map(|metrics| {
            let counter = self
                .ut_settle
                .get_or_insert_with(|| {
                    metrics
                        .utilization_timer
                        .with_label_values(&["repair_settle"])
                })
                .clone();
            metrics.vantage_repair_settle_calls_total.inc();
            metrics::UtilizationTimer::from_counter(counter)
        });
        let mut cur = r;
        let mut frames: Vec<BlockRef> = Vec::new();
        let mut steps: u64 = 0;
        let verified = loop {
            steps += 1;
            if self.settled.contains(&cur) {
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
                self.record_blocked(&frames, &h);
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

            let parent_ref = (author, height - 1, parent_h);
            self.note_authorized(parent_ref.clone());
            frames.push(cur);
            cur = parent_ref;
        };
        self.walk_steps_settle += steps;

        if verified {
            // Retain ancestors before descendants so retention remains prefix-closed.
            while let Some(frame) = frames.pop() {
                self.retain_and_serve(&frame, effects);
                self.mark_settled(frame);
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
        self.pending_settle.len()
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
        self.settled.contains(r)
    }

    #[cfg(test)]
    pub(crate) fn is_pending_settle(&self, r: &BlockRef) -> bool {
        self.pending_settle.contains(r)
    }

    #[cfg(test)]
    pub(crate) fn blocks_for_test(
        &self,
    ) -> parking_lot::MutexGuard<'_, crate::vantage::lanes::BlockCache> {
        self.blocks.lock()
    }
}
