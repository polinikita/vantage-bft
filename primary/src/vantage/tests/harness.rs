// PHASE4-SPEC.md §12 / PHASE5-SPEC.md §4 -- shared in-proc N-engine test harness: each
// `Node` bundles one party's full stack (`LaneManager` + `Repairer` + `AgbEngine` +
// `Frontier` + `Cursor` + `Pacemaker`), wired by directly routing each other's
// `Effect`s -- the same cross-component dispatch `vantage::node::VantageCore` performs
// against a real network/timer runtime (§1/§3), driven synchronously here (no real
// sockets or sleeps) so tests are fast and deterministic. Shared by
// `integration_tests.rs`, `crash_fault_tests.rs`, and `convergence_tests.rs`.

// clippy::needless_range_loop: this harness builds N-party fixtures with `for i in
// 0..n` throughout, indexing several parallel per-party collections (nodes, keys,
// addresses) at once inside each loop body -- clippy's own iterator rewrite handles
// one collection at a time and would need `.zip()`-chaining several iterators per
// loop for no real readability gain over the current explicit index; test-only code,
// not hiding any dead logic.
#![allow(clippy::needless_range_loop)]

use super::common::*;
use crate::primary::View;
use crate::vantage::agb::{AgbEngine, DigestStatements, EchoOut, ProposalOut, ReadyOut, TimerKind};
use crate::vantage::control::ControlLog;
use crate::vantage::frontier::Frontier;
use crate::vantage::lanes::{AckAggregator, AckAvailability, LaneManager, SharedAckAggregator};
use crate::vantage::node::Inbound;
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::repair::Repairer;
use crate::vantage::resolve::Resolver;
use crate::vantage::{Cursor, Effect};
use config::Committee;
use crypto::PublicKey;
use metrics::Metrics;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

pub struct Node {
    pub name: PublicKey,
    pub lm: LaneManager,
    /// Mirrors `VantageCore::recheck_pending`: `recheck_all` is coalesced to once per
    /// `drain_local` pass instead of once per credited availability ref. Kept in step
    /// with production so every suite built on this harness exercises the SAME
    /// evaluation point production uses -- the whole reason the flag exists.
    pub recheck_pending: bool,
    pub ack_aggregator: SharedAckAggregator,
    pub rep: Repairer,
    pub agb: AgbEngine,
    pub frontier: Frontier,
    pub cursor: Cursor,
    pub pacemaker: Pacemaker,
    pub resolver: Resolver,
    pub control: ControlLog,
    /// signature-free.tex §8.3 "Digest-named AGB statements" -- mirrors
    /// `VantageCore::digest_stmts` exactly.
    pub digest_stmts: DigestStatements,
    /// Mirrors `VantageCore::digest_statements` exactly: `false` (the default, via
    /// `Node::new`) leaves `drain_local`'s `Effect::BroadcastEcho`/`BroadcastReady`
    /// fan-out byte-identical to before this field existed. `true` (opt in via
    /// `with_digest_statements`) sends the compact digest-named encoding instead.
    pub digest_statements: bool,
    /// A test-only cap on `try_propose_effects` (mirroring `integration_tests.rs`'s
    /// original `MAX_VIEWS`): this harness never seeds new lane content after the
    /// initial round, so every view would otherwise propose over identical
    /// already-quorum'd manifest content and cascade forever -- nothing in a
    /// synchronous, timer-less-by-default harness throttles it. Comfortably above
    /// whatever the owning test needs, purely so it terminates.
    pub max_views: View,
    /// Whether this node is still running. A "crashed" node (`alive = false`) produces
    /// no further effects at all (`dispatch`/`fire_due_timers`/`try_propose_effects`/
    /// `enter_view_effects` are all no-ops); the harness never enqueues a message
    /// addressed to one.
    pub alive: bool,
    /// (deadline, view, kind) -- mirrors `VantageCore`'s own timer queue exactly, so
    /// fallback/absolute deadlines can be driven by advancing a shared `now`.
    pub timers: Vec<(Instant, View, TimerKind)>,
    /// Mirrors `VantageCore`'s own control-round timer queue.
    pub control_timers: Vec<(Instant, crate::vantage::control::Round)>,
    /// PHASE5-SPEC.md §4's convergence test: while `true`, every inbound wish
    /// component (piggybacked on a response, or a standalone `VantageWish`) addressed
    /// to this node is buffered here instead of being absorbed by `Pacemaker` -- the
    /// response's own AGB processing still happens normally either way (only the wish
    /// sub-channel is delayed). `release_wishes` replays every held wish, in arrival
    /// order.
    pub wish_partitioned: bool,
    pub held_wishes: Vec<(PublicKey, View)>,
    /// PHASE6-SPEC.md §9 gate amendment: attached to `agb` (mirrors
    /// `vantage::node::VantageCore::spawn`'s own `with_metrics` wiring) so tests can
    /// assert on `vantage_seals`'s per-route breakdown, same as production.
    pub metrics: Arc<Metrics>,
    /// Mirrors `VantageCore::ack_watermarks` exactly: `false` (the default, via
    /// `Node::new`) leaves `drain_local`'s `Effect::BroadcastAck` fan-out byte-
    /// identical to before this field existed. `true` (opt in via
    /// `with_ack_watermarks`) suppresses that fan-out -- a test must then drive
    /// `avail_tick` itself to substitute the periodic watermark broadcast a real
    /// `VantageCore::run` would schedule.
    pub ack_watermarks: bool,
}

impl Node {
    pub fn new(name: PublicKey, path: &str, max_views: View) -> Self {
        Self::new_with_committee(name, path, max_views, test_committee())
    }

    /// PHASE7: `Node::new`'s generalization over an arbitrary committee -- the fixed
    /// `test_committee()` (n=4, f=1) never allows a genuine `k >= 2` batch
    /// (`agb::batch_cap` floors at `f`, which is 1 there), so batching-specific
    /// end-to-end tests need a bigger one. `Node::new` itself is unchanged (still
    /// `test_committee()`) -- it just delegates here now.
    pub fn new_with_committee(
        name: PublicKey,
        path: &str,
        max_views: View,
        committee: Committee,
    ) -> Self {
        let (lm, _store) = new_lane_manager_with_committee(name, path, committee.clone());
        let rep = new_repairer_with_committee(name, &lm, committee.clone());
        let registry = prometheus::Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        let agb =
            new_agb_engine_with_committee(name, committee.clone()).with_metrics(metrics.clone());
        let digest_stmts = DigestStatements::new(TEST_DELTA_MS).with_metrics(metrics.clone());
        let frontier = Frontier::new(name, committee.clone());
        let cursor = Cursor::new(
            committee.clone(),
            lm.sid().clone(),
            lm.genesis().clone(),
            MAX_BLOCK_PAYLOAD,
            lm.blocks_handle(),
        );
        let pacemaker = Pacemaker::new(name, &committee);
        let resolver = Resolver::new(committee.size(), TEST_DELTA_MS);
        let mut control = ControlLog::new(name, committee.clone(), lm.sid().clone(), TEST_DELTA_MS);
        // See `ControlLog::max_rounds_for_test`'s doc comment: nothing throttles a
        // `⊥`-valued control round's instantaneous advance in this synchronous
        // harness. Comfortably above whatever an owning test needs (mirrors
        // `max_views`'s own reasoning exactly). PHASE6-SPEC.md §8 finding: this cap is
        // a hard, non-retriable ceiling (`try_propose`'s own `r > max` guard has no
        // way to un-stick once tripped) -- a test that lets the round machine burn
        // through its ENTIRE budget on `⊥` rounds before its own real content ever
        // becomes submittable would permanently strand it. Byzantine-suite scenarios
        // with a multi-step AGB-level setup phase (2/3/4) must defer starting the
        // control-round clock (`ControlLog::genesis`, normally called by `boot`) until
        // that setup is done -- see `harness::boot_without_control`/`start_control`.
        control.set_max_rounds_for_test(2000);
        Self {
            name,
            lm,
            ack_aggregator: Arc::new(parking_lot::Mutex::new(AckAggregator::new(committee))),
            rep,
            agb,
            frontier,
            cursor,
            pacemaker,
            resolver,
            control,
            digest_stmts,
            digest_statements: false,
            max_views,
            recheck_pending: false,
            alive: true,
            timers: Vec::new(),
            control_timers: Vec::new(),
            wish_partitioned: false,
            held_wishes: Vec::new(),
            metrics,
            ack_watermarks: false,
        }
    }

    /// Opt this node into the ack-watermark front-end (`Parameters::ack_watermarks`)
    /// -- see the field's own doc comment. Test-only builder; production wiring
    /// (`vantage::node::VantageCore::build`) reads the flag from `Parameters` instead.
    pub fn with_ack_watermarks(mut self, on: bool) -> Self {
        self.ack_watermarks = on;
        self
    }

    /// Opt this node into digest-named AGB statements (`Parameters::
    /// digest_statements`) -- see the field's own doc comment. Test-only builder;
    /// production wiring (`vantage::node::VantageCore::build`) reads the flag from
    /// `Parameters` instead.
    pub fn with_digest_statements(mut self, on: bool) -> Self {
        self.digest_statements = on;
        self
    }

    pub fn try_propose_effects(&mut self, now: Instant) -> Vec<Effect> {
        if !self.alive {
            return Vec::new();
        }
        let mut effects = Vec::new();
        if self.frontier.a_i() >= self.max_views {
            return effects; // test-only cap, see the field's doc comment
        }
        let view = self.frontier.next_turn();
        let entries: Vec<crate::vantage::ResolutionEntry> =
            if self.agb.proposer(view) == self.name && !self.frontier.already_proposed(view) {
                let agb = &self.agb;
                let control = &self.control;
                let resolved = |u: View| agb.is_sealed(u) || control.is_anchor_resolved(u);
                self.resolver.decide_prefix(agb, view, now, resolved)
            } else {
                Vec::new()
            };
        let proposal = match entries.len() {
            0 => self
                .frontier
                .try_propose(&self.lm, None)
                .map(ProposalOut::Single),
            1 => self
                .frontier
                .try_propose(&self.lm, entries.into_iter().next())
                .map(ProposalOut::Single),
            _ => self
                .frontier
                .propose_view_batch(view, &self.lm, entries)
                .map(ProposalOut::Batch),
        };
        if let Some(proposal) = proposal {
            effects.push(Effect::BroadcastPropose(proposal.clone()));
            effects.extend(match proposal {
                ProposalOut::Single(p) => {
                    self.agb
                        .on_propose(self.name, p, now, &mut self.lm, &mut self.rep)
                }
                ProposalOut::Batch(p) => {
                    self.agb
                        .on_propose_batch(self.name, p, now, &mut self.lm, &mut self.rep)
                }
            });
        }
        effects
    }

    /// PHASE5-SPEC.md §3: execute a formal `Effect::Enter(view)` exactly as
    /// `VantageCore::enter_view_effects` does -- `AgbEngine::enter` + `Frontier::enter`
    /// (whose W5(c) floor can activate further views via its own contiguous-advance
    /// loop, each run through `AgbEngine::activate`), then re-check R1.
    pub fn enter_view_effects(&mut self, view: View, now: Instant) -> Vec<Effect> {
        if !self.alive {
            return Vec::new();
        }
        let mut effects = self.agb.enter(view, now, &mut self.lm, &mut self.rep);
        let activated = self.frontier.enter(view);
        for v in activated {
            effects.extend(self.agb.activate(v, &mut self.lm, &mut self.rep));
        }
        effects.extend(self.try_propose_effects(now));
        effects
    }

    /// Shared by every response arm below and the standalone `Wish` arm: absorb a
    /// first-hand wish, or (while wish-partitioned) buffer it for later replay.
    fn absorb_wish(&mut self, sender: PublicKey, x: View) -> Vec<Effect> {
        if self.wish_partitioned {
            self.held_wishes.push((sender, x));
            return Vec::new();
        }
        self.pacemaker.on_wish(sender, x)
    }

    fn on_ack_availability(&mut self, availability: AckAvailability, _now: Instant) -> Vec<Effect> {
        self.recheck_pending = true;
        self.lm.process_ack_availability(availability)
    }

    fn record_ack(
        &mut self,
        sender: PublicKey,
        reference: crate::vantage::block::BlockRef,
        now: Instant,
    ) -> Vec<Effect> {
        let availability = {
            let mut aggregator = self.ack_aggregator.lock();
            aggregator.record_ack(sender, reference).availability
        };
        availability
            .map(|availability| self.on_ack_availability(availability, now))
            .unwrap_or_default()
    }

    pub async fn dispatch(&mut self, inbound: Inbound, now: Instant) -> Vec<Effect> {
        if !self.alive {
            return Vec::new();
        }
        match inbound {
            Inbound::Publish(sender, header) => self.lm.process_publish(sender, header).await,
            Inbound::Serve(header) => self.rep.on_serve(header),
            Inbound::HeadersRequest(digests, requestor) => {
                let mut effects = Vec::new();
                for d in digests {
                    effects.extend(self.rep.on_request(requestor, d));
                }
                effects
            }
            Inbound::AckAvailability(availability) => self.on_ack_availability(availability, now),
            Inbound::Ack(ack) => self.record_ack(ack.sender, ack.reference(), now),
            Inbound::Avail(entries, sender) => {
                let refs = self.lm.resolve_watermark(sender, &entries);
                let mut effects = Vec::new();
                for r in refs {
                    effects.extend(self.record_ack(sender, r, now));
                }
                effects
            }
            Inbound::Propose(proposal) => {
                let sender = self.agb.proposer(proposal.view());
                match proposal {
                    ProposalOut::Single(p) => {
                        self.agb
                            .on_propose(sender, p, now, &mut self.lm, &mut self.rep)
                    }
                    ProposalOut::Batch(p) => {
                        self.agb
                            .on_propose_batch(sender, p, now, &mut self.lm, &mut self.rep)
                    }
                }
            }
            Inbound::Echo(echo) => {
                let mut effects = self.absorb_wish(echo.sender(), echo.wish());
                effects.extend(match echo {
                    EchoOut::Single(e) => self.agb.on_echo(e, &mut self.rep),
                    EchoOut::Batch(e) => self.agb.on_echo_batch(e, &mut self.rep),
                });
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            Inbound::EchoSkip(view, sender, wish) => {
                let mut effects = self.absorb_wish(sender, wish);
                effects.extend(self.agb.on_echo_skip(view, sender));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            Inbound::Ready(ready) => {
                let mut effects = self.absorb_wish(ready.sender(), ready.wish());
                effects.extend(match ready {
                    ReadyOut::Single(r) => self.agb.on_ready(r, &mut self.rep),
                    ReadyOut::Batch(r) => self.agb.on_ready_batch(r, &mut self.rep),
                });
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            Inbound::NoReady(view, sender, wish) => {
                let mut effects = self.absorb_wish(sender, wish);
                effects.extend(self.agb.on_noready(view, sender));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            Inbound::Wish(view, sender) => self.absorb_wish(sender, view),
            Inbound::CompReport(view, digest, sender) => {
                self.control.on_comp_report(view, digest, sender)
            }
            Inbound::ControlInit(proposal, b_w) => {
                let sender = self.control.control_leader(proposal.round);
                self.control.on_control_init(sender, proposal, b_w)
            }
            Inbound::ControlEcho(sender, proposal) => {
                self.control.on_control_echo(sender, proposal)
            }
            Inbound::ControlReady(sender, proposal) => {
                self.control.on_control_ready(sender, proposal)
            }
            Inbound::ControlCommit(sender, round) => self.control.on_control_commit(sender, round),
            Inbound::ControlTimeoutVote(sender, round) => {
                self.control.on_control_timeout_vote(sender, round)
            }
            Inbound::ControlTimeoutAccept(sender, round) => {
                self.control.on_control_timeout_accept(sender, round)
            }
            Inbound::ControlFetch(view, digest, requester) => {
                self.control.on_control_fetch(requester, view, digest)
            }
            Inbound::ControlServe(view, proposal) => self.control.on_control_serve(view, proposal),
            // Mirrors `vantage::node::VantageCore::dispatch_inbound`'s own arm.
            Inbound::SkipVote(view, sender) => self.agb.on_skip_vote(view, sender),
            // signature-free.tex §8.3 "Digest-named AGB statements" -- mirrors
            // `vantage::node::VantageCore::dispatch_inbound`'s own four arms.
            Inbound::EchoDigest(msg) => {
                let mut effects = self.absorb_wish(msg.sender, msg.wish);
                effects.extend(self.digest_stmts.on_echo_digest(
                    msg,
                    now,
                    &mut self.agb,
                    &mut self.rep,
                ));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            Inbound::ReadyDigest(msg) => {
                let mut effects = self.absorb_wish(msg.sender, msg.wish);
                effects.extend(self.digest_stmts.on_ready_digest(
                    msg,
                    now,
                    &mut self.agb,
                    &mut self.rep,
                ));
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            Inbound::BodyFetch(view, digest, requester) => self
                .digest_stmts
                .on_body_fetch(requester, view, digest, &self.agb),
            Inbound::BodyServe(view, proposal) => {
                let mut effects =
                    self.digest_stmts
                        .on_body_serve(view, proposal, &mut self.agb, &mut self.rep);
                effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
                effects
            }
            // Mechanism A (`vantage::resume`): mirrors `vantage::node::VantageCore::
            // dispatch_inbound`'s own `Inbound::LaneResume` arm's clamp + serve
            // decision. Deliberately simpler than production in two PACING-only
            // respects that no existing test in this suite depends on: no
            // `resume_batch` cap (serves through our own tip in one go) and no
            // `ResumeServe` dedup memo (this harness has no analogue of `VantageCore::
            // resume_serve`, and nothing here drives the requester-side trigger tick
            // that would call this repeatedly) -- the CORRECTNESS-bearing part
            // (foreign-lane rejection, floor clamp) is unchanged from production.
            Inbound::LaneResume(author, from, requester) => {
                if author != self.name {
                    return Vec::new();
                }
                let floor = self.lm.earliest_authored_height(&author);
                let from = from.max(floor);
                let tip = self.lm.own_tip_height();
                if from > tip {
                    return Vec::new();
                }
                let mut effects = Vec::new();
                for height in from..=tip {
                    if let Some(header) = self.lm.author_block_at(&author, height) {
                        effects.push(Effect::ResumeServeTo(requester, header));
                    }
                }
                effects
            }
            // reconnect-replay plan (server-authoritative floor, v3): a SEPARATE,
            // transport-level mechanism (volatile sends, session-death drop
            // accounting, the shared dirty/in-flight maps) this synchronous,
            // no-real-network harness has no analogue of at all -- unlike
            // Mechanism A's `LaneResume` above, which this harness models a
            // simplified version of. Covered instead by `VantageCore`'s own
            // `#[cfg(test)] mod tests` (in `node.rs`), which drives a REAL `Wire`
            // (dirty map, in-flight map, outbox) directly -- see that module's own
            // doc comment for why (private fields/constructors). Never constructed
            // by this harness's own test suites.
            Inbound::ResumeHello(..) | Inbound::ReplayDone(..) => Vec::new(),
        }
    }

    /// Mirrors `VantageCore::run`'s control-timer branch: fire every control-round
    /// timer whose deadline is now `<= now`.
    pub fn fire_due_control_timers(&mut self, now: Instant) -> Vec<Effect> {
        if !self.alive {
            return Vec::new();
        }
        let mut due = Vec::new();
        self.control_timers.retain(|(d, r)| {
            if *d <= now {
                due.push(*r);
                false
            } else {
                true
            }
        });
        let mut effects = Vec::new();
        for round in due {
            effects.extend(self.control.on_control_round_timer(round));
        }
        effects
    }

    /// Mirrors `VantageCore::run`'s `agb_sleep` branch: fire every timer whose deadline
    /// is now `<= now`.
    pub fn fire_due_timers(&mut self, now: Instant) -> Vec<Effect> {
        if !self.alive {
            return Vec::new();
        }
        let mut due = Vec::new();
        self.timers.retain(|(d, v, k)| {
            if *d <= now {
                due.push((*v, *k));
                false
            } else {
                true
            }
        });
        let mut effects = Vec::new();
        for (view, kind) in due {
            match kind {
                TimerKind::EchoFallback => effects.extend(self.agb.on_echo_fallback_timer(
                    view,
                    &mut self.lm,
                    &mut self.rep,
                )),
                TimerKind::EchoAbsolute => {
                    effects.extend(self.agb.on_echo_absolute_timer(view, &mut self.rep))
                }
                TimerKind::ReadyAbsolute => effects.extend(self.agb.on_ready_timer(view)),
            }
        }
        effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
        effects
    }

    /// PHASE5-SPEC.md §4's convergence test: stop buffering and replay every held
    /// wish, in arrival order (each may itself produce further effects -- amplified
    /// broadcasts and/or formal entries).
    pub fn release_wishes(&mut self) -> Vec<Effect> {
        self.wish_partitioned = false;
        let held = std::mem::take(&mut self.held_wishes);
        let mut effects = Vec::new();
        for (sender, x) in held {
            effects.extend(self.pacemaker.on_wish(sender, x));
        }
        effects
    }
}

/// Drains `initial` (and everything it transitively produces) for `nodes[idx]`,
/// routing every `Broadcast*`/`RequestTo`/`ServeTo` effect to the other nodes' shared
/// `outbox` instead of a real network -- mirrors `VantageCore::execute` exactly,
/// including D5-3's wish-stamping-at-serialization-time and the WISH pacemaker's own
/// effects. Messages addressed to a dead node are simply never enqueued (`!alive`).
pub fn drain_local(
    nodes: &mut [Node],
    idx: usize,
    initial: Vec<Effect>,
    now: Instant,
    outbox: &mut VecDeque<(usize, Inbound)>,
) {
    if !nodes[idx].alive {
        return;
    }
    let n = nodes.len();
    let mut queue: VecDeque<Effect> = initial.into();
    // Same outer loop as `VantageCore::execute`: drain, then service the coalesced
    // `recheck_all` ONCE and drain what it produced, until both are quiet.
    loop {
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::BroadcastPublish(header) => {
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::Publish(header.author, header.clone())));
                        }
                    }
                }
                Effect::BroadcastAck(ack) => {
                    // Self-ack path always runs; the fan-out to other nodes is suppressed
                    // when `ack_watermarks` is on, mirroring `VantageCore::execute`'s
                    // identical gating -- a test that turns this on must drive `avail_tick`
                    // itself to substitute the periodic watermark broadcast.
                    let ack_watermarks = nodes[idx].ack_watermarks;
                    {
                        let node = &mut nodes[idx];
                        queue.extend(node.record_ack(node.name, ack.reference(), now));
                    }
                    if !ack_watermarks {
                        for j in 0..n {
                            if j != idx && nodes[j].alive {
                                outbox.push_back((j, Inbound::Ack(ack.clone())));
                            }
                        }
                    }
                }
                // AVAIL-ECHO-SPEC.md: mirror `VantageCore::execute`'s arm exactly, so an
                // integration test with `echo_avail_claims` on exercises the real
                // crediting path (monotonicity + linkage + credit_refs) rather than
                // silently dropping every claim.
                Effect::AvailClaimed(sender, resolved) => {
                    let refs = nodes[idx].lm.note_claim(sender, &resolved);
                    let credited: Vec<_> = refs
                        .into_iter()
                        .flat_map(|r| {
                            let node = &mut nodes[idx];
                            node.record_ack(sender, r, now)
                        })
                        .collect();
                    queue.extend(credited);
                }
                Effect::SyncBatches(..) => {} // payloads are always empty in this harness
                Effect::RequestTo(peer, digest) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == peer) {
                        if nodes[j].alive {
                            outbox.push_back((
                                j,
                                Inbound::HeadersRequest(vec![digest], nodes[idx].name),
                            ));
                        }
                    }
                }
                Effect::ServeTo(peer, header) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == peer) {
                        if nodes[j].alive {
                            outbox.push_back((j, Inbound::Serve(header)));
                        }
                    }
                }
                Effect::BlockCached(digest) => {
                    let node = &mut nodes[idx];
                    // Ack-watermark front-end: retry any watermark pending on this
                    // author, before `on_block_available` consumes `digest` by value.
                    let retried = node.lm.retry_pending_avail(&digest);
                    for (sender, r) in retried {
                        queue.extend(node.record_ack(sender, r, now));
                    }
                    queue.extend(node.rep.on_block_available(digest));
                    node.recheck_pending = true;
                    queue.extend(node.cursor.retry());
                }
                Effect::BroadcastPropose(p) => {
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::Propose(p.clone())));
                        }
                    }
                }
                Effect::BroadcastEcho(mut e) => {
                    e.set_wish(nodes[idx].pacemaker.own_watermark());
                    // signature-free.tex §8.3 "Digest-named AGB statements": mirrors
                    // `VantageCore::execute`'s identical emission-side translation --
                    // computed once (not per-destination), never applied to `Batch`.
                    let translated = if nodes[idx].digest_statements {
                        match &e {
                            EchoOut::Single(single) => Some(single.to_digest(nodes[idx].agb.sid())),
                            EchoOut::Batch(_) => None,
                        }
                    } else {
                        None
                    };
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            match &translated {
                                Some(digest_msg) => {
                                    outbox.push_back((j, Inbound::EchoDigest(digest_msg.clone())))
                                }
                                None => outbox.push_back((j, Inbound::Echo(e.clone()))),
                            }
                        }
                    }
                }
                Effect::BroadcastEchoSkip(view) => {
                    let sender = nodes[idx].name;
                    let wish = nodes[idx].pacemaker.own_watermark();
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::EchoSkip(view, sender, wish)));
                        }
                    }
                }
                Effect::BroadcastReady(mut r) => {
                    r.set_wish(nodes[idx].pacemaker.own_watermark());
                    // signature-free.tex §8.3: mirrors `Effect::BroadcastEcho`'s
                    // identical translation immediately above.
                    let translated = if nodes[idx].digest_statements {
                        match &r {
                            ReadyOut::Single(single) => {
                                Some(single.to_digest(nodes[idx].agb.sid()))
                            }
                            ReadyOut::Batch(_) => None,
                        }
                    } else {
                        None
                    };
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            match &translated {
                                Some(digest_msg) => {
                                    outbox.push_back((j, Inbound::ReadyDigest(digest_msg.clone())))
                                }
                                None => outbox.push_back((j, Inbound::Ready(r.clone()))),
                            }
                        }
                    }
                }
                Effect::BroadcastNoReady(view) => {
                    let sender = nodes[idx].name;
                    let wish = nodes[idx].pacemaker.own_watermark();
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::NoReady(view, sender, wish)));
                        }
                    }
                }
                // No wish piggyback, unlike `BroadcastEchoSkip`/`BroadcastNoReady` above.
                Effect::BroadcastSkipVote(view) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::SkipVote(view, sender)));
                        }
                    }
                }
                Effect::BroadcastWish(view) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::Wish(view, sender)));
                        }
                    }
                }
                Effect::Fixed(view, well_formed) => {
                    let node = &mut nodes[idx];
                    let activated = node.frontier.record_fixed(view, well_formed);
                    for v in activated {
                        queue.extend(node.agb.activate(v, &mut node.lm, &mut node.rep));
                    }
                    queue.extend(node.try_propose_effects(now));
                    // signature-free.tex §8.3: mirrors `VantageCore::execute`'s
                    // identical `Effect::Fixed` addendum.
                    queue.extend(node.digest_stmts.on_local_fixed(
                        view,
                        &mut node.agb,
                        &mut node.rep,
                    ));
                }
                Effect::Completed(view, c, t) => {
                    queue.extend(nodes[idx].cursor.on_completed(view, c, t));
                }
                Effect::Sealed(view, outcome) => {
                    queue.extend(nodes[idx].cursor.on_sealed(view, outcome));
                }
                Effect::ArmTimer(view, kind, deadline) => {
                    nodes[idx].timers.push((deadline, view, kind));
                }
                Effect::NotifyCommitted(..) => {}
                Effect::Enter(view) => {
                    queue.extend(nodes[idx].enter_view_effects(view, now));
                }
                Effect::RaiseWish(target) => {
                    queue.extend(nodes[idx].pacemaker.raise_own_wish(target));
                }

                // --- PHASE6-SPEC.md §5 (reports + control log) ---
                Effect::CompletionReportable(view, proposal) => {
                    queue.extend(nodes[idx].control.on_completion_reportable(view, proposal));
                }
                Effect::BroadcastCompReport(view, digest) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox
                                .push_back((j, Inbound::CompReport(view, digest.clone(), sender)));
                        }
                    }
                }
                Effect::BroadcastControlInit(proposal, b_w) => {
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((
                                j,
                                Inbound::ControlInit(proposal.clone(), b_w.clone()),
                            ));
                        }
                    }
                }
                Effect::BroadcastControlEcho(proposal) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::ControlEcho(sender, proposal.clone())));
                        }
                    }
                }
                Effect::BroadcastControlReady(proposal) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::ControlReady(sender, proposal.clone())));
                        }
                    }
                }
                Effect::BroadcastControlCommit(round) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::ControlCommit(sender, round)));
                        }
                    }
                }
                Effect::BroadcastControlTimeoutVote(round) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::ControlTimeoutVote(sender, round)));
                        }
                    }
                }
                Effect::BroadcastControlTimeoutAccept(round) => {
                    let sender = nodes[idx].name;
                    for j in 0..n {
                        if j != idx && nodes[j].alive {
                            outbox.push_back((j, Inbound::ControlTimeoutAccept(sender, round)));
                        }
                    }
                }
                Effect::ControlFetchTo(peer, view, digest) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == peer) {
                        if nodes[j].alive {
                            outbox.push_back((
                                j,
                                Inbound::ControlFetch(view, digest, nodes[idx].name),
                            ));
                        }
                    }
                }
                Effect::ControlServeTo(peer, view, proposal) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == peer) {
                        if nodes[j].alive {
                            outbox.push_back((j, Inbound::ControlServe(view, proposal)));
                        }
                    }
                }
                Effect::ArmControlTimer(round, deadline) => {
                    nodes[idx].control_timers.push((deadline, round));
                }

                // --- PHASE6-SPEC.md §6 (anchors) ---
                Effect::ApplyAnchor(view, outcome, refs) => {
                    let node = &mut nodes[idx];
                    for r in refs {
                        queue.extend(node.rep.authorize(r));
                    }
                    queue.extend(node.agb.submit_anchor(view, outcome));
                }

                // --- signature-free.tex §8.3 "Digest-named AGB statements" ---
                Effect::BodyFetchTo(peer, view, digest) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == peer) {
                        if nodes[j].alive {
                            outbox
                                .push_back((j, Inbound::BodyFetch(view, digest, nodes[idx].name)));
                        }
                    }
                }
                Effect::BodyServeTo(peer, view, proposal) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == peer) {
                        if nodes[j].alive {
                            outbox.push_back((j, Inbound::BodyServe(view, proposal)));
                        }
                    }
                }

                // --- Mechanism A (sender-side lane resume, `vantage::resume`) ---
                // Same wire encoding as `Effect::BroadcastPublish` (`Header(_, false)`,
                // which always maps to `Inbound::Publish` regardless of unicast vs.
                // broadcast delivery -- see `vantage::wire::Wire::enqueue_resume_header`'s
                // own doc comment), just routed to exactly one target instead of every
                // live node.
                Effect::ResumeServeTo(requester, header) => {
                    if let Some(j) = nodes.iter().position(|nd| nd.name == requester) {
                        if nodes[j].alive {
                            outbox.push_back((j, Inbound::Publish(header.author, header.clone())));
                        }
                    }
                }
            }
        }
        if !nodes[idx].recheck_pending {
            break;
        }
        nodes[idx].recheck_pending = false;
        let node = &mut nodes[idx];
        let rechecked = node.agb.recheck_all(&mut node.lm, &mut node.rep);
        if rechecked.is_empty() {
            break;
        }
        queue.extend(rechecked);
    }
}

/// PHASE6-SPEC.md §8: deliver `inbound` directly to exactly the given node indices,
/// bypassing every `Broadcast*` effect's own all-others fan-out in `drain_local`. This
/// is the suite's one interception hook: it models a Byzantine party's CONTENT-level
/// behavior (withholding a message from some parties, or sending disjoint/forked
/// content to disjoint subsets) -- never declared-sender spoofing (scenario 7's
/// documented non-defense boundary stays untouched: every message constructed this way
/// still carries its true, honestly-computed sender/leader identity, exactly as
/// `dispatch` would derive it from a real channel). `targets` may include dead nodes;
/// they are silently skipped (mirrors every `drain_local` arm's own `nodes[j].alive`
/// check).
pub fn deliver_only_to(
    nodes: &[Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    targets: &[usize],
    inbound: Inbound,
) {
    for &i in targets {
        if nodes[i].alive {
            outbox.push_back((i, inbound.clone()));
        }
    }
}

pub async fn run_to_quiescence(
    nodes: &mut [Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    now: Instant,
) {
    while let Some((idx, inbound)) = outbox.pop_front() {
        if !nodes[idx].alive {
            continue;
        }
        let effects = nodes[idx].dispatch(inbound, now).await;
        drain_local(nodes, idx, effects, now, outbox);
    }
}

/// Test-only substitute for `VantageCore::run`'s periodic ack-watermark tick: for
/// every live node with `ack_watermarks` on, flushes `LaneManager::take_avail_flush`
/// (if dirty) and fans the result out to every other live node as `Inbound::Avail`,
/// then drains to quiescence -- mirrors the production tick's own broadcast + the
/// receiving cores' immediate resolution. A no-op for a node with `ack_watermarks`
/// off (byte-identical to never calling this at all).
pub async fn avail_tick(nodes: &mut [Node], now: Instant, outbox: &mut VecDeque<(usize, Inbound)>) {
    let n = nodes.len();
    for idx in 0..n {
        if !nodes[idx].alive || !nodes[idx].ack_watermarks {
            continue;
        }
        if let Some(entries) = nodes[idx].lm.take_avail_flush() {
            let sender = nodes[idx].name;
            for j in 0..n {
                if j != idx && nodes[j].alive {
                    outbox.push_back((j, Inbound::Avail(entries.clone(), sender)));
                }
            }
        }
    }
    run_to_quiescence(nodes, outbox, now).await;
}

/// Genesis bootstrap (§4/W1) for every live node: enter view 1, then the WISH
/// pacemaker's own genesis wish(2) -- mirrors `VantageCore::run`'s boot sequence
/// exactly. Dead nodes (pre-marked `alive = false` by the caller, to simulate a crash
/// from the very start) never boot at all.
pub async fn boot(nodes: &mut [Node], now: Instant, outbox: &mut VecDeque<(usize, Inbound)>) {
    for i in 0..nodes.len() {
        if !nodes[i].alive {
            continue;
        }
        let mut effects = nodes[i].enter_view_effects(1, now);
        effects.extend(nodes[i].pacemaker.genesis());
        effects.extend(nodes[i].control.genesis());
        drain_local(nodes, i, effects, now, outbox);
    }
    run_to_quiescence(nodes, outbox, now).await;
}

/// Like `boot`, but does NOT start the control-round clock (`ControlLog::genesis`) --
/// pair with `start_control` once whatever AGB-level setup a test needs (e.g. the
/// Byzantine suite's multi-step disjoint-proposal deliveries) is done. PHASE6-SPEC.md
/// §8 finding: the control round machine's `⊥`-valued rounds advance instantly on
/// every `run_to_quiescence` call, regardless of real content -- starting it at
/// genesis and then running a long, `run_to_quiescence`-heavy AGB-level setup phase
/// before any real value is ever submittable can burn through the entire test-only
/// `max_rounds_for_test` budget on empty rounds, permanently stranding the round
/// machine before it ever gets a chance to propose the real value (the cap is a hard,
/// non-retriable ceiling by design, not a soft/resettable one).
pub async fn boot_without_control(
    nodes: &mut [Node],
    now: Instant,
    outbox: &mut VecDeque<(usize, Inbound)>,
) {
    for i in 0..nodes.len() {
        if !nodes[i].alive {
            continue;
        }
        let mut effects = nodes[i].enter_view_effects(1, now);
        effects.extend(nodes[i].pacemaker.genesis());
        drain_local(nodes, i, effects, now, outbox);
    }
    run_to_quiescence(nodes, outbox, now).await;
}

/// Starts the control-round clock (`ControlLog::genesis`) for every live node -- pairs
/// with `boot_without_control`, see its doc comment.
pub async fn start_control(
    nodes: &mut [Node],
    now: Instant,
    outbox: &mut VecDeque<(usize, Inbound)>,
) {
    for i in 0..nodes.len() {
        if !nodes[i].alive {
            continue;
        }
        let effects = nodes[i].control.genesis();
        drain_local(nodes, i, effects, now, outbox);
    }
    run_to_quiescence(nodes, outbox, now).await;
}

/// Advance every live node's clock to `now` and fire any timers now due, draining each
/// node's resulting effects to quiescence -- mirrors `VantageCore::run`'s `agb_sleep`
/// branch (processing one node's wakeup at a time is fine: correctness only needs
/// every timer due at this `now` to eventually fire, not a particular cross-node
/// order).
pub async fn advance_time(
    nodes: &mut [Node],
    outbox: &mut VecDeque<(usize, Inbound)>,
    now: Instant,
) {
    for idx in 0..nodes.len() {
        if !nodes[idx].alive {
            continue;
        }
        let mut effects = nodes[idx].fire_due_timers(now);
        effects.extend(nodes[idx].fire_due_control_timers(now));
        drain_local(nodes, idx, effects, now, outbox);
        run_to_quiescence(nodes, outbox, now).await;
    }
}
