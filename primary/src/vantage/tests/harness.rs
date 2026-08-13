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
    pub recheck_pending: bool,
    pub ack_aggregator: SharedAckAggregator,
    pub rep: Repairer,
    pub agb: AgbEngine,
    pub frontier: Frontier,
    pub cursor: Cursor,
    pub pacemaker: Pacemaker,
    pub resolver: Resolver,
    pub control: ControlLog,
    pub digest_stmts: DigestStatements,
    pub digest_statements: bool,
    pub max_views: View,
    pub alive: bool,
    pub timers: Vec<(Instant, View, TimerKind)>,
    pub control_timers: Vec<(Instant, crate::vantage::control::Round)>,
    pub wish_partitioned: bool,
    pub held_wishes: Vec<(PublicKey, View)>,
    pub metrics: Arc<Metrics>,
    pub ack_watermarks: bool,
}

impl Node {
    pub fn new(name: PublicKey, path: &str, max_views: View) -> Self {
        Self::new_with_committee(name, path, max_views, test_committee())
    }

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

    pub fn with_ack_watermarks(mut self, on: bool) -> Self {
        self.ack_watermarks = on;
        self
    }

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
            return effects; // Enforce the test view limit.
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
            Inbound::SequenceAnnounce(..)
            | Inbound::SequenceAnnounceBatch(..)
            | Inbound::SequenceRequest(..)
            | Inbound::SequenceRecords(..)
            | Inbound::SequenceDeltaRequest(..)
            | Inbound::SequenceDelta(..)
            | Inbound::SequenceDeltaRangeRequest(..)
            | Inbound::SequenceDeltaRange(..)
            | Inbound::SequenceOutcomeRequest(..)
            | Inbound::SequenceOutcome(..)
            | Inbound::SequenceUnavailable(..)
            | Inbound::SequenceHeadersRequest(..)
            | Inbound::SequenceHeaders(..) => Vec::new(),
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
            Inbound::SkipVote(view, sender) => self.agb.on_skip_vote(view, sender),
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
            Inbound::ResumeHello(..) | Inbound::ReplayDone(..) => Vec::new(),
        }
    }

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
                TimerKind::ReadyAbsolute => {
                    effects.extend(self.agb.on_ready_timer(view, &mut self.rep))
                }
            }
        }
        effects.extend(self.agb.recheck_all(&mut self.lm, &mut self.rep));
        effects
    }

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
                Effect::SyncBatches(..) => {} // Harness payloads are empty.
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
                Effect::QuarantineTips(tips) => {
                    let (frontier, lm) = (&mut nodes[idx].frontier, &nodes[idx].lm);
                    frontier.quarantine_tips(&tips, lm);
                }
                Effect::BroadcastReady(mut r) => {
                    r.set_wish(nodes[idx].pacemaker.own_watermark());
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
                Effect::SequenceFinalized { .. } => {}
                Effect::RecoverOwnLane(_) => {}
                Effect::Enter(view) => {
                    queue.extend(nodes[idx].enter_view_effects(view, now));
                }
                Effect::RaiseWish(target) => {
                    queue.extend(nodes[idx].pacemaker.raise_own_wish(target));
                }

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

                Effect::ApplyAnchor(view, outcome, refs) => {
                    let node = &mut nodes[idx];
                    for r in refs {
                        queue.extend(node.rep.authorize(r));
                    }
                    queue.extend(node.agb.submit_anchor(view, outcome));
                }

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
