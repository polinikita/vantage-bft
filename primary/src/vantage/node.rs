// PHASE4-SPEC.md §1 -- the single spawned `VantageCore` task: owns `LaneManager` +
// `Repairer` + `AgbEngine` + `Frontier` + `Cursor` and executes their returned
// `Effect`s. One owning loop avoids shared locks entirely (the components are
// synchronous/effect-returning state machines); the shared `BlockCache` mutex stays as
// the one piece of genuinely shared state (§3.3's cross-notification hook).

use crate::messages::{Ack, Header};
use crate::primary::{PrimaryMessage, PrimaryWorkerMessage, View, CHANNEL_CAPACITY};
use crate::vantage::agb::{AgbEngine, Echo, Ready, TimerKind, ViewProposal};
use crate::vantage::block;
use crate::vantage::control::{ControlLog, ControlProposal, Round};
use crate::vantage::cursor::Cursor;
use crate::vantage::frontier::Frontier;
use crate::vantage::lanes::{BlockCache, LaneManager, SharedBlocks};
use crate::vantage::pacemaker::Pacemaker;
use crate::vantage::repair::Repairer;
use crate::vantage::resolve::Resolver;
use crate::vantage::Effect;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use crypto::{Digest, PublicKey};
use futures::sink::SinkExt as _;
use metrics::Metrics;
use network::{CancelHandler, MessageHandler, ReliableSender, SimpleSender, Writer};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::error::TryRecvError;

/// Inbound messages routed to `VantageCore`, either from the network
/// (`VantageReceiverHandler`) or from `PrimaryReceiverHandler`'s `HeadersRequest` arm
/// (shared wire variant with Autobahn).
///
/// `Clone` (PHASE6-SPEC.md §8): the Byzantine test suite's `harness::deliver_only_to`
/// needs to hand the identical constructed message to several distinct node indices
/// (e.g. an equivocating leader's two different proposals each going to a disjoint
/// subset) -- every constituent field type is already `Clone`, so this is a free,
/// behavior-neutral derive (production code never clones an `Inbound`).
#[derive(Debug, Clone)]
pub enum Inbound {
    /// `Header(h, false)`: publish path. Provenance is claimed-by-author (D4 ruling,
    /// PHASE3-NOTES.md §5/§11) -- there is no channel identity to compare `h.author`
    /// against, so the dispatcher always passes `h.author` itself as the trusted
    /// sender.
    Publish(PublicKey, Header),
    /// `Header(h, true)`: serve path.
    Serve(Header),
    HeadersRequest(Vec<Digest>, PublicKey),
    Ack(Ack),
    /// `VantagePropose` carries no sender field on the wire (§2) -- see
    /// `VantageCore::dispatch_inbound` for how the trusted sender is derived.
    Propose(ViewProposal),
    Echo(Echo),
    /// PHASE5-SPEC.md §2: trailing field is the piggybacked wish watermark (D5-2).
    EchoSkip(View, PublicKey, View),
    Ready(Ready),
    /// PHASE5-SPEC.md §2: trailing field is the piggybacked wish watermark (D5-2).
    NoReady(View, PublicKey, View),
    /// PHASE5-SPEC.md §2: a standalone `VantageWish` (W2 amplification).
    Wish(View, PublicKey),
    /// PHASE6-SPEC.md §5.
    CompReport(View, Digest, PublicKey),
    /// `ControlInit` carries no sender field on the wire (same D4 class as `Propose`)
    /// -- the trusted sender is derived as this round's control leader by
    /// `VantageCore::dispatch_inbound`.
    ControlInit(ControlProposal, Option<ViewProposal>),
    ControlEcho(PublicKey, ControlProposal),
    ControlReady(PublicKey, ControlProposal),
    ControlCommit(PublicKey, Round),
    ControlTimeoutVote(PublicKey, Round),
    ControlTimeoutAccept(PublicKey, Round),
    ControlFetch(View, Digest, PublicKey),
    ControlServe(View, ViewProposal),
}

/// Network receiver handler for the Vantage assembly's `primary_to_primary` port.
/// Deliberately a distinct type from Autobahn's `PrimaryReceiverHandler` (which stays
/// byte-identical, untouched) -- the two assemblies never share a handler.
#[derive(Clone)]
pub struct VantageReceiverHandler {
    pub tx: Sender<Inbound>,
}

#[async_trait]
impl MessageHandler for VantageReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        let _ = writer.send(Bytes::from("Ack")).await;
        let message: PrimaryMessage = bincode::deserialize(&serialized)?;
        let inbound = match message {
            PrimaryMessage::Header(h, false) => Inbound::Publish(h.author, h),
            PrimaryMessage::Header(h, true) => Inbound::Serve(h),
            PrimaryMessage::HeadersRequest(digests, requestor) => Inbound::HeadersRequest(digests, requestor),
            PrimaryMessage::VantageAck(a) => Inbound::Ack(a),
            PrimaryMessage::VantagePropose(p) => Inbound::Propose(p),
            PrimaryMessage::VantageEcho(e) => Inbound::Echo(e),
            PrimaryMessage::VantageEchoSkip(v, s, w) => Inbound::EchoSkip(v, s, w),
            PrimaryMessage::VantageReady(r) => Inbound::Ready(r),
            PrimaryMessage::VantageNoReady(v, s, w) => Inbound::NoReady(v, s, w),
            PrimaryMessage::VantageWish(v, s) => Inbound::Wish(v, s),
            PrimaryMessage::CompReport(v, d, s) => Inbound::CompReport(v, d, s),
            PrimaryMessage::ControlInit(p, b) => Inbound::ControlInit(p, b),
            PrimaryMessage::ControlEcho(p, s) => Inbound::ControlEcho(s, p),
            PrimaryMessage::ControlReady(p, s) => Inbound::ControlReady(s, p),
            PrimaryMessage::ControlCommit(r, s) => Inbound::ControlCommit(s, r),
            PrimaryMessage::ControlTimeoutVote(r, s) => Inbound::ControlTimeoutVote(s, r),
            PrimaryMessage::ControlTimeoutAccept(r, s) => Inbound::ControlTimeoutAccept(s, r),
            PrimaryMessage::ControlFetch(v, d, s) => Inbound::ControlFetch(v, d, s),
            PrimaryMessage::ControlServe(v, p) => Inbound::ControlServe(v, p),
            // Autobahn-only variants never reach the Vantage assembly's port; ignore
            // rather than panic (defense in depth against a misrouted message).
            _ => return Ok(()),
        };
        self.tx.send(inbound).await.expect("Failed to send vantage message");
        Ok(())
    }
}

pub struct VantageCore {
    name: PublicKey,
    lm: LaneManager,
    rep: Repairer,
    agb: AgbEngine,
    frontier: Frontier,
    cursor: Cursor,
    pacemaker: Pacemaker,
    resolver: Resolver,
    control: ControlLog,

    network: ReliableSender,
    worker_network: SimpleSender,
    cancel_handlers: Vec<CancelHandler>,

    other_primaries: Vec<(PublicKey, SocketAddr)>,
    worker_addresses: HashMap<WorkerId, SocketAddr>,

    header_size: usize,
    max_header_delay: u64,
    digests: Vec<(Digest, WorkerId)>,
    payload_size: usize,

    /// §10 timer queue: (deadline, view, kind), drained by the run loop's `sleep_until`
    /// branch (message channels are checked first every iteration -- §5's tie-break
    /// rule: "drain message queues before taking a timer branch"). D7-4 (PHASE7-PREP-
    /// NOTES.md): a min-heap by deadline (`Reverse` makes `BinaryHeap`, a max-heap by
    /// default, pop smallest-`Instant`-first) -- `peek()`/`pop()` are O(1)/O(log n),
    /// replacing the previous plain `Vec`'s O(n) `next_deadline` rescan on every
    /// single message the node processes. Deterministic-equivalent: the identical
    /// timers still fire at the identical deadlines producing the identical effects
    /// (once past the lazy stale-discard below, itself also a no-op-preserving
    /// optimization) -- only the internal data structure's complexity changes.
    timers: BinaryHeap<Reverse<(Instant, View, TimerKind)>>,
    /// PHASE6-SPEC.md §5: the control-round timer queue, mirroring `timers` exactly
    /// (kept separate rather than folding `Round` into the `View`-typed queue above --
    /// the two counters are semantically distinct and this avoids a confusing shared
    /// currency). D7-4: same min-heap fix as `timers`.
    control_timers: BinaryHeap<Reverse<(Instant, Round)>>,

    /// D1 payload-sync bookkeeping: outstanding `(digest, worker_id)` keys per header
    /// digest, so `LaneManager::set_payload_ready` (which unconditionally marks a block
    /// payload-ready once called -- see its doc comment) is only called once *every*
    /// missing batch for that header has actually arrived, not on the first one.
    pending_payload: HashMap<Digest, HashSet<(Digest, WorkerId)>>,
    store: Store,
    tx_payload_ready: Sender<(Digest, Digest, WorkerId)>,

    /// PHASE7-PREP-NOTES.md Finding A: kept for `sample_metrics`'s 1s progress-gauge
    /// tick -- a separate clone from the ones already handed to `lm`/`rep`/`agb`
    /// (`Arc<Metrics>` is freely shareable; this is not new metrics plumbing, just one
    /// more clone of the same handle every node already builds).
    metrics: Option<Arc<Metrics>>,
}

impl VantageCore {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        store: Store,
        metrics: Option<Arc<Metrics>>,
        rx_our_digests: Receiver<(Digest, WorkerId)>,
    ) -> Sender<Inbound> {
        let (tx_vantage, rx_vantage) = channel(CHANNEL_CAPACITY);
        let (tx_payload_ready, rx_payload_ready) = channel(CHANNEL_CAPACITY);

        let sid = block::session_id(&committee);
        let genesis = block::genesis_digest(&sid);
        let blocks: SharedBlocks = Arc::new(Mutex::new(BlockCache::new()));

        let mut lm = LaneManager::with_shared_blocks(name, committee.clone(), parameters.max_block_payload, store.clone(), blocks.clone());
        let mut rep = Repairer::new(name, committee.clone(), sid.clone(), genesis.clone(), parameters.max_block_payload, blocks.clone());
        let mut agb = AgbEngine::new(name, committee.clone(), sid.clone(), parameters.delta_ms);
        let core_metrics = metrics.clone();
        if let Some(m) = metrics {
            lm = lm.with_metrics(m.clone());
            rep = rep.with_metrics(m.clone());
            agb = agb.with_metrics(m);
        }
        let frontier = Frontier::new(name, committee.clone());
        let cursor = Cursor::new(committee.clone(), sid.clone(), genesis, parameters.max_block_payload, blocks);
        let pacemaker = Pacemaker::new(name, &committee);
        let resolver = Resolver::new(committee.size(), parameters.delta_ms);
        let control = ControlLog::new(name, committee.clone(), sid, parameters.delta_ms);

        let other_primaries: Vec<(PublicKey, SocketAddr)> = committee
            .others_primaries(&name)
            .into_iter()
            .map(|(pk, addr)| (pk, addr.primary_to_primary))
            .collect();
        let worker_addresses: HashMap<WorkerId, SocketAddr> = committee
            .our_workers_by_id(&name)
            .expect("Our public key is not in the committee")
            .into_iter()
            .map(|(id, addr)| (id, addr.primary_to_worker))
            .collect();

        // PHASE7-PREP-NOTES.md (WAN-shaped local runs, optional item): resolved once,
        // relative to OUR OWN committee index -- empty (== current behavior) unless
        // `--latency-table`/`--mimic-latency-ms` set `parameters.latency_table`. Our
        // own worker addresses are never keys in this map (`Committee::latency_map`
        // always skips `other == myself`), so `worker_network` below stays undelayed.
        let latency_map = parameters
            .latency_table
            .as_deref()
            .map(|table| committee.latency_map(&name, table))
            .unwrap_or_default();

        let core = Self {
            name,
            lm,
            rep,
            agb,
            frontier,
            cursor,
            pacemaker,
            resolver,
            control,
            network: ReliableSender::new().with_latency(latency_map.clone()),
            worker_network: SimpleSender::new().with_latency(latency_map),
            cancel_handlers: Vec::new(),
            other_primaries,
            worker_addresses,
            header_size: parameters.header_size,
            max_header_delay: parameters.max_header_delay,
            digests: Vec::new(),
            payload_size: 0,
            timers: BinaryHeap::new(),
            control_timers: BinaryHeap::new(),
            pending_payload: HashMap::new(),
            store,
            tx_payload_ready,
            metrics: core_metrics,
        };
        tokio::spawn(core.run(rx_vantage, rx_our_digests, rx_payload_ready));
        tx_vantage
    }

    async fn run(
        mut self,
        mut rx_vantage: Receiver<Inbound>,
        mut rx_our_digests: Receiver<(Digest, WorkerId)>,
        mut rx_payload_ready: Receiver<(Digest, Digest, WorkerId)>,
    ) {
        let boot = Instant::now();
        // Genesis bootstrap (§4/PHASE5-SPEC.md W1): every party enters view 1 at boot,
        // then the WISH pacemaker sets its own wish to 2 and broadcasts it.
        let mut effects = self.enter_view_effects(1, boot);
        effects.extend(self.pacemaker.genesis());
        // PHASE6-SPEC.md §5: the control log's own genesis (enter control round 1).
        effects.extend(self.control.genesis());
        self.execute(effects, boot).await;

        let header_timer = tokio::time::sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(header_timer);

        // PHASE7-PREP-NOTES.md Finding A: 1s progress-gauge sample tick. Metrics-only
        // (no protocol effects produced); independent of the message-driven `execute`
        // path entirely, so it cannot perturb ordering/timing of anything else in this
        // loop beyond the tick's own negligible CPU cost.
        let mut metrics_tick = tokio::time::interval(Duration::from_secs(1));
        metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // P4-3: bound `cancel_handlers`' otherwise-unbounded growth under sustained
            // honest traffic. Retains only handlers whose oneshot hasn't resolved yet
            // (`TryRecvError::Empty` -- genuinely still in flight, dropping it would
            // cancel `ReliableSender`'s retransmit-until-ack retry for a message that
            // hasn't been ack'd yet); a resolved or closed one has already done its job
            // (or its connection died and it'll never resolve) and is dropped.
            self.prune_cancel_handlers();

            // D7-4: O(1) peek instead of the previous O(n) full-`Vec` rescan.
            let next_deadline = self.timers.peek().map(|Reverse((d, _, _))| *d);
            let agb_sleep = async {
                match next_deadline {
                    Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(agb_sleep);

            // D7-4: O(1) peek instead of the previous O(n) full-`Vec` rescan.
            let next_control_deadline = self.control_timers.peek().map(|Reverse((d, _))| *d);
            let control_sleep = async {
                match next_control_deadline {
                    Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(control_sleep);

            tokio::select! {
                biased;

                Some(inbound) = rx_vantage.recv() => {
                    let now = Instant::now();
                    let effects = self.dispatch_inbound(inbound, now).await;
                    self.execute(effects, now).await;
                }

                Some((header_digest, digest, worker_id)) = rx_payload_ready.recv() => {
                    let now = Instant::now();
                    let mut resolved = false;
                    if let Some(set) = self.pending_payload.get_mut(&header_digest) {
                        set.remove(&(digest, worker_id));
                        resolved = set.is_empty();
                    }
                    if resolved {
                        self.pending_payload.remove(&header_digest);
                        // P4-4: payload arriving can be the event that flips
                        // `direct_pub`/`author_ok` for a C/T entry the positive gate is
                        // waiting on -- re-poll it, same reasoning as the `Ack` arm.
                        let mut effects = self.lm.set_payload_ready(&header_digest);
                        effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                        self.execute(effects, now).await;
                    }
                }

                Some((digest, worker_id)) = rx_our_digests.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                    if self.payload_size >= self.header_size {
                        let now = Instant::now();
                        let payload = self.digests.drain(..).collect();
                        self.payload_size = 0;
                        let (_, effects) = self.lm.publish_own(payload).await;
                        self.execute(effects, now).await;
                        header_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_header_delay));
                    }
                }

                () = &mut header_timer => {
                    let now = Instant::now();
                    let payload = self.digests.drain(..).collect();
                    self.payload_size = 0;
                    let (_, effects) = self.lm.publish_own(payload).await;
                    self.execute(effects, now).await;
                    header_timer.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(self.max_header_delay));
                }

                () = &mut agb_sleep, if next_deadline.is_some() => {
                    let now = Instant::now();
                    // D7-4: pop-based firing (O(log n) per pop, vs. the previous O(n)
                    // `retain` scan) + lazy stale discard -- if the timer's underlying
                    // one-shot event already happened organically (echo/ready already
                    // sent for this view), its dispatch would be a no-op through the
                    // handler's own guard anyway (see `AgbEngine::echo_sent`/
                    // `ready_sent`'s doc comments); skip constructing/dispatching it
                    // entirely rather than paying for a call that immediately returns
                    // an empty `Vec`. Deterministic-equivalent: identical effects
                    // either way, since the discarded call was always going to return
                    // nothing.
                    let mut effects = Vec::new();
                    while let Some(&Reverse((d, view, kind))) = self.timers.peek() {
                        if d > now {
                            break;
                        }
                        self.timers.pop();
                        let moot = match kind {
                            TimerKind::EchoFallback | TimerKind::EchoAbsolute => self.agb.echo_sent(view),
                            TimerKind::ReadyAbsolute => self.agb.ready_sent(view),
                        };
                        if moot {
                            continue;
                        }
                        match kind {
                            TimerKind::EchoFallback => effects.extend(self.agb.on_echo_fallback_timer(view, &mut self.lm, &mut self.rep)),
                            TimerKind::EchoAbsolute => effects.extend(self.agb.on_echo_absolute_timer(view, &mut self.rep)),
                            TimerKind::ReadyAbsolute => effects.extend(self.agb.on_ready_timer(view)),
                        }
                    }
                    // PHASE6-SPEC.md §2 `MetaOK`: "persistent -- re-evaluate on state
                    // change like the rest of the gate". A pending view w's MetaOK can
                    // depend on THIS party's own echo/ready for an earlier view u that
                    // just changed here -- retry every pending positive gate.
                    effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                    self.execute(effects, now).await;
                }

                () = &mut control_sleep, if next_control_deadline.is_some() => {
                    let now = Instant::now();
                    // D7-4: pop-based firing + lazy stale discard, same reasoning as
                    // the `agb_sleep` branch above -- a round timer whose round has
                    // already advanced/voted is moot through `on_control_round_timer`'s
                    // own `r != self.curr_round || self.voted` guard; skip dispatching
                    // it.
                    let mut effects = Vec::new();
                    while let Some(&Reverse((d, round))) = self.control_timers.peek() {
                        if d > now {
                            break;
                        }
                        self.control_timers.pop();
                        if round != self.control.curr_round() || self.control.voted() {
                            continue;
                        }
                        effects.extend(self.control.on_control_round_timer(round));
                    }
                    self.execute(effects, now).await;
                }

                _ = metrics_tick.tick() => {
                    self.sample_metrics();
                }
            }
        }
    }

    /// PHASE7-PREP-NOTES.md Finding A: publish the six progress gauges from each
    /// component's own accessor -- metrics-only, no effects, no protocol interaction.
    /// A no-op if this node was spawned without a `Metrics` handle.
    fn sample_metrics(&self) {
        let Some(metrics) = &self.metrics else { return };
        metrics.vantage_entered_view.set(self.pacemaker.entered_view() as i64);
        metrics.vantage_frontier_a_i.set(self.frontier.a_i() as i64);
        metrics.vantage_cursor_next_view.set(self.cursor.next_view() as i64);
        metrics.vantage_control_round.set(self.control.curr_round() as i64);
        metrics.vantage_control_delivered_len.set(self.control.delivered_log_len() as i64);
        metrics.vantage_control_consume_pos.set(self.control.consume_pos() as i64);
        // PHASE7-PREP-NOTES.md Delta=1000 investigation: diagnostic-only observational
        // log (no behavior change) -- the linearly-scanned timer queues' current sizes.
        log::info!("vantage node: timers.len()={} control_timers.len()={} cancel_handlers.len()={}", self.timers.len(), self.control_timers.len(), self.cancel_handlers.len());
    }

    /// R1's "should we propose next" check (§4), as a pure effect-producing step so it
    /// can be inlined both at boot and inside `execute`'s `Fixed` handling without
    /// recursive `async fn` calls. PHASE6-SPEC.md §4: when it's genuinely our turn,
    /// consult the `Resolver` for this turn's `M` (data-only `None`, or a recovery
    /// entry) before building the proposal -- computed only when it's actually our
    /// turn, so `Frontier::try_propose`'s own gate (unaffected) stays the sole
    /// authority on whether a proposal is emitted at all.
    fn try_propose_effects(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();
        let view = self.frontier.next_turn();
        let m = if self.agb.proposer(view) == self.name && !self.frontier.already_proposed(view) {
            let agb = &self.agb;
            let control = &self.control;
            self.resolver.decide(agb, view, now, |u| agb.is_sealed(u) || control.is_anchor_resolved(u))
        } else {
            None
        };
        if let Some(proposal) = self.frontier.try_propose(&self.lm, m) {
            effects.push(Effect::BroadcastPropose(proposal.clone()));
            effects.extend(self.agb.on_propose(self.name, proposal, now, &mut self.lm, &mut self.rep));
        }
        effects
    }

    /// PHASE5-SPEC.md §3: execute a formal `Effect::Enter(view)` as `AgbEngine::enter`
    /// + `Frontier::enter`, in that order -- `Frontier::enter`'s W5(c) floor can newly
    /// activate further views (its own contiguous-advance loop, on top of `view`
    /// itself), each of which must still run through `AgbEngine::activate` exactly like
    /// `Effect::Fixed`'s handling does, and a floor raise can newly satisfy R1 too.
    /// Shared by the genesis boot call (view 1) and every subsequent WISH-driven entry.
    fn enter_view_effects(&mut self, view: View, now: Instant) -> Vec<Effect> {
        let mut effects = self.agb.enter(view, now, &mut self.lm, &mut self.rep);
        let activated = self.frontier.enter(view);
        for v in activated {
            effects.extend(self.agb.activate(v, now, &mut self.lm, &mut self.rep));
        }
        effects.extend(self.try_propose_effects(now));
        effects
    }

    async fn dispatch_inbound(&mut self, inbound: Inbound, now: Instant) -> Vec<Effect> {
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
            Inbound::Ack(ack) => {
                // P4-4: an ack can be the event that pushes a C/T entry's ack stake
                // across the f+1/2f+1 thresholds `author_ok`/`is_q_available` read --
                // the R2 positive gate must be re-polled, the same as after a
                // `BlockCached` wakeup, or a view whose *last* gate-enabling event was
                // an ack (rather than a fresh publish) would never echo.
                let mut effects = self.lm.process_ack(ack.sender, ack.reference());
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects
            }
            Inbound::Propose(proposal) => {
                // D4 (PHASE4-SPEC.md §13's standing note): `ViewProposal` carries no
                // sender field and there is no channel identity to check it against
                // (same class of gap as `Header`'s publish path, PHASE3-NOTES.md §5) --
                // production trusts any received proposal for `view` as if it came
                // from `proposer(view)`. `AgbEngine::on_propose`'s `sender ==
                // proposer(view)` guard remains meaningful for unit tests exercising a
                // wrong-sender proposal directly.
                let claimed_sender = self.agb.proposer(proposal.view);
                self.agb.on_propose(claimed_sender, proposal, now, &mut self.lm, &mut self.rep)
            }
            // PHASE5-SPEC.md §3: absorb every response's piggybacked wish (W2) BEFORE
            // handing the response to `AgbEngine` -- wish processing (amplification,
            // then entry) is independent of statement counting, so this ordering vs.
            // the engine's own processing is fine either way; absorbing first keeps the
            // four arms below symmetric with `Inbound::Wish`'s own handling.
            Inbound::Echo(echo) => {
                let mut effects = self.pacemaker.on_wish(echo.sender, echo.wish);
                effects.extend(self.agb.on_echo(echo, &mut self.rep));
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects
            }
            Inbound::EchoSkip(view, sender, wish) => {
                let mut effects = self.pacemaker.on_wish(sender, wish);
                effects.extend(self.agb.on_echo_skip(view, sender));
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects
            }
            Inbound::Ready(ready) => {
                let mut effects = self.pacemaker.on_wish(ready.sender, ready.wish);
                effects.extend(self.agb.on_ready(ready, &mut self.rep));
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects
            }
            Inbound::NoReady(view, sender, wish) => {
                let mut effects = self.pacemaker.on_wish(sender, wish);
                effects.extend(self.agb.on_noready(view, sender));
                effects.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                effects
            }
            // A standalone `VantageWish` (W2 amplification): never an echo/ready/ack/
            // origin bit/resolution justification -- it only ever schedules views.
            Inbound::Wish(view, sender) => self.pacemaker.on_wish(sender, view),

            // --- PHASE6-SPEC.md §5 (reports + control log) ---
            Inbound::CompReport(view, digest, sender) => self.control.on_comp_report(view, digest, sender),
            Inbound::ControlInit(proposal, b_w) => {
                // Same D4 class as `Propose`: no sender field on the wire, trusted as
                // this round's control leader.
                let claimed_sender = self.control.control_leader(proposal.round);
                self.control.on_control_init(claimed_sender, proposal, b_w)
            }
            Inbound::ControlEcho(sender, proposal) => self.control.on_control_echo(sender, proposal),
            Inbound::ControlReady(sender, proposal) => self.control.on_control_ready(sender, proposal),
            Inbound::ControlCommit(sender, round) => self.control.on_control_commit(sender, round),
            Inbound::ControlTimeoutVote(sender, round) => self.control.on_control_timeout_vote(sender, round),
            Inbound::ControlTimeoutAccept(sender, round) => self.control.on_control_timeout_accept(sender, round),
            Inbound::ControlFetch(view, digest, requester) => self.control.on_control_fetch(requester, view, digest),
            Inbound::ControlServe(view, proposal) => self.control.on_control_serve(view, proposal),
        }
    }

    /// Drains `initial` (and every effect transitively produced while draining it)
    /// against real I/O. A plain `VecDeque`-backed loop (not recursive `async fn`
    /// calls) so cross-component chains (e.g. `Fixed` -> `Frontier::record_fixed` ->
    /// `AgbEngine::activate` -> more effects) stay within one `Future`.
    async fn execute(&mut self, initial: Vec<Effect>, now: Instant) {
        let mut queue: VecDeque<Effect> = initial.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::BroadcastPublish(header) => {
                    let bytes = bincode::serialize(&PrimaryMessage::Header(header, false)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastAck(ack) => {
                    let bytes = bincode::serialize(&PrimaryMessage::VantageAck(ack)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::SyncBatches(author, header_digest, missing) => {
                    self.sync_batches(author, header_digest, missing).await;
                }
                Effect::RequestTo(peer, digest) => {
                    let bytes = bincode::serialize(&PrimaryMessage::HeadersRequest(vec![digest], self.name)).expect("serializes");
                    self.send_to(peer, Bytes::from(bytes)).await;
                }
                Effect::ServeTo(peer, header) => {
                    let bytes = bincode::serialize(&PrimaryMessage::Header(header, true)).expect("serializes");
                    self.send_to(peer, Bytes::from(bytes)).await;
                }
                Effect::BlockCached(digest) => {
                    queue.extend(self.rep.on_block_available(digest));
                    queue.extend(self.agb.recheck_all(now, &mut self.lm, &mut self.rep));
                    queue.extend(self.cursor.retry());
                }
                Effect::BroadcastPropose(p) => {
                    let bytes = bincode::serialize(&PrimaryMessage::VantagePropose(p)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                // PHASE5-SPEC.md §3/D5-3: every response effect is stamped with our
                // current wish watermark here, at serialization time -- `AgbEngine`
                // itself stays watermark-free (its own construction sites use a `0`
                // placeholder, or none at all for `EchoSkip`/`NoReady`, which are
                // effects carrying just a `View` to begin with).
                Effect::BroadcastEcho(mut e) => {
                    e.wish = self.pacemaker.own_watermark();
                    let bytes = bincode::serialize(&PrimaryMessage::VantageEcho(e)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastEchoSkip(view) => {
                    let wish = self.pacemaker.own_watermark();
                    let bytes = bincode::serialize(&PrimaryMessage::VantageEchoSkip(view, self.name, wish)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastReady(mut r) => {
                    r.wish = self.pacemaker.own_watermark();
                    let bytes = bincode::serialize(&PrimaryMessage::VantageReady(r)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastNoReady(view) => {
                    let wish = self.pacemaker.own_watermark();
                    let bytes = bincode::serialize(&PrimaryMessage::VantageNoReady(view, self.name, wish)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::Fixed(view, well_formed) => {
                    let activated = self.frontier.record_fixed(view, well_formed);
                    for v in activated {
                        queue.extend(self.agb.activate(v, now, &mut self.lm, &mut self.rep));
                    }
                    queue.extend(self.try_propose_effects(now));
                }
                Effect::Completed(view, c, t) => {
                    queue.extend(self.cursor.on_completed(view, c, t));
                }
                Effect::Sealed(view, outcome) => {
                    queue.extend(self.cursor.on_sealed(view, outcome));
                }
                Effect::ArmTimer(view, kind, deadline) => {
                    self.timers.push(Reverse((deadline, view, kind)));
                }
                Effect::NotifyCommitted(commit_millis, by_worker) => {
                    self.notify_committed(commit_millis, by_worker).await;
                }
                Effect::BroadcastWish(view) => {
                    let bytes = bincode::serialize(&PrimaryMessage::VantageWish(view, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::Enter(view) => {
                    queue.extend(self.enter_view_effects(view, now));
                }
                Effect::RaiseWish(target) => {
                    queue.extend(self.pacemaker.raise_own_wish(target));
                }

                // --- PHASE6-SPEC.md §5 (reports + control log) ---
                Effect::CompletionReportable(view, proposal) => {
                    // D7-1 (PHASE7-PREP-NOTES.md): this is the FIRST genuine
                    // completion of a carrier with M != None -- whether we proposed
                    // it ourselves or another party did -- so it's exactly the
                    // "observed CompReport for a carrier resolving u" evidence the
                    // in-flight marker should refresh on, independent of (and in
                    // addition to) `Resolver::decide`'s own immediate refresh for its
                    // own attempts.
                    if let Some(entry) = &proposal.m {
                        self.resolver.note_carrier_report(entry.target_view(), now);
                    }
                    queue.extend(self.control.on_completion_reportable(view, proposal));
                }
                Effect::BroadcastCompReport(view, digest) => {
                    let bytes = bincode::serialize(&PrimaryMessage::CompReport(view, digest, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastControlInit(proposal, b_w) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlInit(proposal, b_w)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastControlEcho(proposal) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlEcho(proposal, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastControlReady(proposal) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlReady(proposal, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastControlCommit(round) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlCommit(round, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastControlTimeoutVote(round) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlTimeoutVote(round, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::BroadcastControlTimeoutAccept(round) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlTimeoutAccept(round, self.name)).expect("serializes");
                    self.broadcast(Bytes::from(bytes)).await;
                }
                Effect::ControlFetchTo(peer, view, digest) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlFetch(view, digest, self.name)).expect("serializes");
                    self.send_to(peer, Bytes::from(bytes)).await;
                }
                Effect::ControlServeTo(peer, view, proposal) => {
                    let bytes = bincode::serialize(&PrimaryMessage::ControlServe(view, proposal)).expect("serializes");
                    self.send_to(peer, Bytes::from(bytes)).await;
                }
                Effect::ArmControlTimer(round, deadline) => {
                    self.control_timers.push(Reverse((deadline, round)));
                }

                // --- PHASE6-SPEC.md §6 (anchors) ---
                Effect::ApplyAnchor(view, outcome, refs) => {
                    for r in refs {
                        queue.extend(self.rep.authorize(r));
                    }
                    queue.extend(self.agb.submit_anchor(view, outcome));
                }
            }
        }
    }

    /// P4-3: drop every cancel handler that has already resolved (message ack'd) or
    /// closed (connection gone, will never resolve) -- keeps only the ones
    /// `ReliableSender` may still be actively retrying, so the retry-until-ack
    /// semantics are unaffected (`Connection::keep_alive` treats a dropped receiver's
    /// closed sender as cancellation, per `network::reliable_sender`, so we must never
    /// drop one that's genuinely still pending).
    fn prune_cancel_handlers(&mut self) {
        self.cancel_handlers.retain_mut(|handler| matches!(handler.try_recv(), Err(TryRecvError::Empty)));
    }

    async fn broadcast(&mut self, data: Bytes) {
        let addresses: Vec<SocketAddr> = self.other_primaries.iter().map(|(_, a)| *a).collect();
        let handlers = self.network.broadcast(addresses, data).await;
        self.cancel_handlers.extend(handlers);
    }

    async fn send_to(&mut self, peer: PublicKey, data: Bytes) {
        if let Some(addr) = self.other_primaries.iter().find(|(pk, _)| *pk == peer).map(|(_, a)| *a) {
            let handler = self.network.send(addr, data).await;
            self.cancel_handlers.push(handler);
        }
    }

    /// D1/§1: ask our own workers to sync `missing` batches for `author`'s block
    /// (`header_digest`), then spawn one `store.notify_read` waiter per missing key;
    /// once *every* key for this header has resolved, call
    /// `LaneManager::set_payload_ready`.
    async fn sync_batches(&mut self, author: PublicKey, header_digest: Digest, missing: Vec<(Digest, WorkerId)>) {
        if missing.is_empty() {
            return;
        }
        let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        for (digest, worker_id) in &missing {
            by_worker.entry(*worker_id).or_default().push(digest.clone());
        }
        for (worker_id, digests) in by_worker {
            if let Some(addr) = self.worker_addresses.get(&worker_id) {
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Synchronize(digests, author)).expect("serializes");
                self.worker_network.send(*addr, Bytes::from(bytes)).await;
            }
        }

        let set: HashSet<(Digest, WorkerId)> = missing.iter().cloned().collect();
        self.pending_payload.insert(header_digest.clone(), set);
        for (digest, worker_id) in missing {
            let mut store = self.store.clone();
            let tx = self.tx_payload_ready.clone();
            let header_digest = header_digest.clone();
            tokio::spawn(async move {
                let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                if store.notify_read(key).await.is_ok() {
                    let _ = tx.send((header_digest, digest, worker_id)).await;
                }
            });
        }
    }

    /// Commit metric (Phase-2 parity, §9): forward the cursor's per-`WorkerId`
    /// notification to our own workers -- the existing worker-side observe path
    /// (`worker::synchronizer`) does the rest.
    async fn notify_committed(&mut self, commit_millis: u64, by_worker: Vec<(WorkerId, Vec<Digest>)>) {
        for (worker_id, digests) in by_worker {
            if let Some(addr) = self.worker_addresses.get(&worker_id) {
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Committed(commit_millis, digests)).expect("serializes");
                self.worker_network.send(*addr, Bytes::from(bytes)).await;
            }
        }
    }
}
