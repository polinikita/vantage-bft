// Mechanism A -- sender-side lane resume, motivated by the windowed `--withhold`
// experiment: in that experiment every lane develops a 30s hole at half the nodes
// while withholding is active; afterwards nothing re-disseminates those blocks
// (broadcast is fire-and-forget for a never-sent message; TCP never dropped anything
// so the reliable-sender's own retry-until-ack never triggers either), pull-repair is
// reference-triggered so it never fires on a gap nobody has referenced yet, and every
// protocol flatlines permanently. This is the fix: a correct sender re-publishes the
// missing span of its OWN lane once a peer's ack-census reports a persistent gap
// against it.
//
// Modeled on Starfish's subscription resume (`SubscribeBroadcastRequest` sent on
// (re)connect, crates/starfish-core/src/net_sync.rs:1923-1929; a per-peer cursor
// stream serving own blocks from the requested round, batch-capped,
// crates/starfish-core/src/broadcaster.rs:538-575,863-869 +
// crates/starfish-core/src/dag_state.rs:3256-3273), but triggered by an ack-census
// gap instead of reconnection, and requester-paced rather than server-cursor-driven:
// the author serves at most one batch per request and keeps no per-requester cursor
// state at all. PACING (corrected -- see git history for the earlier, tick-paced
// version this replaced): the requester's own frontier advances on RECEIPT of a
// batch, and its NEXT REQUEST follows immediately at that point -- not on the next
// periodic tick. The tick (`Parameters::resume_check_period_ms`) is only the EPISODE
// DETECTOR (the two-consecutive-ticks persistence bar that tells a transient blip
// apart from a real gap) and the retry driver for a request whose answer hasn't
// landed yet; once an episode is established, `VantageCore`/`SimpleItCore`'s
// `try_resume_request` is ALSO called straight from the receive path
// (`Inbound::Publish`, `on_payload_ready`) any time a publish advances that
// author's frontier while a gap remains, draining the backlog at receipt/round-trip
// pace instead of once per tick. This is what actually lets a `resume_batch`-sized
// batch cadence approach Starfish's own continuous per-peer stream throughput,
// rather than degrading it into a 1 Hz request/response ping-pong -- the "one batch
// per request, no server cursor state" simplification is preserved either way; only
// how QUICKLY the requester asks for the next one changed.
//
// Scope: this module owns ONLY the memo/backoff/dedup bookkeeping around the
// trigger/serve DECISION -- one `ResumeTrigger` (requester side) and one
// `ResumeServe` (author side) instance per `VantageCore`/`SimpleItCore`. The
// frontier/availability facts it consults are read (never duplicated) from
// `vantage::lanes::LaneManager::own_direct_frontier`/`avail_high`; the actual block
// lookup/clamp is `vantage::lanes::LaneManager::author_block_at`/
// `earliest_authored_height`/`own_tip_height`; the actual wire hand-off is
// `vantage::wire::Wire::enqueue_resume`/`enqueue_resume_header`, a non-blocking
// `try_send` onto a dedicated off-run-loop sender task (`wire::spawn_resume_sender`)
// -- see that function's doc comment for why a per-send timeout (this module's
// previous `SEND_TIMEOUT`, deleted -- see git history) is no longer needed at all:
// the run loop never performs the network send itself anymore, so there is nothing
// left on ITS side for a slow destination to block. All of that orchestration lives
// in the caller (`vantage::node`/`simpleit::node`), which already owns those three
// types -- this module stays a small, independently unit-testable piece of pure
// logic with no network/lane-cache/metrics dependency of its own.

// reconnect-replay plan §2.6/§3/§6/§14 (v3, server-authoritative floor): a SECOND,
// unrelated mechanism sharing this module purely by the orchestrator's own file
// layout choice -- Mechanism A above resumes LANE CONTENT (own blocks); this part
// resumes ONE-SHOT AGB/consensus messages (`VantageCore::broadcast_recorded`'s
// outbox) lost to a volatile session death. `pending_low` (the authoritative floor
// this mechanism serves from) lives on `VantageCore` itself, not here -- everything
// below is the pure, unit-testable bookkeeping AROUND it: the requester-side "do we
// have an open ask toward peer X, and is it time to (re-)Hello" state machine
// (`ReplayEpisodes`), the author-side "is a replay to X already in flight"
// query (`InFlightState`/`in_flight_state`) and per-peer served-bytes budget
// (`ServeBudget`), and the author-side "when did we last serve-or-nudge X"
// cooldown (`NudgeMemo`). None of these touch the network, the outbox, or
// `pending_low` directly -- see `vantage::node::VantageCore`'s own wiring (the Hello/
// Done dispatch arms, `resume_tick`) for how they compose.
//
// D4 caveats (Byzantine-forgery boundary, unchanged in spirit from v1/v2, restated
// for v3's authority inversion -- module doc, per the design doc's own instruction):
// (i) a Byzantine `j` inflating its own wish (the Hello floor HINT) only ever
// under-asks for `j`'s OWN messages -- and under v3 even that is moot: the serve
// floor is `min(hello.floor, pending_low[j])`, and honest authors' `pending_low`
// never consults `j`'s claims at all (it is fed exclusively by the transport's own
// exact drop reports). (ii) A forged `Hello(sender=j, floor=0)` -- claiming a
// non-member's or the wrong party's identity is already rejected upstream by
// `dispatch_inbound`'s membership gate, so this is `j` forging ITS OWN floor low --
// only HELPS `j`: the serve goes to `j`'s own committee address, from `min(0,
// pending_low[j])`, a SUPERSET of whatever `j` actually needs; it wastes at most the
// per-peer serve budget (`ServeBudget`) -- no suppression lever exists (the old v1/v2
// latched-floor authority that a low forged floor could exploit is gone by
// construction). (iii) A forged `Done(sender=j, complete=true)` closes OUR episode
// toward `j` early -- but if `j` genuinely dropped something of ours, `j`'s OWN
// `pending_low[us]` (fed by `j`'s transport, not by anything `j` can forge) drives
// `j`'s own server-side nudge loop, which reopens the exchange from `j`'s side
// regardless of what our episode thinks: a forged early close costs a DELAY (until
// the next nudge/tick), never a permanent loss.

use crypto::PublicKey;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::primary::Height;

/// Requester-side trigger state (design doc step 2). One instance per
/// `VantageCore`/`SimpleItCore`, covering every OTHER committee member's lane (the
/// caller skips `author == self`; this type itself is author-agnostic beyond using
/// `author` as its memo key, so nothing here enforces that).
///
/// Models a small per-author state machine, NOT independent per-tick coin flips:
/// "not yet established" (a candidate gap has been seen on at most one immediately-
/// preceding tick -- the two-consecutive-ticks persistence bar gates entry) versus
/// "established" (this author's gap has already cleared that bar at least once and
/// stays active for the rest of THIS episode). The distinction matters because the
/// design doc's step 3 explicitly describes ONGOING catch-up as immediate, not
/// re-gated: "the requester's frontier advances on receipt, its next request
/// follows" -- i.e. once an episode is established, a request follows as soon as
/// the PREVIOUS one's ENTIRE expected span has actually landed, rather than
/// re-earning two fresh ticks for every incremental batch.
///
/// Per-(lane) in-flight cap of 1: an established episode still only ever has ONE
/// outstanding, unanswered request at a time. This matters because a served batch
/// answers as `resume_batch` SEPARATE `Header(_, false)` publishes, each one
/// individually advancing `frontier` and each one individually reaching this type's
/// caller (`VantageCore`/`SimpleItCore`'s receipt-continuation hooks). Without the
/// cap, EVERY one of those `resume_batch` individual arrivals would look like "the
/// gap moved, ask again" and fire its own follow-up request -- a batch of N lands,
/// and up to N new (mostly redundant, since most of what they'd ask for is already
/// mid-flight) requests would fire, compounding every round trip. The cap holds a
/// single request "open" until `frontier` reaches its own requested span's end (the
/// whole batch landed, or the author's own tip was short of it and nothing more is
/// coming for this ask either way) -- only THEN does the next check ask for
/// whatever is current. A request whose span hasn't fully landed within `backoff`
/// is retried for the CURRENT gap position (not the stale original `from` -- some
/// of it may already have arrived), the same safety net a lost/dropped ack already
/// has elsewhere in this codebase.
#[derive(Default)]
pub struct ResumeTrigger {
    /// Per author, while NOT YET established: the gap height (`from`) seen on the
    /// immediately preceding tick -- the raw material for the two-consecutive-ticks
    /// check. No entry means either no gap has been seen yet, or the entry was
    /// cleared (gap closed, or just promoted to `established`).
    pending: HashMap<PublicKey, Height>,
    /// Per author: has this gap EPISODE already cleared the initial persistence bar.
    /// Cleared (removed) the moment the gap fully closes (`avail < frontier+1`) --
    /// the NEXT gap against this same author, even if it reopens at the identical
    /// height, is a fresh episode and must re-earn persistence from scratch. NOT
    /// cleared merely because `frontier` advanced while the gap is still open --
    /// that is the "next request follows" continuation case, not a new episode.
    established: HashSet<PublicKey>,
    /// Per author: `(expected_end, sent_at)` of the single outstanding request, if
    /// any -- the in-flight cap of 1. `expected_end = from + resume_batch - 1` AT
    /// THE TIME that request was sent. Cleared (implicitly, by the `from <=
    /// expected_end` check below going false) once `frontier` reaches it; until
    /// then, every check for the SAME still-open request is suppressed unless
    /// `backoff` has elapsed since `sent_at`.
    in_flight: HashMap<PublicKey, (Height, Instant)>,
}

impl ResumeTrigger {
    pub fn new() -> Self {
        Self::default()
    }

    /// One check's worth of the design doc's step-2/step-3 logic, for a single
    /// lane author -- called from BOTH the periodic tick and the receipt-
    /// continuation hooks (`VantageCore`/`SimpleItCore::try_resume_request`'s only
    /// caller-visible entry point). `frontier`/`avail` are the caller's already-
    /// computed `LaneManager::own_direct_frontier`/`avail_high` values for
    /// `author`; `resume_batch` is `Parameters::resume_batch` (the span size THIS
    /// call would request, used only to size the in-flight cap below -- see this
    /// type's own doc comment).
    ///
    /// Returns `Some(from)` -- the height to name in a fresh
    /// `VantageLaneResume(author, from, self)` -- iff there is a gap right now
    /// (`avail >= frontier+1`), AND EITHER this exact gap height was already seen
    /// on the immediately preceding call for this author (first-time persistence:
    /// two consecutive ticks), OR this author's episode is already established AND
    /// there is no still-open in-flight request blocking it (continuation: fire
    /// again as soon as the previous request's whole span has landed, or `backoff`
    /// has elapsed since it was sent).
    pub fn check(
        &mut self,
        author: PublicKey,
        frontier: Height,
        avail: Height,
        now: Instant,
        backoff: Duration,
        resume_batch: Height,
    ) -> Option<Height> {
        let from = frontier + 1;
        if avail < from {
            // Gap closed: this episode is over. A later gap against this author --
            // even one that happens to reopen at this exact height -- is a fresh
            // episode and must re-earn persistence.
            self.pending.remove(&author);
            self.established.remove(&author);
            self.in_flight.remove(&author);
            return None;
        }
        if !self.established.contains(&author) {
            if self.pending.get(&author) == Some(&from) {
                self.established.insert(author);
                // Falls through to the in-flight-cap-gated fire below -- the tick
                // that crosses the persistence bar also sends the first request;
                // there is no reason to burn a THIRD tick just to re-confirm what
                // two already established, and there cannot yet be an in-flight
                // entry for a freshly-established episode.
            } else {
                self.pending.insert(author, from);
                return None;
            }
        }
        if let Some(&(expected_end, sent_at)) = self.in_flight.get(&author) {
            // `from <= expected_end` means the single outstanding request's span
            // has NOT fully landed yet (some or none of it has) -- the in-flight
            // cap of 1 in effect. Only retry it, for the CURRENT gap position, once
            // `backoff` has elapsed.
            if from <= expected_end && now.duration_since(sent_at) < backoff {
                return None;
            }
            // Either the whole span landed (`from > expected_end`) or backoff
            // elapsed on a stale/partial one -- either way this call is free to
            // issue a (possibly brand new, possibly retried) request below, which
            // replaces this entry.
        }
        let end = from.saturating_add(resume_batch.max(1)).saturating_sub(1);
        self.in_flight.insert(author, (end, now));
        Some(from)
    }
}

/// Author-side serve dedup state (design doc step 3): per requester, the last `from`
/// this party served a resume batch for and when -- "one-shot dedup on the author
/// side: at most one batch served per (requester, from) per `resume_backoff_ms`".
/// One instance per `VantageCore`/`SimpleItCore`, since every entry here is
/// necessarily about THIS party's own lane (a `VantageLaneResume` naming any other
/// author is rejected before reaching this type -- see
/// `vantage::node::VantageCore::dispatch_inbound`'s `Inbound::LaneResume` arm).
#[derive(Default)]
pub struct ResumeServe {
    last_served: HashMap<PublicKey, (Height, Instant)>,
}

impl ResumeServe {
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` iff a batch for `(requester, from)` should be served now -- i.e. this
    /// is NOT a repeat of the same `(requester, from)` this party already served
    /// within the last `backoff`. `from` here is the ALREADY-CLAMPED height (the
    /// caller's own GC-floor clamp has already run) -- comparing the clamped value
    /// is what actually determines the served span, so that is what dedup should key
    /// on, not the requester's raw (possibly out-of-range) wire field.
    ///
    /// Recording happens unconditionally on a `true` result -- callers must not
    /// invoke this speculatively and then decide not to serve after all, or a later
    /// genuine repeat within the backoff window would wrongly be let through.
    pub fn should_serve(
        &mut self,
        requester: PublicKey,
        from: Height,
        now: Instant,
        backoff: Duration,
    ) -> bool {
        if let Some((last_from, at)) = self.last_served.get(&requester) {
            if *last_from == from && now.duration_since(*at) < backoff {
                return false;
            }
        }
        self.last_served.insert(requester, (from, now));
        true
    }
}

/// reconnect-replay plan §2.6: requester-side episode bookkeeping -- per peer we
/// might be behind (a `VantageResumeHello` we've sent, or intend to keep sending,
/// asking THEM to replay toward US), whether an episode is currently open and when we
/// last sent a Hello as part of it. An episode opens on (i) our own reconnect event,
/// (ii)/re-Hello: the periodic tick for an already-open episode past
/// `resume_backoff_ms`, or (iii) receipt of a Hello FROM that peer (reciprocation);
/// it closes on `VantageReplayDone(complete=true)`, or auto-closes past
/// `replay_episode_max_ms` (the expiry valve) -- reopened by the next (i)/(iii)
/// event, or by a `Done(complete=false)` continuation (§14 A3: silencing outright
/// after a partial serve is exactly the bug that amendment fixes elsewhere; this
/// type's own `open` has no such silencing -- see `VantageCore`'s `ReplayDone` arm).
///
/// Docker-validation regression (mutual-Hello-loop oscillator): reciprocation
/// (`on_hello_received`, below) is gated by `last_hello_sent`, a SENT-memo that
/// SURVIVES episode close -- per plan §7's exact sentence: "both sides memo
/// `last_hello_at` at send; an incoming Hello reciprocates only if own `last_hello_
/// at` for that peer is stale". Episode-PRESENCE gating (`is_open()` alone, this
/// type's own earlier, defective form) is NOT equivalent: `Done(complete=true)`
/// rides the task-owned replay pool's `ReliableSender` (its own TCP connection);
/// the reciprocal Hello we're gating rides the MAIN pool's `send_message` (a
/// DIFFERENT TCP connection, additionally delayed by that pool's own 5ms batching
/// coalescer) -- two independent connections have no relative ordering guarantee
/// AT ALL. Whenever a `Done(complete=true)` outraces the peer's own reciprocal
/// Hello, presence-gating finds the episode ALREADY closed, reciprocates with a
/// FRESH Hello, and the peer -- symmetrically racing the exact same way -- does
/// the same back: the pair phase-locks into a mutual Hello/serve loop at the
/// backoff period, forever (confirmed in docker: ~14 replay frames/s + ~0.6
/// Hello/s steady chatter between two nodes, from boot, never quiescing). The
/// sent-memo is immune BECAUSE closing an episode does not erase "I sent this
/// peer a Hello moments ago" -- `last_hello_sent` is peer-scoped, not
/// episode-scoped, and is updated at the actual send (`VantageCore::send_resume_
/// hello`/`send_nudge_hello`), never touched by `close`.
#[derive(Default)]
pub struct ReplayEpisodes {
    open: HashMap<PublicKey, Episode>,
    /// The sent-memo above -- committee-bounded (one entry per peer), so a plain
    /// `HashMap` (never pruned, matching `resume::NudgeMemo::last`/`ServeBudget::
    /// windows`'s identical convention elsewhere in this module).
    last_hello_sent: HashMap<PublicKey, Instant>,
}

struct Episode {
    /// When this episode was (last) opened -- the expiry valve's own reference
    /// instant. Continuation (`open`, called again for an already-open episode)
    /// resets this, treating "the answer to our last Hello just arrived, and there
    /// is more to come" as equivalent to a fresh event for valve purposes.
    opened_at: Instant,
    /// When we last sent a Hello as part of this episode -- the backoff gate
    /// `tick`'s re-Hello (and nothing else: `open`'s own immediate send is never
    /// backoff-gated) checks against.
    last_hello_at: Instant,
}

impl ReplayEpisodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_open(&self, peer: &PublicKey) -> bool {
        self.open.contains_key(peer)
    }

    /// (i)/continuation: unconditionally (re)opens the episode toward `peer`,
    /// resetting both the expiry valve and the backoff gate to `now`. The caller
    /// ALWAYS sends a Hello immediately after calling this -- it is never itself
    /// backoff-gated (a one-shot event, or a continuation the server itself is
    /// actively answering, both warrant an immediate ask).
    pub fn open(&mut self, peer: PublicKey, now: Instant) {
        // Diagnostics only (module doc: item 4f) -- distinguishes a genuinely NEW
        // episode from a continuation's re-open; never consulted for correctness.
        let cause = if self.open.contains_key(&peer) {
            "reopened"
        } else {
            "opened"
        };
        log::debug!("vantage resume: episode {cause}: peer={peer}");
        self.open.insert(
            peer,
            Episode {
                opened_at: now,
                last_hello_at: now,
            },
        );
    }

    /// (iii): reciprocation on receiving a Hello FROM `peer`. Gated by the SENT-
    /// memo (`last_hello_sent`), per plan §7's exact sentence -- see this type's
    /// own doc comment for why episode-presence gating alone is NOT equivalent
    /// (the Done-vs-Hello cross-pool race). Reciprocates (opens a fresh episode,
    /// returns `true`) iff NO episode is currently open toward `peer` AND EITHER
    /// we have never sent `peer` a Hello, or `backoff` has elapsed since the last
    /// one we sent -- otherwise a no-op, returning `false` (our own tick, or the
    /// in-flight send this Hello likely raced, is already covering it).
    pub fn on_hello_received(&mut self, peer: PublicKey, now: Instant, backoff: Duration) -> bool {
        if self.is_open(&peer) {
            return false;
        }
        let sent_recently = self
            .last_hello_sent
            .get(&peer)
            .is_some_and(|&at| now.duration_since(at) < backoff);
        if sent_recently {
            return false;
        }
        self.open(peer, now);
        true
    }

    /// Records that a Hello was just sent to `peer` (ANY trigger -- event,
    /// tick re-Hello, reciprocal, or nudge; `VantageCore::send_resume_hello`/
    /// `send_nudge_hello` are this map's only two writers, so every actual send is
    /// covered regardless of which of the four call sites triggered it). Deliberately
    /// NOT touched by `close`/the expiry valve -- surviving episode close is the
    /// entire point (this type's own doc comment).
    pub fn record_hello_sent(&mut self, peer: PublicKey, now: Instant) {
        self.last_hello_sent.insert(peer, now);
    }

    /// (ii) + the expiry valve: called once per `resume_tick` for every peer with an
    /// episode open. Closes (removes) the episode instead, returning `false`, once
    /// `max_age` has elapsed since it was (last) opened -- "reopened by the next
    /// event/nudge" (design doc §2.6), never by this method. Otherwise returns
    /// `true` (and records a fresh `last_hello_at`) iff `backoff` has elapsed since
    /// the last Hello sent for this episode; `false` (no-op) if the backoff hasn't
    /// elapsed yet. A peer with no open episode is not tracked at all -- `false`.
    pub fn tick(
        &mut self,
        peer: PublicKey,
        now: Instant,
        backoff: Duration,
        max_age: Duration,
    ) -> bool {
        let Some(episode) = self.open.get_mut(&peer) else {
            return false;
        };
        if now.duration_since(episode.opened_at) >= max_age {
            log::debug!("vantage resume: episode expired: peer={peer}");
            self.open.remove(&peer);
            return false;
        }
        if now.duration_since(episode.last_hello_at) >= backoff {
            episode.last_hello_at = now;
            true
        } else {
            false
        }
    }

    /// `VantageReplayDone(complete=true)`: this episode is answered, close it.
    pub fn close(&mut self, peer: &PublicKey) {
        if self.open.remove(peer).is_some() {
            log::debug!("vantage resume: episode closed: peer={peer}");
        }
    }
}

/// reconnect-replay plan §6: author-side in-flight-replay-stream query, over a
/// caller-owned map (`VantageCore`/the resume task's shared `Arc<Mutex<HashMap<
/// PublicKey, Instant>>>` -- see `vantage::wire::InFlightMap`) this module never
/// allocates or holds itself, keeping this a pure, lock-free, unit-testable
/// decision: `InFlight` blocks a fresh Hello outright (the running stream already
/// serves `>= pending_low`, a superset of any concurrent ask -- §6); `Expired`
/// additionally reports that a STALE entry was found, distinct from `Absent` (the
/// common case for an honest, caught-up peer) so the caller can bump a metric and
/// evict the stale entry once, rather than treating every subsequent Hello as if it
/// still needed a fresh discovery.
///
/// TTL = `Parameters::replay_episode_max_ms` (audit-3 A6), NOT a shorter value:
/// strict `Message`-priority scheduling (§5) means replay throughput is not
/// guaranteed, so a shorter TTL could expire mid-drain and cause a duplicate
/// re-serve; sizing it to the requester's own episode lifetime avoids that by
/// construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InFlightState {
    Absent,
    InFlight,
    Expired,
}

/// See `InFlightState`'s own doc comment.
pub fn in_flight_state(
    map: &HashMap<PublicKey, Instant>,
    peer: &PublicKey,
    now: Instant,
    ttl: Duration,
) -> InFlightState {
    match map.get(peer) {
        None => InFlightState::Absent,
        Some(&started) if now.duration_since(started) < ttl => InFlightState::InFlight,
        Some(_) => InFlightState::Expired,
    }
}

/// reconnect-replay plan §6: author-side per-peer served-bytes budget, a rolling
/// `resume_backoff_ms` window capped at `Parameters::replay_serve_max_bytes` --
/// bounds per-peer extraction to roughly `replay_serve_max_bytes / resume_backoff_ms`
/// bytes/s. "Rolling" here means a fixed window anchored the first time a peer is
/// served after its previous window (if any) has fully elapsed -- not a sliding
/// average -- mirroring `ResumeServe`'s own backoff-window semantics above rather
/// than introducing a second windowing convention into this one file.
#[derive(Default)]
pub struct ServeBudget {
    windows: HashMap<PublicKey, (Instant, usize)>,
}

impl ServeBudget {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes still available to serve `peer` in its CURRENT window -- `max_bytes`
    /// (a fresh window) if none is open yet, or the previous one has fully elapsed;
    /// `max_bytes` minus whatever has already been `record`ed in the still-open
    /// window otherwise. Never mutates -- pair with `record` after the caller
    /// decides how many bytes it actually served (which may be zero, e.g. a
    /// deferred-to-next-window Hello).
    pub fn remaining(
        &self,
        peer: PublicKey,
        now: Instant,
        window: Duration,
        max_bytes: usize,
    ) -> usize {
        match self.windows.get(&peer) {
            Some(&(start, served)) if now.duration_since(start) < window => {
                max_bytes.saturating_sub(served)
            }
            _ => max_bytes,
        }
    }

    /// Record `bytes` just served to `peer` -- opens a fresh window (dated `now`)
    /// if none is open, or the previous one has fully elapsed; otherwise accumulates
    /// into the still-open one. A `bytes = 0` call still opens a window if none
    /// exists, matching `remaining`'s own "is there an open window" check.
    pub fn record(&mut self, peer: PublicKey, bytes: usize, now: Instant, window: Duration) {
        match self.windows.get_mut(&peer) {
            Some((start, served)) if now.duration_since(*start) < window => {
                *served += bytes;
            }
            _ => {
                self.windows.insert(peer, (now, bytes));
            }
        }
    }
}

/// reconnect-replay plan §2.6/§14 A3: author-side "when did we last serve-or-nudge
/// peer X" cooldown -- the single timestamp both A3's nudge condition ("backoff
/// elapsed since last serve-or-nudge to X") and its own refresh (on either a serve OR
/// a nudge) touch, so a serve and a nudge share one cooldown instead of two
/// independently-ticking timers that could otherwise both fire back to back for the
/// same peer.
#[derive(Default)]
pub struct NudgeMemo {
    last: HashMap<PublicKey, Instant>,
}

impl NudgeMemo {
    pub fn new() -> Self {
        Self::default()
    }

    /// A3's own backoff-timing third of its nudge condition -- the caller is
    /// responsible for the other two (`pending_low[peer].is_some()` and `peer` not
    /// in-flight; this type never touches either, keeping it a pure, no-network,
    /// no-shared-map decision). `true` iff `backoff` has elapsed since the last
    /// recorded serve-or-nudge toward `peer`, or none was ever recorded.
    pub fn due(&self, peer: PublicKey, now: Instant, backoff: Duration) -> bool {
        self.last
            .get(&peer)
            .is_none_or(|&at| now.duration_since(at) >= backoff)
    }

    /// Record that a serve or a nudge just happened toward `peer`.
    pub fn record(&mut self, peer: PublicKey, now: Instant) {
        self.last.insert(peer, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> PublicKey {
        PublicKey([byte; 32])
    }

    /// "no trigger in a gap-free run": `avail <= frontier` (no height with an
    /// (f+1)-mark beyond what's already held) never fires, no matter how many ticks
    /// elapse. `resume_batch` is irrelevant here (the gap-free branch returns before
    /// ever consulting it) -- a fixed placeholder throughout this test.
    #[test]
    fn no_trigger_when_gap_free() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);
        for _ in 0..5 {
            assert_eq!(trigger.check(author, 10, 10, now, backoff, 1), None);
            assert_eq!(trigger.check(author, 10, 9, now, backoff, 1), None);
        }
    }

    /// "trigger fires after two consecutive gap ticks and not before": the first
    /// tick observing a gap only records it; the SAME gap on the very next tick
    /// fires. `resume_batch=1` matches this test's own one-height-per-check shape
    /// (see `established_episode_continues_immediately_as_frontier_advances`'s doc
    /// comment for why the exact value matters once an episode continues).
    #[test]
    fn trigger_fires_after_two_consecutive_gap_ticks() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        // frontier=10, avail=12 => gap at from=11.
        assert_eq!(
            trigger.check(author, 10, 12, now, backoff, 1),
            None,
            "first observation must not fire"
        );
        assert_eq!(
            trigger.check(
                author,
                10,
                12,
                now + Duration::from_millis(1_000),
                backoff,
                1
            ),
            Some(11),
            "second consecutive observation of the SAME gap must fire"
        );
    }

    /// A single one-tick blip (gap observed, then gone, then back) must not count as
    /// two CONSECUTIVE observations -- persistence resets whenever the gap closes.
    #[test]
    fn single_tick_blip_does_not_count_as_persistent() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        // Gap closes for one tick (e.g. avail dipped, or was a stale read).
        assert_eq!(
            trigger.check(
                author,
                10,
                10,
                now + Duration::from_millis(1_000),
                backoff,
                1
            ),
            None
        );
        // Gap reappears -- this must count as observation #1 again, not #3.
        assert_eq!(
            trigger.check(
                author,
                10,
                12,
                now + Duration::from_millis(2_000),
                backoff,
                1
            ),
            None,
            "the memo was cleared by the gap-free tick; this is a fresh first observation"
        );
        assert_eq!(
            trigger.check(
                author,
                10,
                12,
                now + Duration::from_millis(3_000),
                backoff,
                1
            ),
            Some(11)
        );
    }

    /// "backoff suppresses repeat requests": once fired for (author, from), further
    /// ticks with the identical persistent gap are suppressed until `backoff`
    /// elapses, then fire again. `frontier` never advances in this test, so this
    /// exercises the in-flight cap's own backoff-retry branch (`from <=
    /// expected_end` stays true throughout, since nothing ever moves `from`).
    #[test]
    fn backoff_suppresses_repeat_requests() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(trigger.check(author, 10, 12, t1, backoff, 1), Some(11));

        // Still within backoff of t1: every subsequent tick with the same gap is
        // suppressed, even though persistence itself is satisfied every time.
        for ms in [1_500, 2_000, 3_000, 4_900] {
            let t = now + Duration::from_millis(ms);
            assert_eq!(
                trigger.check(author, 10, 12, t, backoff, 1),
                None,
                "at t+{ms}ms the backoff since t+1000ms has not yet elapsed"
            );
        }

        // Backoff has now elapsed since t1 (t1 + 4000ms = now+5000ms).
        let t2 = now + Duration::from_millis(5_001);
        assert_eq!(trigger.check(author, 10, 12, t2, backoff, 1), Some(11));
    }

    /// Design doc step 3: "the requester's frontier advances on receipt, its next
    /// request follows" -- once an episode is established, a frontier advance that
    /// fully lands the PREVIOUS request's whole span (here, `resume_batch=1`, so
    /// every single-height advance qualifies) must fire the next request
    /// IMMEDIATELY, not re-earn a second two-tick persistence wait. Re-gating every
    /// incremental batch would halve steady-state catch-up throughput for no
    /// correctness benefit; see `established_episode_waits_for_the_whole_in_flight_
    /// batch_before_continuing` for the complementary case (`resume_batch > 1`,
    /// continuation withheld until the FULL span lands).
    #[test]
    fn established_episode_continues_immediately_as_frontier_advances() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(
            trigger.check(author, 10, 12, t1, backoff, 1),
            Some(11),
            "second consecutive tick establishes the episode and fires the first request"
        );

        // Frontier advances to 11 (the resume batch delivered height 11, fully
        // landing the resume_batch=1 span this exact request asked for) -- the VERY
        // NEXT check must fire for the new gap (from=12) immediately, not wait for a
        // second observation of it.
        let t2 = now + Duration::from_millis(1_500);
        assert_eq!(
            trigger.check(author, 11, 12, t2, backoff, 1),
            Some(12),
            "established episode: continues immediately once the in-flight span \
             (here, exactly one height) has fully landed"
        );

        // And again, immediately, as frontier keeps advancing.
        let t3 = now + Duration::from_millis(2_500);
        assert_eq!(
            trigger.check(author, 12, 12, t3, backoff, 1),
            None,
            "frontier caught up to avail exactly -- no gap left, nothing to request"
        );
    }

    /// The in-flight cap of 1, complementing the immediately-preceding test:
    /// `resume_batch=8` means ONE request answers with up to 8 headers, each
    /// arriving as its own separate publish and each one individually advancing
    /// `frontier` by exactly 1 -- receipt-continuation must NOT fire a fresh
    /// request on every one of those individual arrivals (that would compound a
    /// single round trip's answer into up to 8 redundant new asks); it must wait
    /// until `frontier` reaches the END of the span this request already covers.
    #[test]
    fn established_episode_waits_for_the_whole_in_flight_batch_before_continuing() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);
        let resume_batch = 8;

        // avail is far ahead (100) so the gap never closes across this whole test.
        assert_eq!(
            trigger.check(author, 10, 100, now, backoff, resume_batch),
            None
        );
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(
            trigger.check(author, 10, 100, t1, backoff, resume_batch),
            Some(11),
            "establishes the episode; requests heights 11..=18 (resume_batch=8)"
        );

        // Headers 11..=17 arrive one at a time, each individually advancing
        // frontier by 1 -- NONE of these may fire a new request: the in-flight
        // request's own span (11..=18) hasn't fully landed yet.
        for (i, height) in (11..=17u64).enumerate() {
            let t = t1 + Duration::from_millis(100 + i as u64);
            assert_eq!(
                trigger.check(author, height, 100, t, backoff, resume_batch),
                None,
                "height {height} landed, but the in-flight span (11..=18) is not \
                 fully covered yet -- must not fire a redundant new request"
            );
        }

        // Height 18 (the LAST one in the in-flight span) lands -- frontier=18,
        // from=19, which is beyond `expected_end=18`: the whole batch is in, and
        // continuation fires immediately for the next span.
        let t_last = t1 + Duration::from_millis(200);
        assert_eq!(
            trigger.check(author, 18, 100, t_last, backoff, resume_batch),
            Some(19),
            "the in-flight span's last height just landed -- fire immediately for \
             the next one, at receipt pace, not tick pace"
        );
    }

    /// A gap that fully closes (frontier catches up to avail) ends the episode; a
    /// LATER, distinct gap against the SAME author -- even reopening at the same
    /// height a coincidence would produce -- is a fresh episode and must re-earn the
    /// two-consecutive-ticks persistence bar from scratch, not reuse the old
    /// "established" state.
    #[test]
    fn closed_gap_ends_the_episode_next_gap_re_earns_persistence() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(trigger.check(author, 10, 12, t1, backoff, 1), Some(11));
        let t2 = now + Duration::from_millis(2_000);
        assert_eq!(trigger.check(author, 11, 12, t2, backoff, 1), Some(12));

        // Frontier fully catches up to avail -- the gap closes.
        let t3 = now + Duration::from_millis(3_000);
        assert_eq!(trigger.check(author, 12, 12, t3, backoff, 1), None);

        // A fresh gap opens later (avail advanced further) -- must re-earn
        // persistence: not on the first tick...
        let t4 = now + Duration::from_millis(4_000);
        assert_eq!(
            trigger.check(author, 12, 15, t4, backoff, 1),
            None,
            "new episode's first observation must not fire immediately"
        );
        // ...only on the second consecutive tick observing the SAME new gap.
        let t5 = now + Duration::from_millis(5_000);
        assert_eq!(trigger.check(author, 12, 15, t5, backoff, 1), Some(13));
    }

    /// Independent lanes (distinct authors) never interfere with each other's memo.
    #[test]
    fn independent_authors_do_not_interfere() {
        let mut trigger = ResumeTrigger::new();
        let (a1, a2) = (key(1), key(2));
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(a1, 10, 12, now, backoff, 1), None);
        // a2's very first observation, same tick -- independently not persistent yet.
        assert_eq!(trigger.check(a2, 5, 5, now, backoff, 1), None); // a2 has no gap at all
        assert_eq!(
            trigger.check(a1, 10, 12, now + Duration::from_millis(1_000), backoff, 1),
            Some(11)
        );
    }

    /// Author-side dedup: a repeat of the identical (requester, from) within backoff
    /// is suppressed, and a DIFFERENT `from` for the same requester is never
    /// suppressed by an unrelated prior entry. This is also exactly the guard
    /// against burst amplification under receipt-continuation: a requester's
    /// periodic tick can decide to retry the SAME (author, from) at close to the
    /// same instant a genuinely NEW request for that pair also lands (e.g. a
    /// transport-level retransmit of an already-in-flight ask) -- from the
    /// author's side both arrive as two `VantageLaneResume`s naming the identical
    /// (requester, from), and this dedup is what stops that from ever producing
    /// two served batches.
    #[test]
    fn resume_serve_dedup_same_from_suppressed_different_from_not() {
        let mut serve = ResumeServe::new();
        let requester = key(9);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert!(serve.should_serve(requester, 5, now, backoff));
        assert!(
            !serve.should_serve(requester, 5, now + Duration::from_millis(1_000), backoff),
            "identical (requester, from) within backoff must be suppressed"
        );
        assert!(
            serve.should_serve(requester, 13, now + Duration::from_millis(1_000), backoff),
            "a different `from` for the same requester is never suppressed by the from=5 entry"
        );
    }

    /// The specific interleaving requested for Mechanism A's pacing fix: a tick's
    /// retry of an UNANSWERED (author, from) races a receipt-driven continuation
    /// that only fires once that SAME request's answer actually lands (advancing
    /// `frontier`, closing THAT gap, and opening the next one at `from+resume_
    /// batch`). Simulated end to end through the two REQUESTER-side call sites
    /// (`try_resume_request`'s shared logic, exercised here as direct `check` calls
    /// standing in for the tick and for `Inbound::Publish`'s receipt hook) and the
    /// one AUTHOR-side dedup they both funnel through.
    #[test]
    fn tick_retry_racing_receipt_continuation_never_double_serves() {
        let mut trigger = ResumeTrigger::new();
        let mut serve = ResumeServe::new();
        let author = key(1);
        let requester = key(9);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        // Establish the episode (two consecutive ticks) and fire the first request.
        // `resume_batch=1` matches this test's own one-height-per-check shape.
        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(trigger.check(author, 10, 12, t1, backoff, 1), Some(11));
        // Author serves it.
        assert!(serve.should_serve(requester, 11, t1, backoff));

        // The requester's OWN frontier hasn't advanced yet (answer still in
        // flight) when its periodic tick fires again at t2 -- a RETRY of the
        // identical (author, from=11), suppressed by the requester's own
        // backoff, so the author never even sees a second wire message from
        // this specific source.
        let t2 = now + Duration::from_millis(1_500);
        assert_eq!(
            trigger.check(author, 10, 12, t2, backoff, 1),
            None,
            "requester-side backoff already suppresses the tick's own retry"
        );

        // Suppose, despite that, a second `VantageLaneResume(author, 11, requester)`
        // reaches the author anyway (transport-level retransmit of the FIRST
        // request, or any other source of duplication upstream of this dedup) --
        // the author-side guard is the one that must hold regardless of why a
        // repeat arrived.
        assert!(
            !serve.should_serve(requester, 11, t2, backoff),
            "author-side dedup must reject a repeat of (requester, from=11) even if \
             the requester-side backoff that normally prevents it is bypassed"
        );

        // The batch lands: frontier advances to 11, closing the from=11 gap and
        // opening from=12 -- receipt-continuation (not another tick) fires
        // immediately for the NEW span, and the author serves it (different
        // `from`, never blocked by the from=11 entry above).
        let t3 = now + Duration::from_millis(1_600);
        assert_eq!(
            trigger.check(author, 11, 12, t3, backoff, 1),
            Some(12),
            "established episode continues immediately once frontier advances, \
             matching the receipt-continuation call site, not the tick's own cadence"
        );
        assert!(serve.should_serve(requester, 12, t3, backoff));
    }

    /// Author-side dedup: once `backoff` has elapsed, the SAME (requester, from) may
    /// be served again -- a suppressed (`false`) call must not itself refresh the
    /// stored timestamp (else the window would never actually expire).
    #[test]
    fn resume_serve_dedup_expires_after_backoff() {
        let mut serve = ResumeServe::new();
        let requester = key(9);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert!(serve.should_serve(requester, 5, now, backoff));
        assert!(!serve.should_serve(requester, 5, now + Duration::from_millis(1_000), backoff));
        assert!(!serve.should_serve(requester, 5, now + Duration::from_millis(3_999), backoff));
        assert!(
            serve.should_serve(requester, 5, now + Duration::from_millis(4_001), backoff),
            "once backoff has elapsed since the ORIGINAL grant (not since a suppressed retry) \
             the same (requester, from) may be served again"
        );
    }

    // --- reconnect-replay plan §2.6/§6/§14 (v3): `ReplayEpisodes`, `InFlightState`/
    // `in_flight_state`, `ServeBudget`, `NudgeMemo`. Unrelated to Mechanism A above
    // (see this module's own doc comment) -- these tests never touch `ResumeTrigger`/
    // `ResumeServe`.

    #[test]
    fn episode_opens_and_reports_open() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();

        assert!(!episodes.is_open(&peer));
        episodes.open(peer, now);
        assert!(episodes.is_open(&peer));
    }

    /// (iii) "memo-deduped": the FIRST Hello from a peer we have no open episode
    /// toward (and have never sent) opens one and asks for a reciprocal send; a
    /// SECOND Hello while that episode is still open must not re-trigger a send
    /// (the tick's own backoff-gated schedule is already covering it).
    #[test]
    fn on_hello_received_is_memo_deduped() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert!(
            episodes.on_hello_received(peer, now, backoff),
            "the first Hello from a peer with no open episode and no recent send \
             must trigger a reciprocal send"
        );
        assert!(episodes.is_open(&peer));
        assert!(
            !episodes.on_hello_received(peer, now + Duration::from_millis(10), backoff),
            "a second Hello while the episode is already open must not re-trigger a send"
        );
    }

    /// **Docker-validation regression**: the mutual-Hello-loop oscillator. `A` has
    /// an open episode toward `B` (having just sent it a Hello); `B`'s answering
    /// `Done(complete=true)` closes `A`'s episode BEFORE `B`'s own reciprocal
    /// Hello -- sent over a DIFFERENT connection (the Done rides the task-owned
    /// replay pool's `ReliableSender`; the reciprocal Hello rides the main pool's
    /// `send_message`, plus that pool's own 5ms batching-coalescer delay) -- ever
    /// lands at `A`; the two frames have no relative ordering guarantee at all.
    /// Episode-PRESENCE gating alone (this type's earlier, defective form) would
    /// see no episode open at that point and reciprocate with a FRESH Hello --
    /// which `B`, racing the identical way, would do right back: the pair
    /// phase-locks into a mutual Hello/serve loop at the backoff period, forever
    /// (confirmed in docker: ~14 replay frames/s + ~0.6 Hello/s steady chatter
    /// between two nodes, from boot, never quiescing). The sent-memo must
    /// suppress this: closing an episode does not erase "I sent this peer a Hello
    /// moments ago".
    #[test]
    fn on_hello_received_is_immune_to_the_done_vs_hello_cross_pool_race() {
        let mut episodes = ReplayEpisodes::new();
        let b = key(2);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        // A has an open episode toward B and just sent it a Hello.
        episodes.open(b, now);
        episodes.record_hello_sent(b, now);

        // B's Done(complete=true) arrives first and closes A's episode -- exactly
        // `VantageCore::on_replay_done`'s own `complete` branch.
        episodes.close(&b);
        assert!(!episodes.is_open(&b));

        // B's reciprocal Hello arrives at A next (the race this regression is
        // about), still well within backoff of A's own send above -- must NOT
        // reciprocate (no new episode), even though the episode is closed.
        let t1 = now + Duration::from_millis(500);
        assert!(
            !episodes.on_hello_received(b, t1, backoff),
            "the sent-memo must suppress reciprocation even though the episode \
             was already closed by the racing Done"
        );
        assert!(!episodes.is_open(&b), "no new episode must have opened");

        // Past backoff, a genuinely new Hello from B must still reciprocate
        // normally -- the legitimate case is unaffected.
        let t2 = now + Duration::from_millis(4_001);
        assert!(
            episodes.on_hello_received(b, t2, backoff),
            "past backoff, reciprocation must still work"
        );
        assert!(episodes.is_open(&b));
    }

    /// (ii): a fresh episode's very first tick, immediately after `open`, must not
    /// re-Hello again (backoff hasn't elapsed since `open`'s own implicit send);
    /// once `backoff` elapses, the next tick fires and refreshes the gate.
    #[test]
    fn tick_re_hellos_only_after_backoff_elapses() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);
        let max_age = Duration::from_millis(60_000);

        episodes.open(peer, now);
        assert!(
            !episodes.tick(peer, now + Duration::from_millis(100), backoff, max_age),
            "backoff has not elapsed since `open`'s own immediate send"
        );
        assert!(episodes.tick(peer, now + Duration::from_millis(4_001), backoff, max_age));
        // The gate just refreshed -- an immediately-following tick must not fire again.
        assert!(!episodes.tick(peer, now + Duration::from_millis(4_100), backoff, max_age));
    }

    /// The expiry valve: an episode past `max_age` auto-closes on the next tick,
    /// regardless of backoff -- "reopened by the next event/nudge", never by `tick`
    /// itself.
    #[test]
    fn tick_closes_the_episode_past_max_age() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);
        let max_age = Duration::from_millis(60_000);

        episodes.open(peer, now);
        assert!(!episodes.tick(peer, now + Duration::from_millis(60_001), backoff, max_age));
        assert!(
            !episodes.is_open(&peer),
            "the episode must auto-close past max_age"
        );
    }

    /// `tick` on a peer with no open episode at all is a harmless no-op (the common
    /// case: `resume_tick` sweeps every OTHER primary every period, most of which
    /// never have an episode open).
    #[test]
    fn tick_on_a_peer_with_no_open_episode_is_a_no_op() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();
        assert!(!episodes.tick(
            peer,
            now,
            Duration::from_millis(4_000),
            Duration::from_millis(60_000)
        ));
    }

    #[test]
    fn close_removes_the_episode() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();
        episodes.open(peer, now);
        episodes.close(&peer);
        assert!(!episodes.is_open(&peer));
    }

    #[test]
    fn in_flight_state_absent_when_never_inserted() {
        let map: HashMap<PublicKey, Instant> = HashMap::new();
        let peer = key(1);
        assert_eq!(
            in_flight_state(&map, &peer, Instant::now(), Duration::from_millis(60_000)),
            InFlightState::Absent
        );
    }

    #[test]
    fn in_flight_state_distinguishes_in_flight_from_expired() {
        let now = Instant::now();
        let mut map = HashMap::new();
        let peer = key(1);
        map.insert(peer, now);
        let ttl = Duration::from_millis(60_000);

        assert_eq!(
            in_flight_state(&map, &peer, now + Duration::from_millis(100), ttl),
            InFlightState::InFlight
        );
        assert_eq!(
            in_flight_state(&map, &peer, now + Duration::from_millis(60_001), ttl),
            InFlightState::Expired,
            "audit-3 A6: TTL = replay_episode_max_ms, distinguishable from a genuinely absent entry"
        );
    }

    #[test]
    fn serve_budget_starts_full_and_depletes_within_a_window() {
        let mut budget = ServeBudget::new();
        let peer = key(1);
        let now = Instant::now();
        let window = Duration::from_millis(4_000);
        let max_bytes = 1_000;

        assert_eq!(budget.remaining(peer, now, window, max_bytes), max_bytes);
        budget.record(peer, 400, now, window);
        assert_eq!(budget.remaining(peer, now, window, max_bytes), 600);
        budget.record(peer, 600, now + Duration::from_millis(10), window);
        assert_eq!(budget.remaining(peer, now, window, max_bytes), 0);
    }

    #[test]
    fn serve_budget_resets_once_the_window_elapses() {
        let mut budget = ServeBudget::new();
        let peer = key(1);
        let now = Instant::now();
        let window = Duration::from_millis(4_000);
        let max_bytes = 1_000;

        budget.record(peer, 1_000, now, window);
        assert_eq!(budget.remaining(peer, now, window, max_bytes), 0);

        let later = now + Duration::from_millis(4_001);
        assert_eq!(
            budget.remaining(peer, later, window, max_bytes),
            max_bytes,
            "a fully-elapsed window must not carry over any of the previous one's usage"
        );
    }

    #[test]
    fn nudge_memo_due_when_never_recorded_and_after_backoff() {
        let mut memo = NudgeMemo::new();
        let peer = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert!(
            memo.due(peer, now, backoff),
            "never recorded -- due immediately"
        );
        memo.record(peer, now);
        assert!(!memo.due(peer, now + Duration::from_millis(100), backoff));
        assert!(memo.due(peer, now + Duration::from_millis(4_001), backoff));
    }

    /// A3's own rationale for sharing one timestamp: recording a SERVE must suppress
    /// a subsequent NUDGE just as a recorded nudge would (they are the same
    /// cooldown), so a server that just served `X` does not also immediately nudge
    /// it.
    #[test]
    fn nudge_memo_serve_and_nudge_share_one_cooldown() {
        let mut memo = NudgeMemo::new();
        let peer = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        memo.record(peer, now); // stands in for "a serve was just enqueued"
        assert!(!memo.due(peer, now + Duration::from_millis(1_000), backoff));
    }
}
