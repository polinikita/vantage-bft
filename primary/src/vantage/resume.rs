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
    pub fn should_serve(&mut self, requester: PublicKey, from: Height, now: Instant, backoff: Duration) -> bool {
        if let Some((last_from, at)) = self.last_served.get(&requester) {
            if *last_from == from && now.duration_since(*at) < backoff {
                return false;
            }
        }
        self.last_served.insert(requester, (from, now));
        true
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
            trigger.check(author, 10, 12, now + Duration::from_millis(1_000), backoff, 1),
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
            trigger.check(author, 10, 10, now + Duration::from_millis(1_000), backoff, 1),
            None
        );
        // Gap reappears -- this must count as observation #1 again, not #3.
        assert_eq!(
            trigger.check(author, 10, 12, now + Duration::from_millis(2_000), backoff, 1),
            None,
            "the memo was cleared by the gap-free tick; this is a fresh first observation"
        );
        assert_eq!(
            trigger.check(author, 10, 12, now + Duration::from_millis(3_000), backoff, 1),
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
        assert_eq!(trigger.check(author, 10, 100, now, backoff, resume_batch), None);
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
}
