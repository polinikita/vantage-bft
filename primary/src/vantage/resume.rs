use crypto::PublicKey;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::primary::Height;

/// Tracks persistent lane gaps and permits one outstanding request per author.
#[derive(Default)]
pub struct ResumeTrigger {
    pending: HashMap<PublicKey, Height>,
    established: HashSet<PublicKey>,
    in_flight: HashMap<PublicKey, (Height, Instant)>,
    max_concurrent: usize,
}

impl ResumeTrigger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the global episode limit; zero means unlimited.
    pub fn with_max_concurrent(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            ..Self::default()
        }
    }

    /// Returns the next missing height after two consecutive observations of a gap.
    ///
    /// An established gap can continue when its requested span arrives or its backoff expires.
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
            self.pending.remove(&author);
            self.established.remove(&author);
            self.in_flight.remove(&author);
            return None;
        }
        if !self.established.contains(&author) {
            if self.pending.get(&author) == Some(&from) {
                if self.max_concurrent != 0 && self.established.len() >= self.max_concurrent {
                    self.pending.insert(author, from);
                    return None;
                }
                self.established.insert(author);
            } else {
                self.pending.insert(author, from);
                return None;
            }
        }
        if let Some(&(expected_end, sent_at)) = self.in_flight.get(&author) {
            if from <= expected_end && now.duration_since(sent_at) < backoff {
                return None;
            }
        }
        let end = from.saturating_add(resume_batch.max(1)).saturating_sub(1);
        self.in_flight.insert(author, (end, now));
        Some(from)
    }
}

/// Deduplicates served spans by requester and starting height.
#[derive(Default)]
pub struct ResumeServe {
    last_served: HashMap<PublicKey, (Height, Instant)>,
}

impl ResumeServe {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an accepted serve and rejects the same span until `backoff` expires.
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

/// Tracks replay requests and Hello retry state for each peer.
#[derive(Default)]
pub struct ReplayEpisodes {
    open: HashMap<PublicKey, Episode>,
    last_hello_sent: HashMap<PublicKey, Instant>,
}

struct Episode {
    opened_at: Instant,
    last_hello_at: Instant,
}

impl ReplayEpisodes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_open(&self, peer: &PublicKey) -> bool {
        self.open.contains_key(peer)
    }

    pub fn open(&mut self, peer: PublicKey, now: Instant) {
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

    /// Opens a reciprocal episode unless one is open or this node sent a recent Hello.
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

    /// Records a Hello independently of episode lifetime so closing cannot trigger a reply loop.
    pub fn record_hello_sent(&mut self, peer: PublicKey, now: Instant) {
        self.last_hello_sent.insert(peer, now);
    }

    /// Returns whether an open episode should send another Hello and expires old episodes.
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

    pub fn close(&mut self, peer: &PublicKey) {
        if self.open.remove(peer).is_some() {
            log::debug!("vantage resume: episode closed: peer={peer}");
        }
    }
}

/// Identifies an admitted replay stream and its generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InFlightEntry {
    pub started: Instant,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InFlightState {
    Absent,
    InFlight,
    Expired(u64),
}

/// Treats an entry as expired only after the replay episode TTL.
pub fn in_flight_state(
    map: &HashMap<PublicKey, InFlightEntry>,
    peer: &PublicKey,
    now: Instant,
    ttl: Duration,
) -> InFlightState {
    match map.get(peer) {
        None => InFlightState::Absent,
        Some(entry) if now.duration_since(entry.started) < ttl => InFlightState::InFlight,
        Some(entry) => InFlightState::Expired(entry.generation),
    }
}

/// Enforces a per-peer byte limit over a fixed time window.
#[derive(Default)]
pub struct ServeBudget {
    windows: HashMap<PublicKey, (Instant, usize)>,
}

impl ServeBudget {
    pub fn new() -> Self {
        Self::default()
    }

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

/// Shares one cooldown between replay serves and nudges for each peer.
#[derive(Default)]
pub struct NudgeMemo {
    last: HashMap<PublicKey, Instant>,
}

impl NudgeMemo {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn due(&self, peer: PublicKey, now: Instant, backoff: Duration) -> bool {
        self.last
            .get(&peer)
            .is_none_or(|&at| now.duration_since(at) >= backoff)
    }

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

    #[test]
    fn global_cap_defers_extra_episodes_until_a_slot_frees() {
        let mut trigger = ResumeTrigger::with_max_concurrent(2);
        let (a, b, c) = (key(1), key(2), key(3));
        let t0 = Instant::now();
        let backoff = Duration::from_millis(4_000);
        let tick = Duration::from_millis(1_000);

        for author in [a, b, c] {
            assert_eq!(trigger.check(author, 10, 12, t0, backoff, 1), None);
        }
        assert_eq!(trigger.check(a, 10, 12, t0 + tick, backoff, 1), Some(11));
        assert_eq!(trigger.check(b, 10, 12, t0 + tick, backoff, 1), Some(11));
        assert_eq!(
            trigger.check(c, 10, 12, t0 + tick, backoff, 1),
            None,
            "third episode must be deferred by the global cap, not established"
        );

        assert_eq!(trigger.check(a, 12, 12, t0 + 2 * tick, backoff, 1), None);
        assert_eq!(
            trigger.check(c, 10, 12, t0 + 2 * tick, backoff, 1),
            Some(11),
            "a freed slot must promote the deferred episode immediately"
        );
    }

    #[test]
    fn zero_cap_is_unlimited() {
        let mut trigger = ResumeTrigger::with_max_concurrent(0);
        let t0 = Instant::now();
        let backoff = Duration::from_millis(4_000);
        let tick = Duration::from_millis(1_000);
        let authors: Vec<_> = (1..=20).map(key).collect();
        for author in &authors {
            assert_eq!(trigger.check(*author, 10, 12, t0, backoff, 1), None);
        }
        for author in &authors {
            assert_eq!(
                trigger.check(*author, 10, 12, t0 + tick, backoff, 1),
                Some(11),
                "no cap must let every established episode fire"
            );
        }
    }

    #[test]
    fn trigger_fires_after_two_consecutive_gap_ticks() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

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

    #[test]
    fn single_tick_blip_does_not_count_as_persistent() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
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

    #[test]
    fn backoff_suppresses_repeat_requests() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(trigger.check(author, 10, 12, t1, backoff, 1), Some(11));

        for ms in [1_500, 2_000, 3_000, 4_900] {
            let t = now + Duration::from_millis(ms);
            assert_eq!(
                trigger.check(author, 10, 12, t, backoff, 1),
                None,
                "at t+{ms}ms the backoff since t+1000ms has not yet elapsed"
            );
        }

        let t2 = now + Duration::from_millis(5_001);
        assert_eq!(trigger.check(author, 10, 12, t2, backoff, 1), Some(11));
    }

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

        let t2 = now + Duration::from_millis(1_500);
        assert_eq!(
            trigger.check(author, 11, 12, t2, backoff, 1),
            Some(12),
            "established episode: continues immediately once the in-flight span \
             (here, exactly one height) has fully landed"
        );

        let t3 = now + Duration::from_millis(2_500);
        assert_eq!(
            trigger.check(author, 12, 12, t3, backoff, 1),
            None,
            "frontier caught up to avail exactly -- no gap left, nothing to request"
        );
    }

    #[test]
    fn established_episode_waits_for_the_whole_in_flight_batch_before_continuing() {
        let mut trigger = ResumeTrigger::new();
        let author = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);
        let resume_batch = 8;

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

        for (i, height) in (11..=17u64).enumerate() {
            let t = t1 + Duration::from_millis(100 + i as u64);
            assert_eq!(
                trigger.check(author, height, 100, t, backoff, resume_batch),
                None,
                "height {height} landed, but the in-flight span (11..=18) is not \
                 fully covered yet -- must not fire a redundant new request"
            );
        }

        let t_last = t1 + Duration::from_millis(200);
        assert_eq!(
            trigger.check(author, 18, 100, t_last, backoff, resume_batch),
            Some(19),
            "the in-flight span's last height just landed -- fire immediately for \
             the next one, at receipt pace, not tick pace"
        );
    }

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

        let t3 = now + Duration::from_millis(3_000);
        assert_eq!(trigger.check(author, 12, 12, t3, backoff, 1), None);

        let t4 = now + Duration::from_millis(4_000);
        assert_eq!(
            trigger.check(author, 12, 15, t4, backoff, 1),
            None,
            "new episode's first observation must not fire immediately"
        );
        let t5 = now + Duration::from_millis(5_000);
        assert_eq!(trigger.check(author, 12, 15, t5, backoff, 1), Some(13));
    }

    #[test]
    fn independent_authors_do_not_interfere() {
        let mut trigger = ResumeTrigger::new();
        let (a1, a2) = (key(1), key(2));
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(a1, 10, 12, now, backoff, 1), None);
        assert_eq!(trigger.check(a2, 5, 5, now, backoff, 1), None);
        assert_eq!(
            trigger.check(a1, 10, 12, now + Duration::from_millis(1_000), backoff, 1),
            Some(11)
        );
    }

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

    #[test]
    fn tick_retry_racing_receipt_continuation_never_double_serves() {
        let mut trigger = ResumeTrigger::new();
        let mut serve = ResumeServe::new();
        let author = key(1);
        let requester = key(9);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        assert_eq!(trigger.check(author, 10, 12, now, backoff, 1), None);
        let t1 = now + Duration::from_millis(1_000);
        assert_eq!(trigger.check(author, 10, 12, t1, backoff, 1), Some(11));
        assert!(serve.should_serve(requester, 11, t1, backoff));

        let t2 = now + Duration::from_millis(1_500);
        assert_eq!(
            trigger.check(author, 10, 12, t2, backoff, 1),
            None,
            "requester-side backoff already suppresses the tick's own retry"
        );

        assert!(
            !serve.should_serve(requester, 11, t2, backoff),
            "author-side dedup must reject a repeat of (requester, from=11) even if \
             the requester-side backoff that normally prevents it is bypassed"
        );

        let t3 = now + Duration::from_millis(1_600);
        assert_eq!(
            trigger.check(author, 11, 12, t3, backoff, 1),
            Some(12),
            "established episode continues immediately once frontier advances, \
             matching the receipt-continuation call site, not the tick's own cadence"
        );
        assert!(serve.should_serve(requester, 12, t3, backoff));
    }

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

    #[test]
    fn episode_opens_and_reports_open() {
        let mut episodes = ReplayEpisodes::new();
        let peer = key(1);
        let now = Instant::now();

        assert!(!episodes.is_open(&peer));
        episodes.open(peer, now);
        assert!(episodes.is_open(&peer));
    }

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

    #[test]
    fn on_hello_received_is_immune_to_the_done_vs_hello_cross_pool_race() {
        let mut episodes = ReplayEpisodes::new();
        let b = key(2);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        episodes.open(b, now);
        episodes.record_hello_sent(b, now);

        episodes.close(&b);
        assert!(!episodes.is_open(&b));

        let t1 = now + Duration::from_millis(500);
        assert!(
            !episodes.on_hello_received(b, t1, backoff),
            "the sent-memo must suppress reciprocation even though the episode \
             was already closed by the racing Done"
        );
        assert!(!episodes.is_open(&b), "no new episode must have opened");

        let t2 = now + Duration::from_millis(4_001);
        assert!(
            episodes.on_hello_received(b, t2, backoff),
            "past backoff, reciprocation must still work"
        );
        assert!(episodes.is_open(&b));
    }

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
        assert!(!episodes.tick(peer, now + Duration::from_millis(4_100), backoff, max_age));
    }

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
        let map: HashMap<PublicKey, InFlightEntry> = HashMap::new();
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
        map.insert(
            peer,
            InFlightEntry {
                started: now,
                generation: 7,
            },
        );
        let ttl = Duration::from_millis(60_000);

        assert_eq!(
            in_flight_state(&map, &peer, now + Duration::from_millis(100), ttl),
            InFlightState::InFlight
        );
        assert_eq!(
            in_flight_state(&map, &peer, now + Duration::from_millis(60_001), ttl),
            InFlightState::Expired(7),
            "an expired entry remains distinct from an absent entry"
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

    #[test]
    fn nudge_memo_serve_and_nudge_share_one_cooldown() {
        let mut memo = NudgeMemo::new();
        let peer = key(1);
        let now = Instant::now();
        let backoff = Duration::from_millis(4_000);

        memo.record(peer, now);
        assert!(!memo.due(peer, now + Duration::from_millis(1_000), backoff));
    }
}
