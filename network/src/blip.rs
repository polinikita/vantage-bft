// Copyright(C) Facebook, Inc. and its affiliates.
// Transient network-level "blip" fault injector (`node local-benchmark --blip-at/
// --blip-for/--blip-node`): reproduces the Autobahn paper's (Giridharan et al.,
// SOSP'24, Figs. 1/7/8) blip experiment -- "we ... trigger a three second blip by
// simulating a single leader failure". Shape (b): reuses the EXISTING per-connection
// delay machinery (`Connection::keep_alive_delayed`/`run_delayed`, already threaded
// via `with_latency`) rather than adding a hold/gate at a different layer -- a
// message's scheduled release instant is bumped forward to the window's end whenever
// it would otherwise land inside `[start, end)`; nothing is ever dropped, only
// delayed ("pause semantics, not loss").
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::Instant as StdInstant;
use tokio::time::Instant;

/// Resolved once per `ReliableSender`/`SimpleSender` connection at spawn time (mirrors
/// `extra_latency`'s own resolve-once convention -- see `spawn_connection` in both
/// sender modules), from `config::blip_targets` (this gate's `targets`) and
/// `Parameters::blip_window` (this gate's `window` -- the SAME `Arc` clone every
/// node's own gate holds, so `node local-benchmark::run`'s single `.set(..)` call, made
/// once measurement start is known, arms every already-spawned connection's gate
/// simultaneously, even ones already mid-`keep_alive_delayed`/`run_delayed`).
pub struct BlipGate {
    /// The destination addresses this node's connections should hold traffic to
    /// during the window -- see `config::blip_targets`'s doc comment for exactly
    /// which addresses land here depending on whether THIS node is the blipped one.
    targets: HashSet<SocketAddr>,
    /// `None` until `node local-benchmark::run` knows the benchmark's measurement
    /// start. Every `clamp` call before then is a no-op, which is always correct: the
    /// window's start-to-be is necessarily still in the future relative to any
    /// message scheduled this early (see `clamp`'s own doc comment).
    window: Arc<OnceLock<(StdInstant, StdInstant)>>,
}

impl BlipGate {
    pub fn new(targets: HashSet<SocketAddr>, window: Arc<OnceLock<(StdInstant, StdInstant)>>) -> Self {
        Self { targets, window }
    }

    /// Whether `address` is one of this gate's held destinations -- checked ONCE, at
    /// `spawn_connection` time (mirrors `latency.get(&address)`), never per-message: a
    /// `Connection` whose own destination doesn't match keeps `blip: None` for its
    /// whole life, so `clamp` (below) is never even called on that connection.
    pub(crate) fn targets(&self, address: &SocketAddr) -> bool {
        self.targets.contains(address)
    }

    /// The actual release instant for a message whose "natural" release (`now()` at
    /// the point it was scheduled, plus any fixed per-connection `extra_latency`) is
    /// `natural_release`. A no-op (`natural_release` verbatim) unless the window has
    /// armed AND `natural_release` falls within `[start, end)` -- in which case the
    /// message is held until exactly `end` (never dropped, only delayed).
    ///
    /// ORDERING (per-connection FIFO is preserved): every one of this crate's
    /// `Connection::keep_alive_delayed`/`run_delayed` loops is a SINGLE tokio task
    /// executing its `tokio::select!` iterations strictly sequentially -- so across
    /// however many arms end up computing a `natural_release` over that connection's
    /// life, the sequence of `now()` values they observe, IN PUSH ORDER, is
    /// monotonically non-decreasing (the same invariant `keep_alive_delayed`'s own doc
    /// comment already relies on for the pre-existing fixed-latency case: "messages
    /// are scheduled in arrival order, their release times are ALSO strictly
    /// increasing in that same order"). Since `extra_latency` is one fixed constant
    /// for the connection's whole life, `natural_release = now() + extra_latency` is
    /// therefore ALSO non-decreasing in push order. `clamp` preserves that: for two
    /// natural releases `a <= b` (in push order, i.e. `a` was pushed no later than
    /// `b`):
    ///   - neither in `[start, end)`: `clamp(a) = a <= b = clamp(b)`.
    ///   - only `a` in `[start, end)`: `clamp(a) = end`. Since `b >= a >= start` and
    ///     `b` is NOT in `[start, end)`, `b` must be `>= end` (it can't be `< start`,
    ///     as that would need `b < start <= a`, contradicting `b >= a`) -- so
    ///     `clamp(a) = end <= b = clamp(b)`.
    ///   - only `b` in `[start, end)`: impossible given `a <= b` -- if `b < end` then
    ///     `a <= b < end`, and `a` not being in `[start, end)` while `a <= b < end`
    ///     forces `a < start`, so `clamp(a) = a < start <= end = clamp(b)`.
    ///   - both in `[start, end)`: `clamp(a) = end = clamp(b)`.
    ///
    /// Every case yields `clamp(a) <= clamp(b)`: release order in push order is
    /// preserved, so the existing delay queue (which always dequeues its FRONT --
    /// i.e. push order -- gated on only that front entry's own release instant) keeps
    /// delivering in strict per-connection FIFO exactly as it already did before this
    /// gate existed.
    pub(crate) fn clamp(&self, natural_release: Instant) -> Instant {
        let Some((start, end)) = self.window.get().copied() else {
            return natural_release;
        };
        let start = Instant::from_std(start);
        let end = Instant::from_std(end);
        if natural_release >= start && natural_release < end {
            end
        } else {
            natural_release
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn gate() -> (BlipGate, Arc<OnceLock<(StdInstant, StdInstant)>>) {
        let window = Arc::new(OnceLock::new());
        (BlipGate::new(HashSet::new(), window.clone()), window)
    }

    #[test]
    fn unarmed_window_is_a_no_op() {
        let (gate, _window) = gate();
        let now = Instant::now();
        assert_eq!(gate.clamp(now), now);
    }

    #[test]
    fn before_window_is_untouched() {
        let (gate, window) = gate();
        let base = StdInstant::now();
        window
            .set((base + Duration::from_secs(10), base + Duration::from_secs(13)))
            .unwrap();
        let release = Instant::from_std(base) + Duration::from_secs(5);
        assert_eq!(gate.clamp(release), release);
    }

    #[test]
    fn inside_window_clamps_to_end() {
        let (gate, window) = gate();
        let base = StdInstant::now();
        let end = base + Duration::from_secs(13);
        window.set((base + Duration::from_secs(10), end)).unwrap();
        let release = Instant::from_std(base) + Duration::from_secs(11);
        assert_eq!(gate.clamp(release), Instant::from_std(end));
    }

    /// Half-open interval: a natural release exactly AT `end` is already past the
    /// window (matches the window itself being `[start, end)`, i.e. traffic resumes
    /// immediately at `end`, not one instant after it).
    #[test]
    fn at_end_is_untouched() {
        let (gate, window) = gate();
        let base = StdInstant::now();
        window
            .set((base + Duration::from_secs(10), base + Duration::from_secs(13)))
            .unwrap();
        let release = Instant::from_std(base) + Duration::from_secs(13);
        assert_eq!(gate.clamp(release), release);
    }

    #[test]
    fn after_window_is_untouched() {
        let (gate, window) = gate();
        let base = StdInstant::now();
        window
            .set((base + Duration::from_secs(10), base + Duration::from_secs(13)))
            .unwrap();
        let release = Instant::from_std(base) + Duration::from_secs(20);
        assert_eq!(gate.clamp(release), release);
    }

    #[test]
    fn targets_matches_only_configured_addresses() {
        let a: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let window = Arc::new(OnceLock::new());
        let gate = BlipGate::new([a].into_iter().collect(), window);
        assert!(gate.targets(&a));
        assert!(!gate.targets(&b));
    }

    /// The ordering-preservation argument in `clamp`'s own doc comment, exercised
    /// directly: a sequence of strictly increasing natural-release instants around a
    /// window (before / straddling / inside / at-end / after) must clamp to a
    /// non-decreasing sequence -- i.e. the release order `clamp` produces never
    /// contradicts arrival (push) order.
    #[test]
    fn clamp_preserves_monotonicity_across_the_window_boundary() {
        let (gate, window) = gate();
        let base = StdInstant::now();
        window
            .set((base + Duration::from_secs(10), base + Duration::from_secs(13)))
            .unwrap();
        let offsets_secs = [0u64, 5, 9, 10, 11, 12, 13, 14, 20];
        let releases: Vec<Instant> = offsets_secs
            .iter()
            .map(|&s| Instant::from_std(base) + Duration::from_secs(s))
            .collect();
        let clamped: Vec<Instant> = releases.iter().map(|&r| gate.clamp(r)).collect();
        for i in 1..clamped.len() {
            assert!(
                clamped[i] >= clamped[i - 1],
                "clamp must not reorder: clamp(+{}s)={:?} < clamp(+{}s)={:?}",
                offsets_secs[i - 1],
                clamped[i - 1],
                offsets_secs[i],
                clamped[i]
            );
        }
    }
}
