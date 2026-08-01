// reconnect-replay plan §5 (server-authoritative floor, v3): the per-party outbox of
// every one-shot broadcast this node has sent VOLATILE (`VantageCore::
// broadcast_recorded`) since it last got pruned -- the raw material a durable replay
// stream (`ReplaySend`) is served FROM once a peer's `pending_low` (or its own
// Hello floor hint) names a gap. Keyed by `Pacemaker::own_watermark()` AT SEND TIME
// (monotone non-decreasing across calls -- audit V1), so "everything sent from view V
// onward" is a plain `BTreeMap::range` -- no separate index, no per-message sequence
// number.
//
// GC (§5): two independent bounds keep this from growing without limit. `prune_below`
// (age/view-based, `Parameters::replay_history_views` behind `own_watermark`, hooked
// into `VantageCore::collect_internal_garbage`) is a CEILING; the byte cap
// (`Parameters::outbox_max_bytes`, enforced inline by `record`, evicting whole oldest
// views -- NEVER the single newest key, even if it alone exceeds the cap) is what
// actually binds in practice at typical Δ (15-25MB plausible steady state per the
// design doc). Either eviction path raises `floor()` -- the lowest key any peer could
// still be served from; a request for anything below it is `clamped` (§6/§14 A2's own
// vocabulary: "if outbox_floor > pending_low[X], the serve is clamped").
//
// Residual coupling (§8, unchanged from v2): nothing downstream of a replay (the ack
// aggregator's per-block dedup, `control::ControlLog`'s report/echo/ready census maps,
// etc.) is ever pruned on account of a message being REPLAYED rather than freshly
// sent -- those maps are keyed by (author, height)/(view, digest)/etc., not by "have I
// seen this exact wire frame before", so a resurrected duplicate is idempotent through
// them exactly like an ordinary retransmit already is. This is safe ONLY because
// resurrection volume is bounded by genuine send volume: this module's own eviction
// means a replay can never resurrect more than `outbox_max_bytes`/`replay_history_
// views` worth of this party's OWN past broadcasts, ever -- it cannot replay something
// that was never sent, and it cannot replay an unbounded amount of history. The
// coupling is intentional, not accidental: re-deriving a separate pruning schedule for
// every one of those maps, keyed to THIS module's own floor, would be strictly more
// code for no additional safety.

use crate::primary::View;
use bytes::Bytes;
use std::collections::BTreeMap;

/// One party's own outbox of one-shot broadcasts recorded for possible replay -- see
/// this module's own doc comment for the full design.
pub struct Outbox {
    views: BTreeMap<View, Vec<Bytes>>,
    total_bytes: usize,
    max_bytes: usize,
    /// The lowest filing key any peer could still be served from -- see this
    /// module's own doc comment ("GC"). Distinct from `views.keys().next()`, which
    /// is meaningless while `views` happens to be momentarily empty (a fresh node,
    /// or one that just evicted everything) -- `floor` stays valid and monotone
    /// across both, and is what callers must clamp a serve-from against.
    floor: View,
}

impl Outbox {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            views: BTreeMap::new(),
            total_bytes: 0,
            max_bytes,
            floor: 0,
        }
    }

    /// File `bytes` under `key` (`Pacemaker::own_watermark()` at send time) --
    /// `VantageCore::broadcast_recorded`'s entire side of the outbox contract.
    ///
    /// `key` must be monotone non-decreasing across calls (audit V1: `own_watermark`
    /// itself never decreases) -- debug-asserted, not enforced in a release build
    /// (the outbox's own `BTreeMap` would simply gain an out-of-order entry rather
    /// than corrupt anything, but nothing in this design ever legitimately produces
    /// one).
    pub fn record(&mut self, key: View, bytes: Bytes) {
        debug_assert!(
            self.views
                .keys()
                .next_back()
                .is_none_or(|&last| key >= last),
            "Outbox::record: own_watermark must be monotone non-decreasing (got {key} after {:?})",
            self.views.keys().next_back()
        );
        self.total_bytes += bytes.len();
        self.views.entry(key).or_default().push(bytes);
        self.evict_over_cap();
    }

    /// Byte-cap eviction (§5): evicts WHOLE oldest views while over `max_bytes`,
    /// never the single newest key (a key so large it alone exceeds the cap is kept
    /// whole -- the resulting overshoot is bounded and documented, mirroring §6's
    /// "a single key larger than the whole budget is served whole" serve-side rule).
    fn evict_over_cap(&mut self) {
        while self.total_bytes > self.max_bytes && self.views.len() > 1 {
            let oldest_key = *self.views.keys().next().expect("len() > 1 checked above");
            if let Some(msgs) = self.views.remove(&oldest_key) {
                self.total_bytes -= msgs.iter().map(Bytes::len).sum::<usize>();
            }
            self.floor = self.floor.max(oldest_key + 1);
        }
    }

    /// Every recorded entry with a filing key `>= from`, in ascending key order --
    /// the raw material `Inbound::ResumeHello`'s handling walks to build a replay
    /// stream and compute `end_key`/`complete` under a byte budget (§6). Does not
    /// itself know about `floor()` or any budget -- clamping/truncation is entirely
    /// the caller's job.
    pub fn slice_from(&self, from: View) -> impl Iterator<Item = (View, &[Bytes])> {
        self.views.range(from..).map(|(&k, v)| (k, v.as_slice()))
    }

    /// Age/view-based GC (§5): discards every entry keyed `< floor`, via
    /// `split_off` (this codebase's standing BTreeMap-prune idiom -- see e.g.
    /// `control::ControlLog::gc_below`). A no-op if `floor` would not actually
    /// advance the current floor (monotone guard, mirrors `VantageCore::
    /// last_gc_floor`'s identical role for the rest of this node's internal GC).
    pub fn prune_below(&mut self, floor: View) {
        if floor <= self.floor {
            return;
        }
        let kept = self.views.split_off(&floor);
        let discarded = std::mem::replace(&mut self.views, kept);
        for msgs in discarded.values() {
            self.total_bytes -= msgs.iter().map(Bytes::len).sum::<usize>();
        }
        self.floor = floor;
    }

    /// The lowest filing key any peer could still be served from -- see this
    /// module's own doc comment.
    pub fn floor(&self) -> View {
        self.floor
    }

    /// reconnect-replay plan §6: builds a replay payload starting at `from`,
    /// truncated to WHOLE keys within `budget` bytes -- never splits a single key's
    /// own `Vec<Bytes>` across the boundary. A key so large it alone exceeds
    /// `budget` is still served whole (the documented, bounded overshoot -- the
    /// same rule `evict_over_cap` applies on the GC side, restated here for the
    /// serve side per audit B3).
    ///
    /// Returns `(msgs, end_key, complete)`: `msgs` is every constituent message
    /// from every included key, in order; `end_key` is the last FULLY included
    /// key's successor (`from` itself if nothing was included at all -- e.g. an
    /// already-empty slice); `complete` is `true` iff EVERY entry from `from`
    /// onward was included (nothing was truncated for budget reasons) -- `false`
    /// iff the budget cut the span short of the outbox's current tip.
    pub fn take_budgeted_slice(&self, from: View, budget: usize) -> (Vec<Bytes>, View, bool) {
        let mut msgs = Vec::new();
        let mut end_key = from;
        let mut remaining = budget;
        for (key, key_msgs) in self.slice_from(from) {
            let key_bytes: usize = key_msgs.iter().map(Bytes::len).sum();
            if !msgs.is_empty() && key_bytes > remaining {
                // At least one key already went out, and this one would exceed
                // the remaining budget -- truncate BEFORE it (whole-key rule).
                return (msgs, end_key, false);
            }
            msgs.extend(key_msgs.iter().cloned());
            remaining = remaining.saturating_sub(key_bytes);
            end_key = key + 1;
        }
        (msgs, end_key, true)
    }

    #[cfg(test)]
    fn total_bytes_for_test(&self) -> usize {
        self.total_bytes
    }

    #[cfg(test)]
    fn view_count_for_test(&self) -> usize {
        self.views.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn record_and_slice_from_returns_ascending_order_grouped_by_key() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, b("a"));
        outbox.record(5, b("b"));
        outbox.record(7, b("c"));
        outbox.record(9, b("d"));

        let got: Vec<(View, Vec<Bytes>)> =
            outbox.slice_from(0).map(|(k, v)| (k, v.to_vec())).collect();
        assert_eq!(
            got,
            vec![
                (5, vec![b("a"), b("b")]),
                (7, vec![b("c")]),
                (9, vec![b("d")]),
            ]
        );
    }

    #[test]
    fn slice_from_skips_everything_below_from() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, b("a"));
        outbox.record(7, b("c"));
        outbox.record(9, b("d"));

        let got: Vec<View> = outbox.slice_from(8).map(|(k, _)| k).collect();
        assert_eq!(got, vec![9]);
    }

    #[test]
    fn floor_starts_at_zero_on_a_fresh_outbox() {
        let outbox = Outbox::new(1 << 20);
        assert_eq!(outbox.floor(), 0);
    }

    #[test]
    fn prune_below_discards_and_raises_floor() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, b("a"));
        outbox.record(7, b("c"));
        outbox.record(9, b("d"));

        outbox.prune_below(8);

        assert_eq!(outbox.floor(), 8);
        let got: Vec<View> = outbox.slice_from(0).map(|(k, _)| k).collect();
        assert_eq!(got, vec![9]);
    }

    #[test]
    fn prune_below_is_a_monotone_no_op_below_the_current_floor() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, b("a"));
        outbox.record(9, b("d"));
        outbox.prune_below(8);
        assert_eq!(outbox.floor(), 8);

        outbox.prune_below(3); // must never lower the floor back down
        assert_eq!(outbox.floor(), 8);
        // ... and the entry it would have "un-pruned" stays gone.
        assert_eq!(
            outbox.slice_from(0).map(|(k, _)| k).collect::<Vec<_>>(),
            vec![9]
        );
    }

    #[test]
    fn byte_cap_evicts_whole_oldest_views_never_splitting_one() {
        // Each message is 10 bytes; the cap fits exactly 2 views' worth.
        let mut outbox = Outbox::new(20);
        outbox.record(1, Bytes::from(vec![0u8; 10]));
        outbox.record(2, Bytes::from(vec![0u8; 10]));
        assert_eq!(outbox.total_bytes_for_test(), 20);

        // A 3rd view pushes total to 30 > cap(20) -- must evict view 1 WHOLE, not
        // trim it down to fit.
        outbox.record(3, Bytes::from(vec![0u8; 10]));

        assert_eq!(outbox.view_count_for_test(), 2);
        assert_eq!(outbox.total_bytes_for_test(), 20);
        let got: Vec<View> = outbox.slice_from(0).map(|(k, _)| k).collect();
        assert_eq!(got, vec![2, 3]);
        assert_eq!(outbox.floor(), 2);
    }

    #[test]
    fn byte_cap_never_evicts_the_single_newest_key() {
        // One key alone already exceeds the whole cap -- must be kept whole (there
        // would be nothing left to serve at all otherwise).
        let mut outbox = Outbox::new(5);
        outbox.record(1, Bytes::from(vec![0u8; 50]));

        assert_eq!(outbox.view_count_for_test(), 1);
        let got: Vec<View> = outbox.slice_from(0).map(|(k, _)| k).collect();
        assert_eq!(got, vec![1]);
        assert_eq!(
            outbox.floor(),
            0,
            "the sole surviving key was never evicted"
        );
    }

    #[test]
    #[should_panic(expected = "monotone")]
    fn record_asserts_monotone_keys() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, b("a"));
        outbox.record(4, b("b")); // must panic in a debug build
    }

    #[test]
    fn take_budgeted_slice_on_an_empty_range_is_complete_and_empty() {
        let outbox = Outbox::new(1 << 20);
        let (msgs, end_key, complete) = outbox.take_budgeted_slice(7, 1_000);
        assert!(msgs.is_empty());
        assert_eq!(end_key, 7, "nothing served -- end_key stays at `from`");
        assert!(
            complete,
            "an empty slice is always complete -- Done(complete=true) only"
        );
    }

    #[test]
    fn take_budgeted_slice_within_budget_serves_everything_and_reports_complete() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, Bytes::from(vec![0u8; 10]));
        outbox.record(7, Bytes::from(vec![0u8; 10]));

        let (msgs, end_key, complete) = outbox.take_budgeted_slice(0, 1_000);
        assert_eq!(msgs.len(), 2);
        assert_eq!(end_key, 8, "last fully served key (7) + 1");
        assert!(complete);
    }

    #[test]
    fn take_budgeted_slice_truncates_at_a_key_boundary_under_budget() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, Bytes::from(vec![0u8; 10]));
        outbox.record(7, Bytes::from(vec![0u8; 10]));
        outbox.record(9, Bytes::from(vec![0u8; 10]));

        // Budget fits exactly key 5's 10 bytes, not key 7's on top of it.
        let (msgs, end_key, complete) = outbox.take_budgeted_slice(0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(end_key, 6, "last FULLY served key (5) + 1 -- never mid-key");
        assert!(!complete, "key 7 and 9 were truncated for budget reasons");
    }

    #[test]
    fn take_budgeted_slice_serves_an_over_budget_single_key_whole() {
        let mut outbox = Outbox::new(1 << 20);
        outbox.record(5, Bytes::from(vec![0u8; 100]));
        outbox.record(7, Bytes::from(vec![0u8; 10]));

        // Budget (10) is smaller than key 5 alone (100 bytes) -- must still be
        // served whole (documented overshoot, audit B3), and since it's the ONLY
        // key that fits under "first key always goes out", key 7 is truncated.
        let (msgs, end_key, complete) = outbox.take_budgeted_slice(0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].len(), 100);
        assert_eq!(end_key, 6);
        assert!(!complete);
    }
}
