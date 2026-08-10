use crate::primary::View;
use bytes::Bytes;
use std::collections::BTreeMap;

/// Stores this node's broadcasts for bounded replay by view.
pub struct Outbox {
    views: BTreeMap<View, Vec<Bytes>>,
    total_bytes: usize,
    max_bytes: usize,
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

    /// Records bytes under a monotonically non-decreasing view key.
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

    /// Evicts complete oldest views and always retains the newest view.
    fn evict_over_cap(&mut self) {
        while self.total_bytes > self.max_bytes && self.views.len() > 1 {
            let oldest_key = *self.views.keys().next().expect("len() > 1 checked above");
            if let Some(msgs) = self.views.remove(&oldest_key) {
                self.total_bytes -= msgs.iter().map(Bytes::len).sum::<usize>();
            }
            self.floor = self.floor.max(oldest_key + 1);
        }
    }

    pub fn slice_from(&self, from: View) -> impl Iterator<Item = (View, &[Bytes])> {
        self.views.range(from..).map(|(&k, v)| (k, v.as_slice()))
    }

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

    /// Returns the lowest view that may still be replayed.
    pub fn floor(&self) -> View {
        self.floor
    }

    /// Returns complete view groups within `budget` bytes.
    ///
    /// The first group is returned whole even when it exceeds the budget. The returned
    /// view is one past the last complete group, and the boolean reports whether all groups
    /// were returned.
    pub fn take_budgeted_slice(&self, from: View, budget: usize) -> (Vec<Bytes>, View, bool) {
        let mut msgs = Vec::new();
        let mut end_key = from;
        let mut remaining = budget;
        for (key, key_msgs) in self.slice_from(from) {
            let key_bytes: usize = key_msgs.iter().map(Bytes::len).sum();
            if !msgs.is_empty() && key_bytes > remaining {
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

        outbox.prune_below(3);
        assert_eq!(outbox.floor(), 8);
        assert_eq!(
            outbox.slice_from(0).map(|(k, _)| k).collect::<Vec<_>>(),
            vec![9]
        );
    }

    #[test]
    fn byte_cap_evicts_whole_oldest_views_never_splitting_one() {
        let mut outbox = Outbox::new(20);
        outbox.record(1, Bytes::from(vec![0u8; 10]));
        outbox.record(2, Bytes::from(vec![0u8; 10]));
        assert_eq!(outbox.total_bytes_for_test(), 20);

        outbox.record(3, Bytes::from(vec![0u8; 10]));

        assert_eq!(outbox.view_count_for_test(), 2);
        assert_eq!(outbox.total_bytes_for_test(), 20);
        let got: Vec<View> = outbox.slice_from(0).map(|(k, _)| k).collect();
        assert_eq!(got, vec![2, 3]);
        assert_eq!(outbox.floor(), 2);
    }

    #[test]
    fn byte_cap_never_evicts_the_single_newest_key() {
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
        outbox.record(4, b("b"));
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

        let (msgs, end_key, complete) = outbox.take_budgeted_slice(0, 10);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].len(), 100);
        assert_eq!(end_key, 6);
        assert!(!complete);
    }
}
