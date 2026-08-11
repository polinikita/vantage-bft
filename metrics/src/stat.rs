// Exact histogram support for transaction latency metrics.

use std::{collections::BTreeMap, ops::AddAssign, time::Duration};

use tokio::sync::mpsc;

/// Exact histogram that retains every observation for percentile calculation.
/// The reporter task owns the histogram; producers use `HistogramSender`.
pub struct PreciseHistogram<T> {
    points: BTreeMap<T, usize>,
    sum: T,
    count: usize,
    receiver: mpsc::UnboundedReceiver<(T, usize)>,
}

/// Cloneable producer handle for a `PreciseHistogram`.
#[derive(Clone)]
pub struct HistogramSender<T> {
    sender: mpsc::UnboundedSender<(T, usize)>,
}

pub fn histogram<T: Default>() -> (PreciseHistogram<T>, HistogramSender<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let sender = HistogramSender { sender };
    let histogram = PreciseHistogram {
        points: BTreeMap::new(),
        sum: Default::default(),
        count: 0,
        receiver,
    };
    (histogram, sender)
}

impl<T: Send> HistogramSender<T> {
    pub fn observe(&self, t: T) {
        self.observe_n(t, 1);
    }

    pub fn observe_n(&self, t: T, count: usize) {
        if count > 0 {
            self.sender.send((t, count)).ok();
        }
    }
}

pub trait MulUsize {
    fn mul_usize(self, count: usize) -> Self;
}

impl MulUsize for Duration {
    fn mul_usize(self, count: usize) -> Self {
        let nanos = self.as_nanos().saturating_mul(count as u128);
        Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
    }
}

impl MulUsize for usize {
    fn mul_usize(self, count: usize) -> Self {
        self.saturating_mul(count)
    }
}

impl<T: Ord + AddAssign + DivUsize + MulUsize + Copy + Default> PreciseHistogram<T> {
    pub fn observe(&mut self, point: T) {
        self.observe_n(point, 1);
    }

    pub fn observe_n(&mut self, point: T, count: usize) {
        if count == 0 {
            return;
        }
        *self.points.entry(point).or_default() += count;
        self.sum += point.mul_usize(count);
        self.count += count;
    }

    pub fn avg(&self) -> Option<T> {
        if self.count == 0 {
            return None;
        }
        Some(self.sum.div_usize(self.count))
    }

    /// Running sum.
    pub fn total_sum(&self) -> T {
        self.sum
    }

    /// Running count.
    pub fn total_count(&self) -> usize {
        self.count
    }

    pub fn pcts<const N: usize>(&mut self, pct: [usize; N]) -> Option<[T; N]> {
        if self.count == 0 {
            return None;
        }
        let mut result = [T::default(); N];
        for (i, pct) in pct.iter().enumerate() {
            let target = self.pct1000_index(*pct);
            let mut seen = 0usize;
            result[i] = *self
                .points
                .iter()
                .find_map(|(point, count)| {
                    seen += *count;
                    (seen > target).then_some(point)
                })
                .expect("a non-empty histogram contains its percentile");
        }
        Some(result)
    }

    pub fn pct(&mut self, pct1000: usize) -> Option<T> {
        self.pcts([pct1000]).map(|[p]| p)
    }

    /// Maximum observation.
    pub fn max(&mut self) -> Option<T> {
        self.points.last_key_value().map(|(point, _)| *point)
    }

    pub fn receive_all(&mut self) {
        while let Ok((point, count)) = self.receiver.try_recv() {
            self.observe_n(point, count);
        }
    }

    pub fn clear_receive_all(&mut self) {
        self.clear();
        self.receive_all();
    }

    pub fn clear(&mut self) {
        self.points.clear();
        self.sum = T::default();
        self.count = 0;
    }

    fn pct1000_index(&self, pct1000: usize) -> usize {
        debug_assert!(pct1000 < 1000);
        self.count * pct1000 / 1000
    }
}

pub trait DivUsize {
    fn div_usize(&self, u: usize) -> Self;
}

impl DivUsize for Duration {
    fn div_usize(&self, u: usize) -> Self {
        *self / u as u32
    }
}

impl DivUsize for usize {
    fn div_usize(&self, u: usize) -> Self {
        self / u
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_observations_preserve_exact_statistics() {
        let (mut histogram, sender) = histogram();
        sender.observe_n(10usize, 4);
        sender.observe(20);
        histogram.receive_all();

        assert_eq!(histogram.total_count(), 5);
        assert_eq!(histogram.total_sum(), 60);
        assert_eq!(histogram.avg(), Some(12));
        assert_eq!(histogram.pcts([500, 900]), Some([10, 20]));
        assert_eq!(histogram.max(), Some(20));

        histogram.clear();
        assert_eq!(histogram.total_count(), 0);
        assert_eq!(histogram.total_sum(), 0);
        assert_eq!(histogram.pct(500), None);
    }
}
