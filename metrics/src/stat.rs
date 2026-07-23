// Ported from starfish (`~/code/starfish/crates/starfish-core/src/stat.rs`, Apache-2.0)
// for Starfish-parity latency measurement (PHASE2-SPEC.md #5). Channel-based observation
// (`HistogramSender::observe`, a plain unbounded-channel push) keeps the hot path
// lock-free; `PreciseHistogram` itself is drained and queried only by the periodic
// reporter task.

use std::{ops::AddAssign, time::Duration};

use tokio::sync::mpsc;

/// An exact (not bucketed) histogram: keeps every observed point so percentiles are
/// computed precisely, not approximated. Intended for periodic draining by a single
/// reporter task, not for direct concurrent access -- producers go through
/// `HistogramSender`.
pub struct PreciseHistogram<T> {
    points: Vec<T>,
    sum: T,
    count: usize,
    receiver: mpsc::UnboundedReceiver<T>,
}

/// Cheap, cloneable handle producers use to feed a `PreciseHistogram` without ever
/// touching a lock: `observe` is an unbounded-channel send.
#[derive(Clone)]
pub struct HistogramSender<T> {
    sender: mpsc::UnboundedSender<T>,
}

pub fn histogram<T: Default>() -> (PreciseHistogram<T>, HistogramSender<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let sender = HistogramSender { sender };
    let histogram = PreciseHistogram {
        points: Default::default(),
        sum: Default::default(),
        count: 0,
        receiver,
    };
    (histogram, sender)
}

impl<T: Send> HistogramSender<T> {
    pub fn observe(&self, t: T) {
        self.sender.send(t).ok();
    }
}

impl<T: Ord + AddAssign + DivUsize + Copy + Default> PreciseHistogram<T> {
    pub fn observe(&mut self, point: T) {
        self.points.push(point);
        self.sum += point;
        self.count += 1;
    }

    pub fn avg(&self) -> Option<T> {
        if self.points.is_empty() {
            return None;
        }
        Some(self.sum.div_usize(self.points.len()))
    }

    /// Running sum, not reset on `clear`/`clear_receive_all`.
    pub fn total_sum(&self) -> T {
        self.sum
    }

    /// Running count, not reset on `clear`/`clear_receive_all`.
    pub fn total_count(&self) -> usize {
        self.count
    }

    pub fn pcts<const N: usize>(&mut self, pct: [usize; N]) -> Option<[T; N]> {
        if self.points.is_empty() {
            return None;
        }
        // Sorting is O(n log n) worst case but close to O(n) on the already-mostly-sorted
        // data typical of repeated calls, since we sort in place instead of cloning.
        self.points.sort();
        let mut result = [T::default(); N];
        for (i, pct) in pct.iter().enumerate() {
            result[i] = *self.points.get(self.pct1000_index(*pct)).unwrap();
        }
        Some(result)
    }

    pub fn pct(&mut self, pct1000: usize) -> Option<T> {
        self.pcts([pct1000]).map(|[p]| p)
    }

    /// The exact maximum observation (not an approximation via a high percentile
    /// index, which rounds down and under-reports for large point counts).
    pub fn max(&mut self) -> Option<T> {
        if self.points.is_empty() {
            return None;
        }
        self.points.sort();
        self.points.last().copied()
    }

    pub fn receive_all(&mut self) {
        while let Ok(d) = self.receiver.try_recv() {
            self.observe(d);
        }
    }

    pub fn clear_receive_all(&mut self) {
        self.clear();
        self.receive_all();
    }

    pub fn clear(&mut self) {
        self.points.clear();
    }

    fn pct1000_index(&self, pct1000: usize) -> usize {
        debug_assert!(pct1000 < 1000);
        self.points.len() * pct1000 / 1000
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
