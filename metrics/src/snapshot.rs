//! In-process reading of a node's own `Registry` (PHASE2-SPEC.md §8): `local-benchmark`
//! self-hosts every node in one process, so it can read each one's committed-transaction
//! latency directly from its `Registry::gather()` output -- the same gauges/counters the
//! HTTP endpoint serves, just read via the typed protobuf structs instead of an HTTP
//! round-trip and text-format parse. Cross-node aggregation mirrors
//! `benchmark/benchmark/logs.py`'s audited rules exactly (max for count/misses, since
//! every node's committer processes the whole replicated commit stream; summed sum/
//! sum-of-squares for the avg/stddev ratio, which is invariant to that same scaling;
//! median across nodes for percentiles) so a `local-benchmark` run and a `fab remote` run
//! report comparable numbers.

use prometheus::Registry;
use std::collections::BTreeMap;

/// One node's own view of the transaction-committed-latency metrics, read directly from
/// its `Registry` (not summed with anyone else's yet).
#[derive(Clone, Copy, Debug, Default)]
pub struct LatencySnapshot {
    pub count: u64,
    pub sum_micros: u64,
    pub squared_sum_micros: u64,
    pub p25_micros: u64,
    pub p50_micros: u64,
    pub p75_micros: u64,
    pub p90_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
    pub misses: u64,
    pub committed_transactions: u64,
    pub committed_bytes: u64,
}

/// Reads the current gauge/counter values from `registry`. Callers should call
/// `MetricReporter::force_report` immediately beforehand so the histogram gauges reflect
/// every observation up to that instant, not whatever the last periodic tick saw.
/// Returns `None` if this node never observed a single committed transaction (the
/// `transaction_committed_latency` gauge vector doesn't exist until the first
/// observation -- see `HistogramReporter::report`).
pub fn read_latency_snapshot(registry: &Registry) -> Option<LatencySnapshot> {
    read_latency_snapshot_for(registry, "transaction_committed_latency")
}

/// The materialised-latency counterpart of `read_latency_snapshot`: same shape, read
/// from `transaction_materialised_latency` instead.
///
/// The two series differ by exactly the payload-availability cost. `transaction_
/// committed_latency` stops at the primary's ordering decision (`commit_millis`, stamped
/// at commit and carried to the worker); `transaction_materialised_latency` stops when
/// the batch is actually read and deserialised locally, so a batch this node had to
/// fetch contributes its ORIGINAL commit instant to the first series and its LATER
/// arrival instant to the second. Only the second is comparable to starfish, whose
/// `block_handler::transaction_observer` likewise stamps at the moment the block's
/// transactions are in hand.
///
/// `misses`/`committed_transactions`/`committed_bytes` are shared, not per-series --
/// both snapshots report the same underlying counters.
pub fn read_materialised_latency_snapshot(registry: &Registry) -> Option<LatencySnapshot> {
    read_latency_snapshot_for(registry, "transaction_materialised_latency")
}

/// Shared body of the two readers above. `base` names the histogram-gauge family; the
/// sum-of-squares counter is always `{base}_squared_micros` (see `Metrics`'s own
/// registration of both pairs).
fn read_latency_snapshot_for(registry: &Registry, base: &str) -> Option<LatencySnapshot> {
    let families = registry.gather();
    let squared = format!("{base}_squared_micros");

    let gauge = |metric: &str, label: &str| -> Option<u64> {
        families
            .iter()
            .find(|f| f.get_name() == metric)
            .and_then(|f| {
                f.get_metric()
                    .iter()
                    .find(|m| {
                        m.get_label()
                            .iter()
                            .any(|l| l.get_name() == "v" && l.get_value() == label)
                    })
                    .map(|m| m.get_gauge().get_value() as u64)
            })
    };
    let counter = |metric: &str| -> u64 {
        families
            .iter()
            .find(|f| f.get_name() == metric)
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_counter().get_value() as u64)
            .unwrap_or(0)
    };

    let count = gauge(base, "count")?;

    Some(LatencySnapshot {
        count,
        sum_micros: gauge(base, "sum").unwrap_or(0),
        squared_sum_micros: counter(&squared),
        p25_micros: gauge(base, "p25").unwrap_or(0),
        p50_micros: gauge(base, "p50").unwrap_or(0),
        p75_micros: gauge(base, "p75").unwrap_or(0),
        p90_micros: gauge(base, "p90").unwrap_or(0),
        p99_micros: gauge(base, "p99").unwrap_or(0),
        max_micros: gauge(base, "max").unwrap_or(0),
        misses: counter("latency_misses"),
        committed_transactions: counter("committed_transactions"),
        committed_bytes: counter("committed_bytes"),
    })
}

/// Cross-node aggregate, computed with the same rules `logs.py::_real_transaction_latency`
/// uses (PHASE2-SPEC.md §5 amendments). `None` if no node ever observed a transaction.
#[derive(Clone, Copy, Debug)]
pub struct AggregatedLatency {
    pub avg_micros: f64,
    pub stddev_micros: f64,
    pub p25_micros: u64,
    pub p50_micros: u64,
    pub p75_micros: u64,
    pub p90_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
    /// Max across nodes -- every node counts (approximately) the same replicated
    /// commit stream, not a disjoint partition of it.
    pub count: u64,
    pub misses: u64,
    pub nodes_reporting: usize,
}

pub fn aggregate_latency_snapshots(snapshots: &[LatencySnapshot]) -> Option<AggregatedLatency> {
    if snapshots.is_empty() {
        return None;
    }
    let max_count = snapshots.iter().map(|s| s.count).max().unwrap_or(0);
    if max_count == 0 {
        return None;
    }

    let total_count: u64 = snapshots.iter().map(|s| s.count).sum();
    let total_sum: u64 = snapshots.iter().map(|s| s.sum_micros).sum();
    let total_squared_sum: u64 = snapshots.iter().map(|s| s.squared_sum_micros).sum();
    let max_misses = snapshots.iter().map(|s| s.misses).max().unwrap_or(0);

    let avg_micros = total_sum as f64 / total_count as f64;
    let variance = total_squared_sum as f64 / total_count as f64 - avg_micros * avg_micros;
    let stddev_micros = if variance > 0.0 { variance.sqrt() } else { 0.0 };

    Some(AggregatedLatency {
        avg_micros,
        stddev_micros,
        p25_micros: median(snapshots.iter().map(|s| s.p25_micros)),
        p50_micros: median(snapshots.iter().map(|s| s.p50_micros)),
        p75_micros: median(snapshots.iter().map(|s| s.p75_micros)),
        p90_micros: median(snapshots.iter().map(|s| s.p90_micros)),
        p99_micros: median(snapshots.iter().map(|s| s.p99_micros)),
        max_micros: snapshots.iter().map(|s| s.max_micros).max().unwrap_or(0),
        count: max_count,
        misses: max_misses,
        nodes_reporting: snapshots.len(),
    })
}

/// PHASE6-SPEC.md §9 gate amendment: reads the `vantage_seals` counter vector (labeled
/// by `route`) from a PRIMARY's own registry (distinct from the worker registries
/// `read_latency_snapshot` reads -- `vantage_seals` lives on `AgbEngine`, primary-side).
/// Empty on the two Autobahn paths (nothing ever observes into it there) and before any
/// view has sealed. Keyed by route name, in whatever order the registry reports them
/// (`BTreeMap` for deterministic, alphabetical print order).
pub fn read_seal_route_counts(registry: &Registry) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let families = registry.gather();
    let Some(family) = families.iter().find(|f| f.get_name() == "vantage_seals") else {
        return out;
    };
    for m in family.get_metric() {
        let Some(route) = m.get_label().iter().find(|l| l.get_name() == "route") else {
            continue;
        };
        out.insert(
            route.get_value().to_string(),
            m.get_counter().get_value() as u64,
        );
    }
    out
}

/// PHASE7-PREP-NOTES.md Finding A: one node's own progress-gauge snapshot, read from a
/// PRIMARY's own registry (same registry `read_seal_route_counts` reads). Plain
/// unlabeled `IntGauge`s, so each family has exactly one metric with no labels --
/// unlike `read_latency_snapshot`'s `v`-labeled histogram gauges above.
#[derive(Clone, Copy, Debug, Default)]
pub struct VantageProgress {
    pub entered_view: i64,
    pub frontier_a_i: i64,
    pub cursor_next_view: i64,
    pub control_round: i64,
    pub control_delivered_len: i64,
    pub control_consume_pos: i64,
}

/// Reads the six Finding-A progress gauges. These `IntGauge`s are always registered
/// (Phase-3-counter pattern: registered on every primary's registry, defaulting to 0),
/// so this never returns `None` in practice -- the `Option` return only mirrors
/// `read_latency_snapshot`'s shape for a family that could in principle be missing (a
/// registry this crate didn't build, e.g. in a future caller). On the two Autobahn
/// paths (no `VantageCore` ever constructed) the gauges simply stay at 0 forever.
pub fn read_vantage_progress(registry: &Registry) -> Option<VantageProgress> {
    let families = registry.gather();
    let gauge = |metric: &str| -> Option<i64> {
        families
            .iter()
            .find(|f| f.get_name() == metric)
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_gauge().get_value() as i64)
    };
    Some(VantageProgress {
        entered_view: gauge("vantage_entered_view")?,
        frontier_a_i: gauge("vantage_frontier_a_i")?,
        cursor_next_view: gauge("vantage_cursor_next_view")?,
        control_round: gauge("vantage_control_round")?,
        control_delivered_len: gauge("vantage_control_delivered_len")?,
        control_consume_pos: gauge("vantage_control_consume_pos")?,
    })
}

/// METRICS-DASHBOARD-SPEC.md §2: reads a single unlabeled `IntCounter`'s current
/// value from `registry`. `0` if the metric doesn't exist on this registry (e.g. a
/// worker registry queried for a primary-only counter).
pub fn read_counter(registry: &Registry, name: &str) -> u64 {
    registry
        .gather()
        .iter()
        .find(|f| f.get_name() == name)
        .and_then(|f| f.get_metric().first())
        .map(|m| m.get_counter().get_value() as u64)
        .unwrap_or(0)
}

/// METRICS-DASHBOARD-SPEC.md §1/§2: reads a labeled `IntCounterVec`'s current values
/// from `registry`, keyed by the value of `label` (e.g. `type` or `proc`). Empty if
/// the metric family doesn't exist on this registry.
pub fn read_counter_vec(registry: &Registry, name: &str, label: &str) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    let families = registry.gather();
    let Some(family) = families.iter().find(|f| f.get_name() == name) else {
        return out;
    };
    for m in family.get_metric() {
        let Some(l) = m.get_label().iter().find(|l| l.get_name() == label) else {
            continue;
        };
        *out.entry(l.get_value().to_string()).or_insert(0) += m.get_counter().get_value() as u64;
    }
    out
}

/// Median of an unordered `u64` iterator (even-length: average of the two middle
/// values, rounded down -- consistent with reporting in whole microseconds).
fn median(values: impl Iterator<Item = u64>) -> u64 {
    let mut values: Vec<u64> = values.collect();
    values.sort_unstable();
    let len = values.len();
    if len == 0 {
        return 0;
    }
    if len % 2 == 1 {
        values[len / 2]
    } else {
        (values[len / 2 - 1] + values[len / 2]) / 2
    }
}
