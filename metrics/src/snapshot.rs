//! In-process reads and aggregation of registry metrics.

use prometheus::Registry;
use std::collections::BTreeMap;

/// One node's transaction latency metrics.
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

/// Reads the current latency gauges and counters. Callers should call
/// `MetricReporter::force_report` first. Returns `None` before the first observation.
pub fn read_latency_snapshot(registry: &Registry) -> Option<LatencySnapshot> {
    read_latency_snapshot_for(registry, "transaction_committed_latency")
}

/// Reads the materialized latency snapshot.
///
/// Committed latency ends at ordering. Materialized latency ends when the worker has
/// the batch locally. The shared counters are reported in both snapshots.
pub fn read_materialised_latency_snapshot(registry: &Registry) -> Option<LatencySnapshot> {
    read_latency_snapshot_for(registry, "transaction_materialised_latency")
}

/// Reads an exact duration histogram exported by `MetricReporter`.
#[cfg(feature = "pipeline-tracing")]
pub fn read_duration_snapshot(registry: &Registry, name: &str) -> Option<LatencySnapshot> {
    read_latency_snapshot_for(registry, name)
}

/// Shared reader for the two latency gauge families.
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

/// Cross-node latency aggregate. Returns `None` if no node observed a transaction.
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
    /// Maximum count across nodes because each node observes the replicated stream.
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

/// Reads primary seal counts keyed by route. Returns an empty map when unavailable.
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

/// Reads one primary's progress gauges.
#[derive(Clone, Copy, Debug, Default)]
pub struct VantageProgress {
    pub entered_view: i64,
    pub frontier_a_i: i64,
    pub cursor_next_view: i64,
    pub resolution_view: i64,
    pub resolution_height: i64,
    pub resolution_pending_anchors: i64,
    pub own_watermark: i64,
    pub entry_target: i64,
    pub omega_q: i64,
    /// Number of retained `BlockCache` entries.
    pub block_cache_len: i64,
}

/// Reads the primary progress gauges. Returns `None` if a required gauge is absent.
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
        resolution_view: gauge("vantage_resolution_view")?,
        resolution_height: gauge("vantage_resolution_height")?,
        resolution_pending_anchors: gauge("vantage_resolution_pending_anchors")?,
        own_watermark: gauge("vantage_own_watermark").unwrap_or(0),
        entry_target: gauge("vantage_entry_target").unwrap_or(0),
        omega_q: gauge("vantage_omega_q").unwrap_or(0),
        block_cache_len: gauge("vantage_block_cache_len").unwrap_or(0),
    })
}

/// Reads an unlabeled counter. Returns `0` if the metric is absent.
pub fn read_counter(registry: &Registry, name: &str) -> u64 {
    registry
        .gather()
        .iter()
        .find(|f| f.get_name() == name)
        .and_then(|f| f.get_metric().first())
        .map(|m| m.get_counter().get_value() as u64)
        .unwrap_or(0)
}

/// Reads a labeled counter family keyed by `label`. Returns an empty map if absent.
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

/// Median of an unordered `u64` iterator. Even-length inputs round down.
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
