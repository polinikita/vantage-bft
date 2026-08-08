//! Starfish-parity measurement substrate (PHASE2-SPEC.md #5), shared by `primary` and
//! `worker`. Ported minimally from `~/code/starfish/crates/starfish-core/src/{stat,
//! metrics,prometheus}.rs` -- see each module's header for exactly what was kept and
//! what was deliberately trimmed or swapped for this workspace's own infrastructure.
//! The Phase-3+ Vantage core reuses this crate rather than a new one.

pub mod metrics;
pub mod prometheus;
pub mod snapshot;
pub mod stat;

pub use crate::metrics::{
    spawn_queue_sampler, AsPrometheusMetric, HistogramReporter, MetricReporter, Metrics,
    QueueProbe, StoreProbe, UtilizationTimer, UtilizationTimerVecExt,
};
pub use crate::prometheus::{register_process_collector, start_prometheus_server};
pub use crate::snapshot::{
    aggregate_latency_snapshots, read_counter, read_counter_vec, read_latency_snapshot,
    read_materialised_latency_snapshot, read_seal_route_counts, read_vantage_progress,
    AggregatedLatency, LatencySnapshot, VantageProgress,
};
pub use crate::stat::{histogram, DivUsize, HistogramSender, PreciseHistogram};
