//! Shared metrics substrate for primary and worker processes.

pub mod metrics;
#[cfg(feature = "pipeline-tracing")]
mod pipeline;
pub mod prometheus;
pub mod snapshot;
pub mod stat;

pub use crate::metrics::{
    spawn_queue_sampler, AsPrometheusMetric, HistogramReporter, MetricReporter, Metrics,
    QueueProbe, StoreProbe, UtilizationTimer, UtilizationTimerVecExt,
};
#[cfg(feature = "pipeline-tracing")]
pub use crate::pipeline::PipelineMetrics;
pub use crate::prometheus::{register_process_collector, start_prometheus_server};
#[cfg(feature = "pipeline-tracing")]
pub use crate::snapshot::read_duration_snapshot;
pub use crate::snapshot::{
    aggregate_latency_snapshots, read_counter, read_counter_vec, read_latency_snapshot,
    read_materialised_latency_snapshot, read_seal_route_counts, read_vantage_progress,
    AggregatedLatency, LatencySnapshot, VantageProgress,
};
pub use crate::stat::{histogram, DivUsize, HistogramSender, MulUsize, PreciseHistogram};
