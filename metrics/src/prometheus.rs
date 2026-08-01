// Ported from starfish (`~/code/starfish/crates/starfish-core/src/prometheus.rs`,
// Apache-2.0) for the Starfish-parity metrics endpoint (PHASE2-SPEC.md #5).
//
// Deviations from starfish: no `tower_http::compression::CompressionLayer` (an internal
// endpoint scraped a handful of times per run by the benchmark harness needs no response
// compression) and no custom `runtime::{Handle, JoinHandle}` wrapper (starfish's own
// multi-runtime abstraction; plain `tokio::spawn` suffices here).

use std::net::SocketAddr;

use axum::{extract::Extension, http::StatusCode, routing::get, Router};
use prometheus::{Registry, TextEncoder};
use tokio::{net::TcpListener, task::JoinHandle};

pub const METRICS_ROUTE: &str = "/metrics";

/// Registers CPU-time and resident-memory metrics for this OS process.
///
/// Standalone primary/worker binaries call this after constructing their registry,
/// giving Prometheus one `process_*` series per independently running process.  The
/// in-process benchmark deliberately does not call it: all validators there share a
/// PID, so attaching the same process collector to every per-validator registry would
/// falsely duplicate whole-process resource usage.  The upstream collector is Linux-
/// only; other platforms keep the endpoint operational but expose no `process_*`
/// series.
pub fn register_process_collector(registry: &Registry) -> Result<(), prometheus::Error> {
    #[cfg(target_os = "linux")]
    {
        registry.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = registry;
        Ok(())
    }
}

/// Always-on Prometheus text-exposition endpoint (starfish parity: it also serves
/// Phase-3+ protocol metrics once those land on the same registry).
pub fn start_prometheus_server(
    address: SocketAddr,
    registry: &Registry,
) -> JoinHandle<Result<(), std::io::Error>> {
    let app = Router::new()
        .route(METRICS_ROUTE, get(metrics))
        .layer(Extension(registry.clone()));

    log::info!("Prometheus server booted on {}", address);
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await?;
        axum::serve(listener, app).await
    })
}

async fn metrics(registry: Extension<Registry>) -> (StatusCode, String) {
    let metrics_families = registry.gather();
    match TextEncoder.encode_to_string(&metrics_families) {
        Ok(metrics) => (StatusCode::OK, metrics),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Unable to encode metrics: {error}"),
        ),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn process_collector_exports_cpu_and_resident_memory() {
        let registry = Registry::new();
        register_process_collector(&registry).unwrap();

        let names: Vec<_> = registry
            .gather()
            .into_iter()
            .map(|family| family.get_name().to_owned())
            .collect();
        assert!(names.iter().any(|name| name == "process_cpu_seconds_total"));
        assert!(names
            .iter()
            .any(|name| name == "process_resident_memory_bytes"));
    }
}
