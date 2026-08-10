// Prometheus endpoint and process metrics.

use std::net::SocketAddr;

use axum::{extract::Extension, http::StatusCode, routing::get, Router};
use prometheus::{Registry, TextEncoder};
use tokio::{net::TcpListener, task::JoinHandle};

pub const METRICS_ROUTE: &str = "/metrics";

/// Registers CPU-time and resident-memory metrics for this process.
/// Process metrics are available on Linux.
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

/// Starts the Prometheus text-exposition endpoint.
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
