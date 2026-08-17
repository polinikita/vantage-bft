//! Finite-delay injector for original lane-header publication.
//!
//! A dedicated reliable sender gives every selected header its own release time
//! without spawning a task per block. Only `Header(_, false)` traffic is routed
//! here; repair responses continue over the ordinary network path.

use bytes::Bytes;
use metrics::Metrics;
use network::{BatchConfig, CancelHandler, ChannelAuth, ReliableSender};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct DelayedHeaderSender {
    network: ReliableSender,
    destinations: Vec<SocketAddr>,
    metrics: Option<Arc<Metrics>>,
}

impl DelayedHeaderSender {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        destinations: Vec<SocketAddr>,
        base_latency: &HashMap<SocketAddr, Duration>,
        additional_delay_ms: u64,
        batch: BatchConfig,
        auth: Option<Arc<ChannelAuth>>,
        retry_backoff_max_ms: u64,
        metrics: Option<Arc<Metrics>>,
    ) -> Option<Self> {
        if destinations.is_empty() || additional_delay_ms == 0 {
            return None;
        }
        let additional = Duration::from_millis(additional_delay_ms);
        let latency = destinations
            .iter()
            .map(|address| {
                let delay = base_latency.get(address).copied().unwrap_or_default() + additional;
                (*address, delay)
            })
            .collect();
        let mut network = ReliableSender::new()
            .with_latency(latency)
            .with_batching(batch)
            .with_channel_auth(auth)
            .with_retry_backoff_max_ms(retry_backoff_max_ms);
        if let Some(value) = &metrics {
            network = network.with_metrics(value.clone());
        }
        Some(Self {
            network,
            destinations,
            metrics,
        })
    }

    pub(crate) async fn broadcast(&mut self, payload: Bytes) -> Vec<CancelHandler> {
        let copies = self.destinations.len() as u64;
        if let Some(metrics) = &self.metrics {
            metrics.late_header_messages_scheduled_total.inc_by(copies);
            metrics
                .late_header_bytes_scheduled_total
                .inc_by((payload.len() as u64).saturating_mul(copies));
        }
        self.network
            .broadcast_typed(self.destinations.clone(), payload, "Header")
            .await
    }
}
