use crate::{histogram, HistogramReporter, HistogramSender};
use prometheus::Registry;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone)]
pub struct PipelineMetrics {
    pub transaction_to_batch_seal_latency: HistogramSender<Duration>,
    pub batch_processing_latency: HistogramSender<Duration>,
    pub transaction_commit_to_materialised_latency: HistogramSender<Duration>,
    pub vantage_digest_to_block_publish_latency: HistogramSender<Duration>,
    pub vantage_block_publish_to_quorum_latency: HistogramSender<Duration>,
    pub vantage_block_publish_to_proposal_latency: HistogramSender<Duration>,
    pub vantage_block_publish_to_commit_latency: HistogramSender<Duration>,
    pub vantage_proposal_to_seal_latency: HistogramSender<Duration>,
    pub vantage_seal_to_finalize_latency: HistogramSender<Duration>,
}

pub struct PipelineReporter {
    reporters: Vec<Mutex<HistogramReporter<Duration>>>,
}

impl PipelineMetrics {
    pub fn new(registry: &Registry) -> (Self, PipelineReporter) {
        let mut reporters = Vec::new();

        macro_rules! metric {
            ($name:literal) => {{
                let (histogram, sender) = histogram();
                reporters.push(Mutex::new(HistogramReporter::new_in_registry(
                    histogram, registry, $name,
                )));
                sender
            }};
        }

        let metrics = Self {
            transaction_to_batch_seal_latency: metric!("transaction_to_batch_seal_latency"),
            batch_processing_latency: metric!("batch_processing_latency"),
            transaction_commit_to_materialised_latency: metric!(
                "transaction_commit_to_materialised_latency"
            ),
            vantage_digest_to_block_publish_latency: metric!(
                "vantage_digest_to_block_publish_latency"
            ),
            vantage_block_publish_to_quorum_latency: metric!(
                "vantage_block_publish_to_quorum_latency"
            ),
            vantage_block_publish_to_proposal_latency: metric!(
                "vantage_block_publish_to_proposal_latency"
            ),
            vantage_block_publish_to_commit_latency: metric!(
                "vantage_block_publish_to_commit_latency"
            ),
            vantage_proposal_to_seal_latency: metric!("vantage_proposal_to_seal_latency"),
            vantage_seal_to_finalize_latency: metric!("vantage_seal_to_finalize_latency"),
        };
        (metrics, PipelineReporter { reporters })
    }
}

impl PipelineReporter {
    pub fn force_report(&self) {
        for reporter in &self.reporters {
            let mut reporter = reporter.lock().unwrap();
            reporter.receive_all();
            reporter.report();
        }
    }
}
