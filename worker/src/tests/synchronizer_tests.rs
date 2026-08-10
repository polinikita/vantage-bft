// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener};
use std::fs;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn synchronize() {
    let (tx_message, rx_message) = channel(1);

    let mut keys = keys();
    let (name, _) = keys.pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(9_000);

    let path = ".db_test_synchronize";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    let registry = prometheus::Registry::new();
    let (metrics, _reporter) = Metrics::new(&registry);
    Synchronizer::spawn(
        name,
        id,
        committee.clone(),
        store.clone(),
        50,
        1_000_000,
        3,
        rx_message,
        std::collections::HashMap::new(),
        metrics,
        BatchConfig::default(),
    );

    let (target, _) = keys.pop().unwrap();
    let address = committee.worker(&target, &id).unwrap().worker_to_worker;
    let missing = vec![batch_digest()];
    let message = WorkerMessage::BatchRequest(missing.clone(), name);
    let serialized = bincode::serialize(&message).unwrap();
    let handle = listener(address, Some(Bytes::from(serialized)));

    let message = PrimaryWorkerMessage::Synchronize(missing, target);
    tx_message.send(message).await.unwrap();

    assert!(handle.await.is_ok());
}

/// Deferred misses are retried and stale entries are evicted.
#[cfg(feature = "benchmark")]
mod benchmark_metrics_tests {
    use super::*;
    use crate::common::{committee_with_base_port, keys};
    use crypto::Blake3Hasher;
    use metrics::Metrics;

    /// Build a synchronizer directly so its metric state can be inspected.
    fn new_test_synchronizer(store: Store, metrics: Arc<Metrics>) -> Synchronizer {
        let mut keys = keys();
        let (name, _) = keys.pop().unwrap();
        let committee = committee_with_base_port(9_200);
        let (_tx_message, rx_message) = channel(1);
        Synchronizer {
            name,
            id: 0,
            committee,
            store,
            gc_depth: 50,                // Not used by these tests.
            sync_retry_delay: 1_000_000, // Ensure it is not triggered.
            sync_retry_nodes: 3,         // Not used by these tests.
            rx_message,
            network: SimpleSender::new(),
            round: Round::default(),
            pending: HashMap::new(),
            metrics,
            observed_commits: HashSet::new(),
            observed_commits_order: BTreeSet::new(),
            pending_misses: BTreeMap::new(),
        }
    }

    /// Build a transaction using the client wire format.
    fn make_tx(id: u64, submitted_millis: u64, payload: &[u8]) -> Bytes {
        let mut tx = Vec::with_capacity(17 + payload.len());
        tx.push(1u8);
        tx.extend_from_slice(&id.to_be_bytes());
        tx.extend_from_slice(&submitted_millis.to_le_bytes());
        tx.extend_from_slice(payload);
        Bytes::from(tx)
    }

    /// Compute the content-addressed digest used by the processor.
    fn digest_of(bytes: &[u8]) -> Digest {
        let mut hasher = Blake3Hasher::new();
        hasher.update(bytes);
        Digest(hasher.finalize().into())
    }

    /// Read one labeled histogram statistic from the registry.
    fn read_histogram_gauge(
        registry: &prometheus::Registry,
        metric: &str,
        label: &str,
    ) -> Option<u64> {
        registry
            .gather()
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
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[tokio::test]
    async fn deferred_miss_is_retried_and_counted_once() {
        let path = ".db_test_deferred_miss_retry";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();

        let registry = prometheus::Registry::new();
        let (metrics, reporter) = Metrics::new(&registry);
        let mut synchronizer = new_test_synchronizer(store.clone(), metrics.clone());

        let commit_millis = 1_700_000_000_000u64;
        let committed_latency_ms = 250u64;
        let submitted_millis = commit_millis - committed_latency_ms;

        let tx = make_tx(7, submitted_millis, b"hello-world-payload");
        let wire_bytes = bincode::serialize(&WorkerMessage::Batch(vec![tx.clone()])).unwrap();
        let digest = digest_of(&wire_bytes);

        let deferred = synchronizer
            .observe_committed(commit_millis, vec![digest.clone()])
            .await;
        assert_eq!(
            deferred.len(),
            1,
            "a store miss must be deferred, not dropped"
        );
        assert_eq!(metrics.latency_misses.get(), 1);
        assert_eq!(metrics.latency_misses_resolved.get(), 0);
        assert_eq!(metrics.committed_transactions.get(), 0);
        assert!(!synchronizer.observed_commits.contains(&digest));
        assert!(
            synchronizer
                .pending_misses
                .contains_key(&(commit_millis, digest.clone())),
            "the miss must be recorded with its ORIGINAL commit instant"
        );

        store
            .clone()
            .write(digest.to_vec(), wire_bytes.clone())
            .await;

        let miss = deferred.into_iter().next().unwrap();
        let (resolved_digest, resolved_commit_millis) = Synchronizer::metrics_waiter(
            miss.digest,
            miss.commit_millis,
            store.clone(),
            miss.cancel,
        )
        .await
        .expect("notify_read must resolve once the batch is written");
        let materialise_lower_bound = now_millis();
        synchronizer
            .finish_deferred_retry(resolved_digest, resolved_commit_millis)
            .await;
        let materialise_upper_bound = now_millis();

        assert_eq!(metrics.committed_transactions.get(), 1);
        assert_eq!(metrics.committed_bytes.get(), tx.len() as u64);
        assert_eq!(metrics.latency_misses_resolved.get(), 1);
        assert_eq!(
            metrics.latency_misses.get(),
            1,
            "the original deferral must not be double-counted"
        );
        assert!(synchronizer.observed_commits.contains(&digest));
        assert!(!synchronizer
            .pending_misses
            .contains_key(&(commit_millis, digest.clone())));

        reporter.force_report();
        let committed_p50_micros =
            read_histogram_gauge(&registry, "transaction_committed_latency", "p50")
                .expect("one observation must produce a p50");
        assert_eq!(committed_p50_micros, committed_latency_ms * 1_000);

        let materialised_p50_micros =
            read_histogram_gauge(&registry, "transaction_materialised_latency", "p50")
                .expect("one observation must produce a p50");
        assert!(
            materialised_p50_micros > committed_p50_micros,
            "materialised latency ({materialised_p50_micros} us) must exceed committed \
             latency ({committed_p50_micros} us) for a deferred-then-resolved miss"
        );
        let materialised_lower = (materialise_lower_bound - submitted_millis) * 1_000;
        let materialised_upper = (materialise_upper_bound - submitted_millis) * 1_000;
        assert!(
            materialised_p50_micros >= materialised_lower
                && materialised_p50_micros <= materialised_upper,
            "materialised latency {materialised_p50_micros} us outside expected \
             [{materialised_lower}, {materialised_upper}] us"
        );

        let deferred_again = synchronizer
            .observe_committed(commit_millis + 1, vec![digest.clone()])
            .await;
        assert!(deferred_again.is_empty());
        assert_eq!(metrics.committed_transactions.get(), 1);
    }

    #[tokio::test]
    async fn stale_entries_are_pruned() {
        let path = ".db_test_prune_stale_metrics";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();

        let registry = prometheus::Registry::new();
        let (metrics, _reporter) = Metrics::new(&registry);
        let mut synchronizer = new_test_synchronizer(store.clone(), metrics.clone());

        let old_commit_millis = 1_000_000_000_000u64;

        let old_missing_tx = make_tx(1, old_commit_millis - 10, b"old-missing");
        let old_missing_bytes =
            bincode::serialize(&WorkerMessage::Batch(vec![old_missing_tx])).unwrap();
        let old_missing_digest = digest_of(&old_missing_bytes);

        let old_hit_tx = make_tx(2, old_commit_millis - 10, b"old-hit");
        let old_hit_bytes = bincode::serialize(&WorkerMessage::Batch(vec![old_hit_tx])).unwrap();
        let old_hit_digest = digest_of(&old_hit_bytes);
        store
            .clone()
            .write(old_hit_digest.to_vec(), old_hit_bytes)
            .await;

        let deferred = synchronizer
            .observe_committed(
                old_commit_millis,
                vec![old_missing_digest.clone(), old_hit_digest.clone()],
            )
            .await;
        assert_eq!(deferred.len(), 1);
        assert!(synchronizer
            .pending_misses
            .contains_key(&(old_commit_millis, old_missing_digest.clone())));
        assert!(synchronizer.observed_commits.contains(&old_hit_digest));
        assert!(synchronizer
            .observed_commits_order
            .contains(&(old_commit_millis, old_hit_digest.clone())));

        let new_commit_millis = old_commit_millis + BENCHMARK_METRICS_RETENTION_MILLIS + 1;
        let fresh_tx = make_tx(3, new_commit_millis - 5, b"fresh");
        let fresh_bytes = bincode::serialize(&WorkerMessage::Batch(vec![fresh_tx])).unwrap();
        let fresh_digest = digest_of(&fresh_bytes);
        store
            .clone()
            .write(fresh_digest.to_vec(), fresh_bytes)
            .await;

        let deferred_again = synchronizer
            .observe_committed(new_commit_millis, vec![fresh_digest.clone()])
            .await;
        assert!(deferred_again.is_empty());

        assert!(synchronizer.pending_misses.is_empty());
        assert!(!synchronizer.observed_commits.contains(&old_hit_digest));
        assert!(!synchronizer
            .observed_commits_order
            .contains(&(old_commit_millis, old_hit_digest)));
        assert!(synchronizer.observed_commits.contains(&fresh_digest));

        let miss = deferred.into_iter().next().unwrap();
        let resolved = Synchronizer::metrics_waiter(
            miss.digest,
            miss.commit_millis,
            store.clone(),
            miss.cancel,
        )
        .await;
        assert_eq!(
            resolved, None,
            "a pruned miss's waiter must be canceled, not left dangling"
        );
    }
}
