use crate::messages::Header;
use crate::primary::PrimaryWorkerMessage;
use crate::vantage::lanes::LaneManager;
use crate::vantage::wire::Wire;
use crate::vantage::Effect;
use config::WorkerId;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::Sender;

/// Tracks payload readiness and committed output delivery.
pub struct PayloadIo {
    /// Missing `(batch digest, worker)` keys for each header.
    pub(crate) pending_payload: HashMap<Digest, HashSet<(Digest, WorkerId)>>,
    pub(crate) store: Store,
    pub(crate) tx_payload_ready: Sender<(Digest, Digest, WorkerId)>,

    /// Delivers committed headers to the application.
    pub(crate) tx_output: Sender<Header>,

    /// Last synchronization time for each batch and worker.
    pub(crate) last_synchronize: HashMap<(Digest, WorkerId), Instant>,

    /// Last retry-carrying synchronization time for each worker.
    pub(crate) last_retry_synchronize: HashMap<WorkerId, Instant>,

    pub(crate) last_synchronize_pruned_at: Instant,

    pub(crate) metrics: Option<Arc<Metrics>>,
}

/// Minimum interval between retries for one batch and worker.
const SYNCHRONIZE_RESEND_MIN_INTERVAL: Duration = Duration::from_millis(1_000);

/// Minimum interval between retry-carrying messages to one worker.
const RETRY_SYNCHRONIZE_MIN_INTERVAL: Duration = Duration::from_millis(1_000);

pub(crate) struct SynchronizeDecision {
    /// Keys to send in caller order.
    pub(crate) send: Vec<(Digest, WorkerId)>,
    /// Workers whose messages contain retries and must close the aggregate gate.
    pub(crate) carried_retry: HashSet<WorkerId>,
}

/// Sends new keys immediately and requires retries to pass both rate limits.
pub(crate) fn decide_synchronize(
    requested: &[(Digest, WorkerId)],
    newly: &HashSet<(Digest, WorkerId)>,
    last_synchronize: &HashMap<(Digest, WorkerId), Instant>,
    last_retry_synchronize: &HashMap<WorkerId, Instant>,
    now: Instant,
) -> SynchronizeDecision {
    let mut retry_allowed: HashSet<WorkerId> = HashSet::new();
    for (_, worker_id) in requested {
        if retry_allowed.contains(worker_id) {
            continue;
        }
        let open = last_retry_synchronize
            .get(worker_id)
            .is_none_or(|last| now.duration_since(*last) >= RETRY_SYNCHRONIZE_MIN_INTERVAL);
        if open {
            retry_allowed.insert(*worker_id);
        }
    }

    let mut send = Vec::new();
    let mut carried_retry: HashSet<WorkerId> = HashSet::new();
    for (digest, worker_id) in requested {
        let key = (digest.clone(), *worker_id);
        if !newly.contains(&key) {
            let key_due = last_synchronize
                .get(&key)
                .is_none_or(|last| now.duration_since(*last) >= SYNCHRONIZE_RESEND_MIN_INTERVAL);
            if !key_due || !retry_allowed.contains(worker_id) {
                continue;
            }
            carried_retry.insert(*worker_id);
        }
        send.push(key);
    }
    SynchronizeDecision {
        send,
        carried_retry,
    }
}

pub(crate) fn block_was_cached(effects: &[Effect], digest: &Digest) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, Effect::BlockCached(cached) if cached == digest))
}

pub(crate) async fn append_missing_payload_sync(
    lm: &mut LaneManager,
    header: &Header,
    effects: &mut Vec<Effect>,
) {
    if !block_was_cached(effects, &header.id) {
        return;
    }
    let missing = lm.missing_payload(header).await;
    if !missing.is_empty() {
        effects.push(Effect::SyncBatches(
            header.author,
            header.id.clone(),
            missing,
        ));
    }
}

impl PayloadIo {
    pub(crate) fn new(
        store: Store,
        tx_payload_ready: Sender<(Digest, Digest, WorkerId)>,
        tx_output: Sender<Header>,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        Self {
            pending_payload: HashMap::new(),
            store,
            tx_payload_ready,
            tx_output,
            last_synchronize: HashMap::new(),
            last_retry_synchronize: HashMap::new(),
            last_synchronize_pruned_at: Instant::now(),
            metrics,
        }
    }

    fn prune_last_synchronize(&mut self, now: Instant) {
        if now.duration_since(self.last_synchronize_pruned_at) < SYNCHRONIZE_RESEND_MIN_INTERVAL {
            return;
        }
        self.last_synchronize
            .retain(|_, last| now.duration_since(*last) < SYNCHRONIZE_RESEND_MIN_INTERVAL);
        self.last_synchronize_pruned_at = now;
    }

    pub(crate) fn publish_sizes(&self) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        metrics
            .vantage_pending_payload_headers
            .set(self.pending_payload.len() as i64);
        metrics.vantage_pending_payload_keys.set(
            self.pending_payload
                .values()
                .map(|set| set.len())
                .sum::<usize>() as i64,
        );
        metrics
            .vantage_last_synchronize_len
            .set(self.last_synchronize.len() as i64);
    }

    pub(crate) async fn sync_batches(
        &mut self,
        wire: &mut Wire,
        author: PublicKey,
        header_digest: Digest,
        missing: Vec<(Digest, WorkerId)>,
    ) {
        if missing.is_empty() {
            return;
        }
        let mut seen = HashSet::new();
        let requested: Vec<(Digest, WorkerId)> = missing
            .into_iter()
            .filter(|entry| seen.insert(entry.clone()))
            .collect();
        if requested.is_empty() {
            return;
        }

        let newly_pending = {
            let pending = self
                .pending_payload
                .entry(header_digest.clone())
                .or_default();
            requested
                .iter()
                .filter_map(|(digest, worker_id)| {
                    let entry = (digest.clone(), *worker_id);
                    pending.insert(entry.clone()).then_some(entry)
                })
                .collect::<Vec<_>>()
        };

        let now = Instant::now();
        let newly: HashSet<(Digest, WorkerId)> = newly_pending.iter().cloned().collect();
        let decision = decide_synchronize(
            &requested,
            &newly,
            &self.last_synchronize,
            &self.last_retry_synchronize,
            now,
        );

        let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        for (digest, worker_id) in &decision.send {
            self.last_synchronize
                .insert((digest.clone(), *worker_id), now);
            by_worker
                .entry(*worker_id)
                .or_default()
                .push(digest.clone());
        }

        for worker_id in decision.carried_retry {
            self.last_retry_synchronize.insert(worker_id, now);
        }

        self.prune_last_synchronize(now);
        self.publish_sizes();

        for (worker_id, digests) in by_worker {
            if let Some(addr) = wire.worker_addr(worker_id) {
                let message = PrimaryWorkerMessage::SynchronizeAuthor(digests, author);
                let bytes = bincode::serialize(&message).expect("serializes");
                wire.send_to_worker(addr, bytes, message.type_name()).await;
            }
        }

        for (digest, worker_id) in newly_pending {
            let mut store = self.store.clone();
            let tx = self.tx_payload_ready.clone();
            let header_digest = header_digest.clone();
            tokio::spawn(async move {
                let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                if store.notify_read(key).await.is_ok() {
                    let _ = tx.send((header_digest, digest, worker_id)).await;
                }
            });
        }
    }

    /// Notifies workers and the application of a commit timestamped in UTC milliseconds.
    pub(crate) async fn notify_committed(
        &mut self,
        wire: &mut Wire,
        commit_millis: u64,
        by_worker: Vec<(WorkerId, Vec<Digest>)>,
        headers: Vec<Header>,
    ) {
        for (worker_id, digests) in by_worker {
            if let Some(addr) = wire.worker_addr(worker_id) {
                let bytes =
                    bincode::serialize(&PrimaryWorkerMessage::Committed(commit_millis, digests))
                        .expect("serializes");
                wire.send_to_worker(addr, bytes, "Committed").await;
            }
        }
        for header in headers {
            if let Err(e) = self.tx_output.send(header).await {
                log::debug!("Failed to send block through the output channel: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(b: u8) -> Digest {
        Digest([b; 32])
    }

    #[test]
    fn newly_pending_keys_ignore_both_gates() {
        let now = Instant::now();
        let key = (digest(1), 0);
        let requested = vec![key.clone()];
        let newly: HashSet<_> = requested.iter().cloned().collect();
        let mut last_sync = HashMap::new();
        last_sync.insert(key.clone(), now);
        let mut last_retry = HashMap::new();
        last_retry.insert(0u32, now);

        let d = decide_synchronize(&requested, &newly, &last_sync, &last_retry, now);
        assert_eq!(d.send, vec![key]);
        assert!(
            d.carried_retry.is_empty(),
            "a newly-pending key must not close the retry gate"
        );
    }

    #[test]
    fn retry_within_its_key_interval_is_withheld() {
        let now = Instant::now();
        let key = (digest(2), 0);
        let requested = vec![key.clone()];
        let mut last_sync = HashMap::new();
        last_sync.insert(key, now);

        let d = decide_synchronize(
            &requested,
            &HashSet::new(),
            &last_sync,
            &HashMap::new(),
            now,
        );
        assert!(d.send.is_empty());
    }

    #[test]
    fn due_retries_coalesce_into_one_permitted_call() {
        let now = Instant::now();
        let requested: Vec<_> = (1..=5).map(|b| (digest(b), 0u32)).collect();
        let last_sync: HashMap<_, _> = requested
            .iter()
            .map(|k| (k.clone(), now - Duration::from_secs(10)))
            .collect();

        let d = decide_synchronize(
            &requested,
            &HashSet::new(),
            &last_sync,
            &HashMap::new(),
            now,
        );
        assert_eq!(
            d.send.len(),
            5,
            "all due retries ride the one permitted call"
        );
        assert_eq!(d.carried_retry, HashSet::from([0u32]));
    }

    #[test]
    fn aggregate_gate_withholds_due_retries_until_it_reopens() {
        let now = Instant::now();
        let requested: Vec<_> = (1..=5).map(|b| (digest(b), 0u32)).collect();
        let last_sync: HashMap<_, _> = requested
            .iter()
            .map(|k| (k.clone(), now - Duration::from_secs(10)))
            .collect();
        let mut last_retry = HashMap::new();
        last_retry.insert(0u32, now);

        let d = decide_synchronize(&requested, &HashSet::new(), &last_sync, &last_retry, now);
        assert!(
            d.send.is_empty(),
            "aggregate gate must bound the message rate"
        );
        assert!(d.carried_retry.is_empty());

        let later = now + RETRY_SYNCHRONIZE_MIN_INTERVAL;
        let d = decide_synchronize(&requested, &HashSet::new(), &last_sync, &last_retry, later);
        assert_eq!(d.send.len(), 5, "withheld retries must not be dropped");
    }

    #[test]
    fn the_aggregate_gate_is_per_worker() {
        let now = Instant::now();
        let requested = vec![(digest(1), 0u32), (digest(2), 1u32)];
        let last_sync: HashMap<_, _> = requested
            .iter()
            .map(|k| (k.clone(), now - Duration::from_secs(10)))
            .collect();
        let mut last_retry = HashMap::new();
        last_retry.insert(0u32, now);

        let d = decide_synchronize(&requested, &HashSet::new(), &last_sync, &last_retry, now);
        assert_eq!(d.send, vec![(digest(2), 1u32)]);
        assert_eq!(d.carried_retry, HashSet::from([1u32]));
    }
}
