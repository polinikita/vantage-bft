// D1 payload-sync bookkeeping + commit-notification output for `VantageCore`, split out
// of `vantage::node` so a second consensus protocol can reuse the identical
// payload-ready tracking and committed-output forwarding without depending on Vantage's
// own protocol state (`agb`, `frontier`, `cursor`, `pacemaker`, `resolver`, `control`).
// Pure code motion out of `node.rs`, mirroring `vantage::wire`'s own extraction (see
// that module's doc comment). `PayloadIo` takes `&mut Wire` as a parameter rather than
// owning or borrowing one as a field, since `Wire` is not protocol state: Simple-IT's
// own `SimpleItCore` builds its own `Wire` and threads it through the same two calls.
// `VantageCore::on_payload_ready`, which reads/writes `pending_payload` but is not
// itself one of the two moved methods, is re-pointed to `self.payload.pending_payload`
// at its two call sites.

use crate::messages::Header;
use crate::primary::PrimaryWorkerMessage;
use crate::vantage::wire::Wire;
use config::WorkerId;
use crypto::{Digest, PublicKey};
use metrics::Metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::Store;
use tokio::sync::mpsc::Sender;

/// D1 payload-sync bookkeeping and commit-notification output state `VantageCore` owns.
/// Per-field rationale below is carried over verbatim from `VantageCore`'s previous
/// copy of each field.
pub struct PayloadIo {
    /// D1 payload-sync bookkeeping: outstanding `(digest, worker_id)` keys per header
    /// digest, so `LaneManager::set_payload_ready` (which unconditionally marks a block
    /// payload-ready once called -- see its doc comment) is only called once *every*
    /// missing batch for that header has actually arrived, not on the first one.
    pub(crate) pending_payload: HashMap<Digest, HashSet<(Digest, WorkerId)>>,
    pub(crate) store: Store,
    pub(crate) tx_payload_ready: Sender<(Digest, Digest, WorkerId)>,

    /// PHASE7-PREP-NOTES.md: pays down PHASE4-NOTES.md §6's scope cut -- forwards each
    /// cursor-committed `Header` to the top-level application, the same output-channel
    /// shape `Committer` (Autobahn) already feeds. `Primary::spawn`'s `Vantage` arm
    /// used to drop the `tx_output` it's handed (never referenced it), so this
    /// channel's receiver (`node`/`local_benchmark`'s `rx_output`) closed immediately;
    /// `node::main`'s `analyze(rx_output)` loop returning on a closed channel is what
    /// hit the `unreachable!()` right after every primary's boot line.
    pub(crate) tx_output: Sender<Header>,

    /// Last time a `Synchronize` actually went to a worker for this `(digest, worker_id)`,
    /// so a re-send is a RETRY rather than an unbounded repeat. See
    /// `SYNCHRONIZE_RESEND_MIN_INTERVAL`.
    pub(crate) last_synchronize: HashMap<(Digest, WorkerId), Instant>,

    /// Last time a RETRY-carrying `Synchronize` went to this worker at all -- the
    /// aggregate bound that the per-key map above cannot provide. See
    /// `RETRY_SYNCHRONIZE_MIN_INTERVAL`.
    pub(crate) last_retry_synchronize: HashMap<WorkerId, Instant>,

    /// When `last_synchronize` was last pruned. That map was insert-only: one entry per
    /// distinct `(digest, worker_id)` ever synced, never removed, so it grew without bound
    /// for the life of the process -- worst on exactly the nodes already in trouble.
    /// Pruning is semantically free (see `prune_last_synchronize`) but O(n), so it runs on
    /// a cadence rather than per call.
    pub(crate) last_synchronize_pruned_at: Instant,

    /// Progress gauges for the two maps above, both of which grow on a node whose worker
    /// has stopped materialising -- and neither of which was observable when that happened
    /// on 2026-08-08. `Option` to match this module's convention throughout: a library
    /// caller may run without a metrics registry, in which case `publish_sizes` is a no-op.
    pub(crate) metrics: Option<Arc<Metrics>>,
}

/// Minimum gap between two `Synchronize` messages naming the same `(digest, worker_id)`.
///
/// `sync_batches` used to re-send EVERY still-missing entry on EVERY call, deliberately, as
/// a transport retry for a lost primary-to-worker message. Measured on the 2026-08-08 n=50
/// @200k netem run, that turned into request amplification on exactly the nodes that could
/// least afford it: a node whose worker had stopped materialising had every arriving header
/// re-request its whole missing set, producing **~600 Synchronize/s against ~11/s on a
/// healthy node** -- all of it aimed at a worker that was already not draining its store
/// queue. The retry is still wanted; repeating it per header arrival is not.
///
/// One second matches `Parameters::sync_retry_delay`'s own order for the worker-side retry,
/// so the two layers do not beat against each other.
const SYNCHRONIZE_RESEND_MIN_INTERVAL: Duration = Duration::from_millis(1_000);

/// Minimum gap between two `Synchronize` messages to the same worker that carry any RETRY
/// key. The per-key interval above bounds how often ONE key is repeated; it cannot bound
/// the MESSAGE rate, and that distinction is what the 2026-08-08 fix missed.
///
/// `sync_batches` runs once per arriving header. With a large missing set, at any instant
/// SOME key has passed its per-key interval, so nearly every call still emitted a message
/// -- measured 793-958 Synchronize/s sustained for 60s on the three wedged nodes, against
/// ~11/s healthy. The per-key gate was working exactly as specified; the specification
/// under-constrained the aggregate.
///
/// Only retries are gated. A NEWLY-pending key is always sent immediately: it gates
/// `LaneManager::set_payload_ready` and therefore the local cursor, so delaying it would
/// trade a bandwidth problem for a liveness one. Equal to the per-key interval on purpose
/// -- the two gates then coincide, and the steady state is one coalesced retry message per
/// worker per second carrying every key that is due, instead of ~800 messages/s carrying
/// whichever few happened to be due at each header arrival.
const RETRY_SYNCHRONIZE_MIN_INTERVAL: Duration = Duration::from_millis(1_000);

/// What `decide_synchronize` concluded for one `sync_batches` call.
pub(crate) struct SynchronizeDecision {
    /// Keys to put on the wire now, in the caller's original order.
    pub(crate) send: Vec<(Digest, WorkerId)>,
    /// Workers whose message carries at least one RETRY, and whose aggregate gate must
    /// therefore be closed. Deliberately not "every worker we sent to": a message of purely
    /// newly-pending keys must leave the gate open, or the next genuine retry waits an extra
    /// interval for nothing.
    pub(crate) carried_retry: HashSet<WorkerId>,
}

/// Apply both `Synchronize` rate limits. Pure, so the two bounds can be tested without a
/// `Wire`, a `Store` or a committee -- and so the decision can be read in one place instead
/// of inferred from a loop with two interleaved gates.
///
/// A newly-pending key always goes out: it gates `LaneManager::set_payload_ready` and hence
/// the local cursor. A retry must clear BOTH its own per-key interval and its worker's
/// aggregate gate. A retry held back by the aggregate gate is deliberately left UNSTAMPED by
/// the caller, so it stays due for the next permitted call -- stamping an unsent key would
/// defer it another full interval and silently turn a rate limit into dropped retries.
pub(crate) fn decide_synchronize(
    requested: &[(Digest, WorkerId)],
    newly: &HashSet<(Digest, WorkerId)>,
    last_synchronize: &HashMap<(Digest, WorkerId), Instant>,
    last_retry_synchronize: &HashMap<WorkerId, Instant>,
    now: Instant,
) -> SynchronizeDecision {
    // Resolved once per worker, before the key loop.
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

impl PayloadIo {
    /// Drop `last_synchronize` entries older than the per-key interval.
    ///
    /// Semantically free: the gate treats an ABSENT key and a key whose stamp is older than
    /// `SYNCHRONIZE_RESEND_MIN_INTERVAL` identically -- both read "due" -- so an entry that
    /// old carries no information. What it costs is memory, and without this the map is
    /// insert-only for the life of the process. Bounded afterwards by the keys actually
    /// touched within the last interval.
    fn prune_last_synchronize(&mut self, now: Instant) {
        if now.duration_since(self.last_synchronize_pruned_at) < SYNCHRONIZE_RESEND_MIN_INTERVAL {
            return;
        }
        self.last_synchronize
            .retain(|_, last| now.duration_since(*last) < SYNCHRONIZE_RESEND_MIN_INTERVAL);
        self.last_synchronize_pruned_at = now;
    }

    /// Publish both maps' sizes. Called wherever either changes, so neither gauge can go
    /// stale on a node that has stopped calling `sync_batches`.
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

    /// D1/§1: ask our own workers to sync `missing` batches for `author`'s block
    /// (`header_digest`), then spawn one `store.notify_read` waiter per newly-pending
    /// key. Repeated calls merge into the existing per-header set and may resend the
    /// worker request as a transport retry, but never create a second waiter for the
    /// same `(header_digest, digest, worker_id)`. Once *every* key for this header has
    /// resolved, the core calls `LaneManager::set_payload_ready`.
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

        // `missing` was produced by a fresh local-store probe, so re-sending a still-pending
        // entry is a legitimate retry for a lost primary-to-worker Synchronize -- but only at
        // a bounded rate, and TWO bounds are needed:
        //
        //   newly pending  -> always sent, immediately. It gates
        //                     `LaneManager::set_payload_ready` and so the local cursor.
        //   a retry        -> sent only if it clears its own per-key interval
        //                     (`SYNCHRONIZE_RESEND_MIN_INTERVAL`) AND this worker's
        //                     aggregate gate (`RETRY_SYNCHRONIZE_MIN_INTERVAL`).
        //
        // The per-key bound alone was not enough. `sync_batches` runs once per arriving
        // header, and with a large missing set some key is always past its interval, so
        // nearly every call still emitted a message: 793-958 Synchronize/s sustained on the
        // three nodes that wedged on 2026-08-08, against ~11/s healthy. Waiter creation
        // stays restricted to `newly_pending` regardless.
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

        // Close the aggregate gate only for workers that actually received a retry. A
        // worker whose gate was open but carried nothing but newly-pending keys must stay
        // open, or the next genuine retry waits an extra interval for no reason.
        for worker_id in decision.carried_retry {
            self.last_retry_synchronize.insert(worker_id, now);
        }

        self.prune_last_synchronize(now);
        self.publish_sizes();

        for (worker_id, digests) in by_worker {
            if let Some(addr) = wire.worker_addr(worker_id) {
                let bytes = bincode::serialize(&PrimaryWorkerMessage::Synchronize(digests, author))
                    .expect("serializes");
                wire.send_to_worker(addr, bytes, "Synchronize").await;
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

    /// Commit metric (Phase-2 parity, §9): forward the cursor's per-`WorkerId`
    /// notification to our own workers -- the existing worker-side observe path
    /// (`worker::synchronizer`) does the rest. Also (PHASE7-PREP-NOTES.md, paying down
    /// PHASE4-NOTES.md §6's scope cut) forwards each committed `Header` to the
    /// top-level application via `tx_output`, the same shape/tolerance as Autobahn's
    /// `Committer` (`primary/src/committer.rs`): a closed or full receiver is logged,
    /// not treated as fatal -- `node::main`'s `analyze` loop is a no-op consumer either
    /// way, and other assemblies' equivalent sends already tolerate this identically.
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

    /// A newly-pending key is never rate-limited: it gates payload readiness and so the
    /// local cursor, and delaying it would trade a bandwidth problem for a liveness one.
    #[test]
    fn newly_pending_keys_ignore_both_gates() {
        let now = Instant::now();
        let key = (digest(1), 0);
        let requested = vec![key.clone()];
        let newly: HashSet<_> = requested.iter().cloned().collect();
        // Both gates deliberately shut: same key sent a moment ago, worker gated too.
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

    /// A retry must clear its OWN interval.
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

    /// The aggregate bound: many due retries to one worker on one call are allowed, and
    /// they are COALESCED into that call rather than dribbled out per header arrival.
    #[test]
    fn due_retries_coalesce_into_one_permitted_call() {
        let now = Instant::now();
        let requested: Vec<_> = (1..=5).map(|b| (digest(b), 0u32)).collect();
        // Every key long overdue; worker gate open (absent).
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

    /// The aggregate bound is what the per-key bound could not provide: with the worker's
    /// gate shut, due retries are withheld even though each key's own interval has elapsed.
    /// This is the 793-958 msg/s case.
    #[test]
    fn aggregate_gate_withholds_due_retries_until_it_reopens() {
        let now = Instant::now();
        let requested: Vec<_> = (1..=5).map(|b| (digest(b), 0u32)).collect();
        let last_sync: HashMap<_, _> = requested
            .iter()
            .map(|k| (k.clone(), now - Duration::from_secs(10)))
            .collect();
        let mut last_retry = HashMap::new();
        last_retry.insert(0u32, now); // just sent a retry-carrying message

        let d = decide_synchronize(&requested, &HashSet::new(), &last_sync, &last_retry, now);
        assert!(
            d.send.is_empty(),
            "aggregate gate must bound the message rate"
        );
        assert!(d.carried_retry.is_empty());

        // ... and reopens once the interval passes, with nothing lost: the caller never
        // stamped the withheld keys, so they are still due.
        let later = now + RETRY_SYNCHRONIZE_MIN_INTERVAL;
        let d = decide_synchronize(&requested, &HashSet::new(), &last_sync, &last_retry, later);
        assert_eq!(d.send.len(), 5, "withheld retries must not be dropped");
    }

    /// Per-worker, not global: one worker being gated must not silence another.
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
