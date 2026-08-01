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
use std::collections::{HashMap, HashSet};
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
}

impl PayloadIo {
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

        // `missing` was produced by a fresh local-store probe. Resend every unique
        // still-missing entry, including one already pending, so a duplicate accepted
        // SERVE can retry a lost primary-to-worker Synchronize message. Only waiter
        // creation is restricted to `newly_pending` below.
        let mut by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        for (digest, worker_id) in &requested {
            by_worker
                .entry(*worker_id)
                .or_default()
                .push(digest.clone());
        }
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
