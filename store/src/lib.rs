// Copyright(C) Facebook, Inc. and its affiliates.
use rocksdb::{
    BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, Options, WriteBatch,
    WriteOptions,
};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::time::{interval, Duration};

/// Flush cadence for the pending write batch: 50 ms => 20 `write_opt` calls per second
/// per `Store` instance. A validator process tree holds one primary store plus one
/// store per worker (K=1 in every benchmark configuration), so **40 batch writes per
/// second per validator**, from two independent tickers on the same device. That rate is
/// load-INDEPENDENT: it is set by the timer, not by traffic.
///
/// Reference point in `~/code/iota/crates/starfish`: `DagState::flush`
/// (`core/src/dag_state.rs:2481`) issues one `WriteBatch` per own block
/// (`core/src/core.rs:1212`) plus one per committed-leader batch
/// (`core/src/commit_observer.rs:228`). The latter is reached from `try_commit` ONLY
/// when leaders were actually sequenced -- the loop `break`s on
/// `sequenced_leaders.is_empty()` -- so it tracks the commit cadence, not the
/// peer-block arrival rate, and it coalesces (`handle_committed_leaders` takes a
/// `Vec`), so it is bounded ABOVE by the round rate rather than equal to it. Both
/// triggers are therefore per-round: `min_block_delay = 50 ms` only caps the round
/// rate at 20/s, and the achieved rate in production is ~10 rounds/s, giving
/// **<= 20 write operations/s per validator**, one DB, `sync=false`, no explicit
/// memtable flush.
///
/// This store is at 40/s per validator (20/s x 2 instances), i.e. ~2x starfish rather
/// than parity, and the gap is structural: starfish has ONE DB where a validator here
/// has two processes with two RocksDB directories. Halving the tickers to 100 ms would
/// match starfish exactly, but it would also double the single batch that can block
/// `vantage::lanes::missing_payload` behind it in the 100-slot channel -- the very
/// critical-path stall this batching exists to shorten -- and 40 vs 20 batched writes
/// per second is far below any rate that shows up on the device. The factor of 2 is
/// deliberate; see the audit trail in `~/vantage-measurements/2026-08-01/HANDOFF.md`
/// section 30.
///
/// Before batching, this store issued one `put_opt` per key. Every validator persists
/// its own AND all n-1 peers' batches, and the client's burst grid is 50 ms
/// (`node/src/client.rs:65`) so each node seals ~20 batches/s: ~780 `put_opt`/s per
/// validator at n=20 and ~1 980/s at n=50, i.e. 40-100x more disk write operations than
/// starfish for the same work. Note this changes the number of write OPERATIONS, not
/// the number of key-level puts (unchanged) or the bytes written (unchanged).
const FLUSH_INTERVAL_MS: u64 = 50;

/// Memory bound on the un-flushed batch, not a rate knob: a safety valve for load
/// spikes so `pending` cannot grow without limit between ticks. At the measured
/// worst case (~128 MB/s of batch bytes per node at n=50 / 250k tx/s) a 50 ms window
/// holds ~6.4 MB, 5x under this threshold, so the ticker -- not this valve -- governs
/// the flush rate; it would take ~670 MB/s per store to invert that.
///
/// PER INSTANCE, so the bound on a one-process-per-validator deployment (docker, AWS,
/// `benchmark/`) is 2 x 32 MiB. `node/src/local_benchmark.rs` runs every node in ONE
/// process, where the aggregate worst case is `live_nodes x (1 + workers) x 32 MiB`
/// (3.2 GiB at n=50) -- unreachable in practice at the rates above, but the reason this
/// constant is not raised further.
const MAX_PENDING_BYTES: usize = 32 * 1024 * 1024;

#[cfg(test)]
#[path = "tests/store_tests.rs"]
pub mod store_tests;

pub type StoreError = rocksdb::Error;
type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;

/// RocksDB tuning profile (PHASE2-SPEC.md #7), ported from starfish
/// (`~/code/starfish/crates/starfish-core/src/rocks_store.rs`). Starfish splits one DB
/// into metadata-vs-bulk-data column families; this artifact already separates the same
/// concerns into per-component `Store` *instances* (primary's one store for
/// headers/certs/payload markers, each worker's own store for batch bytes), so starfish's
/// two per-CF profiles map onto the two `Store::new_with_profile` call sites instead of
/// column families. Each DB still has a single (default) column family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreProfile {
    /// Small values, point lookups (headers, certificates, payload digest markers):
    /// level compaction, smaller blocks, smaller cache. Used by the primary store.
    Metadata,
    /// Large, append-heavy values (worker batch bytes): universal compaction, larger
    /// blocks, larger cache. Used by worker stores.
    Data,
}

pub enum StoreCommand {
    Write(Key, Value),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    /// Batched point lookup: one command, one `multi_get`, one reply for N keys.
    /// Exists because the consensus hot path probes every payload digest of an
    /// inbound block (up to `max_block_payload`), and issuing those as N separate
    /// `Read`s serialized N round-trips through this single actor -- each one
    /// queued behind whatever writes were already in the 100-slot channel.
    /// Errors are per KEY, not per request: a key whose lookup fails is reported as
    /// `None` (absent) and logged, exactly as N separate `Read`s followed by the
    /// callers' `unwrap_or(None)` behaved before this command existed. Failing the
    /// whole request instead would turn one bad key into "every key of this block is
    /// missing", amplifying a transient read error into up to `max_block_payload`
    /// spurious batch-sync requests.
    ReadMany(Vec<Key>, oneshot::Sender<Vec<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Value>>),
}

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    /// Opens (or creates) the store at `path` with the `Metadata` tuning profile. Kept
    /// as the default entry point so every existing call site / test fixture is
    /// unaffected by the Phase-2 tuning work; callers that want the `Data` profile use
    /// `new_with_profile` explicitly.
    pub fn new(path: &str) -> StoreResult<Self> {
        Self::new_with_profile(path, StoreProfile::Metadata)
    }

    pub fn new_with_profile(path: &str, profile: StoreProfile) -> StoreResult<Self> {
        let mut opts = db_options();
        apply_profile(&mut opts, profile);

        let db = rocksdb::DB::open(&opts, path)?;

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false);

        log::debug!("Opened store at {} with profile {:?}", path, profile);

        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            // Writes accumulate in `pending` and land as ONE RocksDB WriteBatch per
            // flush: 1000/FLUSH_INTERVAL_MS = 20 `write_opt` calls per second instead of
            // one per key. `pending` doubles as a read-your-writes overlay: every Read /
            // ReadMany / NotifyRead consults it BEFORE RocksDB, and `notify_read`
            // obligations are resolved the moment the value is accepted, so deferring
            // the disk write is invisible to every caller WITHIN a process lifetime.
            //
            // ACROSS lifetimes it is not: `set_sync(false)` still put each WAL record in
            // the kernel page cache before `put_opt` returned, where it survived a
            // SIGKILL/`docker kill` and was replayed by WAL recovery on the next
            // `DB::open`; a `HashMap` in this task's frame does not, and nothing here
            // flushes on a signal (no handler exists) or on runtime drop (tokio drops
            // tasks without polling). That IS a durability-class change, from
            // page-cache-deep to process-memory-deep, for a window of <= 50 ms. It is
            // unobservable in this artifact for a specific reason, not because
            // `sync=false` made it free: no code path ever reads store state written by
            // a PREVIOUS process lifetime. There is no replay-from-disk and no scan --
            // `db` is private to this actor, `StoreCommand` has no iterator or delete
            // variant, and every key ever read is a digest that arrived over the network
            // in the same lifetime. Vantage persists no consensus state at all (headers,
            // blocks, acks, echoes, readys, control log and the replay outbox are
            // in-memory), `--crash` leaves nodes unspawned rather than restarting them,
            // and the benchmark harness wipes the DB directories before every run.
            let mut pending: HashMap<Key, Value> = HashMap::new();
            let mut pending_bytes: usize = 0;
            // Deliberately the DEFAULT `MissedTickBehavior::Burst`. `Delay` would reset
            // the deadline to (tick completion + 50 ms) after any branch body that ran
            // long -- a stalled `write_opt`, a cold `db.get` -- so the effective period
            // would become (stall + 50 ms) and the schedule would drift permanently away
            // from "flush at least every 50 ms". With `Burst` the first missed deadline
            // fires immediately and flushes the backlog, and each additional catch-up
            // tick is a free no-op thanks to `flush_pending`'s `is_empty` guard.
            let mut ticker = interval(Duration::from_millis(FLUSH_INTERVAL_MS));

            loop {
                tokio::select! {
                    command = rx.recv() => {
                        let Some(command) = command else {
                            // Channel closed: flush what we hold, then exit.
                            flush_pending(&db, &mut pending, &mut pending_bytes, &write_opts);
                            break;
                        };
                        match command {
                            StoreCommand::Write(key, value) => {
                                // ORDERING HAZARD: obligations are drained BEFORE the
                                // insert, inverting the pre-batching order (`put_opt`
                                // came first). That is safe only because this whole arm
                                // is AWAIT-FREE: a woken waiter's follow-up `Read` has
                                // to traverse the mpsc channel, and cannot be dequeued
                                // until this arm returns, so read-after-notify is
                                // guaranteed by FIFO rather than by scheduling -- true
                                // even on a multi-thread runtime where `oneshot::send`
                                // can resume the waiter on another core immediately.
                                // `worker::synchronizer` relies on exactly this. Adding
                                // any `.await` between here and the insert below breaks
                                // it silently.
                                if let Some(mut senders) = obligations.remove(&key) {
                                    while let Some(s) = senders.pop_front() {
                                        let _ = s.send(Ok(value.clone()));
                                    }
                                }
                                pending_bytes += key.len() + value.len();
                                pending.insert(key, value);
                                // Safety valve only -- at benchmark rates the ticker
                                // fires long before this, so the rate stays at
                                // 1000/FLUSH_INTERVAL_MS = 20 batch writes per second.
                                if pending_bytes >= MAX_PENDING_BYTES {
                                    flush_pending(
                                        &db, &mut pending, &mut pending_bytes, &write_opts,
                                    );
                                }
                            }
                            StoreCommand::Read(key, sender) => {
                                let response = match pending.get(&key) {
                                    Some(value) => Ok(Some(value.clone())),
                                    None => db.get(&key),
                                };
                                let _ = sender.send(response);
                            }
                            StoreCommand::ReadMany(keys, sender) => {
                                // `multi_get` preserves input order 1:1; the pending
                                // overlay is applied per index so the reply order still
                                // matches `keys` exactly.
                                let mut out: Vec<Option<Value>> = Vec::with_capacity(keys.len());
                                let mut miss_idx = Vec::new();
                                let mut miss_keys = Vec::new();
                                for (i, key) in keys.iter().enumerate() {
                                    match pending.get(key) {
                                        Some(value) => out.push(Some(value.clone())),
                                        None => {
                                            out.push(None);
                                            miss_idx.push(i);
                                            miss_keys.push(key);
                                        }
                                    }
                                }
                                for (slot, got) in
                                    miss_idx.iter().zip(db.multi_get(&miss_keys))
                                {
                                    match got {
                                        Ok(value) => out[*slot] = value,
                                        // Per-key isolation -- see `ReadMany`'s doc
                                        // comment. The slot stays `None`.
                                        Err(e) => log::error!(
                                            "Store read failed for one key of a batch of {}: {}",
                                            keys.len(),
                                            e
                                        ),
                                    }
                                }
                                let _ = sender.send(out);
                            }
                            StoreCommand::NotifyRead(key, sender) => {
                                let response = match pending.get(&key) {
                                    Some(value) => Ok(Some(value.clone())),
                                    None => db.get(&key),
                                };
                                match response {
                                    Ok(None) => obligations
                                        .entry(key)
                                        .or_insert_with(VecDeque::new)
                                        .push_back(sender),
                                    _ => {
                                        let _ = sender.send(response.map(|x| x.unwrap()));
                                    }
                                }
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        flush_pending(&db, &mut pending, &mut pending_bytes, &write_opts);
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    pub async fn read(&mut self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read command from store")
    }

    /// Batched counterpart of `read`: one actor round-trip for N keys, results in the
    /// SAME order as `keys`, with an unreadable key reported as `None` (see
    /// `StoreCommand::ReadMany`). An empty `keys` short-circuits without touching the
    /// store.
    ///
    /// Snapshot semantics differ from N sequential `read`s in one way worth knowing:
    /// this is ONE command on a single-task actor, so a `Write` accepted while the batch
    /// is in flight is visible either to every key or to none, never to a suffix. The
    /// only caller (`vantage::lanes::missing_payload`) is unaffected -- a marker landing
    /// mid-probe just means one more entry is reported missing, and the resulting
    /// `SyncBatches` resolves immediately from the overlay.
    pub async fn read_many(&mut self, keys: Vec<Key>) -> Vec<Option<Value>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::ReadMany(keys, sender))
            .await
        {
            panic!("Failed to send ReadMany command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to ReadMany command from store")
    }

    pub async fn notify_read(&mut self, key: Key) -> StoreResult<Value> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyRead(key, sender))
            .await
        {
            panic!("Failed to send NotifyRead command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyRead command from store")
    }
}

/// Write everything buffered in `pending` as ONE atomic RocksDB batch and clear it.
/// A no-op when nothing is buffered, mirroring starfish's `has_data_to_write` early
/// return so idle nodes issue no disk writes at all (the 20 Hz task wakeup remains).
///
/// Aborts the process if the write fails. Batching makes the alternatives untenable:
/// pre-batching, a failed `put_opt` was silently ignored and lost ONE key; silently
/// ignoring a failed batch would lose up to `MAX_PENDING_BYTES` at once, and because
/// nothing in this codebase ever rewrites a key, every `notify_read` waiting on a lost
/// key would hang until its own staleness pruner cancelled it while `db.get` kept
/// serving the pre-write value. Retrying instead of clearing is no better: a
/// permanently-unwritable DB would then grow `pending` without bound, since the valve's
/// own flush attempt fails too.
///
/// `abort` rather than `panic!` because this runs on a DETACHED `tokio::spawn`ed task
/// whose `JoinHandle` is dropped: a panic would unwind only the actor, leaving the node
/// running against a dead store with every later `Store::write` panicking on the closed
/// channel in whatever task happened to call it. Production starfish does
/// `.unwrap_or_else(|e| panic!("Failed to write to storage: {e:?}"))`
/// (`~/code/iota/crates/starfish/core/src/dag_state.rs:2548`), but its flush runs on the
/// core thread, so that panic genuinely takes the node down; `abort` is what reproduces
/// that here. `write_opt` with `sync=false` fails only on genuine I/O or corruption
/// errors, which is not a recoverable condition for a validator.
fn flush_pending(
    db: &rocksdb::DB,
    pending: &mut HashMap<Key, Value>,
    pending_bytes: &mut usize,
    write_opts: &WriteOptions,
) {
    if pending.is_empty() {
        return;
    }
    let mut batch = WriteBatch::default();
    for (key, value) in pending.drain() {
        batch.put(key, value);
    }
    *pending_bytes = 0;
    db.write_opt(batch, write_opts).unwrap_or_else(|e| {
        log::error!("Failed to write store batch to storage, aborting: {}", e);
        std::process::abort();
    });
}

/// DB-wide options shared by both profiles (starfish `rocks_store.rs::open()`, the
/// settings that apply once per DB rather than per column family).
fn db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    // Raise fd limit and cap open files to avoid "too many open files" errors.
    if let Ok(fdlimit::Outcome::LimitRaised { to, .. }) = fdlimit::raise_fd_limit() {
        opts.set_max_open_files((to / 8) as i32);
    }

    // Table cache sharding to reduce lock contention.
    opts.set_table_cache_num_shard_bits(10);

    // Compression: LZ4 for hot levels (fast), Zstd for bottommost (compact).
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    opts.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

    // Write buffer settings.
    opts.set_db_write_buffer_size(2 * 1024 * 1024 * 1024); // 2 GiB global limit.
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MiB per (the one) CF.
    opts.set_max_write_buffer_number(6);

    // WAL limit.
    opts.set_max_total_wal_size(2 * 1024 * 1024 * 1024); // 2 GiB.

    // Parallelism.
    opts.increase_parallelism(8);

    // Sync and I/O settings.
    opts.set_use_fsync(false); // fdatasync is sufficient; writes also go through
                               // explicit WriteOptions::set_sync(false) below.
    opts.set_writable_file_max_buffer_size(64 * 1024 * 1024);

    // Compaction tuning (shared baseline; profile-specific L0 triggers/style below).
    opts.set_target_file_size_base(128 * 1024 * 1024);

    // Write performance.
    opts.set_enable_pipelined_write(true);
    opts.set_memtable_prefix_bloom_ratio(0.02);

    opts
}

/// Block-based table options with bloom filter and LRU cache (starfish
/// `rocks_store.rs::block_options`).
fn block_options(block_cache_size_mb: usize, block_size_bytes: usize) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(block_size_bytes);
    block_opts.set_block_cache(&Cache::new_lru_cache(block_cache_size_mb << 20));
    // 10-bit bloom filter = ~1% false positive rate.
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts
}

/// Profile-specific deltas (starfish `metadata_cf_options` / `data_cf_options`), applied
/// on top of `db_options()` -- there being no column families here, both the shared and
/// the per-profile settings land on the same (default CF's) `Options`.
fn apply_profile(opts: &mut Options, profile: StoreProfile) {
    match profile {
        StoreProfile::Metadata => {
            // Level compaction (rocksdb's default style -- no explicit call needed)
            // with aligned L0 triggers.
            let l0_trigger = 4;
            opts.set_level_zero_file_num_compaction_trigger(l0_trigger);
            opts.set_level_zero_slowdown_writes_trigger(l0_trigger * 12);
            opts.set_level_zero_stop_writes_trigger(l0_trigger * 16);

            opts.set_block_based_table_factory(&block_options(128, 16 << 10));
        }
        StoreProfile::Data => {
            // Universal compaction for append-heavy bulk data.
            opts.set_compaction_style(DBCompactionStyle::Universal);
            opts.set_level_zero_file_num_compaction_trigger(80);
            opts.set_level_zero_slowdown_writes_trigger(96);
            opts.set_level_zero_stop_writes_trigger(128);

            opts.set_block_based_table_factory(&block_options(512, 128 << 10));
        }
    }
}
