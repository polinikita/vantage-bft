// Copyright(C) Facebook, Inc. and its affiliates.
use rocksdb::{
    BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, Options, WriteBatch,
    WriteOptions,
};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::time::{interval, Duration, MissedTickBehavior};

/// Flush cadence for the pending write batch. 50 ms => ~20 flushes/s, matching
/// production starfish, which writes one application-level batch per protocol event
/// (own proposal per round + per commit batch) rather than one per item. Before
/// batching, this store issued one `put_opt` per key: ~2 500 writes/s per validator at
/// n=50 (every worker persists its own AND all n-1 peers' batches, ~50 seals/s each),
/// i.e. two orders of magnitude more disk operations than starfish for the same work.
const FLUSH_INTERVAL_MS: u64 = 50;

/// Memory bound on the un-flushed batch, not a rate knob: a safety valve for load
/// spikes so `pending` cannot grow without limit between ticks. At the measured
/// worst case (~128 MB/s of batch bytes per node at n=50 / 250k tx/s) a 50 ms window
/// holds ~6.4 MB, so the ticker -- not this threshold -- governs the flush rate.
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
    ReadMany(Vec<Key>, oneshot::Sender<StoreResult<Vec<Option<Value>>>>),
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
            // flush, so the store issues ~1000/FLUSH_INTERVAL_MS writes per second
            // instead of one per key. This mirrors production starfish, which flushes a
            // single application-level batch per protocol event (~20x/s) rather than
            // per item. `pending` doubles as a read-your-writes overlay: every Read /
            // ReadMany / NotifyRead consults it BEFORE RocksDB, so deferring the disk
            // write is invisible to callers, and `notify_read` obligations are still
            // resolved the moment the value is accepted (durability was already only
            // page-cache-deep -- `set_sync(false)` -- so this changes no guarantee).
            let mut pending: HashMap<Key, Value> = HashMap::new();
            let mut pending_bytes: usize = 0;
            let mut ticker = interval(Duration::from_millis(FLUSH_INTERVAL_MS));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

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
                                if let Some(mut senders) = obligations.remove(&key) {
                                    while let Some(s) = senders.pop_front() {
                                        let _ = s.send(Ok(value.clone()));
                                    }
                                }
                                pending_bytes += key.len() + value.len();
                                pending.insert(key, value);
                                // Safety valve only -- at benchmark rates the ticker
                                // fires long before this, so the flush rate stays at
                                // 1000/FLUSH_INTERVAL_MS per second.
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
                                let mut failure = None;
                                if !miss_keys.is_empty() {
                                    for (slot, got) in
                                        miss_idx.iter().zip(db.multi_get(&miss_keys))
                                    {
                                        match got {
                                            Ok(value) => out[*slot] = value,
                                            Err(e) => {
                                                failure = Some(e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                let _ = sender.send(match failure {
                                    Some(e) => Err(e),
                                    None => Ok(out),
                                });
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
    /// SAME order as `keys`. An empty `keys` short-circuits without touching the store.
    pub async fn read_many(&mut self, keys: Vec<Key>) -> StoreResult<Vec<Option<Value>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
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
/// return so idle nodes issue no disk writes at all. Still `sync=false`: this changes
/// the number of write operations, not the durability class.
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
    if let Err(e) = db.write_opt(batch, write_opts) {
        log::error!("Failed to flush store write batch: {}", e);
    }
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
