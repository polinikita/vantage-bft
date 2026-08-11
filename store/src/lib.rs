// Copyright(C) Facebook, Inc. and its affiliates.
use rocksdb::{
    BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, Options, WriteBatch,
    WriteOptions,
};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::time::{interval, Duration};

/// Pending writes flush every 100 ms. The interval bounds write frequency and batches
/// writes from all store commands received since the previous flush.
const FLUSH_INTERVAL_MS: u64 = 100;

/// Maximum pending write bytes per store instance. This bounds memory between flushes.
const MAX_PENDING_BYTES: usize = 32 * 1024 * 1024;

#[cfg(test)]
#[path = "tests/store_tests.rs"]
pub mod store_tests;

pub type StoreError = rocksdb::Error;
type StoreResult<T> = Result<T, StoreError>;

type Key = Vec<u8>;
type Value = Vec<u8>;

/// RocksDB tuning profile selected for the stored value size and write pattern.
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
    WriteMany(Vec<(Key, Value)>),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    /// Batched lookup preserving input order. Per-key read errors are returned as
    /// missing values so one failure does not invalidate the full request.
    ReadMany(Vec<Key>, oneshot::Sender<Vec<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Value>>),
}

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
    /// Epoch-millisecond stamp written after each completed actor-loop iteration.
    /// It measures actor liveness, including when the store is idle.
    heartbeat_millis: Arc<AtomicU64>,
    /// Monotonic count of commands removed from the channel. Use deltas to measure
    /// actor throughput and distinguish a full active queue from held permits.
    commands_drained: Arc<AtomicU64>,
}

/// Return wall-clock epoch milliseconds, or zero before the epoch.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Store {
    /// Open or create a metadata-profile store.
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
        let heartbeat_millis = Arc::new(AtomicU64::new(now_millis()));
        let heartbeat = heartbeat_millis.clone();
        let commands_drained = Arc::new(AtomicU64::new(0));
        let drained = commands_drained.clone();
        tokio::spawn(async move {
            // `pending` is a read-your-writes overlay. Reads consult it before RocksDB;
            // notify readers are resolved as soon as a value is accepted. Writes remain
            // process-local until `flush_pending` calls RocksDB.
            let mut pending: HashMap<Key, Value> = HashMap::new();
            let mut pending_bytes: usize = 0;
            // Burst behavior flushes immediately after a missed tick and avoids
            // permanently shifting the flush schedule after a slow operation.
            let mut ticker = interval(Duration::from_millis(FLUSH_INTERVAL_MS));

            loop {
                tokio::select! {
                    command = rx.recv() => {
                        // Count commands when they leave the channel.
                        drained.fetch_add(1, Ordering::Relaxed);
                        let Some(command) = command else {
                            // Channel closed: flush what we hold, then exit.
                            flush_pending(&db, &mut pending, &mut pending_bytes, &write_opts);
                            break;
                        };
                        match command {
                            StoreCommand::Write(key, value) => {
                                stage_write(
                                    &mut obligations,
                                    &mut pending,
                                    &mut pending_bytes,
                                    key,
                                    value,
                                );
                                if pending_bytes >= MAX_PENDING_BYTES {
                                    flush_pending(
                                        &db, &mut pending, &mut pending_bytes, &write_opts,
                                    );
                                }
                            }
                            StoreCommand::WriteMany(entries) => {
                                for (key, value) in entries {
                                    stage_write(
                                        &mut obligations,
                                        &mut pending,
                                        &mut pending_bytes,
                                        key,
                                        value,
                                    );
                                    if pending_bytes >= MAX_PENDING_BYTES {
                                        flush_pending(
                                            &db, &mut pending, &mut pending_bytes, &write_opts,
                                        );
                                    }
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
                                        // Preserve per-key error isolation; this slot stays
                                        // missing.
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
                // Update the heartbeat only after the selected operation completes.
                heartbeat.store(now_millis(), Ordering::Relaxed);
            }
        });
        Ok(Self {
            channel: tx,
            heartbeat_millis,
            commands_drained,
        })
    }

    /// Occupancy of the actor's bounded command channel (capacity `queue_capacity()`).
    ///
    /// Current channel occupancy, computed from its permit counts.
    pub fn queue_depth(&self) -> usize {
        self.channel.max_capacity() - self.channel.capacity()
    }

    /// Channel capacity used to compute occupancy.
    pub fn queue_capacity(&self) -> usize {
        self.channel.max_capacity()
    }

    /// Raw epoch-millisecond heartbeat. Callers compute age using their own clock.
    pub fn heartbeat_millis(&self) -> u64 {
        self.heartbeat_millis.load(Ordering::Relaxed)
    }

    /// Monotonic number of commands dequeued since construction.
    pub fn commands_drained(&self) -> u64 {
        self.commands_drained.load(Ordering::Relaxed)
    }

    pub async fn write(&mut self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    pub async fn write_many(&mut self, entries: Vec<(Key, Value)>) {
        if entries.is_empty() {
            return;
        }
        if let Err(e) = self.channel.send(StoreCommand::WriteMany(entries)).await {
            panic!("Failed to send WriteMany command to store: {}", e);
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

    /// Read many keys in input order. An empty request returns immediately. The whole
    /// request is processed by one store command, so all keys see the same snapshot.
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

/// Add one write to the read overlay and resolve matching waiters.
fn stage_write(
    obligations: &mut HashMap<Key, VecDeque<oneshot::Sender<StoreResult<Value>>>>,
    pending: &mut HashMap<Key, Value>,
    pending_bytes: &mut usize,
    key: Key,
    value: Value,
) {
    if let Some(mut senders) = obligations.remove(&key) {
        while let Some(sender) = senders.pop_front() {
            let _ = sender.send(Ok(value.clone()));
        }
    }

    let key_len = key.len();
    let value_len = value.len();
    if let Some(previous) = pending.insert(key, value) {
        *pending_bytes = pending_bytes.saturating_sub(key_len + previous.len());
    }
    *pending_bytes = pending_bytes.saturating_add(key_len + value_len);
}

/// Flush pending writes as one atomic RocksDB batch. A failed write aborts the process
/// because the store actor cannot safely continue after an I/O or corruption error.
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

/// Options shared by all store profiles.
fn db_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    // Raise the file-descriptor limit and cap open files.
    if let Ok(fdlimit::Outcome::LimitRaised { to, .. }) = fdlimit::raise_fd_limit() {
        opts.set_max_open_files((to / 8) as i32);
    }

    // Shard the table cache to reduce lock contention.
    opts.set_table_cache_num_shard_bits(10);

    // Use LZ4 for hot levels and Zstd for the bottommost level.
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    opts.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

    // Bound write buffers.
    opts.set_db_write_buffer_size(2 * 1024 * 1024 * 1024); // 2 GiB global limit.
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MiB per column family.
    opts.set_max_write_buffer_number(6);

    // Bound the WAL size.
    opts.set_max_total_wal_size(2 * 1024 * 1024 * 1024); // 2 GiB.

    // Configure RocksDB parallelism.
    opts.increase_parallelism(8);

    // Configure sync and I/O behavior.
    opts.set_use_fsync(false); // fdatasync is sufficient; writes also go through
                               // explicit WriteOptions::set_sync(false) below.
    opts.set_writable_file_max_buffer_size(64 * 1024 * 1024);

    // Shared compaction baseline.
    opts.set_target_file_size_base(128 * 1024 * 1024);

    // Enable write-path optimizations.
    opts.set_enable_pipelined_write(true);
    opts.set_memtable_prefix_bloom_ratio(0.02);

    opts
}

/// Block-based table options with a bloom filter and LRU cache.
fn block_options(block_cache_size_mb: usize, block_size_bytes: usize) -> BlockBasedOptions {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(block_size_bytes);
    block_opts.set_block_cache(&Cache::new_lru_cache(block_cache_size_mb << 20));
    // 10-bit bloom filter = ~1% false positive rate.
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts
}

/// Apply profile-specific compaction and block-cache settings.
fn apply_profile(opts: &mut Options, profile: StoreProfile) {
    match profile {
        StoreProfile::Metadata => {
            // Use level compaction with aligned L0 triggers.
            let l0_trigger = 4;
            opts.set_level_zero_file_num_compaction_trigger(l0_trigger);
            opts.set_level_zero_slowdown_writes_trigger(l0_trigger * 12);
            opts.set_level_zero_stop_writes_trigger(l0_trigger * 16);

            opts.set_block_based_table_factory(&block_options(128, 16 << 10));
        }
        StoreProfile::Data => {
            // Use universal compaction for append-heavy data.
            opts.set_compaction_style(DBCompactionStyle::Universal);
            opts.set_level_zero_file_num_compaction_trigger(80);
            opts.set_level_zero_slowdown_writes_trigger(96);
            opts.set_level_zero_stop_writes_trigger(128);

            opts.set_block_based_table_factory(&block_options(512, 128 << 10));
        }
    }
}
