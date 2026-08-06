// Copyright(C) Facebook, Inc. and its affiliates.
// Transaction-generating client, extracted from `benchmark_client.rs` (PHASE2-SPEC.md
// §8) so both the standalone `benchmark_client` binary and the in-process
// `local-benchmark` subcommand share exactly one implementation.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// How the transaction payload (bytes 17..size) is filled. Mirrors starfish's
/// `TransactionMode` (`all_zero` is upstream-equivalent; `random` is the honest mode that
/// defeats accidental compression/dedup anywhere in the stack). `all_zero` (snake_case)
/// is the starfish-aligned canonical wire spelling; `parse` also accepts the legacy
/// `all-zero` (hyphen) spelling as an alias.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    AllZero,
    Random,
}

impl TransactionMode {
    pub fn parse(s: &str) -> Result<Self> {
        // Legacy hyphen spelling ("all-zero") accepted as an alias; starfish-aligned
        // snake_case ("all_zero") is canonical.
        match s.replace('-', "_").as_str() {
            "all_zero" => Ok(Self::AllZero),
            "random" => Ok(Self::Random),
            _ => Err(anyhow::anyhow!(
                "invalid --mode '{}': expected 'all_zero' or 'random'",
                s
            )),
        }
    }

    /// METRICS-DASHBOARD-SPEC.md §8: canonical string label for `transaction_mode_info`
    /// -- the exact (canonical) strings `--mode` already accepts. Only called from
    /// `local_benchmark.rs`, which is compiled into the `node` binary target, not
    /// `benchmark_client` (both share this file via `#[path = "client.rs"]`, so
    /// clippy's per-binary dead-code analysis flags it as unused from
    /// `benchmark_client`'s own compilation unit) -- genuinely used from the `node`
    /// binary, not dead code.
    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            TransactionMode::AllZero => "all_zero",
            TransactionMode::Random => "random",
        }
    }
}

pub struct Client {
    pub target: SocketAddr, //specifies the worker to connect to
    pub size: usize,        //specifies the bit size of transactions
    pub rate: u64,
    pub nodes: Vec<SocketAddr>, //specifies the addresses of all nodes. Currently only used to wait for them to be alive, but also necessary if we wanted to receive result replies (from any node).
    pub mode: TransactionMode,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // Header is [1 B marker][8 B id, BE][8 B submission timestamp, LE] = 17 B
        // (starfish's own header is 16 B; we keep the extra marker byte so the legacy
        // sample-tx cross-validation metric keeps working byte-identically).
        if self.size < 17 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 17 bytes (1 B marker + 8 B id + 8 B timestamp)",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Submit all transactions.
        //
        // Fable audit item 2 originally guarded only the `rate < PRECISION` case, where
        // `rate / PRECISION` truncates to 0 and the loop would silently send nothing
        // forever. But that same truncation loses throughput at EVERY rate that is not a
        // multiple of `PRECISION`, not only sub-grid ones: at 20 nodes and an aggregate
        // 1000 tx/s each client's rate is 50, `50 / 20 == 2`, and the achieved rate is
        // `2 * 20 == 40` tx/s per client -- 800 aggregate, a silent 20% shortfall against
        // the requested 1000. Only multiples of 400 were reachable at n=20.
        //
        // So the fractional accumulator below is now the ONLY path, at every rate. It is
        // a standard fixed-point (Bresenham) rate limiter: it reproduces the old `burst`
        // value exactly whenever `rate` divides evenly by `PRECISION`, and otherwise
        // alternates between floor and ceil so the one-second average is exactly `rate`.
        // At rate 50 it emits 2,3,2,3,... per tick -- 50 tx/s, not 40.
        //
        // `this_tick_count` may legitimately be 0 on a given tick for a sub-grid rate;
        // the `for x in 0..this_tick_count` loop below then simply does not execute, so
        // the `counter % this_tick_count` inside it is never evaluated at zero.
        if self.rate > 0 && self.rate < PRECISION {
            warn!(
                "Per-client rate {} tx/s is below the sampling precision ({} ticks/s), so \
                 pacing is inherently bursty (~1 tx roughly every {} ms); the one-second \
                 average is still exactly {} tx/s.",
                self.rate,
                PRECISION,
                (PRECISION as f64 / self.rate as f64 * BURST_DURATION as f64).round() as u64,
                self.rate,
            );
        }
        let mut sub_burst_carry: u64 = 0;
        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            // One fixed-point accumulator for every rate (see the comment above the
            // `sub_burst_carry` declaration). Emits floor or ceil of `rate / PRECISION`
            // on each tick so the one-second average is exactly `rate`; identical to the
            // old unconditional `burst` whenever `rate` divides evenly by `PRECISION`.
            let this_tick_count = {
                sub_burst_carry += self.rate;
                let n = sub_burst_carry / PRECISION;
                sub_burst_carry %= PRECISION;
                n
            };

            for x in 0..this_tick_count {
                if x == counter % this_tick_count {
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", counter);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(counter); // This counter identifies the tx.
                } else {
                    r += 1;
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Ensures all clients send different txs.
                };

                // Starfish-parity embedded submission timestamp (bytes 9..17, UTC millis,
                // LE) -- every tx gets one, samples included.
                let now_millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is before the UNIX epoch")
                    .as_millis() as u64;
                tx.put_u64_le(now_millis);

                // Fill the remaining payload (bytes 17..size) per the configured mode.
                match self.mode {
                    TransactionMode::AllZero => tx.resize(self.size, 0u8), //Truncate any bits past size
                    TransactionMode::Random => {
                        let mut payload = vec![0u8; self.size - tx.len()];
                        rand::thread_rng().fill(&mut payload[..]);
                        tx.put_slice(&payload);
                    }
                }
                let bytes = tx.split().freeze(); //split() moves byte content from tx to bytes (i.e. avoids copy). freeze() makes it const so it can be shared. (bytes can now be used/sent async)
                                                 //Note: Does not sign transactions. Transaction id-s are not unique w.r.t to content.
                if let Err(e) = transport.send(bytes).await {
                    //Uses TCP connection to send request to assigned worker. Note: Optimistically only sending to one worker.
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
