// Copyright(C) Facebook, Inc. and its affiliates.
// Shared transaction-generating client for the standalone and local benchmark commands.
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

/// How bytes 17..size of the transaction payload are filled.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    AllZero,
    Random,
}

impl TransactionMode {
    pub fn parse(s: &str) -> Result<Self> {
        // Accept the hyphenated spelling for compatibility.
        match s.replace('-', "_").as_str() {
            "all_zero" => Ok(Self::AllZero),
            "random" => Ok(Self::Random),
            _ => Err(anyhow::anyhow!(
                "invalid --mode '{}': expected 'all_zero' or 'random'",
                s
            )),
        }
    }

    /// Return the canonical metric label.
    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            TransactionMode::AllZero => "all_zero",
            TransactionMode::Random => "random",
        }
    }
}

pub struct Client {
    pub target: SocketAddr, // Worker address.
    pub size: usize,        // Transaction size in bytes.
    pub rate: u64,
    pub nodes: Vec<SocketAddr>, // Addresses to await before sending.
    pub mode: TransactionMode,
    /// Epoch-millisecond time before which the client submits no transactions.
    /// Use the same value as `Parameters::metrics_active_at_ms`.
    pub activate_at_ms: Option<u64>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // Header is [1 B marker][8 B id, BE][8 B submission timestamp, LE] = 17 B
        // The extra marker byte keeps sample transaction metrics compatible.
        if self.size < 17 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 17 bytes (1 B marker + 8 B id + 8 B timestamp)",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Hold off submission until the metrics-active window opens (see
        // `activate_at_ms`). Deliberately AFTER the connect, so the TCP session is
        // already established when the window opens and the first transaction isn't
        // delayed by a handshake.
        if let Some(at) = self.activate_at_ms {
            let now_millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is before the unix epoch")
                .as_millis() as u64;
            if at > now_millis {
                let wait = at - now_millis;
                info!(
                    "Holding transaction submission for {} ms, until the \
                     metrics-active window opens at {} (epoch ms)",
                    wait, at
                );
                sleep(Duration::from_millis(wait)).await;
            } else {
                // Submit immediately when the activation time has passed.
                warn!(
                    "Metrics-active window at {} (epoch ms) already elapsed {} ms ago; \
                     submitting immediately -- the startup transient will be included \
                     in this run's latency distribution",
                    at,
                    now_millis - at
                );
            }
        }

        // Submit all transactions.
        //
        // Use a fractional accumulator so the average rate remains exact.
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

            // Emit floor or ceil of rate / precision on each tick.
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
