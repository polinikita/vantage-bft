// Copyright(C) Facebook, Inc. and its affiliates.
// Shared transaction-generating client.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// How payload bytes after the 17-byte header are filled.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    AllZero,
    Random,
}

impl TransactionMode {
    pub fn parse(s: &str) -> Result<Self> {
        match s.replace('-', "_").as_str() {
            "all_zero" => Ok(Self::AllZero),
            "random" => Ok(Self::Random),
            _ => Err(anyhow::anyhow!(
                "invalid --mode '{}': expected 'all_zero' or 'random'",
                s
            )),
        }
    }

    /// Returns the canonical metric label.
    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            TransactionMode::AllZero => "all_zero",
            TransactionMode::Random => "random",
        }
    }
}

pub struct Client {
    pub target: SocketAddr,
    pub size: usize,
    pub rate: u64,
    pub nodes: Vec<SocketAddr>,
    pub mode: TransactionMode,
    /// Whether generated transactions count as benchmark goodput. Uncounted
    /// payload still traverses and consumes the complete protocol data path.
    pub counted: bool,
    /// Epoch-millisecond time before which the client submits no transactions.
    /// Use the same value as `Parameters::metrics_active_at_ms`.
    pub activate_at_ms: Option<u64>,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20;
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // Header: marker, big-endian ID, little-endian submission timestamp.
        if self.size < 17 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 17 bytes (1 B marker + 8 B id + 8 B timestamp)",
            ));
        }

        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

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
                warn!(
                    "Metrics-active window at {} (epoch ms) already elapsed {} ms ago; \
                     submitting immediately -- the startup transient will be included \
                     in this run's latency distribution",
                    at,
                    now_millis - at
                );
            }
        }

        // Use a fractional accumulator to preserve the average rate.
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
        let mut rng = StdRng::from_entropy();
        let mut random_id: u64 = rng.gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // Parsed by benchmark tooling.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            let this_tick_count = {
                sub_burst_carry += self.rate;
                let n = sub_burst_carry / PRECISION;
                sub_burst_carry %= PRECISION;
                n
            };

            for x in 0..this_tick_count {
                if !self.counted {
                    random_id = random_id.wrapping_add(1);
                    tx.put_u8(2u8);
                    tx.put_u64(random_id);
                } else if x == counter % this_tick_count {
                    debug!("Sending sample transaction {}", counter);

                    tx.put_u8(0u8);
                    tx.put_u64(counter);
                } else {
                    random_id = random_id.wrapping_add(1);
                    tx.put_u8(1u8);
                    tx.put_u64(random_id);
                };

                // Store the submission timestamp in bytes 9..17, in UTC milliseconds.
                let now_millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is before the UNIX epoch")
                    .as_millis() as u64;
                tx.put_u64_le(now_millis);

                match self.mode {
                    TransactionMode::AllZero => tx.resize(self.size, 0u8),
                    TransactionMode::Random => {
                        let payload_start = tx.len();
                        tx.resize(self.size, 0u8);
                        rng.fill(&mut tx[payload_start..]);
                    }
                }
                let bytes = tx.split().freeze();
                if let Err(e) = transport.send(bytes).await {
                    warn!("Failed to send transaction: {}", e);
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // Parsed by benchmark tooling.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
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
