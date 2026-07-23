// Copyright(C) Facebook, Inc. and its affiliates.
mod error;
mod receiver;
mod reliable_sender;
mod simple_sender;

#[cfg(test)]
#[path = "tests/common.rs"]
pub mod common;

pub use crate::receiver::{MessageHandler, Receiver, Writer};
pub use crate::reliable_sender::{CancelHandler, ReliableSender};
pub use crate::simple_sender::SimpleSender;

use std::sync::atomic::{AtomicU64, Ordering};

/// PHASE7-PREP-NOTES.md (optional harness addition): a starfish-style artificial
/// per-send delay (`starfish`'s `mimic_extra_latency`/`uniform_latency_ms` pattern in
/// `~/code/starfish/crates/starfish-core/src/network.rs`, read-only reference),
/// deliberately reduced to the single knob `local-benchmark --mimic-latency-ms`
/// actually needs: one process-wide fixed delay (ms), default 0 = off (current
/// behavior, byte-identical), applied uniformly to every outbound send on both
/// senders below. Not starfish's full per-connection geodistributed latency table
/// (adversarial ramp, per-peer randomized jitter) -- out of scope for a harness-only
/// diagnostic knob; a real per-peer table would be a much larger, separate change.
/// Global rather than threaded through every `ReliableSender`/`SimpleSender`/
/// `Primary::spawn`/`Worker::spawn` call site (all unchanged) -- a single process-wide
/// benchmark run has exactly one intended mimic delay, so a `static` avoids a
/// signature change rippling through every spawn path for a diagnostic-only knob.
static MIMIC_LATENCY_MS: AtomicU64 = AtomicU64::new(0);

/// Sets the process-wide artificial per-send delay. Call once, before spawning any
/// node (`local-benchmark`'s own startup, if `--mimic-latency-ms` > 0); every
/// `ReliableSender`/`SimpleSender` connection already running (or spawned after) reads
/// the current value on every send, so later calls take effect immediately too.
pub fn set_mimic_latency_ms(ms: u64) {
    MIMIC_LATENCY_MS.store(ms, Ordering::Relaxed);
}

/// Reads the current artificial per-send delay; `Duration::ZERO` (no sleep) when unset
/// (the default) -- current behavior, unchanged.
fn mimic_latency() -> std::time::Duration {
    std::time::Duration::from_millis(MIMIC_LATENCY_MS.load(Ordering::Relaxed))
}
