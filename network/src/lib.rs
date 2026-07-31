// Copyright(C) Facebook, Inc. and its affiliates.
mod batch;
mod blip;
mod error;
mod receiver;
mod reliable_sender;
mod simple_sender;

#[cfg(test)]
#[path = "tests/common.rs"]
pub mod common;

pub use crate::batch::BatchConfig;
pub use crate::blip::BlipGate;
pub use crate::receiver::{MessageHandler, Receiver, Writer};
pub use crate::reliable_sender::{CancelHandler, ReliableSender};
pub use crate::simple_sender::SimpleSender;

// PHASE7-PREP-NOTES.md (WAN-shaped local runs, optional item): per-destination
// artificial send latency, starfish-style (reference, read-only:
// `~/code/starfish/crates/starfish-core/src/network.rs`'s `generate_latency_table` +
// its per-connection `extra_connection_latency` field/application). `ReliableSender`/
// `SimpleSender` each carry an OPTIONAL `HashMap<SocketAddr, Duration>` (default
// empty), set once via `with_latency(..)` right after construction; a `Connection`
// spawned for a given address looks up its own entry ONCE at spawn time and sleeps it
// (a no-op when zero/absent) immediately before every real `writer.send(..)` for the
// rest of that connection's life. This is per-connection, exactly like starfish's own
// injection point: each destination's dedicated task applies its own fixed delay to
// its own FIFO message stream, so per-link ordering is preserved and unrelated links
// (any address absent from the map, which is every address when the map is empty) are
// completely unaffected -- the default (empty map) is byte-identical to pre-existing
// behavior, satisfying invariant 4 for both Autobahn paths.

// `node local-benchmark --blip-at/--blip-for/--blip-node`: a transient "blip" fault
// injector riding the SAME per-connection plumbing as the latency injection just
// above -- see `blip` module's own doc comment for the exact mechanism (a dynamic
// EXTRA delay, computed at the point a message's release is scheduled, clamping it
// forward to the blip window's end whenever it would otherwise land inside the
// window) and `BlipGate::clamp`'s doc comment for the per-connection ordering
// argument. `ReliableSender`/`SimpleSender` each carry an OPTIONAL `Arc<BlipGate>`
// (default `None`), set once via `with_blip(..)` right after construction, resolved
// per-connection at spawn time exactly like `with_latency`'s own map.
