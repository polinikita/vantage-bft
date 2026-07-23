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
