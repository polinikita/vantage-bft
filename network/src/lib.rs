// Copyright(C) Facebook, Inc. and its affiliates.
mod batch;
mod error;
mod receiver;
mod reliable_sender;
mod simple_sender;

#[cfg(test)]
#[path = "tests/common.rs"]
pub mod common;

pub use crate::batch::BatchConfig;
pub use crate::receiver::{MessageHandler, Receiver, Writer};
pub use crate::reliable_sender::{CancelHandler, DirtyMap, ReliableSender};
pub use crate::simple_sender::SimpleSender;

// Senders may apply a fixed per-destination delay. The delay is read when each
// connection starts and preserves FIFO order on that connection. An empty map disables
// the delay.
