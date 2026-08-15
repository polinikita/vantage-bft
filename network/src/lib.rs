// Copyright(C) Facebook, Inc. and its affiliates.
//! Network senders and receivers.
//! Senders preserve FIFO order when applying per-destination delay.

mod batch;
mod codec;
mod error;
mod receiver;
mod reliable_sender;
mod simple_sender;

#[cfg(test)]
#[path = "tests/common.rs"]
pub mod common;

pub use crate::batch::BatchConfig;
pub use crate::codec::{frame_codec, MAX_FRAME_LENGTH};
pub use crate::receiver::{MessageHandler, Receiver, Writer};
pub use crate::reliable_sender::{begin_process_shutdown, CancelHandler, DirtyMap, ReliableSender};
pub use crate::simple_sender::SimpleSender;
