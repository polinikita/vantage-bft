// Copyright(C) Facebook, Inc. and its affiliates.
//! Network senders and receivers.
//! Senders preserve FIFO order when applying per-destination delay.

mod batch;
mod channel_auth;
mod codec;
mod error;
mod receiver;
mod reliable_sender;
mod simple_sender;

#[cfg(test)]
#[path = "tests/common.rs"]
pub mod common;

pub use crate::batch::BatchConfig;
pub use crate::channel_auth::{ChannelAuth, Role};
pub use crate::codec::{frame_codec, AuthCodec, MAX_FRAME_LENGTH, TAG_LEN};
pub use crate::receiver::{MessageHandler, Receiver, Writer};
pub use crate::reliable_sender::{begin_process_shutdown, CancelHandler, DirtyMap, ReliableSender};
pub use crate::simple_sender::SimpleSender;
