// Copyright(C) Facebook, Inc. and its affiliates.
//! Shared length-delimited framing for every peer connection.

use tokio_util::codec::LengthDelimitedCodec;

/// Largest single message the transport accepts.
///
/// `LengthDelimitedCodec` defaults to 8 MiB. An `n = 100` committee exceeds
/// that on the consensus plane, and the sender then fails to encode the frame
/// and drops it: the message is lost with no retransmission, so the protocol
/// stalls rather than degrades. Both ends build their codec here because a
/// receiver with a smaller limit rejects what the sender emits.
pub const MAX_FRAME_LENGTH: usize = 64 * 1024 * 1024;

/// Build the length-delimited codec used by senders and receivers.
pub fn frame_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}
