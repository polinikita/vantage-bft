// Transport-level batching for serialized messages. The same bundle format is used by
// both senders and decoded by `Receiver`.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use tokio::time::{Duration, Instant};

/// Configuration shared by senders and receivers. Peers must use the same setting.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BatchConfig {
    /// Whether to combine multiple messages into one frame.
    pub enabled: bool,
    /// Flush when buffered payload reaches this size, in bytes.
    pub max_bytes: usize,
    /// Maximum time to retain a non-empty buffer before flushing, in milliseconds.
    pub max_delay_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_bytes: 65_536,
            max_delay_ms: 5,
        }
    }
}

impl BatchConfig {
    pub fn max_delay(&self) -> Duration {
        Duration::from_millis(self.max_delay_ms)
    }
}

/// Bundle payload: `[count: u32-LE]` followed by `count` pairs of message length and
/// message bytes. The outer length-delimited frame contains the complete bundle.
pub(crate) fn encode_bundle(items: &[Bytes]) -> Bytes {
    let payload_bytes: usize = items.iter().map(|m| m.len()).sum();
    let mut out = BytesMut::with_capacity(4 + payload_bytes + 4 * items.len());
    out.put_u32_le(items.len() as u32);
    for msg in items {
        out.put_u32_le(msg.len() as u32);
        out.put_slice(msg);
    }
    out.freeze()
}

#[derive(Debug)]
pub(crate) struct DecodeBundleError(String);

impl fmt::Display for DecodeBundleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed bundle frame: {}", self.0)
    }
}

/// Inverse of `encode_bundle`. `payload` is the already de-framed bundle bytes.
pub(crate) fn decode_bundle(payload: &Bytes) -> Result<Vec<Bytes>, DecodeBundleError> {
    let mut buf = payload.clone();
    if buf.remaining() < 4 {
        return Err(DecodeBundleError("truncated count".to_string()));
    }
    let count = buf.get_u32_le() as usize;
    // Bound the count before allocation. Each item needs a four-byte length prefix.
    if count > buf.remaining() / 4 {
        return Err(DecodeBundleError(format!(
            "count {} exceeds what the {} remaining byte(s) could possibly hold",
            count,
            buf.remaining()
        )));
    }
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(DecodeBundleError("truncated length prefix".to_string()));
        }
        let len = buf.get_u32_le() as usize;
        if buf.remaining() < len {
            return Err(DecodeBundleError("truncated message body".to_string()));
        }
        messages.push(buf.copy_to_bytes(len));
    }
    Ok(messages)
}

/// Per-session outbound accumulator. `T` stores sender-specific metadata for each
/// message. Unflushed items are returned by `drain` when the session ends.
pub(crate) struct Coalescer<T> {
    items: Vec<(Bytes, T)>,
    bytes: usize,
}

impl<T> Coalescer<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Append one arrival. Returns `true` if this push just armed the flush timer
    /// (i.e. the coalescer was empty before this call) -- the caller is responsible
    /// for actually (re)computing its own deadline from this signal, since `Instant`
    /// bookkeeping lives with the caller's own select-loop state, not here.
    pub(crate) fn push(&mut self, data: Bytes, extra: T) -> bool {
        let just_armed = self.items.is_empty();
        self.bytes += data.len();
        self.items.push((data, extra));
        just_armed
    }

    /// Whether the size cap has been reached and an immediate flush is due.
    pub(crate) fn over_cap(&self, max_bytes: usize) -> bool {
        self.bytes >= max_bytes
    }

    /// Build the bundle frame + collect every constituent's extra payload, clearing
    /// accumulated state.
    pub(crate) fn flush(&mut self) -> (Bytes, Vec<T>) {
        let msgs: Vec<Bytes> = self.items.iter().map(|(d, _)| d.clone()).collect();
        let bundle = encode_bundle(&msgs);
        let extras = self.items.drain(..).map(|(_, e)| e).collect();
        self.bytes = 0;
        (bundle, extras)
    }

    /// Return unflushed items without encoding them. Callers must encode them before
    /// requeueing when batching is enabled.
    pub(crate) fn drain(&mut self) -> Vec<(Bytes, T)> {
        self.bytes = 0;
        std::mem::take(&mut self.items)
    }
}

/// A `None` deadline leaves the flush timer pending.
pub(crate) async fn sleep_until_or_pending(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundle_roundtrip() {
        let msgs = vec![
            Bytes::from_static(b"hello"),
            Bytes::from_static(b""),
            Bytes::from_static(b"world!"),
        ];
        let bundle = encode_bundle(&msgs);
        let decoded = decode_bundle(&bundle).unwrap();
        assert_eq!(decoded, msgs);
    }

    #[test]
    fn empty_bundle_roundtrips_to_zero_messages() {
        let bundle = encode_bundle(&[]);
        assert_eq!(decode_bundle(&bundle).unwrap(), Vec::<Bytes>::new());
    }

    #[test]
    fn decode_rejects_truncated_frame() {
        assert!(decode_bundle(&Bytes::from_static(&[1, 0])).is_err());
        // count = 1 but no length/body follows.
        assert!(decode_bundle(&Bytes::from_static(&[1, 0, 0, 0])).is_err());
    }

    /// Reject a count that cannot fit in the remaining bytes before allocation.
    #[test]
    fn decode_rejects_huge_count_without_preallocating() {
        // count = 0xFFFFFFFF, no body at all.
        let huge_count_no_body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(decode_bundle(&huge_count_no_body).is_err());

        // count = 0xFFFFFFFF with a little real trailing data -- still nowhere near
        // The count still cannot fit in the remaining bytes.
        let mut with_trailer = BytesMut::from(&[0xFF, 0xFF, 0xFF, 0xFF][..]);
        with_trailer.put_slice(b"only a few bytes follow");
        assert!(decode_bundle(&with_trailer.freeze()).is_err());
    }

    /// The decoder must return an error or a best-effort result for arbitrary bytes
    /// without panicking.
    #[test]
    fn decode_does_not_panic_on_raw_non_bundle_bytes() {
        // A plausible raw bincode-ish payload: no relation to the bundle format at all.
        let raw_payloads: [&[u8]; 4] = [
            &[0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &[1, 2, 3],
            &[],
            &[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02],
        ];
        for raw in raw_payloads {
            // The result is not important; the decoder must not panic.
            let _ = decode_bundle(&Bytes::copy_from_slice(raw));
        }
    }

    #[test]
    fn coalescer_flush_and_cap() {
        let mut c: Coalescer<u32> = Coalescer::new();
        assert!(c.is_empty());
        assert!(c.push(Bytes::from_static(b"abc"), 1)); // first push arms the timer
        assert!(!c.push(Bytes::from_static(b"de"), 2)); // second push doesn't re-arm
        assert!(!c.over_cap(10));
        assert!(c.over_cap(5));
        let (bundle, extras) = c.flush();
        assert!(c.is_empty());
        assert_eq!(extras, vec![1, 2]);
        assert_eq!(
            decode_bundle(&bundle).unwrap(),
            vec![Bytes::from_static(b"abc"), Bytes::from_static(b"de")]
        );
    }
}
