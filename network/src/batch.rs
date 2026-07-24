// Transport-level per-peer outbound message batching (coalescing), protocol-
// transparent: this module knows nothing about `PrimaryMessage`/`WorkerMessage`/etc,
// it only ever sees already-serialized `Bytes`. Applies uniformly to every sender
// (`ReliableSender`/`SimpleSender`) and is decoded uniformly by `network::Receiver`,
// so all three protocols (vantage, autobahn-optimistic, autobahn-seamless) get it for
// free.
//
// Off by default (`BatchConfig::default().enabled == false`): every batching branch
// in `reliable_sender.rs`/`simple_sender.rs`/`receiver.rs` is gated on `enabled`, so
// the flag-off path never builds a `Coalescer`, never calls `encode_bundle`/
// `decode_bundle`, and the wire is byte-identical to pre-batching behavior (one frame
// per message, exactly as today).
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;
use tokio::time::{Duration, Instant};

/// Mirrors `Parameters::compress_network`'s plumbing contract: a small `Copy` config
/// threaded alongside `compress` into every `ReliableSender`/`SimpleSender` this node
/// spawns (`with_batching`), and into every `network::Receiver` it spawns (`acks`/
/// `batch` at `spawn_full`). Committee-wide consistent by construction, same as
/// `compress_network` (every node's `Parameters` comes from the same generated
/// config).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BatchConfig {
    /// Off by default -- no coalescer is ever consulted, byte-identical wire/behavior.
    pub enabled: bool,
    /// Hybrid flush size cap, in bytes of buffered (pre-bundle-header) payload. A
    /// push that crosses this threshold flushes immediately (near-zero added latency
    /// under high fan-in, since the cap fills before the delay timer would fire).
    pub max_bytes: usize,
    /// Hybrid flush delay, armed on the first message buffered after an empty
    /// coalescer; fires (flushing whatever accumulated) if the size cap is never
    /// reached first.
    pub max_delay_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        // 5 ms costs only ~2.5 ms average added latency (a message waits ~window/2)
        // -- negligible next to a WAN's ~400 ms p50 -- while coalescing substantially
        // more per flush than a 1 ms window, which matters more as n grows (n~50/100).
        // `max_bytes`'s size cap still short-circuits this window under a burst.
        Self { enabled: false, max_bytes: 65_536, max_delay_ms: 5 }
    }
}

impl BatchConfig {
    pub fn max_delay(&self) -> Duration {
        Duration::from_millis(self.max_delay_ms)
    }
}

/// Bundle frame payload (transport-level, self-contained): `[count: u32-LE]
/// ( [len: u32-LE][msg bytes] ) x count`. This is the value handed to the existing
/// `wire_bytes`/outer length-delimited-framing pipeline as a single logical message --
/// compression (if on) and the outer frame length prefix wrap the WHOLE bundle, and it
/// occupies exactly one delay-queue slot (one injected latency for the whole bundle).
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

/// Inverse of `encode_bundle`. `payload` is the already de-framed, already-
/// decompressed bundle bytes (mirrors the send side's ordering: compress the whole
/// bundle before the outer frame; decompress the whole frame before splitting it back
/// into sub-messages).
pub(crate) fn decode_bundle(payload: &Bytes) -> Result<Vec<Bytes>, DecodeBundleError> {
    let mut buf = payload.clone();
    if buf.remaining() < 4 {
        return Err(DecodeBundleError("truncated count".to_string()));
    }
    let count = buf.get_u32_le() as usize;
    // FIX 2 (P2, DoS, adversarial audit): bound `count` against the remaining bytes
    // BEFORE reserving -- every sub-message needs at least 4 bytes for its own length
    // prefix, so `count` can never legitimately exceed `remaining / 4`. Without this,
    // a crafted (or mis-framed, see FIX 1) frame with `count = 0xFFFFFFFF` would
    // `Vec::with_capacity` ~137 GB and abort the process before the existing
    // per-element truncation checks below ever run.
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

/// Per-connected-session outbound coalescing accumulator. Generic over `T`, the extra
/// per-message payload each sender kind carries alongside the raw bytes
/// (`ReliableSender` carries a fan-out `Vec<oneshot::Sender<Bytes>>` per constituent
/// message so every original `send()`'s `CancelHandler` still resolves off the ONE
/// bundle ack; `SimpleSender` carries nothing, `T = ()`).
///
/// Deliberately NOT a `Connection` field: it only exists for the life of one connected
/// session (`keep_alive_*`/`run_*`). If the connection drops mid-accumulation, its
/// unflushed contents are handed back (via `drain`) to the caller's own retry/requeue
/// path as ordinary individual entries -- nothing needs to survive a reconnect, and
/// nothing is silently dropped.
pub(crate) struct Coalescer<T> {
    items: Vec<(Bytes, T)>,
    bytes: usize,
}

impl<T> Coalescer<T> {
    pub(crate) fn new() -> Self {
        Self { items: Vec::new(), bytes: 0 }
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

    /// Drain without building a frame -- used when the connection drops mid-
    /// accumulation. The caller MUST re-encode whatever comes back through
    /// `encode_bundle` (as one bundle) before requeuing it, rather than treating these
    /// as ordinary raw entries -- see `reliable_sender.rs`'s FIX 1b: anything that
    /// reaches `Connection::buffer` while batching is enabled must already be
    /// bundle-framed, since `keep_alive_*` writes buffered entries to the wire
    /// verbatim and the peer's `Receiver` runs every frame through `decode_bundle`
    /// while batching is on.
    pub(crate) fn drain(&mut self) -> Vec<(Bytes, T)> {
        self.bytes = 0;
        std::mem::take(&mut self.items)
    }
}

/// A `None` deadline means "no message is currently buffered, don't arm the flush
/// timer at all" -- mirrors `keep_alive_delayed`'s own `delay_queue.front()`-driven
/// `due` future (pending forever when the guarded resource is empty).
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
        let msgs = vec![Bytes::from_static(b"hello"), Bytes::from_static(b""), Bytes::from_static(b"world!")];
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

    /// FIX 2 (P2, DoS, adversarial audit): a `count` that couldn't possibly fit in
    /// the remaining bytes must be rejected BEFORE `Vec::with_capacity(count)` runs --
    /// this must return an `Err`, not abort the process trying to reserve ~137 GB.
    #[test]
    fn decode_rejects_huge_count_without_preallocating() {
        // count = 0xFFFFFFFF, no body at all.
        let huge_count_no_body = Bytes::from_static(&[0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(decode_bundle(&huge_count_no_body).is_err());

        // count = 0xFFFFFFFF with a little real trailing data -- still nowhere near
        // enough to hold that many sub-messages (each needs >= 4 bytes), so this must
        // still be rejected by the bound, not by exhausting memory mid-loop.
        let mut with_trailer = BytesMut::from(&[0xFF, 0xFF, 0xFF, 0xFF][..]);
        with_trailer.put_slice(b"only a few bytes follow");
        assert!(decode_bundle(&with_trailer.freeze()).is_err());
    }

    /// FIX 1 (adversarial audit): simulates the exact mis-framing scenario the sender-
    /// side bug could have produced -- an arbitrary raw (non-bundle) payload, such as
    /// a bincode-serialized message, handed to `decode_bundle` as if it were a bundle
    /// frame. The receiver must never panic/abort on this input; it either rejects it
    /// outright or (if the leading 4 bytes happen to parse as a small, in-range count)
    /// returns some best-effort split -- the actual guarantee against this ever
    /// reaching `decode_bundle` for real lives in `reliable_sender.rs` (FIX 1a/1b:
    /// every `Connection::buffer` entry is bundle-framed whenever batching is on), not
    /// here; this test only locks that the decoder itself is panic-free on garbage.
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
            // Must return a `Result`, never panic/abort -- what it decides to (either
            // arm is acceptable here) is not the point of this test.
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
        assert_eq!(decode_bundle(&bundle).unwrap(), vec![Bytes::from_static(b"abc"), Bytes::from_static(b"de")]);
    }
}
