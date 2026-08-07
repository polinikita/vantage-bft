#!/usr/bin/env bash
# Measure RSS growth of a local n-node committee -- a leak detector, not a benchmark.
#
# Why local: `local-benchmark` runs every node in ONE process, so a per-node leak shows up
# amplified n-fold in a single RSS series. That makes a 13 MB/s/node leak (the 2026-08-07
# n=100 AckAggregator finding) trivially visible in 30 seconds on a laptop, where the AWS
# run needed a 123s window across 100 machines to establish the same number.
#
# Reports MB/s over the second half of the run only: the first half includes RocksDB
# warm-up and allocator growth, which are one-off and would flatter or spoil the slope.
#
# Usage: rss-growth.sh [seconds] [nodes] [rate]
set -uo pipefail
cd "$(dirname "$0")/.."

SECS="${1:-30}"
NODES="${2:-30}"
RATE="${3:-100}"

echo "==> building (release)"
cargo build --release --features benchmark -p node >/dev/null 2>&1 || {
    echo "BUILD FAILED" >&2; exit 1; }

DATA_DIR="${TMPDIR:-/tmp}/vantage-rss-$$"
mkdir -p "$DATA_DIR"
trap 'kill "${PID:-}" 2>/dev/null; rm -rf "$DATA_DIR"' EXIT

echo "==> n=$NODES @ $RATE tx/s for ${SECS}s, sampling RSS every 1s"
RUST_LOG=error ./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size 512 \
    --duration "$((SECS + 10))" --delta-ms 200 \
    --data-dir "$DATA_DIR" --mimic-latency-ms 0 >/dev/null 2>&1 &
PID=$!

SAMPLES=()
for _ in $(seq 1 "$SECS"); do
    sleep 1
    kill -0 "$PID" 2>/dev/null || break
    # ps reports RSS in KiB on macOS and Linux alike.
    KB=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
    [ -n "$KB" ] && SAMPLES+=("$KB")
done
kill "$PID" 2>/dev/null
wait "$PID" 2>/dev/null

N=${#SAMPLES[@]}
if [ "$N" -lt 8 ]; then
    echo "RSS: only $N samples -- process died early, cannot measure" >&2
    exit 1
fi

HALF=$((N / 2))
FIRST=${SAMPLES[$HALF]}
LAST=${SAMPLES[$((N - 1))]}
SPAN=$((N - 1 - HALF))
python3 - "$FIRST" "$LAST" "$SPAN" "$N" "$NODES" <<'PY'
import sys
first, last, span, n, nodes = (int(x) for x in sys.argv[1:6])
mb = (last - first) / 1024.0
rate = mb / span if span else 0.0
print("-----------------------------------------")
print(f" RSS start(2nd half) : {first/1024:8.1f} MB")
print(f" RSS end             : {last/1024:8.1f} MB")
print(f" growth              : {rate:8.2f} MB/s total  ({rate/nodes:.3f} MB/s per node)")
print(f" samples             : {n} ({span}s measured slope)")
print("-----------------------------------------")
# The AWS n=100 leak was 13.43 MB/s PER NODE. Anything near that per node is the same bug.
print("LEAK" if rate / nodes > 1.0 else "OK (sub-1 MB/s per node)")
PY
