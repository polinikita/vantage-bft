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
trap 'kill "${PID:-}" 2>/dev/null; rm -rf "$DATA_DIR" "$DATA_DIR/../rss-run.log"' EXIT

echo "==> n=$NODES @ $RATE tx/s for ${SECS}s, sampling RSS every 1s"
# `--protocol vantage` is NOT optional: without it `local-benchmark` runs an Autobahn
# path, no `VantageCore` is ever constructed, and every vantage gauge stays 0 forever.
# Omitting it once already produced a bogus A/B -- an AckAggregator change measured as
# "no effect" in a process that never built an AckAggregator.
RUST_LOG=error ./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size 512 \
    --protocol vantage --mode random \
    --duration "$((SECS + 10))" --delta-ms 200 \
    --max-batch-delay-ms 5 --max-header-delay-ms 50 --crash 0 \
    --batch-max-bytes 65536 --batch-max-delay-ms 5 --timeline \
    --data-dir "$DATA_DIR" --mimic-latency-ms 0 >"$DATA_DIR/../rss-run.log" 2>&1 &
PID=$!

# ATTRIBUTION comes from the --timeline log, NOT from the HTTP metrics endpoints:
# `local-benchmark` serves those from a registry the core never writes to, so every gauge
# reads 0 there (including `entered_view`, while the same run's in-process registry shows
# thousands of views). Sampling a monotone collection size next to RSS gives
# bytes-per-entry, which is what decides whether a collection is the leak.
SAMPLES=()
for _ in $(seq 1 "$SECS"); do
    sleep 1
    kill -0 "$PID" 2>/dev/null || break
    # ps reports RSS in KiB on macOS and Linux alike.
    KB=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
    [ -n "$KB" ] && SAMPLES+=("$KB")
done

# Pull node-0's block-cache series out of the timeline the run just wrote.
CACHE=()
while read -r v; do CACHE+=("$v"); done < <(
    grep -E "node-0 " "$DATA_DIR/../rss-run.log" 2>/dev/null \
    | sed -n 's/.*cache=\([0-9]*\).*/\1/p')
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

CN=${#CACHE[@]}
CFIRST=0; CLAST=0; CSPAN=0
if [ "$CN" -ge 8 ]; then
    CH=$((CN / 2))
    CFIRST=${CACHE[$CH]}
    CLAST=${CACHE[$((CN - 1))]}
    CSPAN=$((CN - 1 - CH))
fi

python3 - "$FIRST" "$LAST" "$SPAN" "$N" "$NODES" "$CFIRST" "$CLAST" "$CSPAN" <<'PY'
import sys
first, last, span, n, nodes, cfirst, clast, cspan = (int(x) for x in sys.argv[1:9])
mb = (last - first) / 1024.0
rate = mb / span if span else 0.0
per_node = rate / nodes
print("-----------------------------------------")
print(f" RSS start(2nd half) : {first/1024:8.1f} MB")
print(f" RSS end             : {last/1024:8.1f} MB")
print(f" growth              : {rate:8.2f} MB/s total  ({per_node:.3f} MB/s per node)")
print(f" samples             : {n} ({span}s measured slope)")
if cspan:
    cache_rate = (clast - cfirst) / cspan
    print(f" block_cache_len     : {cfirst:8,} -> {clast:,}  (+{cache_rate:,.0f} entries/s, node 0)")
    if cache_rate > 0:
        # Per-node RSS growth attributed to one block-cache entry. If this lands in a
        # plausible header-plus-index range (roughly 0.3-1.5 KB) the cache accounts for
        # the leak; far above it means something else dominates.
        print(f" => {per_node*1e6/cache_rate:8.0f} bytes of per-node RSS growth per cache entry")
print("-----------------------------------------")
# The AWS n=100 leak was 13.43 MB/s PER NODE. Anything near that per node is the same bug.
print("LEAK" if per_node > 1.0 else "OK (sub-1 MB/s per node)")
PY
