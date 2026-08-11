#!/usr/bin/env bash
# Measure RSS growth as a leak check.
# local-benchmark runs all nodes in one process; report the second-half slope.
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
# Vantage is required for VantageCore metrics.
RUST_LOG=error ./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size 512 \
    --protocol vantage --mode random \
    --duration "$((SECS + 10))" --delta-ms 200 \
    --max-batch-delay-ms 5 --max-header-delay-ms 100 --crash 0 \
    --batch-max-bytes 65536 --batch-max-delay-ms 5 --timeline \
    --data-dir "$DATA_DIR" --mimic-latency-ms 0 >"$DATA_DIR/../rss-run.log" 2>&1 &
PID=$!

# Read cache growth from the timeline.
SAMPLES=()
for _ in $(seq 1 "$SECS"); do
    sleep 1
    kill -0 "$PID" 2>/dev/null || break
    # ps reports RSS in KiB on macOS and Linux.
    KB=$(ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ')
    [ -n "$KB" ] && SAMPLES+=("$KB")
done

# Read node-0 block-cache values from the run timeline.
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
    if cache_rate <= 0:
        # A flat cache does not explain RSS growth.
        print(" => cache is BOUNDED; residual growth is elsewhere")
    if cache_rate > 0:
        # Estimate RSS growth per cache entry.
        print(f" => {per_node*1e6/cache_rate:8.0f} bytes of per-node RSS growth per cache entry")
print("-----------------------------------------")
print("LEAK" if per_node > 1.0 else "OK (sub-1 MB/s per node)")
PY
