#!/usr/bin/env bash
# Check throughput, latency, misses, and cursor progress.
# Defaults: 10 validators, 1,000 tx/s, 60 seconds, and the AWS RTT matrix.
#
# Usage:  ./local-dryrun/regress.sh [duration_s] [nodes] [rate] [wan|loopback]
# Exit nonzero when a threshold fails.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DURATION="${1:-60}"
NODES="${2:-10}"
RATE="${3:-1000}"
TX_SIZE=512
DELTA_MS=200
MAX_BATCH_DELAY_MS=20
# Production header cadence.
MAX_HEADER_DELAY_MS=100

RTT_MODE="${4:-wan}"

MIN_TPS=$(( RATE * 85 / 100 ))
MAX_CURSOR_LAG=50
if [ "$RTT_MODE" = "loopback" ]; then
    LATENCY_FLAG="--mimic-latency-ms 0"
    MAX_P50_MS=250
    MAX_P99_MS=800
else
    # No flag selects the AWS RTT matrix.
    LATENCY_FLAG=""
    MAX_P50_MS=900
    MAX_P99_MS=2000
fi

DATA_DIR="${TMPDIR:-/tmp}/vantage-regress-$$"
# Keep the log outside the cleared data directory.
LOG="${TMPDIR:-/tmp}/vantage-regress-$$.log"

echo "==> building (release, --features benchmark)"
if ! cargo build --release --features benchmark -j 4 2>&1 | tail -3; then
    echo "REGRESSION: build failed" >&2
    exit 1
fi

mkdir -p "$DATA_DIR"
trap 'rm -rf "$DATA_DIR" "$LOG"' EXIT

if [ "$RTT_MODE" = "loopback" ]; then
    echo "==> running n=$NODES @ $RATE tx/s for ${DURATION}s (delta=${DELTA_MS}ms, loopback)"
else
    echo "==> running n=$NODES @ $RATE tx/s for ${DURATION}s (delta=${DELTA_MS}ms, aws_rtt($NODES) WAN mimic -- same table as the AWS runs)"
fi
# shellcheck disable=SC2086  # LATENCY_FLAG is intentionally word-split (empty = use aws_rtt)
RUST_LOG=warn ./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size "$TX_SIZE" \
    --protocol vantage --mode random --duration "$DURATION" \
    --delta-ms "$DELTA_MS" --max-batch-delay-ms "$MAX_BATCH_DELAY_MS" \
    --max-header-delay-ms "$MAX_HEADER_DELAY_MS" --crash 0 \
    --data-dir "$DATA_DIR" $LATENCY_FLAG \
    --batch-max-bytes 65536 --batch-max-delay-ms 5 --timeline > "$LOG" 2>&1
rc=$?
if [ $rc -ne 0 ]; then
    echo "REGRESSION: node local-benchmark exited $rc" >&2
    grep -iE "panic|thread .* panicked" "$LOG" | head -5 >&2
    exit 1
fi

TPS=$(grep -oE 'Consensus TPS: [0-9]+' "$LOG" | tail -1 | grep -oE '[0-9]+')
LAT=$(grep -oE 'p50/p90/p99 [0-9.]+/[0-9.]+/[0-9.]+ ms' "$LOG" | tail -1 | grep -oE '[0-9.]+/[0-9.]+/[0-9.]+')
MISSES=$(grep -oE '[0-9]+ misses' "$LOG" | tail -1 | grep -oE '^[0-9]+')
ROUTES=$(grep -oE 'Total seal routes.*' "$LOG" | tail -1)
P50=${LAT%%/*}; REST=${LAT#*/}; P90=${REST%%/*}; P99=${REST##*/}

if [ -z "${TPS:-}" ] || [ -z "${LAT:-}" ]; then
    echo "REGRESSION: no RESULTS block -- the run produced no summary" >&2
    tail -20 "$LOG" >&2
    exit 1
fi

# Read cursor lag from the last timeline row per node.
read -r LAG_MED LAG_MAX STUCK < <(python3 - "$LOG" <<'PY'
import re, sys, statistics as st
last = {}
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"\[timeline\] T\+(\d+)\s+node-(\d+)\s+entered=(\d+)\s+a_i=(\d+)\s+cursor=(\d+)", line)
    if m:
        _, node, ent, _, cur = (int(x) for x in m.groups())
        last[node] = (ent, cur)
if not last:
    print("-1 -1 -1"); sys.exit()
lags = [e - c for e, c in last.values()]
stuck = sum(1 for _, c in last.values() if c <= 1)
print(f"{int(st.median(lags))} {max(lags)} {stuck}")
PY
)

echo
echo "-----------------------------------------"
printf ' n=%s @ %s tx/s, %ss, rtt=%s\n' "$NODES" "$RATE" "$DURATION" "$RTT_MODE"
printf ' Consensus TPS : %s   (>= %s)\n' "$TPS" "$MIN_TPS"
printf ' p50/p90/p99   : %s/%s/%s ms   (p50 <= %s, p99 <= %s)\n' \
       "$P50" "$P90" "$P99" "$MAX_P50_MS" "$MAX_P99_MS"
printf ' misses        : %s   (== 0)\n' "${MISSES:-?}"
printf ' cursor lag    : med %s, max %s   (max <= %s)\n' "$LAG_MED" "$LAG_MAX" "$MAX_CURSOR_LAG"
printf ' wedged nodes  : %s   (== 0)\n' "$STUCK"
printf ' %s\n' "${ROUTES:-seal routes: (not printed)}"
echo "-----------------------------------------"

fail=0
awk -v v="$TPS" -v t="$MIN_TPS"    'BEGIN{exit !(v+0 < t+0)}' && { echo "REGRESSION: TPS $TPS < $MIN_TPS" >&2; fail=1; }
awk -v v="$P50" -v t="$MAX_P50_MS" 'BEGIN{exit !(v+0 > t+0)}' && { echo "REGRESSION: p50 ${P50}ms > ${MAX_P50_MS}ms" >&2; fail=1; }
awk -v v="$P99" -v t="$MAX_P99_MS" 'BEGIN{exit !(v+0 > t+0)}' && { echo "REGRESSION: p99 ${P99}ms > ${MAX_P99_MS}ms" >&2; fail=1; }
[ "${MISSES:-1}" != "0" ] && { echo "REGRESSION: ${MISSES} missed transactions (expected 0)" >&2; fail=1; }
[ "$LAG_MED" = "-1" ] && { echo "REGRESSION: no timeline rows -- cannot check the cursor" >&2; fail=1; }
[ "$LAG_MAX" != "-1" ] && [ "$LAG_MAX" -gt "$MAX_CURSOR_LAG" ] 2>/dev/null && \
    { echo "REGRESSION: cursor lag $LAG_MAX > $MAX_CURSOR_LAG -- AGB is advancing without the output cursor" >&2; fail=1; }
[ "$STUCK" != "0" ] && [ "$STUCK" != "-1" ] && \
    { echo "REGRESSION: $STUCK node(s) with a wedged cursor (<= view 1)" >&2; fail=1; }

[ $fail -ne 0 ] && exit 1
echo "PASS"
