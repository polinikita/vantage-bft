#!/usr/bin/env bash
# Regression guard: run after EVERY improvement iteration, before spending on AWS.
#
# Default n=30 @ 100 tx/s. Sized to the dev machine, deliberately:
#   - n=30 is the ceiling for a comfortable local run (n=40 oversubscribes a 14-core
#     box: 40 primaries + 40 workers + clients, and views start going split purely
#     from CPU starvation -- see repro-anchor.sh, which exploits that on purpose).
#   - 100 tx/s, not 1000: at n=30/1000 the machine is loaded enough to degrade latency,
#     which makes the p50/p99 thresholds below track the laptop rather than the code.
#     The guard's job is to catch behaviour changes, not to benchmark throughput.
#
# What it asserts, and why each one exists:
#   TPS / misses / p50 / p99   -- the run still commits at the offered rate.
#   cursor lag                 -- entered_view minus cursor_next_view. This is the check
#                                 that would have caught the 2026-08-08 n=100 failures on
#                                 the first run instead of the fifth: AGB can race to view
#                                 554 while the output cursor sits at 1, and every
#                                 throughput/latency metric still looks plausible because
#                                 the few transactions that do commit are fast.
#   no node with cursor <= 1   -- a wedged cursor, the same failure's terminal state.
#
# Budget: under a MINUTE end to end (30s run + a cached build). Long enough for the
# cursor/seal-route signals to be meaningful, short enough to run on every iteration
# without thinking about it.
#
# Usage:  ./local-dryrun/regress.sh [duration_s] [nodes] [rate]
# Exits non-zero on regression, naming the threshold that failed.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DURATION="${1:-30}"
NODES="${2:-30}"
RATE="${3:-100}"
TX_SIZE=512
DELTA_MS=200          # mirrors configs/sweep20-vantage.yaml / sweep100-vantage.yaml
MAX_BATCH_DELAY_MS=20
MAX_HEADER_DELAY_MS=50

MIN_TPS=$(( RATE * 85 / 100 ))   # 85% of offered
MAX_P50_MS=250
MAX_P99_MS=800
MAX_CURSOR_LAG=50                # healthy is 1-2; 50 is a wide net for a laptop hiccup

DATA_DIR="${TMPDIR:-/tmp}/vantage-regress-$$"
# Outside --data-dir on purpose: local-benchmark clears that directory on boot, which
# deleted a log written into it and made a successful run look like it produced nothing.
LOG="${TMPDIR:-/tmp}/vantage-regress-$$.log"

echo "==> building (release, --features benchmark)"
if ! cargo build --release --features benchmark -j 4 2>&1 | tail -3; then
    echo "REGRESSION: build failed" >&2
    exit 1
fi

mkdir -p "$DATA_DIR"
trap 'rm -rf "$DATA_DIR" "$LOG"' EXIT

echo "==> running n=$NODES @ $RATE tx/s for ${DURATION}s (delta=${DELTA_MS}ms, loopback)"
RUST_LOG=warn ./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size "$TX_SIZE" \
    --protocol vantage --mode random --duration "$DURATION" \
    --delta-ms "$DELTA_MS" --max-batch-delay-ms "$MAX_BATCH_DELAY_MS" \
    --max-header-delay-ms "$MAX_HEADER_DELAY_MS" --crash 0 \
    --data-dir "$DATA_DIR" --mimic-latency-ms 0 \
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

# Cursor lag from the last --timeline row per node.
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
printf ' n=%s @ %s tx/s, %ss\n' "$NODES" "$RATE" "$DURATION"
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
