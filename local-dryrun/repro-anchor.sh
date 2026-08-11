#!/usr/bin/env bash
# Reproduce the anchor/resolution liveness condition locally.
# `--withhold k` withholds payloads from k nodes.
#
# Usage:  ./local-dryrun/repro-anchor.sh [duration_s] [nodes] [rate] [withhold]
# Exit 0 means the anchor path works or was not needed; exit 1 means liveness failed.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DURATION="${1:-60}"
NODES="${2:-30}"
RATE="${3:-100}"
WITHHOLD="${4:-5}"
# Withholding payloads and a short control delay produce split views.
DELTA_MS="${5:-20}"
LATENCY_MS="${6:-50}"
MAX_CURSOR_LAG=50

DATA_DIR="${TMPDIR:-/tmp}/vantage-anchor-$$"
LOG="${TMPDIR:-/tmp}/vantage-anchor-$$.log"

echo "==> building (release, --features benchmark)"
cargo build --release --features benchmark -j 4 2>&1 | tail -1 || exit 1
mkdir -p "$DATA_DIR"
trap 'rm -rf "$DATA_DIR" "$LOG"' EXIT

echo "==> n=$NODES @ $RATE tx/s, ${DURATION}s, withhold=$WITHHOLD delta=${DELTA_MS}ms lat=${LATENCY_MS}ms"
RUST_LOG=warn ./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size 512 \
    --protocol vantage --mode random --duration "$DURATION" \
    --delta-ms "$DELTA_MS" --max-batch-delay-ms 20 --max-header-delay-ms 100 --crash 0 \
    --withhold "$WITHHOLD" \
    --data-dir "$DATA_DIR" --mimic-latency-ms "$LATENCY_MS" \
    --batch-max-bytes 65536 --batch-max-delay-ms 5 --timeline > "$LOG" 2>&1
rc=$?

TPS=$(grep -oE 'Consensus TPS: [0-9]+' "$LOG" | tail -1 | grep -oE '[0-9]+')
ROUTES=$(grep -oE 'Total seal routes.*' "$LOG" | tail -1)

read -r LAG_MED LAG_MAX STUCK ENT_MED CUR_MED DELIV < <(python3 - "$LOG" <<'PY'
import re, sys, statistics as st
last = {}
for line in open(sys.argv[1], errors="replace"):
    m = re.search(r"\[timeline\] T\+(\d+)\s+node-(\d+)\s+entered=(\d+)\s+a_i=(\d+)\s+"
                  r"cursor=(\d+)\s+round=(\d+)\s+delivered=(\d+)", line)
    if m:
        _, node, ent, _, cur, _, deliv = (int(x) for x in m.groups())
        last[node] = (ent, cur, deliv)
if not last:
    print("-1 -1 -1 -1 -1 -1"); sys.exit()
lags = [e - c for e, c, _ in last.values()]
print(f"{int(st.median(lags))} {max(lags)} "
      f"{sum(1 for _, c, _ in last.values() if c <= 1)} "
      f"{int(st.median([e for e, _, _ in last.values()]))} "
      f"{int(st.median([c for _, c, _ in last.values()]))} "
      f"{int(st.median([d for _, _, d in last.values()]))}")
PY
)

echo
echo "-----------------------------------------"
printf ' n=%s @ %s tx/s, withhold=%s, %ss (exit %s)\n' "$NODES" "$RATE" "$WITHHOLD" "$DURATION" "$rc"
printf ' Consensus TPS   : %s\n' "${TPS:-?}"
printf ' entered / cursor: %s / %s   (lag med %s, max %s)\n' "$ENT_MED" "$CUR_MED" "$LAG_MED" "$LAG_MAX"
printf ' control delivered: %s\n' "$DELIV"
printf ' wedged nodes    : %s\n' "$STUCK"
printf ' %s\n' "${ROUTES:-seal routes: (not printed)}"
echo "-----------------------------------------"

# Large cursor lag indicates a stalled anchor path.
anchor=0
case "${ROUTES:-}" in *anchor_full*|*anchor_skip*) anchor=1 ;; esac

if [ "$LAG_MAX" != "-1" ] && [ "$LAG_MAX" -gt "$MAX_CURSOR_LAG" ] 2>/dev/null; then
    if [ "$anchor" -eq 0 ]; then
        echo "REPRODUCED (absent): cursor lag $LAG_MAX with ZERO anchor_* seals -- the" >&2
        echo "  anchor/resolution path never sealed at all." >&2
        exit 1
    fi
    echo "REPRODUCED (rate-limited): cursor lag $LAG_MAX WITH anchor_* seals present." >&2
    echo "  The anchor path functions but cannot keep up with the view rate, so the" >&2
    echo "  output cursor falls unboundedly behind. Compare anchor seals/s against the" >&2
    echo "  view rate above." >&2
    exit 1
fi
if [ "$anchor" -eq 1 ]; then
    echo "PASS: anchor path sealed views (anchor_* present) and the cursor kept up"
else
    echo "INCONCLUSIVE: no split views arose (no anchor_* needed, cursor kept up)."
    echo "  Raise --withhold or --nodes to force them."
fi
