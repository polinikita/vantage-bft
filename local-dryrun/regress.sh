#!/usr/bin/env bash
# Regression guard: n=20 @ 1000 tx/s, run after EVERY improvement iteration.
#
# Why this config: n=20/1000 is the largest committee vantage commits reliably on
# (validated repeatedly on AWS), so it is the case a straggler/recovery fix must not
# break. n=100 is where the interesting failures live, but an AWS n=100 point costs
# ~$7 and answers roughly one question; this costs nothing, takes ~90s, and catches
# the class of mistake that actually happens -- a "performance" change that alters
# behaviour. Run it BEFORE spending on AWS, not instead of.
#
# Parameters mirror configs/sweep20-vantage.yaml (delta_ms 200, tx_size 512) so a
# regression here is meaningful against the recorded AWS numbers. Latency is pure
# loopback (`--mimic-latency-ms 0`, explicit -- omitting it silently injects the
# 10-region AWS matrix), which keeps the p50/p99 thresholds below deterministic;
# it measures machine capacity, not geography.
#
# Usage:  ./local-dryrun/regress.sh [duration_s]
# Exits non-zero on regression, printing which threshold failed.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

DURATION="${1:-60}"
NODES=20
RATE=1000
TX_SIZE=512
DELTA_MS=200
MAX_BATCH_DELAY_MS=20
MAX_HEADER_DELAY_MS=50

# Thresholds. Baseline measured at 34a3a86 on this machine: TPS 1001,
# p50/p90/p99 = 68/91/132 ms, 0 misses over 60s. These are set with real headroom --
# the point is to catch "it stopped committing" / "latency exploded" / "transactions
# went missing", not to police single-digit-percent jitter on a laptop.
MIN_TPS=900             # 90% of offered; below this the committee is not keeping up
MAX_P50_MS=250
MAX_P99_MS=800
DATA_DIR="${TMPDIR:-/tmp}/vantage-regress-$$"
# The log must live OUTSIDE --data-dir: `node local-benchmark` clears that directory on
# boot, which silently deleted a log written into it (the run then "produced no
# summary" even though it had succeeded).
LOG="${TMPDIR:-/tmp}/vantage-regress-$$.log"

echo "==> building (release, --features benchmark)"
if ! cargo build --release --features benchmark -j 4 2>&1 | tail -3; then
    echo "REGRESSION: build failed" >&2
    exit 1
fi

mkdir -p "$DATA_DIR"
trap 'rm -rf "$DATA_DIR" "$LOG"' EXIT

echo "==> running n=$NODES @ $RATE tx/s for ${DURATION}s (delta=${DELTA_MS}ms, loopback)"
./target/release/node local-benchmark \
    --nodes "$NODES" --workers 1 --rate "$RATE" --tx-size "$TX_SIZE" \
    --protocol vantage --mode random --duration "$DURATION" \
    --delta-ms "$DELTA_MS" --max-batch-delay-ms "$MAX_BATCH_DELAY_MS" \
    --max-header-delay-ms "$MAX_HEADER_DELAY_MS" --crash 0 \
    --data-dir "$DATA_DIR" --mimic-latency-ms 0 \
    --batch-max-bytes 65536 --batch-max-delay-ms 5 > "$LOG" 2>&1
rc=$?

# `local-benchmark` prints RESULTS then exits 0; a non-zero rc means it died, which is
# itself a regression worth failing on rather than parsing around.
if [ $rc -ne 0 ]; then
    echo "REGRESSION: node local-benchmark exited $rc" >&2
    grep -iE "panic|error|thread .* panicked" "$LOG" | head -5 >&2
    exit 1
fi

# The summary lines this parses (node/src/local_benchmark.rs `print_results`):
#   Consensus TPS: <n> tx/s
#   Real transaction latency: avg .. p50/p90/p99 <a>/<b>/<c> ms (<n> txs, <m> misses)
TPS=$(grep -oE 'Consensus TPS: [0-9]+' "$LOG" | tail -1 | grep -oE '[0-9]+')
LAT=$(grep -oE 'p50/p90/p99 [0-9.]+/[0-9.]+/[0-9.]+ ms' "$LOG" | tail -1 | grep -oE '[0-9.]+/[0-9.]+/[0-9.]+')
MISSES=$(grep -oE '[0-9]+ misses' "$LOG" | tail -1 | grep -oE '^[0-9]+')
P50=${LAT%%/*}; REST=${LAT#*/}; P90=${REST%%/*}; P99=${REST##*/}

if [ -z "${TPS:-}" ] || [ -z "${LAT:-}" ]; then
    echo "REGRESSION: could not parse a RESULTS block -- the run produced no summary" >&2
    tail -20 "$LOG" >&2
    exit 1
fi

echo
echo "-----------------------------------------"
printf ' n=%s @ %s tx/s, %ss\n' "$NODES" "$RATE" "$DURATION"
printf ' Consensus TPS : %s   (threshold >= %s)\n' "$TPS" "$MIN_TPS"
printf ' p50/p90/p99   : %s/%s/%s ms   (p50 <= %s, p99 <= %s)\n' \
       "$P50" "$P90" "$P99" "$MAX_P50_MS" "$MAX_P99_MS"
printf ' misses        : %s   (threshold 0)\n' "${MISSES:-?}"
echo "-----------------------------------------"

fail=0
awk -v v="$TPS"  -v t="$MIN_TPS"   'BEGIN{exit !(v+0 < t+0)}' && { echo "REGRESSION: TPS $TPS < $MIN_TPS" >&2; fail=1; }
awk -v v="$P50"  -v t="$MAX_P50_MS" 'BEGIN{exit !(v+0 > t+0)}' && { echo "REGRESSION: p50 ${P50}ms > ${MAX_P50_MS}ms" >&2; fail=1; }
awk -v v="$P99"  -v t="$MAX_P99_MS" 'BEGIN{exit !(v+0 > t+0)}' && { echo "REGRESSION: p99 ${P99}ms > ${MAX_P99_MS}ms" >&2; fail=1; }
[ "${MISSES:-1}" != "0" ] && { echo "REGRESSION: ${MISSES} missed transactions (expected 0)" >&2; fail=1; }

if [ $fail -ne 0 ]; then exit 1; fi
echo "PASS"
