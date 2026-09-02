#!/usr/bin/env bash
# Repeat the Q5 mixed-open campaign across committee sizes n=3f+1.
#   SIZES="10 13 16" STAMP=20260902 ./run_scaling_sweep.sh
# Each size runs REPETITIONS (default 3) fresh committees with the 150 s /
# 30 s-injection profile and plots into recovery-runs/<STAMP>-n<N>-sweep-r<R>.
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
SIZES="${SIZES:-10 13 16 19 22 25 28}"
STAMP="${STAMP:-$(date -u +%Y%m%d)}"
REPETITIONS="${REPETITIONS:-3}"
for N in $SIZES; do
  rm -f data/manifest.json
  SD=60; [ "$N" -ge 28 ] && SD=90
  export NODES=$N START_DELAY=$SD NETEM_LIMIT_PKTS=200000 REPETITIONS
  export RUN_ROOT="$SCRIPT_DIR/recovery-runs/${STAMP}-n${N}-sweep-r${REPETITIONS}"
  mkdir -p "$RUN_ROOT"
  { git -C .. rev-parse HEAD; git -C .. status --short; } > "$RUN_ROOT/git-state.txt" 2>/dev/null
  echo "=== $(date -u +%FT%TZ) START n=$N ==="
  ./direct_resolver_q5.sh 2>&1 | tee "$RUN_ROOT/campaign.log"
  python3 direct_resolver_q5.py --campaign-root "$RUN_ROOT" --output-dir "$RUN_ROOT/figures" > "$RUN_ROOT/plot.log" 2>&1
  echo "=== $(date -u +%FT%TZ) END n=$N ==="
done
echo "=== SWEEP FINISHED ==="
