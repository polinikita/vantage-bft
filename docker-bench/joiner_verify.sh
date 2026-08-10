#!/usr/bin/env bash
# Does the recovered joiner stop syncing, reach peer parity, and contribute?
#
# Pass conditions, all four:
#   1. vantage_sequence_sync_recovered -> 1
#   2. transfers/s -> 0 and STAYS 0
#   3. lag settles toward the healthy peer spread (~19-23 views), not 50-120
#   4. own-committed / own-published ratio comparable to peers
#
# --settle 180 so there is a long post-recovery window to prove (2) and (3) rather than
# just catching the moment of catch-up.
set -uo pipefail
cd /Users/nikitapolianskii/code/vantage/docker-bench
# NOT under ./data -- late_joiner.sh's gen step recreates that directory, which unlinks
# the run log mid-run and loses the report (observed 2026-08-10: anchor1's log vanished).
OUT="${OUT:-./joiner-runs}"
mkdir -p "$OUT"
banner () { echo "===== $* :: $(date +%T)"; }

NAME="${1:-latch}"
# Committee size; the joiner is node-(NODES-1). joiner_score.py reads the same variable.
NODES="${NODES:-21}"
banner "$NAME START (nodes=$NODES)"
./late_joiner.sh --nodes "$NODES" --rate 1000 --down 60 --settle 120 \
    --interval 50 \
    --sequence-sync-min-gap-views 100 \
    --sequence-sync-shed-gap-views 100 > "$OUT/$NAME.log" 2>&1
banner "$NAME RUN EXIT=$?"
python3 joiner_report.py --window 700 > "$OUT/$NAME.report" 2>&1
banner "$NAME REPORT done"
banner "ALL DONE"
