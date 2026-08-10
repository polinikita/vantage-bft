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
OUT="${OUT:-./data/joiner-runs}"
mkdir -p "$OUT"
banner () { echo "===== $* :: $(date +%T)"; }

NAME="${1:-latch}"
banner "$NAME START"
./late_joiner.sh --nodes 21 --rate 1000 --down 60 --settle 120 \
    --interval 50 \
    --sequence-sync-min-gap-views 100 \
    --sequence-sync-shed-gap-views 100 > "$OUT/$NAME.log" 2>&1
banner "$NAME RUN EXIT=$?"
python3 joiner_report.py --window 700 > "$OUT/$NAME.report" 2>&1
banner "$NAME REPORT done"
banner "ALL DONE"
