#!/usr/bin/env bash
# Runs and scores the late-joiner recovery regression.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

OUT="${OUT:-./joiner-runs}"
NAME="${1:-late-joiner}"
NODES="${NODES:-10}"
JOINER="node-$((NODES - 1))-primary"
mkdir -p "$OUT"

banner() {
    echo "===== $* :: $(date +%T)"
}

DAY="$(date +%F)"
START="$(date +%T)"
banner "$NAME START (nodes=$NODES)"

RUN_ARGS=(
    --nodes "$NODES"
    --rate 1000
    --down 60
    --settle 120
    --interval 50
    --sequence-sync-min-gap-views 100
    --sequence-sync-shed-gap-views 300
)
./late_joiner.sh "${RUN_ARGS[@]}" > "$OUT/$NAME.log" 2>&1
RUN_RC=$?
END="$(date +%T)"
banner "$NAME RUN EXIT=$RUN_RC"

python3 joiner_report.py --joiner "$JOINER" --nodes "$NODES" --window 700 \
    > "$OUT/$NAME.report" 2>&1
REPORT_RC=$?
cat "$OUT/$NAME.report"

NODES="$NODES" python3 joiner_score.py "$START" "$END" "$DAY" > "$OUT/$NAME.score" 2>&1
SCORE_RC=$?
cat "$OUT/$NAME.score"

if [ "$RUN_RC" -ne 0 ] || [ "$REPORT_RC" -ne 0 ] || [ "$SCORE_RC" -ne 0 ]; then
    banner "$NAME FAIL"
    exit 1
fi

banner "$NAME PASS"
