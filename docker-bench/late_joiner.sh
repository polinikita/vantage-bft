#!/usr/bin/env bash
# Restart one validator after an outage and allow state sync.
#
# Usage: ./late_joiner.sh [--nodes 10] [--rate 1000] [--down 60] [--settle 90]
set -euo pipefail
cd "$(dirname "$0")"

NODES=10; RATE=1000; DOWN=60; SETTLE=90; INTERVAL=20; JOINER=""
EXTRA=()
while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2;;
        --rate) RATE="$2"; shift 2;;
        --down) DOWN="$2"; shift 2;;
        --settle) SETTLE="$2"; shift 2;;
        --interval) INTERVAL="$2"; shift 2;;
        --no-state-sync) EXTRA+=(--no-state-sync); shift;;
        --no-build) EXTRA+=(--no-build); shift;;
        --sequence-sync-shed-gap-views) EXTRA+=(--sequence-sync-shed-gap-views "$2"); shift 2;;
        --sequence-sync-min-gap-views) EXTRA+=(--sequence-sync-min-gap-views "$2"); shift 2;;
        --sequence-sync-rearm-gap-views) EXTRA+=(--sequence-sync-rearm-gap-views "$2"); shift 2;;
        # Header delay sets lane block density, which is what catch-up cost scales with.
        --max-header-delay-ms) EXTRA+=(--max-header-delay-ms "$2"); shift 2;;
        --sequence-sync-chunk-records) EXTRA+=(--sequence-sync-chunk-records "$2"); shift 2;;
        --sequence-sync-chunk-digests) EXTRA+=(--sequence-sync-chunk-digests "$2"); shift 2;;
        --sequence-sync-chunk-outcomes) EXTRA+=(--sequence-sync-chunk-outcomes "$2"); shift 2;;
        --sequence-sync-chunk-outcome-items) EXTRA+=(--sequence-sync-chunk-outcome-items "$2"); shift 2;;
        *) echo "unknown flag: $1" >&2; exit 2;;
    esac
done
JOINER="vantage-node-$((NODES - 1))"
# Keep clients active for the full run.
DURATION=$((15 + DOWN + SETTLE + 20))

echo "==> late joiner: n=$NODES, $JOINER down for ${DOWN}s, ${SETTLE}s to catch up"

./run.sh --nodes "$NODES" --rate "$RATE" --duration "$DURATION" \
    --sequence-checkpoint-interval "$INTERVAL" \
    ${EXTRA[@]+"${EXTRA[@]}"} &
RUN_PID=$!

# Stop the joiner during warm-up.
until docker ps --format '{{.Names}}' | grep -qx "$JOINER"; do sleep 2; done
sleep 5
echo "==> stopping $JOINER at $(date +%T)"
docker stop -t 0 "$JOINER" >/dev/null

sleep "$DOWN"
echo "==> starting $JOINER at $(date +%T) -- it must now state sync from nothing"
docker start "$JOINER" >/dev/null

wait "$RUN_PID"
