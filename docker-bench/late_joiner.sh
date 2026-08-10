#!/usr/bin/env bash
# SEQUENCE-CHECKPOINT-SYNC-PLAN.md Phase C: can a validator that missed the start rejoin
# purely by state sync?
#
# The node is stopped, not paused: the process dies, its in-memory consensus state goes
# with it, and it comes back at view 1 with an empty cursor. Nothing replays the history it
# missed -- reconnect replay works from a coarse WISH watermark and cannot reconstruct a
# minute of finalized output. So catching up is state sync or nothing, which is exactly the
# case that was impossible before Phase C.
#
# NOTE ON WHAT THIS DOES AND DOES NOT SHOW. There is no application snapshot, so the joiner
# has base_view = 0 and must download and execute EVERY block from view 1. It can only do
# that because `SequenceStore` and `BlockCache` are both retained indefinitely. This
# therefore tests the install path, not survival under bounded retention.
#
#     ./late_joiner.sh [--nodes 21] [--rate 1000] [--down 60] [--settle 90]
set -euo pipefail
cd "$(dirname "$0")"

NODES=21; RATE=1000; DOWN=60; SETTLE=90; INTERVAL=20; JOINER=""
# Control arm. With state sync off this is the pre-Phase-C behaviour, which is the only
# way to show that the install -- not ordinary dissemination -- is what recovers the node.
EXTRA=()
while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2;;
        --rate) RATE="$2"; shift 2;;
        --down) DOWN="$2"; shift 2;;
        --settle) SETTLE="$2"; shift 2;;
        --interval) INTERVAL="$2"; shift 2;;
        --no-state-sync) EXTRA+=(--no-state-sync); shift;;
        --sequence-sync-shed-gap-views) EXTRA+=(--sequence-sync-shed-gap-views "$2"); shift 2;;
        --sequence-sync-min-gap-views) EXTRA+=(--sequence-sync-min-gap-views "$2"); shift 2;;
        *) echo "unknown flag: $1" >&2; exit 2;;
    esac
done
JOINER="vantage-node-$((NODES - 1))"
# Warm-up before the outage + the outage itself + time to sync afterwards. The clients
# start when Compose starts, while run.sh's visible timeline starts only after the killed
# joiner comes back and every target is healthy. Add margin so the client workload is
# still alive during the recovery window instead of expiring during readiness.
DURATION=$((15 + DOWN + SETTLE + 20))

echo "==> late joiner: n=$NODES, $JOINER down for ${DOWN}s, ${SETTLE}s to catch up"

./run.sh --nodes "$NODES" --rate "$RATE" --duration "$DURATION" \
    --sequence-checkpoint-interval "$INTERVAL" \
    ${EXTRA[@]+"${EXTRA[@]}"} &
RUN_PID=$!

# Wait for the joiner to exist, then kill it before it can learn anything useful.
until docker ps --format '{{.Names}}' | grep -qx "$JOINER"; do sleep 2; done
sleep 5
echo "==> stopping $JOINER at $(date +%T)"
docker stop -t 0 "$JOINER" >/dev/null

sleep "$DOWN"
echo "==> starting $JOINER at $(date +%T) -- it must now state sync from nothing"
docker start "$JOINER" >/dev/null

wait "$RUN_PID"
