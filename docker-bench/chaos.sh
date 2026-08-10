#!/usr/bin/env bash
# Run rolling single-validator outages against a running local cluster.
# One node is down at a time; the final settle window is measured.
# Modes: stop, pause, or cut traffic in both directions.
#
# Usage against an already-running cluster:
#
#   docker-bench/chaos.sh                                  # 6 x 10s pause, 20s gaps
#   docker-bench/chaos.sh --mode pause --outage 10
#   docker-bench/chaos.sh --cycles 10 --outage 5 --gap 30 --seed 7
#
# Writes epoch-millisecond outage events to data/chaos-timeline.json.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/data/manifest.json"
TIMELINE="$SCRIPT_DIR/data/chaos-timeline.json"

MODE=pause
OUTAGE=10
# Keep 20 seconds between outages for independent recovery measurements.
GAP=20
CYCLES=6
SETTLE=30
SEED=""
EXCLUDE=""

usage() {
    cat >&2 <<'EOF'
usage: chaos.sh [--mode stop|pause|cut] [--outage S] [--gap S] [--cycles N]
                [--settle S] [--seed N] [--exclude i,j,...]

  --mode     what "down" means (default pause; stop needs state sync, see header)
  --outage   seconds a victim stays down          (default 10)
  --gap      seconds with everyone up between outages (default 20, and >= 20)
  --cycles   number of outages                    (default 6)
  --settle   final all-up window, in seconds      (default 30)
  --seed     seed for victim selection, for a reproducible run
  --exclude  node indices never chosen as victims
EOF
    exit 2
}

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)    MODE="$2"; shift 2 ;;
        --outage)  OUTAGE="$2"; shift 2 ;;
        --gap)     GAP="$2"; shift 2 ;;
        --cycles)  CYCLES="$2"; shift 2 ;;
        --settle)  SETTLE="$2"; shift 2 ;;
        --seed)    SEED="$2"; shift 2 ;;
        --exclude) EXCLUDE="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "chaos.sh: unknown argument '$1'" >&2; usage ;;
    esac
done

case "$MODE" in stop|pause|cut) ;; *) echo "chaos.sh: bad --mode '$MODE'" >&2; usage ;; esac
# Keep Bash 3.2 compatibility.
for pair in "outage:OUTAGE" "gap:GAP" "cycles:CYCLES" "settle:SETTLE"; do
    flag="${pair%%:*}"; v="${pair##*:}"
    case "${!v}" in ''|*[!0-9]*)
        echo "chaos.sh: --$flag must be a non-negative integer" >&2; usage ;;
    esac
done
[ "$CYCLES" -ge 1 ] || { echo "chaos.sh: --cycles must be >= 1" >&2; exit 2; }
[ "$OUTAGE" -ge 1 ] || { echo "chaos.sh: --outage must be >= 1" >&2; exit 2; }
# Require a recovery gap between outages.
[ "$CYCLES" -eq 1 ] || [ "$GAP" -ge 20 ] || {
    echo "chaos.sh: --gap must be >= 20 so the committee re-converges between" \
         "outages (got $GAP)" >&2; exit 2; }
[ -f "$MANIFEST" ] || { echo "chaos.sh: $MANIFEST not found -- run gen.py/run.sh first" >&2; exit 1; }

NODES="$(python3 -c "import json;print(json.load(open('$MANIFEST'))['nodes'])")"
# Require at least four nodes for a one-node outage.
[ "$NODES" -ge 4 ] || { echo "chaos.sh: need n >= 4 to hold one node down (n=$NODES)" >&2; exit 1; }
FAULT_BUDGET=$(( (NODES - 1) / 3 ))

# Exclude nodes listed by --exclude from victim selection.
CANDIDATES=()
for ((i = 0; i < NODES; i++)); do
    skip=0
    IFS=, read -ra ex <<<"$EXCLUDE"
    for e in "${ex[@]:-}"; do [ -n "$e" ] && [ "$e" = "$i" ] && skip=1; done
    [ "$skip" -eq 0 ] && CANDIDATES+=("$i")
done
[ "${#CANDIDATES[@]}" -gt 0 ] || { echo "chaos.sh: --exclude removed every candidate" >&2; exit 2; }

[ -n "$SEED" ] && RANDOM="$SEED"

container() { echo "vantage-node-$1"; }
now_ms() { python3 -c "import time;print(int(time.time()*1000))"; }

running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$(container "$1")" 2>/dev/null)" = "true" ]
}

# Restore the current node on exit.
DOWN_NODE=""
DOWN_MODE=""
restore_current() {
    [ -n "$DOWN_NODE" ] || return 0
    local n="$DOWN_NODE"
    DOWN_NODE=""
    echo "chaos.sh: restoring node $n ($DOWN_MODE)"
    case "$DOWN_MODE" in
        stop)  docker start "$(container "$n")" >/dev/null 2>&1 || true ;;
        pause) docker unpause "$(container "$n")" >/dev/null 2>&1 || true ;;
        cut)   [ -n "${BLIP_PID:-}" ] && kill "$BLIP_PID" 2>/dev/null || true ;;
    esac
}
trap restore_current EXIT INT TERM

take_down() {
    local n="$1"
    DOWN_NODE="$n"; DOWN_MODE="$MODE"
    case "$MODE" in
        stop)  docker stop -t 0 "$(container "$n")" >/dev/null ;;
        pause) docker pause "$(container "$n")" >/dev/null ;;
        cut)   "$SCRIPT_DIR/blip.sh" "$n" all "$OUTAGE" cut >/dev/null & BLIP_PID=$! ;;
    esac
}

bring_up() {
    local n="$1"
    case "$MODE" in
        stop)  docker start "$(container "$n")" >/dev/null ;;
        pause) docker unpause "$(container "$n")" >/dev/null ;;
        cut)   wait "$BLIP_PID" 2>/dev/null || true; BLIP_PID="" ;;  # blip.sh self-restores
    esac
    DOWN_NODE=""
    # Confirm that the node is running.
    for _ in $(seq 1 30); do
        running "$n" && return 0
        sleep 1
    done
    echo "chaos.sh: node $n did NOT come back after 30s -- aborting" >&2
    exit 1
}

echo "chaos.sh: n=$NODES f=$FAULT_BUDGET mode=$MODE outage=${OUTAGE}s gap=${GAP}s" \
     "cycles=$CYCLES settle=${SETTLE}s seed=${SEED:-<unseeded>}"
echo "chaos.sh: victims drawn from ${#CANDIDATES[@]} candidate(s); one down at a time"

# Require all candidate nodes to be running.
for i in "${CANDIDATES[@]}"; do
    running "$i" || { echo "chaos.sh: node $i is not running; start the cluster first" >&2; exit 1; }
done

START_MS="$(now_ms)"
EVENTS=()
for ((c = 1; c <= CYCLES; c++)); do
    victim="${CANDIDATES[$((RANDOM % ${#CANDIDATES[@]}))]}"
    down_ms="$(now_ms)"
    echo "chaos.sh: [cycle $c/$CYCLES] $MODE node $victim for ${OUTAGE}s"
    take_down "$victim"
    sleep "$OUTAGE"
    bring_up "$victim"
    up_ms="$(now_ms)"
    echo "chaos.sh: [cycle $c/$CYCLES] node $victim back after $(( (up_ms - down_ms) / 1000 ))s"
    EVENTS+=("{\"cycle\":$c,\"node\":$victim,\"down_ms\":$down_ms,\"up_ms\":$up_ms}")
    if [ "$c" -lt "$CYCLES" ] && [ "$GAP" -gt 0 ]; then
        sleep "$GAP"
    fi
done

SETTLE_START_MS="$(now_ms)"
echo "chaos.sh: all $NODES up; settling for ${SETTLE}s -- this window is the measurement"
sleep "$SETTLE"
END_MS="$(now_ms)"

# Check for nodes that remain down.
DEAD=()
for ((i = 0; i < NODES; i++)); do running "$i" || DEAD+=("$i"); done

python3 - "$TIMELINE" "$START_MS" "$SETTLE_START_MS" "$END_MS" <<PYEOF
import json, sys
path, start, settle, end = sys.argv[1], *map(int, sys.argv[2:5])
json.dump({
    "mode": "$MODE", "nodes": $NODES, "fault_budget": $FAULT_BUDGET,
    "outage_s": $OUTAGE, "gap_s": $GAP, "cycles": $CYCLES, "settle_s": $SETTLE,
    "seed": ${SEED:-None}, "exclude": "$EXCLUDE",
    "start_ms": start, "settle_start_ms": settle, "end_ms": end,
    "events": [${EVENTS[*]:+$(IFS=,; echo "${EVENTS[*]}")}],
    "dead_at_end": [${DEAD[*]:+$(IFS=,; echo "${DEAD[*]}")}],
}, open(path, "w"), indent=2)
print(f"chaos.sh: timeline -> {path}")
PYEOF

if [ "${#DEAD[@]}" -gt 0 ]; then
    echo "chaos.sh: ${#DEAD[@]} node(s) not running at the end: ${DEAD[*]}" >&2
    exit 1
fi
echo "chaos.sh: all $NODES nodes running at the end"
