#!/usr/bin/env bash
# Run one synchronized multi-validator blackout against a running local cluster.
# All listed nodes are SIGKILLed at once (docker kill, no SIGTERM trap), held
# down together, then restarted in place with their stores intact. This mirrors
# wan-bench's crash-restart fault (docker kill -s KILL / docker start) so the
# scenario can be validated locally before an AWS run.
#
# Usage against an already-running cluster:
#
#   docker-bench/blackout.sh --nodes 7,8,9                  # 20s in, 20s down
#   docker-bench/blackout.sh --nodes 7,8,9 --at 20 --down 20 --settle 60
#
# Writes epoch-millisecond outage events to data/chaos-timeline.json in the
# schema chaos_report.py consumes (one event per node, one shared cycle).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/data/manifest.json"
TIMELINE="$SCRIPT_DIR/data/chaos-timeline.json"

NODES_ARG=""
AT=20
DOWN=20
SETTLE=60

usage() {
    cat >&2 <<'EOF'
usage: blackout.sh --nodes i,j,... [--at S] [--down S] [--settle S]

  --nodes    validator indices to kill together (required)
  --at       seconds to wait before the blackout      (default 20)
  --down     seconds the victims stay down together   (default 20)
  --settle   all-up observation window, in seconds    (default 60)
EOF
    exit 2
}

while [ $# -gt 0 ]; do
    case "$1" in
        --nodes)  NODES_ARG="$2"; shift 2 ;;
        --at)     AT="$2"; shift 2 ;;
        --down)   DOWN="$2"; shift 2 ;;
        --settle) SETTLE="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "blackout.sh: unknown argument '$1'" >&2; usage ;;
    esac
done

[ -n "$NODES_ARG" ] || { echo "blackout.sh: --nodes is required" >&2; usage; }
# Keep Bash 3.2 compatibility.
for pair in "at:AT" "down:DOWN" "settle:SETTLE"; do
    flag="${pair%%:*}"; v="${pair##*:}"
    case "${!v}" in ''|*[!0-9]*)
        echo "blackout.sh: --$flag must be a non-negative integer" >&2; usage ;;
    esac
done
[ "$DOWN" -ge 1 ] || { echo "blackout.sh: --down must be >= 1" >&2; exit 2; }
[ -f "$MANIFEST" ] || { echo "blackout.sh: $MANIFEST not found -- run gen.py/run.sh first" >&2; exit 1; }

NODES="$(python3 -c "import json;print(json.load(open('$MANIFEST'))['nodes'])")"
FAULT_BUDGET=$(( (NODES - 1) / 3 ))

VICTIMS=()
IFS=, read -ra parts <<<"$NODES_ARG"
for i in "${parts[@]}"; do
    case "$i" in ''|*[!0-9]*)
        echo "blackout.sh: bad node index '$i'" >&2; exit 2 ;;
    esac
    [ "$i" -lt "$NODES" ] || { echo "blackout.sh: node $i out of range (n=$NODES)" >&2; exit 2; }
    VICTIMS+=("$i")
done
[ "${#VICTIMS[@]}" -le "$FAULT_BUDGET" ] || {
    echo "blackout.sh: ${#VICTIMS[@]} victims exceed the fault budget" \
         "f=$FAULT_BUDGET for n=$NODES" >&2; exit 2; }

container() { echo "vantage-node-$1"; }
now_ms() { python3 -c "import time;print(int(time.time()*1000))"; }

running() {
    [ "$(docker inspect -f '{{.State.Running}}' "$(container "$1")" 2>/dev/null)" = "true" ]
}

# Restore every victim on exit, whatever state the script died in.
restore_all() {
    for i in "${VICTIMS[@]}"; do
        running "$i" || docker start "$(container "$i")" >/dev/null 2>&1 || true
    done
}
trap restore_all EXIT INT TERM

echo "blackout.sh: n=$NODES f=$FAULT_BUDGET victims=[${VICTIMS[*]}]" \
     "at=${AT}s down=${DOWN}s settle=${SETTLE}s"

for ((i = 0; i < NODES; i++)); do
    running "$i" || { echo "blackout.sh: node $i is not running; start the cluster first" >&2; exit 1; }
done

START_MS="$(now_ms)"
[ "$AT" -gt 0 ] && sleep "$AT"

down_ms="$(now_ms)"
echo "blackout.sh: SIGKILL nodes [${VICTIMS[*]}] for ${DOWN}s"
for i in "${VICTIMS[@]}"; do
    docker kill --signal=KILL "$(container "$i")" >/dev/null
done
sleep "$DOWN"

echo "blackout.sh: restarting nodes [${VICTIMS[*]}]"
for i in "${VICTIMS[@]}"; do
    docker start "$(container "$i")" >/dev/null
done
up_ms="$(now_ms)"
for i in "${VICTIMS[@]}"; do
    for _ in $(seq 1 30); do
        running "$i" && break
        sleep 1
    done
    running "$i" || { echo "blackout.sh: node $i did NOT come back after 30s" >&2; exit 1; }
done

SETTLE_START_MS="$up_ms"
echo "blackout.sh: all victims restarted; observing for ${SETTLE}s"
sleep "$SETTLE"
END_MS="$(now_ms)"

DEAD=()
for ((i = 0; i < NODES; i++)); do running "$i" || DEAD+=("$i"); done

EVENTS=()
for i in "${VICTIMS[@]}"; do
    EVENTS+=("{\"cycle\":1,\"node\":$i,\"down_ms\":$down_ms,\"up_ms\":$up_ms}")
done

python3 - "$TIMELINE" "$START_MS" "$SETTLE_START_MS" "$END_MS" <<PYEOF
import json, sys
path, start, settle, end = sys.argv[1], *map(int, sys.argv[2:5])
json.dump({
    "mode": "blackout", "nodes": $NODES, "fault_budget": $FAULT_BUDGET,
    "outage_s": $DOWN, "gap_s": 0, "cycles": 1, "settle_s": $SETTLE,
    "seed": None, "exclude": "",
    "victims": [$(IFS=,; echo "${VICTIMS[*]}")],
    "start_ms": start, "settle_start_ms": settle, "end_ms": end,
    "events": [$(IFS=,; echo "${EVENTS[*]}")],
    "dead_at_end": [${DEAD[*]:+$(IFS=,; echo "${DEAD[*]}")}],
}, open(path, "w"), indent=2)
print(f"blackout.sh: timeline -> {path}")
PYEOF

if [ "${#DEAD[@]}" -gt 0 ]; then
    echo "blackout.sh: ${#DEAD[@]} node(s) not running at the end: ${DEAD[*]}" >&2
    exit 1
fi
echo "blackout.sh: all $NODES nodes running at the end"
