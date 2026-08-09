#!/usr/bin/env bash
# chaos.sh -- rolling single-validator outages against a running local cluster.
#
# Takes ONE validator down at a time for `--outage` seconds, brings it back, waits
# `--gap`, repeats `--cycles` times, then leaves the whole committee up for a final
# `--settle` window. The settle window is the measurement: a protocol that is merely
# degraded catches up inside it, one that is broken does not.
#
# Never more than one node down at once, so the committee stays inside its own
# fault budget (f = floor((n-1)/3) >= 1 for n >= 4) throughout -- any loss of
# liveness is then a protocol/implementation property, not an over-provisioned
# adversary.
#
# THREE MEANINGS OF "DOWN", because they exercise completely different code:
#
#   stop  -- `docker stop -t 0` + `docker start`. SIGKILL, so the process dies with
#            no graceful shutdown, and comes back re-reading its persisted store
#            from ./data/node-N. This is true CRASH RECOVERY: new PID, new TCP
#            sockets, cold in-memory state, and `entrypoint.sh` re-applies this
#            node's netem on the way up. `-t 0` matters: plain `docker stop` waits
#            up to 10s for SIGTERM to be honoured, which would silently swallow a
#            10s outage window.
#
#            NOT A RESILIENCE TEST TODAY. Vantage has no state sync, so a restarted
#            node cannot rejoin a committee that has moved on -- it re-enters the
#            broadcast layer but its commit cursor never leaves view 1, and it is
#            gone for good. Measured 2026-08-09: one restart permanently removes a
#            validator, so at n=4 the second one takes the committee below quorum
#            and everything stops. That is the missing feature, not a bug, and this
#            mode only becomes meaningful once state sync exists. Use `pause`.
#   pause -- `docker pause`/`unpause` (cgroup freezer). The process keeps its
#            memory AND its established TCP connections; peers see a peer that has
#            simply stopped reading. Models a long GC/scheduler stall, and isolates
#            "can the protocol absorb a stalled peer" from "can a node rejoin".
#   cut   -- delegates to blip.sh's `cut` mode: iptables REJECT with tcp-reset in
#            both directions. The process stays alive and keeps making progress on
#            its own; only the links die. Isolates reconnect from restart.
#
# Usage (against an ALREADY-RUNNING cluster -- start `run.sh` in another shell, or
# use its --duration to outlive this script):
#
#   docker-bench/chaos.sh                                  # 6 x 10s pause, 20s gaps
#   docker-bench/chaos.sh --mode pause --outage 10
#   docker-bench/chaos.sh --cycles 10 --outage 5 --gap 30 --seed 7
#
# Writes a timeline to data/chaos-timeline.json with epoch-millisecond stamps so
# every outage can be lined up against Prometheus after the fact.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/data/manifest.json"
TIMELINE="$SCRIPT_DIR/data/chaos-timeline.json"

MODE=pause
OUTAGE=10
# At least 20s between outages. A gap shorter than recovery lets the next victim go
# down while the committee is still re-converging from the last one, so a failure at
# cycle N is really a failure of N stacked partial recoveries and cannot be read as
# "one node paused once". 20s is the floor, not a tuning knob.
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
# Flag name spelled out rather than derived with ${v,,}: that is a bash-4 expansion,
# and blip.sh already documents that macOS ships bash 3.2 as /bin/bash.
for pair in "outage:OUTAGE" "gap:GAP" "cycles:CYCLES" "settle:SETTLE"; do
    flag="${pair%%:*}"; v="${pair##*:}"
    case "${!v}" in ''|*[!0-9]*)
        echo "chaos.sh: --$flag must be a non-negative integer" >&2; usage ;;
    esac
done
[ "$CYCLES" -ge 1 ] || { echo "chaos.sh: --cycles must be >= 1" >&2; exit 2; }
[ "$OUTAGE" -ge 1 ] || { echo "chaos.sh: --outage must be >= 1" >&2; exit 2; }
# Enforced, not advisory: below this the run measures stacked partial recoveries
# rather than the response to a single outage, and nothing about the result would
# say so. See GAP's own comment.
[ "$CYCLES" -eq 1 ] || [ "$GAP" -ge 20 ] || {
    echo "chaos.sh: --gap must be >= 20 so the committee re-converges between" \
         "outages (got $GAP)" >&2; exit 2; }
[ -f "$MANIFEST" ] || { echo "chaos.sh: $MANIFEST not found -- run gen.py/run.sh first" >&2; exit 1; }

NODES="$(python3 -c "import json;print(json.load(open('$MANIFEST'))['nodes'])")"
# f = floor((n-1)/3). One victim at a time needs f >= 1, i.e. n >= 4. Below that a
# single outage is already a quorum loss and the run would measure nothing.
[ "$NODES" -ge 4 ] || { echo "chaos.sh: need n >= 4 to hold one node down (n=$NODES)" >&2; exit 1; }
FAULT_BUDGET=$(( (NODES - 1) / 3 ))

# Candidate victims: every index, minus --exclude.
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

# Whatever is down when we die -- Ctrl-C, TERM, or a failed docker command under
# `set -e` -- must come back up. A chaos harness that strands a stopped validator
# turns every later measurement on this cluster into a silent lie.
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
    # Assert the node is actually back before the next cycle. Without this a failed
    # restart is invisible until the end, and every later cycle silently runs with a
    # permanently missing validator -- i.e. against a shrinking committee.
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

# Refuse to start against a cluster that is already degraded -- otherwise cycle 1
# takes the committee to two nodes down and the run measures the wrong thing.
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

# Report which nodes are alive at the end. A node that crashed on its own (rather
# than by our hand) shows up here, and that is a finding, not a harness error.
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
    echo "chaos.sh: FINDING -- ${#DEAD[@]} node(s) not running at the end: ${DEAD[*]}" >&2
    exit 1
fi
echo "chaos.sh: all $NODES nodes running at the end"
