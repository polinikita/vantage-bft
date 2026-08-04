#!/usr/bin/env bash
# ring-cut.sh -- rotationally symmetric DIRECTIONAL degradation: every node x
# loses its outgoing application connections to the next K peers on the ring,
# (x+1 .. x+K) mod n, where K = ceil(pct% of its n-1 peer links) -- at n=20 and
# the default pct=10 that is K=2, i.e. ~10% of every node's connections.
#
# Directionality: the rules REJECT (tcp-reset) x's egress to the target peers'
# LISTENING ports only (primary_to_primary and worker_to_worker from
# manifest.json). x -> y application connections die immediately and cannot
# reconnect; y -> x connections keep working, because x's packets on those
# sockets carry an ephemeral destination port and pass. Every node therefore
# has exactly K dead outgoing and K dead incoming links (from x-1..x-K), and
# quorums remain available everywhere.
#
# Unlike blip.sh (single node <-> peer set), this installs rules on ALL nodes.
#
# Usage:
#   docker-bench/ring-cut.sh apply [pct]      # install and leave in place
#   docker-bench/ring-cut.sh remove           # tear the rules down everywhere
#   docker-bench/ring-cut.sh <seconds> [pct]  # apply, hold, remove (trap-safe)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/data/manifest.json"
CHAIN="VRING"

usage() {
    echo "usage: $0 apply [pct] | remove | <seconds> [pct]" >&2
    exit 2
}

[ $# -ge 1 ] || usage
CMD="$1"
PCT="${2:-10}"
[ -f "$MANIFEST" ] || { echo "ring-cut.sh: $MANIFEST not found -- run gen.py first" >&2; exit 1; }

read -r NODES IP_PREFIX IP_OFFSET PORT_P2P PORT_W2W < <(python3 -c "
import json
m = json.load(open('$MANIFEST'))
print(m['nodes'], m['node_ip_prefix'], m['node_ip_offset'],
      m['ports']['primary_to_primary'], m['ports']['worker_to_worker'])
")

# K = ceil(pct% of the n-1 peer links of each node), never 0, never all peers.
K=$(( ((NODES - 1) * PCT + 99) / 100 ))
[ "$K" -ge 1 ] || K=1
[ "$K" -lt "$NODES" ] || { echo "ring-cut.sh: pct=$PCT cuts every peer" >&2; exit 2; }

# RING_OFFSETS overrides which ring offsets are cut (comma-separated, e.g.
# "7,13"). Default 1..K targets each node's immediate successors -- which is
# maximally adversarial to round-robin leader succession (leader.rs walks
# committee order, so slot s hands off to exactly the (x+1) edge). Distant
# offsets keep the same loss fraction while leaving succession intact,
# separating "tolerates asymmetric loss" from "tolerates loss aligned with
# rotation".
if [ -n "${RING_OFFSETS:-}" ]; then
    IFS=',' read -r -a OFFSETS <<< "$RING_OFFSETS"
    for o in "${OFFSETS[@]}"; do
        [[ "$o" =~ ^[0-9]+$ ]] && [ "$o" -ge 1 ] && [ "$o" -lt "$NODES" ] || {
            echo "ring-cut.sh: bad offset '$o' in RING_OFFSETS" >&2; exit 2; }
    done
else
    OFFSETS=()
    for (( o = 1; o <= K; o++ )); do OFFSETS+=("$o"); done
fi

node_ip() { echo "${IP_PREFIX}$(( IP_OFFSET + $1 ))"; }
dexec()   { docker exec "vantage-node-$1" "${@:2}"; }
stamp()   { date -u '+%Y-%m-%dT%H:%M:%SZ'; }

remove_node() {
    dexec "$1" iptables -D OUTPUT -j "$CHAIN" >/dev/null 2>&1 || true
    dexec "$1" iptables -F "$CHAIN" >/dev/null 2>&1 || true
    dexec "$1" iptables -X "$CHAIN" >/dev/null 2>&1 || true
}

apply_node() {
    local x="$1" i peer ip port
    remove_node "$x"
    dexec "$x" iptables -N "$CHAIN"
    for i in "${OFFSETS[@]}"; do
        peer=$(( (x + i) % NODES ))
        ip="$(node_ip "$peer")"
        for port in "$PORT_P2P" "$PORT_W2W"; do
            dexec "$x" iptables -A "$CHAIN" -d "$ip/32" -p tcp --dport "$port" \
                -j REJECT --reject-with tcp-reset
        done
    done
    dexec "$x" iptables -I OUTPUT 1 -j "$CHAIN"
}

apply_all() {
    echo "ring-cut: applying offsets [${OFFSETS[*]}] (pct=$PCT%) on $NODES nodes at $(stamp)"
    local x
    for (( x = 0; x < NODES; x++ )); do
        apply_node "$x"
    done
    # Directional validation on the 0 -> (0+first-offset) edge.
    local v=${OFFSETS[0]}
    if dexec 0 timeout 2 bash -c "echo > /dev/tcp/$(node_ip "$v")/$PORT_P2P" 2>/dev/null; then
        echo "ring-cut: VALIDATION FAILED -- node 0 can still reach node $v:$PORT_P2P" >&2
    else
        echo "ring-cut: validated dead  0 -> $v:$PORT_P2P"
    fi
    if dexec "$v" timeout 2 bash -c "echo > /dev/tcp/$(node_ip 0)/$PORT_P2P" 2>/dev/null; then
        echo "ring-cut: validated alive $v -> 0:$PORT_P2P (directional)"
    else
        echo "ring-cut: VALIDATION FAILED -- reverse direction $v -> 0:$PORT_P2P is dead too" >&2
    fi
}

remove_all() {
    local x
    for (( x = 0; x < NODES; x++ )); do
        remove_node "$x"
    done
    echo "ring-cut: removed on $NODES nodes at $(stamp)"
}

case "$CMD" in
    apply)  apply_all ;;
    remove) remove_all ;;
    ''|*[!0-9]*) usage ;;
    *)
        trap remove_all EXIT INT TERM
        apply_all
        sleep "$CMD"
        ;;
esac
