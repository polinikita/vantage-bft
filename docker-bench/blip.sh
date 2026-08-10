#!/usr/bin/env bash
# Drop or reject traffic between two nodes. Restore rules on exit.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/data/manifest.json"

usage() {
    echo "usage: $0 <i> <j|all> <seconds> drop|cut" >&2
    exit 2
}

[ $# -eq 4 ] || usage
I="$1"; J="$2"; SECONDS_ARG="$3"; MODE="$4"

[ -f "$MANIFEST" ] || { echo "blip.sh: $MANIFEST not found -- run gen.py first" >&2; exit 1; }
case "$MODE" in
    drop|cut) ;;
    *) usage ;;
esac
case "$SECONDS_ARG" in
    ''|*[!0-9]*) usage ;;
esac

read -r NODES IP_PREFIX IP_OFFSET < <(python3 -c "
import json
m = json.load(open('$MANIFEST'))
print(m['nodes'], m['node_ip_prefix'], m['node_ip_offset'])
")

[[ "$I" =~ ^[0-9]+$ ]] && [ "$I" -ge 0 ] && [ "$I" -lt "$NODES" ] || {
    echo "blip.sh: i must be an integer in [0, $((NODES - 1))]" >&2; exit 2;
}

node_ip() { echo "${IP_PREFIX}$(( IP_OFFSET + $1 ))"; }

CONTAINER="vantage-node-${I}"

PEER_IPS=()
if [ "$J" = "all" ]; then
    for ((k = 0; k < NODES; k++)); do
        [ "$k" -eq "$I" ] && continue
        PEER_IPS+=("$(node_ip "$k")")
    done
else
    [[ "$J" =~ ^[0-9]+$ ]] && [ "$J" -ge 0 ] && [ "$J" -lt "$NODES" ] || {
        echo "blip.sh: j must be an integer in [0, $((NODES - 1))] or 'all'" >&2; exit 2;
    }
    [ "$J" -ne "$I" ] || { echo "blip.sh: i and j must differ" >&2; exit 2; }
    PEER_IPS=("$(node_ip "$J")")
fi
# Require at least one peer.
[ "${#PEER_IPS[@]}" -gt 0 ] || { echo "blip.sh: no peer(s) to disrupt (single-node cluster?)" >&2; exit 2; }

# Build mode-specific rules without empty arrays under `set -u`.
# In cut mode, allow local RSTs before rejecting other output.
iptables_rule() {
    if [ "$MODE" = drop ]; then
        dexec iptables "$1" "$2" "$3" "$4" -j DROP
    else
        # `--reject-with tcp-reset` requires a TCP match on nf_tables iptables.
        dexec iptables "$1" "$2" "$3" "$4" -p tcp -j REJECT --reject-with tcp-reset
    fi
}

# Allow locally generated RST packets.
rst_passthrough() {
    dexec iptables "$1" OUTPUT -d "$2" -p tcp --tcp-flags RST RST -j ACCEPT
}

dexec() { docker exec "$CONTAINER" "$@"; }

apply() {
    for ip in "${PEER_IPS[@]}"; do
        iptables_rule -I INPUT  -s "$ip"
        iptables_rule -I OUTPUT -d "$ip"
        if [ "$MODE" = cut ]; then
            rst_passthrough -I "$ip"
        fi
    done
}

remove() {
    for ip in "${PEER_IPS[@]}"; do
        if [ "$MODE" = cut ]; then
            rst_passthrough -D "$ip" 2>/dev/null || true
        fi
        iptables_rule -D INPUT  -s "$ip" 2>/dev/null || true
        iptables_rule -D OUTPUT -d "$ip" 2>/dev/null || true
    done
}

trap remove EXIT INT TERM

echo "blip.sh: ${MODE} node ${I} (${CONTAINER}) <-> ${J} [${PEER_IPS[*]}] for ${SECONDS_ARG}s"
apply
sleep "$SECONDS_ARG"
echo "blip.sh: restoring node ${I} <-> ${J}"
