#!/usr/bin/env bash
# blip.sh <i> <j|all> <seconds> drop|cut
#
# Host-side link-disruption orchestrator: for `seconds`, isolates node i from node j
# (or from every other node, with `all`) by `docker exec`-ing iptables rules into
# container i, then removes them (idempotent cleanup via trap -- Ctrl-C mid-blip still
# cleans up).
#
#   drop  -- iptables ... -j DROP: packets to/from the peer are silently discarded.
#            No RST, no ICMP unreachable. Existing TCP connections do not get an
#            explicit teardown signal -- they just stop getting ACKs, so peers see
#            retransmits/timeouts and eventually the OS-level keepalive/retry gives up.
#            This is "pause" semantics: if the outage ends before either side's
#            retransmission gives up, the SAME TCP connection resumes as if nothing
#            happened (no reconnect, no lost socket state).
#   cut   -- iptables ... -j REJECT --reject-with tcp-reset: an immediate RST is sent
#            for both directions, actively tearing down any open TCP connection between
#            the two nodes right away. This exercises the disconnect/reconnect path
#            (new TCP handshake, `network` crate's own retry/backoff) rather than a
#            silent pause.
#
# Usage:
#   docker-bench/blip.sh 0 1 5 drop     # isolate node 0 <-> node 1 for 5s, pause semantics
#   docker-bench/blip.sh 2 all 10 cut   # isolate node 2 from everyone for 10s, reset semantics
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
# Guards every "${PEER_IPS[@]}" expansion below against bash's own long-standing
# nounset-vs-empty-array quirk (pre-4.4 bash treats "${arr[@]}" on a truly empty array
# as an unbound-variable error under `set -u`; macOS still ships bash 3.2 as /bin/bash)
# -- rather than relying on a `${arr[@]+"${arr[@]}"}` idiom at every use site, just
# reject the one input combination (a single-node cluster with `all`) that could ever
# make this array empty, since there is nothing meaningful to blip there anyway.
[ "${#PEER_IPS[@]}" -gt 0 ] || { echo "blip.sh: no peer(s) to disrupt (single-node cluster?)" >&2; exit 2; }

# `iptables_rule <-I|-D> <INPUT|OUTPUT> <-s|-d> <ip>`: branches on $MODE directly
# rather than building a shared "extra args" array, so no call site needs to expand a
# possibly-empty array under `set -u` (same bash-3.2 concern as above).
iptables_rule() {
    if [ "$MODE" = drop ]; then
        dexec iptables "$1" "$2" "$3" "$4" -j DROP
    else
        dexec iptables "$1" "$2" "$3" "$4" -j REJECT --reject-with tcp-reset
    fi
}

dexec() { docker exec "$CONTAINER" "$@"; }

apply() {
    for ip in "${PEER_IPS[@]}"; do
        iptables_rule -I INPUT  -s "$ip"
        iptables_rule -I OUTPUT -d "$ip"
    done
}

remove() {
    for ip in "${PEER_IPS[@]}"; do
        iptables_rule -D INPUT  -s "$ip" 2>/dev/null || true
        iptables_rule -D OUTPUT -d "$ip" 2>/dev/null || true
    done
}

trap remove EXIT INT TERM

echo "blip.sh: ${MODE} node ${I} (${CONTAINER}) <-> ${J} [${PEER_IPS[*]}] for ${SECONDS_ARG}s"
apply
sleep "$SECONDS_ARG"
echo "blip.sh: restoring node ${I} <-> ${J}"
# Cleanup itself runs via the EXIT trap (fires exactly once, on normal completion,
# Ctrl-C, or TERM alike) -- not called again here.
