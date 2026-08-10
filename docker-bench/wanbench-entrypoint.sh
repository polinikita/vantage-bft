#!/usr/bin/env bash
# Run one primary, worker, and client using committee.json private IPs.
#
# Client variables:
#   OWN_TX_ADDR    this node's worker transaction endpoint
#   PEER_TX_ADDRS  space-separated transaction endpoints for all nodes
#   ACTIVATE_AT_MS optional epoch-ms start time. Match parameters.json so the
#                  first submitted transaction is counted.
#                  Unset means submit immediately.
#
# Files are mounted at /wanbench. This script does not configure tc.
set -euo pipefail

: "${NODE_INDEX:?}"; : "${N_NODES:?}"
: "${OWN_TX_ADDR:?wan-bench must pass OWN_TX_ADDR}"
: "${PEER_TX_ADDRS:?wan-bench must pass PEER_TX_ADDRS}"
RATE="${RATE:-100}"; TX_SIZE="${TX_SIZE:-512}"; TX_MODE="${TX_MODE:-random}"
ACTIVATE_AT_MS="${ACTIVATE_AT_MS:-}"
VERBOSITY="${NODE_VERBOSITY:--vv}"

KEYS=/wanbench/key.json
COMMITTEE=/wanbench/committee.json
PARAMETERS=/wanbench/parameters.json
LOGDIR=/wanbench/logs; STOREDIR=/wanbench/store
mkdir -p "$LOGDIR" "$STOREDIR/primary" "$STOREDIR/worker-0"

# Pass protocol flags through unchanged.
EXTRA=("$@")

echo "wanbench: node $NODE_INDEX/$N_NODES primary"
/usr/local/bin/node $VERBOSITY run \
    --keys "$KEYS" --committee "$COMMITTEE" --parameters "$PARAMETERS" \
    --store "$STOREDIR/primary" "${EXTRA[@]}" primary \
    >"$LOGDIR/primary.log" 2>&1 &
PRIMARY_PID=$!

echo "wanbench: node $NODE_INDEX/$N_NODES worker 0"
/usr/local/bin/node $VERBOSITY run \
    --keys "$KEYS" --committee "$COMMITTEE" --parameters "$PARAMETERS" \
    --store "$STOREDIR/worker-0" "${EXTRA[@]}" worker --id 0 \
    >"$LOGDIR/worker0.log" 2>&1 &
WORKER_PID=$!

# shellcheck disable=SC2206
PEERS=($PEER_TX_ADDRS)
CLIENT_EXTRA=()
if [ -n "$ACTIVATE_AT_MS" ]; then
  CLIENT_EXTRA+=(--activate-at-ms "$ACTIVATE_AT_MS")
fi
echo "wanbench: client -> $OWN_TX_ADDR, ${#PEERS[@]} peers, rate ${RATE} tx/s${ACTIVATE_AT_MS:+, submitting from ${ACTIVATE_AT_MS} (epoch ms)}"
# `--nodes` must be last; following arguments are addresses.
/usr/local/bin/benchmark_client "$OWN_TX_ADDR" \
    --size "$TX_SIZE" --rate "$RATE" --mode "$TX_MODE" \
    "${CLIENT_EXTRA[@]}" \
    --nodes "${PEERS[@]}" \
    >"$LOGDIR/client.log" 2>&1 &
CLIENT_PID=$!

cleanup() { kill "$PRIMARY_PID" "$WORKER_PID" "$CLIENT_PID" >/dev/null 2>&1 || true; }
trap 'echo "wanbench: stop signal"; cleanup; exit 0' INT TERM
trap cleanup EXIT

while kill -0 "$PRIMARY_PID" 2>/dev/null && kill -0 "$WORKER_PID" 2>/dev/null \
      && kill -0 "$CLIENT_PID" 2>/dev/null; do
    sleep 1
done
echo "wanbench: a process exited early -- see $LOGDIR/*.log" >&2
exit 1
