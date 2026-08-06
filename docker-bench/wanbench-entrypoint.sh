#!/usr/bin/env bash
# wan-bench container entrypoint: run ONE node (primary + worker 0 + client) from a
# committee.json carrying REAL addresses, for deployment on arbitrary private IPs
# (AWS et al.) where the gen.py contiguous NODE_IP_PREFIX/OFFSET scheme does NOT
# hold. See docker-bench/entrypoint.sh for the local-compose counterpart.
#
# The primary and worker take all their peer addresses from committee.json via
# `node run` directly, so they need nothing extra. Only the benchmark client needs
# explicit targets, which wan-bench passes in the environment (it knows every
# node's private IP):
#
#   OWN_TX_ADDR    this node's own worker transactions endpoint, e.g. 10.0.0.4:6005
#   PEER_TX_ADDRS  space-separated transactions endpoints of ALL nodes
#   ACTIVATE_AT_MS optional absolute epoch-ms instant before which the client submits
#                  nothing. Must equal parameters.json's `metrics_active_at_ms`, which
#                  wan-bench derives from the same value, so the first transaction
#                  submitted is also the first one the nodes count. Unset -> submit
#                  immediately, which folds the committee-formation transient into the
#                  run's latency distribution.
#
# Files are bind-mounted by wan-bench at /wanbench. Latency is applied by wan-bench
# on the HOST (tc netem) in netem mode, or self-injected by `node` via the extra
# flags in mimic mode -- either way this script does not touch tc.
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

# Extra flags (delta, latency-table/mimic, ack-watermarks, ...) are passed through
# verbatim by wan-bench as this script's own arguments.
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
# --nodes takes num_args(1..), so it must come LAST: any flag after it would be
# swallowed as another address.
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
