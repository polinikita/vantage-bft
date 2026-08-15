#!/usr/bin/env bash
# Start one primary, worker, and benchmark client with optional tc netem latency.
# Exit nonzero if any process exits unexpectedly.
#
# Node addresses come from the generated environment; the transaction port is fixed.
set -euo pipefail

: "${NODE_INDEX:?NODE_INDEX must be set}"
: "${N_NODES:?N_NODES must be set}"
: "${NODE_IP_PREFIX:?NODE_IP_PREFIX must be set}"
: "${NODE_IP_OFFSET:?NODE_IP_OFFSET must be set}"
: "${CLIENT_NODE_INDICES:=}"
: "${TX_RATE_SHARE:?TX_RATE_SHARE must be set}"
: "${TX_COUNTED:=true}"
: "${ADVERSARIAL_TX_RATE_SHARE:=0}"
: "${TX_SIZE:?TX_SIZE must be set}"
: "${TX_MODE:?TX_MODE must be set}"

TRANSACTIONS_PORT=6005

KEYS=/data/key.json
COMMITTEE=/shared/committee.json
PARAMETERS=/shared/parameters.json
LOGDIR=/data/logs
STOREDIR=/data/store
VERBOSITY="${NODE_VERBOSITY:--vv}"

mkdir -p "$LOGDIR" "$STOREDIR/primary" "$STOREDIR/worker-0"

if [ -x /data/tc-setup.sh ]; then
    echo "entrypoint: applying tc netem rules"
    /data/tc-setup.sh || echo "entrypoint: tc-setup.sh failed (continuing without latency injection)" >&2
else
    echo "entrypoint: no /data/tc-setup.sh found, skipping latency injection" >&2
fi

own_addr() {
    echo "${NODE_IP_PREFIX}$((NODE_IP_OFFSET + NODE_INDEX)):${TRANSACTIONS_PORT}"
}

all_worker_addrs() {
    local i
    local active_indices=()
    if [ -n "$CLIENT_NODE_INDICES" ]; then
        IFS=, read -ra active_indices <<<"$CLIENT_NODE_INDICES"
    else
        for ((i = 0; i < N_NODES; i++)); do
            active_indices+=("$i")
        done
    fi
    for i in "${active_indices[@]}"; do
        echo "${NODE_IP_PREFIX}$((NODE_IP_OFFSET + i)):${TRANSACTIONS_PORT}"
    done
}

echo "entrypoint: node $NODE_INDEX/$N_NODES starting primary"
/usr/local/bin/node $VERBOSITY run \
    --keys "$KEYS" --committee "$COMMITTEE" --parameters "$PARAMETERS" \
    --store "$STOREDIR/primary" primary \
    >"$LOGDIR/primary.log" 2>&1 &
PRIMARY_PID=$!

echo "entrypoint: node $NODE_INDEX/$N_NODES starting worker 0"
/usr/local/bin/node $VERBOSITY run \
    --keys "$KEYS" --committee "$COMMITTEE" --parameters "$PARAMETERS" \
    --store "$STOREDIR/worker-0" worker --id 0 \
    >"$LOGDIR/worker0.log" 2>&1 &
WORKER_PID=$!

mapfile -t PEER_ADDRS < <(all_worker_addrs)
echo "entrypoint: node $NODE_INDEX/$N_NODES starting client -> $(own_addr), rate ${TX_RATE_SHARE} tx/s"
CLIENT_COUNT_ARGS=()
if [ "$TX_COUNTED" != "true" ]; then
    CLIENT_COUNT_ARGS+=(--uncounted)
fi
/usr/local/bin/benchmark_client "$(own_addr)" \
    --size "$TX_SIZE" --rate "$TX_RATE_SHARE" --mode "$TX_MODE" \
    "${CLIENT_COUNT_ARGS[@]}" \
    --nodes "${PEER_ADDRS[@]}" \
    >"$LOGDIR/client.log" 2>&1 &
CLIENT_PID=$!

ADVERSARIAL_CLIENT_PID=""
if [ "$ADVERSARIAL_TX_RATE_SHARE" -gt 0 ]; then
    echo "entrypoint: node $NODE_INDEX/$N_NODES starting uncounted adversarial client -> $(own_addr), rate ${ADVERSARIAL_TX_RATE_SHARE} tx/s"
    /usr/local/bin/benchmark_client "$(own_addr)" \
        --size "$TX_SIZE" --rate "$ADVERSARIAL_TX_RATE_SHARE" --mode "$TX_MODE" \
        --uncounted --nodes "${PEER_ADDRS[@]}" \
        >"$LOGDIR/adversarial-client.log" 2>&1 &
    ADVERSARIAL_CLIENT_PID=$!
fi

cleanup() {
    kill "$PRIMARY_PID" "$WORKER_PID" "$CLIENT_PID" ${ADVERSARIAL_CLIENT_PID:+"$ADVERSARIAL_CLIENT_PID"} \
        >/dev/null 2>&1 || true
}
graceful_stop() {
    echo "entrypoint: received stop signal, shutting down"
    cleanup
    exit 0
}
trap graceful_stop INT TERM
trap cleanup EXIT

while kill -0 "$PRIMARY_PID" 2>/dev/null && kill -0 "$WORKER_PID" 2>/dev/null \
    && kill -0 "$CLIENT_PID" 2>/dev/null \
    && { [ -z "$ADVERSARIAL_CLIENT_PID" ] || kill -0 "$ADVERSARIAL_CLIENT_PID" 2>/dev/null; }; do
    sleep 1
done

echo "entrypoint: one of primary/worker/client exited early -- see $LOGDIR/*.log" >&2
exit 1
