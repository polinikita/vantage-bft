#!/usr/bin/env bash
# docker-bench container entrypoint: applies this node's tc netem latency, then runs
# one primary + one worker (id 0) + one benchmark_client, all three backgrounded and
# logging to the per-node volume. Exits nonzero the moment any of the three dies
# (see the supervisor loop at the bottom) so `docker compose ps` / `docker inspect`
# reports a crash instead of silently limping along on two remaining processes.
#
# Addressing scheme -- NODE_IP_PREFIX/NODE_IP_OFFSET come from the environment
# (gen.py-generated compose file), NOT hardcoded here, so a `gen.py --subnet-base`
# override actually takes effect at runtime and not just in committee.json/the
# compose file's own IPs. The transactions port itself IS a fixed constant (every
# container uses the identical 8-port layout -- see docker-bench/gen.py's module
# docstring), since distinct container IPs already make that safe.
set -euo pipefail

: "${NODE_INDEX:?NODE_INDEX must be set}"
: "${N_NODES:?N_NODES must be set}"
: "${NODE_IP_PREFIX:?NODE_IP_PREFIX must be set}"
: "${NODE_IP_OFFSET:?NODE_IP_OFFSET must be set}"
: "${TX_RATE_SHARE:?TX_RATE_SHARE must be set}"
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
    for ((i = 0; i < N_NODES; i++)); do
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
/usr/local/bin/benchmark_client "$(own_addr)" \
    --size "$TX_SIZE" --rate "$TX_RATE_SHARE" --mode "$TX_MODE" \
    --nodes "${PEER_ADDRS[@]}" \
    >"$LOGDIR/client.log" 2>&1 &
CLIENT_PID=$!

cleanup() {
    kill "$PRIMARY_PID" "$WORKER_PID" "$CLIENT_PID" >/dev/null 2>&1 || true
}
graceful_stop() {
    echo "entrypoint: received stop signal, shutting down"
    cleanup
    exit 0
}
trap graceful_stop INT TERM
trap cleanup EXIT

# Supervisor: as long as primary, worker AND client are all still alive, keep looping.
# The moment any one of them exits (crash, or an unhandled error) fall through to the
# nonzero exit below -- `docker compose down`/SIGTERM take the graceful_stop path
# above instead, which exits 0.
while kill -0 "$PRIMARY_PID" 2>/dev/null && kill -0 "$WORKER_PID" 2>/dev/null && kill -0 "$CLIENT_PID" 2>/dev/null; do
    sleep 1
done

echo "entrypoint: one of primary/worker/client exited early -- see $LOGDIR/*.log" >&2
exit 1
