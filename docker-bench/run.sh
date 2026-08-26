#!/usr/bin/env bash
# Generate, build, run, monitor, and stop a docker-bench cluster.
#
# Usage:
#   ./run.sh --nodes 4 --rate 200 --duration 60 --protocol vantage
#   ./run.sh --nodes 7 --crash 2 --rate 200 --duration 60 --protocol vantage
#   ./run.sh --nodes 4 --rate 200 --duration 90 --withhold 1 --withhold-at 30 \
#       --withhold-for 20
#
# --crash leaves the first N validators absent from genesis. --no-build reuses
# vantage-docker-bench:latest. Other flags are passed to gen.py.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Use an absolute repository path in cleanup output.
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MONITORING_COMPOSE="$REPO_ROOT/monitoring/docker-compose.yml"
MONITORING_OVERRIDE="$SCRIPT_DIR/monitoring-compose.yml"
PROMETHEUS_CONFIG_PATH="$SCRIPT_DIR/data/prometheus.yaml"
MONITORING_NETWORK="vantage-bench_vantage_net"
PROMETHEUS_CONTAINER_ID=""
BENCHMARK_RUNNING=0
# 0 selects a committee-scaled default once --nodes is known.
READY_TIMEOUT=${READY_TIMEOUT:-0}

NODES=4
RATE=200
DURATION=60
PROTOCOL=vantage
CRASH=0
BUILD_IMAGE=1
EXTRA=()

while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2 ;;
        --rate) RATE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --protocol) PROTOCOL="$2"; shift 2 ;;
        --crash) CRASH="$2"; shift 2 ;;
        --no-build) BUILD_IMAGE=0; shift ;;
        -h|--help)
            sed -n '2,15p' "$SCRIPT_DIR/run.sh"
            exit 0
            ;;
        *) EXTRA+=("$1"); shift ;;
    esac
done

case "$NODES" in
    ''|*[!0-9]*) echo "run.sh: --nodes must be a positive integer" >&2; exit 2 ;;
esac
[ "$NODES" -gt 0 ] || { echo "run.sh: --nodes must be positive" >&2; exit 2; }

if [ "$READY_TIMEOUT" -eq 0 ]; then
    # Startup grows with the committee: every validator installs one netem
    # class per peer, and Prometheus must see two targets per validator.
    READY_TIMEOUT=$((120 + NODES * 6))
fi
case "$CRASH" in
    ''|*[!0-9]*) echo "run.sh: --crash must be a non-negative integer" >&2; exit 2 ;;
esac
FAULT_BUDGET=$(( (NODES - 1) / 3 ))
[ "$CRASH" -le "$FAULT_BUDGET" ] || {
    echo "run.sh: --crash $CRASH exceeds fault budget f=$FAULT_BUDGET for n=$NODES" >&2
    exit 2
}
ACTIVE_COUNT=$((NODES - CRASH))
ACTIVE_SERVICES=()
for ((i = CRASH; i < NODES; i++)); do
    ACTIVE_SERVICES+=("node-$i")
done

# Refuse overlapping runs; all runs share one Compose project.
if docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps -q 2>/dev/null | grep -q .; then
    echo "run.sh: a benchmark cluster is already up from another run.sh -- wait for it" \
         "to finish, or clear it with:" >&2
    echo "    docker compose -f $SCRIPT_DIR/docker-compose.yml down" >&2
    exit 1
fi

monitoring_compose() {
    PROMETHEUS_CONFIG="$PROMETHEUS_CONFIG_PATH" docker compose \
        -f "$MONITORING_COMPOSE" -f "$MONITORING_OVERRIDE" "$@"
}

disconnect_monitoring_network() {
    if [ -n "$PROMETHEUS_CONTAINER_ID" ] && \
        docker inspect "$PROMETHEUS_CONTAINER_ID" >/dev/null 2>&1; then
        docker network disconnect "$MONITORING_NETWORK" "$PROMETHEUS_CONTAINER_ID" \
            >/dev/null 2>&1 || true
    fi
}

down_benchmark() {
    # Preserve Prometheus samples before removing the network.
    disconnect_monitoring_network
    docker compose -f docker-compose.yml down
    BENCHMARK_RUNNING=0
}

cleanup_on_interrupt() {
    echo "run.sh: interrupted, tearing down compose stack" >&2
    down_benchmark || true
}

cleanup_on_exit() {
    status=$?
    if [ "$BENCHMARK_RUNNING" -eq 1 ]; then
        echo "run.sh: failed, tearing down compose stack" >&2
        down_benchmark || true
    fi
    return "$status"
}
trap cleanup_on_interrupt INT TERM
trap cleanup_on_exit EXIT

echo "==> [1/7] gen (nodes=$NODES crash=$CRASH rate=$RATE duration=$DURATION protocol=$PROTOCOL)"
# Preserve empty-array behavior across Bash versions.
python3 gen.py --nodes "$NODES" --crash "$CRASH" --rate "$RATE" --duration "$DURATION" --protocol "$PROTOCOL" \
    "${EXTRA[@]+"${EXTRA[@]}"}"

if [ "$BUILD_IMAGE" -eq 1 ]; then
    echo "==> [2/7] build (DOCKER_BUILDKIT=1)"
    BUILD_START=$SECONDS
    DOCKER_BUILDKIT=1 docker build -f Dockerfile -t vantage-docker-bench:latest ..
    echo "    build took $((SECONDS - BUILD_START))s"
else
    echo "==> [2/7] build skipped (--no-build)"
    docker image inspect vantage-docker-bench:latest >/dev/null 2>&1 || {
        echo "run.sh: vantage-docker-bench:latest is missing; omit --no-build" >&2
        exit 1
    }
fi

echo "==> [3/7] starting $ACTIVE_COUNT/$NODES validators"
BENCHMARK_RUNNING=1
docker compose -f docker-compose.yml up -d "${ACTIVE_SERVICES[@]}"
if [ "$CRASH" -gt 0 ]; then
    echo "    permanent from-genesis crash set: node-0..node-$((CRASH - 1))"
fi

echo "==> [4/7] starting Prometheus and Grafana"
# Recreate Prometheus with current targets; its volume preserves samples.
monitoring_compose up -d --force-recreate prometheus
monitoring_compose up -d grafana
PROMETHEUS_CONTAINER_ID="$(monitoring_compose ps -q prometheus)"

echo "==> [5/7] waiting for active validators and all $((ACTIVE_COUNT * 2)) active Prometheus target(s) (timeout ${READY_TIMEOUT}s)"
WAIT_START=$SECONDS
until python3 - "$NODES" "$CRASH" <<'PYEOF'
import json, sys, urllib.request
from pathlib import Path
n = int(sys.argv[1])
crash = int(sys.argv[2])
manifest = json.loads(Path("data/manifest.json").read_text())
base = manifest["host_primary_metrics_base"]
ok = 0
for i in range(crash, n):
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{base + i}/metrics", timeout=1)
        ok += 1
    except Exception:
        pass
sys.exit(0 if ok == n - crash else 1)
PYEOF
do
    if [ $((SECONDS - WAIT_START)) -ge "$READY_TIMEOUT" ]; then
        echo "run.sh: timed out waiting for containers; check 'docker compose logs'" >&2
        down_benchmark || true
        exit 1
    fi
    sleep 1
done
until python3 - "$NODES" "$CRASH" <<'PYEOF'
import json, sys, urllib.request

n = int(sys.argv[1])
crash = int(sys.argv[2])
expected = {
    label
    for i in range(crash, n)
    for label in (f"node-{i}-primary", f"node-{i}-worker-0")
}
try:
    with urllib.request.urlopen("http://127.0.0.1:9095/api/v1/targets?state=active", timeout=1) as response:
        targets = json.load(response)["data"]["activeTargets"]
    healthy = {
        target["labels"].get("node")
        for target in targets
        if target.get("health") == "up"
    }
    with urllib.request.urlopen("http://127.0.0.1:3003/api/health", timeout=1) as response:
        grafana = json.load(response)
except Exception:
    sys.exit(1)
sys.exit(0 if expected <= healthy and grafana.get("database") == "ok" else 1)
PYEOF
do
    if [ $((SECONDS - WAIT_START)) -ge "$READY_TIMEOUT" ]; then
        echo "run.sh: timed out waiting for Grafana/Prometheus; check " \
             "'docker compose -f monitoring/docker-compose.yml logs'" >&2
        down_benchmark || true
        exit 1
    fi
    sleep 1
done
echo "    validators, Prometheus, and Grafana ready after $((SECONDS - WAIT_START))s"
echo "    Grafana dashboard: http://localhost:3003/d/vantage-local-benchmark"
echo "    Prometheus targets: http://localhost:9095/targets"

echo "==> [6/7] running for ${DURATION}s (live timeline -- run blip.sh in another terminal to inject a blip)"
# --watch reports progress and computes TPS over its measurement window.
python3 results.py --watch --duration "$DURATION"

echo "==> [7/7] stopping validators"
down_benchmark
echo "    Grafana remains available with this run's metrics:"
echo "    http://localhost:3003/d/vantage-local-benchmark"
echo "    Stop monitoring later with: docker compose -f $MONITORING_COMPOSE down"
