#!/usr/bin/env bash
# End-to-end docker-bench orchestration: gen -> build -> up -> monitoring -> wait ->
# run duration (live timeline, ending in a self-baselined summary) -> down validators.
#
# Usage:
#   ./run.sh --nodes 4 --rate 200 --duration 60 --protocol vantage
#   ./run.sh --nodes 4 --rate 200 --duration 90 --protocol vantage --withhold 1 --withhold-at 30 --withhold-for 20
#
# --nodes/--rate/--duration/--protocol are handled here (also needed by this script
# itself, for the build/wait/timeline steps); every other flag (--tx-size, --mode,
# --no-latency, --withhold*, --delta-ms, ...) is passed straight through to gen.py --
# see `python3 gen.py --help` for the full list.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Resolved (no `..` segment) purely so the "stop monitoring later with:" hint at the
# tail prints a path a human can read and paste; `$SCRIPT_DIR/../monitoring` worked
# fine but echoed as `docker-bench/../monitoring/...`.
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MONITORING_COMPOSE="$REPO_ROOT/monitoring/docker-compose.yml"
MONITORING_OVERRIDE="$SCRIPT_DIR/monitoring-compose.yml"
PROMETHEUS_CONFIG_PATH="$SCRIPT_DIR/data/prometheus.yaml"
MONITORING_NETWORK="vantage-bench_vantage_net"
PROMETHEUS_CONTAINER_ID=""
BENCHMARK_RUNNING=0
READY_TIMEOUT=180

NODES=4
RATE=200
DURATION=60
PROTOCOL=vantage
EXTRA=()

while [ $# -gt 0 ]; do
    case "$1" in
        --nodes) NODES="$2"; shift 2 ;;
        --rate) RATE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --protocol) PROTOCOL="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,12p' "$SCRIPT_DIR/run.sh"
            exit 0
            ;;
        *) EXTRA+=("$1"); shift ;;
    esac
done

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
    # Prometheus stays up to retain this run's samples, but it must release the
    # benchmark network before Compose can remove that network.
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

echo "==> [1/7] gen (nodes=$NODES rate=$RATE duration=$DURATION protocol=$PROTOCOL)"
# `${EXTRA[@]+"${EXTRA[@]}"}`, not a plain `"${EXTRA[@]}"`: EXTRA is empty in the
# common case (no passthrough flags), and pre-4.4 bash treats `"${arr[@]}"` on a truly
# empty array as an unbound-variable error under `set -u` -- macOS still ships bash 3.2
# as /bin/bash, confirmed to actually hit this otherwise. This idiom expands to nothing
# when empty and to the normal, word-split list when not, on bash 3.2 all the way to 5.x.
python3 gen.py --nodes "$NODES" --rate "$RATE" --duration "$DURATION" --protocol "$PROTOCOL" \
    "${EXTRA[@]+"${EXTRA[@]}"}"

echo "==> [2/7] build (DOCKER_BUILDKIT=1)"
BUILD_START=$SECONDS
DOCKER_BUILDKIT=1 docker build -f Dockerfile -t vantage-docker-bench:latest ..
echo "    build took $((SECONDS - BUILD_START))s"

echo "==> [3/7] starting validators"
BENCHMARK_RUNNING=1
docker compose -f docker-compose.yml up -d

echo "==> [4/7] starting Prometheus and Grafana"
# gen.py deletes and recreates data/prometheus.yaml each run. A bind-mounted file in
# an existing Prometheus container would otherwise retain the deleted inode and keep
# scraping the previous run's targets, even though the source path looks unchanged to
# Compose. Recreate Prometheus deliberately; its named TSDB volume preserves the
# rolling 24-hour history across container recreation. Then start/reuse Grafana.
monitoring_compose up -d --force-recreate prometheus
monitoring_compose up -d grafana
PROMETHEUS_CONTAINER_ID="$(monitoring_compose ps -q prometheus)"

echo "==> [5/7] waiting for validators and all $((NODES * 2)) Prometheus target(s) (timeout ${READY_TIMEOUT}s)"
WAIT_START=$SECONDS
until python3 - "$NODES" <<'PYEOF'
import json, sys, urllib.request
from pathlib import Path
n = int(sys.argv[1])
manifest = json.loads(Path("data/manifest.json").read_text())
base = manifest["host_primary_metrics_base"]
ok = 0
for i in range(n):
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{base + i}/metrics", timeout=1)
        ok += 1
    except Exception:
        pass
sys.exit(0 if ok == n else 1)
PYEOF
do
    if [ $((SECONDS - WAIT_START)) -ge "$READY_TIMEOUT" ]; then
        echo "run.sh: timed out waiting for containers; check 'docker compose logs'" >&2
        down_benchmark || true
        exit 1
    fi
    sleep 1
done
until python3 - "$NODES" <<'PYEOF'
import json, sys, urllib.request

n = int(sys.argv[1])
expected = {
    label
    for i in range(n)
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
# --watch prints one TIMELINE: line every 10s (total/delta/tps + committee-median
# p50 committed and materialised latency; the cadence matches the nodes' own
# latency-gauge refresh), then its own SUMMARY (TPS self-baselined from this
# watch's own first/last samples -- see results.py; a separate one-shot
# `results.py` call afterwards would instead divide the CUMULATIVE committed_total,
# which includes whatever was already committed during the "wait" step above, by
# --duration, silently inflating the reported rate).
python3 results.py --watch --duration "$DURATION"

echo "==> [7/7] stopping validators"
down_benchmark
echo "    Grafana remains available with this run's metrics:"
echo "    http://localhost:3003/d/vantage-local-benchmark"
echo "    Stop monitoring later with: docker compose -f $MONITORING_COMPOSE down"
