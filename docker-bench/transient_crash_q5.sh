#!/usr/bin/env bash
# Run the paper's n=10,f=3 transient-crash diagnostic and export one-second data.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NODES=10
VICTIMS="0,1,2"
DURATION="${DURATION:-130}"
START_DELAY="${START_DELAY:-60}"
FAULT_START="${FAULT_START:-20}"
FAULT_DURATION="${FAULT_DURATION:-30}"
SETTLE_DURATION="${SETTLE_DURATION:-60}"
RATE="${RATE:-1000}"
NETEM_LIMIT_PKTS="${NETEM_LIMIT_PKTS:-100000}"
PRIMARY_METRICS_BASE="${PRIMARY_METRICS_BASE:-19000}"
WORKER_METRICS_BASE="${WORKER_METRICS_BASE:-19100}"
RUN_ROOT="${RUN_ROOT:-$SCRIPT_DIR/recovery-runs/$(date -u +%Y%m%dT%H%M%SZ)-n10-transient}"
NO_BUILD="${NO_BUILD:-0}"

if [ "$FAULT_DURATION" -lt 30 ]; then
    echo "transient_crash_q5.sh: FAULT_DURATION must be at least 30 seconds" >&2
    exit 2
fi
if [ "$DURATION" -lt $((FAULT_START + FAULT_DURATION + SETTLE_DURATION)) ]; then
    echo "transient_crash_q5.sh: DURATION leaves no complete recovery interval" >&2
    exit 2
fi

mkdir -p "$RUN_ROOT"

archive_data() {
    local destination="$1"
    mkdir -p "$destination/data"
    local source
    for source in \
        data/manifest.json \
        data/parameters.json \
        data/committee.json \
        data/chaos-timeline.json; do
        [ ! -f "$source" ] || cp "$source" "$destination/data/"
    done
    local logs target
    for logs in data/node-*/logs; do
        target="$destination/data/${logs#data/}"
        mkdir -p "$target"
        cp "$logs"/*.log "$target/" 2>/dev/null || true
    done
}

wait_until_epoch_ms() {
    local target_ms="$1"
    python3 - "$target_ms" <<'PY'
import sys, time
target = int(sys.argv[1]) / 1000
time.sleep(max(0.0, target - time.time()))
PY
}

inject_fault() {
    local launched_ms="$1"
    local deadline=$((SECONDS + 300))
    local active_ms=0
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ -f data/manifest.json ]; then
            active_ms="$(jq -r '.active_at_ms // 0' data/manifest.json 2>/dev/null || echo 0)"
            if [ "$active_ms" -gt "$launched_ms" ]; then
                break
            fi
        fi
        sleep 1
    done
    if [ "$active_ms" -le "$launched_ms" ]; then
        echo "transient_crash_q5.sh: synchronized epoch was not created" >&2
        return 1
    fi
    wait_until_epoch_ms $((active_ms + FAULT_START * 1000))
    ./blackout.sh \
        --nodes "$VICTIMS" \
        --at 0 \
        --down "$FAULT_DURATION" \
        --settle "$SETTLE_DURATION"
}

export_prometheus() {
    local destination="$1"
    local manifest="$destination/data/manifest.json"
    local start_s end_s matcher throughput_query latency_query
    start_s="$(jq -r '.active_at_ms / 1000' "$manifest")"
    end_s="$(jq -r '(.active_at_ms / 1000) + .duration' "$manifest")"
    matcher="$(jq -r '[.load_node_indices[] | "node-\(.)-worker-0"] | join("|")' "$manifest")"
    throughput_query="quantile(0.5, rate(committed_transactions{node=~\"(${matcher})\"}[5s]))"
    latency_query="quantile(0.5, (transaction_materialised_latency_window{v=\"p50\",node=~\"(${matcher})\"} and on(node) transaction_materialised_latency_window{v=\"count\",node=~\"(${matcher})\"} > 0)) / 1000"

    printf 'throughput=%s\nlatency=%s\nscrape_interval=1s\nquery_step=1s\n' \
        "$throughput_query" "$latency_query" >"$destination/prometheus-queries.txt"
    curl --fail --silent --show-error --get \
        --data-urlencode "query=$throughput_query" \
        --data-urlencode "start=$start_s" \
        --data-urlencode "end=$end_s" \
        --data-urlencode "step=1s" \
        http://127.0.0.1:9095/api/v1/query_range \
        >"$destination/data/prometheus-throughput.json"
    curl --fail --silent --show-error --get \
        --data-urlencode "query=$latency_query" \
        --data-urlencode "start=$start_s" \
        --data-urlencode "end=$end_s" \
        --data-urlencode "step=1s" \
        http://127.0.0.1:9095/api/v1/query_range \
        >"$destination/data/prometheus-latency-window.json"
}

launched_ms="$(python3 -c 'import time; print(int(time.time() * 1000))')"
inject_fault "$launched_ms" >"$RUN_ROOT/blackout.log" 2>&1 &
fault_pid=$!
cleanup() {
    if kill -0 "$fault_pid" 2>/dev/null; then
        kill "$fault_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

command=(
    ./run.sh
    --nodes "$NODES"
    --rate "$RATE"
    --duration "$DURATION"
    --protocol vantage
    --start-delay "$START_DELAY"
    --host-primary-metrics-base "$PRIMARY_METRICS_BASE"
    --host-worker-metrics-base "$WORKER_METRICS_BASE"
    --delta-ms 200
    --metrics-report-interval-ms 1000
    --netem-limit-pkts "$NETEM_LIMIT_PKTS"
    --load-exclude "$VICTIMS"
)
if [ "$NO_BUILD" = 1 ]; then
    command+=(--no-build)
fi

printf 'COMMAND:' | tee "$RUN_ROOT/run.log"
printf ' %q' "${command[@]}" | tee -a "$RUN_ROOT/run.log"
printf '\n' | tee -a "$RUN_ROOT/run.log"
set +e
"${command[@]}" 2>&1 | tee -a "$RUN_ROOT/run.log"
run_status=${PIPESTATUS[0]}
wait "$fault_pid"
fault_status=$?
set -e
trap - EXIT INT TERM

if [ "$run_status" -ne 0 ] || [ "$fault_status" -ne 0 ]; then
    echo "transient_crash_q5.sh: run failed (run=$run_status blackout=$fault_status)" >&2
    exit 1
fi

archive_data "$RUN_ROOT"
export_prometheus "$RUN_ROOT"
python3 transient_crash_q5.py \
    --run-root "$RUN_ROOT" \
    --output-dir "$RUN_ROOT/figures" \
    | tee "$RUN_ROOT/figure-summary.json"

echo "Transient-crash Q5 artifacts: $RUN_ROOT"
