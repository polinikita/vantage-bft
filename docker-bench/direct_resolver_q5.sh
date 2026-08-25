#!/usr/bin/env bash
# Run the repeated mixed-open diagnostic used by the Vantage Q5 evaluation.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
shopt -s nullglob

RUST_TOOLCHAIN_BIN="${RUST_TOOLCHAIN_BIN:-}"
if [ -n "$RUST_TOOLCHAIN_BIN" ]; then
    PATH="$RUST_TOOLCHAIN_BIN:$PATH"
fi

NODES="${NODES:-10}"
FAULTS=$(( (NODES - 1) / 3 ))
REPETITIONS="${REPETITIONS:-3}"
SINGLE_TARGET="${SINGLE_TARGET:-0}"
DURATION="${DURATION:-150}"
START_DELAY="${START_DELAY:-30}"
FAULT_START="${FAULT_START:-20}"
FAULT_DURATION="${FAULT_DURATION:-30}"
CORRECT_RATE="${CORRECT_RATE:-1000}"
ADVERSARIAL_RATE="${ADVERSARIAL_RATE:-600}"
NETEM_LIMIT_PKTS="${NETEM_LIMIT_PKTS:-100000}"
PRIMARY_METRICS_BASE="${PRIMARY_METRICS_BASE:-19000}"
WORKER_METRICS_BASE="${WORKER_METRICS_BASE:-19100}"
RUN_ROOT="${RUN_ROOT:-$SCRIPT_DIR/recovery-runs/$(date -u +%Y%m%dT%H%M%SZ)-direct-q5}"

if [ "$NODES" -ne 10 ] || [ "$FAULTS" -ne 3 ]; then
    echo "direct_resolver_q5.sh: the paper campaign is fixed at n=10,f=3" >&2
    exit 2
fi
if [ "$REPETITIONS" -lt 1 ]; then
    echo "direct_resolver_q5.sh: REPETITIONS must be positive" >&2
    exit 2
fi
if [ "$SINGLE_TARGET" != 0 ] && [ "$SINGLE_TARGET" != 1 ]; then
    echo "direct_resolver_q5.sh: SINGLE_TARGET must be 0 or 1" >&2
    exit 2
fi

profile=mixed
if [ "$SINGLE_TARGET" = 1 ]; then
    profile=mixed-single
fi

archive_data() {
    local destination="$1"
    mkdir -p "$destination/data"
    for source in data/manifest.json data/parameters.json data/committee.json; do
        [ ! -f "$source" ] || cp "$source" "$destination/data/"
    done
    local logs target
    for logs in data/node-*/logs; do
        target="$destination/data/${logs#data/}"
        mkdir -p "$target"
        cp "$logs"/*.log "$target/" 2>/dev/null || true
    done
}

export_prometheus() {
    local destination="$1"
    local manifest="$destination/data/manifest.json"
    local start_s end_s matcher throughput_query latency_query
    start_s="$(jq -r '.active_at_ms / 1000' "$manifest")"
    end_s="$(jq -r '(.active_at_ms / 1000) + .duration' "$manifest")"
    matcher="$(jq -r '
        . as $m
        | [.withholding_node_indices[]] as $faulty
        | [range(0; .nodes) | select(. as $i | ($faulty | index($i) | not))
           | "node-\(.)-worker-0"]
        | join("|")
    ' "$manifest")"
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

mkdir -p "$RUN_ROOT"
overall=0
for repetition in $(seq 1 "$REPETITIONS"); do
    destination="$RUN_ROOT/rep-$repetition/$profile"
    mkdir -p "$destination"
    command=(
        ./run.sh
        --nodes "$NODES"
        --rate "$CORRECT_RATE"
        --duration "$DURATION"
        --protocol vantage
        --start-delay "$START_DELAY"
        --host-primary-metrics-base "$PRIMARY_METRICS_BASE"
        --host-worker-metrics-base "$WORKER_METRICS_BASE"
        --delta-ms 200
        --metrics-report-interval-ms 1000
        --netem-limit-pkts "$NETEM_LIMIT_PKTS"
        --no-state-sync
        --vantage-gc-window-views 10000
        --withhold "$FAULTS"
        --mixed-open-stress
        --correct-load-only
        --adversarial-rate "$ADVERSARIAL_RATE"
        --withhold-at "$FAULT_START"
        --withhold-for "$FAULT_DURATION"
    )
    if [ "$SINGLE_TARGET" = 1 ]; then
        command+=(--mixed-open-single-target)
    fi
    if [ "$repetition" -gt 1 ]; then
        command+=(--no-build)
    fi

    printf 'COMMAND:' | tee "$destination/run.log"
    printf ' %q' "${command[@]}" | tee -a "$destination/run.log"
    printf '\n' | tee -a "$destination/run.log"
    "${command[@]}" 2>&1 | tee -a "$destination/run.log"
    run_status=${PIPESTATUS[0]}
    if [ "$run_status" -ne 0 ]; then
        overall=1
        echo "REPETITION $repetition: run failed before analysis (run=$run_status)"
        continue
    fi
    archive_data "$destination"
    export_prometheus "$destination" || overall=1

    report_args=(
        python3 recovery_report.py
        --scenario mixed
        --data-dir "$destination/data"
        --run-log "$destination/run.log"
        --output "$destination/report.json"
    )
    if [ "$SINGLE_TARGET" = 1 ]; then
        report_args+=(--minimum-open-views 1)
    fi
    "${report_args[@]}" 2>&1 | tee "$destination/report.txt"
    report_status=${PIPESTATUS[0]}
    if [ "$run_status" -ne 0 ] || [ "$report_status" -ne 0 ]; then
        overall=1
        echo "REPETITION $repetition: diagnostic check failed (run=$run_status report=$report_status)"
    else
        echo "REPETITION $repetition: PASS"
    fi
done

echo "Direct-resolver Q5 artifacts: $RUN_ROOT"
exit "$overall"
