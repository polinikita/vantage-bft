#!/usr/bin/env bash
# Run the local Vantage resolver/recoverability qualification.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
shopt -s nullglob

CLEAN_DURATION="${CLEAN_DURATION:-60}"
CRASH_DURATION="${CRASH_DURATION:-90}"
MIXED_DURATION="${MIXED_DURATION:-150}"
START_DELAY="${START_DELAY:-30}"
NODES="${NODES:-20}"
FAULTS=$(( (NODES - 1) / 3 ))
ADVERSARIAL_RATE="${ADVERSARIAL_RATE:-$((100 * FAULTS))}"
NETEM_LIMIT_PKTS="${NETEM_LIMIT_PKTS:-100000}"
PRIMARY_METRICS_BASE="${PRIMARY_METRICS_BASE:-19000}"
WORKER_METRICS_BASE="${WORKER_METRICS_BASE:-19100}"
RUN_ROOT="${RUN_ROOT:-$SCRIPT_DIR/recovery-runs/$(date -u +%Y%m%dT%H%M%SZ)}"
REUSE_IMAGE=0

if [ "${1:-}" = "--reuse-image" ]; then
    REUSE_IMAGE=1
    shift
fi
if [ "$#" -ne 0 ]; then
    echo "usage: $0 [--reuse-image]" >&2
    exit 2
fi

mkdir -p "$RUN_ROOT"
echo "Local Vantage recovery qualification: $RUN_ROOT"

archive_data() {
    local destination="$1"
    mkdir -p "$destination/data"
    for source in data/manifest.json data/parameters.json data/committee.json; do
        if [ -f "$source" ]; then
            cp "$source" "$destination/data/"
        fi
    done
    local logs target
    for logs in data/node-*/logs; do
        target="$destination/data/${logs#data/}"
        mkdir -p "$target"
        cp "$logs"/*.log "$target/" 2>/dev/null || true
    done
}

run_scenario() {
    local scenario="$1"
    local duration="$2"
    local build_mode="$3"
    shift 3
    local destination="$RUN_ROOT/$scenario"
    mkdir -p "$destination"
    local command=(
        ./run.sh
        --nodes "$NODES"
        --rate 1000
        --duration "$duration"
        --protocol vantage
        --start-delay "$START_DELAY"
        --host-primary-metrics-base "$PRIMARY_METRICS_BASE"
        --host-worker-metrics-base "$WORKER_METRICS_BASE"
        --delta-ms 200
        --netem-limit-pkts "$NETEM_LIMIT_PKTS"
        --no-state-sync
        --vantage-gc-window-views 10000
    )
    if [ "$build_mode" = "reuse" ]; then
        command+=(--no-build)
    fi
    command+=("$@")

    printf 'COMMAND:' | tee "$destination/run.log"
    printf ' %q' "${command[@]}" | tee -a "$destination/run.log"
    printf '\n' | tee -a "$destination/run.log"
    "${command[@]}" 2>&1 | tee -a "$destination/run.log"
    local run_status=${PIPESTATUS[0]}
    archive_data "$destination"

    local report_status=1
    if [ -f "$destination/data/manifest.json" ]; then
        python3 recovery_report.py \
            --scenario "$scenario" \
            --data-dir "$destination/data" \
            --run-log "$destination/run.log" \
            --output "$destination/report.json" \
            2>&1 | tee "$destination/report.txt"
        report_status=${PIPESTATUS[0]}
    else
        echo "No generated manifest; report could not run" | tee "$destination/report.txt"
    fi

    if [ "$run_status" -ne 0 ] || [ "$report_status" -ne 0 ]; then
        echo "SCENARIO $scenario: FAIL (run=$run_status report=$report_status)"
        return 1
    fi
    echo "SCENARIO $scenario: PASS"
}

overall=0
initial_build=build
if [ "$REUSE_IMAGE" -eq 1 ]; then
    initial_build=reuse
fi

run_scenario clean "$CLEAN_DURATION" "$initial_build" || overall=1
run_scenario crash "$CRASH_DURATION" reuse --crash "$FAULTS" || overall=1
run_scenario mixed "$MIXED_DURATION" reuse \
    --withhold "$FAULTS" \
    --mixed-open-stress \
    --correct-load-only \
    --adversarial-rate "$ADVERSARIAL_RATE" \
    --withhold-at 20 \
    --withhold-for 10 || overall=1

echo "Local recovery qualification artifacts: $RUN_ROOT"
if [ "$overall" -ne 0 ]; then
    echo "LOCAL RECOVERY QUALIFICATION: FAIL"
    exit 1
fi
echo "LOCAL RECOVERY QUALIFICATION: PASS"
