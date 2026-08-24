#!/usr/bin/env bash
# Matched n=31 mixed-open sweep for the hash-resolver batch cap beta.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
shopt -s nullglob

NODES="${NODES:-31}"
FAULTS=$(( (NODES - 1) / 3 ))
RATE="${RATE:-1000}"
ADVERSARIAL_RATE="${ADVERSARIAL_RATE:-$((100 * FAULTS))}"
DURATION="${DURATION:-150}"
START_DELAY="${START_DELAY:-60}"
NETEM_LIMIT_PKTS="${NETEM_LIMIT_PKTS:-100000}"
PRIMARY_METRICS_BASE="${PRIMARY_METRICS_BASE:-19000}"
WORKER_METRICS_BASE="${WORKER_METRICS_BASE:-19100}"
RUN_ROOT="${RUN_ROOT:-$SCRIPT_DIR/recovery-runs/$(date -u +%Y%m%dT%H%M%SZ)-beta-sweep}"
REUSE_IMAGE=0

if [ "${1:-}" = "--reuse-image" ]; then
    REUSE_IMAGE=1
    shift
fi
if [ "$#" -ne 0 ]; then
    echo "usage: $0 [--reuse-image]" >&2
    exit 2
fi
if [ "$NODES" -ne 31 ]; then
    echo "resolution_beta_sweep.sh: matched design requires NODES=31" >&2
    exit 2
fi

mkdir -p "$RUN_ROOT"
echo "Vantage resolver beta sweep: $RUN_ROOT"

archive_data() {
    local destination="$1"
    mkdir -p "$destination/data"
    local source logs target
    for source in data/manifest.json data/parameters.json data/committee.json; do
        if [ -f "$source" ]; then
            cp "$source" "$destination/data/"
        fi
    done
    for logs in data/node-*/logs; do
        target="$destination/data/${logs#data/}"
        mkdir -p "$target"
        cp "$logs"/*.log "$target/" 2>/dev/null || true
    done
}

run_one() {
    local beta="$1"
    local repetition="$2"
    local build_mode="$3"
    local destination="$RUN_ROOT/beta-$beta-rep-$repetition"
    mkdir -p "$destination"
    local command=(
        ./run.sh
        --nodes "$NODES"
        --rate "$RATE"
        --duration "$DURATION"
        --protocol vantage
        --start-delay "$START_DELAY"
        --host-primary-metrics-base "$PRIMARY_METRICS_BASE"
        --host-worker-metrics-base "$WORKER_METRICS_BASE"
        --delta-ms 200
        --netem-limit-pkts "$NETEM_LIMIT_PKTS"
        --resolution-batch-cap "$beta"
        --no-state-sync
        --vantage-gc-window-views 10000
        --withhold "$FAULTS"
        --mixed-open-stress
        --correct-load-only
        --adversarial-rate "$ADVERSARIAL_RATE"
        --withhold-at 20
        --withhold-for 10
    )
    if [ "$build_mode" = "reuse" ]; then
        command+=(--no-build)
    fi

    printf 'COMMAND:' | tee "$destination/run.log"
    printf ' %q' "${command[@]}" | tee -a "$destination/run.log"
    printf '\n' | tee -a "$destination/run.log"
    "${command[@]}" 2>&1 | tee -a "$destination/run.log"
    local run_status=${PIPESTATUS[0]}
    archive_data "$destination"

    local report_status=1
    if [ -f "$destination/data/manifest.json" ]; then
        python3 recovery_report.py \
            --scenario mixed \
            --data-dir "$destination/data" \
            --run-log "$destination/run.log" \
            --output "$destination/report.json" \
            2>&1 | tee "$destination/report.txt"
        report_status=${PIPESTATUS[0]}
    fi
    if [ "$run_status" -ne 0 ] || [ "$report_status" -ne 0 ]; then
        echo "BETA $beta REP $repetition: FAIL (run=$run_status report=$report_status)"
        return 1
    fi
    echo "BETA $beta REP $repetition: PASS"
}

# Rotate order so beta is not confounded with server warm-up or run order.
orders=("16 32 64" "64 16 32" "32 64 16")
overall=0
build_mode=build
if [ "$REUSE_IMAGE" -eq 1 ]; then
    build_mode=reuse
fi
for repetition in 1 2 3; do
    read -r -a betas <<< "${orders[$((repetition - 1))]}"
    for beta in "${betas[@]}"; do
        run_one "$beta" "$repetition" "$build_mode" || overall=1
        build_mode=reuse
    done
done

python3 resolution_beta_report.py "$RUN_ROOT" \
    --json-output "$RUN_ROOT/summary.json" \
    --markdown-output "$RUN_ROOT/summary.md" | tee "$RUN_ROOT/summary.txt"

echo "Vantage resolver beta sweep artifacts: $RUN_ROOT"
if [ "$overall" -ne 0 ]; then
    echo "RESOLUTION BETA SWEEP: FAIL"
    exit 1
fi
echo "RESOLUTION BETA SWEEP: PASS"
