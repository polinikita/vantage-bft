#!/usr/bin/env bash
# End-to-end docker-bench orchestration: gen -> build -> up -> wait -> run duration
# (live timeline, ending in a self-baselined summary) -> down.
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
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
            sed -n '2,12p' "$0"
            exit 0
            ;;
        *) EXTRA+=("$1"); shift ;;
    esac
done

cleanup_on_interrupt() {
    echo "run.sh: interrupted, tearing down compose stack" >&2
    docker compose -f docker-compose.yml down || true
}
trap cleanup_on_interrupt INT TERM

echo "==> [1/6] gen (nodes=$NODES rate=$RATE duration=$DURATION protocol=$PROTOCOL)"
# `${EXTRA[@]+"${EXTRA[@]}"}`, not a plain `"${EXTRA[@]}"`: EXTRA is empty in the
# common case (no passthrough flags), and pre-4.4 bash treats `"${arr[@]}"` on a truly
# empty array as an unbound-variable error under `set -u` -- macOS still ships bash 3.2
# as /bin/bash, confirmed to actually hit this otherwise. This idiom expands to nothing
# when empty and to the normal, word-split list when not, on bash 3.2 all the way to 5.x.
python3 gen.py --nodes "$NODES" --rate "$RATE" --duration "$DURATION" --protocol "$PROTOCOL" \
    "${EXTRA[@]+"${EXTRA[@]}"}"

echo "==> [2/6] build (DOCKER_BUILDKIT=1)"
BUILD_START=$SECONDS
DOCKER_BUILDKIT=1 docker build -f Dockerfile -t vantage-docker-bench:latest ..
echo "    build took $((SECONDS - BUILD_START))s"

echo "==> [3/6] up"
docker compose -f docker-compose.yml up -d

echo "==> [4/6] waiting for all $NODES primary metrics endpoint(s) to answer (timeout 60s)"
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
    if [ $((SECONDS - WAIT_START)) -ge 60 ]; then
        echo "run.sh: timed out waiting for containers; check 'docker compose logs'" >&2
        docker compose -f docker-compose.yml down || true
        exit 1
    fi
    sleep 1
done
echo "    all nodes answering after $((SECONDS - WAIT_START))s"

echo "==> [5/6] running for ${DURATION}s (live timeline -- run blip.sh in another terminal to inject a blip)"
# --watch prints one TIMELINE: line/sec, then its own SUMMARY (TPS self-baselined from
# this watch's own first/last samples -- see results.py; a separate one-shot
# `results.py` call afterwards would instead divide the CUMULATIVE committed_total,
# which includes whatever was already committed during the "wait" step above, by
# --duration, silently inflating the reported rate).
python3 results.py --watch --duration "$DURATION"

echo "==> [6/6] down"
docker compose -f docker-compose.yml down
