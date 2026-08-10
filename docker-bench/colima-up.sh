#!/usr/bin/env bash
# Start a native-arm64 Colima VM for Docker benchmarks on macOS.
# Containers provide Linux networking tools and store RocksDB data through virtiofs.
#
# Usage:   ./colima-up.sh      (idempotent; reuses a running VM)
# Verify:  docker context ls   -> colima-<profile> should be CURRENT
# Restore: docker context use desktop-linux; colima stop --profile <profile>
set -euo pipefail

PROFILE="${VANTAGE_COLIMA_PROFILE:-vantage}"
CPU="${VANTAGE_COLIMA_CPU:-12}"
MEMORY="${VANTAGE_COLIMA_MEM:-32}"   # GB
DISK="${VANTAGE_COLIMA_DISK:-120}"   # GB

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

command -v colima >/dev/null 2>&1 || { echo "colima not installed: brew install colima docker" >&2; exit 1; }

if colima status --profile "$PROFILE" >/dev/null 2>&1; then
    say "Colima '$PROFILE' already running -- reusing (CPU/memory changes need a manual stop/start)"
else
    say "Starting Colima '$PROFILE' (${CPU} CPU / ${MEMORY} GB / ${DISK} GB, vz+virtiofs)"
    colima start --profile "$PROFILE" \
        --cpu "$CPU" --memory "$MEMORY" --disk "$DISK" \
        --vm-type vz --mount-type virtiofs
fi

# Select the active Docker context.
docker context use "colima-$PROFILE" >/dev/null

say "Active docker daemon"
docker info --format '  CPUs={{.NCPU}}  Mem={{.MemTotal}}  Server={{.ServerVersion}}'

cat <<EOF

$(say "Ready")
Run benchmarks from macOS exactly as before -- the docker CLI now targets this VM:

    cd docker-bench
    ./run.sh --nodes 4 --rate 200 --duration 120 --protocol vantage
    ./blip.sh 0 all 15 cut          # from a second shell, mid-run

Metrics ports are published to the VM and forwarded to the Mac's loopback by
Colima, so the existing 127.0.0.1:<port>/metrics scrapes keep working.

Confirm the WAN emulation is live inside a running cluster (it is applied by
each container's own generated tc-setup.sh, not by this script):
    docker logs vantage-node-0 2>&1 | grep tc-setup
    docker exec vantage-node-0 tc qdisc show dev eth0

When finished, put the CLI back on Docker Desktop and stop the VM:
    docker context use desktop-linux && colima stop --profile $PROFILE
EOF
