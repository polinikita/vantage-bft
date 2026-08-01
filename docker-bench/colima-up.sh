#!/usr/bin/env bash
# Host-side (macOS) provisioning for running docker-bench on a native-arm64
# Colima Linux VM instead of Docker Desktop.
#
# WHY THIS EXISTS. Docker Desktop's VM defaults to a small slice of the host
# (measured here: 4 CPUs / 15.6 GB on a 14-core / 48 GB machine). A docker-bench
# cluster runs 3 processes per node (primary + worker + client), so even n=4
# oversubscribes 4 CPUs threefold -- and the symptom is not a clean slowdown but
# nondeterministic protocol stalls: consensus timeouts are wall-clock, so a
# descheduled primary looks exactly like a crashed one to its peers. Runs on the
# starved VM produced 68 tx/s outliers and multi-second dead windows that never
# reproduced with adequate CPU. Any measurement taken there is worthless for
# protocol comparison. This script provisions a VM sized for the real workload.
#
# Modeled on dev-tools/iota-private-network/experiments/colima-up.sh in the iota
# repo (same author, same host): native aarch64, vz + virtiofs.
#
# UNLIKE that script, nothing is cloned into the VM and no experiment runs inside
# it. Colima mounts $HOME read-WRITE under virtiofs (verified), and `colima start`
# points the host docker CLI at this VM's daemon -- so `docker-bench/run.sh` is
# driven from macOS exactly as before and only the daemon moves. That matters
# because gen.py shells out to a native `cargo`/`node` binary for key generation,
# which exists on the Mac and not in the VM. Everything the experiment needs on
# Linux (tc netem, iptables) runs INSIDE the containers, which already ship
# iproute2/iptables and hold NET_ADMIN, so the VM itself needs no provisioning.
# Per-node RocksDB bind mounts land on the Mac filesystem over virtiofs; fine at
# these rates, revisit if an I/O-heavy configuration says otherwise.
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

# `colima start` already switches the active docker context; make it explicit so a
# re-run against an already-running VM also leaves the CLI pointed at it.
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
