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
# repo (same author, same host): native aarch64, vz + virtiofs, repo cloned onto
# the VM's own large data disk rather than driven over a read-only host mount.
#
# Usage:   ./colima-up.sh            (idempotent; reuses a running VM)
#          ./colima-up.sh --sync     (only re-sync the repo into the VM)
set -euo pipefail

PROFILE="${VANTAGE_COLIMA_PROFILE:-vantage}"
CPU="${VANTAGE_COLIMA_CPU:-12}"
MEMORY="${VANTAGE_COLIMA_MEM:-32}"   # GB
DISK="${VANTAGE_COLIMA_DISK:-120}"   # GB
VM_REPO_DIR="vantage"

REPO_ROOT="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
BRANCH="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

command -v colima >/dev/null 2>&1 || { echo "colima not installed: brew install colima docker" >&2; exit 1; }

if [ "${1:-}" != "--sync" ]; then
    if colima status --profile "$PROFILE" >/dev/null 2>&1; then
        say "Colima '$PROFILE' already running -- reusing (CPU/memory changes need a manual stop/start)"
    else
        say "Starting Colima '$PROFILE' (${CPU} CPU / ${MEMORY} GB / ${DISK} GB, vz+virtiofs)"
        colima start --profile "$PROFILE" \
            --cpu "$CPU" --memory "$MEMORY" --disk "$DISK" \
            --vm-type vz --mount-type virtiofs
    fi

    say "Provisioning VM packages (git, python3, curl, iptables, iproute2)"
    colima ssh --profile "$PROFILE" -- bash -lc '
        set -euo pipefail
        sudo apt-get update -qq
        sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
            git python3 curl iptables iproute2 rsync >/dev/null
        echo "  python3: $(python3 --version)"
        echo "  iptables: $(command -v iptables)"
    '
fi

# The repo lives on the VM's large data disk (vdb): docker-bench bind-mounts
# per-node RocksDB stores and logs under docker-bench/data/, which must be
# writable -- the macOS-side virtiofs mount is not a safe place for that, and
# the root disk is too small. ~/vantage inside the VM is a symlink to it.
say "Syncing branch '$BRANCH' into the VM (~/$VM_REPO_DIR on the data disk)"
colima ssh --profile "$PROFILE" -- bash -lc "
    set -euo pipefail
    SRC='$REPO_ROOT'
    LINK=\"\$HOME/$VM_REPO_DIR\"
    BIG_PARENT=/mnt/lima-colima-$PROFILE
    big_total=\$(df -BG --output=size \"\$BIG_PARENT\" 2>/dev/null | tail -1 | tr -dc 0-9 || echo 0)
    if [ \"\${big_total:-0}\" -ge 50 ]; then
        sudo mkdir -p \"\$BIG_PARENT\"; sudo chown \"\$(id -u):\$(id -g)\" \"\$BIG_PARENT\"
        DST=\"\$BIG_PARENT/$VM_REPO_DIR\"
        if [ -d \"\$LINK\" ] && [ ! -L \"\$LINK\" ]; then
            [ -e \"\$DST\" ] || { sudo mv \"\$LINK\" \"\$DST\"; sudo chown -R \"\$(id -u):\$(id -g)\" \"\$DST\"; }
            rm -rf \"\$LINK\"
        fi
        ln -sfn \"\$DST\" \"\$LINK\"
    else
        echo \"  WARNING: \$BIG_PARENT only \${big_total}G -- keeping repo on the root disk\"
        DST=\"\$LINK\"
    fi

    if [ -d \"\$DST/.git\" ]; then
        git -C \"\$DST\" fetch origin --prune 2>/dev/null || git -C \"\$DST\" fetch \"\$SRC\" --prune
    else
        git clone \"\$SRC\" \"\$DST\"
    fi
    git -C \"\$DST\" checkout -B '$BRANCH' 2>/dev/null || git -C \"\$DST\" checkout '$BRANCH'
    git -C \"\$DST\" fetch \"\$SRC\" '$BRANCH'
    git -C \"\$DST\" reset --hard FETCH_HEAD

    # Overlay uncommitted working-tree state (modified + untracked, .gitignore
    # respected), then apply deletions -- same contract as the iota script, so
    # an experiment can run without committing first.
    DIRTY=\$(git -C \"\$SRC\" ls-files -mo --exclude-standard)
    if [ -n \"\$DIRTY\" ]; then
        echo \"  overlaying \$(echo \"\$DIRTY\" | wc -l | tr -d ' ') uncommitted file(s)\"
        printf '%s\n' \"\$DIRTY\" | rsync -a --files-from=- \"\$SRC/\" \"\$DST/\"
    fi
    git -C \"\$SRC\" ls-files -d | while IFS= read -r f; do
        [ -n \"\$f\" ] && rm -f \"\$DST/\$f\"
    done

    git -C \"\$DST\" log --oneline -1
    echo \"  repo on \$DST (\$(df -h --output=avail \"\$DST\" 2>/dev/null | tail -1 | tr -d ' ') free)\"
"

cat <<EOF

$(say "Ready")
Run benchmarks FROM INSIDE the VM (the host's docker context is left untouched):

    colima ssh --profile $PROFILE
    cd ~/$VM_REPO_DIR/docker-bench
    ./run.sh --nodes 4 --rate 200 --duration 120 --protocol vantage

Faults, from a second shell in the same VM:
    cd ~/$VM_REPO_DIR/docker-bench && ./blip.sh 0 all 15 cut

Metrics are published on the VM's own loopback; from the Mac use
'colima ssh --profile $PROFILE -- curl -s localhost:PORT/metrics', or add a
port forward. Re-run this script to push new local commits/edits.
Tear down: colima stop --profile $PROFILE
EOF
