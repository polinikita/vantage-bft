#!/usr/bin/env python3
"""`node local-benchmark` launcher (METRICS-DASHBOARD-SPEC.md §5), starfish-style UX
(~/code/starfish/local-dryrun is the read-only reference for the UX only -- that one
drives one Docker container per validator via a bash script; this one drives NATIVE
`node local-benchmark` processes, the deliberate Phase-2 §8 deviation -- see
README.md).

Usage:
    python3 dryrun.py                     # uses config.yml in this directory
    python3 dryrun.py --config other.yml
    python3 dryrun.py --no-build           # skip the cargo build step
    python3 dryrun.py --down               # tear the monitoring stack down on exit

Requires: Python 3, stdlib + pyyaml only (`pip install pyyaml` if missing).
"""
import argparse
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit(
        "dryrun.py needs pyyaml (stdlib + pyyaml only, per METRICS-DASHBOARD-SPEC.md §5). "
        "Install it in this Python's environment: pip install pyyaml"
    )

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_UID = "vantage-local-benchmark"
GRAFANA_URL = "http://localhost:3003"
COMPOSE_FILE = REPO_ROOT / "monitoring" / "docker-compose.yml"

REQUIRED_KEYS = [
    "protocol", "nodes", "workers", "rate", "tx_size", "mode", "duration",
    "delta_ms", "max_batch_delay_ms", "max_header_delay_ms", "crash",
    "latency_table", "compress_network", "data_dir",
]


def load_config(path: Path) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        sys.exit(f"{path}: missing required key(s): {missing}")
    return cfg


def build(no_build: bool) -> None:
    if no_build:
        print("[dryrun] --no-build: skipping cargo build")
        return
    print("[dryrun] CARGO_BUILD_JOBS=4 cargo build --release --features benchmark ...")
    env = dict(os.environ, CARGO_BUILD_JOBS="4")
    subprocess.run(
        ["cargo", "build", "--release", "--features", "benchmark", "-j", "4"],
        cwd=REPO_ROOT / "node", env=env, check=True,
    )


def generate_prometheus_targets(data_dir: Path, nodes: int, workers: int, base_port: int = 4000) -> Path:
    """Mirrors `config::Committee::local_benchmark`'s deterministic port allocation
    (config/src/lib.rs) exactly, so this file has real content BEFORE `node
    local-benchmark` itself has ever run -- needed so `docker compose up` has
    something real to bind-mount (an about-to-exist file, mounted read-only, would
    otherwise make Docker create an empty directory in its place). `node
    local-benchmark` regenerates the identical file on its own boot -- a harmless,
    idempotent overwrite once it starts.

    Port layout per node: 1 (consensus_to_consensus) + 3 (primary: primary_to_primary,
    worker_to_primary, metrics) + `workers` * 4 (primary_to_worker, transactions,
    worker_to_worker, metrics) -- primary metrics at node_base+3, worker j's metrics
    at node_base+4+4*j+3.
    """
    block = 4 + workers * 4
    lines = [
        "global:", "  scrape_interval: 1s", "scrape_configs:",
        "  - job_name: 'vantage-local-benchmark'", "    static_configs:",
    ]
    for i in range(nodes):
        node_base = base_port + i * block
        lines += [
            f"      - targets: ['host.docker.internal:{node_base + 3}']",
            "        labels:",
            f"          node: 'node-{i}-primary'",
        ]
        for j in range(workers):
            worker_metrics = node_base + 4 + j * 4 + 3
            lines += [
                f"      - targets: ['host.docker.internal:{worker_metrics}']",
                "        labels:",
                f"          node: 'node-{i}-worker-{j}'",
            ]
    data_dir.mkdir(parents=True, exist_ok=True)
    target_file = data_dir / "prometheus.yaml"
    target_file.write_text("\n".join(lines) + "\n")
    print(f"[dryrun] wrote {target_file} ({nodes} node(s) x {workers} worker(s))")
    return target_file


def wait_for_grafana_health(timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{GRAFANA_URL}/api/health", timeout=2) as resp:
                if resp.status == 200:
                    print("[dryrun] grafana healthy")
                    return True
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(1)
    print("[dryrun] WARNING: grafana did not report healthy within "
          f"{timeout}s -- continuing anyway (it may still be starting up)")
    return False


def start_monitoring(prometheus_config: Path) -> None:
    print(f"[dryrun] docker compose -f {COMPOSE_FILE} up -d "
          f"(PROMETHEUS_CONFIG={prometheus_config}) ...")
    env = dict(os.environ, PROMETHEUS_CONFIG=str(prometheus_config))
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"],
        env=env, check=True,
    )
    wait_for_grafana_health()


def stop_monitoring() -> None:
    print("[dryrun] --down: docker compose down ...")
    subprocess.run(["docker", "compose", "-f", str(COMPOSE_FILE), "down"], check=False)


def open_dashboard() -> None:
    url = f"{GRAFANA_URL}/d/{DASHBOARD_UID}"
    print(f"[dryrun] dashboard: {url}")
    if sys.platform == "darwin":
        subprocess.run(["open", url], check=False)  # best-effort, per spec
    else:
        print("[dryrun] (not macOS -- open the URL above manually)")


def build_local_benchmark_args(cfg: dict, binary: Path) -> list:
    args = [
        str(binary), "local-benchmark",
        "--nodes", str(cfg["nodes"]),
        "--workers", str(cfg["workers"]),
        "--rate", str(cfg["rate"]),
        "--tx-size", str(cfg["tx_size"]),
        "--protocol", str(cfg["protocol"]),
        "--mode", str(cfg["mode"]),
        "--duration", str(cfg["duration"]),
        "--delta-ms", str(cfg["delta_ms"]),
        "--max-batch-delay-ms", str(cfg["max_batch_delay_ms"]),
        "--max-header-delay-ms", str(cfg["max_header_delay_ms"]),
        "--crash", str(cfg["crash"]),
        "--data-dir", str(cfg["data_dir"]),
    ]
    latency_table = str(cfg.get("latency_table") or "none").strip()
    if latency_table.lower() != "none":
        args += ["--latency-table", latency_table]
    if cfg.get("compress_network"):
        args.append("--compress-network")
    # Transport-level batching: optional (not in REQUIRED_KEYS), off by default --
    # byte-identical wire/behavior when omitted, mirroring `compress_network`'s own
    # optional-flag handling just above.
    if cfg.get("batch_messages"):
        args.append("--batch-messages")
        if cfg.get("batch_max_bytes") is not None:
            args += ["--batch-max-bytes", str(cfg["batch_max_bytes"])]
        if cfg.get("batch_max_delay_ms") is not None:
            args += ["--batch-max-delay-ms", str(cfg["batch_max_delay_ms"])]
    return args


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=str(Path(__file__).parent / "config.yml"),
                         help="Path to the run config (default: config.yml next to this script)")
    parser.add_argument("--no-build", action="store_true", help="Skip the cargo build step")
    parser.add_argument("--down", action="store_true",
                         help="Tear the monitoring stack down on exit "
                              "(default: leave it running for post-run inspection)")
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    data_dir = (REPO_ROOT / cfg["data_dir"]).resolve()

    build(args.no_build)

    prometheus_config = generate_prometheus_targets(data_dir, int(cfg["nodes"]), int(cfg["workers"]))
    start_monitoring(prometheus_config)
    open_dashboard()

    binary = REPO_ROOT / "target" / "release" / "node"
    if not binary.exists():
        sys.exit(f"{binary} not found -- run without --no-build first")

    cmd = build_local_benchmark_args(cfg, binary)
    print(f"[dryrun] {' '.join(cmd)}")

    exit_code = 0
    proc = None
    try:
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT)
        exit_code = proc.wait()
    except KeyboardInterrupt:
        # `node local-benchmark` is in the same foreground process group, so it
        # already received the same SIGINT and is doing its own clean shutdown
        # (tokio::signal::ctrl_c(), node/src/local_benchmark.rs) -- just wait for its
        # RESULTS block to print, don't send a second signal unless it hangs.
        print("\n[dryrun] Ctrl-C -- waiting for node local-benchmark's clean shutdown...")
        try:
            exit_code = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            print("[dryrun] did not exit within 30s of Ctrl-C -- terminating")
            proc.terminate()
            exit_code = proc.wait()
    finally:
        print(f"[dryrun] RESULTS printed above; per-node logs/stores/prometheus.yaml under {data_dir}")
        if args.down:
            stop_monitoring()
        else:
            print(f"[dryrun] monitoring stack left running (pass --down to tear it down). "
                  f"Dashboard: {GRAFANA_URL}/d/{DASHBOARD_UID}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
