#!/usr/bin/env python3
"""Run mandatory local protocol regressions against a benchmark-enabled node."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile


RESULT_PREFIX = "BENCHMARK_RESULT "


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/node"))
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument("--crash", type=int, default=6)
    parser.add_argument("--rate", type=int, default=1_000)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--base-port", type=int, default=19_600)
    return parser.parse_args()


def parse_result(lines: list[str]) -> dict[str, str]:
    result_line = next((line for line in lines if line.startswith(RESULT_PREFIX)), None)
    if result_line is None:
        raise RuntimeError("benchmark emitted no BENCHMARK_RESULT line")
    return dict(re.findall(r"([a-z0-9_]+)=([^\s]+)", result_line))


def main() -> int:
    args = parse_args()
    if args.nodes < 4:
        raise SystemExit("--nodes must be at least 4")
    if not 0 <= args.crash < args.nodes:
        raise SystemExit("--crash must be in [0, nodes)")
    if args.nodes < 3 * args.crash + 1:
        raise SystemExit("--nodes must satisfy n >= 3f + 1 for --crash=f")
    if args.rate <= 0:
        raise SystemExit("--rate must be positive")

    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary does not exist: {binary}")

    scenario = f"vantage-n{args.nodes}-crash{args.crash}"
    with tempfile.TemporaryDirectory(prefix=f"{scenario}-") as data_dir:
        command = [
            str(binary),
            "local-benchmark",
            "--nodes",
            str(args.nodes),
            "--crash",
            str(args.crash),
            "--rate",
            str(args.rate),
            "--protocol",
            "vantage",
            "--warmup",
            str(args.warmup),
            "--duration",
            str(args.duration),
            "--base-port",
            str(args.base_port),
            "--data-dir",
            data_dir,
        ]
        env = {**os.environ, "RUST_LOG": "error"}
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        lines = []
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            lines.append(line.rstrip("\n"))
        returncode = process.wait()

    label = f"n={args.nodes}/f={args.crash}"
    if returncode != 0:
        raise SystemExit(f"{label} crash regression exited with status {returncode}")
    fault_message = (
        f"Crash fault: {args.crash} of {args.nodes} nodes never spawned"
    )
    if not any(fault_message in line for line in lines):
        raise SystemExit(f"{label} crash regression did not activate the permanent fault")
    if not any("real 10-AWS-region RTT matrix" in line for line in lines):
        raise SystemExit(f"{label} crash regression did not use the AWS RTT matrix")

    result = parse_result(lines)
    failures = []
    if result.get("protocol") != "vantage":
        failures.append(f"protocol={result.get('protocol')!r}")
    if int(result.get("offered_tps", "0")) != args.rate:
        failures.append(f"offered_tps={result.get('offered_tps')!r}")
    if int(result.get("panics", "-1")) != 0:
        failures.append(f"panics={result.get('panics')!r}")
    if float(result.get("throughput_pct", "0")) < 85.0:
        failures.append(f"throughput_pct={result.get('throughput_pct')!r} < 85")
    if float(result.get("materialized_p50_ms", "inf")) > 5_000.0:
        failures.append(
            f"materialized_p50_ms={result.get('materialized_p50_ms')!r} > 5000"
        )
    if failures:
        raise SystemExit(
            f"{label} permanent-crash regression failed: " + "; ".join(failures)
        )

    print(
        f"REGRESSION_PASS {scenario} "
        f"throughput={float(result['throughput_pct']):.1f}% "
        f"p50={float(result['materialized_p50_ms']):.1f}ms"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
