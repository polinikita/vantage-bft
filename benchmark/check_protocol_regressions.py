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
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary does not exist: {binary}")

    with tempfile.TemporaryDirectory(prefix="vantage-n20-crash6-") as data_dir:
        command = [
            str(binary),
            "local-benchmark",
            "--nodes",
            "20",
            "--crash",
            "6",
            "--rate",
            "1000",
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

    if returncode != 0:
        raise SystemExit(f"n=20/f=6 crash regression exited with status {returncode}")
    if not any("Crash fault: 6 of 20 nodes never spawned" in line for line in lines):
        raise SystemExit("n=20/f=6 crash regression did not activate the permanent fault")
    if not any("real 10-AWS-region RTT matrix" in line for line in lines):
        raise SystemExit("n=20/f=6 crash regression did not use the AWS RTT matrix")

    result = parse_result(lines)
    failures = []
    if result.get("protocol") != "vantage":
        failures.append(f"protocol={result.get('protocol')!r}")
    if int(result.get("offered_tps", "0")) != 1_000:
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
        raise SystemExit("n=20/f=6 permanent-crash regression failed: " + "; ".join(failures))

    print(
        "REGRESSION_PASS vantage-n20-crash6 "
        f"throughput={float(result['throughput_pct']):.1f}% "
        f"p50={float(result['materialized_p50_ms']):.1f}ms"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
