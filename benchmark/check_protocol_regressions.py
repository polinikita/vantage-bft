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
PROTOCOLS = (
    "vantage",
    "autobahn-optimistic",
    "autobahn-seamless",
    "simple-it",
    "simple-it-bracha",
)
PROTOCOL_ARGS = {
    "vantage": (),
    # Autobahn's liveness proof uses a conservative 10*Delta round timer.
    # local-benchmark's default Delta is 200 ms.
    "autobahn-optimistic": ("--all-to-all", "--timeout-delay-ms", "2000"),
    "autobahn-seamless": ("--timeout-delay-ms", "2000"),
    "simple-it": (),
    "simple-it-bracha": (),
}
PORT_STRIDE = 1_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/node"))
    parser.add_argument(
        "--protocols",
        default="all",
        help="Comma-separated protocol names, or 'all' (default)",
    )
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument("--crash", type=int, default=6)
    parser.add_argument("--rate", type=int, default=1_000)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--base-port", type=int, default=19_600)
    parser.add_argument("--min-throughput-pct", type=float, default=50.0)
    parser.add_argument("--max-p50-ms", type=float, default=15_000.0)
    return parser.parse_args()


def parse_protocols(value: str) -> tuple[str, ...]:
    if value == "all":
        return PROTOCOLS
    protocols = tuple(part.strip() for part in value.split(",") if part.strip())
    if not protocols:
        raise SystemExit("--protocols must name at least one protocol")
    unknown = [protocol for protocol in protocols if protocol not in PROTOCOL_ARGS]
    if unknown:
        raise SystemExit(
            "unknown --protocols value(s): "
            + ", ".join(unknown)
            + "; choose from "
            + ", ".join(PROTOCOLS)
        )
    if len(set(protocols)) != len(protocols):
        raise SystemExit("--protocols must not contain duplicates")
    return protocols


def parse_result(lines: list[str]) -> dict[str, str]:
    result_line = next((line for line in lines if line.startswith(RESULT_PREFIX)), None)
    if result_line is None:
        raise RuntimeError("benchmark emitted no BENCHMARK_RESULT line")
    return dict(re.findall(r"([a-z0-9_]+)=([^\s]+)", result_line))


def run_protocol(
    args: argparse.Namespace,
    binary: Path,
    protocol: str,
    base_port: int,
) -> list[str]:
    scenario = f"{protocol}-n{args.nodes}-crash{args.crash}"
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
            protocol,
            "--warmup",
            str(args.warmup),
            "--duration",
            str(args.duration),
            "--base-port",
            str(base_port),
            "--data-dir",
            data_dir,
            *PROTOCOL_ARGS[protocol],
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

    failures = []
    if returncode != 0:
        failures.append(f"exit_status={returncode}")

    fault_message = f"Crash fault: {args.crash} of {args.nodes} nodes never spawned"
    if not any(fault_message in line for line in lines):
        failures.append("permanent fault not activated")
    if not any("real 10-AWS-region RTT matrix" in line for line in lines):
        failures.append("AWS RTT matrix not activated")
    if any("panicked at" in line for line in lines):
        failures.append("process output contains a panic")

    try:
        result = parse_result(lines)
    except RuntimeError as error:
        failures.append(str(error))
        result = {}

    if result.get("protocol") != protocol:
        failures.append(f"protocol={result.get('protocol')!r}")
    if int(result.get("offered_tps", "0")) != args.rate:
        failures.append(f"offered_tps={result.get('offered_tps')!r}")
    if int(result.get("panics", "-1")) != 0:
        failures.append(f"panics={result.get('panics')!r}")
    if float(result.get("throughput_pct", "0")) < args.min_throughput_pct:
        failures.append(
            f"throughput_pct={result.get('throughput_pct')!r} "
            f"< {args.min_throughput_pct:g}"
        )
    if float(result.get("materialized_p50_ms", "inf")) > args.max_p50_ms:
        failures.append(
            f"materialized_p50_ms={result.get('materialized_p50_ms')!r} "
            f"> {args.max_p50_ms:g}"
        )

    if failures:
        print(f"REGRESSION_FAIL {scenario}: " + "; ".join(failures))
        return failures

    print(
        f"REGRESSION_PASS {scenario} "
        f"throughput={float(result['throughput_pct']):.1f}% "
        f"p50={float(result['materialized_p50_ms']):.1f}ms"
    )
    return []


def main() -> int:
    args = parse_args()
    protocols = parse_protocols(args.protocols)
    if args.nodes < 4:
        raise SystemExit("--nodes must be at least 4")
    if not 0 <= args.crash < args.nodes:
        raise SystemExit("--crash must be in [0, nodes)")
    if args.nodes < 3 * args.crash + 1:
        raise SystemExit("--nodes must satisfy n >= 3f + 1 for --crash=f")
    if args.rate <= 0:
        raise SystemExit("--rate must be positive")
    if args.warmup < 0 or args.duration <= 0:
        raise SystemExit("--warmup must be non-negative and --duration must be positive")
    if not 0.0 <= args.min_throughput_pct <= 100.0:
        raise SystemExit("--min-throughput-pct must be in [0, 100]")
    if args.max_p50_ms <= 0.0:
        raise SystemExit("--max-p50-ms must be positive")
    final_port = args.base_port + PORT_STRIDE * (len(protocols) - 1)
    if not 1_024 <= args.base_port <= final_port <= 64_000:
        raise SystemExit("--base-port range must stay within [1024, 64000]")

    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"benchmark binary does not exist: {binary}")

    failed = []
    for index, protocol in enumerate(protocols):
        failures = run_protocol(
            args,
            binary,
            protocol,
            args.base_port + PORT_STRIDE * index,
        )
        if failures:
            failed.append(protocol)

    if failed:
        raise SystemExit("permanent-crash regressions failed: " + ", ".join(failed))
    print("REGRESSION_MATRIX_PASS protocols=" + ",".join(protocols))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
