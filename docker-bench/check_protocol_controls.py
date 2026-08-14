#!/usr/bin/env python3
"""Run the n=20 Docker/netem clean, crash, and Byzantine-lane controls."""

from __future__ import annotations

import argparse
import csv
import json
import platform
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DOCKER_DIR = ROOT / "docker-bench"
RUNNER = DOCKER_DIR / "run.sh"
DATA_DIR = DOCKER_DIR / "data"
RESULT_RE = re.compile(r"^DOCKER_BENCH_RESULT\s+(.+)$")
PANIC_RE = re.compile(r"panicked at|thread .* panicked", re.IGNORECASE)


@dataclass(frozen=True)
class Protocol:
    key: str
    label: str
    flags: tuple[str, ...]


PROTOCOLS = (
    Protocol("vantage", "Vantage", ()),
    Protocol(
        "autobahn-optimistic",
        "Autobahn optimistic (all-to-all)",
        ("--all-to-all",),
    ),
    Protocol(
        "autobahn-seamless",
        "Autobahn seamless",
        (),
    ),
    Protocol(
        "simple-it",
        "Simple-IT (Opt-RBC)",
        (),
    ),
    Protocol(
        "simple-it-bracha",
        "Simple-IT (Bracha-RBC)",
        (),
    ),
)

SCENARIOS = ("clean", "crash", "withhold")


def parse_selection(value: str, choices: tuple[str, ...], flag: str) -> list[str]:
    if value == "all":
        return list(choices)
    selected = [item.strip() for item in value.split(",") if item.strip()]
    unknown = sorted(set(selected) - set(choices))
    if unknown:
        raise argparse.ArgumentTypeError(
            f"{flag} contains unknown value(s): {', '.join(unknown)}"
        )
    return [item for item in choices if item in selected]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument("--faults", type=int, default=6)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--protocols", default="all")
    parser.add_argument("--scenarios", default="all")
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--reuse-image",
        action="store_true",
        help="reuse vantage-docker-bench:latest instead of rebuilding the first point",
    )
    parser.add_argument("--min-reachable-throughput-pct", type=float, default=85.0)
    parser.add_argument("--max-p50-ratio", type=float, default=2.0)
    parser.add_argument("--p50-slack-ms", type=float, default=500.0)
    parser.add_argument(
        "--no-latency-gate",
        action="store_true",
        help="record fault/clean latency ratios without failing on them",
    )
    args = parser.parse_args()

    protocol_keys = tuple(protocol.key for protocol in PROTOCOLS)
    try:
        args.protocol_keys = parse_selection(
            args.protocols, protocol_keys, "--protocols"
        )
        args.scenario_keys = parse_selection(args.scenarios, SCENARIOS, "--scenarios")
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))

    if args.nodes < 1:
        parser.error("--nodes must be positive")
    fault_budget = (args.nodes - 1) // 3
    if not 0 <= args.faults <= fault_budget:
        parser.error(
            f"--faults must be between 0 and {fault_budget} for n={args.nodes}"
        )
    if args.rate < 1:
        parser.error("--rate must be positive")
    if args.duration < 20:
        parser.error("--duration must be at least 20 seconds for two metric intervals")
    if not args.protocol_keys:
        parser.error("--protocols selected no protocols")
    if not args.scenario_keys:
        parser.error("--scenarios selected no scenarios")
    if args.faults == 0 and any(key != "clean" for key in args.scenario_keys):
        parser.error("--faults must be positive for crash or withhold scenarios")
    if args.max_p50_ratio < 1:
        parser.error("--max-p50-ratio must be at least 1")
    if args.p50_slack_ms < 0:
        parser.error("--p50-slack-ms must be non-negative")

    if args.output is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.output = ROOT / "benchmark" / "results" / f"docker-controls-{stamp}"
    else:
        args.output = args.output.resolve()
    if args.output.exists():
        parser.error(f"--output already exists: {args.output}")
    return args


def command_output(command: list[str]) -> str:
    try:
        return subprocess.check_output(
            command, cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def image_exists() -> bool:
    return (
        subprocess.run(
            ["docker", "image", "inspect", "vantage-docker-bench:latest"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )


def expected_reachable_tps(rate: int, nodes: int, faults: int, scenario: str) -> float:
    if scenario in ("clean", "crash"):
        return float(rate)
    quotient, remainder = divmod(rate, nodes)
    unavailable = sum(quotient + (index < remainder) for index in range(faults))
    return float(rate - unavailable)


def scenario_flags(scenario: str, nodes: int, faults: int) -> list[str]:
    if scenario == "clean":
        return []
    if scenario == "crash":
        return ["--crash", str(faults)]
    if scenario == "withhold":
        return [
            "--withhold",
            str(faults),
            "--withhold-count",
            str(nodes - faults),
            "--withhold-fixed-receivers",
            "--withhold-repair",
        ]
    raise ValueError(f"unknown scenario {scenario}")


def scan_panics() -> list[str]:
    matches: list[str] = []
    for path in sorted(DATA_DIR.glob("node-*/logs/*.log")):
        try:
            with path.open(errors="replace") as source:
                for line_number, line in enumerate(source, 1):
                    if PANIC_RE.search(line):
                        relative = path.relative_to(DOCKER_DIR)
                        matches.append(f"{relative}:{line_number}: {line.strip()}")
                        if len(matches) >= 100:
                            return matches
        except OSError:
            continue
    return matches


def should_echo(line: str) -> bool:
    stripped = line.lstrip()
    return (
        stripped.startswith("==>")
        or stripped.startswith("TIMELINE:")
        or stripped.startswith("Consensus TPS:")
        or stripped.startswith("Real transaction latency:")
        or stripped.startswith("Materialised transaction latency:")
        or stripped.startswith("DOCKER_BENCH_RESULT")
        or "timed out" in stripped
        or "failed" in stripped.lower()
    )


def run_point(
    args: argparse.Namespace,
    scenario: str,
    protocol: Protocol,
    *,
    no_build: bool,
) -> dict[str, Any]:
    run_id = f"{scenario}-{protocol.key}"
    log_path = args.output / f"{run_id}.log"
    command = [
        str(RUNNER),
        "--nodes",
        str(args.nodes),
        "--rate",
        str(args.rate),
        "--duration",
        str(args.duration),
        "--protocol",
        protocol.key,
        *protocol.flags,
        *scenario_flags(scenario, args.nodes, args.faults),
    ]
    if no_build:
        command.append("--no-build")

    print(
        f"\n[{scenario:8s}] {protocol.label} "
        f"n={args.nodes} f={0 if scenario == 'clean' else args.faults} "
        f"offered={args.rate} TPS",
        flush=True,
    )
    result_payload: dict[str, Any] | None = None
    with log_path.open("w") as log:
        log.write("COMMAND: " + shlex.join(command) + "\n\n")
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
        )
        assert process.stdout is not None
        try:
            for line in process.stdout:
                log.write(line)
                match = RESULT_RE.match(line.rstrip("\n"))
                if match:
                    result_payload = json.loads(match.group(1))
                if should_echo(line):
                    print(line, end="", flush=True)
        except KeyboardInterrupt:
            process.terminate()
            process.wait(timeout=30)
            raise
        returncode = process.wait()

    panic_matches = scan_panics()
    expected_tps = expected_reachable_tps(args.rate, args.nodes, args.faults, scenario)
    record: dict[str, Any] = {
        "run_id": run_id,
        "scenario": scenario,
        "protocol": protocol.key,
        "protocol_label": protocol.label,
        "nodes": args.nodes,
        "faults": 0 if scenario == "clean" else args.faults,
        "offered_tps": args.rate,
        "expected_reachable_tps": expected_tps,
        "expected_live_workers": (
            args.nodes - args.faults if scenario == "crash" else args.nodes
        ),
        "returncode": returncode,
        "panics": len(panic_matches),
        "panic_matches": panic_matches,
        "command": command,
        "log": log_path.name,
    }
    if result_payload is None:
        record["error"] = "missing DOCKER_BENCH_RESULT"
        return record

    real = result_payload["real_latency_ms"]
    materialised = result_payload["materialised_latency_ms"]
    committed_tps = float(result_payload["committed_tps"])
    record.update(
        {
            "measurement_seconds": result_payload["measurement_seconds"],
            "submitted_tps": float(result_payload["submitted_tps"]),
            "committed_tps": committed_tps,
            "reachable_throughput_pct": 100 * committed_tps / expected_tps,
            "reachable_workers": result_payload["reachable_workers"],
            "real_p50_ms": real["p50"],
            "real_p90_ms": real["p90"],
            "real_p99_ms": real["p99"],
            "materialised_p50_ms": materialised["p50"],
            "materialised_p90_ms": materialised["p90"],
            "materialised_p99_ms": materialised["p99"],
        }
    )
    return record


def evaluate(args: argparse.Namespace, records: list[dict[str, Any]]) -> None:
    clean_p50 = {
        record["protocol"]: record.get("materialised_p50_ms")
        for record in records
        if record["scenario"] == "clean"
    }
    for record in records:
        failures: list[str] = []
        throughput = record.get("reachable_throughput_pct")
        if record["returncode"] != 0:
            failures.append(f"exit={record['returncode']}")
        if record.get("error"):
            failures.append(record["error"])
        if record["panics"]:
            failures.append(f"panics={record['panics']}")
        reachable_workers = record.get("reachable_workers")
        if reachable_workers != record["expected_live_workers"]:
            failures.append(
                f"workers={reachable_workers}/{record['expected_live_workers']}"
            )
        if throughput is None or throughput < args.min_reachable_throughput_pct:
            failures.append(
                "throughput="
                + ("missing" if throughput is None else f"{throughput:.1f}%")
            )

        baseline = clean_p50.get(record["protocol"])
        current = record.get("materialised_p50_ms")
        if current is None:
            failures.append("materialised-p50=missing")
        ratio = None
        latency_limit = None
        if record["scenario"] != "clean" and baseline and current is not None:
            ratio = current / baseline
            latency_limit = max(
                baseline * args.max_p50_ratio, baseline + args.p50_slack_ms
            )
            if not args.no_latency_gate and current > latency_limit:
                failures.append(
                    f"materialised-p50={current:.1f}ms>{latency_limit:.1f}ms"
                )
        record["materialised_p50_vs_clean"] = ratio
        record["materialised_p50_limit_ms"] = latency_limit
        record["passed"] = not failures
        record["failures"] = failures


CSV_FIELDS = (
    "scenario",
    "protocol",
    "nodes",
    "faults",
    "offered_tps",
    "expected_reachable_tps",
    "submitted_tps",
    "committed_tps",
    "reachable_throughput_pct",
    "real_p50_ms",
    "real_p90_ms",
    "real_p99_ms",
    "materialised_p50_ms",
    "materialised_p90_ms",
    "materialised_p99_ms",
    "materialised_p50_vs_clean",
    "panics",
    "passed",
    "log",
)


def fmt(value: Any, digits: int = 1) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def save(
    args: argparse.Namespace, metadata: dict[str, Any], records: list[dict[str, Any]]
) -> None:
    evaluate(args, records)
    (args.output / "records.json").write_text(
        json.dumps({"metadata": metadata, "records": records}, indent=2) + "\n"
    )
    with (args.output / "records.csv").open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    lines = [
        "# Docker/netem protocol controls",
        "",
        f"Commit: `{metadata['commit']}`",
        "",
        "| Scenario | Protocol | Committed TPS | Reachable target | Target % | "
        "Materialized p50 / p99 | p50 vs clean | Panics | Result |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for record in records:
        result = "PASS" if record.get("passed") else "FAIL"
        if record.get("failures"):
            result += ": " + ", ".join(record["failures"])
        ratio = record.get("materialised_p50_vs_clean")
        lines.append(
            f"| {record['scenario']} | {record['protocol_label']} | "
            f"{fmt(record.get('committed_tps'))} | "
            f"{fmt(record['expected_reachable_tps'])} | "
            f"{fmt(record.get('reachable_throughput_pct'))}% | "
            f"{fmt(record.get('materialised_p50_ms'))} / "
            f"{fmt(record.get('materialised_p99_ms'))} ms | "
            f"{('-' if ratio is None else f'{ratio:.2f}x')} | "
            f"{record['panics']} | {result} |"
        )
    (args.output / "summary.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "commit": command_output(["git", "rev-parse", "HEAD"]),
        "branch": command_output(["git", "branch", "--show-current"]),
        "platform": platform.platform(),
        "docker": command_output(
            ["docker", "version", "--format", "{{.Server.Version}}"]
        ),
        "nodes": args.nodes,
        "faults": args.faults,
        "rate": args.rate,
        "duration": args.duration,
        "protocols": args.protocol_keys,
        "scenarios": args.scenario_keys,
        "latency": "AWS RTT matrix via per-destination tc netem",
        "withholding": (
            f"first {args.faults} validators send lane data only among themselves "
            f"and refuse repair to the other {args.nodes - args.faults}"
        ),
        "min_reachable_throughput_pct": args.min_reachable_throughput_pct,
        "max_p50_ratio": args.max_p50_ratio,
        "p50_slack_ms": args.p50_slack_ms,
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    selected_protocols = [p for p in PROTOCOLS if p.key in args.protocol_keys]
    records: list[dict[str, Any]] = []
    reuse_image = args.reuse_image
    for scenario in args.scenario_keys:
        for protocol in selected_protocols:
            record = run_point(args, scenario, protocol, no_build=reuse_image)
            records.append(record)
            reuse_image = image_exists()
            save(args, metadata, records)
            if record.get("committed_tps") is not None:
                print(
                    f"  recorded {record['committed_tps']:.1f} TPS, "
                    f"materialized p50={fmt(record.get('materialised_p50_ms'))} ms, "
                    f"panics={record['panics']}",
                    flush=True,
                )
            else:
                print(f"  no result; see {record['log']}", flush=True)

    save(args, metadata, records)
    passed = all(record.get("passed", False) for record in records)
    print(f"\nResults: {args.output}")
    print("DOCKER_CONTROL_MATRIX_PASS" if passed else "DOCKER_CONTROL_MATRIX_FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
