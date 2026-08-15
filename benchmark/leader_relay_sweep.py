#!/usr/bin/env python3
"""Run the n=20 Docker/netem optimistic leader-relay load sweep."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "docker-bench/run.sh"
PLOTTER = ROOT / "benchmark/plot_leader_burden.py"
RESULT_RE = re.compile(r"^DOCKER_BENCH_RESULT\s+(.*)$", re.MULTILINE)
DEFAULT_RATES = (1_000, 2_000, 5_000, 10_000, 20_000, 40_000, 80_000)


@dataclass(frozen=True)
class Protocol:
    key: str
    label: str
    cli: tuple[str, ...] = ()


PROTOCOLS = (
    Protocol("autobahn-optimistic", "Autobahn optimistic (all-to-all)", ("--all-to-all",)),
    Protocol("vantage", "Vantage"),
    Protocol("simple-it", "Simple-IT (Opt-RBC)"),
    Protocol("autobahn-seamless", "Autobahn seamless (control)"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument("--faults", type=int, default=6)
    parser.add_argument("--rates", default=",".join(map(str, DEFAULT_RATES)))
    parser.add_argument("--duration", type=int, default=70)
    parser.add_argument("--tx-size", type=int, default=512)
    parser.add_argument("--egress-mbps", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--accept-pct", type=float, default=95.0)
    parser.add_argument(
        "--protocols",
        default="autobahn-optimistic,vantage,simple-it",
        help="comma-separated protocol keys; add autobahn-seamless for the diagnostic control",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="20-second, 1,000 TPS smoke run with one repetition",
    )
    args = parser.parse_args()
    if args.quick:
        args.rates = "1000"
        args.duration = 20
        args.repeats = 1
    try:
        args.rates = sorted(
            {int(value.strip().replace("_", "")) for value in args.rates.split(",")}
        )
    except ValueError as error:
        parser.error(f"invalid --rates: {error}")
    selected = {item.strip() for item in args.protocols.split(",") if item.strip()}
    known = {protocol.key for protocol in PROTOCOLS}
    if selected - known:
        parser.error(f"unknown protocol(s): {', '.join(sorted(selected - known))}")
    args.protocol_defs = [protocol for protocol in PROTOCOLS if protocol.key in selected]
    if not args.protocol_defs:
        parser.error("--protocols selected no protocols")
    if args.nodes < 4 or args.faults != (args.nodes - 1) // 3:
        parser.error("this experiment requires the maximal f=floor((n-1)/3) fault set")
    if any(rate <= 0 for rate in args.rates):
        parser.error("--rates must contain positive values")
    if args.duration < 20:
        parser.error("--duration must be at least 20 seconds (the first 10 seconds are discarded)")
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    if args.egress_mbps <= 0:
        parser.error("--egress-mbps must be positive and explicitly disclosed")
    if args.output is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.output = ROOT / "benchmark/results" / f"leader-relay-n{args.nodes}-{stamp}"
    return args


def command_output(command: list[str]) -> str:
    try:
        return subprocess.check_output(
            command, cwd=ROOT, text=True, stderr=subprocess.STDOUT
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        return f"unavailable: {error}"


def build_image() -> None:
    print("Building the release Docker image once...", flush=True)
    subprocess.run(
        [
            "docker",
            "build",
            "-f",
            str(ROOT / "docker-bench/Dockerfile"),
            "-t",
            "vantage-docker-bench:latest",
            str(ROOT),
        ],
        cwd=ROOT,
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
        check=True,
    )


def parse_result(output: str) -> dict[str, Any]:
    matches = RESULT_RE.findall(output)
    if len(matches) != 1:
        raise RuntimeError(f"expected one DOCKER_BENCH_RESULT line, found {len(matches)}")
    return json.loads(matches[0])


def retain_run_artifacts(destination: Path) -> None:
    source = ROOT / "docker-bench/data"
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "parameters.json", "committee.json", "prometheus.yaml"):
        path = source / name
        if path.is_file():
            shutil.copy2(path, destination / name)
    for node_dir in sorted(source.glob("node-*")):
        retained = destination / node_dir.name
        retained.mkdir(exist_ok=True)
        for name in ("tc-setup.sh",):
            path = node_dir / name
            if path.is_file():
                shutil.copy2(path, retained / name)
        logs = node_dir / "logs"
        if logs.is_dir():
            shutil.copytree(logs, retained / "logs", dirs_exist_ok=True)


class Study:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.records: list[dict[str, Any]] = []
        self.first_failed: dict[str, int | None] = {}

    def save(self) -> None:
        (self.args.output / "measurements.json").write_text(
            json.dumps(self.records, indent=2, sort_keys=True) + "\n"
        )
        if not self.records:
            return
        fields = sorted({key for record in self.records for key in record})
        with (self.args.output / "measurements.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(self.records)

    def matching(self, protocol: Protocol, rate: int) -> list[dict[str, Any]]:
        return [
            record
            for record in self.records
            if record["protocol"] == protocol.key and record["offered_tps"] == rate
        ]

    def ensure(self, protocol: Protocol, rate: int, count: int) -> None:
        records = self.matching(protocol, rate)
        while len(records) < count:
            records.append(self.run_one(protocol, rate, len(records)))

    def run_one(self, protocol: Protocol, rate: int, repetition: int) -> dict[str, Any]:
        run_id = f"{protocol.key}-{rate}-r{repetition + 1}"
        log_path = self.args.output / "raw" / f"{run_id}.log"
        command = [
            str(RUNNER),
            "--nodes",
            str(self.args.nodes),
            "--rate",
            str(rate),
            "--duration",
            str(self.args.duration),
            "--protocol",
            protocol.key,
            "--no-build",
            "--tx-size",
            str(self.args.tx_size),
            "--withhold",
            str(self.args.faults),
            "--withhold-publisher-stride",
            "1",
            "--leader-relay",
            "--egress-mbps",
            str(self.args.egress_mbps),
            *protocol.cli,
        ]
        print(
            f"[relay] {protocol.label:34s} total offered={rate:>7,d} TPS "
            f"r={repetition + 1}",
            flush=True,
        )
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=self.args.duration + 300,
        )
        log_path.write_text("COMMAND: " + " ".join(command) + "\n\n" + completed.stdout)
        retain_run_artifacts(self.args.output / "raw" / run_id)
        if completed.returncode != 0:
            raise RuntimeError(f"{run_id} exited {completed.returncode}; see {log_path}")
        try:
            result = parse_result(completed.stdout)
        except RuntimeError as error:
            raise RuntimeError(f"{error}; see {log_path}") from error

        honest_offered = float(result["honest_offered_tps"])
        committed = float(result["committed_tps"])
        delivery_pct = 100.0 * committed / honest_offered if honest_offered else 0.0
        commit_latency = result["real_latency_ms"]
        materialised_latency = result["materialised_latency_ms"]
        record = {
            "run_id": run_id,
            "protocol": protocol.key,
            "protocol_label": protocol.label,
            "offered_tps": rate,
            "honest_offered_tps": honest_offered,
            "committed_tps": committed,
            "honest_delivery_pct": delivery_pct,
            "accepted": delivery_pct >= self.args.accept_pct,
            "repetition": repetition,
            "measurement_seconds": result["measurement_seconds"],
            "p50_ms": materialised_latency["p50"] or commit_latency["p50"] or 0.0,
            "p90_ms": materialised_latency["p90"] or commit_latency["p90"] or 0.0,
            "p99_ms": materialised_latency["p99"] or commit_latency["p99"] or 0.0,
            "commit_p50_ms": commit_latency["p50"] or 0.0,
            "commit_p99_ms": commit_latency["p99"] or 0.0,
            "prepare_sync_events": result["prepare_sync_events"],
            "prepare_missing_headers": result["prepare_missing_headers"],
            "prepare_sync_completed": result["prepare_sync_completed"],
            "prepare_sync_mean_wait_ms": result["prepare_sync_mean_wait_ms"],
            "optimistic_batch_relay_mbps": result["optimistic_batch_relay_mbps"],
            "max_node_optimistic_batch_relay_mbps": result[
                "max_node_optimistic_batch_relay_mbps"
            ],
            "max_node_wire_mbps": result["max_node_wire_mbps"],
            "log": str(log_path.relative_to(self.args.output)),
        }
        self.records.append(record)
        self.save()
        verdict = "PASS" if record["accepted"] else "KNEE"
        print(
            f"  {verdict}: honest {committed:,.0f}/{honest_offered:,.0f} TPS "
            f"({delivery_pct:.1f}%), p50/p99={record['p50_ms']:.0f}/"
            f"{record['p99_ms']:.0f} ms, peak egress "
            f"{record['max_node_wire_mbps']:.1f} Mbit/s",
            flush=True,
        )
        return record

    def pilot(self) -> None:
        for protocol in self.args.protocol_defs:
            self.first_failed[protocol.key] = None
            failures = 0
            for rate in self.args.rates:
                self.ensure(protocol, rate, 1)
                if not self.matching(protocol, rate)[0]["accepted"]:
                    failures += 1
                    if self.first_failed[protocol.key] is None:
                        self.first_failed[protocol.key] = rate
                    if failures >= 2:
                        break

    def repeat_boundaries(self) -> None:
        if self.args.repeats <= 1:
            return
        for protocol in self.args.protocol_defs:
            tested = [rate for rate in self.args.rates if self.matching(protocol, rate)]
            failed = self.first_failed[protocol.key]
            boundary = {tested[-1]} if tested else set()
            accepted = [rate for rate in tested if self.matching(protocol, rate)[0]["accepted"]]
            if accepted:
                boundary.add(accepted[-1])
            if failed is not None:
                boundary.add(failed)
            for rate in sorted(boundary):
                self.ensure(protocol, rate, self.args.repeats)


def write_provenance(args: argparse.Namespace) -> None:
    payload = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "argv": sys.argv,
        "git_head": command_output(["git", "rev-parse", "HEAD"]),
        "git_status": command_output(["git", "status", "--short"]),
        "docker": command_output(["docker", "version", "--format", "{{.Server.Version}}"]),
        "python": sys.version,
        "platform": platform.platform(),
        "fault_model": (
            "uniform total load; f Byzantine lane authors mark their normal load uncounted, "
            "broadcast headers, narrowcast each batch to the Byzantine cohort plus an f-wide "
            "correct group (2f direct holders, one below quorum), aggregate one batch per Delta, "
            "advance the receiver group by f after five batches (a 5-Delta epoch), and refuse "
            "repair; Byzantine consensus leaders propose certified cuts while honest leaders "
            "serve optimistic repair normally"
        ),
        "latency_model": "ten-region AWS RTT matrix applied as one-way tc netem delays",
        "arguments": {
            key: ([item.key for item in value] if key == "protocol_defs" else value)
            for key, value in vars(args).items()
        },
    }
    payload["arguments"] = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in payload["arguments"].items()
    }
    (args.output / "provenance.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )


def main() -> int:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "raw").mkdir()
    if not args.no_build:
        build_image()
    study = Study(args)
    study.pilot()
    study.repeat_boundaries()
    write_provenance(args)
    subprocess.run(
        [
            sys.executable,
            str(PLOTTER),
            str(args.output / "measurements.csv"),
            "--output",
            str(args.output / "leader-relay.png"),
            "--subtitle",
            (
                f"n={args.nodes}, f={args.faults} · uniform total load · AWS RTT netem · "
                "faulty batches reach 2f direct holders (<2f+1 quorum) · "
                f"{args.egress_mbps:,} Mbit/s/validator cap"
            ),
        ],
        cwd=ROOT,
        check=True,
    )
    print(f"Study complete: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
