#!/usr/bin/env python3
"""Run and plot the n=10 clean/late-header five-protocol local study."""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RATES = [
    100,
    1_000,
    2_500,
    5_000,
    10_000,
    20_000,
    40_000,
    80_000,
    160_000,
    240_000,
    320_000,
]
RESULT_RE = re.compile(r"^BENCHMARK_RESULT\s+(.*)$", re.MULTILINE)
TICK_RE = re.compile(
    r"^MEASUREMENT_TICK sec=(\d+) submitted=(\d+) committed=(\d+) backlog=(\d+)$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class Protocol:
    key: str
    label: str
    cli: tuple[str, ...]
    color: str
    marker: str


PROTOCOLS = [
    Protocol("vantage", "Vantage", ("--protocol", "vantage"), "#0072B2", "o"),
    Protocol(
        "autobahn-optimistic",
        "Autobahn optimistic (all-to-all)",
        ("--protocol", "autobahn-optimistic", "--all-to-all"),
        "#D55E00",
        "s",
    ),
    Protocol(
        "autobahn-seamless",
        "Autobahn seamless",
        ("--protocol", "autobahn-seamless"),
        "#E69F00",
        "^",
    ),
    Protocol(
        "simple-it",
        "Simple-IT (Opt-RBC)",
        ("--protocol", "simple-it"),
        "#009E73",
        "D",
    ),
    Protocol(
        "simple-it-bracha",
        "Simple-IT (Bracha-RBC)",
        ("--protocol", "simple-it-bracha"),
        "#CC79A7",
        "v",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/node")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--tx-size", type=int, default=512)
    parser.add_argument("--mode", default="random", choices=("random", "all-zero"))
    parser.add_argument("--rates", default=",".join(map(str, DEFAULT_RATES)))
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--late-publishers", type=int, default=3)
    parser.add_argument("--late-receivers", type=int, default=3)
    parser.add_argument("--late-delay-ms", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--accept-pct", type=float, default=95.0)
    parser.add_argument("--slope-fraction", type=float, default=0.01)
    parser.add_argument("--slope-floor", type=float, default=100.0)
    parser.add_argument("--base-port", type=int, default=14_000)
    parser.add_argument("--protocols", default=",".join(p.key for p in PROTOCOLS))
    parser.add_argument("--widths", default="0,1,2,3")
    parser.add_argument("--no-width", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Smoke mode: 1k TPS, 1 s warmup, 2 s measurement, one repetition",
    )
    args = parser.parse_args()
    if args.quick:
        args.rates = "1000"
        args.warmup = 1
        args.duration = 2
        args.repeats = 1
        args.widths = "0,3"
    args.rates = sorted(
        {int(value.replace("_", "")) for value in args.rates.split(",")}
    )
    args.widths = sorted({int(value) for value in args.widths.split(",")})
    selected = {value.strip() for value in args.protocols.split(",") if value.strip()}
    args.protocol_defs = [p for p in PROTOCOLS if p.key in selected]
    unknown = selected - {p.key for p in PROTOCOLS}
    if unknown:
        parser.error(f"unknown protocol(s): {', '.join(sorted(unknown))}")
    if not args.protocol_defs:
        parser.error("--protocols selected no protocols")
    if not args.rates or any(rate <= 0 for rate in args.rates):
        parser.error("--rates must contain positive integers")
    if not args.no_width and (
        not args.widths or any(width < 0 for width in args.widths)
    ):
        parser.error("--widths must contain non-negative integers")
    if args.repeats < 1 or args.warmup < 0 or args.duration <= 0:
        parser.error(
            "--repeats must be positive, --warmup non-negative, and --duration positive"
        )
    if args.late_publishers <= 0 or args.late_receivers <= 0 or args.late_delay_ms <= 0:
        parser.error("late publishers, receivers, and delay must all be positive")
    if args.nodes != 10:
        parser.error("this study is defined for --nodes 10")
    if args.late_publishers + max(args.widths + [args.late_receivers]) > args.nodes:
        parser.error("late publisher and receiver groups must be disjoint and fit in n")
    if args.output is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.output = ROOT / "benchmark/results" / f"local-five-protocol-{stamp}"
    return args


def command_output(command: list[str]) -> str:
    try:
        return subprocess.check_output(
            command, cwd=ROOT, text=True, stderr=subprocess.STDOUT
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        return f"unavailable: {error}"


def build(args: argparse.Namespace) -> None:
    if args.no_build:
        return
    print("Building release benchmark binary...", flush=True)
    subprocess.run(
        ["cargo", "build", "--release", "-p", "node", "--features", "benchmark"],
        cwd=ROOT,
        check=True,
    )


def parse_value(value: str) -> Any:
    try:
        return float(value) if any(char in value for char in ".eE") else int(value)
    except ValueError:
        return value


def parse_result(output: str) -> dict[str, Any]:
    matches = RESULT_RE.findall(output)
    if len(matches) != 1:
        raise RuntimeError(f"expected one BENCHMARK_RESULT line, found {len(matches)}")
    return {
        key: parse_value(value)
        for key, value in (token.split("=", 1) for token in matches[0].split())
    }


def parse_ticks(output: str) -> list[dict[str, int]]:
    return [
        {
            "sec": int(sec),
            "submitted": int(submitted),
            "committed": int(committed),
            "backlog": int(backlog),
        }
        for sec, submitted, committed, backlog in TICK_RE.findall(output)
    ]


def latter_half_slope(ticks: list[dict[str, int]]) -> float:
    points = ticks[len(ticks) // 2 :]
    if len(points) < 2:
        return 0.0
    xs = [float(point["sec"]) for point in points]
    ys = [float(point["backlog"]) for point in points]
    x_mean = statistics.fmean(xs)
    y_mean = statistics.fmean(ys)
    denominator = sum((x - x_mean) ** 2 for x in xs)
    return (
        0.0
        if denominator == 0
        else sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denominator
    )


class Study:
    def __init__(self, args: argparse.Namespace, data_root: Path):
        self.args = args
        self.data_root = data_root
        self.records: list[dict[str, Any]] = []
        self.curve_rates: dict[tuple[str, str], list[int]] = {}
        self.first_failed: dict[tuple[str, str], int | None] = {}
        self.measurements_path = args.output / "measurements.json"

    def save(self) -> None:
        self.measurements_path.write_text(
            json.dumps(self.records, indent=2, sort_keys=True) + "\n"
        )

    def matching(
        self, protocol: Protocol, rate: int, receivers: int
    ) -> list[dict[str, Any]]:
        publishers = self.args.late_publishers if receivers else 0
        return [
            record
            for record in self.records
            if record["protocol"] == protocol.key
            and record["offered_tps"] == rate
            and record["late_publishers"] == publishers
            and record["late_receivers"] == receivers
        ]

    def ensure(
        self, protocol: Protocol, rate: int, receivers: int, count: int, phase: str
    ) -> list[dict[str, Any]]:
        records = self.matching(protocol, rate, receivers)
        while len(records) < count:
            records.append(self.run_one(protocol, rate, receivers, len(records), phase))
        return records

    def run_one(
        self, protocol: Protocol, rate: int, receivers: int, repetition: int, phase: str
    ) -> dict[str, Any]:
        publishers = self.args.late_publishers if receivers else 0
        condition = "clean" if receivers == 0 else f"late-k{receivers}"
        run_id = f"{phase}-{condition}-{protocol.key}-{rate}-r{repetition}"
        data_dir = self.data_root / run_id
        log_path = self.args.output / "raw" / f"{run_id}.log"
        command = [
            str(self.args.binary),
            "local-benchmark",
            "--nodes",
            str(self.args.nodes),
            "--workers",
            str(self.args.workers),
            "--rate",
            str(rate),
            "--tx-size",
            str(self.args.tx_size),
            "--mode",
            self.args.mode,
            "--warmup",
            str(self.args.warmup),
            "--duration",
            str(self.args.duration),
            "--base-port",
            str(self.args.base_port),
            "--data-dir",
            str(data_dir),
            "--delta-ms",
            "200",
            "--late-header-publishers",
            str(publishers),
            "--late-header-receivers",
            str(receivers),
            "--late-header-delay-ms",
            str(self.args.late_delay_ms),
            "--timeline",
            *protocol.cli,
        ]
        print(
            f"[{phase}] {condition:8s} {protocol.label:34s} offered={rate:>7,d} TPS r={repetition + 1}",
            flush=True,
        )
        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.monotonic()
        completed = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={**os.environ, "RUST_LOG": "error", "RUST_BACKTRACE": "1"},
            timeout=self.args.warmup + self.args.duration + 180,
        )
        wall_seconds = time.monotonic() - started
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_seconds = (usage_after.ru_utime + usage_after.ru_stime) - (
            usage_before.ru_utime + usage_before.ru_stime
        )
        log_path.write_text("COMMAND: " + " ".join(command) + "\n\n" + completed.stdout)
        if not self.args.keep_data:
            shutil.rmtree(data_dir, ignore_errors=True)
        if completed.returncode != 0:
            raise RuntimeError(
                f"{run_id} exited {completed.returncode}; see {log_path}"
            )
        try:
            result = parse_result(completed.stdout)
        except RuntimeError as error:
            raise RuntimeError(f"{error}; see {log_path}") from error
        ticks = parse_ticks(completed.stdout)
        slope = latter_half_slope(ticks)
        slope_limit = max(self.args.slope_floor, rate * self.args.slope_fraction)
        accepted = (
            float(result["throughput_pct"]) >= self.args.accept_pct
            and slope <= slope_limit
            and int(result.get("panics", 0)) == 0
        )
        record = {
            "run_id": run_id,
            "phase": phase,
            "protocol": protocol.key,
            "protocol_label": protocol.label,
            "condition": condition,
            "late_publishers": publishers,
            "late_receivers": receivers,
            "late_delay_ms": self.args.late_delay_ms if receivers else 0,
            "offered_tps": rate,
            "repetition": repetition,
            "accepted": accepted,
            "backlog_slope_tps": slope,
            "backlog_slope_limit_tps": slope_limit,
            "wall_seconds": wall_seconds,
            "cpu_seconds": cpu_seconds,
            "average_cpu_cores": cpu_seconds / wall_seconds if wall_seconds else 0.0,
            "ticks": ticks,
            "result": result,
            "command": command,
            "log": str(log_path.relative_to(self.args.output)),
        }
        self.records.append(record)
        self.save()
        verdict = "PASS" if accepted else "FAIL"
        print(
            f"  {verdict}: {float(result['committed_tps']):,.0f} TPS "
            f"({float(result['throughput_pct']):.1f}%), backlog slope {slope:,.1f} tx/s",
            flush=True,
        )
        return record

    def pilot_scaling(self) -> None:
        for receivers, condition in ((0, "clean"), (self.args.late_receivers, "late")):
            for protocol in self.args.protocol_defs:
                key = (condition, protocol.key)
                self.curve_rates[key] = []
                self.first_failed[key] = None
                for rate in self.args.rates:
                    record = self.ensure(protocol, rate, receivers, 1, "scaling")[0]
                    self.curve_rates[key].append(rate)
                    if not record["accepted"]:
                        self.first_failed[key] = rate
                        break

    def repeat_boundaries(self) -> None:
        if self.args.repeats <= 1:
            return
        for protocol in self.args.protocol_defs:
            boundary_rates: set[int] = set()
            for condition in ("clean", "late"):
                rates = self.curve_rates[(condition, protocol.key)]
                failed = self.first_failed[(condition, protocol.key)]
                accepted = [
                    rate
                    for rate in rates
                    if self.matching(
                        protocol,
                        rate,
                        0 if condition == "clean" else self.args.late_receivers,
                    )[0]["accepted"]
                ]
                if accepted:
                    boundary_rates.add(accepted[-1])
                if failed is not None:
                    boundary_rates.add(failed)
                elif rates:
                    boundary_rates.add(rates[-1])
            for condition, receivers in (
                ("clean", 0),
                ("late", self.args.late_receivers),
            ):
                retained = set(self.curve_rates[(condition, protocol.key)])
                for rate in sorted(boundary_rates & retained):
                    self.ensure(
                        protocol, rate, receivers, self.args.repeats, "boundary"
                    )

    def choose_r_star(self) -> int:
        vantage = next(
            (p for p in self.args.protocol_defs if p.key == "vantage"),
            self.args.protocol_defs[0],
        )
        candidates = []
        for rate in self.args.rates:
            clean = all(
                self.matching(protocol, rate, 0)
                and self.matching(protocol, rate, 0)[0]["accepted"]
                for protocol in self.args.protocol_defs
            )
            delayed_vantage = (
                self.matching(vantage, rate, self.args.late_receivers)
                and self.matching(vantage, rate, self.args.late_receivers)[0][
                    "accepted"
                ]
            )
            if clean and delayed_vantage:
                candidates.append(rate)
        chosen = max(candidates) if candidates else min(self.args.rates)
        print(f"Receiver-width sweep rate R* = {chosen:,} TPS", flush=True)
        return chosen

    def width_sweep(self, rate: int) -> None:
        if self.args.no_width:
            return
        for width in self.args.widths:
            repeats = (
                self.args.repeats
                if width in (min(self.args.widths), max(self.args.widths))
                else 1
            )
            for protocol in self.args.protocol_defs:
                self.ensure(protocol, rate, width, repeats, "width")


def median_range(
    records: Iterable[dict[str, Any]], field: str
) -> tuple[float, float, float]:
    values = [float(record["result"][field]) for record in records]
    return statistics.median(values), min(values), max(values)


def scaling_records(
    study: Study, condition: str, protocol: Protocol, rate: int
) -> list[dict[str, Any]]:
    receivers = 0 if condition == "clean" else study.args.late_receivers
    return study.matching(protocol, rate, receivers)


def plot_scaling(study: Study) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    fig, axes = plt.subplots(1, 2, figsize=(12.8, 5.0), sharex=True, sharey=True)
    y_floor = max(1.0, min(study.args.rates) / 100.0)
    for axis, condition, title in zip(
        axes,
        ("clean", "late"),
        (
            "Clean publication",
            f"{study.args.late_publishers} Byzantine publishers → "
            f"{study.args.late_receivers} receivers, +{study.args.late_delay_ms} ms",
        ),
    ):
        for protocol in study.args.protocol_defs:
            rates = study.curve_rates[(condition, protocol.key)]
            medians, lows, highs = [], [], []
            for rate in rates:
                median, low, high = median_range(
                    scaling_records(study, condition, protocol, rate), "committed_tps"
                )
                plotted_median = max(median, y_floor)
                medians.append(plotted_median)
                lows.append(plotted_median - max(low, y_floor))
                highs.append(max(high, y_floor) - plotted_median)
            axis.errorbar(
                rates,
                medians,
                yerr=[lows, highs],
                color=protocol.color,
                linewidth=1.8,
                capsize=2.5,
                label=protocol.label,
            )
            failed = study.first_failed[(condition, protocol.key)]
            for index, point_rate in enumerate(rates):
                is_failed = failed == point_rate
                axis.plot(
                    [point_rate],
                    [medians[index]],
                    marker=protocol.marker,
                    markerfacecolor="white" if is_failed else protocol.color,
                    markeredgecolor=protocol.color,
                    markeredgewidth=1.7 if is_failed else 1.0,
                    markersize=7 if is_failed else 5.5,
                    linestyle="none",
                    zorder=10,
                )
        bound = max(study.args.rates)
        axis.plot(
            [min(study.args.rates), bound],
            [min(study.args.rates), bound],
            "--",
            color="0.55",
            linewidth=1,
            label="ideal y=x",
        )
        axis.set_xscale("log")
        axis.set_yscale("log")
        axis.grid(True, which="both", alpha=0.22)
        axis.set_title(title)
        axis.set_xlabel("Offered load (tx/s)")
    axes[0].set_ylabel("Committed throughput (tx/s)")
    handles = [
        Line2D(
            [0],
            [0],
            color=protocol.color,
            marker=protocol.marker,
            linewidth=1.8,
            markersize=5.5,
            label=protocol.label,
        )
        for protocol in study.args.protocol_defs
    ]
    handles.append(
        Line2D([0], [0], color="0.55", linestyle="--", linewidth=1, label="ideal y=x")
    )
    fig.legend(
        handles=handles,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=3,
        frameon=False,
    )
    fig.suptitle(
        "n=10 local scaling with the built-in 10-region AWS RTT matrix", y=0.995
    )
    fig.tight_layout(rect=(0, 0, 1, 0.84))
    for suffix in ("png", "pdf"):
        fig.savefig(
            study.args.output / f"scaling.{suffix}", dpi=220, bbox_inches="tight"
        )
    plt.close(fig)


def plot_width(study: Study, rate: int) -> None:
    if study.args.no_width:
        return
    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(9.4, 4.7))
    for protocol in study.args.protocol_defs:
        medians, lows, highs = [], [], []
        for width in study.args.widths:
            median, low, high = median_range(
                study.matching(protocol, rate, width), "committed_tps"
            )
            medians.append(median)
            lows.append(median - low)
            highs.append(high - median)
        axis.errorbar(
            study.args.widths,
            medians,
            yerr=[lows, highs],
            color=protocol.color,
            marker=protocol.marker,
            linewidth=1.8,
            capsize=2.5,
            label=protocol.label,
        )
    axis.axhline(rate, color="0.55", linestyle="--", linewidth=1, label="offered load")
    axis.set_xticks(study.args.widths)
    axis.set_xlabel(
        f"Delayed receivers K ({study.args.late_publishers} Byzantine publishers)"
    )
    axis.set_ylabel("Committed throughput (tx/s)")
    axis.set_title(f"Receiver-width sensitivity at R*={rate:,} tx/s")
    axis.grid(True, alpha=0.22)
    axis.legend(
        loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, fontsize=8
    )
    fig.tight_layout()
    for suffix in ("png", "pdf"):
        fig.savefig(
            study.args.output / f"receiver_width.{suffix}", dpi=220, bbox_inches="tight"
        )
    plt.close(fig)


def write_csvs(study: Study) -> None:
    rows = []
    for record in study.records:
        row = {
            key: value
            for key, value in record.items()
            if key not in ("ticks", "result", "command")
        }
        row.update(record["result"])
        rows.append(row)
    if rows:
        fields = sorted({key for row in rows for key in row})
        with (study.args.output / "measurements.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    aggregate_rows = []
    groups: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
    for record in study.records:
        key = (
            record["condition"],
            record["protocol"],
            record["offered_tps"],
            record["late_receivers"],
        )
        groups.setdefault(key, []).append(record)
    for (condition, protocol, offered, receivers), records in sorted(groups.items()):
        throughput = median_range(records, "committed_tps")
        p50 = median_range(records, "materialized_p50_ms")
        p99 = median_range(records, "materialized_p99_ms")
        aggregate_rows.append(
            {
                "condition": condition,
                "protocol": protocol,
                "offered_tps": offered,
                "late_receivers": receivers,
                "runs": len(records),
                "committed_tps_median": throughput[0],
                "committed_tps_min": throughput[1],
                "committed_tps_max": throughput[2],
                "materialized_p50_ms_median": p50[0],
                "materialized_p99_ms_median": p99[0],
                "backlog_slope_tps_median": statistics.median(
                    r["backlog_slope_tps"] for r in records
                ),
                "cpu_cores_median": statistics.median(
                    r["average_cpu_cores"] for r in records
                ),
                "network_bytes_median": statistics.median(
                    float(r["result"]["network_bytes"]) for r in records
                ),
                "accepted_runs": sum(bool(r["accepted"]) for r in records),
            }
        )
    if aggregate_rows:
        with (study.args.output / "summary.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(aggregate_rows[0]))
            writer.writeheader()
            writer.writerows(aggregate_rows)


def write_provenance(args: argparse.Namespace, r_star: int) -> None:
    provenance = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "argv": sys.argv,
        "git_head": command_output(["git", "rev-parse", "HEAD"]),
        "git_status": command_output(["git", "status", "--short"]),
        "rustc": command_output(["rustc", "--version"]),
        "python": sys.version,
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "latency_model": "config::LatencyTable::aws_rtt; RTT matrix converted to one-way delay",
        "r_star_tps": r_star,
        "arguments": {
            key: value for key, value in vars(args).items() if key != "protocol_defs"
        },
    }
    provenance["arguments"] = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in provenance["arguments"].items()
    }
    (args.output / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n"
    )
    (args.output / "README.md").write_text(
        "# Local five-protocol study\n\n"
        "`scaling.pdf`/`.png` is the primary two-panel figure. Open markers are the first "
        "failed rate (throughput below the acceptance threshold or a growing latter-half backlog). "
        "Whiskers show min–max over boundary repetitions. `receiver_width.*` is the supporting K "
        f"sweep at R*={r_star:,} tx/s. Raw process output, individual measurements, aggregate CSV, "
        "and exact provenance are retained alongside the figures. Databases are discarded unless "
        "the run used `--keep-data`.\n"
    )


def main() -> int:
    args = parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "raw").mkdir()
    build(args)
    binary = args.binary if args.binary.is_absolute() else ROOT / args.binary
    args.binary = binary.resolve()
    if not args.binary.exists():
        raise SystemExit(f"benchmark binary does not exist: {args.binary}")

    if args.keep_data:
        data_root = args.output / "data"
        data_root.mkdir()
        temporary = None
    else:
        temporary = tempfile.TemporaryDirectory(prefix="vantage-five-protocol-")
        data_root = Path(temporary.name)

    study = Study(args, data_root)
    try:
        study.pilot_scaling()
        study.repeat_boundaries()
        r_star = study.choose_r_star()
        study.width_sweep(r_star)
        write_csvs(study)
        plot_scaling(study)
        plot_width(study, r_star)
        write_provenance(args, r_star)
    finally:
        if temporary is not None:
            temporary.cleanup()
    print(f"Study complete: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
