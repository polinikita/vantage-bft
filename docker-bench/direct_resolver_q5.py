#!/usr/bin/env python3
"""Audit and plot repeated direct-resolver mixed-open diagnostics."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import statistics
import warnings
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from recovery_report import events_by_view, parse_events


def numeric_summary(values: list[float]) -> dict[str, float]:
    return {
        "minimum": min(values),
        "median": statistics.median(values),
        "maximum": max(values),
    }


def load_prometheus(path: Path, active_at_ms: int, duration: int) -> np.ndarray:
    body = json.loads(path.read_text())
    result = body.get("data", {}).get("result", [])
    if body.get("status") != "success" or len(result) != 1:
        raise ValueError(f"expected one successful Prometheus series in {path}")
    output = np.full(duration + 1, np.nan)
    start_s = active_at_ms / 1_000.0
    for timestamp, value in result[0]["values"]:
        second = int(round(float(timestamp) - start_s))
        if 0 <= second <= duration:
            output[second] = float(value)
    return output


def aggregate_band(series: list[np.ndarray]) -> dict[str, np.ndarray]:
    matrix = np.asarray(series, dtype=float)
    with np.errstate(all="ignore"), warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
        return {
            "minimum": np.nanmin(matrix, axis=0),
            "median": np.nanmedian(matrix, axis=0),
            "maximum": np.nanmax(matrix, axis=0),
        }


def local_backlog_series(
    events: list[dict],
    active_at_ms: int,
    duration: int,
    minimum_view: int,
    maximum_view: int,
) -> np.ndarray:
    opens = {
        int(event["view"]): int(event["epoch_ms"])
        for event in events
        if event["kind"] == "completed_open"
        and minimum_view <= int(event["view"]) <= maximum_view
    }
    seals = {
        int(event["view"]): int(event["epoch_ms"])
        for event in events
        if event["kind"] == "seal"
        and minimum_view <= int(event["view"]) <= maximum_view
    }
    output = np.zeros(duration + 1)
    for second in range(duration + 1):
        epoch_ms = active_at_ms + second * 1_000
        output[second] = sum(
            opened <= epoch_ms < seals.get(view, 2**63 - 1)
            for view, opened in opens.items()
        )
    return output


def common_target_rows(
    correct_events: dict[int, list[dict]], minimum_view: int, maximum_view: int
) -> list[dict]:
    opens = {
        node: events_by_view(events, "completed_open")
        for node, events in correct_events.items()
    }
    seals = {
        node: events_by_view(events, "seal") for node, events in correct_events.items()
    }
    targets = set.intersection(*(set(mapping) for mapping in opens.values()))
    targets &= set.intersection(*(set(mapping) for mapping in seals.values()))
    rows = []
    for target in sorted(targets):
        if not minimum_view <= target <= maximum_view:
            continue
        rows.append(
            {
                "target": target,
                "completed_all_ms": max(
                    int(mapping[target]["epoch_ms"]) for mapping in opens.values()
                ),
                "sealed_all_ms": max(
                    int(mapping[target]["epoch_ms"]) for mapping in seals.values()
                ),
            }
        )
    return rows


def load_run(path: Path, single_target: bool) -> dict:
    data = path / "data"
    manifest = json.loads((data / "manifest.json").read_text())
    parameters = json.loads((data / "parameters.json").read_text())
    report = json.loads((path / "report.json").read_text())
    failed = {check["name"] for check in report["checks"] if not check["passed"]}
    if failed:
        raise ValueError(f"unexpected failed checks in {path}: {sorted(failed)}")
    required_passes = {
        "correct logs present",
        "state-sync installation disabled",
        "all correct validators finalized",
        "panic free",
        "workers remained reachable",
        "measurement coverage",
        "useful throughput continued",
        "residual mixed views observed",
        "bounded direct-holder split",
        "per-target resolver recovery",
        "ordered output passed recovered views",
        "open backlog drained",
    }
    passed = {check["name"] for check in report["checks"] if check["passed"]}
    missing = required_passes - passed
    if missing:
        raise ValueError(f"missing required checks in {path}: {sorted(missing)}")

    n = int(manifest["nodes"])
    f = (n - 1) // 3
    if (
        n != 10
        or f != 3
        or int(manifest["duration"]) != 150
        or int(parameters["withhold_at_ms"]) != 20_000
        or int(parameters["withhold_for_ms"]) != 30_000
        or int(manifest["honest_offered_tps"]) != 1_000
        or int(manifest["adversarial_rate"]) != 600
        or not manifest["latency"]
        or int(manifest["netem_limit_pkts"]) != 100_000
        or manifest["sequence_install_enabled"]
        or bool(parameters.get("vantage_mixed_open_single_target", False))
        != single_target
    ):
        raise ValueError(f"unexpected Q5 configuration in {path}")

    correct_nodes = [int(node) for node in report["correct_nodes"]]
    correct_events = {
        node: parse_events(data / f"node-{node}" / "logs" / "primary.log")
        for node in correct_nodes
    }
    stress_views = [int(view) for view in report["details"]["stress_views"]]
    minimum_view, maximum_view = min(stress_views), max(stress_views)
    duration = int(manifest["duration"])
    active_at_ms = int(manifest["active_at_ms"])
    per_node_backlog = [
        local_backlog_series(
            events, active_at_ms, duration, minimum_view, maximum_view
        )
        for events in correct_events.values()
    ]
    backlog = np.median(np.asarray(per_node_backlog), axis=0)
    targets = common_target_rows(correct_events, minimum_view, maximum_view)
    fault_start_ms = active_at_ms + int(parameters["withhold_at_ms"])
    fault_duration_s = int(parameters["withhold_for_ms"]) / 1_000.0
    fault_end_ms = fault_start_ms + int(parameters["withhold_for_ms"])
    arrivals_during_fault = sum(
        fault_start_ms <= row["completed_all_ms"] < fault_end_ms for row in targets
    )
    seals_during_fault = sum(
        fault_start_ms <= row["sealed_all_ms"] < fault_end_ms for row in targets
    )
    outstanding_at_end = sum(
        row["completed_all_ms"] <= fault_end_ms < row["sealed_all_ms"]
        for row in targets
    )
    last_seal_ms = max(
        (
            row["sealed_all_ms"]
            for row in targets
            if row["completed_all_ms"] <= fault_end_ms
        ),
        default=fault_end_ms,
    )
    drain_seconds = max(0.0, (last_seal_ms - fault_end_ms) / 1_000.0)
    tail_rate = outstanding_at_end / drain_seconds if drain_seconds else 0.0

    throughput = report["details"]["throughput"]
    recovery = report["details"]["completion_to_all_resolver_sealed_ms"]
    direct = report["details"]["direct_resolver_dynamics"]
    sustained = report["details"]["sustained_attack_dynamics"]
    return {
        "path": path,
        "manifest": manifest,
        "parameters": parameters,
        "report": report,
        "throughput_series": load_prometheus(
            data / "prometheus-throughput.json", active_at_ms, duration
        ),
        "latency_series": load_prometheus(
            data / "prometheus-latency.json", active_at_ms, duration
        ),
        "backlog_series": backlog,
        "record": {
            "guarded_views": int(report["details"]["recovery_count"]),
            "all_fault_cohort_views": len(targets),
            "fault_cohort_arrivals": arrivals_during_fault,
            "fault_cohort_arrival_rate_per_second": (
                arrivals_during_fault / fault_duration_s
            ),
            "fault_cohort_seals": seals_during_fault,
            "fault_cohort_seal_rate_per_second": seals_during_fault / fault_duration_s,
            "completion_to_all_median_ms": float(recovery["median"]),
            "completion_to_all_p95_ms": float(recovery["p95"]),
            "completion_to_all_maximum_ms": float(recovery["maximum"]),
            "active_decisions_per_second": float(
                direct["all_correct_decisions_per_second"]
            ),
            "in_attack_arrival_rate_per_second": float(
                sustained["arrival_rate_per_second"]
            ),
            "in_attack_seal_rate_per_second": float(
                sustained["service_rate_per_second"]
            ),
            "in_attack_backlog_start": int(sustained["backlog_at_start"]),
            "in_attack_backlog_end": int(sustained["backlog_at_end"]),
            "in_attack_backlog_peak": int(sustained["backlog_peak"]),
            "fault_end_outstanding_views": outstanding_at_end,
            "fault_end_to_all_sealed_s": drain_seconds,
            "post_attack_drain_rate_views_per_second": tail_rate,
            "correct_tps": float(throughput["committed_tps"]),
            "materialised_p50_ms": float(
                throughput["materialised_latency_ms"]["p50"]
            ),
            "materialised_p99_ms": float(
                throughput["materialised_latency_ms"]["p99"]
            ),
            "median_node_cpu_cores": float(throughput["median_node_cpu_cores"]),
            "maximum_node_wire_mbps": float(throughput["max_node_wire_mbps"]),
        },
    }


def plot_band(
    axis: plt.Axes,
    x: np.ndarray,
    band: dict[str, np.ndarray],
    color: str,
    label: str,
    *,
    step: bool = False,
) -> None:
    axis.fill_between(
        x,
        band["minimum"],
        band["maximum"],
        color=color,
        alpha=0.16,
        linewidth=0,
        step="post" if step else None,
    )
    if step:
        axis.step(x, band["median"], where="post", color=color, label=label)
    else:
        axis.plot(x, band["median"], color=color, label=label)


def write_series_csv(
    path: Path,
    seconds: np.ndarray,
    bands: dict[str, dict[str, np.ndarray]],
) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(
            output,
            fieldnames=["metric", "sec", "minimum", "median", "maximum"],
        )
        writer.writeheader()
        for metric, band in bands.items():
            for index, second in enumerate(seconds):
                writer.writerow(
                    {
                        "metric": metric,
                        "sec": int(second),
                        "minimum": band["minimum"][index],
                        "median": band["median"][index],
                        "maximum": band["maximum"][index],
                    }
                )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--profile", choices=("sustained", "single"), default="sustained"
    )
    args = parser.parse_args(argv)

    single_target = args.profile == "single"
    run_dir = "mixed-single" if single_target else "mixed"
    repetitions = sorted(args.campaign_root.glob(f"rep-*/{run_dir}"))
    if len(repetitions) != 3:
        parser.error(f"expected exactly three repetitions, found {len(repetitions)}")
    runs = [load_run(path, single_target) for path in repetitions]
    if single_target:
        for run in runs:
            if (
                run["record"]["guarded_views"] != 1
                or run["record"]["all_fault_cohort_views"] != 1
            ):
                raise ValueError(
                    f"single-target profile did not create exactly one common-open view in {run['path']}"
                )
    committees = [
        (run["path"] / "data" / "committee.json").read_bytes() for run in runs
    ]
    if len({hashlib.sha256(value).digest() for value in committees}) != len(runs):
        raise ValueError("repetitions must use fresh committees")
    if "build skipped (--no-build)" not in (runs[1]["path"] / "run.log").read_text():
        raise ValueError("repetition 2 did not reuse the fixed image")
    if "build skipped (--no-build)" not in (runs[2]["path"] / "run.log").read_text():
        raise ValueError("repetition 3 did not reuse the fixed image")
    image_match = re.search(
        r"writing image sha256:([0-9a-f]{64})",
        (runs[0]["path"] / "run.log").read_text(),
    )
    if image_match is None:
        raise ValueError("could not recover the fixed image digest")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    seconds = np.arange(int(runs[0]["manifest"]["duration"]) + 1)
    bands = {
        "throughput_tps": aggregate_band(
            [run["throughput_series"] for run in runs]
        ),
        "unresolved_views": aggregate_band(
            [run["backlog_series"] for run in runs]
        ),
        "materialised_p50_ms": aggregate_band(
            [run["latency_series"] for run in runs]
        ),
    }
    output_stem = "direct-resolver-q5-single" if single_target else "direct-resolver-q5"
    write_series_csv(args.output_dir / f"{output_stem}.csv", seconds, bands)

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 8.2,
            "axes.titlesize": 8.8,
            "axes.labelsize": 8.2,
            "legend.fontsize": 7.2,
            "lines.linewidth": 1.25,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )
    blue, orange, green = "#245b8a", "#c55a11", "#2f7d4a"
    figure = plt.figure(figsize=(7.0, 4.75))
    grid = figure.add_gridspec(2, 2, hspace=0.42, wspace=0.31)
    throughput_axis = figure.add_subplot(grid[0, :])
    backlog_axis = figure.add_subplot(grid[1, 0])
    latency_axis = figure.add_subplot(grid[1, 1])
    axes = (throughput_axis, backlog_axis, latency_axis)
    for axis in axes:
        axis.axvspan(20, 50, color="#b8b8b8", alpha=0.34, linewidth=0)
        axis.grid(axis="y", color="#d5d5d5", linewidth=0.6)
        axis.set_xlim(0, 150)

    plot_band(
        throughput_axis,
        seconds,
        bands["throughput_tps"],
        blue,
        "Median; min–max band",
    )
    throughput_axis.axhline(
        1_000, color="black", linestyle="--", linewidth=0.9, label="Offered load"
    )
    throughput_axis.set_ylabel("Committed tx/s\n(5-s rate)")
    throughput_axis.set_title("(a) Correct-load throughput")
    throughput_axis.legend(loc="upper right", frameon=False, ncol=2)

    plot_band(
        backlog_axis,
        seconds,
        bands["unresolved_views"],
        green,
        "Median; min–max band",
        step=True,
    )
    backlog_axis.set_xlabel("Time since measurement start (s)")
    backlog_axis.set_ylabel("Unresolved views")
    backlog_axis.set_title("(b) Fault-cohort resolver backlog")
    backlog_axis.set_ylim(bottom=0)
    backlog_axis.legend(loc="upper right", frameon=False)

    plot_band(
        latency_axis,
        seconds,
        bands["materialised_p50_ms"],
        orange,
        "Median; min–max band",
    )
    latency_axis.set_xlabel("Time since measurement start (s)")
    latency_axis.set_ylabel("Materialization p50 (ms)")
    latency_axis.set_title("(c) Cumulative output latency")
    latency_axis.set_yscale("log")
    latency_axis.legend(loc="upper right", frameon=False)

    figure.savefig(
        args.output_dir / f"{output_stem}.pdf", bbox_inches="tight", dpi=300
    )
    figure.savefig(
        args.output_dir / f"{output_stem}.png", bbox_inches="tight", dpi=220
    )
    plt.close(figure)

    records = [run["record"] for run in runs]
    numeric_keys = records[0].keys()
    aggregate = {
        key: numeric_summary([float(record[key]) for record in records])
        for key in numeric_keys
    }
    latency_peak_index = int(np.nanargmax(bands["materialised_p50_ms"]["median"]))
    throughput_peak_index = int(np.nanargmax(bands["throughput_tps"]["median"]))
    timeline_summary = {
        "median_curve_peak_unresolved_views": float(
            np.nanmax(bands["unresolved_views"]["median"])
        ),
        "median_curve_peak_materialised_p50_ms": float(
            bands["materialised_p50_ms"]["median"][latency_peak_index]
        ),
        "median_curve_peak_materialised_p50_at_s": int(seconds[latency_peak_index]),
        "median_curve_peak_committed_tps": float(
            bands["throughput_tps"]["median"][throughput_peak_index]
        ),
        "median_curve_peak_committed_tps_at_s": int(seconds[throughput_peak_index]),
    }
    summary = {
        "campaign_root": str(args.campaign_root),
        "profile": args.profile,
        "image_sha256": image_match.group(1),
        "repetitions": len(runs),
        "configuration": {
            "nodes": 10,
            "fault_budget": 3,
            "correct_offered_tps": 1_000,
            "uncounted_adversarial_tps": 600,
            "duration_s": 150,
            "fault_start_s": 20,
            "fault_duration_s": 30,
            "delta_ms": 200,
            "netem_limit_pkts": 100_000,
            "state_sync": False,
            "prometheus_step_s": 1,
            "single_target": single_target,
        },
        "per_repetition": records,
        "aggregate": aggregate,
        "timeline": timeline_summary,
    }
    (args.output_dir / f"{output_stem}-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
