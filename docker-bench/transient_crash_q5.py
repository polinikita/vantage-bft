#!/usr/bin/env python3
"""Validate and plot the transient-crash diagnostic (n=3f+1 validators, f crashed)."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


RESULT_PREFIX = "DOCKER_BENCH_RESULT "


def load_series(
    path: Path, active_at_s: float, duration: int
) -> list[tuple[float, float]]:
    payload = json.loads(path.read_text())
    results = payload.get("data", {}).get("result", [])
    if payload.get("status") != "success" or len(results) != 1:
        raise ValueError(f"expected one Prometheus series in {path}, found {len(results)}")
    observed: dict[int, float] = {}
    previous_second: int | None = None
    for epoch, value in results[0]["values"]:
        offset = float(epoch) - active_at_s
        second = int(round(offset))
        if abs(offset - second) > 1e-6 or (
            previous_second is not None and second <= previous_second
        ):
            raise ValueError(f"expected an ordered one-second Prometheus grid in {path}")
        if 0 <= second <= duration and value not in ("NaN", "+Inf", "-Inf"):
            observed[second] = float(value)
        previous_second = second
    if len(observed) < 2:
        raise ValueError(f"Prometheus series is too short in {path}")
    return [
        (float(second), observed.get(second, math.nan))
        for second in range(duration + 1)
    ]


def interval_values(
    series: list[tuple[float, float]], start: float, end: float
) -> list[float]:
    return [
        value
        for second, value in series
        if start <= second <= end and math.isfinite(value)
    ]


def median_between(
    series: list[tuple[float, float]], start: float, end: float
) -> float | None:
    values = interval_values(series, start, end)
    return statistics.median(values) if values else None


def mean_between(
    series: list[tuple[float, float]], start: float, end: float
) -> float | None:
    values = interval_values(series, start, end)
    return statistics.fmean(values) if values else None


def max_between(
    series: list[tuple[float, float]], start: float, end: float
) -> float | None:
    values = interval_values(series, start, end)
    return max(values) if values else None


def quantile_between(
    series: list[tuple[float, float]], start: float, end: float, q: float
) -> float | None:
    values = sorted(interval_values(series, start, end))
    if not values:
        return None
    position = q * (len(values) - 1)
    low = int(math.floor(position))
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (position - low)


def configure_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 8.5,
            "axes.titlesize": 9,
            "axes.labelsize": 8.5,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 7.5,
            "axes.linewidth": 0.7,
            "grid.linewidth": 0.45,
            "lines.linewidth": 1.5,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)

    data = args.run_root / "data"
    manifest = json.loads((data / "manifest.json").read_text())
    parameters = json.loads((data / "parameters.json").read_text())
    timeline = json.loads((data / "chaos-timeline.json").read_text())
    victims = {int(node) for node in timeline["victims"]}
    nodes = int(manifest["nodes"])
    expected_faults = (nodes - 1) // 3
    if nodes != 3 * expected_faults + 1 or len(victims) != expected_faults:
        raise ValueError("transient-crash profile must crash exactly f validators of n=3f+1")
    if victims != {int(node) for node in manifest["load_excluded_node_indices"]}:
        raise ValueError("crash victims must be exactly the zero-load placement set")
    if int(manifest["honest_offered_tps"]) != 1000:
        raise ValueError("counted 1000 tx/s load must stay on non-victims")
    expected_non_victims = set(range(nodes)) - victims
    if {int(node) for node in manifest["load_node_indices"]} != expected_non_victims:
        raise ValueError("all and only non-victims must carry counted load")
    if not manifest.get("latency") or int(manifest["netem_limit_pkts"]) != 100_000:
        raise ValueError("ten-region netem with a 100000-packet limit is required")
    if int(parameters.get("metrics_report_interval_ms", 0)) != 1_000:
        raise ValueError("latency histogram must use one-second reporter windows")
    protocol = str(parameters.get("protocol", "vantage"))
    if protocol == "vantage" and (
        not parameters.get("sequence_checkpoints")
        or not parameters.get("sequence_install_enabled")
    ):
        raise ValueError("state sync must be enabled")

    run_results = [
        json.loads(line[len(RESULT_PREFIX) :])
        for line in (args.run_root / "run.log").read_text(errors="replace").splitlines()
        if line.startswith(RESULT_PREFIX)
    ]
    reachable = int(run_results[0].get("reachable_workers", 0)) if len(run_results) == 1 else -1
    dead_victims_allowed = protocol != "vantage"
    if len(run_results) != 1 or reachable != nodes and not (
        dead_victims_allowed and reachable >= nodes - expected_faults
    ):
        raise ValueError("the run must end with every worker endpoint reachable")
    for node in range(nodes):
        log = data / f"node-{node}" / "logs" / "primary.log"
        if not log.is_file():
            raise ValueError(f"missing primary log for node {node}")
        text = log.read_text(errors="replace").lower()
        if "panicked at" in text or "fatal runtime error" in text:
            raise ValueError(f"panic signature in node {node} primary log")

    active_at_s = int(manifest["active_at_ms"]) / 1_000.0
    events = timeline["events"]
    fault_start = (min(int(event["down_ms"]) for event in events) / 1_000.0) - active_at_s
    fault_end = (max(int(event["up_ms"]) for event in events) / 1_000.0) - active_at_s
    if fault_end - fault_start < 30.0:
        raise ValueError("validators were not crashed for at least 30 seconds")

    duration_int = int(manifest["duration"])
    throughput = load_series(
        data / "prometheus-throughput.json", active_at_s, duration_int
    )
    latency = load_series(
        data / "prometheus-latency-window.json", active_at_s, duration_int
    )
    query_config = dict(
        line.split("=", 1)
        for line in (args.run_root / "prometheus-queries.txt").read_text().splitlines()
    )
    if query_config.get("scrape_interval") != "1s" or query_config.get(
        "query_step"
    ) != "1s":
        raise ValueError("Prometheus was not configured on a one-second grid")
    for query_name in ("throughput", "latency"):
        query = query_config.get(query_name, "")
        for node in range(nodes):
            label = f"node-{node}-worker-0"
            if (node in expected_non_victims) != (label in query):
                raise ValueError(
                    f"{query_name} population does not match the non-victims"
                )

    duration = float(duration_int)
    offered = float(manifest["honest_offered_tps"])
    configure_style()
    fig, (top, bottom) = plt.subplots(
        2,
        1,
        figsize=(7.0, 3.75),
        sharex=True,
        gridspec_kw={"height_ratios": [1.0, 1.0], "hspace": 0.23},
    )
    blue = "#245b8a"
    orange = "#c55a11"
    shade = "#b8b8b8"

    top.plot(*zip(*throughput), color=blue, label="Committed")
    top.axhline(offered, color="black", linestyle="--", linewidth=1.0, label="Offered")
    top.axvspan(fault_start, fault_end, color=shade, alpha=0.34, linewidth=0)
    top.set_ylim(bottom=0)
    top.set_ylabel("Committed tx/s\n(5-s rate)")
    top.set_title("(a) Throughput during crash and restart")
    top.grid(axis="y", color="#d5d5d5")
    top.legend(loc="upper left", frameon=False, ncol=2)

    bottom.plot(*zip(*latency), color=orange, label="Non-victim p50")
    bottom.axvspan(fault_start, fault_end, color=shade, alpha=0.34, linewidth=0)
    peak_time, peak_latency = max(
        (point for point in latency if math.isfinite(point[1])),
        key=lambda point: point[1],
    )
    bottom.annotate(
        f"peak {peak_latency:,.0f} ms",
        (peak_time, peak_latency),
        xytext=(7, -16),
        textcoords="offset points",
        ha="left",
        va="top",
        fontsize=7.5,
        color=orange,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.75, "pad": 1.0},
    )
    bottom.text(
        (fault_start + fault_end) / 2,
        0.96,
        f"{len(victims)} validators crashed",
        transform=bottom.get_xaxis_transform(),
        ha="center",
        va="top",
        fontsize=7.5,
        color="#444444",
        bbox={
            "facecolor": "white",
            "edgecolor": "none",
            "alpha": 0.78,
            "pad": 1.0,
        },
    )
    bottom.set_xlim(0, duration)
    bottom.set_ylim(bottom=0)
    bottom.set_xlabel("Time since measurement start (s)")
    bottom.set_ylabel("Materialization p50\n(latest 1-s window, ms)")
    bottom.set_title("(b) Non-victim materialization latency")
    bottom.grid(axis="y", color="#d5d5d5")
    bottom.legend(loc="upper right", frameon=False)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for extension in ("pdf", "png"):
        fig.savefig(
            args.output_dir / f"transient-crash-performance.{extension}",
            bbox_inches="tight",
            dpi=220,
        )
    plt.close(fig)

    final_window_start = max(fault_end + 20.0, duration - 20.0)
    summary = {
        "nodes": int(manifest["nodes"]),
        "faults": len(victims),
        "fault_start_s": fault_start,
        "fault_end_s": fault_end,
        "fault_duration_s": fault_end - fault_start,
        "throughput_pre_mean_tps": mean_between(
            throughput, 5.0, max(5.0, fault_start - 2.0)
        ),
        "throughput_crash_mean_tps": mean_between(
            throughput, fault_start + 5.0, fault_end - 2.0
        ),
        "throughput_final_mean_tps": mean_between(
            throughput, final_window_start, duration
        ),
        "latency_pre_median_ms": median_between(
            latency, 5.0, max(5.0, fault_start - 2.0)
        ),
        "latency_crash_peak_ms": max_between(
            latency, fault_start, fault_end
        ),
        "latency_crash_median_ms": median_between(
            latency, fault_start + 5.0, fault_end
        ),
        "latency_crash_mean_ms": mean_between(
            latency, fault_start + 5.0, fault_end
        ),
        "latency_crash_p90_ms": quantile_between(
            latency, fault_start + 5.0, fault_end, 0.9
        ),
        "throughput_crash_min_tps": min(
            interval_values(throughput, fault_start + 5.0, fault_end), default=None
        ),
        "early_refusals": protocol == "vantage" and bool(parameters.get("early_refusals", True)),
        "latency_post_restart_peak_ms": max_between(
            latency, fault_end, duration
        ),
        "latency_peak_ms": peak_latency,
        "latency_peak_s": peak_time,
        "latency_final_median_ms": median_between(
            latency, final_window_start, duration
        ),
        "protocol": protocol,
        "state_sync_enabled": protocol == "vantage",
        "latency_population": sorted(expected_non_victims),
        "prometheus_step_s": 1,
        "latency_window_s": 1,
        "overall_committed_tps": float(run_results[0]["committed_tps"]),
        "all_workers_reachable_at_end": reachable == nodes,
        "victims_survived_restart": reachable == nodes,
    }
    (args.output_dir / "transient-crash-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
