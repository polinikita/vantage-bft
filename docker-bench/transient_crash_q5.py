#!/usr/bin/env python3
"""Validate and plot the paper's n=20 transient-crash diagnostic."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_series(path: Path, active_at_s: float) -> list[tuple[float, float]]:
    payload = json.loads(path.read_text())
    results = payload.get("data", {}).get("result", [])
    if len(results) != 1:
        raise ValueError(f"expected one Prometheus series in {path}, found {len(results)}")
    return [
        (float(epoch) - active_at_s, float(value))
        for epoch, value in results[0]["values"]
        if value not in ("NaN", "+Inf", "-Inf")
    ]


def interval_values(
    series: list[tuple[float, float]], start: float, end: float
) -> list[float]:
    return [value for second, value in series if start <= second <= end]


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
    expected_faults = (int(manifest["nodes"]) - 1) // 3
    if int(manifest["nodes"]) != 20 or expected_faults != 6 or len(victims) != 6:
        raise ValueError("paper transient-crash profile must use n=20 and f=6")
    if victims != {int(node) for node in manifest["withholding_node_indices"]}:
        raise ValueError("crash victims must be exactly the zero-load placement set")
    if not manifest.get("correct_load_only") or int(manifest["honest_offered_tps"]) != 1000:
        raise ValueError("counted 1000 tx/s load must stay on non-victims")
    if not manifest.get("latency") or int(manifest["netem_limit_pkts"]) != 100_000:
        raise ValueError("ten-region netem with a 100000-packet limit is required")
    if not parameters.get("sequence_checkpoints") or not parameters.get(
        "sequence_install_enabled"
    ):
        raise ValueError("state sync must be enabled")

    active_at_s = int(manifest["active_at_ms"]) / 1_000.0
    events = timeline["events"]
    fault_start = (min(int(event["down_ms"]) for event in events) / 1_000.0) - active_at_s
    fault_end = (max(int(event["up_ms"]) for event in events) / 1_000.0) - active_at_s
    if fault_end - fault_start < 30.0:
        raise ValueError("validators were not crashed for at least 30 seconds")

    throughput = load_series(data / "prometheus-throughput.json", active_at_s)
    latency = load_series(data / "prometheus-latency.json", active_at_s)
    if not throughput or not latency:
        raise ValueError("Prometheus exports are empty")

    duration = float(manifest["duration"])
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

    bottom.plot(*zip(*latency), color=orange, label="Materialization p50")
    bottom.axvspan(fault_start, fault_end, color=shade, alpha=0.34, linewidth=0)
    peak_time, peak_latency = max(latency, key=lambda point: point[1])
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
        "6 validators crashed",
        transform=bottom.get_xaxis_transform(),
        ha="center",
        va="top",
        fontsize=7.5,
        color="#444444",
    )
    bottom.set_xlim(0, duration)
    bottom.set_ylim(bottom=0)
    bottom.set_xlabel("Time since measurement start (s)")
    bottom.set_ylabel("Cumulative materialization\np50 (ms)")
    bottom.set_title("(b) Committee-median latency")
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
        "latency_peak_ms": peak_latency,
        "latency_peak_s": peak_time,
        "latency_final_median_ms": median_between(
            latency, final_window_start, duration
        ),
        "state_sync_enabled": True,
    }
    (args.output_dir / "transient-crash-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
