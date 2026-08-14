#!/usr/bin/env python3
"""Plot the capacity envelope and latency impact of a local data-drop study."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


PROTOCOLS = [
    ("vantage", "Vantage", "#0072B2"),
    ("autobahn-optimistic", "Autobahn optimistic\n(all-to-all)", "#D55E00"),
    ("autobahn-seamless", "Autobahn seamless", "#E69F00"),
    ("simple-it", "Simple-IT (Opt-RBC)", "#009E73"),
    ("simple-it-bracha", "Simple-IT (Bracha-RBC)", "#CC79A7"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def median(records: list[dict], field: str) -> float:
    return statistics.median(float(record["result"][field]) for record in records)


def main() -> int:
    args = parse_args()
    records = json.loads((args.study / "measurements.json").read_text())
    output = args.output or args.study / "drop_impact.png"
    dropped_records = [
        record for record in records if record["condition"].startswith("drop-")
    ]
    drop_publishers = max(
        (int(record["drop_publishers"]) for record in dropped_records), default=0
    )
    drop_receivers = max(
        (int(record["drop_receivers"]) for record in dropped_records), default=0
    )
    repair_suppressed = any(
        bool(record.get("repair_suppressed", False)) for record in dropped_records
    )

    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    fig, (capacity_axis, latency_axis) = plt.subplots(
        1, 2, figsize=(13.4, 5.7), gridspec_kw={"width_ratios": (1.05, 1.25)}
    )
    positions = list(reversed(range(len(PROTOCOLS))))
    tested_rates = sorted({int(record["offered_tps"]) for record in records})
    min_rate, max_rate = min(tested_rates), max(tested_rates)

    for y, (key, label, color) in zip(positions, PROTOCOLS):
        dropped = [
            record
            for record in records
            if record["protocol"] == key and record["condition"].startswith("drop-")
        ]
        passing = sorted(
            {int(record["offered_tps"]) for record in dropped if record["accepted"]}
        )
        failing = sorted(
            {int(record["offered_tps"]) for record in dropped if not record["accepted"]}
        )
        last_pass = passing[-1] if passing else None
        first_fail = failing[0] if failing else None

        capacity_axis.hlines(y, min_rate, max_rate, color="0.9", linewidth=7, zorder=0)
        if last_pass is not None:
            capacity_axis.plot(
                last_pass,
                y,
                marker=">" if first_fail is None else "o",
                color=color,
                markersize=9,
                zorder=3,
            )
            capacity_axis.annotate(
                (
                    f"≥{last_pass / 1000:g}k"
                    if first_fail is None
                    else f"{last_pass / 1000:g}k pass"
                ),
                (last_pass, y),
                xytext=(7, 0) if first_fail is None else (-7, 11 if y == 0 else -11),
                textcoords="offset points",
                va="center",
                ha="left" if first_fail is None else "right",
                color=color,
                fontsize=9,
            )
        if first_fail is not None:
            if last_pass is not None:
                capacity_axis.hlines(
                    y, last_pass, first_fail, color=color, linestyle=":", linewidth=1.5
                )
            capacity_axis.plot(
                first_fail,
                y,
                marker="o",
                markerfacecolor="white",
                markeredgecolor=color,
                markeredgewidth=2,
                markersize=9,
                zorder=4,
            )
            if last_pass is None:
                capacity_axis.annotate(
                    f"<{first_fail / 1000:g}k",
                    (first_fail, y),
                    xytext=(7, 0),
                    textcoords="offset points",
                    va="center",
                    color=color,
                    fontsize=9,
                )
            else:
                capacity_axis.annotate(
                    f"{first_fail / 1000:g}k fail",
                    (first_fail, y),
                    xytext=(7, 10),
                    textcoords="offset points",
                    va="center",
                    color=color,
                    fontsize=9,
                )

        clean_at_common = [
            record
            for record in records
            if record["protocol"] == key
            and record["condition"] == "clean"
            and int(record["offered_tps"]) == min_rate
        ]
        drop_at_common = [
            record for record in dropped if int(record["offered_tps"]) == min_rate
        ]
        for offset, sample, sample_color, marker in (
            (0.12, clean_at_common, "0.55", "s"),
            (-0.12, drop_at_common, color, "o"),
        ):
            p50 = median(sample, "materialized_p50_ms")
            p99 = median(sample, "materialized_p99_ms")
            latency_axis.hlines(
                y + offset, p50, p99, color=sample_color, linewidth=2.2, alpha=0.95
            )
            latency_axis.plot(
                p50, y + offset, marker=marker, color=sample_color, markersize=7
            )
            latency_axis.plot(
                p99, y + offset, marker="|", color=sample_color, markersize=10
            )

    labels = [label for _, label, _ in PROTOCOLS]
    capacity_axis.set_yticks(positions, labels)
    capacity_axis.set_xscale("log")
    capacity_axis.set_xlim(min_rate / 1.35, max_rate * 1.8)
    capacity_axis.set_xlabel("Offered load (tx/s)")
    capacity_axis.set_title("Stable-load envelope under data omission")
    capacity_axis.grid(True, axis="x", which="both", alpha=0.22)
    capacity_axis.text(
        0.01,
        -0.19,
        "solid = last passing point   ○ = first failing point   ▸ = no failure observed",
        transform=capacity_axis.transAxes,
        fontsize=8.5,
        color="0.35",
    )

    latency_axis.set_yticks(positions, labels)
    latency_axis.set_xscale("log")
    latency_axis.set_xlabel("Materialized latency (ms, p50 → p99)")
    latency_axis.set_title(f"Latency at the common {min_rate:,} tx/s point")
    latency_axis.grid(True, axis="x", which="both", alpha=0.22)
    latency_axis.legend(
        handles=[
            Line2D([0], [0], color="0.55", marker="s", label="clean"),
            Line2D(
                [0],
                [0],
                color="0.2",
                marker="o",
                label=f"{drop_publishers}×{drop_receivers} Byzantine omission",
            ),
        ],
        loc="lower right",
        frameon=False,
    )

    fig.suptitle(
        "n=10 local data-lane omission stress (exploratory, one run per point)",
        fontsize=15,
        y=0.995,
    )
    fig.text(
        0.5,
        0.925,
        "publishers omit original headers and batches to the fixed receiver group · "
        + (
            "lane repair suppressed · consensus traffic remains normal"
            if repair_suppressed
            else "repair/control traffic remains normal"
        ),
        ha="center",
        fontsize=10,
        color="0.3",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.89), w_pad=3.2)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=220, bbox_inches="tight")
    fig.savefig(output.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
