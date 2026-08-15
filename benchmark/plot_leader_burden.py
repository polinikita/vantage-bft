#!/usr/bin/env python3
"""Plot honest goodput, latency, and leader egress for a relay-load sweep."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


PROTOCOLS = (
    ("autobahn-optimistic", "Autobahn optimistic (all-to-all)", "#D55E00", "s"),
    ("vantage", "Vantage", "#0072B2", "o"),
    ("simple-it", "Simple-IT (Opt-RBC)", "#009E73", "D"),
    ("simple-it-bracha", "Simple-IT (Bracha RBC)", "#CC79A7", "v"),
    ("autobahn-seamless", "Autobahn seamless (control)", "#E69F00", "^"),
)

REQUIRED = {
    "protocol",
    "offered_tps",
    "honest_offered_tps",
    "committed_tps",
    "p50_ms",
    "p99_ms",
    "max_node_wire_mbps",
    "max_node_optimistic_batch_relay_mbps",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "measurements",
        type=Path,
        nargs="+",
        help="one or more sweep CSV files; repeated points are combined by median",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--title", default="Optimistic availability concentrates recovery on the leader"
    )
    parser.add_argument(
        "--subtitle",
        default=(
            "n=20, f=6 · uniform offered load · AWS RTT netem · Byzantine batches "
            "have f direct holders (one below PoA) in fixed per-lane groups"
        ),
    )
    parser.add_argument("--accept-pct", type=float, default=95.0)
    return parser.parse_args()


def load(path: Path) -> list[dict[str, float | str]]:
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source))
    fields = set(rows[0]) if rows else set()
    missing = REQUIRED - fields
    if missing:
        raise SystemExit(f"{path}: missing columns: {', '.join(sorted(missing))}")
    numeric = REQUIRED - {"protocol"}
    return [
        {
            key: float(value) if key in numeric else value
            for key, value in row.items()
        }
        for row in rows
    ]


def grouped_medians(
    rows: list[dict[str, float | str]], protocol: str
) -> list[dict[str, float]]:
    groups: dict[float, list[dict[str, float | str]]] = defaultdict(list)
    for row in rows:
        if row["protocol"] == protocol:
            groups[float(row["offered_tps"])].append(row)
    points = []
    for offered, records in sorted(groups.items()):
        point = {"offered_tps": offered}
        for field in REQUIRED - {"protocol", "offered_tps"}:
            values = [float(record[field]) for record in records]
            point[field] = statistics.median(values)
            point[f"{field}_min"] = min(values)
            point[f"{field}_max"] = max(values)
        points.append(point)
    return points


def main() -> int:
    args = parse_args()
    rows = [row for path in args.measurements for row in load(path)]
    output = args.output or args.measurements[0].with_suffix(".png")

    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    figure, (throughput_axis, latency_axis, egress_axis) = plt.subplots(
        1, 3, figsize=(16.2, 5.6)
    )
    ideal: dict[float, float] = {}
    for key, label, color, marker in PROTOCOLS:
        points = grouped_medians(rows, key)
        if not points:
            continue
        offered = [point["offered_tps"] for point in points]
        committed = [point["committed_tps"] for point in points]
        p50 = [point["p50_ms"] for point in points]
        p99 = [point["p99_ms"] for point in points]
        egress = [point["max_node_wire_mbps"] for point in points]
        relay = [point["max_node_optimistic_batch_relay_mbps"] for point in points]
        for point in points:
            ideal[point["offered_tps"]] = point["honest_offered_tps"]

        throughput_axis.plot(offered, committed, color=color, linewidth=2.2, label=label)
        latency_axis.plot(offered, p50, color=color, linewidth=2.2)
        latency_axis.plot(offered, p99, color=color, linestyle=":", linewidth=1.7)
        egress_axis.plot(offered, egress, color=color, linewidth=2.2)
        if key == "autobahn-optimistic" and any(relay):
            egress_axis.plot(
                offered,
                relay,
                color=color,
                linestyle="--",
                linewidth=1.5,
                alpha=0.9,
            )

        for point, x, y, y50, y99, wire in zip(
            points, offered, committed, p50, p99, egress
        ):
            healthy = y >= point["honest_offered_tps"] * args.accept_pct / 100.0
            face = color if healthy else "white"
            for axis, value, size in (
                (throughput_axis, y, 6.5),
                (latency_axis, y50, 6.0),
                (latency_axis, y99, 5.0),
                (egress_axis, wire, 6.0),
            ):
                axis.plot(
                    [x],
                    [value],
                    marker=marker,
                    markersize=size,
                    markerfacecolor=face,
                    markeredgecolor=color,
                    markeredgewidth=1.4,
                    linestyle="none",
                    zorder=5,
                )

    ideal_points = sorted(ideal.items())
    if ideal_points:
        throughput_axis.plot(
            [point[0] for point in ideal_points],
            [point[1] for point in ideal_points],
            color="0.45",
            linestyle="--",
            linewidth=1.2,
            label="honest offered load",
        )

    for axis in (throughput_axis, latency_axis, egress_axis):
        axis.set_xscale("log")
        axis.set_xlabel("Total offered load (tx/s)")
        axis.grid(True, which="both", alpha=0.22)
    throughput_axis.set_yscale("log")
    throughput_axis.set_ylabel("Committed honest throughput (tx/s)")
    throughput_axis.set_title("Useful throughput")
    latency_axis.set_yscale("log")
    latency_axis.set_ylabel("Honest transaction latency (ms)")
    latency_axis.set_title("Commit latency")
    egress_axis.set_ylabel("Peak validator wire egress (Mbit/s)")
    egress_axis.set_title("Peak per-validator egress")

    handles, labels = throughput_axis.get_legend_handles_labels()
    figure.legend(
        handles
        + [
            Line2D([0], [0], color="0.25", linewidth=2.2, label="p50"),
            Line2D([0], [0], color="0.25", linestyle=":", linewidth=1.7, label="p99"),
            Line2D(
                [0],
                [0],
                color="#D55E00",
                linestyle="--",
                linewidth=1.5,
                label="Autobahn relayed payload",
            ),
            Line2D(
                [0],
                [0],
                color="0.35",
                marker="o",
                markerfacecolor="white",
                linewidth=0,
                label=f"< {args.accept_pct:g}% honest delivery",
            ),
        ],
        labels
        + [
            "p50",
            "p99",
            "Autobahn relayed payload",
            f"< {args.accept_pct:g}% honest delivery",
        ],
        loc="upper center",
        bbox_to_anchor=(0.5, 0.9),
        ncol=4,
        frameon=False,
        fontsize=8.5,
    )
    figure.suptitle(args.title, fontsize=15, y=0.985)
    figure.text(0.5, 0.94, args.subtitle, ha="center", fontsize=9.5, color="0.3")
    figure.tight_layout(rect=(0, 0, 1, 0.79), w_pad=2.4)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=220, bbox_inches="tight")
    figure.savefig(output.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(figure)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
