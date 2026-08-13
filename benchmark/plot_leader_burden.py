#!/usr/bin/env python3
"""Plot useful throughput and latency for the leader-relay stress experiment."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


PROTOCOLS = (
    ("autobahn-optimistic", "Autobahn optimistic (all-to-all)", "#D55E00", "s"),
    ("vantage", "Vantage", "#0072B2", "o"),
    ("simple-it", "Simple-IT (Opt-RBC)", "#009E73", "D"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("measurements", type=Path, help="CSV produced by the experiment")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--offered-tps", type=int, default=5_000)
    return parser.parse_args()


def load_measurements(path: Path) -> list[dict[str, float | str]]:
    with path.open(newline="") as source:
        records = list(csv.DictReader(source))
    required = {"protocol", "adversarial_tps", "useful_tps", "p50_ms", "p99_ms"}
    if not records or not required <= records[0].keys():
        missing = sorted(required - (records[0].keys() if records else set()))
        raise SystemExit(f"{path}: missing columns: {', '.join(missing)}")
    return [
        {
            "protocol": record["protocol"],
            "adversarial_tps": float(record["adversarial_tps"]),
            "useful_tps": float(record["useful_tps"]),
            "p50_ms": float(record["p50_ms"]),
            "p99_ms": float(record["p99_ms"]),
        }
        for record in records
    ]


def main() -> int:
    args = parse_args()
    records = load_measurements(args.measurements)
    output = args.output or args.measurements.with_suffix(".png")

    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    figure, (throughput_axis, latency_axis) = plt.subplots(1, 2, figsize=(13.6, 5.8))
    for key, label, color, marker in PROTOCOLS:
        points = sorted(
            (record for record in records if record["protocol"] == key),
            key=lambda record: record["adversarial_tps"],
        )
        if not points:
            continue
        background = [point["adversarial_tps"] / 1_000 for point in points]
        useful = [point["useful_tps"] for point in points]
        p50 = [point["p50_ms"] for point in points]
        p99 = [point["p99_ms"] for point in points]
        throughput_axis.plot(
            background,
            useful,
            color=color,
            marker=marker,
            markersize=7,
            linewidth=2.3,
            label=label,
        )
        latency_axis.plot(
            background,
            p50,
            color=color,
            marker=marker,
            markersize=6,
            linewidth=2.3,
        )
        latency_axis.plot(background, p99, color=color, linestyle=":", linewidth=1.7)

    throughput_axis.axhline(
        args.offered_tps,
        color="0.25",
        linestyle="--",
        linewidth=1.3,
        label=f"useful load offered ({args.offered_tps:,} TPS)",
    )
    throughput_axis.axhline(
        args.offered_tps * 0.95,
        color="0.65",
        linestyle=":",
        linewidth=1.2,
        label="95% stability threshold",
    )
    throughput_axis.set_ylim(0, args.offered_tps * 1.12)
    throughput_axis.set_xlabel("Adversarial lane payload (thousand tx/s)")
    throughput_axis.set_ylabel("Committed useful throughput (tx/s)")
    throughput_axis.set_title("Useful throughput")
    throughput_axis.grid(True, alpha=0.22)
    throughput_axis.legend(frameon=False, fontsize=9, loc="lower left")

    latency_axis.set_yscale("log")
    latency_axis.set_xlabel("Adversarial lane payload (thousand tx/s)")
    latency_axis.set_ylabel("Committed useful-transaction latency (ms)")
    latency_axis.set_title("Latency of transactions that commit")
    latency_axis.grid(True, which="both", alpha=0.22)
    latency_axis.legend(
        handles=[
            Line2D([0], [0], color="0.25", linewidth=2.3, label="p50"),
            Line2D([0], [0], color="0.25", linewidth=1.7, linestyle=":", label="p99"),
        ],
        frameon=False,
        loc="upper left",
    )
    latency_axis.text(
        0.99,
        0.02,
        "Latency becomes survivor-biased once useful throughput collapses.",
        transform=latency_axis.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.5,
        color="0.38",
    )

    figure.suptitle("Leader-relay burden under narrowcast data lanes", fontsize=15, y=0.995)
    figure.text(
        0.5,
        0.925,
        "n=20 · AWS RTT matrix via tc netem · 5,000 useful TPS on 14 correct lanes · "
        "6 silent Byzantine authors · each correct leader misses 2 of 6 faulty lanes",
        ha="center",
        fontsize=9.5,
        color="0.3",
    )
    figure.tight_layout(rect=(0, 0.02, 1, 0.89), w_pad=3.0)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=220, bbox_inches="tight")
    figure.savefig(output.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(figure)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
