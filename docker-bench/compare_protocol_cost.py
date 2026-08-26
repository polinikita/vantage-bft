#!/usr/bin/env python3
"""Cross-protocol cost comparison for docker-bench control runs.

Reads one or more `records.json` files produced by
`check_protocol_controls.py` and reports, per protocol and scenario, the
throughput and latency actually achieved together with the CPU, wire, and
memory spent to achieve it. With several inputs the repetitions are
aggregated (mean and spread) so a difference between protocols can be read
against its own run-to-run noise.

The measurement-validity section is the important half: a cost comparison is
only meaningful if every protocol reached its offered load on an unsaturated
host, so shortfalls, unreachable workers, panics, and CPU cross-check
mismatches are called out rather than averaged away.
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

# Metrics compared across protocols. (json key, label, digits, lower_is_better)
COST_METRICS = (
    ("committed_tps", "Committed TPS", 1, False),
    ("materialised_p50_ms", "Mat. p50 (ms)", 1, True),
    ("materialised_p99_ms", "Mat. p99 (ms)", 1, True),
    ("cpu_cores_total", "CPU cores (agg)", 2, True),
    ("mean_node_cpu_cores", "CPU cores/node", 3, True),
    ("max_node_cpu_cores", "CPU cores peak node", 3, True),
    ("cpu_ms_per_committed_tx", "CPU-ms per tx", 2, True),
    ("wire_mbps_total", "Wire Mbit/s (agg)", 1, True),
    ("mean_node_wire_mbps", "Wire Mbit/s/node", 2, True),
    ("max_node_wire_mbps", "Wire Mbit/s peak node", 2, True),
    ("wire_bytes_per_committed_tx", "Wire B per tx", 0, True),
    ("mean_node_rss_mib", "RSS MiB/node", 0, True),
)


def load(paths: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    metadata: list[dict[str, Any]] = []
    for path in paths:
        payload = json.loads(path.read_text())
        metadata.append(payload.get("metadata", {}))
        for record in payload.get("records", []):
            record["_source"] = path.parent.name
            records.append(record)
    return records, metadata


def fmt(value: Any, digits: int) -> str:
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def aggregate(values: list[float]) -> tuple[float | None, float | None]:
    """Mean and half-spread of the repetitions of one measurement."""
    clean = [v for v in values if v is not None]
    if not clean:
        return None, None
    if len(clean) == 1:
        return clean[0], None
    return statistics.mean(clean), (max(clean) - min(clean)) / 2


def validity_notes(group: list[dict[str, Any]]) -> list[str]:
    notes: list[str] = []
    for record in group:
        tag = record.get("_source", "?")
        offered = record.get("expected_reachable_tps")
        committed = record.get("committed_tps")
        workers = record.get("reachable_workers")
        expected_workers = record.get("expected_live_workers")
        if record.get("error"):
            notes.append(f"{tag}: {record['error']}")
        if record.get("returncode"):
            notes.append(f"{tag}: exit={record['returncode']}")
        if record.get("panics"):
            notes.append(f"{tag}: panics={record['panics']}")
        if workers is not None and workers != expected_workers:
            notes.append(f"{tag}: workers={workers}/{expected_workers}")
        if offered and committed is not None and committed < 0.95 * offered:
            notes.append(
                f"{tag}: throughput shortfall "
                f"{committed:.1f}/{offered:.1f} TPS "
                f"({100 * committed / offered:.1f}%)"
            )
        # The whole-container figure includes the co-located load generator, so
        # it must be at least the primary+worker figure. If it is not, the two
        # independent CPU sources disagree and neither should be trusted.
        validator = record.get("cpu_cores_total")
        container = record.get("cpu_cores_total_container")
        if validator and container and container < validator * 0.98:
            notes.append(
                f"{tag}: CPU cross-check inverted "
                f"(container {container:.2f} < validator {validator:.2f} cores)"
            )
    return notes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "records",
        nargs="+",
        type=Path,
        help="records.json files (one per repetition)",
    )
    parser.add_argument("--scenario", default="clean")
    parser.add_argument("--output", type=Path, help="write the report to this file too")
    args = parser.parse_args()

    records, metadata = load(args.records)
    records = [r for r in records if r.get("scenario") == args.scenario]
    if not records:
        print(f"no records for scenario {args.scenario!r}")
        return 1

    order: list[str] = []
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        key = record["protocol"]
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(record)

    meta = metadata[0] if metadata else {}
    reps = max(len(g) for g in groups.values())
    lines = [
        "# Protocol cost comparison",
        "",
        f"- Scenario: `{args.scenario}`",
        f"- Validators: {meta.get('nodes', '?')}, "
        f"offered {meta.get('rate', '?')} tx/s, "
        f"{meta.get('duration', '?')} s per point",
        f"- Repetitions: {reps} ({', '.join(p.parent.name for p in args.records)})",
        f"- Latency: {meta.get('latency', '?')}",
        f"- Commit: `{meta.get('commit', '?')}`",
        "",
        "CPU is the primary+worker processes only; the co-located load "
        "generator is excluded. Wire is bytes sent, summed over each "
        "validator's primary and worker.",
        "",
    ]

    header = ["Metric"] + [groups[k][0]["protocol_label"] for k in order]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| --- " + "| ---: " * len(order) + "|")

    best: dict[str, tuple[str, float]] = {}
    for key, label, digits, lower_better in COST_METRICS:
        cells = []
        values: dict[str, float] = {}
        for protocol in order:
            mean, spread = aggregate([r.get(key) for r in groups[protocol]])
            if mean is None:
                cells.append("-")
                continue
            values[protocol] = mean
            cell = fmt(mean, digits)
            if spread:
                cell += f" ±{fmt(spread, digits)}"
            cells.append(cell)
        if values:
            winner = (min if lower_better else max)(values, key=values.get)
            best[label] = (winner, values[winner])
        lines.append(f"| {label} | " + " | ".join(cells) + " |")

    # Relative cost against the cheapest protocol on each axis.
    lines += ["", "## Relative cost (1.00 = cheapest on that axis)", ""]
    ratio_metrics = [
        m for m in COST_METRICS if m[3] and m[0] not in ("materialised_p99_ms",)
    ]
    lines.append(
        "| Metric | " + " | ".join(groups[k][0]["protocol_label"] for k in order) + " |"
    )
    lines.append("| --- " + "| ---: " * len(order) + "|")
    for key, label, _digits, _lower in ratio_metrics:
        values = {}
        for protocol in order:
            mean, _ = aggregate([r.get(key) for r in groups[protocol]])
            if mean:
                values[protocol] = mean
        if not values:
            continue
        floor = min(values.values())
        cells = [
            f"{values[p] / floor:.2f}x" if p in values else "-" for p in order
        ]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")

    lines += ["", "## Measurement validity", ""]
    any_notes = False
    for protocol in order:
        notes = validity_notes(groups[protocol])
        label = groups[protocol][0]["protocol_label"]
        if notes:
            any_notes = True
            lines.append(f"- **{label}**: " + "; ".join(notes))
    if not any_notes:
        lines.append(
            "- All protocols reached their offered load with every validator "
            "reachable, no panics, and both CPU sources agreeing."
        )

    report = "\n".join(lines) + "\n"
    print(report)
    if args.output:
        args.output.write_text(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
