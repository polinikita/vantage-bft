#!/usr/bin/env python3
"""Aggregate matched hash-resolver beta sweep artifacts."""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from pathlib import Path

RUN_NAME = re.compile(r"beta-(?P<beta>\d+)-rep-(?P<rep>\d+)$")


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def fmt(value: float | None, digits: int = 1) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def load_runs(root: Path) -> list[dict]:
    runs = []
    for report_path in sorted(root.rglob("report.json")):
        match = RUN_NAME.match(report_path.parent.name)
        if not match:
            continue
        report = json.loads(report_path.read_text())
        details = report["details"]
        dynamics = details.get("resolver_dynamics", {})
        result = details.get("throughput") or {}
        height_rows = dynamics.get("height_rows", [])
        runs.append({
            "beta": int(match.group("beta")),
            "repetition": int(match.group("rep")),
            "passed": bool(report["passed"]),
            "common_open_views": len(details.get("common_open_views", [])),
            "recovery_ms": [
                float(row["completion_to_all_anchor_sealed_ms"])
                for row in details.get("recovery_rows", [])
            ],
            "fault_end_to_drain_ms": float(details["fault_end_to_all_anchor_sealed_ms"]),
            "decision_heights": int(dynamics.get("decision_heights", 0)),
            "anchors_per_block": [float(row["anchors"]) for row in height_rows],
            "applied_targets_per_block": [
                float(row["applied_targets"]) for row in height_rows
            ],
            "height_service_ms": [
                float(row["enter_to_all_decided_ms"])
                for row in height_rows if row.get("enter_to_all_decided_ms") is not None
            ],
            "proposal_to_decide_ms": [
                float(row["proposal_to_all_decided_ms"])
                for row in height_rows if row.get("proposal_to_all_decided_ms") is not None
            ],
            "full_blocks": int(dynamics.get("full_blocks", 0)),
            "queue_peak": int(
                dynamics.get("queue_peak_range", {}).get("maximum", 0)
            ),
            "timed_out_height_views": len(
                dynamics.get("distinct_timed_out_height_views", [])
            ),
            "committed_tps": float(result.get("committed_tps", 0.0)),
            "materialised_p50_ms": result.get("materialised_latency_ms", {}).get("p50"),
            "median_node_cpu_cores": result.get("median_node_cpu_cores"),
            "max_node_cpu_cores": result.get("max_node_cpu_cores"),
            "max_node_wire_mbps": result.get("max_node_wire_mbps"),
            "artifact": str(report_path.parent),
        })
    return runs


def aggregate(runs: list[dict]) -> list[dict]:
    rows = []
    for beta in sorted({run["beta"] for run in runs}):
        group = [run for run in runs if run["beta"] == beta]
        recovery = [value for run in group for value in run["recovery_ms"]]
        anchors = [value for run in group for value in run["anchors_per_block"]]
        targets = [value for run in group for value in run["applied_targets_per_block"]]
        service = [value for run in group for value in run["height_service_ms"]]
        proposal_to_decide = [
            value for run in group for value in run["proposal_to_decide_ms"]
        ]
        drain = [run["fault_end_to_drain_ms"] for run in group]
        cpu = [
            float(run["median_node_cpu_cores"])
            for run in group if run["median_node_cpu_cores"] is not None
        ]
        max_cpu = [
            float(run["max_node_cpu_cores"])
            for run in group if run["max_node_cpu_cores"] is not None
        ]
        wire = [
            float(run["max_node_wire_mbps"])
            for run in group if run["max_node_wire_mbps"] is not None
        ]
        materialised = [
            float(run["materialised_p50_ms"])
            for run in group if run["materialised_p50_ms"] is not None
        ]
        rows.append({
            "beta": beta,
            "repetitions": len(group),
            "passing_repetitions": sum(run["passed"] for run in group),
            "common_open_views_median": median([
                float(run["common_open_views"]) for run in group
            ]),
            "recovery_samples": len(recovery),
            "completion_to_all_sealed_ms": {
                "median": median(recovery),
                "p95": percentile(recovery, 0.95),
                "maximum": max(recovery, default=None),
            },
            "fault_end_to_drain_ms": {
                "median": median(drain),
                "minimum": min(drain, default=None),
                "maximum": max(drain, default=None),
            },
            "decision_heights_median": median([
                float(run["decision_heights"]) for run in group
            ]),
            "anchors_per_block": {
                "median": median(anchors),
                "p95": percentile(anchors, 0.95),
                "maximum": max(anchors, default=None),
            },
            "applied_targets_per_block_median": median(targets),
            "full_block_fraction": (
                sum(run["full_blocks"] for run in group) / len(anchors)
                if anchors else None
            ),
            "height_service_ms": {
                "median": median(service),
                "p95": percentile(service, 0.95),
            },
            "proposal_to_all_decided_ms_median": median(proposal_to_decide),
            "queue_peak_median": median([
                float(run["queue_peak"]) for run in group
            ]),
            "timed_out_height_views_median": median([
                float(run["timed_out_height_views"]) for run in group
            ]),
            "committed_tps_median": median([
                run["committed_tps"] for run in group
            ]),
            "materialised_p50_ms_median": median(materialised),
            "median_node_cpu_cores": median(cpu),
            "max_node_cpu_cores_median": median(max_cpu),
            "max_node_wire_mbps_median": median(wire),
        })
    return rows


def markdown(rows: list[dict]) -> str:
    lines = [
        "| beta | reps pass | mixed opens | decisions | anchors/block | "
        "completion p50/p95 (s) | post-fault drain (s) | height service p50 (s) | "
        "TPS | mat. p50 (ms) | CPU cores med/max | max wire (Mbit/s) |",
        "|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        completion = row["completion_to_all_sealed_ms"]
        drain = row["fault_end_to_drain_ms"]
        service = row["height_service_ms"]
        anchors = row["anchors_per_block"]
        lines.append(
            f"| {row['beta']} | {row['passing_repetitions']}/{row['repetitions']} | "
            f"{fmt(row['common_open_views_median'], 0)} | "
            f"{fmt(row['decision_heights_median'], 0)} | "
            f"{fmt(anchors['median'], 1)}/{fmt(anchors['maximum'], 0)} | "
            f"{fmt(completion['median'] / 1000 if completion['median'] is not None else None, 2)}/"
            f"{fmt(completion['p95'] / 1000 if completion['p95'] is not None else None, 2)} | "
            f"{fmt(drain['median'] / 1000 if drain['median'] is not None else None, 2)} | "
            f"{fmt(service['median'] / 1000 if service['median'] is not None else None, 2)} | "
            f"{fmt(row['committed_tps_median'], 1)} | "
            f"{fmt(row['materialised_p50_ms_median'], 1)} | "
            f"{fmt(row['median_node_cpu_cores'], 2)}/{fmt(row['max_node_cpu_cores_median'], 2)} | "
            f"{fmt(row['max_node_wire_mbps_median'], 1)} |"
        )
    return "\n".join(lines) + "\n"


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args(argv)

    runs = load_runs(args.root)
    if not runs:
        raise SystemExit(f"no beta-N-rep-N/report.json artifacts below {args.root}")
    rows = aggregate(runs)
    payload = {"root": str(args.root), "runs": runs, "by_beta": rows}
    rendered = markdown(rows)
    print(rendered, end="")
    if args.json_output:
        args.json_output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    if args.markdown_output:
        args.markdown_output.write_text(rendered)


if __name__ == "__main__":
    main()
