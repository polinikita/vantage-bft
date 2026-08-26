#!/usr/bin/env python3
"""Measure what post-quantum consensus signatures cost on the Autobahn path.

Runs the same protocol at the same offered load once per signature scheme and
reports the delta against the Ed25519 baseline. The interesting axis is wire
bytes: the ordering path carries one signature per consensus vote and a whole
quorum of them inside every QC, so replacing a 64-byte Ed25519 signature with a
2,420-byte ML-DSA-44 one multiplies the certificate-bearing messages rather
than adding a constant.

Every scheme runs in the same process, sequentially, on one host, so the
comparison is not confounded by a rebuild or a different machine.

    python3 docker-bench/pq_signature_sweep.py \\
        --nodes 20 --rate 400 --duration 90 \\
        --protocol autobahn-optimistic --extra --all-to-all \\
        --schemes ed25519,ml-dsa-44,ml-dsa-65
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "docker-bench" / "run.sh"
RESULT_RE = re.compile(r"^DOCKER_BENCH_RESULT\s+(.*)$")

# Signature and public-key sizes, for reading the measured wire cost against
# the size of the object that changed. FIPS 204 / FIPS 205.
SCHEME_SIZES = {
    "ed25519": (64, 32),
    "ml-dsa-44": (2_420, 1_312),
    "ml-dsa-65": (3_309, 1_952),
    "ml-dsa-87": (4_627, 2_592),
    "slh-dsa-sha2-128s": (7_856, 32),
    "slh-dsa-sha2-128f": (17_088, 32),
    "slh-dsa-sha2-192s": (16_224, 48),
    "slh-dsa-sha2-192f": (35_664, 48),
    "slh-dsa-sha2-256s": (29_792, 64),
    "slh-dsa-sha2-256f": (49_856, 64),
}

REPORT_METRICS = (
    ("committed_tps", "Committed TPS", 1, False),
    ("materialised_p50_ms", "Mat. p50 (ms)", 1, True),
    ("materialised_p99_ms", "Mat. p99 (ms)", 1, True),
    ("wire_mbps_total", "Wire Mbit/s (agg)", 1, True),
    ("mean_node_wire_mbps", "Wire Mbit/s/node", 2, True),
    ("max_node_wire_mbps", "Wire Mbit/s peak node", 2, True),
    ("wire_bytes_per_committed_tx", "Wire B per tx", 0, True),
    ("cpu_cores_total", "CPU cores (agg)", 2, True),
    ("mean_node_cpu_cores", "CPU cores/node", 3, True),
    ("cpu_ms_per_committed_tx", "CPU-ms per tx", 2, True),
    ("mean_node_rss_mib", "RSS MiB/node", 0, True),
)


def run_point(args: argparse.Namespace, scheme: str) -> dict[str, Any]:
    log_path = args.output / f"{args.protocol}-{scheme}.log"
    command = [
        str(RUNNER),
        "--nodes", str(args.nodes),
        "--rate", str(args.rate),
        "--duration", str(args.duration),
        "--protocol", args.protocol,
        "--consensus-signature-scheme", scheme,
        "--no-build",
        *args.extra,
    ]
    print(f"\n=== {args.protocol} / {scheme} "
          f"(n={args.nodes}, {args.rate} tx/s) ===", flush=True)
    payload: dict[str, Any] | None = None
    with log_path.open("w") as log:
        log.write("COMMAND: " + shlex.join(command) + "\n\n")
        process = subprocess.Popen(
            command, cwd=ROOT, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            match = RESULT_RE.match(line.rstrip("\n"))
            if match:
                payload = json.loads(match.group(1))
            stripped = line.lstrip()
            if (
                stripped.startswith("==>")
                or stripped.startswith("Consensus TPS:")
                or stripped.startswith("CPU:")
                or stripped.startswith("Wire out:")
                or stripped.startswith("Memory:")
                or "rror" in stripped
                or "timed out" in stripped
            ):
                print(line, end="", flush=True)
        returncode = process.wait()

    signature_bytes, public_key_bytes = SCHEME_SIZES.get(scheme, (None, None))
    record: dict[str, Any] = {
        "scheme": scheme,
        "protocol": args.protocol,
        "nodes": args.nodes,
        "offered_tps": args.rate,
        "signature_bytes": signature_bytes,
        "public_key_bytes": public_key_bytes,
        "returncode": returncode,
        "log": log_path.name,
    }
    if payload is None:
        record["error"] = "missing DOCKER_BENCH_RESULT"
        return record
    record.update(
        {
            "measurement_seconds": payload["measurement_seconds"],
            "committed_tps": float(payload["committed_tps"]),
            "submitted_tps": float(payload["submitted_tps"]),
            "reachable_workers": payload["reachable_workers"],
            "expected_workers": payload["expected_workers"],
            "materialised_p50_ms": payload["materialised_latency_ms"]["p50"],
            "materialised_p99_ms": payload["materialised_latency_ms"]["p99"],
            "real_p50_ms": payload["real_latency_ms"]["p50"],
            "real_p99_ms": payload["real_latency_ms"]["p99"],
        }
    )
    for key in (
        "wire_mbps_total", "mean_node_wire_mbps", "max_node_wire_mbps",
        "wire_bytes_per_committed_tx", "cpu_cores_total", "mean_node_cpu_cores",
        "max_node_cpu_cores", "cpu_ms_per_committed_tx", "mean_node_rss_mib",
        "cpu_cores_total_container",
    ):
        record[key] = payload.get(key)
    return record


def fmt(value: Any, digits: int) -> str:
    if value is None:
        return "-"
    return f"{value:,.{digits}f}"


def report(args: argparse.Namespace, records: list[dict[str, Any]]) -> str:
    ok = [r for r in records if not r.get("error")]
    baseline = next((r for r in ok if r["scheme"] == "ed25519"), None)
    lines = [
        "# Post-quantum consensus signatures on the Autobahn ordering path",
        "",
        f"- Protocol: `{args.protocol}`"
        + (f" {' '.join(args.extra)}" if args.extra else ""),
        f"- Validators: {args.nodes}, offered {args.rate} tx/s, "
        f"{args.duration} s per point",
        "- Only the ordering path changes scheme; the DAG/data path keeps its "
        "Ed25519 identity key.",
        "",
        "| Metric | " + " | ".join(r["scheme"] for r in ok) + " |",
        "| --- " + "| ---: " * len(ok) + "|",
        "| Signature bytes | "
        + " | ".join(fmt(r["signature_bytes"], 0) for r in ok) + " |",
    ]
    for key, label, digits, _lower in REPORT_METRICS:
        lines.append(
            f"| {label} | " + " | ".join(fmt(r.get(key), digits) for r in ok) + " |"
        )

    if baseline:
        lines += ["", "## Cost relative to Ed25519", ""]
        lines.append("| Metric | " + " | ".join(r["scheme"] for r in ok) + " |")
        lines.append("| --- " + "| ---: " * len(ok) + "|")
        for key, label, _digits, _lower in REPORT_METRICS:
            base = baseline.get(key)
            cells = []
            for record in ok:
                value = record.get(key)
                if not base or value is None:
                    cells.append("-")
                else:
                    cells.append(f"{value / base:.2f}x")
            lines.append(f"| {label} | " + " | ".join(cells) + " |")

    problems = []
    for record in records:
        tag = record["scheme"]
        if record.get("error"):
            problems.append(f"{tag}: {record['error']}")
        if record.get("returncode"):
            problems.append(f"{tag}: exit={record['returncode']}")
        workers = record.get("reachable_workers")
        expected = record.get("expected_workers")
        if workers is not None and workers != expected:
            problems.append(f"{tag}: workers={workers}/{expected}")
        offered = record.get("offered_tps")
        committed = record.get("committed_tps")
        if offered and committed is not None and committed < 0.95 * offered:
            problems.append(
                f"{tag}: throughput shortfall {committed:.1f}/{offered} tx/s "
                f"({100 * committed / offered:.1f}%) -- the scheme could not "
                f"sustain the offered load"
            )
    lines += ["", "## Validity", ""]
    lines += (
        [f"- {p}" for p in problems]
        if problems
        else ["- Every scheme sustained the offered load with all workers reachable."]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--nodes", type=int, default=20)
    parser.add_argument("--rate", type=int, default=400)
    parser.add_argument("--duration", type=int, default=90)
    parser.add_argument("--protocol", default="autobahn-optimistic")
    parser.add_argument(
        "--schemes",
        default="ed25519,ml-dsa-44,ml-dsa-65",
        help="comma-separated consensus signature schemes, in run order",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--extra",
        nargs=argparse.REMAINDER,
        default=[],
        help="remaining arguments are passed to run.sh (e.g. --all-to-all)",
    )
    args = parser.parse_args()
    args.output = args.output.expanduser().resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    schemes = [s.strip() for s in args.schemes.split(",") if s.strip()]

    metadata = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "nodes": args.nodes,
        "rate": args.rate,
        "duration": args.duration,
        "protocol": args.protocol,
        "extra": args.extra,
        "schemes": schemes,
    }
    records: list[dict[str, Any]] = []
    for scheme in schemes:
        records.append(run_point(args, scheme))
        (args.output / "records.json").write_text(
            json.dumps({"metadata": metadata, "records": records}, indent=2) + "\n"
        )
        (args.output / "summary.md").write_text(report(args, records))

    text = report(args, records)
    print("\n" + text)
    print(f"Results: {args.output}")
    return 0 if all(not r.get("error") for r in records) else 1


if __name__ == "__main__":
    sys.exit(main())
