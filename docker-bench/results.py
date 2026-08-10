#!/usr/bin/env python3
"""Scrape worker Prometheus endpoints and report committed totals and latency."""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MANIFEST_PATH = SCRIPT_DIR / "data" / "manifest.json"
# Dashboard URL printed by watch and run.sh.
GRAFANA_DASHBOARD_URL = "http://localhost:3003/d/vantage-local-benchmark"

_LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def load_manifest() -> dict:
    if not MANIFEST_PATH.is_file():
        sys.exit(f"results.py: {MANIFEST_PATH} not found -- run gen.py first")
    return json.loads(MANIFEST_PATH.read_text())


def parse_prometheus_text(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Parse Prometheus text into metric names and labelled samples."""
    samples: dict[str, list[tuple[dict[str, str], float]]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            name, rest = line.split("{", 1)
            labelpart, _, value = rest.rpartition("}")
            labels = {k: v for k, v in _LABEL_RE.findall(labelpart)}
        else:
            name, _, value = line.rpartition(" ")
            labels = {}
        name = name.strip()
        try:
            samples.setdefault(name, []).append((labels, float(value.strip())))
        except ValueError:
            continue
    return samples


def scrape(url: str, timeout: float = 2.0) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError, TimeoutError):
        return None
    return parse_prometheus_text(text)


def counter(samples: dict, name: str) -> int:
    vals = samples.get(name)
    return int(vals[0][1]) if vals else 0


def gauge_by_label(samples: dict, name: str, label: str, value: str) -> int | None:
    for labels, v in samples.get(name, []):
        if labels.get(label) == value:
            return int(v)
    return None


def worker_url(manifest: dict, i: int) -> str:
    port = manifest["host_worker_metrics_base"] + i
    return f"http://127.0.0.1:{port}/metrics"


def median(values: list[int]) -> int:
    if not values:
        return 0
    values = sorted(values)
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) // 2


class NodeSnapshot:
    __slots__ = ("reachable", "committed_transactions", "submitted_transactions",
                 "count", "p50", "p90", "p99", "m50", "m90", "m99")

    def __init__(self):
        self.reachable = False
        self.committed_transactions = 0
        self.submitted_transactions = 0
        self.count = 0
        self.p50 = self.p90 = self.p99 = None
        self.m50 = self.m90 = self.m99 = None


def snapshot_node(manifest: dict, i: int) -> NodeSnapshot:
    s = NodeSnapshot()
    samples = scrape(worker_url(manifest, i))
    if samples is None:
        return s
    s.reachable = True
    s.committed_transactions = counter(samples, "committed_transactions")
    s.submitted_transactions = counter(samples, "submitted_transactions")
    count = gauge_by_label(samples, "transaction_committed_latency", "v", "count")
    if count:
        s.count = count
        s.p50 = gauge_by_label(samples, "transaction_committed_latency", "v", "p50")
        s.p90 = gauge_by_label(samples, "transaction_committed_latency", "v", "p90")
        s.p99 = gauge_by_label(samples, "transaction_committed_latency", "v", "p99")
    # Materialised latency ends when the payload is local.
    if gauge_by_label(samples, "transaction_materialised_latency", "v", "count"):
        s.m50 = gauge_by_label(samples, "transaction_materialised_latency", "v", "p50")
        s.m90 = gauge_by_label(samples, "transaction_materialised_latency", "v", "p90")
        s.m99 = gauge_by_label(samples, "transaction_materialised_latency", "v", "p99")
    return s


def snapshot_all(manifest: dict) -> list[NodeSnapshot]:
    return [snapshot_node(manifest, i) for i in range(manifest["nodes"])]


def committed_total(snapshots: list[NodeSnapshot]) -> int:
    # Each node counts the replicated stream; use the maximum.
    return max((s.committed_transactions for s in snapshots), default=0)


def latency_line(snapshots: list[NodeSnapshot]) -> str:
    with_latency = [s for s in snapshots if s.reachable and s.p50 is not None]
    if not with_latency:
        return " Real transaction latency: no committed transactions observed yet"
    p50 = median([s.p50 for s in with_latency])
    p90 = median([s.p90 for s in with_latency])
    p99 = median([s.p99 for s in with_latency])
    max_count = max(s.count for s in with_latency)
    return (
        f" Real transaction latency: p50/p90/p99 {p50 / 1000:.2f}/{p90 / 1000:.2f}/"
        f"{p99 / 1000:.2f} ms ({max_count} txs; gauges refresh every 10s, may be stale)"
    )


def materialised_line(snapshots: list[NodeSnapshot]) -> str:
    with_latency = [s for s in snapshots if s.reachable and s.m50 is not None]
    if not with_latency:
        return " Materialised transaction latency: no observations yet"
    m50 = median([s.m50 for s in with_latency])
    m90 = median([s.m90 for s in with_latency])
    m99 = median([s.m99 for s in with_latency])
    return (
        f" Materialised transaction latency: p50/p90/p99 {m50 / 1000:.2f}/"
        f"{m90 / 1000:.2f}/{m99 / 1000:.2f} ms (same refresh caveat)"
    )


def median_p50_ms(snapshots: list[NodeSnapshot], attr: str) -> str:
    """Return the committee-median p50 for a timeline field."""
    values = [getattr(s, attr) for s in snapshots if s.reachable and getattr(s, attr) is not None]
    return f"{median(values) / 1000:.1f}" if values else "-"


def print_summary(snapshots: list[NodeSnapshot], n_expected: int) -> None:
    """Print a counter snapshot, not a rate; use `--watch` for an interval rate."""
    reachable = [s for s in snapshots if s.reachable]
    total = committed_total(snapshots)
    submitted = sum(s.submitted_transactions for s in snapshots)

    print("-----------------------------------------")
    print(" docker-bench SUMMARY (point-in-time; use --watch for a rate):")
    print("-----------------------------------------")
    if len(reachable) < n_expected:
        print(f" [WARNING: only {len(reachable)}/{n_expected} worker metrics endpoint(s) reachable]")
    print(f" Committed total (cumulative since container start): {total} tx")
    print(f" Submitted (summed across nodes): {submitted} tx")
    print(latency_line(snapshots))
    print(materialised_line(snapshots))
    print("-----------------------------------------")


def watch(manifest: dict, duration: int | None, interval: int = 10) -> None:
    """Print interval rates and latency gauges, then a watch-window summary."""
    print(f"Grafana dashboard: {GRAFANA_DASHBOARD_URL}")
    sys.stdout.flush()
    prev_total = 0
    first_total: int | None = None
    first_elapsed = 0
    last_snapshots: list[NodeSnapshot] = []
    elapsed = 0
    try:
        while duration is None or elapsed < duration:
            time.sleep(interval)
            elapsed += interval
            snapshots = snapshot_all(manifest)
            last_snapshots = snapshots
            total = committed_total(snapshots)
            if first_total is None:
                first_total = total
                first_elapsed = elapsed
            delta = max(0, total - prev_total)
            print(
                f"TIMELINE: sec={elapsed} committed_total={total} "
                f"committed_delta={delta} tps={delta / interval:.0f} "
                f"p50_ms={median_p50_ms(snapshots, 'p50')} "
                f"mat_p50_ms={median_p50_ms(snapshots, 'm50')}"
            )
            sys.stdout.flush()
            prev_total = total
    except KeyboardInterrupt:
        print()

    if not last_snapshots or first_total is None or elapsed <= first_elapsed:
        return  # Too short a window to derive a rate.
    window = elapsed - first_elapsed
    rate = (prev_total - first_total) / window
    print("-----------------------------------------")
    print(f" docker-bench SUMMARY (measured over this {window}s watch window):")
    print("-----------------------------------------")
    print(f" Consensus TPS: {rate:.0f} tx/s  (delta {prev_total - first_total} tx / {window}s)")
    print(latency_line(last_snapshots))
    print(materialised_line(last_snapshots))
    print("-----------------------------------------")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--watch", action="store_true",
                    help="poll every --interval seconds, printing a TIMELINE: line "
                    "per sample (committed total/delta/tps plus committee-median "
                    "p50 committed and materialised latency), then a SUMMARY with a "
                    "TPS rate self-baselined from this watch's own first/last "
                    "samples. Without --watch, prints a point-in-time snapshot only "
                    "(no derived rate -- see print_summary's own doc comment for why)")
    p.add_argument("--duration", type=float, default=None,
                    help="--watch only: stop after this many seconds (default: until "
                    "Ctrl-C)")
    p.add_argument("--interval", type=int, default=10,
                    help="--watch only: seconds between TIMELINE samples (default "
                    "10, matching the nodes' own latency-gauge refresh)")
    args = p.parse_args(argv)

    manifest = load_manifest()
    if args.watch:
        duration = int(args.duration) if args.duration else None
        watch(manifest, duration, max(1, args.interval))
    else:
        snapshots = snapshot_all(manifest)
        print_summary(snapshots, manifest["nodes"])


if __name__ == "__main__":
    main()
