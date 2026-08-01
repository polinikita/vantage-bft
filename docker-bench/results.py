#!/usr/bin/env python3
"""Scrape every docker-bench container's published Prometheus endpoint and report
committed TPS / latency, matching node/src/local_benchmark.rs's own summary formula
and TIMELINE line format (see that file's `print_results`/timeline loop).

stdlib only (urllib for HTTP, no `requests`; hand-rolled Prometheus text-exposition
parser, no `prometheus_client`).

What's actually exposed over HTTP (metrics/src/{metrics,snapshot}.rs), investigated so
this tool does not have to guess:
  - `committed_transactions` / `committed_bytes` / `submitted_transactions` /
    `submitted_transactions_bytes`: plain unlabeled counters, on each WORKER's own
    `/metrics` (not the primary's) -- exact values, always current.
  - `transaction_committed_latency{v="p50"|"p90"|"p99"|"p25"|"p75"|"max"|"sum"|"count"}`:
    an `IntGaugeVec` on the worker registry -- the EXACT precise-histogram percentiles
    `node local-benchmark`'s own summary reads in-process, in microseconds. So p50 (and
    p90/p99) genuinely are available over HTTP, not just in-process -- better than a
    bucket-interpolated approximation.
  - Caveat: these gauges are only refreshed every 10s by each node's own periodic
    `MetricReporter` tick (metrics/src/metrics.rs `REPORT_INTERVAL`); the in-process
    harness calls `force_report()` right before its own final read to avoid that lag,
    which an external HTTP scraper structurally cannot do. So a one-shot `results.py`
    read can be up to ~10s stale on the tail of a run -- `--watch` mode is the more
    faithful way to see the true shape of a run for that reason, and the final summary
    should be read as "as of the last periodic tick", not "at the exact instant asked".
"""
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

_LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def load_manifest() -> dict:
    if not MANIFEST_PATH.is_file():
        sys.exit(f"results.py: {MANIFEST_PATH} not found -- run gen.py first")
    return json.loads(MANIFEST_PATH.read_text())


def parse_prometheus_text(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """metric name -> list of (labels, value). Handles both `name value` and
    `name{k="v",...} value` lines; skips comments/blank lines."""
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
                 "count", "p50", "p90", "p99")

    def __init__(self):
        self.reachable = False
        self.committed_transactions = 0
        self.submitted_transactions = 0
        self.count = 0
        self.p50 = self.p90 = self.p99 = None


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
    return s


def snapshot_all(manifest: dict) -> list[NodeSnapshot]:
    return [snapshot_node(manifest, i) for i in range(manifest["nodes"])]


def committed_total(snapshots: list[NodeSnapshot]) -> int:
    # Matches local_benchmark.rs's `max_committed_transactions`: every node counts
    # (approximately) the same replicated commit stream, so the cross-node figure is a
    # max, not a sum.
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


def print_summary(snapshots: list[NodeSnapshot], n_expected: int) -> None:
    """Point-in-time read of the monotonic counters -- deliberately NOT a rate.
    `committed_transactions` counts from container start, not from whenever this
    command happens to be invoked, so `total / (a duration this command was merely
    TOLD, with no way to verify)` silently over- or under-states throughput by
    whatever gap exists between container start and invocation (measured: a run
    invoked ~20s into an already-running cluster inflated TPS by >2x this way). Use
    `--watch` for an actual rate -- it self-baselines from its own first and last
    samples instead of trusting an externally supplied duration."""
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
    print("-----------------------------------------")


def watch(manifest: dict, duration: int | None) -> None:
    """Prints one `TIMELINE:` line per second (byte-identical format to `node
    local-benchmark --timeline`), then a SUMMARY computed from THIS watch's own first
    and last samples -- self-baselined, so it is correct regardless of how long the
    cluster had already been running before this call started (unlike a one-shot
    cumulative-total/externally-given-duration division; see `print_summary`)."""
    prev_total = 0
    first_total: int | None = None
    first_elapsed = 0
    last_snapshots: list[NodeSnapshot] = []
    elapsed = 0
    try:
        while duration is None or elapsed < duration:
            time.sleep(1)
            elapsed += 1
            snapshots = snapshot_all(manifest)
            last_snapshots = snapshots
            total = committed_total(snapshots)
            if first_total is None:
                first_total = total
                first_elapsed = elapsed
            print(
                f"TIMELINE: sec={elapsed} committed_total={total} "
                f"committed_delta={max(0, total - prev_total)}"
            )
            sys.stdout.flush()
            prev_total = total
    except KeyboardInterrupt:
        print()

    if not last_snapshots or first_total is None or elapsed <= first_elapsed:
        return  # too short a window to derive a rate from (e.g. duration <= 1)
    window = elapsed - first_elapsed
    rate = (prev_total - first_total) / window
    print("-----------------------------------------")
    print(f" docker-bench SUMMARY (measured over this {window}s watch window):")
    print("-----------------------------------------")
    print(f" Consensus TPS: {rate:.0f} tx/s  (delta {prev_total - first_total} tx / {window}s)")
    print(latency_line(last_snapshots))
    print("-----------------------------------------")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--watch", action="store_true",
                    help="poll at 1 Hz, printing a TIMELINE: line each second "
                    "(node local-benchmark's own --timeline format), then a SUMMARY "
                    "with a TPS rate self-baselined from this watch's own first/last "
                    "samples. Without --watch, prints a point-in-time snapshot only "
                    "(no derived rate -- see print_summary's own doc comment for why)")
    p.add_argument("--duration", type=float, default=None,
                    help="--watch only: stop after this many seconds (default: until "
                    "Ctrl-C)")
    args = p.parse_args(argv)

    manifest = load_manifest()
    if args.watch:
        duration = int(args.duration) if args.duration else None
        watch(manifest, duration)
    else:
        snapshots = snapshot_all(manifest)
        print_summary(snapshots, manifest["nodes"])


if __name__ == "__main__":
    main()
