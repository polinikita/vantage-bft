#!/usr/bin/env python3
"""Scrape worker Prometheus endpoints and report committed totals and latency."""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
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


def counter_f(samples: dict, name: str) -> float:
    """Float-preserving counter read. CPU seconds are fractional."""
    vals = samples.get(name)
    return float(vals[0][1]) if vals else 0.0


def gauge_by_label(samples: dict, name: str, label: str, value: str) -> int | None:
    for labels, v in samples.get(name, []):
        if labels.get(label) == value:
            return int(v)
    return None


def counter_by_label(samples: dict, name: str, label: str, value: str) -> int:
    for labels, sample in samples.get(name, []):
        if labels.get(label) == value:
            return int(sample)
    return 0


def worker_url(manifest: dict, i: int) -> str:
    port = manifest["host_worker_metrics_base"] + i
    return f"http://127.0.0.1:{port}/metrics"


def primary_url(manifest: dict, i: int) -> str:
    port = manifest["host_primary_metrics_base"] + i
    return f"http://127.0.0.1:{port}/metrics"


def median(values: list[int]) -> int:
    if not values:
        return 0
    values = sorted(values)
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) // 2


class NodeSnapshot:
    __slots__ = ("reachable", "committed_transactions",
                 "committed_uncounted_transactions", "submitted_transactions",
                 "count", "p50", "p90", "p99", "m50", "m90", "m99",
                 "wire_bytes_sent", "optimistic_batch_bytes_sent",
                 "prepare_sync_events", "prepare_missing_headers",
                 "prepare_sync_completed", "prepare_sync_wait_micros",
                 "cpu_seconds", "cpu_seconds_container", "rss_bytes",
                 "cpu_source")

    def __init__(self):
        self.reachable = False
        self.committed_transactions = 0
        self.committed_uncounted_transactions = 0
        self.submitted_transactions = 0
        self.count = 0
        self.p50 = self.p90 = self.p99 = None
        self.m50 = self.m90 = self.m99 = None
        self.wire_bytes_sent = 0
        self.optimistic_batch_bytes_sent = 0
        self.prepare_sync_events = 0
        self.prepare_missing_headers = 0
        self.prepare_sync_completed = 0
        self.prepare_sync_wait_micros = 0
        # Consensus CPU: this validator's primary + worker only.
        self.cpu_seconds = 0.0
        # Whole-container CPU, which also includes the co-located load
        # generator. Kept as an independent cross-check of the number above.
        self.cpu_seconds_container = 0.0
        self.rss_bytes = 0
        # "cgroup" when read from this host's cgroups, "process" when only the
        # node's own exported counter was available (a remote fleet).
        self.cpu_source = "none"


CGROUP_ROOT = Path("/sys/fs/cgroup")
CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
# Resolved once per process: container id and cgroup paths never change within
# a run, so the per-sample cost stays two small file reads per validator.
_CPU_PROBES: dict[int, dict | None] = {}
_CPU_PROBES_RESOLVED = False


def container_name(manifest: dict, i: int) -> str:
    prefix = manifest.get("container_name_prefix", "vantage-node-")
    return f"{prefix}{i}"


def _resolve_cpu_probes(manifest: dict) -> None:
    """Locate every validator's cgroup, with one `docker inspect` for the lot.

    The node process exports `process_cpu_seconds_total` with whole-second
    resolution, which deltas to zero over a short window at moderate load. The
    cgroup exposes `usage_usec`, and `/proc/<pid>/stat` exposes per-process
    ticks, so both are read directly instead.

    Container ids are stable for a run, so they are resolved once and together:
    at n=40 a call per validator would add dozens of subprocess round trips to
    the first sample and skew the start of the measurement window.
    """
    global _CPU_PROBES_RESOLVED
    if _CPU_PROBES_RESOLVED:
        return
    _CPU_PROBES_RESOLVED = True
    names = [container_name(manifest, i) for i in range(manifest["nodes"])]
    try:
        # A missing container makes this exit non-zero while still printing the
        # others, so the output is parsed regardless of the status.
        output = subprocess.run(
            ["docker", "inspect", "-f", "{{.Name}} {{.Id}}", *names],
            capture_output=True, text=True, timeout=120,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return
    ids: dict[str, str] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2:
            ids[parts[0].lstrip("/")] = parts[1]
    for i, name in enumerate(names):
        cid = ids.get(name)
        if not cid:
            continue
        scope = CGROUP_ROOT / "system.slice" / f"docker-{cid}.scope"
        if (scope / "cpu.stat").is_file():
            _CPU_PROBES[i] = {"scope": scope}


def _cpu_probe(manifest: dict, i: int) -> dict | None:
    _resolve_cpu_probes(manifest)
    return _CPU_PROBES.get(i)


def _read_usage_usec(scope: Path) -> float:
    try:
        for line in (scope / "cpu.stat").read_text().splitlines():
            if line.startswith("usage_usec "):
                return float(line.split()[1])
    except OSError:
        pass
    return 0.0


def _read_role_cpu_seconds(scope: Path) -> float:
    """CPU seconds of the primary and worker processes in this cgroup.

    The container also runs the benchmark client; attributing its CPU to the
    protocol would overstate consensus cost, so processes are classified by
    their command line and the client is excluded.
    """
    total_ticks = 0
    try:
        pids = (scope / "cgroup.procs").read_text().split()
    except OSError:
        return 0.0
    for pid in pids:
        try:
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            argv = [part.decode("utf-8", "replace") for part in cmdline if part]
            if not argv or "benchmark_client" in argv[0]:
                continue
            if not any(role in argv for role in ("primary", "worker")):
                continue
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[-1].split()
            # utime and stime, fields 14 and 15 of proc(5), 1-indexed.
            total_ticks += int(fields[11]) + int(fields[12])
        except (OSError, ValueError, IndexError):
            continue
    return total_ticks / CLOCK_TICKS


def snapshot_node(manifest: dict, i: int) -> NodeSnapshot:
    s = NodeSnapshot()
    worker_samples = scrape(worker_url(manifest, i))
    if worker_samples is None:
        return s
    s.reachable = True
    s.committed_transactions = counter(worker_samples, "committed_transactions")
    s.committed_uncounted_transactions = counter(
        worker_samples, "committed_uncounted_transactions"
    )
    s.submitted_transactions = counter(worker_samples, "submitted_transactions")
    s.wire_bytes_sent = counter(worker_samples, "bytes_sent_total")
    s.rss_bytes = counter(worker_samples, "process_resident_memory_bytes")
    s.optimistic_batch_bytes_sent = counter_by_label(
        worker_samples, "network_bytes_sent_total", "type", "OptimisticBatch"
    )
    count = gauge_by_label(worker_samples, "transaction_committed_latency", "v", "count")
    if count:
        s.count = count
        s.p50 = gauge_by_label(worker_samples, "transaction_committed_latency", "v", "p50")
        s.p90 = gauge_by_label(worker_samples, "transaction_committed_latency", "v", "p90")
        s.p99 = gauge_by_label(worker_samples, "transaction_committed_latency", "v", "p99")
    # Materialised latency ends when the payload is local.
    if gauge_by_label(worker_samples, "transaction_materialised_latency", "v", "count"):
        s.m50 = gauge_by_label(worker_samples, "transaction_materialised_latency", "v", "p50")
        s.m90 = gauge_by_label(worker_samples, "transaction_materialised_latency", "v", "p90")
        s.m99 = gauge_by_label(worker_samples, "transaction_materialised_latency", "v", "p99")

    probe = _cpu_probe(manifest, i)
    if probe is not None:
        s.cpu_seconds = _read_role_cpu_seconds(probe["scope"])
        s.cpu_seconds_container = _read_usage_usec(probe["scope"]) / 1e6
        s.cpu_source = "cgroup"
    else:
        # No local cgroup for this validator, so it is not a container on this
        # host: a remote fleet driven by an out-of-tree harness, for example.
        # Fall back to the counter the node exports about itself rather than
        # silently reporting no CPU at all. That counter has whole-second
        # resolution, which is useless at low load but immaterial once a node
        # burns several CPU-seconds per measurement window.
        s.cpu_seconds = counter_f(worker_samples, "process_cpu_seconds_total")
        s.cpu_source = "process"

    primary_samples = scrape(primary_url(manifest, i))
    if primary_samples is not None:
        s.wire_bytes_sent += counter(primary_samples, "bytes_sent_total")
        s.rss_bytes += counter(primary_samples, "process_resident_memory_bytes")
        if s.cpu_source == "process":
            s.cpu_seconds += counter_f(primary_samples, "process_cpu_seconds_total")
        s.prepare_sync_events = counter(
            primary_samples, "autobahn_prepare_sync_events_total"
        )
        s.prepare_missing_headers = counter(
            primary_samples, "autobahn_prepare_missing_headers_total"
        )
        s.prepare_sync_completed = counter(
            primary_samples, "autobahn_prepare_sync_completed_total"
        )
        s.prepare_sync_wait_micros = counter(
            primary_samples, "autobahn_prepare_sync_wait_micros_total"
        )
    return s


def counter_deltas(
    first: list[NodeSnapshot], last: list[NodeSnapshot], field: str
) -> list[int]:
    return [
        max(0, int(getattr(after, field)) - int(getattr(before, field)))
        for before, after in zip(first, last)
        if after.reachable
    ]


def float_deltas(
    first: list[NodeSnapshot], last: list[NodeSnapshot], field: str
) -> list[float]:
    """`counter_deltas` for fractional counters (CPU seconds)."""
    return [
        max(0.0, float(getattr(after, field)) - float(getattr(before, field)))
        for before, after in zip(first, last)
        if after.reachable
    ]


def snapshot_all(manifest: dict) -> list[NodeSnapshot]:
    return [snapshot_node(manifest, i) for i in range(manifest["nodes"])]


def committed_total(snapshots: list[NodeSnapshot]) -> int:
    # Each node counts the replicated stream; use the maximum.
    return max((s.committed_transactions for s in snapshots), default=0)


def submitted_total(snapshots: list[NodeSnapshot]) -> int:
    # Submission is partitioned across workers, so aggregate it.
    return sum(s.submitted_transactions for s in snapshots if s.reachable)


def latency_quantiles_ms(
    snapshots: list[NodeSnapshot], *, materialised: bool
) -> dict[str, float | None]:
    attrs = ("m50", "m90", "m99") if materialised else ("p50", "p90", "p99")
    values = [
        [getattr(s, attr) for s in snapshots if s.reachable and getattr(s, attr) is not None]
        for attr in attrs
    ]
    return {
        quantile: (median(samples) / 1000 if samples else None)
        for quantile, samples in zip(("p50", "p90", "p99"), values)
    }


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
    first_submitted: int | None = None
    first_snapshots: list[NodeSnapshot] | None = None
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
                first_submitted = submitted_total(snapshots)
                first_snapshots = snapshots
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

    if (not last_snapshots or first_total is None or first_submitted is None
            or first_snapshots is None
            or elapsed <= first_elapsed):
        return  # Too short a window to derive a rate.
    window = elapsed - first_elapsed
    committed_delta = prev_total - first_total
    uncounted_deltas = counter_deltas(
        first_snapshots, last_snapshots, "committed_uncounted_transactions"
    )
    # Every validator observes the replicated committed stream; do not sum it.
    uncounted_delta = max(uncounted_deltas, default=0)
    submitted_delta = submitted_total(last_snapshots) - first_submitted
    rate = committed_delta / window
    relay_bytes = counter_deltas(
        first_snapshots, last_snapshots, "optimistic_batch_bytes_sent"
    )
    wire_bytes = counter_deltas(first_snapshots, last_snapshots, "wire_bytes_sent")
    sync_events = counter_deltas(first_snapshots, last_snapshots, "prepare_sync_events")
    sync_missing = counter_deltas(
        first_snapshots, last_snapshots, "prepare_missing_headers"
    )
    sync_completed = counter_deltas(
        first_snapshots, last_snapshots, "prepare_sync_completed"
    )
    sync_wait_micros = counter_deltas(
        first_snapshots, last_snapshots, "prepare_sync_wait_micros"
    )
    # Per-validator CPU time consumed inside the measurement window
    # (primary + worker). Divided by the window it reads as "cores busy".
    cpu_deltas = float_deltas(first_snapshots, last_snapshots, "cpu_seconds")
    cpu_container_deltas = float_deltas(
        first_snapshots, last_snapshots, "cpu_seconds_container"
    )
    live = len(cpu_deltas) or 1
    cpu_seconds_total = sum(cpu_deltas)
    cpu_cores_total = cpu_seconds_total / window
    cpu_container_cores_total = sum(cpu_container_deltas) / window
    rss_values = [s.rss_bytes for s in last_snapshots if s.reachable]
    total_sync_events = sum(sync_events)
    total_sync_completed = sum(sync_completed)
    total_sync_wait_micros = sum(sync_wait_micros)
    print("-----------------------------------------")
    print(f" docker-bench SUMMARY (measured over this {window}s watch window):")
    print("-----------------------------------------")
    print(f" Consensus TPS: {rate:.0f} tx/s  (delta {committed_delta} tx / {window}s)")
    if uncounted_delta:
        print(
            " Committed adversarial payload: "
            f"{uncounted_delta / window:.0f} tx/s "
            f"(delta {uncounted_delta} tx; excluded from Consensus TPS)"
        )
    print(latency_line(last_snapshots))
    print(materialised_line(last_snapshots))
    if sum(relay_bytes):
        print(
            " Optimistic leader batch relay: "
            f"{sum(relay_bytes) / window / 1_000_000:.2f} MB/s aggregate, "
            f"peak node {max(relay_bytes) * 8 / window / 1_000_000:.2f} Mbit/s"
        )
    print(
        f" CPU: {cpu_cores_total:.2f} cores aggregate "
        f"({cpu_cores_total / live:.3f} cores/node, "
        f"peak node {max(cpu_deltas, default=0.0) / window:.3f}), "
        + (
            f"{cpu_seconds_total * 1000 / committed_delta:.3f} CPU-ms per committed tx"
            if committed_delta
            else "no commits"
        )
    )
    cpu_sources = {s.cpu_source for s in last_snapshots if s.reachable}
    if "cgroup" in cpu_sources:
        print(
            f"      (whole container incl. load generator: "
            f"{cpu_container_cores_total:.2f} cores aggregate)"
        )
    if "process" in cpu_sources:
        print(
            "      (from the nodes' own process counters, whole-second "
            "resolution: treat as unreliable below a few CPU-seconds per node "
            "per window)"
        )
    print(
        f" Wire out: {sum(wire_bytes) * 8 / window / 1_000_000:.2f} Mbit/s aggregate "
        f"({sum(wire_bytes) * 8 / window / 1_000_000 / live:.2f} /node, "
        f"peak node {max(wire_bytes, default=0) * 8 / window / 1_000_000:.2f}), "
        + (
            f"{sum(wire_bytes) / committed_delta:.0f} B per committed tx"
            if committed_delta
            else "no commits"
        )
    )
    if rss_values:
        print(
            f" Memory: {statistics.mean(rss_values) / 2**20:.0f} MiB mean/node, "
            f"peak node {max(rss_values) / 2**20:.0f} MiB"
        )
    print("-----------------------------------------")
    result = {
        "measurement_seconds": window,
        "committed_transactions": committed_delta,
        "committed_tps": rate,
        "committed_uncounted_transactions": uncounted_delta,
        "committed_uncounted_tps": uncounted_delta / window,
        "submitted_transactions": submitted_delta,
        "submitted_tps": submitted_delta / window,
        "total_offered_tps": manifest.get("rate"),
        "honest_offered_tps": manifest.get("honest_offered_tps", manifest.get("rate")),
        "reachable_workers": sum(s.reachable for s in last_snapshots),
        "expected_workers": manifest["nodes"] - manifest.get("crash", 0),
        "prepare_sync_events": total_sync_events,
        "prepare_missing_headers": sum(sync_missing),
        "prepare_sync_completed": total_sync_completed,
        "prepare_sync_mean_wait_ms": (
            total_sync_wait_micros / total_sync_completed / 1_000
            if total_sync_completed
            else 0.0
        ),
        "optimistic_batch_relay_bytes": sum(relay_bytes),
        "optimistic_batch_relay_mbps": sum(relay_bytes) * 8 / window / 1_000_000,
        "max_node_optimistic_batch_relay_mbps": (
            max(relay_bytes, default=0) * 8 / window / 1_000_000
        ),
        "max_node_wire_mbps": max(wire_bytes, default=0) * 8 / window / 1_000_000,
        "wire_bytes_total": sum(wire_bytes),
        "wire_mbps_total": sum(wire_bytes) * 8 / window / 1_000_000,
        "mean_node_wire_mbps": sum(wire_bytes) * 8 / window / 1_000_000 / live,
        "wire_bytes_per_committed_tx": (
            sum(wire_bytes) / committed_delta if committed_delta else None
        ),
        "cpu_seconds_total": cpu_seconds_total,
        "cpu_cores_total": cpu_cores_total,
        "mean_node_cpu_cores": cpu_cores_total / live,
        "max_node_cpu_cores": max(cpu_deltas, default=0.0) / window,
        "cpu_cores_total_container": cpu_container_cores_total,
        "cpu_sources": sorted(
            {s.cpu_source for s in last_snapshots if s.reachable}
        ),
        "cpu_ms_per_committed_tx": (
            cpu_seconds_total * 1000 / committed_delta if committed_delta else None
        ),
        "mean_node_rss_mib": (
            statistics.mean(rss_values) / 2**20 if rss_values else None
        ),
        "max_node_rss_mib": (max(rss_values) / 2**20 if rss_values else None),
        "real_latency_ms": latency_quantiles_ms(last_snapshots, materialised=False),
        "materialised_latency_ms": latency_quantiles_ms(
            last_snapshots, materialised=True
        ),
    }
    print("DOCKER_BENCH_RESULT " + json.dumps(result, sort_keys=True))


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
