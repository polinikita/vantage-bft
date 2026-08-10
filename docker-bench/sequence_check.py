#!/usr/bin/env python3
"""Check full sequence-head agreement at checkpoint boundaries."""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict

METRIC = "vantage_sequence_boundary_head"


def query_range(prom, expr, start, end, step):
    url = f"{prom}/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": expr, "start": f"{start:.0f}", "end": f"{end:.0f}", "step": step})
    with urllib.request.urlopen(url, timeout=60) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise SystemExit(f"prometheus: {body}")
    return body["data"]["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", default="http://localhost:9095")
    ap.add_argument("--window", type=int, default=3600, help="Seconds to scan")
    ap.add_argument("--step", default="10s")
    a = ap.parse_args()

    end = time.time()
    series = query_range(a.prom, METRIC, end - a.window, end, a.step)
    if not series:
        print("FAIL: no vantage_sequence_boundary_head series.", file=sys.stderr)
        print("      Run long enough to cross a checkpoint boundary.", file=sys.stderr)
        return 1

    # Group claims by session, boundary, and head.
    claims = defaultdict(lambda: defaultdict(set))
    skipped_no_sid = set()
    for s in series:
        node = s["metric"].get("node", s["metric"].get("instance", "?"))
        head = s["metric"].get("head")
        sid = s["metric"].get("sid")
        if not head:
            continue
        if not sid:
            # Ignore series without a run identifier.
            skipped_no_sid.add(node)
            continue
        for _, value in s["values"]:
            claims[(sid, int(float(value)))][head].add(node)

    boundaries = sorted(claims)
    compared = [b for b in boundaries if sum(len(n) for n in claims[b].values()) > 1]
    diverged = [b for b in compared if len(claims[b]) > 1]
    sessions = sorted({sid for sid, _ in boundaries})

    if skipped_no_sid:
        print(f"skipped {len(skipped_no_sid)} series without session labels")
    if not claims:
        print("\nFAIL: every series lacks a session label.",
              file=sys.stderr)
        return 1
    print(f"sessions in window: {len(sessions)}"
          + ("  (scored independently)" if len(sessions) > 1 else ""))
    print(f"boundaries observed: {len(boundaries)}")
    print(f"boundaries reached by 2+ nodes: {len(compared)}")

    if not compared:
        print("\nFAIL: no boundary was reached by more than one node, so nothing was",
              file=sys.stderr)
        print("      actually compared. Run longer, or lower", file=sys.stderr)
        print("      sequence_checkpoint_interval_views.", file=sys.stderr)
        return 1

    if diverged:
        first = diverged[0]
        print(f"\nFAIL: {len(diverged)} boundary/-ies with more than one head.",
              file=sys.stderr)
        print(f"      First divergent boundary: session {first[0][:16]}.. view {first[1]}",
              file=sys.stderr)
        for head, nodes in sorted(claims[first].items()):
            print(f"        {head}  <- {sorted(nodes)}", file=sys.stderr)
        print("\n      Reduce to the first divergent VIEW: the heads are hash-chained,",
              file=sys.stderr)
        print("      so the earliest boundary that differs bounds the divergence to the",
              file=sys.stderr)
        print("      views since the last agreeing boundary.", file=sys.stderr)
        return 1

    widest = max(compared, key=lambda b: sum(len(n) for n in claims[b].values()))
    nodes = sum(len(n) for n in claims[widest].values())
    print(f"widest agreement: boundary {widest[1]} across {nodes} nodes, one head")
    print(f"\nPASS: every compared boundary had exactly one head "
          f"({len(compared)} boundaries).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
