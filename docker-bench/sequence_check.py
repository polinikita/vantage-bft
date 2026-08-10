#!/usr/bin/env python3
"""Check that correct nodes expose the same sequence head at each boundary.

The check compares full hexadecimal heads across nodes.

    python3 docker-bench/sequence_check.py [--prom http://localhost:9095] [--window 3600]

Exit status is zero when all shared boundaries agree.
"""

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
        print("      Is `sequence_checkpoints` enabled? It defaults OFF.", file=sys.stderr)
        return 1

    # (sid, boundary view) -> head hex -> set of nodes.
    #
    # The metric's VALUE is the boundary view and its `head` LABEL is that boundary's
    # head, so one sample is a complete (node, boundary, head) claim. A node re-exports
    # the same pair on every scrape until it passes the next boundary; collapsing to sets
    # makes the result independent of scrape cadence and of how long a node sat there.
    #
    # KEYED BY SESSION, not just by view. Heads are domain-separated by session id, so two
    # runs against different committees derive different heads for the same view BY
    # DESIGN. docker-bench keeps Prometheus up across runs on purpose and node labels
    # repeat, so a window spanning two runs otherwise reports a spurious divergence --
    # observed doing exactly that. Sessions are scored independently and a run is compared
    # only against itself.
    claims = defaultdict(lambda: defaultdict(set))
    skipped_no_sid = set()
    for s in series:
        node = s["metric"].get("node", s["metric"].get("instance", "?"))
        head = s["metric"].get("head")
        sid = s["metric"].get("sid")
        if not head:
            continue
        if not sid:
            # Emitted by a binary predating the sid label. Such a series cannot be
            # attributed to a session, and lumping several runs together under one
            # placeholder manufactures exactly the divergence this gate looks for.
            skipped_no_sid.add(node)
            continue
        for _, value in s["values"]:
            claims[(sid, int(float(value)))][head].add(node)

    boundaries = sorted(claims)
    compared = [b for b in boundaries if sum(len(n) for n in claims[b].values()) > 1]
    diverged = [b for b in compared if len(claims[b]) > 1]
    sessions = sorted({sid for sid, _ in boundaries})

    if skipped_no_sid:
        print(f"skipped {len(skipped_no_sid)} unlabelled series from a pre-sid binary")
    if not claims:
        print("\nFAIL: every series lacked a sid label (pre-sid binary only).",
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
