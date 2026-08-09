#!/usr/bin/env python3
"""Did the late joiner rejoin, and did state sync do it?

Two questions, deliberately kept apart:

1. Did it catch up? `vantage_cursor_next_view` on the joiner against the fleet median.
   This is the outcome, and it is what a reader actually cares about.
2. Did the INSTALL do it, or did ordinary dissemination? A node restarted into a running
   fleet also receives live traffic, so catching up is not by itself evidence for Phase C.
   `vantage_sequence_install_views_applied_total` is what separates them: views the cursor
   advanced over because they were installed, not executed.
"""
import argparse
import json
import sys
import time
import urllib.parse
import urllib.request


def q(prom, expr, start, end, step="5s"):
    url = f"{prom}/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": expr, "start": f"{start:.0f}", "end": f"{end:.0f}", "step": step})
    with urllib.request.urlopen(url, timeout=60) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise SystemExit(f"prometheus: {body}")
    return body["data"]["result"]


def last(series):
    out = {}
    for s in series:
        node = s["metric"].get("node", "?")
        if s["values"]:
            out[node] = float(s["values"][-1][1])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", default="http://localhost:9095")
    ap.add_argument("--joiner", default="node-20-primary")
    ap.add_argument("--window", type=int, default=600)
    a = ap.parse_args()

    end, start = time.time(), time.time() - a.window
    sel = '{node=~".*-primary$"}'

    cursors = last(q(a.prom, f"vantage_cursor_next_view{sel}", start, end))
    if not cursors:
        print("FAIL: no vantage_cursor_next_view series", file=sys.stderr)
        return 1
    joiner = cursors.get(a.joiner)
    peers = sorted(v for n, v in cursors.items() if n != a.joiner)
    if joiner is None or not peers:
        print(f"FAIL: joiner {a.joiner} absent from {sorted(cursors)}", file=sys.stderr)
        return 1
    median = peers[len(peers) // 2]

    applied = last(q(a.prom, f"vantage_sequence_install_views_applied_total{sel}", start, end))
    completed = last(q(a.prom, f"vantage_sequence_install_completed_total{sel}", start, end))
    failed = last(q(a.prom, f"vantage_sequence_install_failed_total{sel}", start, end))
    started = last(q(a.prom, f"vantage_sequence_sync_started_total{sel}", start, end))
    verified = last(q(a.prom, f"vantage_sequence_sync_verified_total{sel}", start, end))
    exhausted = last(q(a.prom, f"vantage_sequence_sync_exhausted_total{sel}", start, end))
    mismatch = last(q(a.prom, f"vantage_sequence_verify_mismatch_total{sel}", start, end))
    selfcheck = last(q(a.prom, f"vantage_sequence_install_selfcheck_match_total{sel}", start, end))
    awaited = last(q(a.prom, f"vantage_sequence_install_views_ready{sel}", start, end))
    total_v = last(q(a.prom, f"vantage_sequence_install_views{sel}", start, end))

    j = lambda d: int(d.get(a.joiner, 0))
    lag = median - joiner
    print(f"{'joiner cursor':<28} {joiner:>10.0f}")
    print(f"{'fleet median cursor':<28} {median:>10.0f}")
    print(f"{'lag (views)':<28} {lag:>10.0f}")
    print()
    print(f"{'transfers started':<28} {j(started):>10}")
    print(f"{'transfers verified':<28} {j(verified):>10}")
    print(f"{'transfers exhausted':<28} {j(exhausted):>10}")
    print(f"{'views staged in target':<28} {j(total_v):>10}")
    print(f"{'views locally held':<28} {j(awaited):>10}")
    print(f"{'VIEWS INSTALLED':<28} {j(applied):>10}")
    print(f"{'targets installed in full':<28} {j(completed):>10}")
    print(f"{'installs refused':<28} {j(failed):>10}")
    print(f"{'head self-check matches':<28} {j(selfcheck):>10}")
    print(f"{'HEAD MISMATCHES (fleet)':<28} {int(sum(mismatch.values())):>10}")
    print()

    ok = True
    if sum(mismatch.values()) > 0:
        print("FAIL: a verified head disagreed with local execution.", file=sys.stderr)
        ok = False
    if j(failed) > 0:
        print("WARN: an install was refused; see the node log for which condition fired.")
    if j(applied) == 0:
        print("INCONCLUSIVE: the joiner installed nothing, so whatever catch-up happened",
              file=sys.stderr)
        print("              was ordinary dissemination, not state sync.", file=sys.stderr)
        ok = False
    elif lag > max(20, 0.02 * median):
        print(f"PARTIAL: installed {j(applied)} views but still {lag:.0f} behind.")
        ok = False
    if ok:
        print(f"PASS: the joiner installed {j(applied)} views and is within {lag:.0f} "
              f"of the fleet.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
