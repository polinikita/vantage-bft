#!/usr/bin/env python3
"""Score a chaos.sh run: did the committee stay live, and did each victim rejoin?

Reads data/chaos-timeline.json (written by chaos.sh) and the local Prometheus, and
answers the two questions the run exists to ask:

  1. LIVENESS -- while one validator was down, did the REMAINING committee keep
     committing? With one node down the fleet's own offered load also drops by 1/n
     (each container runs its own client), so the honest comparison is the surviving
     nodes' commit rate during the outage against their rate in the quiet window
     just before it -- not total fleet throughput, which conflates the two.

  2. REJOIN -- after the victim came back, did it catch up? A node that resumes
     committing but never closes the view gap is the "connects to every peer and
     then never participates" failure, and it is invisible in aggregate TPS.

    python3 docker-bench/chaos_report.py [--prom http://localhost:9095]
"""

import argparse
import json
import pathlib
import statistics
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
COMMITTED = "committed_transactions"     # worker
CURSOR = "vantage_cursor_next_view"      # primary


def q_range(prom, query, start_s, end_s, step="1s"):
    url = f"{prom}/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": query, "start": f"{start_s:.0f}", "end": f"{end_s:.0f}", "step": step})
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise SystemExit(f"prometheus: {body}")
    return body["data"]["result"]


def by_node(series, role):
    """Group on the `node` label, not `instance`.

    Both processes register overlapping gauge names while only one of them ever sets
    a given gauge; keying on the container would collapse the two and silently read
    the wrong process's constant zero. Same trap as the AWS scrape splitting.
    """
    out = {}
    for s in series:
        name = s["metric"].get("node", "")
        if role == "primary" and not name.endswith("-primary"):
            continue
        if role == "worker" and "-worker-" not in name:
            continue
        try:
            idx = int(name.split("-")[1])
        except (IndexError, ValueError):
            continue
        out[idx] = [(float(t), float(v)) for t, v in s["values"]]
    return out


def rate_between(seq, t0, t1):
    """Committed-counter delta / elapsed, over [t0, t1]. None if not enough samples.

    A frozen (paused) process serves no scrapes at all, so its series simply has no
    points in the window -- that absence is the signal, not an error.
    """
    pts = [(t, v) for t, v in seq if t0 <= t <= t1]
    if len(pts) < 2:
        return None
    span = pts[-1][0] - pts[0][0]
    return (pts[-1][1] - pts[0][1]) / span if span > 0 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", default="http://localhost:9095")
    ap.add_argument("--timeline", default=str(HERE / "data" / "chaos-timeline.json"))
    a = ap.parse_args()

    tl = json.loads(pathlib.Path(a.timeline).read_text())
    start, end = tl["start_ms"] / 1000, tl["end_ms"] / 1000
    settle = tl["settle_start_ms"] / 1000
    n = tl["nodes"]

    committed = by_node(q_range(a.prom, COMMITTED, start - 30, end + 5), "worker")
    cursor = by_node(q_range(a.prom, CURSOR, start - 30, end + 5), "primary")
    if not committed:
        raise SystemExit("no committed_transactions series -- is Prometheus still up?")

    print(f"chaos report: mode={tl['mode']} n={n} f={tl['fault_budget']} "
          f"cycles={tl['cycles']} outage={tl['outage_s']}s settle={tl['settle_s']}s")
    print(f"{'cycle':>5} {'victim':>6} {'others tx/s before':>19} {'during':>9}"
          f" {'retained':>9} {'victim resumed':>15}")
    print("-" * 76)

    for ev in tl["events"]:
        v, d0, d1 = ev["node"], ev["down_ms"] / 1000, ev["up_ms"] / 1000
        others = [i for i in committed if i != v]
        before = [r for i in others
                  if (r := rate_between(committed[i], d0 - tl["outage_s"], d0)) is not None]
        during = [r for i in others if (r := rate_between(committed[i], d0, d1)) is not None]
        b = statistics.median(before) if before else 0.0
        du = statistics.median(during) if during else 0.0
        pct = f"{100 * du / b:.0f}%" if b > 0 else "-"
        # Did the victim commit anything at all in the 15s after it was released?
        after = rate_between(committed.get(v, []), d1, d1 + 15)
        resumed = "-" if after is None else ("yes" if after > 0 else "NO")
        print(f"{ev['cycle']:>5} {v:>6} {b:>19.1f} {du:>9.1f} {pct:>9} {resumed:>15}")

    # Convergence over the settle window: every primary should end within a view or
    # two of the fleet. A victim that rejoined but stalled shows up as a wide spread.
    finals = {}
    for i, seq in cursor.items():
        pts = [v for t, v in seq if t >= settle]
        if pts:
            finals[i] = pts[-1]
    print(f"\nsettle window ({tl['settle_s']}s, all {n} up) -- final cursor_next_view:")
    if finals:
        med = statistics.median(finals.values())
        for i in sorted(finals):
            gap = finals[i] - med
            flag = "  <-- LAGGING" if gap < -2 else ""
            print(f"  node {i:>2}: {finals[i]:>8.0f}  ({gap:+.0f} vs median){flag}")
        spread = max(finals.values()) - min(finals.values())
        print(f"  spread {spread:.0f} views across {len(finals)} node(s)")
    else:
        print("  no cursor samples in the settle window")

    tot = {i: rate_between(seq, settle, end) for i, seq in committed.items()}
    live = [i for i, r in tot.items() if r and r > 0]
    print(f"\ncommitting during settle: {len(live)}/{len(committed)} "
          f"node(s){'' if len(live) == len(committed) else ' -- ' + str(sorted(set(committed) - set(live))) + ' silent'}")
    if tl["dead_at_end"]:
        print(f"NOT RUNNING at end: {tl['dead_at_end']}")


if __name__ == "__main__":
    main()
