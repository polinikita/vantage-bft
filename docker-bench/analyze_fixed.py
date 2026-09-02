#!/usr/bin/env python3
"""Summarize VANTAGE_RESOLVER_EVENT traces for a fixed-binary Q5 run root."""
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

root = Path(sys.argv[1])
prop_re = re.compile(r"kind=propose target=(\d+) view=(\d+) keyed=(\d+) entry=(\w+)")
vote_re = re.compile(r"kind=vote target=(\d+) view=(\d+) accept=(\w+) permanent=(\w+)")
decide_re = re.compile(r"kind=direct_resolver_decide view=(\d+)")

for rep_dir in sorted(root.glob("rep-*")):
    rep = rep_dir.name
    checks = {}
    rj = rep_dir / "mixed" / "report.json"
    if rj.exists():
        data = json.loads(rj.read_text())
        for c in data.get("checks", []):
            checks[c["name"]] = c["passed"]
    proposals = defaultdict(list)
    votes = defaultdict(lambda: defaultdict(int))
    decided = set()
    for log in rep_dir.glob("mixed/data/node-*/logs/primary.log"):
        for line in log.open(errors="replace"):
            if "VANTAGE_RESOLVER_EVENT" in line:
                m = prop_re.search(line)
                if m:
                    proposals[int(m[1])].append((int(m[2]), m[4], int(m[3])))
                    continue
                m = vote_re.search(line)
                if m:
                    t = int(m[1])
                    votes[t]["accept" if m[3] == "true" else "reject"] += 1
                    if m[4] == "true":
                        votes[t]["permanent"] += 1
            elif "direct_resolver_decide" in line:
                m = decide_re.search(line)
                if m:
                    decided.add(int(m[1]))
    targets = sorted(set(proposals) | set(votes))
    max_prop_view = {t: max(v for v, _, _ in proposals[t]) for t in proposals}
    first_kind = defaultdict(int)
    fresh_multi = 0
    for t in proposals:
        fresh = sorted((v, e) for v, e, k in proposals[t] if k == 0)
        if fresh:
            first_kind[fresh[0][1]] += 1
            if len({e for _, e in fresh}) > 1:
                fresh_multi += 1
    total_rej = sum(v["reject"] for v in votes.values())
    total_acc = sum(v["accept"] for v in votes.values())
    total_perm = sum(v["permanent"] for v in votes.values())
    print(f"== {rep}  (report checks failing: {[k for k, ok in checks.items() if not ok]})")
    print(f"   resolver targets seen: {len(targets)}; targets with decide events: {len(decided)}")
    if max_prop_view:
        views = sorted(max_prop_view.values())
        print(
            f"   max proposal view per target: median={views[len(views) // 2]} "
            f"p95={views[int(len(views) * 0.95)]} max={views[-1]}"
        )
        worst = sorted(max_prop_view, key=max_prop_view.get, reverse=True)[:5]
        print(f"   worst targets by view: {[(t, max_prop_view[t]) for t in worst]}")
    print(f"   first fresh proposal kind: {dict(first_kind)}; targets whose fresh kinds vary: {fresh_multi}")
    print(f"   votes: accept={total_acc} reject={total_rej} (permanent={total_perm})")
    worst_rej = sorted(votes, key=lambda t: votes[t]["reject"], reverse=True)[:5]
    print(f"   worst targets by rejects: {[(t, votes[t]['reject']) for t in worst_rej]}")
