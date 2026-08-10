#!/usr/bin/env python3
"""Judge the four pass conditions for "recover, contribute, never resync".

Usage: check_latch.py <start HH:MM:SS> <end HH:MM:SS> [date]

Deliberately mechanical: prints PASS/FAIL per condition so the run is judged against
stated criteria rather than eyeballed.
"""
import datetime
import json
import statistics
import sys
import urllib.parse
import urllib.request

PROM = "http://localhost:9095"
JOINER = "node-20-primary"


def rng(expr, s, e, step="20s"):
    url = f"{PROM}/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": expr, "start": f"{s:.0f}", "end": f"{e:.0f}", "step": step})
    try:
        return json.load(urllib.request.urlopen(url, timeout=30))["data"]["result"]
    except Exception as exc:
        print(f"query failed: {exc}", file=sys.stderr)
        return []


def one(expr, s, e, step="20s"):
    r = rng(expr, s, e, step)
    return {t: float(v) for t, v in (r[0]["values"] if r else [])}


def counter_delta(metric, s, e):
    out = {}
    for ser in rng(f'{metric}{{node=~".*-primary$"}}', s, e, "20s"):
        vals = [float(v[1]) for v in ser["values"]]
        out[ser["metric"].get("node", "?")] = sum(max(b - a, 0.0) for a, b in zip(vals, vals[1:]))
    return out


def author_of(node):
    """Map a node label like 'node-20-primary' to the 16-char base64 key prefix used as the
    metric's author label, read from the generated committee."""
    import re
    idx = int(re.search(r"node-(\d+)-", node).group(1))
    path = f"data/node-{idx}/key.json"
    try:
        with open(path) as fh:
            return json.load(fh)["name"][:16]
    except Exception:
        return "?"


def main():
    day = sys.argv[3] if len(sys.argv) > 3 else "2026-08-10"
    fmt = "%Y-%m-%d %H:%M:%S"
    s = datetime.datetime.strptime(f"{day} {sys.argv[1]}", fmt).timestamp()
    e = datetime.datetime.strptime(f"{day} {sys.argv[2]}", fmt).timestamp()

    fleet = one('max(vantage_cursor_next_view{node=~".*-primary$"})', s, e)
    join = one(f'vantage_cursor_next_view{{node="{JOINER}"}}', s, e)
    rec = one(f'vantage_sequence_sync_recovered{{node="{JOINER}"}}', s, e)
    tr = one(f'rate(vantage_sequence_sync_verified_total{{node="{JOINER}"}}[60s])', s, e)
    # Spread among PEERS ONLY. Including the joiner makes max-min contain the joiner's own
    # lag, so "lag <= spread" would be trivially true -- a circular check.
    peer_sel = '{node=~".*-primary$",node!="%s"}' % JOINER
    spread = one(f'max(vantage_cursor_next_view{peer_sel}) - '
                 f'min(vantage_cursor_next_view{peer_sel})', s, e)

    print(" time        lag  recovered  transfers/s")
    times = sorted(fleet)
    for t in times:
        lag = fleet[t] - join.get(t, 0)
        print("  ", datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S"),
              f"{lag:>9,.0f} {rec.get(t, -1):>10.0f} {tr.get(t, -1):>12.2f}")

    # Judge on the LAST THIRD of the window: the tail is where "never again" lives.
    tail = times[len(times) * 2 // 3:]
    if not tail:
        print("\nno samples")
        return 1
    tail_lag = [fleet[t] - join.get(t, 0) for t in tail]
    tail_tr = [tr.get(t, 0.0) for t in tail]
    tail_rec = [rec.get(t, 0.0) for t in tail]
    peer_spread = statistics.median([spread.get(t, 0.0) for t in tail])

    # Contribution must be read on a CAUGHT-UP PEER, per author. The joiner's own view of
    # its commits is confounded: it has not reached the views where its blocks were
    # committed, so it reports zero regardless.
    pub = counter_delta("vantage_blocks_published", s, e)
    by_author = {}
    for ser in rng('vantage_committed_by_author{node="node-0-primary"}', s, e, "20s"):
        vals = [float(v[1]) for v in ser["values"]]
        by_author[ser["metric"].get("author", "?")] = sum(
            max(b - a, 0.0) for a, b in zip(vals, vals[1:]))
    joiner_key = author_of(JOINER)
    jc = by_author.get(joiner_key, 0.0)
    jp = pub.get(JOINER, 0.0)
    jr = (jc / jp) if jp else 0.0
    peer_ratios = []
    for node, p in pub.items():
        if node == JOINER or not p:
            continue
        peer_ratios.append(by_author.get(author_of(node), 0.0) / p)
    pr = statistics.median(peer_ratios) if peer_ratios else 0.0
    print(f"  (data blocks, peer-observed: joiner committed {jc:,.0f} of {jp:,.0f})")

    # CONSENSUS contribution: proposer turns served and committed. Data blocks carry client
    # transactions and are committed by everyone, so they cannot show whether this node is
    # taking its turn in agreement.
    made = counter_delta("vantage_own_proposals_made_total", s, e)
    turns = counter_delta("vantage_own_proposer_turns_total", s, e)
    prop_com = counter_delta("vantage_own_proposals_committed_total", s, e)
    jt, jm, jpc = turns.get(JOINER, 0), made.get(JOINER, 0), prop_com.get(JOINER, 0)
    peer_prop = [(prop_com.get(n, 0) / t) for n, t in turns.items() if n != JOINER and t]
    ppr = statistics.median(peer_prop) if peer_prop else 0.0
    jpr = (jpc / jt) if jt else 0.0
    print(f"  proposals: joiner made {jm:,.0f}, turns {jt:,.0f}, committed {jpc:,.0f} "
          f"-> {jpr:.2f} vs peer median {ppr:.2f}")

    print()
    checks = [
        ("1 recovered latched", all(r == 1 for r in tail_rec), f"tail values {set(tail_rec)}"),
        ("2 transfers stopped", max(tail_tr) == 0.0, f"tail max {max(tail_tr):.2f}/s"),
        ("3 lag ~ peer spread", statistics.median(tail_lag) <= max(2 * peer_spread, 40),
         f"median lag {statistics.median(tail_lag):.0f} vs PEER-ONLY spread {peer_spread:.0f}"),
        ("4 data blocks committed", jr >= 0.8 * pr if pr else False,
         f"joiner ratio {jr:.2f} vs peer median {pr:.2f}"),
        ("5 own PROPOSALS committed", jpr >= 0.8 * ppr if ppr else False,
         f"joiner {jpr:.2f} vs peer median {ppr:.2f} ({jpc:.0f}/{jt:.0f} turns)"),
    ]
    for name, ok, detail in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<22} {detail}")
    return 0 if all(c[1] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
