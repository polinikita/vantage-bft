#!/usr/bin/env python3
"""Judge late-joiner recovery and contribution."""
import datetime
import json
import os
from pathlib import Path
import statistics
import sys
import urllib.parse
import urllib.request

PROM = "http://localhost:9095"
DATA = Path(__file__).resolve().parent / "data"
# The joiner is the last committee node.
JOINER = f"node-{int(os.environ.get('NODES', 10)) - 1}-primary"


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


def series_delta(expr, s, e):
    out = {}
    for ser in rng(expr, s, e, "20s"):
        vals = [float(v[1]) for v in ser["values"]]
        out[ser["metric"].get("node", "?")] = sum(max(b - a, 0.0) for a, b in zip(vals, vals[1:]))
    return out


def counter_delta(metric, s, e):
    return series_delta(f'{metric}{{node=~".*-primary$"}}', s, e)


def author_of(node):
    """Map a node label to its generated committee author prefix."""
    import re
    idx = int(re.search(r"node-(\d+)-", node).group(1))
    path = DATA / f"node-{idx}/key.json"
    try:
        with open(path) as fh:
            return json.load(fh)["name"][:16]
    except Exception:
        return "?"


def main():
    day = sys.argv[3] if len(sys.argv) > 3 else datetime.date.today().isoformat()
    fmt = "%Y-%m-%d %H:%M:%S"
    s = datetime.datetime.strptime(f"{day} {sys.argv[1]}", fmt).timestamp()
    e = datetime.datetime.strptime(f"{day} {sys.argv[2]}", fmt).timestamp()
    if e < s:
        e += datetime.timedelta(days=1).total_seconds()

    fleet = one('max(vantage_cursor_next_view{node=~".*-primary$"})', s, e)
    join = one(f'vantage_cursor_next_view{{node="{JOINER}"}}', s, e)
    rec = one(f'vantage_sequence_sync_recovered{{node="{JOINER}"}}', s, e)
    tr = one(f'rate(vantage_sequence_sync_verified_total{{node="{JOINER}"}}[20s])', s, e)
    # Exclude the joiner from the peer spread.
    peer_sel = '{node=~".*-primary$",node!="%s"}' % JOINER
    spread = one(f'max(vantage_cursor_next_view{peer_sel}) - '
                 f'min(vantage_cursor_next_view{peer_sel})', s, e)

    print(" time        lag  recovered  transfers/s")
    times = sorted(fleet)
    for t in times:
        lag = fleet[t] - join.get(t, 0)
        print("  ", datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S"),
              f"{lag:>9,.0f} {rec.get(t, -1):>10.0f} {tr.get(t, -1):>12.2f}")

    # Score the final third of the run.
    tail = times[len(times) * 2 // 3:]
    if not tail:
        print("\nno samples")
        return 1
    tail_lag = [fleet[t] - join.get(t, 0) for t in tail]
    tail_tr = [tr.get(t, 0.0) for t in tail]
    tail_rec = [rec.get(t, 0.0) for t in tail]
    peer_spread = statistics.median([spread.get(t, 0.0) for t in tail])

    contribution_start = tail[0]
    contribution_end = tail[-1]
    seconds = max(contribution_end - contribution_start, 1)

    # Read committed blocks from a peer.
    pub = counter_delta("vantage_blocks_published", contribution_start, contribution_end)
    by_author = {}
    for ser in rng(
            'vantage_committed_by_author{node="node-0-primary"}',
            contribution_start, contribution_end, "20s"):
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
    print(f"  (tail data blocks, peer-observed: joiner committed {jc:,.0f} of {jp:,.0f})")

    # Score proposals only after recovery, using the same tail as the CPU checks.
    jm = counter_delta(
        "vantage_own_proposals_made_total", contribution_start, contribution_end
    ).get(JOINER, 0.0)
    jt = counter_delta(
        "vantage_own_proposer_turns_total", contribution_start, contribution_end
    ).get(JOINER, 0.0)
    jpc = counter_delta(
        "vantage_own_proposals_committed_total", contribution_start, contribution_end
    ).get(JOINER, 0.0)
    skipped = counter_delta(
        "vantage_own_proposals_skipped_total", contribution_start, contribution_end
    )
    # Derive skipped outcomes when the direct metric is absent.
    js = skipped.get(JOINER, max(jt - jpc, 0.0))
    print(f"  tail proposals: made {jm:,.0f}, turns {jt:,.0f}, "
          f"committed {jpc:,.0f}, skipped {js:,.0f}")

    direct = series_delta(
        'vantage_walk_steps_total{family="direct",node=~".*-primary$"}',
        contribution_start, contribution_end)
    inbound = series_delta(
        'utilization_timer{proc="inbound_dispatch",node=~".*-primary$"}',
        contribution_start, contribution_end)
    effects = series_delta(
        'utilization_timer{proc="effect_execution",node=~".*-primary$"}',
        contribution_start, contribution_end)

    def rate_pair(values):
        joiner_rate = values.get(JOINER, 0.0) / seconds
        peer_rates = [value / seconds for node, value in values.items() if node != JOINER]
        return joiner_rate, statistics.median(peer_rates) if peer_rates else 0.0

    direct_j, direct_p = rate_pair(direct)
    inbound_j, inbound_p = rate_pair(inbound)
    effects_j, effects_p = rate_pair(effects)
    print(f"  tail direct walks/s: joiner {direct_j:,.0f} vs peer median {direct_p:,.0f}")
    print(f"  tail core utilization: inbound {inbound_j / 1e4:.2f}% vs {inbound_p / 1e4:.2f}%, "
          f"effects {effects_j / 1e4:.2f}% vs {effects_p / 1e4:.2f}%")

    print()
    checks = [
        ("1 recovered latched", all(r == 1 for r in tail_rec), f"tail values {set(tail_rec)}"),
        ("2 transfers stopped", max(tail_tr) == 0.0, f"tail max {max(tail_tr):.2f}/s"),
        ("3 lag ~ peer spread", statistics.median(tail_lag) <= max(2 * peer_spread, 40),
         f"median lag {statistics.median(tail_lag):.0f} vs PEER-ONLY spread {peer_spread:.0f}"),
        ("4 data blocks committed", jr >= 0.8 * pr if pr else False,
         f"joiner ratio {jr:.2f} vs peer median {pr:.2f}"),
        ("5 all proposals committed", jm > 0 and js == 0,
         f"joiner made {jm:.0f}, committed {jpc:.0f}, skipped {js:.0f} across {jt:.0f} turns"),
        ("6 direct-walk CPU", direct_p > 0 and direct_j <= 1.5 * direct_p,
         f"joiner {direct_j:.0f}/s vs peer median {direct_p:.0f}/s"),
        ("7 inbound utilization", inbound_p > 0 and inbound_j <= 1.5 * inbound_p,
         f"joiner {inbound_j / 1e4:.2f}% vs peer median {inbound_p / 1e4:.2f}%"),
        ("8 effect utilization", effects_p > 0 and effects_j <= 1.5 * effects_p,
         f"joiner {effects_j / 1e4:.2f}% vs peer median {effects_p / 1e4:.2f}%"),
    ]
    for name, ok, detail in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<22} {detail}")
    return 0 if all(c[1] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
