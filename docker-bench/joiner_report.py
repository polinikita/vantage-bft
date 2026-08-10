#!/usr/bin/env python3
"""Report late-joiner recovery and state-sync installation."""
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


def instant(prom, expr, at):
    url = f"{prom}/api/v1/query?" + urllib.parse.urlencode(
        {"query": expr, "time": f"{at:.0f}"})
    with urllib.request.urlopen(url, timeout=60) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise SystemExit(f"prometheus: {body}")
    return {
        item["metric"].get("node", "?"): float(item["value"][1])
        for item in body["data"]["result"]
    }


def last_all_up(prom, expected, start, end):
    live = {}
    for series in q(prom, 'up{node=~".*-primary$"}', start, end, "1s"):
        node = series["metric"].get("node", "?")
        live[node] = {int(float(t)) for t, value in series["values"] if float(value) == 1.0}
    missing = sorted(expected - live.keys())
    if missing:
        raise SystemExit(f"prometheus: missing primary up series: {missing}")
    common = set.intersection(*(live[node] for node in sorted(expected)))
    if not common:
        raise SystemExit("prometheus: no timestamp has every primary running")
    return max(common)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", default="http://localhost:9095")
    ap.add_argument("--joiner")
    ap.add_argument("--nodes", type=int)
    ap.add_argument("--window", type=int, default=600)
    a = ap.parse_args()

    end, start = time.time(), time.time() - a.window
    sel = '{node=~".*-primary$"}'
    nodes = a.nodes
    if nodes is None:
        nodes = (
            int(a.joiner.removeprefix("node-").removesuffix("-primary")) + 1
            if a.joiner
            else 10
        )
    joiner_name = a.joiner or f"node-{nodes - 1}-primary"
    expected = {f"node-{index}-primary" for index in range(nodes)}
    sample_time = last_all_up(a.prom, expected, start, end)

    cursors = {
        node: value
        for node, value in instant(a.prom, f"vantage_cursor_next_view{sel}", sample_time).items()
        if node in expected
    }
    if not cursors:
        print("FAIL: no vantage_cursor_next_view series", file=sys.stderr)
        return 1
    joiner = cursors.get(joiner_name)
    peers = sorted(v for n, v in cursors.items() if n != joiner_name)
    if joiner is None or not peers:
        print(f"FAIL: joiner {joiner_name} absent from {sorted(cursors)}", file=sys.stderr)
        return 1
    median = peers[len(peers) // 2]

    metric = lambda name: {
        node: value
        for node, value in instant(a.prom, f"{name}{sel}", sample_time).items()
        if node in expected
    }
    applied = metric("vantage_sequence_install_views_applied_total")
    completed = metric("vantage_sequence_install_completed_total")
    failed = metric("vantage_sequence_install_failed_total")
    started = metric("vantage_sequence_sync_started_total")
    verified = metric("vantage_sequence_sync_verified_total")
    exhausted = metric("vantage_sequence_sync_exhausted_total")
    inbound_dropped = metric("vantage_sequence_sync_inbound_dropped_total")
    mismatch = metric("vantage_sequence_verify_mismatch_total")
    selfcheck = metric("vantage_sequence_install_selfcheck_match_total")
    awaited = metric("vantage_sequence_install_views_ready")
    total_v = metric("vantage_sequence_install_views")
    awaited_blocks = metric("vantage_sequence_install_blocks_awaited")
    header_requests = metric("vantage_sequence_install_headers_requested_total")
    headers_received = metric("vantage_sequence_install_headers_received_total")
    header_in_flight = metric("vantage_sequence_install_header_requests_in_flight")
    obsolete = metric("vantage_sequence_install_obsolete_inbound_dropped_total")

    j = lambda d: int(d.get(joiner_name, 0))
    lag = median - joiner
    print(f"{'common live sample':<28} {time.strftime('%H:%M:%S', time.localtime(sample_time)):>10}")
    print(f"{'joiner cursor':<28} {joiner:>10.0f}")
    print(f"{'fleet median cursor':<28} {median:>10.0f}")
    print(f"{'lag (views)':<28} {lag:>10.0f}")
    print()
    print(f"{'transfers started':<28} {j(started):>10}")
    print(f"{'transfers verified':<28} {j(verified):>10}")
    print(f"{'transfers exhausted':<28} {j(exhausted):>10}")
    print(f"{'sequence inbound drops':<28} {j(inbound_dropped):>10}")
    print(f"{'views staged in target':<28} {j(total_v):>10}")
    print(f"{'views locally held':<28} {j(awaited):>10}")
    print(f"{'blocks awaited in window':<28} {j(awaited_blocks):>10}")
    print(f"{'batched headers requested':<28} {j(header_requests):>10}")
    print(f"{'batched headers received':<28} {j(headers_received):>10}")
    print(f"{'batched headers in flight':<28} {j(header_in_flight):>10}")
    print(f"{'VIEWS INSTALLED':<28} {j(applied):>10}")
    print(f"{'stale sync-gap drops':<28} {j(obsolete):>10}")
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
