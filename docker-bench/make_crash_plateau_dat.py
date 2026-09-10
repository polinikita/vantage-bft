#!/usr/bin/env python3
"""Crash-window plateau per protocol and fault budget from a crash campaign.

usage: make_crash_plateau_dat.py STAMP ARM[=LABEL]... -- SIZES...
   e.g. make_crash_plateau_dat.py 20260910 base=vantage autobahn simpleit -- 4 7 10 13 16 19 22 25 28

For each committee size and arm, the repetitions' per-second non-victim p50
curves are combined as in transient_crash_compare.py (median over repetitions),
and the plateau is the median of that curve over the crash window, excluding
its first five seconds.  `lo`/`hi` are the min and max of the same statistic
taken per repetition, `pre` the fault-free p50, `tps_min` the lowest
throughput of the median curve in the window.  Only repetitions that
completed (rep-<k>/figures/transient-crash-summary.json present) count.
Seconds; nan where an arm has no completed repetition.
"""
import json
import re
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from transient_crash_compare import median_curve, window  # noqa: E402

ROOT = Path.home() / "vantage-direct-q5/docker-bench/recovery-runs"


def completed_reps(campaign: Path) -> list[Path]:
    return sorted(
        rep for rep in campaign.glob("rep-*")
        if re.fullmatch(r"rep-\d+", rep.name) and (rep / "figures/transient-crash-summary.json").is_file()
    )


def load_reps(reps: list[Path]) -> list[dict]:
    """The per-repetition series in transient_crash_compare.load()'s shape."""
    out = []
    for rep in reps:
        m = json.load(open(rep / "data/manifest.json"))
        a = m["active_at_ms"] / 1000
        tl = json.load(open(rep / "data/chaos-timeline.json"))
        down = min(e["down_ms"] for e in tl["events"]) / 1000 - a
        up = max(e["up_ms"] for e in tl["events"]) / 1000 - a

        def series(name):
            d = json.load(open(rep / f"data/{name}.json"))["data"]["result"][0]["values"]
            return {int(round(float(t) - a)): float(v) for t, v in d if v not in ("NaN", "+Inf", "-Inf")}

        out.append({"lat": series("prometheus-latency-window"), "tps": series("prometheus-throughput"),
                    "down": down, "up": up, "duration": int(m["duration"])})
    return out


def plateau(reps: list[dict]) -> dict:
    duration = min(r["duration"] for r in reps)
    down = statistics.median(r["down"] for r in reps)
    up = statistics.median(r["up"] for r in reps)
    lmed, _, _ = median_curve(reps, "lat", duration)
    tmed, _, _ = median_curve(reps, "tps", duration)
    per_rep = [statistics.median(window(sorted(r["lat"].items()), r["down"] + 5, r["up"])) for r in reps]
    return dict(
        med=statistics.median(window(lmed, down + 5, up)) / 1000,
        lo=min(per_rep) / 1000,
        hi=max(per_rep) / 1000,
        pre=statistics.median(window(lmed, 5, down - 2)) / 1000,
        tps_min=min(window(tmed, down + 5, up)),
        reps=len(reps),
    )


def main(argv: list[str]) -> None:
    stamp, rest = argv[0], argv[1:]
    split = rest.index("--")
    arms = [(a.split("=")[0], a.split("=")[-1]) for a in rest[:split]]
    sizes = [int(s) for s in rest[split + 1:]]
    keys = ("med", "lo", "hi", "pre", "tps_min", "reps")
    print(" ".join(["f", "n"] + [f"{label}_{k}" for _, label in arms for k in keys]))
    for n in sizes:
        row = [str((n - 1) // 3), str(n)]
        for arm, _ in arms:
            reps = completed_reps(ROOT / f"{stamp}-n{n}-crash-{arm}-r3")
            if not reps:
                row += ["nan"] * 5 + ["0"]
                continue
            p = plateau(load_reps(reps))
            row += [f"{p['med']:.3f}", f"{p['lo']:.3f}", f"{p['hi']:.3f}", f"{p['pre']:.3f}",
                    f"{p['tps_min']:.0f}", str(p["reps"])]
        print(" ".join(row))


if __name__ == "__main__":
    main(sys.argv[1:])
