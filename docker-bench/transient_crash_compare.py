#!/usr/bin/env python3
"""Multi-arm transient-crash figure: per-second medians across repetitions, one curve per arm.

usage: transient_crash_compare.py OUT_DIR ARM=CAMPAIGN_ROOT [ARM=CAMPAIGN_ROOT ...]
Known arms (legend label, color): base, link, autobahn, simpleit; unknown arms use their name.
Prints a JSON summary of the median curves, keyed by arm.
"""
import json, math, statistics, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def load(root):
    reps = []
    for rep in sorted(Path(root).glob("rep-*")):
        m = json.load(open(rep / "data/manifest.json")); a = m["active_at_ms"] / 1000
        tl = json.load(open(rep / "data/chaos-timeline.json"))
        down = min(e["down_ms"] for e in tl["events"]) / 1000 - a
        up = max(e["up_ms"] for e in tl["events"]) / 1000 - a
        def series(name):
            d = json.load(open(rep / f"data/{name}.json"))["data"]["result"][0]["values"]
            return {int(round(float(t) - a)): float(v) for t, v in d if v not in ("NaN", "+Inf", "-Inf")}
        reps.append({"lat": series("prometheus-latency-window"), "tps": series("prometheus-throughput"),
                     "down": down, "up": up, "duration": int(m["duration"]), "offered": float(m["honest_offered_tps"])})
    return reps

def median_curve(reps, key, duration):
    med, lo, hi = [], [], []
    for s in range(duration + 1):
        xs = [r[key][s] for r in reps if s in r[key]]
        if xs:
            med.append((s, statistics.median(xs))); lo.append((s, min(xs))); hi.append((s, max(xs)))
    return med, lo, hi

def window(curve, start, end):
    return [v for s, v in curve if start <= s <= end]

ARMS = {
    "base": ("Vantage, absolute timers only", "#245b8a"),
    "link": ("Vantage, early refusals", "#c55a11"),
    "autobahn": ("Autobahn", "#2e7d32"),
    "simpleit": ("Simple-IT", "#6a1b9a"),
}

def main(out_dir, *specs):
    arms = []
    for spec in specs:
        name, root = spec.split("=", 1)
        label, color = ARMS.get(name, (name, None))
        arms.append((name, label, color, load(root)))
    all_reps = [r for _, _, _, reps in arms for r in reps]
    duration = min(r["duration"] for r in all_reps)
    down = statistics.median(r["down"] for r in all_reps); up = statistics.median(r["up"] for r in all_reps)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8.5, "axes.titlesize": 9, "axes.labelsize": 8.5,
                         "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 7.5, "axes.linewidth": 0.7,
                         "grid.linewidth": 0.45, "lines.linewidth": 1.4, "pdf.fonttype": 42, "ps.fonttype": 42})
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(7.0, 3.9), sharex=True, gridspec_kw={"height_ratios": [0.8, 1.2], "hspace": 0.25})
    summary = {}
    for name, label, color, reps in arms:
        tmed, _, _ = median_curve(reps, "tps", duration)
        lmed, llo, lhi = median_curve(reps, "lat", duration)
        line, = top.plot(*zip(*tmed), color=color, label=label)
        color = line.get_color()
        bottom.plot(*zip(*lmed), color=color, label=label)
        bottom.fill_between([s for s, _ in llo], [v for _, v in llo], [v for _, v in lhi], color=color, alpha=0.18, linewidth=0)
        pre = window(lmed, 5, down - 2); crash = window(lmed, down + 5, up)
        summary[name] = {
            "reps": len(reps),
            "pre_p50_ms": statistics.median(pre),
            "crash_window_p50_ms": statistics.median(crash),
            "crash_window_p90_ms": sorted(crash)[int(0.9 * (len(crash) - 1))],
            "crash_window_peak_ms": max(window(lmed, down, up)),
            "post_restart_peak_ms": max(window(lmed, up, duration)),
            "post_restart_peak_s": max(((v, s) for s, v in lmed if up <= s <= duration))[1],
            "recovered_at_s": next((s for s, v in lmed if s > up and all(w < 1.1 * statistics.median(pre) for t, w in lmed if s <= t <= min(duration, s + 5))), None),
            "final_p50_ms": statistics.median(window(lmed, max(up + 20, duration - 20), duration)),
            "crash_tps_mean": statistics.fmean(window(tmed, down + 5, up - 2)),
            "crash_tps_min": min(window(tmed, down + 5, up)),
            "band_max_ms": max(v for _, v in lhi),
        }
    offered = all_reps[0]["offered"]
    top.axhline(offered, color="black", linestyle="--", linewidth=0.9, label="offered")
    for ax in (top, bottom):
        ax.axvspan(down, up, color="#b8b8b8", alpha=0.34, linewidth=0); ax.grid(axis="y", color="#d5d5d5"); ax.set_ylim(bottom=0)
    # Keep the plateaus legible: clip the latency axis and label any repetition that leaves it.
    y_cap = float(max(1600.0, min(4000.0, 1.15 * max(summary[n]["crash_window_peak_ms"] for n in summary))))
    bottom.set_ylim(0, y_cap)
    slot = 0
    for name, label, color, reps in arms:
        _, _, lhi = median_curve(reps, "lat", duration)
        over = [(s, v) for s, v in lhi if v > y_cap]
        if over:
            s_max, v_max = max(over, key=lambda x: x[1])
            c = [l.get_color() for l in bottom.get_lines() if l.get_label() == label][0]
            bottom.annotate(f"one {label} repetition: {v_max/1000:.1f} s (off scale)", (s_max, y_cap * 0.98),
                            xytext=(10, -16 - 13 * slot), textcoords="offset points", ha="left", va="top", fontsize=7,
                            color=c, arrowprops={"arrowstyle": "-", "color": c, "lw": 0.6})
            slot += 1
    top.set_ylabel("Committed tx/s\n(5-s rate)"); top.set_title("(a) Non-victim throughput"); top.legend(loc="lower left", frameon=False, ncol=3)
    bottom.set_xlim(0, duration); bottom.set_xlabel("Time since measurement start (s)")
    bottom.set_ylabel("Materialization p50\n(latest 1-s window, ms)"); bottom.set_title("(b) Non-victim materialization latency")
    bottom.legend(loc="lower right", frameon=False)
    victims = len(json.load(open(next(Path(arms[0][3] and specs[0].split("=", 1)[1]).glob("rep-*")) / "data/chaos-timeline.json"))["victims"])
    bottom.text((down + up) / 2, 0.96, f"{victims} validators crashed", transform=bottom.get_xaxis_transform(), ha="center", va="top",
                fontsize=7.5, color="#444444", bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.78, "pad": 1.0})
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(out / f"transient-crash-compare.{ext}", bbox_inches="tight", dpi=220)
    summary["fault_start_s"] = down; summary["fault_end_s"] = up
    (out / "transient-crash-compare-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=1, sort_keys=True))

if __name__ == "__main__":
    main(sys.argv[1], *sys.argv[2:])
