#!/usr/bin/env python3
"""Validate Vantage crash-skip and residual resolver recovery runs."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path

EVENT_PREFIX = "VANTAGE_RECOVERY_EVENT "
RESULT_PREFIX = "DOCKER_BENCH_RESULT "
RESOLVER_ROUTES = {"resolver_full", "resolver_core", "resolver_skip"}


def parse_scalar(value: str):
    try:
        return int(value)
    except ValueError:
        return value


def parse_events(path: Path) -> list[dict]:
    events = []
    for line in path.read_text(errors="replace").splitlines():
        marker = line.find(EVENT_PREFIX)
        if marker < 0:
            continue
        fields = {}
        for token in line[marker + len(EVENT_PREFIX):].split():
            key, separator, value = token.partition("=")
            if separator:
                fields[key] = parse_scalar(value)
        if "kind" in fields and "view" in fields and "epoch_ms" in fields:
            events.append(fields)
    return sorted(events, key=lambda event: (event["epoch_ms"], event["view"]))


def events_by_view(events: list[dict], kind: str) -> dict[int, dict]:
    return {event["view"]: event for event in events if event["kind"] == kind}


def load_result(path: Path | None) -> dict | None:
    if path is None:
        return None
    result = None
    for line in path.read_text(errors="replace").splitlines():
        marker = line.find(RESULT_PREFIX)
        if marker >= 0:
            result = json.loads(line[marker + len(RESULT_PREFIX):])
    return result


def common_views(per_node: dict[int, list[dict]], kind: str) -> set[int]:
    sets = [set(events_by_view(events, kind)) for events in per_node.values()]
    return set.intersection(*sets) if sets else set()


def finalization_summary(per_node: dict[int, list[dict]]) -> dict:
    final_views = {}
    for node, events in per_node.items():
        finalized = events_by_view(events, "finalized")
        final_views[node] = max(finalized, default=-1)
    values = list(final_views.values())
    return {
        "by_node": final_views,
        "minimum": min(values, default=-1),
        "maximum": max(values, default=-1),
        "spread": max(values, default=-1) - min(values, default=-1),
    }


def open_backlog(events: list[dict], cutoff_ms: int | None = None) -> tuple[int, int]:
    timeline = []
    for event in events:
        if cutoff_ms is not None and event["epoch_ms"] > cutoff_ms:
            continue
        if event["kind"] == "completed_open":
            timeline.append((event["epoch_ms"], 1, event["view"]))
        elif event["kind"] == "seal":
            timeline.append((event["epoch_ms"], -1, event["view"]))
    # Completion precedes a same-millisecond seal, keeping the gauge model exact.
    timeline.sort(key=lambda item: (item[0], -item[1], item[2]))
    outstanding = set()
    sealed = set()
    peak = 0
    for _, delta, view in timeline:
        if delta > 0:
            if view not in sealed:
                outstanding.add(view)
                peak = max(peak, len(outstanding))
        else:
            sealed.add(view)
            outstanding.discard(view)
    return peak, len(outstanding)


def add_check(checks: list[dict], name: str, passed: bool, detail: str) -> None:
    checks.append({"name": name, "passed": passed, "detail": detail})


def compact_list(values: list, limit: int = 12) -> str:
    if len(values) <= limit:
        return str(values)
    return f"{values[:limit]} ... (+{len(values) - limit} more)"


def distribution_summary(values: list[int]) -> dict:
    if not values:
        return {"count": 0, "minimum": None, "median": None, "p95": None, "maximum": None}
    ordered = sorted(values)
    p95 = ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]
    return {
        "count": len(ordered),
        "minimum": ordered[0],
        "median": statistics.median(ordered),
        "p95": p95,
        "maximum": ordered[-1],
    }


def direct_resolver_dynamics(
    per_node: dict[int, list[dict]],
    open_maps: dict[int, dict[int, dict]],
    window_start_ms: int,
    window_end_ms: int,
) -> dict:
    """Summarize independent target decisions during the attacked-view drain."""
    decision_maps = {
        node: events_by_view(events, "direct_resolver_decide")
        for node, events in per_node.items()
    }
    complete_targets = set.intersection(
        *(set(mapping) for mapping in decision_maps.values())
    ) if decision_maps else set()
    complete_targets &= set.intersection(
        *(set(mapping) for mapping in open_maps.values())
    ) if open_maps else set()

    rows = []
    for target in sorted(complete_targets):
        decisions = [mapping[target] for mapping in decision_maps.values()]
        all_correct_decided = max(int(event["epoch_ms"]) for event in decisions)
        if not window_start_ms <= all_correct_decided <= window_end_ms:
            continue
        completion_all = max(
            int(mapping[target]["epoch_ms"]) for mapping in open_maps.values()
        )
        first_decided = min(int(event["epoch_ms"]) for event in decisions)
        active = [int(event.get("active", 0)) for event in decisions]
        rows.append({
            "target": target,
            "completion_all_ms": completion_all,
            "first_decided_ms": first_decided,
            "all_correct_decided_ms": all_correct_decided,
            "completion_to_all_decided_ms": all_correct_decided - completion_all,
            "decision_spread_ms": all_correct_decided - first_decided,
            "median_active_targets_at_decision": statistics.median(active),
            "maximum_active_targets_at_decision": max(active),
        })

    service_start = min((row["first_decided_ms"] for row in rows), default=None)
    service_end = max((row["all_correct_decided_ms"] for row in rows), default=None)
    service_seconds = (
        (service_end - service_start) / 1_000
        if service_start is not None and service_end is not None and service_end > service_start
        else None
    )
    return {
        "all_correct_target_decisions": len(rows),
        "target_rows": rows,
        "completion_to_all_decided_ms": distribution_summary([
            row["completion_to_all_decided_ms"] for row in rows
        ]),
        "decision_spread_ms": distribution_summary([
            row["decision_spread_ms"] for row in rows
        ]),
        "active_targets_at_decision": distribution_summary([
            int(row["median_active_targets_at_decision"]) for row in rows
        ]),
        "service_seconds": service_seconds,
        "all_correct_decisions_per_second": (
            len(rows) / service_seconds if service_seconds else None
        ),
    }


def sustained_attack_dynamics(
    common_open: set[int],
    open_maps: dict[int, dict[int, dict]],
    seal_maps: dict[int, dict[int, dict]],
    fault_start_ms: int,
    fault_end_ms: int,
    guard_ms: int,
) -> dict:
    """Compare globally visible mixed-open arrivals with all-correct seals."""
    attack_ms = fault_end_ms - fault_start_ms
    # Discard the attack's first third as a queue warmup, then stop before the
    # 7*Delta containment guard so every counted arrival remains fault-contained.
    window_start_ms = fault_start_ms + max(guard_ms, attack_ms // 3)
    window_end_ms = fault_end_ms - guard_ms
    window_ms = max(0, window_end_ms - window_start_ms)

    arrivals = {
        view: max(int(mapping[view]["epoch_ms"]) for mapping in open_maps.values())
        for view in common_open
    }
    services = {
        view: max(int(mapping[view]["epoch_ms"]) for mapping in seal_maps.values())
        for view in common_open
        if all(view in mapping for mapping in seal_maps.values())
    }

    def backlog_before(epoch_ms: int) -> int:
        return sum(
            arrival < epoch_ms and services.get(view, sys.maxsize) >= epoch_ms
            for view, arrival in arrivals.items()
        )

    arrivals_in_window = sum(
        window_start_ms <= epoch_ms < window_end_ms for epoch_ms in arrivals.values()
    )
    services_in_window = sum(
        window_start_ms <= epoch_ms < window_end_ms for epoch_ms in services.values()
    )
    backlog_start = backlog_before(window_start_ms)
    backlog_end = backlog_before(window_end_ms)

    outstanding = backlog_start
    peak = outstanding
    timeline = [
        (epoch_ms, 1, view)
        for view, epoch_ms in arrivals.items()
        if window_start_ms <= epoch_ms < window_end_ms
    ] + [
        (epoch_ms, -1, view)
        for view, epoch_ms in services.items()
        if window_start_ms <= epoch_ms < window_end_ms
    ]
    timeline.sort(key=lambda item: (item[0], -item[1], item[2]))
    for _, delta, _ in timeline:
        outstanding += delta
        peak = max(peak, outstanding)

    samples = []
    if window_ms > 0:
        epoch_ms = window_start_ms
        while epoch_ms < window_end_ms:
            samples.append((epoch_ms, backlog_before(epoch_ms)))
            epoch_ms += 1_000
        samples.append((window_end_ms, backlog_end))

    linear_slope_per_second = None
    if len(samples) >= 2:
        xs = [(epoch_ms - window_start_ms) / 1_000 for epoch_ms, _ in samples]
        ys = [value for _, value in samples]
        x_mean = statistics.mean(xs)
        y_mean = statistics.mean(ys)
        denominator = sum((x - x_mean) ** 2 for x in xs)
        if denominator:
            linear_slope_per_second = sum(
                (x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)
            ) / denominator

    seconds = window_ms / 1_000
    accounting_error = backlog_end - (
        backlog_start + arrivals_in_window - services_in_window
    )
    return {
        "window_start_ms": window_start_ms,
        "window_end_ms": window_end_ms,
        "window_seconds": seconds,
        "arrivals": arrivals_in_window,
        "all_correct_seals": services_in_window,
        "arrival_rate_per_second": arrivals_in_window / seconds if seconds else None,
        "service_rate_per_second": services_in_window / seconds if seconds else None,
        "service_headroom_per_second": (
            (services_in_window - arrivals_in_window) / seconds if seconds else None
        ),
        "backlog_at_start": backlog_start,
        "backlog_at_end": backlog_end,
        "backlog_peak": peak,
        "backlog_net_growth": backlog_end - backlog_start,
        "backlog_accounting_error": accounting_error,
        "backlog_linear_fit_per_second": linear_slope_per_second,
    }


def validate_common(
    scenario: str,
    manifest: dict,
    parameters: dict,
    per_node: dict[int, list[dict]],
    result: dict | None,
    max_final_spread: int,
) -> tuple[list[dict], dict]:
    checks = []
    finalization = finalization_summary(per_node)
    expected_live = (
        manifest["nodes"] - len(manifest.get("withholding_node_indices", []))
        if scenario == "mixed"
        else manifest["nodes"] - manifest.get("crash", 0)
    )
    add_check(
        checks,
        "correct logs present",
        len(per_node) == expected_live,
        f"parsed {len(per_node)} correct-validator logs",
    )
    add_check(
        checks,
        "state-sync installation disabled",
        not parameters.get("sequence_install_enabled", True)
        and not parameters.get("sequence_checkpoints", True),
        "recovery is measured without checkpoint/state-sync installation",
    )
    add_check(
        checks,
        "all correct validators finalized",
        finalization["minimum"] >= 0,
        f"last-view range={finalization['minimum']}..{finalization['maximum']}",
    )
    add_check(
        checks,
        "final cursor spread",
        finalization["spread"] <= max_final_spread,
        f"spread={finalization['spread']} views (limit {max_final_spread})",
    )

    panic_nodes = []
    for node in per_node:
        text = (Path(manifest["_data_dir"]) / f"node-{node}" / "logs" / "primary.log").read_text(
            errors="replace"
        ).lower()
        if "panicked at" in text or "fatal runtime error" in text:
            panic_nodes.append(node)
    add_check(
        checks,
        "panic free",
        not panic_nodes,
        "no primary panic signatures" if not panic_nodes else f"panic signatures on {panic_nodes}",
    )

    if result is not None:
        expected_workers = manifest["nodes"] - manifest.get("crash", 0)
        reachable = int(result.get("reachable_workers", 0))
        measured_seconds = float(result.get("measurement_seconds", 0.0))
        minimum_seconds = 0.70 * float(manifest.get("duration", 0))
        offered = float(manifest.get("honest_offered_tps", manifest.get("rate", 0)))
        committed = float(result.get("committed_tps", 0))
        add_check(
            checks,
            "workers remained reachable",
            reachable == expected_workers,
            f"reachable={reachable}/{expected_workers}",
        )
        add_check(
            checks,
            "measurement coverage",
            measured_seconds >= minimum_seconds,
            f"complete-snapshot window={measured_seconds:.1f}s "
            f"(minimum {minimum_seconds:.1f}s); incomplete scrapes="
            f"{int(result.get('incomplete_scrape_samples', 0))}",
        )
        add_check(
            checks,
            "useful throughput continued",
            offered > 0 and committed >= 0.80 * offered,
            f"committed={committed:.1f} tx/s, offered-correct={offered:.1f} tx/s",
        )
    else:
        add_check(checks, "run result captured", False, "missing DOCKER_BENCH_RESULT")
    return checks, {"finalization": finalization, "throughput": result}


def validate_clean(
    manifest: dict,
    parameters: dict,
    per_node: dict[int, list[dict]],
) -> tuple[list[dict], dict]:
    checks = []
    # The consensus processes start before the synchronized load epoch. Ignore
    # that startup/no-data transition and match results.py's first 10-second
    # sampling interval when evaluating the fault-free control.
    window_start = int(manifest["active_at_ms"]) + 10_000
    window_end = int(manifest["ended_at_ms"]) - int(parameters["delta_ms"])
    opens = [
        (node, event["view"])
        for node, events in per_node.items()
        for event in events
        if event["kind"] == "completed_open"
        and window_start <= event["epoch_ms"] <= window_end
    ]
    fallback_seals = [
        (node, event["view"], event.get("route"))
        for node, events in per_node.items()
        for event in events
        if event["kind"] == "seal" and event.get("route") in RESOLVER_ROUTES | {"vote_skip"}
        and window_start <= event["epoch_ms"] <= window_end
    ]
    add_check(checks, "no steady-state completed-open views", not opens, f"observed={len(opens)}")
    add_check(
        checks,
        "no recovery-route seals",
        not fallback_seals,
        "all seals used direct/fast paths" if not fallback_seals else f"observed={fallback_seals[:8]}",
    )
    return checks, {
        "steady_window_start_ms": window_start,
        "steady_window_end_ms": window_end,
        "completed_open_events": opens,
        "recovery_route_seals": fallback_seals,
    }


def validate_crash(
    manifest: dict,
    parameters: dict,
    per_node: dict[int, list[dict]],
) -> tuple[list[dict], dict]:
    checks = []
    delta_ms = int(parameters["delta_ms"])
    bound_ms = 7 * delta_ms
    entry_bound_ms = 2 * delta_ms
    active_at = int(manifest["active_at_ms"])
    ended_at = int(manifest["ended_at_ms"])
    common_vote = {
        view
        for view in common_views(per_node, "seal")
        if all(events_by_view(events, "seal")[view].get("route") == "vote_skip"
               for events in per_node.values())
    }
    observations = []
    for view in sorted(common_vote):
        entries = [events_by_view(events, "enter").get(view) for events in per_node.values()]
        seals = [events_by_view(events, "seal").get(view) for events in per_node.values()]
        if any(event is None for event in entries + seals):
            continue
        first_entry = min(event["epoch_ms"] for event in entries)
        last_seal = max(event["epoch_ms"] for event in seals)
        if first_entry < active_at or last_seal > ended_at - delta_ms:
            continue
        observations.append(
            {
                "view": view,
                "first_entry_ms": first_entry,
                "entry_spread_ms": max(event["epoch_ms"] for event in entries)
                - first_entry,
                "last_seal_ms": last_seal,
                "first_entry_to_all_sealed_ms": last_seal - first_entry,
                "seal_spread_ms": max(event["epoch_ms"] for event in seals)
                - min(event["epoch_ms"] for event in seals),
            }
        )
    entry_violations = [row for row in observations if row["entry_spread_ms"] > entry_bound_ms]
    violations = [row for row in observations if row["first_entry_to_all_sealed_ms"] > bound_ms]
    add_check(
        checks,
        "common clean-crash skip views observed",
        bool(observations),
        f"observed={len(observations)} views after the measurement epoch",
    )
    add_check(
        checks,
        "clean-crash entry premise",
        bool(observations) and not entry_violations,
        f"max={max((row['entry_spread_ms'] for row in observations), default=-1)} ms; "
        f"bound=2Delta={entry_bound_ms} ms; violations={len(entry_violations)}",
    )
    add_check(
        checks,
        "configured post-GST skip bound",
        bool(observations) and not violations,
        f"max={max((row['first_entry_to_all_sealed_ms'] for row in observations), default=-1)} ms; "
        f"bound=7Delta={bound_ms} ms; violations={len(violations)}",
    )
    latencies = [row["first_entry_to_all_sealed_ms"] for row in observations]
    entry_spreads = [row["entry_spread_ms"] for row in observations]
    spreads = [row["seal_spread_ms"] for row in observations]
    return checks, {
        "bound_ms": bound_ms,
        "entry_bound_ms": entry_bound_ms,
        "observed_views": len(observations),
        "first_observed_view": observations[0]["view"] if observations else None,
        "last_observed_view": observations[-1]["view"] if observations else None,
        "entry_spread_ms": distribution_summary(entry_spreads),
        "first_entry_to_all_sealed_ms": distribution_summary(latencies),
        "seal_spread_ms": distribution_summary(spreads),
        "entry_violations": entry_violations,
        "violations": violations,
    }


def validate_mixed(
    manifest: dict,
    parameters: dict,
    per_node: dict[int, list[dict]],
    all_events: dict[int, list[dict]],
    minimum_open_views: int,
) -> tuple[list[dict], dict]:
    checks = []
    delta_ms = int(parameters["delta_ms"])
    fault_start = int(manifest["active_at_ms"]) + int(parameters["withhold_at_ms"])
    fault_end = fault_start + int(parameters["withhold_for_ms"])
    publishers = set(manifest["withholding_node_indices"])
    expected_g1 = int(manifest["mixed_open_correct_grade_one"])
    expected_g0 = int(manifest["mixed_open_correct_grade_zero"])

    stress = {}
    for node in publishers:
        for event in all_events[node]:
            if event["kind"] == "stress_propose" and fault_start <= event["epoch_ms"] < fault_end:
                stress[event["view"]] = {**event, "publisher_node": node}
    # Keep both the selected tip's faulted publication lead-in and all of its
    # AGB response stages inside the finite fault window. Boundary proposals
    # are still audited for eventual drain, but a start-boundary proposal may
    # name a pre-fault tip and an end-boundary proposal may receive resumed
    # Byzantine responses.
    fault_containment_guard_ms = 7 * delta_ms
    mature_stress = {
        view: event
        for view, event in stress.items()
        if (
            fault_start + fault_containment_guard_ms
            <= event["epoch_ms"]
            <= fault_end - fault_containment_guard_ms
        )
    }
    open_maps = {node: events_by_view(events, "completed_open") for node, events in per_node.items()}
    seal_maps = {node: events_by_view(events, "seal") for node, events in per_node.items()}
    finalized_maps = {node: events_by_view(events, "finalized") for node, events in per_node.items()}
    common_open = {
        view for view in mature_stress if all(view in mapping for mapping in open_maps.values())
    }
    split_violations = []
    missing_seals = []
    route_mismatches = []
    missing_finalized = []
    recovery_rows = []
    for view in sorted(common_open):
        opens = [mapping[view] for mapping in open_maps.values()]
        seals = [mapping.get(view) for mapping in seal_maps.values()]
        # Completion records the first quorum of correct ECHOs, not a final
        # all-n census. The deterministic holder map therefore gives an upper
        # bound of f direct holders and a lower bound of n-2f repair-only
        # holders at this instant; it need not expose the exact final f/(n-2f)
        # split before the view completes.
        if any(
            not (
                0 < event["echo_g1"] <= expected_g1
                and event["echo_g0"] >= expected_g0
                and event["echo_g1"] + event["echo_g0"] == event["quorum"]
                and event["ready_g1"] < event["quorum"]
                and event["ready_g0"] < event["quorum"]
            )
            for event in opens
        ):
            split_violations.append(view)
        if any(event is None for event in seals):
            missing_seals.append(view)
            continue
        if any(event.get("route") not in RESOLVER_ROUTES for event in seals):
            route_mismatches.append(view)
            continue
        if any(view not in mapping for mapping in finalized_maps.values()):
            missing_finalized.append(view)
        completion_first = min(event["epoch_ms"] for event in opens)
        seal_last = max(event["epoch_ms"] for event in seals)
        recovery_rows.append(
            {
                "view": view,
                "publisher_node": stress[view]["publisher_node"],
                "completion_first_ms": completion_first,
                "all_resolver_sealed_ms": seal_last,
                "completion_to_all_resolver_sealed_ms": seal_last - completion_first,
                "seal_spread_ms": max(event["epoch_ms"] for event in seals)
                - min(event["epoch_ms"] for event in seals),
                "routes": sorted({event["route"] for event in seals}),
            }
        )

    missing_open = sorted(set(mature_stress) - common_open)
    bad_tips = sorted(view for view, event in mature_stress.items() if event.get("tips") != 1)
    measurement_end = manifest.get("ended_at_ms")
    backlog = {
        node: open_backlog(
            events,
            int(measurement_end) if measurement_end is not None else None,
        )
        for node, events in per_node.items()
    }
    peak_min = min((peak for peak, _ in backlog.values()), default=0)
    peak_max = max((peak for peak, _ in backlog.values()), default=0)
    residual = {node: tail for node, (_, tail) in backlog.items() if tail}
    post_fault_progress = {
        node: sum(
            1
            for event in events
            if event["kind"] == "finalized" and event["epoch_ms"] >= fault_end + delta_ms
        )
        for node, events in per_node.items()
    }
    recovery_latencies = [row["completion_to_all_resolver_sealed_ms"] for row in recovery_rows]
    recovery_latency_summary = distribution_summary(recovery_latencies)
    recovery_spreads = [row["seal_spread_ms"] for row in recovery_rows]
    resolver_window_start = min(
        (row["completion_first_ms"] for row in recovery_rows),
        default=fault_start,
    )
    resolver_window_end = max(
        (row["all_resolver_sealed_ms"] for row in recovery_rows),
        default=fault_end,
    )
    dynamics = direct_resolver_dynamics(
        per_node,
        open_maps,
        resolver_window_start,
        resolver_window_end,
    )
    sustained = sustained_attack_dynamics(
        common_open,
        open_maps,
        seal_maps,
        fault_start,
        fault_end,
        fault_containment_guard_ms,
    )

    add_check(
        checks,
        "deterministic stress proposals",
        len(mature_stress) >= minimum_open_views and not bad_tips,
        f"mature={len(mature_stress)}, one-tip violations={compact_list(bad_tips)}",
    )
    add_check(
        checks,
        "residual mixed views observed",
        len(common_open) >= minimum_open_views,
        f"common-open={len(common_open)}; other mature stress views refined or did not "
        f"complete open everywhere={compact_list(missing_open)}",
    )
    add_check(
        checks,
        "bounded direct-holder split",
        bool(common_open) and not split_violations,
        f"completion quorum has grade1<=f={expected_g1} and "
        f"grade0>=n-2f={expected_g0}; violations={compact_list(split_violations)}",
    )
    add_check(
        checks,
        "per-target resolver recovery",
        len(recovery_rows) == len(common_open) and not missing_seals and not route_mismatches,
        f"resolver-sealed={len(recovery_rows)}/{len(common_open)}; completion-to-all ms "
        f"median/p95/max={recovery_latency_summary['median']}/"
        f"{recovery_latency_summary['p95']}/{recovery_latency_summary['maximum']}; "
        f"unsealed={compact_list(missing_seals)}; "
        f"wrong-route={compact_list(route_mismatches)}",
    )
    add_check(
        checks,
        "ordered output passed recovered views",
        not missing_finalized,
        "every recovered view finalized"
        if not missing_finalized
        else f"missing={compact_list(missing_finalized)}",
    )
    add_check(
        checks,
        "multiple unresolved views accumulated",
        peak_min >= 2,
        f"per-correct-node peak range={peak_min}..{peak_max}",
    )
    add_check(
        checks,
        "open backlog drained",
        not residual,
        "all logged open views reached a terminal seal"
        if not residual
        else f"residual on {len(residual)} correct nodes; maximum={max(residual.values())}",
    )
    add_check(
        checks,
        "post-fault progress",
        all(count > 0 for count in post_fault_progress.values()),
        f"minimum post-fault finalized events={min(post_fault_progress.values(), default=0)}",
    )
    route_counts = {}
    for row in recovery_rows:
        for route in row["routes"]:
            route_counts[route] = route_counts.get(route, 0) + 1
    return checks, {
        "fault_start_ms": fault_start,
        "fault_end_ms": fault_end,
        "fault_containment_guard_ms": fault_containment_guard_ms,
        "stress_views": sorted(stress),
        "mature_stress_views": sorted(mature_stress),
        "common_open_views": sorted(common_open),
        "split_violations": split_violations,
        "missing_seals": missing_seals,
        "route_mismatches": route_mismatches,
        "missing_finalized": missing_finalized,
        "backlog_peak_and_final_by_node": backlog,
        "recovery_count": len(recovery_rows),
        "recovery_rows": recovery_rows,
        "completion_to_all_resolver_sealed_ms": recovery_latency_summary,
        "fault_end_to_all_resolver_sealed_ms": (
            max((row["all_resolver_sealed_ms"] for row in recovery_rows), default=fault_end)
            - fault_end
        ),
        "common_open_views_per_fault_second": (
            len(common_open) / (int(parameters["withhold_for_ms"]) / 1_000)
            if int(parameters["withhold_for_ms"]) > 0 else None
        ),
        "resolver_seal_spread_ms": distribution_summary(recovery_spreads),
        "resolver_route_counts": route_counts,
        "direct_resolver_dynamics": dynamics,
        "sustained_attack_dynamics": sustained,
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("clean", "crash", "mixed"), required=True)
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).parent / "data")
    parser.add_argument("--run-log", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--minimum-open-views", type=int, default=3)
    parser.add_argument("--max-final-spread", type=int, default=50)
    args = parser.parse_args(argv)

    manifest = json.loads((args.data_dir / "manifest.json").read_text())
    parameters = json.loads((args.data_dir / "parameters.json").read_text())
    manifest["_data_dir"] = str(args.data_dir)
    n = int(manifest["nodes"])
    publishers = set(manifest.get("withholding_node_indices", []))
    if args.scenario == "mixed":
        correct_nodes = [node for node in range(n) if node not in publishers]
    else:
        correct_nodes = list(range(int(manifest.get("crash", 0)), n))

    all_events = {}
    missing_logs = []
    for node in range(n):
        log = args.data_dir / f"node-{node}" / "logs" / "primary.log"
        if log.is_file():
            all_events[node] = parse_events(log)
        elif node in correct_nodes or node in publishers:
            missing_logs.append(node)
    if missing_logs:
        sys.exit(f"recovery_report.py: missing primary logs for nodes {missing_logs}")
    per_node = {node: all_events[node] for node in correct_nodes}
    result = load_result(args.run_log)

    checks, details = validate_common(
        args.scenario,
        manifest,
        parameters,
        per_node,
        result,
        args.max_final_spread,
    )
    if args.scenario == "clean":
        scenario_checks, scenario_details = validate_clean(manifest, parameters, per_node)
    elif args.scenario == "crash":
        scenario_checks, scenario_details = validate_crash(manifest, parameters, per_node)
    else:
        scenario_checks, scenario_details = validate_mixed(
            manifest,
            parameters,
            per_node,
            all_events,
            args.minimum_open_views,
        )
    checks.extend(scenario_checks)
    details.update(scenario_details)
    passed = all(check["passed"] for check in checks)
    report = {
        "scenario": args.scenario,
        "passed": passed,
        "nodes": n,
        "fault_budget": (n - 1) // 3,
        "correct_nodes": correct_nodes,
        "checks": checks,
        "details": details,
    }
    if args.output:
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    print(f"Vantage recovery report: scenario={args.scenario} n={n} "
          f"f={(n - 1) // 3} status={'PASS' if passed else 'FAIL'}")
    for check in checks:
        print(f"  {'PASS' if check['passed'] else 'FAIL'}  {check['name']}: {check['detail']}")
    if args.output:
        print(f"  report: {args.output}")
    print(
        "VANTAGE_RECOVERY_REPORT "
        + json.dumps(
            {
                "scenario": args.scenario,
                "passed": passed,
                "nodes": n,
                "fault_budget": (n - 1) // 3,
                "failed_checks": [check["name"] for check in checks if not check["passed"]],
            },
            sort_keys=True,
        )
    )
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
