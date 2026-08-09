#!/usr/bin/env python3
"""Generate keys, committee.json, parameters.json, per-node tc netem scripts and
docker-compose.yml for the docker-bench distributed local benchmark.

stdlib only (no PyYAML, no requests) -- docker-compose.yml is emitted as hand-formatted
text, keys/committee/parameters are plain json.dump.

Targets Python 3.7+ (the `from __future__ import annotations` below defers type-hint
evaluation so the PEP 604/585 syntax used throughout -- `str | None`, `list[str]` --
never actually executes on an older interpreter; only the annotations themselves are
newer-looking).

Layout produced under docker-bench/data/ (gitignored, regenerated on every run):
    manifest.json           node count, port layout, IP scheme, run parameters
    committee.json          shared committee (mounted read-only into every container)
    parameters.json         shared Parameters (mounted read-only into every container)
    prometheus.yaml         primary + worker scrape targets for every validator
    node-<i>/key.json        that node's keypair (config::KeyPair JSON)
    node-<i>/tc-setup.sh     that node's own netem egress-delay script

Also writes docker-bench/docker-compose.yml (n services, static IPs on 172.28.0.0/16).

Addressing/port scheme (must stay in sync with entrypoint.sh/blip.sh/results.py --
see docker-bench/README.md):
  - node i (0-based) gets container IP 172.28.1.<10+i> on the `vantage_net` bridge.
  - every container listens on the SAME 8 ports (distinct IPs make this safe):
        6000 consensus_to_consensus   6001 primary_to_primary
        6002 worker_to_primary        6003 primary metrics (Prometheus)
        6004 primary_to_worker (w0)   6005 transactions (w0, client target)
        6006 worker_to_worker (w0)    6007 worker metrics (w0, Prometheus)
  - host-published (for results.py, run purely on the docker host's loopback):
        127.0.0.1:<9000+i>  -> primary metrics (6003)
        127.0.0.1:<9100+i>  -> worker-0 metrics (6007)
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
COMPOSE_PATH = SCRIPT_DIR / "docker-compose.yml"

# ---------------------------------------------------------------------------
# Fixed addressing / port scheme (see module docstring -- entrypoint.sh, blip.sh
# and results.py all re-derive the same numbers from docker-bench/data/manifest.json
# rather than hardcoding a second copy, so there is exactly one source of truth: this
# dict).
# ---------------------------------------------------------------------------
# Defaults match the spec exactly; `--subnet-base` (below) only ever overrides these
# at the request of an explicit flag, e.g. to dodge a same-address-space collision with
# some other, unrelated docker-compose project already using 172.28.0.0/16 on this
# host (a real thing: encountered exactly this while developing docker-bench itself).
SUBNET = "172.28.0.0/16"
NODE_IP_PREFIX = "172.28.1."
NODE_IP_OFFSET = 10  # node i -> <subnet-base>.1.<10+i>
PORTS = {
    "consensus_to_consensus": 6000,
    "primary_to_primary": 6001,
    "worker_to_primary": 6002,
    "primary_metrics": 6003,
    "primary_to_worker": 6004,
    "transactions": 6005,
    "worker_to_worker": 6006,
    "worker_metrics": 6007,
}
HOST_PRIMARY_METRICS_BASE = 9000
HOST_WORKER_METRICS_BASE = 9100
MAX_NODES = 40  # owner constraint, raised from 20 on 2026-08-05 for local A/B
                # sweeps. Note the resource reality: colima has 12 CPUs, so large
                # local committees share far less CPU than the 8-vCPU-per-node AWS
                # runs. Fine for A/B, not for absolute numbers.

# 10-region AWS RTT matrix (milliseconds), ported VERBATIM from
# config/src/lib.rs::RTT_LATENCY_TABLE, itself ported verbatim from
# ~/code/starfish/crates/starfish-core/src/network.rs. `node local-benchmark`'s
# `LatencyTable::aws_rtt` builds the identical table for the in-process harness;
# docker-bench instead bakes it into real `tc netem` delay, so every node's
# parameters.json below pins `mimic_latency_ms: 0` to disable the in-process
# simulation and avoid injecting the delay twice.
# Per-class netem queue depth in PACKETS -- see its use in `render_tc_script` for why
# netem's 1000-packet default is a trap at n>=50.
NETEM_LIMIT_PKTS = 100_000

RTT_LATENCY_TABLE = [
    [1, 14, 104, 112, 198, 65, 68, 110, 201, 146],
    [14, 1, 106, 122, 196, 78, 67, 103, 189, 142],
    [104, 106, 1, 215, 281, 163, 29, 50, 143, 238],
    [112, 122, 215, 1, 309, 175, 176, 220, 299, 254],
    [198, 196, 281, 309, 1, 137, 254, 268, 150, 101],
    [65, 78, 163, 175, 137, 1, 127, 172, 226, 108],
    [68, 67, 29, 176, 254, 127, 1, 38, 125, 199],
    [110, 103, 50, 220, 268, 172, 38, 1, 148, 245],
    [201, 189, 143, 299, 150, 226, 125, 148, 1, 140],
    [146, 142, 238, 254, 101, 108, 199, 245, 140, 1],
]

PROTOCOL_CHOICES = (
    "autobahn-optimistic",
    "autobahn-seamless",
    "vantage",
    "simple-it",
    "simple-it-bracha",
)


def one_way_ms(i: int, j: int) -> float:
    """One-way delay (ms) node i should inject towards node j, region = index % 10,
    diagonal forced to 0 -- identical convention to `LatencyTable::aws_rtt`."""
    if i == j:
        return 0.0
    return RTT_LATENCY_TABLE[i % 10][j % 10] / 2.0


def node_ip(i: int) -> str:
    return f"{NODE_IP_PREFIX}{NODE_IP_OFFSET + i}"


def container_name(i: int) -> str:
    return f"vantage-node-{i}"


# ---------------------------------------------------------------------------
# Native `node` binary (used only to invoke `generate_keys` -- gen.py runs before the
# docker image exists, per run.sh's own gen -> build -> up order, so key generation
# uses a native build of the SAME source tree rather than the containerized one).
# ---------------------------------------------------------------------------
def ensure_node_binary(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit).resolve()
        if not p.is_file():
            sys.exit(f"--node-bin {p} does not exist")
        return p

    env_override = os.environ.get("NODE_BIN")
    if env_override:
        p = Path(env_override).resolve()
        if not p.is_file():
            sys.exit(f"$NODE_BIN {p} does not exist")
        return p

    native = REPO_ROOT / "target" / "release" / "node"
    if native.is_file():
        return native

    print(
        "-- native target/release/node not found; building it now "
        "(cargo build --release --features benchmark --bin node, CARGO_BUILD_JOBS=4)",
        file=sys.stderr,
    )
    env = dict(os.environ)
    env["CARGO_BUILD_JOBS"] = "4"
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--features",
            "benchmark",
            "--bin",
            "node",
        ],
        cwd=REPO_ROOT / "node",
        env=env,
        check=True,
    )
    if not native.is_file():
        sys.exit(f"cargo build finished but {native} is still missing")
    return native


def generate_keys(node_bin: Path, out_dir: Path, n: int) -> list[str]:
    """Runs `node generate_keys` once per node (deliverable 2's CRIB: reuse the real
    binary rather than reimplementing ed25519 keypair generation/serialization in
    Python). Returns each node's base64 public key ("name"), in container-index order."""
    pubkeys = []
    for i in range(n):
        node_dir = out_dir / f"node-{i}"
        node_dir.mkdir(parents=True, exist_ok=True)
        key_path = node_dir / "key.json"
        subprocess.run(
            [str(node_bin), "generate_keys", "--filename", str(key_path)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        pubkeys.append(json.loads(key_path.read_text())["name"])
    return pubkeys


# ---------------------------------------------------------------------------
# committee.json / parameters.json (config::Committee / config::Parameters JSON shape,
# confirmed against a real `node local-benchmark --nodes 4 --duration 1` export --
# see docker-bench/README.md's "caching design" section for the CRIB command).
# ---------------------------------------------------------------------------
def build_committee(pubkeys: list[str]) -> dict:
    authorities = {}
    for i, pubkey in enumerate(pubkeys):
        ip = node_ip(i)
        authorities[pubkey] = {
            "stake": 1,
            "consensus": {"consensus_to_consensus": f"{ip}:{PORTS['consensus_to_consensus']}"},
            "primary": {
                "primary_to_primary": f"{ip}:{PORTS['primary_to_primary']}",
                "worker_to_primary": f"{ip}:{PORTS['worker_to_primary']}",
                "metrics": f"{ip}:{PORTS['primary_metrics']}",
            },
            "workers": {
                "0": {
                    "primary_to_worker": f"{ip}:{PORTS['primary_to_worker']}",
                    "transactions": f"{ip}:{PORTS['transactions']}",
                    "worker_to_worker": f"{ip}:{PORTS['worker_to_worker']}",
                    "metrics": f"{ip}:{PORTS['worker_metrics']}",
                }
            },
        }
    return {"authorities": authorities}


def build_parameters(args: argparse.Namespace) -> dict:
    # `implied_optimistic_tips()` is None for vantage/simple-it/simple-it-bracha (no
    # warning either way); only autobahn-seamless needs this flipped to false to avoid
    # `reconcile_protocol`'s harmless-but-noisy mismatch warning on every node's log.
    use_optimistic_tips = args.protocol != "autobahn-seamless"
    return {
        "timeout_delay": args.timeout_delay_ms,
        "header_size": 1000,
        "max_header_delay": args.max_header_delay_ms,
        "gc_depth": 50,
        "sync_retry_delay": 5000,
        "sync_retry_nodes": 3,
        "batch_size": 500_000,
        "max_batch_delay": args.max_batch_delay_ms,
        "use_optimistic_tips": use_optimistic_tips,
        "use_parallel_proposals": True,
        "k": 4,
        "use_fast_path": True,
        "fast_path_timeout": 500,
        "use_ride_share": False,
        "car_timeout": 2000,
        "all_to_all": args.all_to_all,
        "simulate_asynchrony": False,
        "asynchrony_start": 20_000,
        "asynchrony_duration": 10_000,
        "protocol": args.protocol,
        "tx_mode": args.mode,
        "max_block_payload": 16,
        "delta_ms": args.delta_ms,
        "vantage_gc_window_views": 200,
        "simpleit_gc_window_rounds": 50,
        "ack_watermarks": not args.no_ack_watermarks,
        "ack_watermark_period_ms": args.ack_watermark_period_ms,
        "digest_statements": not args.no_digest_statements,
        # SEQUENCE-CHECKPOINT-SYNC-PLAN.md: default-on state sync. `--no-state-sync`
        # disables both the checkpoint log and install path for control runs.
        "sequence_checkpoints": not args.no_state_sync,
        "sequence_checkpoint_interval_views": args.sequence_checkpoint_interval,
        "sequence_sync_min_gap_views": args.sequence_sync_min_gap_views,
        "sequence_sync_chunk_outcomes": args.sequence_sync_chunk_outcomes,
        "sequence_install_enabled": not args.no_state_sync,
        # Explicit Some(0), not absent/null, REGARDLESS of --no-latency: real one-way
        # delay (when enabled) comes from tc netem, not from this process, so the
        # in-process aws_rtt default (`node run`'s own fallback whenever this key is
        # null/absent) must always be pinned off here, or every link would be delayed
        # twice. See RTT_LATENCY_TABLE's doc comment above.
        "mimic_latency_ms": 0,
        "batch_messages": not args.no_batch_messages,
        "batch_max_bytes": args.batch_max_bytes,
        "batch_max_delay_ms": args.batch_max_delay_ms,
        "withhold_senders": args.withhold,
        "withhold_at_ms": None if args.withhold_at is None else args.withhold_at * 1000,
        "withhold_for_ms": args.withhold_for * 1000,
        "resume_check_period_ms": 1000,
        "resume_backoff_ms": 4000,
        "resume_batch": 64,
        # Measurement ablation (KNOB 1/2): see --no-reconnect-replay/
        # --retry-backoff-max-ms's own help text -- these two flags create three
        # cleanly separable benchmark arms (true-before / cap-only / full).
        "reconnect_replay": not args.no_reconnect_replay,
        "retry_backoff_max_ms": args.retry_backoff_max_ms,
    }


# ---------------------------------------------------------------------------
# Per-node tc netem script (deliverable 4). HTB root qdisc + one netem-delay child
# class per PEER, selected via a u32 filter on destination IP -- latency is modeled
# per AUTHORITY (i.e. per peer container), not per service port, matching
# `Committee::addresses_of`'s own doc comment for the in-process LatencyTable. Traffic
# to any address NOT covered by a filter (this node's own IP -- primary<->worker-0
# co-located traffic -- docker DNS, the compose network gateway, metrics scrapes'
# return path, ...) falls through to the default class, untouched.
# ---------------------------------------------------------------------------
def render_tc_script(i: int, n: int, iface_hint: str, enabled: bool) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by docker-bench/gen.py -- do not edit by hand, rerun gen.py instead.",
        "set -euo pipefail",
        "",
        f'IFACE="${{TC_IFACE:-{iface_hint}}}"',
        '# Fall back to the default-route interface if the hint above is not present '
        '(robustness only; docker bridge networks normally give eth0).',
        'if ! ip link show "$IFACE" >/dev/null 2>&1; then',
        '  IFACE="$(ip -o -4 route show to default | awk \'{print $5; exit}\')"',
        'fi',
        'if [ -z "$IFACE" ]; then',
        '  echo "tc-setup: could not determine a network interface, skipping latency injection" >&2',
        "  exit 0",
        "fi",
        "",
        "# Idempotent: drop whatever this script last installed before reinstalling.",
        'tc qdisc del dev "$IFACE" root >/dev/null 2>&1 || true',
        "",
    ]
    if not enabled:
        lines.append('echo "tc-setup: latency injection disabled (--no-latency), leaving $IFACE unshaped"')
        return "\n".join(lines) + "\n"

    # `quantum 60000` on every class: HTB's own default quantum (rate / r2q) blows
    # past its "advisable" ceiling at rate 10gbit and prints a harmless but noisy
    # "quantum ... is big" kernel warning per class; an explicit quantum sidesteps the
    # auto-computation entirely. The rate ceiling itself is a formality -- these
    # classes exist to select a netem delay per destination, not to actually shape
    # bandwidth, so 10gbit is simply "high enough to never be the bottleneck".
    lines.append('tc qdisc add dev "$IFACE" root handle 1: htb default 999')
    lines.append('tc class add dev "$IFACE" parent 1: classid 1:999 htb rate 10gbit quantum 60000')
    for j in range(n):
        if j == i:
            continue
        mid = NODE_IP_OFFSET + j  # class/qdisc minor id, decimal, just needs to be unique per peer
        delay = one_way_ms(i, j)
        peer_ip = node_ip(j)
        lines.append("")
        lines.append(f"# node {i} -> node {j} ({peer_ip}): {delay:.1f} ms one-way "
                      f"(region {i % 10} -> region {j % 10}, RTT {RTT_LATENCY_TABLE[i % 10][j % 10]} ms)")
        lines.append(f'tc class add dev "$IFACE" parent 1: classid 1:{mid} htb rate 10gbit quantum 60000')
        # `limit` BEFORE `delay` (canonical netem argument order), and EXPLICIT because
        # netem's own default is 1000 PACKETS per qdisc and it tail-drops past it
        # SILENTLY -- no error, no log, and the loss lands inside the emulated WAN where
        # it reads as protocol packet loss. A delay qdisc must hold the whole
        # bandwidth-delay product: in-flight bytes per class = per-peer rate x one-way delay.
        #
        # HOW MUCH HEADROOM THE DEFAULT ACTUALLY HAS, measured rather than guessed (AWS
        # n=50 @ 200k tx/s under netem): 102.66 MB/s per node across 49 peers = 2.10 MB/s
        # per peer, ~4.2 MB/s bidirectional per link. At the worst region pair's 154 ms
        # one-way that is 323,693 B in flight = ~216 packets at 1500 B MTU -- only 22% of
        # netem's 1000-packet default, and with TSO/GSO the kernel queues large skbs so the
        # real count is a handful. The default was therefore NOT being exceeded at that
        # scale. An earlier version of this comment claimed ~1,400 packets and was wrong by
        # roughly an order of magnitude.
        #
        # Set anyway, because the margin is finite and the failure is silent: 1000 packets
        # x 1500 B / 154 ms is ~9.7 MB/s per peer, so the default binds at about 4.6x this
        # rate -- reachable by raising the offered load, and sooner on bursty traffic. It is
        # also LATENCY-TIERED, so the highest-RTT classes hit any cap first and the artifact
        # would concentrate on exactly the regions a real WAN failure does, making a local
        # repro chase a ghost. wan-bench already sets it (prepare.py's `netem limit`);
        # docker-bench did not. 100k packets costs only queue headroom -- netem allocates
        # lazily.
        lines.append(f'tc qdisc add dev "$IFACE" parent 1:{mid} handle {mid}: '
                     f'netem limit {NETEM_LIMIT_PKTS} delay {delay:.1f}ms')
        lines.append(
            f'tc filter add dev "$IFACE" protocol ip parent 1:0 prio 1 u32 '
            f'match ip dst {peer_ip}/32 flowid 1:{mid}'
        )
    lines.append("")
    lines.append('echo "tc-setup: latency injection active on $IFACE ('
                  f'{n - 1} peer class(es))"')
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# docker-compose.yml (hand-formatted; no PyYAML dependency, stdlib only).
# ---------------------------------------------------------------------------
def render_compose(n: int, args: argparse.Namespace) -> str:
    rate_share = -(-args.rate // n)  # ceil(rate / n), matches Parameters::div_ceil below
    out = [
        "# Generated by docker-bench/gen.py -- do not edit by hand, rerun gen.py instead.",
        "name: vantage-bench",
        "services:",
    ]
    for i in range(n):
        ip = node_ip(i)
        out += [
            f"  node-{i}:",
            "    image: vantage-docker-bench:latest",
            f"    container_name: {container_name(i)}",
            f"    hostname: node-{i}",
            "    cap_add:",
            "      - NET_ADMIN",
            "    networks:",
            "      vantage_net:",
            f"        ipv4_address: {ip}",
            "    environment:",
            f"      NODE_INDEX: \"{i}\"",
            f"      N_NODES: \"{n}\"",
            # entrypoint.sh derives every node's address from these two rather than
            # hardcoding them, so `--subnet-base` (gen.py) actually takes effect at
            # runtime too -- not just in the generated committee.json/compose IPs.
            f"      NODE_IP_PREFIX: \"{NODE_IP_PREFIX}\"",
            f"      NODE_IP_OFFSET: \"{NODE_IP_OFFSET}\"",
            f"      TX_RATE_SHARE: \"{rate_share}\"",
            f"      TX_SIZE: \"{args.tx_size}\"",
            f"      TX_MODE: \"{args.mode}\"",
            f"      PROTOCOL: \"{args.protocol}\"",
            # RUST_LOG overrides the -vv default filter (env_logger::from_env in
            # node/src/main.rs), letting a diagnostic run scope debug logging to
            # exact modules without drowning primary.log in whole-crate debug output.
            *([f"      RUST_LOG: \"{args.rust_log}\""] if args.rust_log else []),
            "    volumes:",
            "      - ./data/committee.json:/shared/committee.json:ro",
            "      - ./data/parameters.json:/shared/parameters.json:ro",
            f"      - ./data/node-{i}:/data",
            "    ports:",
            f"      - \"127.0.0.1:{HOST_PRIMARY_METRICS_BASE + i}:{PORTS['primary_metrics']}\"",
            f"      - \"127.0.0.1:{HOST_WORKER_METRICS_BASE + i}:{PORTS['worker_metrics']}\"",
            "    restart: \"no\"",
        ]
    out += [
        "networks:",
        "  vantage_net:",
        "    driver: bridge",
        "    ipam:",
        "      config:",
        f"        - subnet: {SUBNET}",
    ]
    return "\n".join(out) + "\n"


def render_prometheus(n: int) -> str:
    """Prometheus targets reachable from the monitoring container on vantage_net."""
    out = [
        "# Generated by docker-bench/gen.py -- do not edit by hand, rerun gen.py instead.",
        "global:",
        "  scrape_interval: 1s",
        "scrape_configs:",
        "  - job_name: 'vantage-docker-bench'",
        "    static_configs:",
    ]
    for i in range(n):
        name = container_name(i)
        out += [
            f"      - targets: ['{name}:{PORTS['primary_metrics']}']",
            "        labels:",
            f"          node: 'node-{i}-primary'",
            f"      - targets: ['{name}:{PORTS['worker_metrics']}']",
            "        labels:",
            f"          node: 'node-{i}-worker-0'",
        ]
    return "\n".join(out) + "\n"


def write_manifest(n: int, args: argparse.Namespace) -> None:
    manifest = {
        "nodes": n,
        "protocol": args.protocol,
        "rate": args.rate,
        "duration": args.duration,
        "tx_size": args.tx_size,
        "mode": args.mode,
        "latency": args.latency,
        "sequence_checkpoints": not args.no_state_sync,
        "sequence_checkpoint_interval_views": args.sequence_checkpoint_interval,
        "sequence_sync_min_gap_views": args.sequence_sync_min_gap_views,
        "sequence_sync_chunk_outcomes": args.sequence_sync_chunk_outcomes,
        "sequence_install_enabled": not args.no_state_sync,
        "subnet": SUBNET,
        "node_ip_prefix": NODE_IP_PREFIX,
        "node_ip_offset": NODE_IP_OFFSET,
        "ports": PORTS,
        "host_primary_metrics_base": HOST_PRIMARY_METRICS_BASE,
        "host_worker_metrics_base": HOST_WORKER_METRICS_BASE,
        "container_name_prefix": "vantage-node-",
    }
    (DATA_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate keys/committee/parameters/tc-scripts/docker-compose.yml "
        "for the docker-bench distributed local benchmark."
    )
    p.add_argument("--nodes", type=int, default=4, help="number of authorities (default 4)")
    p.add_argument("--rate", type=int, default=200, help="aggregate input rate, tx/s (default 200)")
    p.add_argument("--duration", type=int, default=60, help="benchmark duration, s (default 60; "
                    "informational only here -- run.sh/results.py are what actually enforce it)")
    p.add_argument("--protocol", choices=PROTOCOL_CHOICES, default="vantage")
    p.add_argument("--tx-size", type=int, default=512, help="transaction size, bytes (default 512)")
    # "all-zero" (hyphen) kept as a legacy alias; "all_zero" (snake_case) is the
    # starfish-aligned canonical spelling -- normalized just below, right after
    # parsing, so every downstream use (parameters.json's tx_mode, the compose
    # file's TX_MODE env, manifest.json) sees only the canonical spelling.
    p.add_argument("--mode", choices=["all_zero", "all-zero", "random"], default="random")
    p.add_argument("--no-latency", dest="latency", action="store_false",
                    help="skip tc netem injection entirely (pure docker-LAN speed)")
    p.add_argument("--node-bin", default=None, help="path to a prebuilt `node` binary "
                    "(default: target/release/node, built on demand)")
    p.add_argument("--subnet-base", default="172.28", metavar="A.B",
                    help="first two octets of the /16 docker bridge network (default "
                    "172.28, per spec); override only if that address space collides "
                    "with another docker-compose project already on this host")
    # Parameters passthrough (mirrors `node local-benchmark`'s own flag names).
    p.add_argument("--delta-ms", type=int, default=200)
    p.add_argument("--timeout-delay-ms", type=int, default=1000,
                   help="protocol round timeout in milliseconds (default 1000; "
                        "Simple-IT with Opt-RBC requires 8 * --delta-ms)")
    p.add_argument("--max-batch-delay-ms", type=int, default=20)
    p.add_argument("--max-header-delay-ms", type=int, default=50)
    p.add_argument("--no-batch-messages", action="store_true")
    p.add_argument("--batch-max-bytes", type=int, default=65536)
    p.add_argument("--batch-max-delay-ms", type=int, default=5)
    p.add_argument("--all-to-all", action="store_true")
    p.add_argument("--no-ack-watermarks", action="store_true",
               help="disable periodic availability watermarks (ON by default)")
    p.add_argument("--ack-watermark-period-ms", type=int, default=50)
    p.add_argument("--no-state-sync", action="store_true",
                   help="disable sequence checkpoint state sync and installation")
    p.add_argument("--sequence-checkpoint-interval", type=int, default=100,
                   help="checkpoint boundary interval K in views; must be small "
                        "enough that the run crosses several boundaries on 2+ nodes")
    p.add_argument("--sequence-sync-min-gap-views", type=int, default=50,
                   help="minimum certified cursor gap that starts state sync (default 50)")
    p.add_argument("--sequence-sync-chunk-outcomes", type=int, default=8,
                   help="terminal outcome bodies per state-sync response (default 8)")
    p.add_argument("--no-digest-statements", action="store_true",
               help="disable digest-named AGB statements (ON by default)")
    p.add_argument("--no-reconnect-replay", action="store_true",
                   help="Measurement ablation KNOB 1: disable the server-floored "
                        "volatile one-shot replay mechanism (outbox + Hello/Done "
                        "exchange). ON by default; this flag restores pre-mechanism "
                        "behavior -- one-shot AGB/consensus broadcasts go out durable, "
                        "unrecorded. Vantage only (no effect on autobahn-*/simple-it*). "
                        "Pair with --retry-backoff-max-ms to isolate this mechanism's "
                        "own effect from the reconnect backoff cap that changed "
                        "alongside it.")
    p.add_argument("--retry-backoff-max-ms", type=int, default=2000,
                   help="Measurement ablation KNOB 2: the reconnect-waiter's "
                        "exponential-backoff ceiling, ms. Transport-level -- applies "
                        "uniformly to every protocol's primary-to-primary connections. "
                        "Default 2000 (today's cap); pass 60000 to reproduce the "
                        "pre-cap-change baseline.")
    p.add_argument("--withhold", type=int, default=0)
    p.add_argument("--withhold-at", type=int, default=None)
    p.add_argument("--withhold-for", type=int, default=30)
    p.add_argument("--rust-log", default=None, metavar="FILTER",
                   help="RUST_LOG filter for every container's node processes, e.g. "
                        "'info,primary::vantage::resume=debug' -- overrides the -vv "
                        "default; omit for the normal info-level logs")
    args = p.parse_args(argv)
    # Single normalization site for the legacy hyphen alias -- see --mode's help
    # above. Every use of `args.mode` from here on sees the canonical spelling.
    args.mode = args.mode.replace("-", "_")

    if not (1 <= args.nodes <= MAX_NODES):
        p.error(f"--nodes must be between 1 and {MAX_NODES}")
    if args.tx_size < 17:
        p.error("--tx-size must be at least 17 bytes (1 B marker + 8 B id + 8 B timestamp)")
    if not (0 <= args.withhold <= args.nodes):
        p.error("--withhold must be between 0 and --nodes")
    if args.withhold_at is not None and args.withhold == 0:
        p.error("--withhold-at requires --withhold > 0")
    if args.sequence_checkpoint_interval < 1:
        p.error("--sequence-checkpoint-interval must be at least 1")
    if args.sequence_sync_min_gap_views < 0:
        p.error("--sequence-sync-min-gap-views must be non-negative")
    if args.sequence_sync_chunk_outcomes < 1:
        p.error("--sequence-sync-chunk-outcomes must be at least 1")
    if not re.fullmatch(r"\d{1,3}\.\d{1,3}", args.subnet_base):
        p.error("--subnet-base must look like 'A.B' (e.g. 172.28)")
    return args


def main(argv=None) -> None:
    global SUBNET, NODE_IP_PREFIX
    args = parse_args(argv)
    n = args.nodes
    SUBNET = f"{args.subnet_base}.0.0/16"
    NODE_IP_PREFIX = f"{args.subnet_base}.1."

    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True)

    node_bin = ensure_node_binary(args.node_bin)
    print(f"-- using node binary: {node_bin}")

    print(f"-- generating {n} keypair(s)")
    pubkeys = generate_keys(node_bin, DATA_DIR, n)

    print("-- writing committee.json / parameters.json")
    committee = build_committee(pubkeys)
    (DATA_DIR / "committee.json").write_text(json.dumps(committee, indent=2) + "\n")
    parameters = build_parameters(args)
    (DATA_DIR / "parameters.json").write_text(json.dumps(parameters, indent=2) + "\n")

    print("-- writing per-node tc netem scripts")
    for i in range(n):
        script = render_tc_script(i, n, iface_hint="eth0", enabled=args.latency)
        script_path = DATA_DIR / f"node-{i}" / "tc-setup.sh"
        script_path.write_text(script)
        script_path.chmod(0o755)

    print("-- writing manifest.json")
    write_manifest(n, args)

    print("-- writing prometheus.yaml")
    (DATA_DIR / "prometheus.yaml").write_text(render_prometheus(n))

    print(f"-- writing {COMPOSE_PATH}")
    COMPOSE_PATH.write_text(render_compose(n, args))

    # Sanity-check the subnet actually covers every assigned container IP.
    net = ipaddress.ip_network(SUBNET)
    for i in range(n):
        assert ipaddress.ip_address(node_ip(i)) in net

    print(f"-- done: {n} node(s), protocol={args.protocol}, rate={args.rate} tx/s, "
          f"latency={'on' if args.latency else 'off'}")
    print(f"   data dir: {DATA_DIR}")
    print(f"   compose file: {COMPOSE_PATH}")


if __name__ == "__main__":
    main()
