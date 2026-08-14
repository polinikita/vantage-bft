#!/usr/bin/env python3
"""Generate Docker benchmark keys, configuration, network shaping, and targets."""
from __future__ import annotations

import argparse
import base64
import ipaddress
import json
import math
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

# Fixed addresses and ports. Override the subnet only for host conflicts.
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
MAX_NODES = 40  # Local resource limit.

# AWS RTT values are milliseconds; tc applies half as one-way delay.
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
    """Return the one-way delay in milliseconds from node i to node j."""
    if i == j:
        return 0.0
    return RTT_LATENCY_TABLE[i % 10][j % 10] / 2.0


def node_ip(i: int) -> str:
    return f"{NODE_IP_PREFIX}{NODE_IP_OFFSET + i}"


def container_name(i: int) -> str:
    return f"vantage-node-{i}"


# Native node binary used for key generation.
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
    """Run `node generate_keys` once per node and return public keys in index order."""
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


# Build committee.json and parameters.json data.
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


def build_parameters(args: argparse.Namespace, pubkeys: list[str]) -> dict:
    # Seamless Autobahn disables optimistic tips.
    use_optimistic_tips = args.protocol != "autobahn-seamless"
    fixed_publishers = []
    fixed_receivers = []
    withhold_senders = args.withhold
    if args.withhold_fixed_receivers:
        fixed_publishers = pubkeys[:args.withhold]
        fixed_receivers = pubkeys[
            args.withhold:args.withhold + args.withhold_count
        ]
        withhold_senders = 0
    elif args.withhold and args.withhold_publisher_stride != 1:
        committee_order = sorted(pubkeys, key=base64.b64decode)
        fixed_publishers = [
            committee_order[(offset * args.withhold_publisher_stride) % args.nodes]
            for offset in range(args.withhold)
        ]
        withhold_senders = 0
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
        "all_to_all": args.all_to_all or args.protocol == "autobahn-optimistic",
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
        "echo_avail_claims": (
            not args.no_ack_watermarks and not args.no_echo_avail_claims
        ),
        "digest_statements": not args.no_digest_statements,
        "vantage_compact_ids": not args.no_compact_ids,
        # Disable checkpointing and installation with --no-state-sync.
        "sequence_checkpoints": not args.no_state_sync,
        "sequence_checkpoint_interval_views": args.sequence_checkpoint_interval,
        "sequence_sync_min_gap_views": args.sequence_sync_min_gap_views,
        "sequence_sync_shed_gap_views": args.sequence_sync_shed_gap_views,
        "sequence_sync_chunk_outcomes": args.sequence_sync_chunk_outcomes,
        "sequence_sync_chunk_outcome_items": args.sequence_sync_chunk_outcome_items,
        "sequence_install_enabled": not args.no_state_sync,
        # Apply latency with tc, not in-process.
        "mimic_latency_ms": 0,
        "batch_messages": not args.no_batch_messages,
        "batch_max_bytes": args.batch_max_bytes,
        "batch_max_delay_ms": args.batch_max_delay_ms,
        "withhold_senders": withhold_senders,
        "withhold_publishers": fixed_publishers,
        "withhold_count": args.withhold_count,
        "withhold_stride": args.withhold_stride,
        "withhold_receivers": fixed_receivers,
        "withhold_repair": args.withhold_repair,
        "withhold_headers": not args.withhold_batches_only,
        "withhold_at_ms": None if args.withhold_at is None else args.withhold_at * 1000,
        "withhold_for_ms": args.withhold_for * 1000,
        "resume_check_period_ms": 1000,
        "resume_backoff_ms": 4000,
        "resume_batch": 64,
        # Configure reconnect replay and backoff.
        "reconnect_replay": not args.no_reconnect_replay,
        "retry_backoff_max_ms": args.retry_backoff_max_ms,
    }


# Build one tc netem delay class per peer.
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

    # Set the HTB quantum explicitly.
    lines.append('tc qdisc add dev "$IFACE" root handle 1: htb default 999')
    lines.append('tc class add dev "$IFACE" parent 1: classid 1:999 htb rate 10gbit quantum 60000')
    for j in range(n):
        if j == i:
            continue
        mid = NODE_IP_OFFSET + j  # Unique class/qdisc minor id.
        delay = one_way_ms(i, j)
        peer_ip = node_ip(j)
        lines.append("")
        lines.append(f"# node {i} -> node {j} ({peer_ip}): {delay:.1f} ms one-way "
                      f"(region {i % 10} -> region {j % 10}, RTT {RTT_LATENCY_TABLE[i % 10][j % 10]} ms)")
        lines.append(f'tc class add dev "$IFACE" parent 1: classid 1:{mid} htb rate 10gbit quantum 60000')
        # Set a large queue for high-RTT links.
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


# Render Compose without a YAML library.
def withholding_publisher_indices(
    args: argparse.Namespace,
    pubkeys: list[str],
    parameters: dict,
) -> list[int]:
    """Return node indices selected as Byzantine lane publishers by Rust."""
    selected = parameters["withhold_publishers"]
    if not selected and parameters["withhold_senders"]:
        selected = sorted(pubkeys, key=base64.b64decode)[:parameters["withhold_senders"]]
    index_by_key = {key: index for index, key in enumerate(pubkeys)}
    return [index_by_key[key] for key in selected]


def distribute_rate(total: int, node_indices: list[int]) -> dict[int, int]:
    """Split an aggregate rate exactly, with at most one tx/s of skew."""
    if not node_indices:
        if total:
            raise ValueError("cannot distribute a non-zero rate over no nodes")
        return {}
    quotient, remainder = divmod(total, len(node_indices))
    return {
        index: quotient + (offset < remainder)
        for offset, index in enumerate(node_indices)
    }


def render_compose(
    n: int,
    args: argparse.Namespace,
    load_node_indices: list[int],
    adversarial_node_indices: list[int],
) -> str:
    load_rates = distribute_rate(args.rate, load_node_indices)
    adversarial_rates = distribute_rate(args.adversarial_rate, adversarial_node_indices)
    client_node_indices = ",".join(str(i) for i in range(args.crash, n))
    out = [
        "# Generated by docker-bench/gen.py -- do not edit by hand, rerun gen.py instead.",
        "name: vantage-bench",
        "services:",
    ]
    for i in range(n):
        ip = node_ip(i)
        node_rate = load_rates.get(i, 0)
        adversarial_node_rate = adversarial_rates.get(i, 0)
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
            # entrypoint.sh uses these address values.
            f"      NODE_IP_PREFIX: \"{NODE_IP_PREFIX}\"",
            f"      NODE_IP_OFFSET: \"{NODE_IP_OFFSET}\"",
            f"      CLIENT_NODE_INDICES: \"{client_node_indices}\"",
            f"      TX_RATE_SHARE: \"{node_rate}\"",
            f"      ADVERSARIAL_TX_RATE_SHARE: \"{adversarial_node_rate}\"",
            f"      TX_SIZE: \"{args.tx_size}\"",
            f"      TX_MODE: \"{args.mode}\"",
            f"      PROTOCOL: \"{args.protocol}\"",
            # RUST_LOG overrides the default filter.
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
    """Return Prometheus targets on vantage_net."""
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


def write_manifest(
    n: int,
    args: argparse.Namespace,
    load_node_indices: list[int],
    adversarial_node_indices: list[int],
) -> None:
    manifest = {
        "nodes": n,
        "crash": args.crash,
        "protocol": args.protocol,
        "rate": args.rate,
        "duration": args.duration,
        "tx_size": args.tx_size,
        "mode": args.mode,
        "latency": args.latency,
        "sequence_checkpoints": not args.no_state_sync,
        "sequence_checkpoint_interval_views": args.sequence_checkpoint_interval,
        "sequence_sync_min_gap_views": args.sequence_sync_min_gap_views,
        "sequence_sync_shed_gap_views": args.sequence_sync_shed_gap_views,
        "sequence_sync_chunk_outcomes": args.sequence_sync_chunk_outcomes,
        "sequence_sync_chunk_outcome_items": args.sequence_sync_chunk_outcome_items,
        "sequence_install_enabled": not args.no_state_sync,
        "echo_avail_claims": (
            not args.no_ack_watermarks and not args.no_echo_avail_claims
        ),
        "vantage_compact_ids": not args.no_compact_ids,
        "withhold_senders": args.withhold,
        "withhold_publisher_stride": args.withhold_publisher_stride,
        "withhold_count": args.withhold_count,
        "withhold_stride": args.withhold_stride,
        "withhold_fixed_receivers": args.withhold_fixed_receivers,
        "withhold_batches_only": args.withhold_batches_only,
        "withhold_repair": args.withhold_repair,
        "correct_load_only": args.correct_load_only,
        "load_node_indices": load_node_indices,
        "adversarial_rate": args.adversarial_rate,
        "adversarial_node_indices": adversarial_node_indices,
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
    p.add_argument("--crash", type=int, default=0,
                   help="first N validators remain absent; load is spread over live nodes")
    p.add_argument("--rate", type=int, default=200, help="aggregate input rate, tx/s (default 200)")
    p.add_argument("--duration", type=int, default=60, help="benchmark duration, s (default 60; "
                    "informational only here -- run.sh/results.py are what actually enforce it)")
    p.add_argument("--protocol", choices=PROTOCOL_CHOICES, default="vantage")
    p.add_argument("--tx-size", type=int, default=512, help="transaction size, bytes (default 512)")
    p.add_argument("--mode", choices=["all_zero", "all-zero", "random"], default="random")
    p.add_argument("--no-latency", dest="latency", action="store_false",
                    help="skip tc netem injection entirely (pure docker-LAN speed)")
    p.add_argument("--node-bin", default=None, help="path to a prebuilt `node` binary "
                    "(default: target/release/node, built on demand)")
    p.add_argument("--subnet-base", default="172.28", metavar="A.B",
                    help="first two octets of the /16 docker bridge network (default "
                    "172.28, per spec); override only if that address space collides "
                    "with another docker-compose project already on this host")
    # Match local-benchmark parameter names.
    p.add_argument("--delta-ms", type=int, default=200)
    p.add_argument("--timeout-delay-ms", type=int, default=1000,
                   help="protocol round timeout in milliseconds (default 1000; "
                        "Simple-IT with Opt-RBC requires 8 * --delta-ms)")
    p.add_argument("--max-batch-delay-ms", type=int, default=20)
    p.add_argument("--max-header-delay-ms", type=int, default=100)
    p.add_argument("--no-batch-messages", action="store_true")
    p.add_argument("--batch-max-bytes", type=int, default=65536)
    p.add_argument("--batch-max-delay-ms", type=int, default=5)
    p.add_argument(
        "--all-to-all",
        action="store_true",
        help="enable all-to-all exchange (already implied by autobahn-optimistic)",
    )
    p.add_argument("--no-ack-watermarks", action="store_true",
               help="disable compressed availability claims and use per-block ACKs")
    p.add_argument("--ack-watermark-period-ms", type=int, default=50)
    p.add_argument("--no-echo-avail-claims", action="store_true",
                   help="use periodic VantageAvail watermarks instead of echo claims")
    p.add_argument("--no-state-sync", action="store_true",
                   help="disable sequence checkpoint state sync and installation")
    p.add_argument("--sequence-checkpoint-interval", type=int, default=20,
                   help="checkpoint boundary interval K in views; must be small "
                        "enough that the run crosses several boundaries on 2+ nodes")
    p.add_argument("--sequence-sync-shed-gap-views", type=int, default=300,
                   help="gap above which ordinary consensus traffic is shed; must be "
                        ">= --sequence-sync-min-gap-views (default 300)")
    p.add_argument("--sequence-sync-min-gap-views", type=int, default=100,
                   help="minimum certified cursor gap that starts state sync (default 100)")
    p.add_argument("--sequence-sync-chunk-outcomes", type=int, default=256,
                   help="maximum outcome views per state-sync response (default 256)")
    p.add_argument("--sequence-sync-chunk-outcome-items", type=int, default=1600,
                   help="maximum manifest references per outcome response (default 1600)")
    p.add_argument("--no-digest-statements", action="store_true",
               help="disable digest-named AGB statements (ON by default)")
    p.add_argument("--no-compact-ids", action="store_true",
                   help="use full Vantage public keys on the primary wire")
    p.add_argument("--no-reconnect-replay", action="store_true",
                   help="disable volatile message replay after reconnect (Vantage only)")
    p.add_argument("--retry-backoff-max-ms", type=int, default=2000,
                   help="maximum reconnect backoff in milliseconds (default 2000)")
    p.add_argument("--withhold", type=int, default=0)
    p.add_argument("--withhold-publisher-stride", type=int, default=1,
                   help="coprime committee-index stride selecting Byzantine publishers")
    p.add_argument("--withhold-count", type=int, default=None,
                   help="peers each withholding node excludes (default: half the committee)")
    p.add_argument("--withhold-stride", type=int, default=1,
                   help="coprime committee-index stride for staggered omissions")
    p.add_argument("--withhold-fixed-receivers", action="store_true",
                   help="bind every withholding publisher to the same disjoint receiver set")
    p.add_argument("--withhold-batches-only", action="store_true",
                   help="drop heavy worker batches but continue original lane headers")
    p.add_argument("--withhold-repair", action="store_true",
                   help="make selected Byzantine publishers ignore all lane repair requests")
    p.add_argument("--correct-load-only", action="store_true",
                   help="distribute counted client load only across non-withholding authors")
    p.add_argument("--adversarial-rate", type=int, default=0,
                   help="aggregate uncounted payload rate placed on withholding publishers; "
                        "the bytes use the full data path but do not count as goodput")
    p.add_argument("--withhold-at", type=int, default=None)
    p.add_argument("--withhold-for", type=int, default=30)
    p.add_argument("--rust-log", default=None, metavar="FILTER",
                   help="RUST_LOG filter for every container's node processes, e.g. "
                        "'info,primary::vantage::resume=debug' -- overrides the -vv "
                        "default; omit for the normal info-level logs")
    args = p.parse_args(argv)
    # Store mode with underscores.
    args.mode = args.mode.replace("-", "_")

    if not (1 <= args.nodes <= MAX_NODES):
        p.error(f"--nodes must be between 1 and {MAX_NODES}")
    if args.tx_size < 17:
        p.error("--tx-size must be at least 17 bytes (1 B marker + 8 B id + 8 B timestamp)")
    if not (0 <= args.withhold <= args.nodes):
        p.error("--withhold must be between 0 and --nodes")
    fault_budget = (args.nodes - 1) // 3
    if not (0 <= args.crash <= fault_budget):
        p.error(f"--crash must be between 0 and {fault_budget} for n={args.nodes}")
    if args.withhold_publisher_stride < 1:
        p.error("--withhold-publisher-stride must be positive")
    if (args.withhold > 0 and
            math.gcd(args.withhold_publisher_stride, args.nodes) != 1):
        p.error("--withhold-publisher-stride must be coprime with --nodes")
    if args.withhold_count is not None and not (0 <= args.withhold_count < args.nodes):
        p.error("--withhold-count must be between 0 and --nodes - 1")
    if args.withhold_count is not None and args.withhold == 0:
        p.error("--withhold-count requires --withhold > 0")
    if args.withhold_stride < 1:
        p.error("--withhold-stride must be positive")
    if (args.withhold > 0 and not args.withhold_fixed_receivers and
            math.gcd(args.withhold_stride, args.nodes) != 1):
        p.error("--withhold-stride must be coprime with --nodes")
    if args.withhold_fixed_receivers and args.withhold_count is None:
        p.error("--withhold-fixed-receivers requires --withhold-count")
    if (args.withhold_fixed_receivers and
            args.withhold + args.withhold_count > args.nodes):
        p.error("fixed withholding publishers and receivers must be disjoint and fit --nodes")
    if args.withhold_fixed_receivers and args.withhold_stride != 1:
        p.error("--withhold-stride is only meaningful for staggered receivers")
    if args.withhold_fixed_receivers and args.withhold_publisher_stride != 1:
        p.error("--withhold-publisher-stride cannot be combined with fixed receivers")
    if args.withhold_batches_only and args.withhold == 0:
        p.error("--withhold-batches-only requires --withhold > 0")
    if args.withhold_repair and args.withhold == 0:
        p.error("--withhold-repair requires --withhold > 0")
    if args.correct_load_only and args.withhold == 0:
        p.error("--correct-load-only requires --withhold > 0")
    if args.correct_load_only and args.withhold == args.nodes:
        p.error("--correct-load-only requires at least one non-withholding node")
    if args.adversarial_rate < 0:
        p.error("--adversarial-rate must be non-negative")
    if args.adversarial_rate > 0 and args.withhold == 0:
        p.error("--adversarial-rate requires --withhold > 0")
    if args.withhold_at is not None and args.withhold == 0:
        p.error("--withhold-at requires --withhold > 0")
    if args.sequence_checkpoint_interval < 1:
        p.error("--sequence-checkpoint-interval must be at least 1")
    if args.sequence_sync_min_gap_views < 0:
        p.error("--sequence-sync-min-gap-views must be non-negative")
    if args.sequence_sync_shed_gap_views < args.sequence_sync_min_gap_views:
        # The shed gap must not be below the sync threshold.
        p.error("--sequence-sync-shed-gap-views must be >= --sequence-sync-min-gap-views")
    if args.sequence_sync_chunk_outcomes < 1:
        p.error("--sequence-sync-chunk-outcomes must be at least 1")
    if args.sequence_sync_chunk_outcome_items < 1:
        p.error("--sequence-sync-chunk-outcome-items must be at least 1")
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
    parameters = build_parameters(args, pubkeys)
    (DATA_DIR / "parameters.json").write_text(json.dumps(parameters, indent=2) + "\n")
    withholding_indices = withholding_publisher_indices(args, pubkeys, parameters)
    withholding_set = set(withholding_indices)
    live_node_indices = list(range(args.crash, n))
    load_node_indices = (
        [index for index in live_node_indices if index not in withholding_set]
        if args.correct_load_only
        else live_node_indices
    )
    adversarial_node_indices = (
        [index for index in withholding_indices if index in live_node_indices]
        if args.adversarial_rate
        else []
    )

    print("-- writing per-node tc netem scripts")
    for i in range(n):
        script = render_tc_script(i, n, iface_hint="eth0", enabled=args.latency)
        script_path = DATA_DIR / f"node-{i}" / "tc-setup.sh"
        script_path.write_text(script)
        script_path.chmod(0o755)

    print("-- writing manifest.json")
    write_manifest(n, args, load_node_indices, adversarial_node_indices)

    print("-- writing prometheus.yaml")
    (DATA_DIR / "prometheus.yaml").write_text(render_prometheus(n))

    print(f"-- writing {COMPOSE_PATH}")
    COMPOSE_PATH.write_text(
        render_compose(n, args, load_node_indices, adversarial_node_indices)
    )

    # Verify that the subnet covers every assigned container IP.
    net = ipaddress.ip_network(SUBNET)
    for i in range(n):
        assert ipaddress.ip_address(node_ip(i)) in net

    print(f"-- done: {n} node(s), protocol={args.protocol}, rate={args.rate} tx/s, "
          f"latency={'on' if args.latency else 'off'}")
    if args.correct_load_only:
        print(f"   counted client load placed on correct node(s): {load_node_indices}")
    elif args.crash:
        print(f"   aggregate client load placed on live node(s): {load_node_indices}")
    if args.adversarial_rate:
        print(
            "   uncounted adversarial payload placed on withholding node(s): "
            f"{adversarial_node_indices} ({args.adversarial_rate} tx/s aggregate)"
        )
    print(f"   data dir: {DATA_DIR}")
    print(f"   compose file: {COMPOSE_PATH}")


if __name__ == "__main__":
    main()
