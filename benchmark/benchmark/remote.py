# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os import makedirs
from os.path import basename, splitext, abspath, dirname, join, isfile
from json import dump, load
from urllib.error import URLError
from time import sleep
import time
from math import ceil
from statistics import mean
from copy import deepcopy
import subprocess
import secrets

from benchmark.config import (
    Committee, Key, NodeParameters, BenchParameters, ConfigError,
    generate_collector_scrape_config,
)
from benchmark.utils import (
    BenchError, Print, PathMaker, progress_bar, scrape_metrics, prometheus_query,
)
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


# METRICS-COLLECTOR-PREP: PromQL for each series the analysis needs off the
# metrics-collector's Prometheus (see `Bench.fetch_collector_metrics`). Names
# verified against metrics/src/metrics.rs's `Metrics::new` registrations, not
# guessed -- see that module for the authoritative list.
COLLECTOR_QUERIES = {
    'committed_transactions_total': 'sum(committed_transactions)',
    'committed_transactions_rate': 'sum(rate(committed_transactions[30s]))',
    # transaction_committed_latency is exposed as a gauge vector labeled by
    # `v` (p25/p50/p75/p90/p99/max/sum/count), not a native Prometheus
    # histogram -- see HistogramReporter::report in metrics/src/metrics.rs.
    'transaction_committed_latency': 'transaction_committed_latency',
    'vantage_seals_by_route': 'sum by (route) (vantage_seals)',
    'network_messages_sent_by_type': 'sum by (type) (network_messages_sent_total)',
    'network_messages_received_by_type': 'sum by (type) (network_messages_received_total)',
    'network_bytes_sent_by_type': 'sum by (type) (network_bytes_sent_total)',
    'network_bytes_received_by_type': 'sum by (type) (network_bytes_received_total)',
    'bytes_sent_total': 'sum(bytes_sent_total)',
    'bytes_received_total': 'sum(bytes_received_total)',
    'submitted_transactions': 'sum(submitted_transactions)',
    'utilization_timer_by_proc': 'sum by (proc) (utilization_timer)',
    'core_queue_length': 'core_queue_length',
    'protocol_info': 'protocol_info',
    'transaction_mode_info': 'transaction_mode_info',
    # PER-RATE-POINT NIC-SATURATION CHECK: 'up' is scrape-health (which
    # targets Prometheus could actually reach during the point -- a target
    # missing from 'up' explains a series being silently absent from
    # everything else here far better than a stale/empty result does).
    # 'bytes_sent_rate_by_node'/'bytes_received_rate_by_node' are the
    # per-node breakdown of the wire byte counters (metrics/src/metrics.rs's
    # bytes_sent_total/bytes_received_total, already counted at the
    # network layer -- see network/src/reliable_sender.rs and
    # simple_sender.rs) that the module docstring's bytes_sent_total/
    # bytes_received_total entries above only sum CLUSTER-wide; free via the
    # scrape config's `node` label (config.generate_collector_scrape_config).
    # 'committed_tps_by_node' is the same per-node breakdown for throughput,
    # for correlating a NIC-saturated node against its own committed rate.
    # 'bytes_sent_rate_by_host'/'bytes_received_rate_by_host' aggregate the
    # SAME counters by the scrape config's `host` label instead (one series
    # per physical instance/NIC, not per primary/worker process) -- with the
    # campaign's `collocate: True`, an authority's primary and its worker
    # share one instance and one NIC, so `by (node)` yields up to 2x as many
    # series as there are hosts and a max over it understates/mislabels the
    # actual per-NIC rate. `_report_nic_peak` reads the by-host series for
    # exactly this reason; the by-node series stay for per-process detail.
    'up': 'up',
    'bytes_sent_rate_by_node': 'sum by (node) (rate(bytes_sent_total[30s]))',
    'bytes_received_rate_by_node': 'sum by (node) (rate(bytes_received_total[30s]))',
    'bytes_sent_rate_by_host': 'sum by (host) (rate(bytes_sent_total[30s]))',
    'bytes_received_rate_by_host': 'sum by (host) (rate(bytes_received_total[30s]))',
    'committed_tps_by_node': 'sum by (node) (rate(committed_transactions[30s]))',
}


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    # --- Working-tree deploy (Phase 7 remote-harness repair) ---------------
    # `git clone`/`git pull` (the original deploy mechanism) can only fetch
    # code that's committed somewhere reachable by the hosts. The tree under
    # test is routinely uncommitted (only the user commits, per their
    # standing workflow), so `install`/`_update` instead rsync the local
    # working tree straight to each host. This is intentionally scoped to
    # these two methods and `_sync_tree`/`_repo_root`/`_ssh_opts` below --
    # nothing else in the harness changes. The audited, citable paper
    # campaign should still run from a tagged, committed revision (git
    # clone), for provenance; this variant is for the smoke test and other
    # pre-campaign runs against a dirty tree. See PHASE7-PREP-NOTES.md
    # #remote.
    RSYNC_EXCLUDES = [
        '.git/',
        'target/',
        'benchmark/logs/',
        'benchmark/results/',
        'benchmark/data/',
        '__pycache__/',
        '*.pyc',
        '.venv/',
        'venv/',
        'fabenv/',
        '*.pem',
    ]

    def __init__(self, ctx, source_build=False):
        # Default (source_build=False): fetch the pre-built nightly binaries
        # (docker.yml's release) instead of compiling remotely -- see
        # `install`/`_update`. `--source-build` (wired through fabfile.py's
        # `install`/`remote`/`campaign` tasks) restores the old rsync +
        # `cargo build` deploy path, kept for debugging a change that isn't
        # in a released binary yet.
        self.source_build = bool(source_build)
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def _repo_root(self):
        ''' Absolute path to the local repo root: this file lives at
        benchmark/benchmark/remote.py, so the root is two directories up. '''
        return abspath(join(dirname(__file__), '..', '..'))

    def _ssh_opts(self):
        # Fresh instances have unknown host keys; Fabric's own Connection
        # already auto-adds them (fabric.Connection.open sets
        # AutoAddPolicy unconditionally), but the plain `ssh`/`rsync`
        # subprocess calls below don't go through Fabric, so they need the
        # same behaviour spelled out explicitly. accept-new (rather than
        # disabling checking outright) still guards against a host key
        # that *changes* after first contact.
        return [
            '-i', self.settings.key_path,
            '-o', 'StrictHostKeyChecking=accept-new',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'LogLevel=ERROR',
        ]

    def _sync_tree(self, ips):
        ''' Working-tree deploy variant: rsync the local repo to each host
        instead of `git clone`/`git pull` (see the class docstring above).
        Incremental -- rsync only ships deltas on repeat runs -- and
        excludes build artifacts, VCS metadata, prior run output, and
        venv/scratch directories (RSYNC_EXCLUDES) so the transferred tree
        stays small; hosts still compile from source as usual. '''
        assert isinstance(ips, list)
        root = self._repo_root()
        exclude_args = []
        for pattern in self.RSYNC_EXCLUDES:
            exclude_args += ['--exclude', pattern]
        # Also honor the repo's own (per-directory) .gitignore files -- this is
        # what actually excludes the volatile `.local-bench*/`/`.db_test*`/etc.
        # local-run scratch directories (config/data churned by concurrent
        # local benchmarking) that RSYNC_EXCLUDES above doesn't enumerate.
        # Without it, rsync's file-list scan can race a concurrent write/delete
        # under one of those directories and abort with a transfer error.
        exclude_args += ['--filter=:- .gitignore']
        ssh_cmd = 'ssh ' + ' '.join(self._ssh_opts())

        Print.info(f'Syncing working tree ({root}) to {len(ips)} machine(s)...')
        for ip in progress_bar(ips, prefix='Syncing working tree:'):
            # Ensure the destination directory exists before rsyncing into it.
            mkdir = subprocess.run(
                [
                    'ssh', *self._ssh_opts(),
                    f'{self.settings.username}@{ip}',
                    f'mkdir -p {self.settings.repo_name}',
                ],
                capture_output=True, text=True,
            )
            if mkdir.returncode != 0:
                raise ExecutionError(
                    f'Failed to prepare {ip} for rsync: {mkdir.stderr.strip()}'
                )

            cmd = [
                'rsync', '-az', '--delete',
                *exclude_args,
                '-e', ssh_cmd,
                f'{root}/',
                f'{self.settings.username}@{ip}:{self.settings.repo_name}/',
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise ExecutionError(
                    f'rsync to {ip} failed (exit {result.returncode}): '
                    f'{result.stderr.strip()}'
                )

    def _nvme_mount_snippet(self):
        ''' NVMe-INSTANCE-STORE: format + mount the local instance-store NVMe
        disk at `PathMaker.REMOTE_STORE_BASE` ('/mnt/db') so the RocksDB store
        (see `_run_single`'s `PathMaker.remote_db_path`) lands on fast local
        disk instead of the EBS root volume -- root cause of the AWS
        throughput collapse (16k tx/s vs 200k locally) on `c5.xlarge`: EBS-only,
        so store I/O both is slow AND competes with the consensus NIC for the
        same underlying bandwidth (EBS is network-attached). `c5d.xlarge` (same
        4 vCPU/NIC) adds exactly one local NVMe SSD -- settings.json's validator
        `instance_type` is `c5d.xlarge`, so this snippet's NVMe-detect loop
        actually finds that disk and the `mkfs.ext4`/mount branch below is the
        one that runs on every validator, not a dormant fallback.

        Validators only -- `install()`'s `hosts` is `manager.hosts(flat=True)`,
        which never includes the metrics-collector (see instance.py); the
        collector stays on plain `c5.xlarge`/EBS and runs no node.

        Idempotent and safe to re-run: guarded by `mountpoint -q /mnt/db` so
        an already-mounted disk is never reformatted. The whole detect/format/
        mount sequence is wrapped in a subshell ending in `true` (its exit
        status is therefore always 0, but note there is no top-level `|| true`
        after the closing paren -- see below) so a detection or format/mount
        failure never aborts the `&&`-chained command list `install()` splices
        this into. This snippet's own job is now best-effort (a failed mount
        just leaves nothing at /mnt/db); provisioning /mnt/db itself as a
        directory the ssh user can write to is a separate, REQUIRED step
        `install()` runs immediately after this snippet (see its own comment
        there) -- so a mount failure is no longer silently papered over, it
        surfaces as that later step's `test -w` failing loudly.

        On the `|| true` placement: a bare trailing `|| true` on this
        snippet's own returned string, spliced into `' && '.join(cmd)`, would
        do far more than make ITS OWN failure non-fatal. POSIX AND-OR lists
        are left-associative with no precedence between `&&` and `||`, so
        `A && B && (snippet) || true && D` parses as
        `((((A && B) && (snippet)) || true) && D)` -- the `|| true` also
        rescues A and B (e.g. a failing `sudo apt-get update`/`install`)
        rather than just this snippet, and `install()` would then report
        success on a testbed that never got its dependencies. Verified
        empirically: `/bin/sh -c 'false && (echo B) || true && echo C'`
        prints `C` and exits 0 even though the leading `false` failed. Ending
        the subshell with `true` INSIDE the parens instead gives the
        subshell its own always-0 exit status without any top-level `||`, so
        a preceding command's failure still fails the whole chain.

        Detection: the instance-store NVMe disk is whichever disk is NEITHER
        the root device NOR already carrying a filesystem/mountpoint of its
        own (its own OR any of its partitions' -- see the per-device check
        below). The root device is resolved via `findmnt`/`lsblk -no PKNAME`
        (the ACTUAL block device backing `/`, whatever its partition layout)
        rather than assumed to be a fixed name like `nvme0n1`: on a
        partitioned root (GPT/UEFI, e.g. a `nvme0n1p1` root partition on
        `nvme0n1`), `lsblk -d`'s disk-level MOUNTPOINT column for `nvme0n1`
        itself is EMPTY (only the partition is mounted) -- the per-device
        check below, which inspects every row `lsblk` reports for a disk (not
        just the disk's own row), catches this case on its own by seeing the
        partition's mountpoint, but the root device is still excluded
        explicitly here too, as a safety net that does not depend on that
        reasoning holding for every AMI/partitioning scheme.

        Per-device MOUNTPOINT/FSTYPE check, not a single 3-column `awk` pass:
        `lsblk -o NAME,MOUNTPOINT,TYPE` with an empty MOUNTPOINT collapses
        under awk's default (whitespace-run) field splitting -- an empty
        middle column isn't an empty field, it's simply not there, so the row
        has 2 tokens, not 3, and a `$3=="disk"` test never matches it (verified
        empirically against real `awk`). Candidate disks are therefore probed
        with separate single-column `lsblk` calls instead, each immune to that
        collapse since there is only one column to (not) split -- and each run
        WITHOUT `head -1`: `lsblk -n -o MOUNTPOINT "$d"`/`lsblk -n -o FSTYPE
        "$d"` list one row per partition of `$d`, not just `$d` itself, and
        `head -1` reads only the whole-disk row, whose MOUNTPOINT/FSTYPE are
        empty on any partitioned disk regardless of whether its partitions are
        in use -- so a non-root disk with mounted partitions (or one that
        merely carries a filesystem, no partitions needed) would pass that
        test as "free" and get `mkfs.ext4 -F`'d over live data. Rejecting a
        candidate if `grep -q .` matches ANY row of either column closes that
        hole: a disk is only a candidate if NOTHING on it -- itself or any
        partition -- is mounted or formatted.

        If found: mkfs.ext4 + mount at /mnt/db + chown to the ssh user. If not
        found (e.g. `fab create` was pointed at a non-`d` instance type): do
        nothing here -- `install()`'s own required step right after this
        snippet still provisions /mnt/db as a plain EBS-root directory, so the
        store path resolves either way.

        KNOWN LIMITATION of the never-touch-a-formatted-disk rule above: an
        instance store that THIS snippet already formatted on an earlier
        `install()`, and which is no longer mounted (only reachable by
        rebooting a validator -- the mount is not in /etc/fstab, and a
        stop/start hands back a blank device instead), now carries ext4 and is
        therefore rejected as a candidate. A re-`install()` in that state
        silently falls back to the plain-EBS-root directory rather than
        re-mounting (or re-formatting) the NVMe disk: store I/O quietly
        returns to EBS speed. That is the deliberate trade: re-`mkfs.ext4 -F`
        of any disk that already holds a filesystem is exactly the data-loss
        hole this rule closes, and it is not worth reopening for a state that
        also throws away the store's contents anyway. Recreate the testbed
        (`fab destroy` + `fab create` + `fab install`) rather than rebooting a
        validator if you need the NVMe store back. '''
        base = PathMaker.REMOTE_STORE_BASE
        user = self.settings.username
        return (
            '('
            'ROOT_SRC=$(findmnt -n -o SOURCE / 2>/dev/null); '
            'ROOT_PK=$(lsblk -no PKNAME "$ROOT_SRC" 2>/dev/null); '
            'if [ -n "$ROOT_PK" ]; then ROOT_DISK="/dev/$ROOT_PK"; '
            'else ROOT_DISK="$ROOT_SRC"; fi; '
            'DEV=""; '
            'for d in $(lsblk -dn -p -o NAME,TYPE | '
            'awk \'$2=="disk"{print $1}\'); do '
            'if [ "$d" = "$ROOT_DISK" ]; then continue; fi; '
            'if lsblk -n -o MOUNTPOINT "$d" 2>/dev/null | grep -q . ; then continue; fi; '
            'if lsblk -n -o FSTYPE "$d" 2>/dev/null | grep -q . ; then continue; fi; '
            'DEV="$d"; break; '
            'done; '
            f'if ! mountpoint -q {base}; then '
            'if [ -n "$DEV" ]; then '
            f'sudo mkfs.ext4 -F -q "$DEV" && sudo mkdir -p {base} && '
            f'sudo mount "$DEV" {base} && '
            f'sudo chown -R {user}:{user} {base}; '
            'fi; '
            'fi; '
            'true'
            ')'
        )

    def install(self):
        ''' Prepare all machines to run the node/benchmark_client binaries.

        Default (self.source_build == False): fetch-binary mode -- minimal
        runtime dependencies only (no Rust toolchain, no source tree; the
        binaries themselves are downloaded per-run by `_update`, see below).
        `--source-build`: the original behavior -- full build toolchain, then
        rsync the working tree so `_update` can compile remotely. '''
        hosts = self.manager.hosts(flat=True)
        print(hosts)

        if self.source_build:
            Print.info('Installing rust and syncing the working tree...')
            base = PathMaker.REMOTE_STORE_BASE
            user = self.settings.username
            cmd = [
                'sudo apt-get update',
                'sudo apt-get -y upgrade',
                'sudo apt-get -y autoremove',

                # The following dependencies prevent the error: [error: linker `cc` not found].
                'sudo apt-get -y install build-essential',
                'sudo apt-get -y install cmake',

                # Install rust (non-interactive).
                'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
                'source $HOME/.cargo/env',
                'rustup default stable',

                # This is missing from the Rocksdb installer (needed for Rocksdb).
                'sudo apt-get install -y clang',
                # e2fsprogs: provides mkfs.ext4 for the NVMe instance-store
                # format step below -- same reason the fetch-binary branch
                # below installs it; `_run_single` passes `--store` under
                # `/mnt/db` regardless of which install mode provisioned the
                # host, so this branch needs it too.
                'sudo apt-get -y install e2fsprogs',
                # NVMe-INSTANCE-STORE: format + mount the local instance-store
                # NVMe disk (if any) at PathMaker.REMOTE_STORE_BASE ('/mnt/db')
                # -- see `_nvme_mount_snippet`'s docstring for the full
                # rationale and the `|| true`-placement pitfall it avoids.
                self._nvme_mount_snippet(),
                # STORE-BASE-REQUIRED: `_run_single` passes `--store
                # PathMaker.remote_db_path(...)` = `/mnt/db/.db-*`
                # unconditionally, and RocksDB (`store/src/lib.rs`,
                # `create_if_missing(true)`) creates only that leaf
                # directory, never `/mnt/db` itself -- and `/mnt` is
                # root-owned. Run deliberately AFTER the NVMe mount attempt
                # above (not folded into its best-effort subshell): it chowns
                # whatever now sits at `{base}` -- the freshly mounted
                # filesystem's root when the NVMe branch above succeeded, a
                # plain directory on the EBS root otherwise -- to the ssh
                # user, and `test -w` makes a failed/partial mount abort
                # install() loudly here instead of surfacing only later as
                # every primary/worker silently dying at boot.
                f'sudo mkdir -p {base} && sudo chown -R {user}:{user} {base} && test -w {base}',
            ]
            try:
                g = Group(*hosts, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)
                self._sync_tree(hosts)
                Print.heading(f'Initialized testbed of {len(hosts)} nodes (source-build mode)')
            except (GroupException, ExecutionError) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                raise BenchError('Failed to install repo on testbed', e)
        else:
            Print.info('Installing runtime dependencies (fetch-binary mode)...')
            repo_name = self.settings.repo_name
            base = PathMaker.REMOTE_STORE_BASE
            user = self.settings.username
            cmd = [
                'sudo apt-get update',
                # curl: downloads the release binaries in `_update`. ca-certificates:
                # verifies the https://github.com download. tmux: `_background_run`
                # launches every primary/worker/client inside a tmux session.
                # e2fsprogs: provides mkfs.ext4 for the NVMe instance-store format
                # step below (normally preinstalled on Ubuntu's base system, but
                # made explicit rather than assumed).
                'sudo apt-get -y install curl ca-certificates tmux e2fsprogs',
                # NVMe-INSTANCE-STORE: format + mount the local instance-store
                # NVMe disk (if any) at PathMaker.REMOTE_STORE_BASE ('/mnt/db')
                # so the RocksDB store lands on fast local disk instead of the
                # EBS root volume -- see `_nvme_mount_snippet`'s docstring for
                # the full rationale (root cause of the AWS throughput
                # collapse), the root-device-exclusion safety argument, and
                # the `|| true`-placement pitfall it avoids. Best-effort: a
                # failed detection/format/mount here leaves nothing at
                # `{base}`, caught loudly by the required step right below.
                self._nvme_mount_snippet(),
                # STORE-BASE-REQUIRED: `_run_single` passes `--store
                # PathMaker.remote_db_path(...)` = `/mnt/db/.db-*`
                # unconditionally, and RocksDB (`store/src/lib.rs`,
                # `create_if_missing(true)`) creates only that leaf
                # directory, never `/mnt/db` itself -- and `/mnt` is
                # root-owned. Run deliberately AFTER the NVMe mount attempt
                # above (not folded into its best-effort subshell): it chowns
                # whatever now sits at `{base}` -- the freshly mounted
                # filesystem's root when the NVMe branch above succeeded, a
                # plain directory on the EBS root otherwise -- to the ssh
                # user, and `test -w` makes a failed/partial mount abort
                # install() loudly here instead of surfacing only later as
                # every primary/worker silently dying at boot.
                f'sudo mkdir -p {base} && sudo chown -R {user}:{user} {base} && test -w {base}',
                # Same directory layout `_config`/`_update`/`_background_run`
                # already expect from the source-build path, just without a
                # compiled-from-source tree underneath it.
                f'mkdir -p {repo_name}/target/release',
            ]
            try:
                g = Group(*hosts, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)
                Print.heading(f'Initialized testbed of {len(hosts)} nodes (fetch-binary mode)')
            except (GroupException, ExecutionError) as e:
                e = FabricError(e) if isinstance(e, GroupException) else e
                raise BenchError('Failed to install runtime dependencies on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user=self.settings.username, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        # Collocate the primary and its workers on the same machine.
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if sum(len(x) for x in hosts.values()) < nodes:
                return []

            # Select the hosts in different data centers.
            ordered = zip(*hosts.values())
            ordered = [x for y in ordered for x in y]
            return ordered[:nodes]

        # Spawn the primary and each worker on a different machine. Each
        # authority runs in a single data center.
        else:
            primaries = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if len(hosts.keys()) < primaries:
                return []
            for ips in hosts.values():
                if len(ips) < bench_parameters.workers + 1:
                    return []

            # Ensure the primary and its workers are in the same region.
            selected = []
            for region in list(hosts.keys())[:primaries]:
                ips = list(hosts[region])[:bench_parameters.workers + 1]
                selected.append(ips)
            return selected

    def _select_hosts_config(self, bench_parameters):
        # Collocate the primary and its workers on the same machine.
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.internal_hosts()
            if sum(len(x) for x in hosts.values()) < nodes:
                return []

            # Select the hosts in different data centers.
            ordered = zip(*hosts.values())
            ordered = [x for y in ordered for x in y]
            return ordered[:nodes]

        # Spawn the primary and each worker on a different machine. Each
        # authority runs in a single data center.
        else:
            primaries = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.internal_hosts()
            if len(hosts.keys()) < primaries:
                return []
            for ips in hosts.values():
                if len(ips) < bench_parameters.workers + 1:
                    return []

            # Ensure the primary and its workers are in the same region.
            selected = []
            for region in list(hosts.keys())[:primaries]:
                ips = list(hosts[region])[:bench_parameters.workers + 1]
                selected.append(ips)
            return selected



    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user=self.settings.username, connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    # Binaries the orchestrator launches (node/Cargo.toml's default `[[bin]]`
    # from src/main.rs, plus the explicit `benchmark_client` `[[bin]]`) --
    # same names commands.py's CommandMaker.run_primary/run_worker/run_client
    # and alias_binaries hardcode, and the same names docker.yml publishes
    # release assets under (`<bin>-linux-amd64`).
    RELEASE_BINARIES = ('node', 'benchmark_client')

    def _check_binary_provenance(self, release_repo, allow_stale_binary):
        ''' BINARY-PROVENANCE CHECK (fetch-binary deploy path only) --------
        Why this exists: `_update`'s fetch-binary branch below downloads
        `node`/`benchmark_client` from the `nightly` GitHub release tag, and
        that tag is MUTABLE -- docker.yml's "Update nightly release" step
        overwrites the same tag on every push to `main`. There is no
        checksum on the download and no version/commit baked into either
        binary (no GIT_SHA, no `git rev-parse`, no `--version`, no vergen
        anywhere in this repo), and `curl -fL` only fails loudly on a
        genuinely MISSING asset -- a STALE one (still the previous commit's
        build, because docker.yml's ~12-minute build for the CURRENT commit
        hasn't finished yet, or was never triggered for it) downloads and
        `chmod +x`s without complaint. The campaign then silently measures
        the wrong code.

        This is worse than an ordinary stale-binary risk because
        `config::Parameters` (config/src/lib.rs) has no
        `#[serde(deny_unknown_fields)]`: a stale binary does not error out
        on a parameter it doesn't recognize, it just silently ignores it.
        Concretely, `mimic_latency_ms` was added recently -- a stale binary
        predating that field would run with NO latency injection while
        .parameters.json says e.g. 100, and nothing would report the
        discrepancy; the campaign's results would simply be wrong.

        Mechanism: docker.yml also uploads a `commit.txt` asset (the
        `${{ github.sha }}` that produced the binaries) to the SAME
        `nightly` release. Fetched here with a LOCAL curl -- on the
        coordinator, BEFORE anything is deployed to the instances, so a
        mismatch is caught before it can taint a run -- and compared against
        the local working tree's own HEAD commit (`git rev-parse HEAD`,
        run in the repo root).

          - Match: the release is current, proceed silently.
          - Mismatch: hard failure (`BenchError`) naming both SHAs, UNLESS
            `allow_stale_binary` is set, in which case it is downgraded to
            a `Print.warn` -- the explicit, opt-in escape hatch for someone
            who has already confirmed the drift doesn't matter for what
            they're about to run.
          - `commit.txt` missing/unfetchable (e.g. an older release
            published before this check existed): NOT a hard failure --
            `Print.warn` and continue. Provenance being unverifiable must
            never break an otherwise-working deploy. '''
        local_head = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            cwd=self._repo_root(), capture_output=True, text=True,
        )
        if local_head.returncode != 0:
            raise BenchError(
                'Fetch-binary deploy failed',
                ExecutionError(
                    'Could not determine the local working tree\'s HEAD '
                    f'commit via `git rev-parse HEAD`: '
                    f'{local_head.stderr.strip()} -- needed for the '
                    'binary-provenance check (see Bench._check_binary_provenance)'
                )
            )
        local_sha = local_head.stdout.strip()

        commit_url = (
            f'https://github.com/{release_repo}/releases/download/'
            'nightly/commit.txt'
        )
        # Anonymous, local curl -- same public-release assumption as the
        # binary fetches themselves, just run HERE (coordinator) instead of
        # on each instance, and BEFORE any instance is touched.
        remote = subprocess.run(
            ['curl', '-fsL', '--retry', '3', commit_url],
            capture_output=True, text=True,
        )
        if remote.returncode != 0:
            Print.warn(
                f'Could not fetch {commit_url} (an older nightly release '
                'that predates binary-provenance stamping, or a transient '
                'network issue) -- skipping the binary-provenance check; '
                'the deployed binaries may or may not match the working '
                'tree'
            )
            return
        remote_sha = remote.stdout.strip()

        if remote_sha != local_sha:
            message = (
                f'Nightly release binary was built from commit '
                f'{remote_sha}, but the local working tree\'s HEAD is '
                f'{local_sha} -- the release predates this working tree, '
                'so the campaign would measure the wrong binary. Either '
                'wait for the docker.yml workflow run for this commit to '
                'finish publishing the nightly release, or pass '
                '--source-build to compile the working tree on the '
                'instances instead. To proceed anyway, set '
                '"allow_stale_binary": true in the bench parameters.'
            )
            if allow_stale_binary:
                Print.warn(message)
            else:
                raise BenchError('Fetch-binary deploy failed', ConfigError(message))

    def _update(self, hosts, collocate, allow_stale_binary=False):
        if collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        if self.source_build:
            Print.info(f'Updating {len(ips)} machines (working tree deploy, source build)...')
            self._sync_tree(ips)
            cmd = [
                'source $HOME/.cargo/env',
                f'(cd {self.settings.repo_name}/node && {CommandMaker.compile()})',
                CommandMaker.alias_binaries(
                    f'./{self.settings.repo_name}/target/release/'
                )
            ]
            g = Group(*ips, user=self.settings.username, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        else:
            release_repo = self.settings.release_repo
            if not release_repo:
                raise BenchError(
                    'Fetch-binary deploy failed',
                    ConfigError(
                        'settings.json is missing "repo.release_repo" '
                        '(e.g. "<OWNER>/<REPO>", the GitHub slug docker.yml '
                        'publishes the nightly release to) -- set it, or '
                        'pass --source-build to compile on the remote hosts '
                        'instead'
                    )
                )

            # BINARY-PROVENANCE CHECK: verify the nightly release actually
            # corresponds to this working tree's HEAD before deploying
            # anything -- see `_check_binary_provenance`'s docstring for why
            # (silent staleness would otherwise invalidate the measurement).
            self._check_binary_provenance(release_repo, allow_stale_binary)

            repo_name = self.settings.repo_name
            Print.info(f'Updating {len(ips)} machines (fetching pre-built binaries)...')
            cmd = [f'mkdir -p {repo_name}/target/release']
            for binary in self.RELEASE_BINARIES:
                url = (
                    f'https://github.com/{release_repo}/releases/download/'
                    f'nightly/{binary}-linux-amd64'
                )
                dest = f'{repo_name}/target/release/{binary}'
                # Anonymous curl -- the release is public, no auth needed.
                # Same parallel-across-hosts pattern as the source-build
                # path above: one Group.run(), not a per-host loop.
                cmd.append(f'curl -fL --retry 3 -o {dest} {url} && chmod +x {dest}')
            cmd.append(CommandMaker.alias_binaries(
                f'./{repo_name}/target/release/'
            ))
            g = Group(*ips, user=self.settings.username, connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, hosts_private, node_parameters, bench_parameters):
        ''' `hosts`: PUBLIC IPs -- used only to reach each instance over SSH
        (config upload below). `hosts_private`: PRIVATE (VPC-internal) IPs,
        index-aligned with `hosts` (same `_select_hosts`/`_select_hosts_config`
        selection, same order) -- these become the Committee's addresses,
        i.e. what nodes and collocated clients actually dial each other on.
        Same-region node<->node/client<->node traffic over public IPs is
        billed cross-instance data transfer and collapses throughput. '''
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]

        if bench_parameters.collocate:
            workers = bench_parameters.workers
            addresses = OrderedDict(
                (x, [y] * (workers + 1)) for x, y in zip(names, hosts_private)
            )
            public_hosts = OrderedDict(
                (x, [y] * (workers + 1)) for x, y in zip(names, hosts)
            )
        else:
            addresses = OrderedDict(
                (x, y) for x, y in zip(names, hosts_private)
            )
            public_hosts = OrderedDict(
                (x, y) for x, y in zip(names, hosts)
            )
        committee = Committee(
            addresses, self.settings.base_port, public_hosts=public_hosts
        )
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes and upload configuration files. Connections MUST
        # go over the PUBLIC ip (committee.public_ips) -- committee.ips()
        # would now return the (private, VPC-only) wire addresses, which the
        # coordinator laptop cannot reach.
        names = names[:len(names)-bench_parameters.faults]
        progress = progress_bar(names, prefix='Uploading config files:')
        for i, name in enumerate(progress):
            for ip in committee.public_ips(name):
                c = Connection(ip, user=self.settings.username, connect_kwargs=self.connect)
                c.run(f'{CommandMaker.cleanup()} || true', hide=True)
                c.put(PathMaker.committee_file(), '.')
                c.put(PathMaker.key_file(i), '.')
                c.put(PathMaker.parameters_file(), '.')

        return committee

    def deploy_monitoring(self, committee_json, faults=0):
        ''' METRICS-COLLECTOR-PREP step 2: install + start Prometheus on the
        dedicated metrics-collector instance (instance.py's COLLECTOR_NAME),
        scraping every validator's primary+worker metrics endpoint over the
        PRIVATE VPC ip at 1s intervals (see
        `config.generate_collector_scrape_config`'s docstring for why PRIVATE,
        not the committee's own public 'metrics' field).

        `committee_json`: the raw committee dict (`.committee.json`'s shape --
        either loaded straight off disk or a live `Committee`'s `.json`
        attribute); `faults`: same slice-out-the-faulty-nodes convention as
        `_config`'s own upload step.

        Docker (`apt-get install docker.io` + `prom/prometheus` image) rather
        than the raw Prometheus binary: one apt package + one image pull, no
        manual arch/version bookkeeping, and idempotent on redeploy (`docker rm
        -f` tolerates a container that's already there) -- the same tradeoff
        starfish's own monitoring/docker-compose.yml already makes locally.

        No-op (with a warning) if no collector instance exists (`fab create`
        predates this feature, or it's still booting) -- monitoring is
        additive, never required for the benchmark itself. '''
        collector = self.manager.collector_host()
        if collector is None:
            Print.warn(
                'No metrics-collector instance found; skipping Prometheus deploy'
            )
            return None
        collector_public_ip, _ = collector

        yaml_text = generate_collector_scrape_config(committee_json, faults=faults)
        local_path = PathMaker.collector_prometheus_file()
        with open(local_path, 'w') as f:
            f.write(yaml_text)

        Print.info(
            f'Deploying Prometheus on the metrics-collector ({collector_public_ip})...'
        )
        c = Connection(
            collector_public_ip, user=self.settings.username, connect_kwargs=self.connect
        )
        install_cmd = ' && '.join([
            'sudo apt-get update -qq',
            'sudo apt-get install -y -qq docker.io',
            'sudo systemctl enable --now docker',
        ])
        c.run(install_cmd, hide=True)
        c.put(local_path, 'prometheus.yml')

        # User-defined bridge network shared by prometheus + grafana below:
        # Docker's embedded DNS resolves sibling container NAMES on such a
        # network (unlike the default bridge), which is what lets the
        # checked-in monitoring/grafana/datasource.yaml's `url:
        # http://prometheus:9090` work here completely unmodified -- see the
        # Grafana section below. `|| true`: idempotent redeploy onto an
        # already-running collector (e.g. between sweep points).
        c.run('sudo docker network create monitor-net || true', hide=True)

        run_cmd = ' && '.join([
            # Idempotent: tolerates redeploying onto the same, already-running
            # collector (e.g. between back-to-back campaigns against the same
            # testbed). `docker rm -f` only removes the CONTAINER, never a
            # named volume -- the TSDB itself lives in the `prometheus-data`
            # named volume mounted at `/prometheus` (Prometheus's own default
            # `--storage.tsdb.path`) below, so an earlier campaign's samples
            # survive this redeploy and stay queryable (subject to
            # `--storage.tsdb.retention.time=7d`) for the whole coordinator
            # session -- this is what makes `_record_run_window`'s "still
            # reconstructable from the collector, pre-`fab destroy`" recovery
            # path actually hold across multiple `fab campaign` calls, not
            # just within one.
            'sudo docker rm -f prometheus || true',
            'sudo docker run -d --name prometheus --restart unless-stopped '
            '--network monitor-net '
            f'-p {InstanceManager.MONITOR_PORT}:9090 '
            f'-v /home/{self.settings.username}/prometheus.yml:/etc/prometheus/prometheus.yml '
            '-v prometheus-data:/prometheus '
            'prom/prometheus --config.file=/etc/prometheus/prometheus.yml '
            '--storage.tsdb.retention.time=7d --web.enable-admin-api',
        ])
        c.run(run_cmd, hide=True)
        Print.heading(
            f'Prometheus is running on the metrics-collector '
            f'(http://{collector_public_ip}:{InstanceManager.MONITOR_PORT})'
        )

        # METRICS-COLLECTOR-STEP-GRAFANA: browse-able dashboard on the
        # collector itself, no laptop-side docker-compose stack required.
        # Reuses the repo's own local monitoring kit (monitoring/grafana/*)
        # VERBATIM -- same datasource uid (Fixed-UID-vantage, load-bearing:
        # every panel in grafana-dashboard.json hardcodes it, see
        # datasource.yaml's own comment) and the same dashboard JSON the
        # local docker-compose stack serves at localhost:3003. Its `url:
        # http://prometheus:9090` already resolves correctly here: this
        # collector's own "prometheus" container (started above) sits on
        # the same `monitor-net` user-defined network, so no edit to the
        # checked-in yaml is needed -- the identical file that targets the
        # local compose stack's "prometheus" service targets this
        # collector's own container, unmodified.
        #
        # Best-effort, wrapped separately from the Prometheus deploy above:
        # a Grafana failure must not be reported as "monitoring deploy
        # failed" when Prometheus (the metrics themselves) came up fine --
        # see run()'s own best-effort wrapping of this whole method for the
        # analogous reasoning one level up.
        try:
            grafana_dir = join(self._repo_root(), 'monitoring', 'grafana')
            with open(join(grafana_dir, 'grafana-dashboard.json'), 'r') as f:
                dashboard_uid = load(f)['uid']

            c.put(join(grafana_dir, 'datasource.yaml'), 'grafana-datasource.yaml')
            c.put(join(grafana_dir, 'dashboard.yaml'), 'grafana-dashboard-provider.yaml')
            c.put(join(grafana_dir, 'grafana-dashboard.json'), 'grafana-dashboard.json')

            home = f'/home/{self.settings.username}'
            # GRAFANA-ADMIN-PASSWORD: instance.py opens GRAFANA_PORT to
            # 0.0.0.0/0 and ::/0 -- world-open BY DESIGN, so anyone on the
            # team can browse the dashboard straight from a laptop -- which
            # means the image's own default admin/admin login would accept
            # logins from the whole internet, and Grafana's datasource proxy
            # is then an SSRF pivot into the VPC. A random password per
            # deploy (never persisted, only printed below) closes that
            # without narrowing the port itself. Sign-up is also disabled --
            # irrelevant with anonymous Viewer access below, but a
            # wrong-default worth closing explicitly.
            grafana_admin_password = secrets.token_urlsafe(12)
            grafana_cmd = ' && '.join([
                'sudo docker rm -f grafana || true',
                'sudo docker run -d --name grafana --restart unless-stopped '
                '--network monitor-net '
                f'-p {InstanceManager.GRAFANA_PORT}:3000 '
                '-e GF_AUTH_ANONYMOUS_ENABLED=true '
                '-e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer '
                '-e "GF_AUTH_ANONYMOUS_ORG_NAME=Main Org." '
                f'-e GF_SECURITY_ADMIN_PASSWORD={grafana_admin_password} '
                '-e GF_USERS_ALLOW_SIGN_UP=false '
                f'-v {home}/grafana-datasource.yaml:/etc/grafana/provisioning/datasources/datasource.yaml '
                f'-v {home}/grafana-dashboard-provider.yaml:/etc/grafana/provisioning/dashboards/dashboard.yaml '
                # Matches dashboard.yaml's own provider `path:
                # /var/lib/grafana/dashboards`.
                f'-v {home}/grafana-dashboard.json:/var/lib/grafana/dashboards/grafana-dashboard.json '
                'grafana/grafana',
            ])
            c.run(grafana_cmd, hide=True)
            Print.info(
                f'Grafana is running on the metrics-collector: '
                f'http://{collector_public_ip}:{InstanceManager.GRAFANA_PORT} '
                f'(dashboard: http://{collector_public_ip}:'
                f'{InstanceManager.GRAFANA_PORT}/d/{dashboard_uid}) '
                f'-- admin login: admin / {grafana_admin_password}'
            )
        except Exception as e:
            Print.warn(f'Failed to deploy Grafana on the metrics-collector: {e}')

        return collector_public_ip

    def fetch_collector_metrics(self, start=None, end=None, step='1s', subdir=None):
        ''' METRICS-COLLECTOR-PREP step 3: pull the key series (COLLECTOR_QUERIES,
        module-level above) off the metrics-collector's Prometheus HTTP API and
        write each as JSON under PathMaker.collector_metrics_path()
        ('collector-metrics/', a sibling of logs/ and results/ -- see that
        method's docstring for why it must NOT live under logs/)/<name>.json,
        so post-run analysis has the comprehensive metrics locally instead of
        re-querying the (about to be `fab destroy`ed) collector.

        `start`/`end` (unix seconds, both or neither): give both for a
        `query_range` covering the run window (e.g. one rate point's own
        boot-to-kill window, or the whole campaign's) at `step` resolution;
        omit both for an instant `query` (Prometheus's last-known value per
        series). An instant query goes stale the moment more than
        Prometheus's 5-minute lookback has elapsed since the series was last
        scraped (e.g. the nodes it was scraping have since been killed) --
        every value silently comes back `[]`, not an error -- so callers
        that already know their window (the per-rate-point call and the
        end-of-campaign call in `run()`, both below) MUST pass `start`/`end`
        rather than rely on the instant-query fallback.

        `subdir`: routes output to collector_metrics_path()/<subdir>/<name>.json
        instead of the flat collector_metrics_path()/<name>.json -- see
        `PathMaker.collector_metrics_dir`'s docstring. None (default) is the
        flat layout, used by the standalone `fab fetch-metrics` task; `run()`
        below passes a campaign-and-protocol-qualified subdir for BOTH the
        per-rate-point call and its own end-of-campaign call, so neither a
        later rate point nor a later campaign against the same testbed can
        overwrite an earlier one's series files.

        Best-effort per series -- one query failing (collector API briefly
        unreachable, a series that was never observed into on this run, or a
        non-JSON response body) prints a warning and continues rather than
        aborting the whole export, same convention as `scrape_metrics`. '''
        collector = self.manager.collector_host()
        if collector is None:
            Print.warn('No metrics-collector instance found; nothing to fetch')
            return
        collector_public_ip, _ = collector
        base_url = f'http://{collector_public_ip}:{InstanceManager.MONITOR_PORT}'

        out_dir = PathMaker.collector_metrics_dir(subdir)
        makedirs(out_dir, exist_ok=True)
        for name, promql in COLLECTOR_QUERIES.items():
            try:
                body = prometheus_query(base_url, promql, start=start, end=end, step=step)
            # ValueError also covers json.JSONDecodeError (a subclass): a 200
            # response with a non-JSON body must be skipped the same as a
            # transport failure, not left to crash the remaining series of
            # this fetch.
            except (URLError, OSError, ValueError) as e:
                Print.warn(f'Failed to fetch {name!r} ({promql!r}) from {base_url}: {e}')
                continue
            with open(PathMaker.collector_metrics_file(name, subdir), 'w') as f:
                dump(body, f, indent=2)
        Print.heading(f'Wrote collector metrics to {out_dir}')

    def _report_nic_peak(self, subdir):
        ''' PER-RATE-POINT NIC-SATURATION VERDICT: read back this point's
        just-written bytes_sent_rate_by_host.json (a query_range response --
        data.result[] each with .metric.host and .values == [[ts, "v"], ...],
        "v" a string per Prometheus's own JSON encoding) and print the single
        peak per-HOST send rate observed anywhere in the window, so whether
        this point pegged the NIC is visible inline in the campaign log
        without waiting for post-hoc analysis.

        Aggregated by `host` (one series per physical instance/NIC), NOT by
        `node` (one series per primary/worker PROCESS): under the campaign's
        `collocate: True`, an authority's primary and its worker are two
        processes sharing one instance and one NIC, so a per-`node` max only
        ever sees one of the two processes' share of that NIC's traffic and
        understates the instance's actual total (see
        `config.generate_collector_scrape_config`'s docstring for the
        `node`-vs-`host` label distinction).

        `self.settings.instance_type` (the c5/c5d.xlarge family this harness
        currently uses, see settings.json) has a NIC baseline of ~1.25 Gbps
        (~156 MB/s decimal) -- a peak near that is the signature of NIC
        saturation, not a consensus/store bottleneck. `mb_s` uses the same
        decimal (1e6) convention as that baseline, not the binary 1024*1024
        MiB one, so the two numbers are directly comparable.

        Missing/malformed file: `Print.warn`s and returns rather than
        raising -- the caller (`run()`) also wraps this together with the
        fetch itself in one best-effort try/except, so a bad read here never
        breaks the sweep. '''
        path = PathMaker.collector_metrics_file('bytes_sent_rate_by_host', subdir)
        try:
            with open(path, 'r') as f:
                body = load(f)
        except (OSError, ValueError) as e:
            Print.warn(f'Failed to read {path} for the NIC-saturation verdict: {e}')
            return
        peak_host, peak_bytes_per_sec = None, -1.0
        for series in body.get('data', {}).get('result', []):
            host = series.get('metric', {}).get('host', '?')
            for _, value in series.get('values', []):
                v = float(value)
                if v > peak_bytes_per_sec:
                    peak_bytes_per_sec, peak_host = v, host
        if peak_host is None:
            return
        mb_s = peak_bytes_per_sec / 1e6
        gbps = (peak_bytes_per_sec * 8) / 1e9
        Print.info(
            f'Peak wire TX per host: {peak_host} {mb_s:.1f} MB/s '
            f'({gbps:.2f} Gbps) -- {self.settings.instance_type} '
            f'(c5/c5d.xlarge family) NIC baseline ~1.25 Gbps'
        )

    def _record_run_window(self, n, rate, protocol, campaign, start, end):
        ''' RUN-WINDOWS LOG: append (or, on a rerun of the exact same point OF
        THE SAME CAMPAIGN, update in place) this rate point's wall-clock
        window to collector_metrics_path()/run-windows.json -- a flat JSON
        list of {nodes, rate, protocol, campaign, start, end} entries, one
        per rate point across the WHOLE coordinator session (read-modify-
        write, not truncate-and-write, so two `fab campaign` calls back-to-
        back against the same testbed both land in the same file).

        `campaign`: the caller's own `campaign_subdir` tag (protocol +
        campaign-start timestamp, see `run()`) -- included in the entry AND
        folded into the in-place-update key alongside nodes/rate/protocol.
        Without it, a SECOND campaign of the SAME protocol at the same
        nodes/rate (e.g. two `vantage` campaigns run back-to-back for
        comparison) would match the first campaign's entry on
        (nodes, rate, protocol) alone and silently overwrite its window
        bounds -- defeating the very recovery path this method exists for,
        and exactly the collision `campaign_subdir` was introduced to
        prevent for the per-point series files themselves (see
        `fetch_collector_metrics`'s docstring). Two DIFFERENT protocols (or
        the same protocol from two different campaigns) are therefore now
        distinguished by `campaign` first and foremost, not by `protocol`
        alone.

        Purpose: even where the per-point `fetch_collector_metrics` call
        above failed outright (collector briefly unreachable), the window
        bounds are still on record, so a post-hoc `query_range` against the
        collector (while it's still alive, pre-`fab destroy`) remains
        reconstructable from this file alone -- this file lives under
        `PathMaker.collector_metrics_path()` (a sibling of logs/, NOT nested
        under it), so the local `rm -r logs` every rate point runs
        (`_run_single`) never deletes it, and Prometheus's own TSDB now
        survives a redeploy between campaigns via a named Docker volume (see
        `deploy_monitoring`), so the window this file records for an earlier
        campaign is still backed by live data when a later campaign's
        `deploy_monitoring` call runs. '''
        path = join(PathMaker.collector_metrics_dir(), 'run-windows.json')
        windows = []
        if isfile(path):
            try:
                with open(path, 'r') as f:
                    windows = load(f)
            except (OSError, ValueError):
                windows = []
        entry = {
            'nodes': n, 'rate': rate, 'protocol': protocol, 'campaign': campaign,
            'start': start, 'end': end,
        }
        for i, existing in enumerate(windows):
            same_point = (
                existing.get('nodes'), existing.get('rate'),
                existing.get('protocol'), existing.get('campaign'),
            ) == (n, rate, protocol, campaign)
            if same_point:
                windows[i] = entry
                break
        else:
            windows.append(entry)
        makedirs(PathMaker.collector_metrics_dir(), exist_ok=True)
        with open(path, 'w') as f:
            dump(windows, f, indent=2)

    def _run_single(self, rate, committee, bench_parameters, debug=False):
        faults = bench_parameters.faults

        # Kill any potentially unfinished run and delete logs. SSH targets
        # are the PUBLIC (physical-host) ips -- committee.ips() now returns
        # the PRIVATE wire addresses, unreachable from the coordinator.
        hosts = committee.public_ips()
        self.kill(hosts=hosts, delete_logs=True)

        # Clear stale LOCAL logs/metrics from a previous run now, not in
        # `_logs()` after this run's `scrape_metrics()` calls below have
        # already written this run's metrics-*.txt into the same directory
        # (Phase 2 added those writes; `_logs()`'s cleanup predates them and
        # otherwise deletes them again before `LogParser.process()` ever
        # reads them, silently zeroing "real transaction latency" every run).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        Print.info('Booting clients...')
        workers_addresses = committee.workers_addresses(faults)
        workers_public_ips = committee.workers_public_ips(faults)
        rate_share = ceil(rate / committee.workers())
        for i, addresses in enumerate(workers_addresses):
            for (id, address), (_, host) in zip(addresses, workers_public_ips[i]):
                # `address` (the client's own submit target, and the peer
                # addresses below) is the PRIVATE committee address -- the
                # client runs ON the instance and talks to its co-located
                # worker/peers over the VPC. `host` (the fabric SSH target
                # to spawn it) is the instance's PUBLIC ip.
                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    rate_share,
                    [x for y in workers_addresses for _, x in y],
                    mode=bench_parameters.tx_mode
                )
                print(cmd)
                log_file = PathMaker.client_log_file(i, id)
                self._background_run(host, cmd, log_file)

        # Run the primaries (except the faulty ones).
        Print.info('Booting primaries...')
        for i, host in enumerate(committee.primary_public_ips(faults)):
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                # NVMe-INSTANCE-STORE: the remote store now lives on the
                # mounted local NVMe instance-store disk (install() formats +
                # mounts it), not the EBS root -- see PathMaker.remote_db_path.
                PathMaker.remote_db_path(i),
                PathMaker.parameters_file(),
                debug=debug
            )
            log_file = PathMaker.primary_log_file(i)
            self._background_run(host, cmd, log_file)

        # Run the workers (except the faulty ones).
        Print.info('Booting workers...')
        for i, addresses in enumerate(workers_addresses):
            for (id, address), (_, host) in zip(addresses, workers_public_ips[i]):
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    # NVMe-INSTANCE-STORE: see the primary's run_primary call
                    # above -- same NVMe-mounted store, per-worker subdirectory.
                    PathMaker.remote_db_path(i, id),
                    PathMaker.parameters_file(),
                    id,  # The worker's id.
                    debug=debug
                )
                log_file = PathMaker.worker_log_file(i, id)
                self._background_run(host, cmd, log_file)

         # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for i in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            tick_size = ceil(duration / 20)
            #print(tick_size, i, bench_parameters.partition_start, bench_parameters.simulate_partition)
            if bench_parameters.simulate_partition and i*tick_size == bench_parameters.partition_start:
                print('simulating partition')
                self._simulate_partition(bench_parameters, committee, faults)
            
            if bench_parameters.simulate_partition and i*tick_size == bench_parameters.partition_start + bench_parameters.partition_duration:
                print('deleting partition')
                self._delete_partition(bench_parameters, committee, faults)

            sleep(ceil(duration / 20))

        # Scrape every node's Prometheus endpoint before killing it (PHASE2-SPEC.md #5)
        # -- real transaction latency lives only in-process, not in the logs.
        Print.info('Scraping metrics...')
        for i, address in enumerate(committee.primary_metrics_addresses(faults)):
            scrape_metrics(address, PathMaker.metrics_primary_file(i))
        for i, addresses in enumerate(committee.workers_metrics_addresses(faults)):
            for (id, address) in addresses:
                scrape_metrics(address, PathMaker.metrics_worker_file(i, id))

        self.kill(hosts=hosts, delete_logs=False)

    def _simulate_partition(self, bench_parameters, committee, faults):
        # `tc ... match ip dst` must target the PRIVATE (wire) address peers
        # actually dial (unchanged); the SSH connection to install that rule
        # must go to the PUBLIC ip of the physical host running primary `i`
        # -- Committee.ip(address) on a (now private) committee address is
        # not reachable from the coordinator laptop.
        primary_public_ips = committee.primary_public_ips(faults)
        for i, address in enumerate(committee.primary_addresses(faults)):
            if i < bench_parameters.partition_nodes:
                print(i, address)
                cmd = []
                #cmd = ['sudo tc qdisc del dev ens4 root']
                cmd.append('sudo tc qdisc add dev ens4 root handle 1: htb')
                cmd.append('sudo tc class add dev ens4 parent 1: classid 1:1 htb rate 10gibps')
                idx = 2
                for j, addr in enumerate(committee.primary_addresses(faults)):
                    if i == j:
                        continue
                    cmd.append('sudo tc class add dev ens4 parent 1:1 classid 1:' + str(idx) + ' htb rate 10gibps')
                    cmd.append('sudo tc qdisc add dev ens4 handle ' + str(idx) + ': parent 1:'
                            + str(idx) + ' netem delay 5000ms')
                    cmd.append('sudo tc filter add dev ens4 pref ' + str(idx) + ' protocol ip u32 match ip dst ' +
                            Committee.ip(addr) + ' flowid 1:' + str(idx))
                    idx = idx + 1
                ip = [primary_public_ips[i]]
                g = Group(*ip, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)
        

         
        #hosts = committee.ips()
        #cmd = ['sudo iptables -A OUTPUT -d ' + ip + ' -j DROP' for ip in partition_ips]
        #cmd = ['sudo tc qdisc add dev ens4 root netem delay 5000ms']
        
        #g = Group(*partition_ips, user='neilgiridharan', connect_kwargs=self.connect)
        #g.run(' && '.join(cmd), hide=True) 
        
        #for i, address in enumerate(committee.primary_addresses(faults)):
        
        #host = Committee.ip(address)
        #for partition_ip in partition_ips:
        #cmd = 'sudo iptables -A OUTPUT -d ' + partition_ip + '-j DROP'
        
        ##log_file = PathMaker.primary_log_file(i)
        #self._background_run(host, cmd, log_file)
    
    def _delete_partition(self, bench_parameters, committee, faults):
        # Same PUBLIC-for-SSH note as `_simulate_partition` above.
        primary_public_ips = committee.primary_public_ips(faults)
        for i, address in enumerate(committee.primary_addresses(faults)):
            if i < bench_parameters.partition_nodes:
                partition_ips = [primary_public_ips[i]]
                cmd = ['sudo tc qdisc del dev ens4 root']
                g = Group(*partition_ips, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)

       
        #hosts = committee.ips()
        #cmd = ['sudo iptables -F']
        #cmd = ['sudo tc qdisc del dev ens4 root']
        #g = Group(*partition_ips, user='neilgiridharan', connect_kwargs=self.connect)
        #g.run(' && '.join(cmd), hide=True) 
        
        #for i, address in enumerate(committee.primary_addresses(faults)):
        #    host = Committee.ip(address)
        #    cmd = 'sudo iptables -F'
        #    log_file = PathMaker.primary_log_file(i)
        #    self._background_run(host, cmd, log_file)

    def _logs(self, committee, faults, duration=None):
        # NOTE: local logs/metrics are cleared in `_run_single` now (before
        # this run's `scrape_metrics()` writes its metrics-*.txt), not here
        # -- doing it here would delete this run's own metrics files, which
        # `_run_single` already wrote into the same `logs/` directory, before
        # `LogParser.process()` below ever gets to read them. See the note
        # in `_run_single`.

        # Download log files. SSH targets are the PUBLIC (physical-host)
        # ips -- Committee.ip(address) on a committee address would now
        # extract a PRIVATE ip, unreachable from the coordinator.
        workers_addresses = committee.workers_addresses(faults)
        workers_public_ips = committee.workers_public_ips(faults)
        progress = progress_bar(workers_addresses, prefix='Downloading workers logs:')
        for i, addresses in enumerate(progress):
            for (id, address), (_, host) in zip(addresses, workers_public_ips[i]):
                c = Connection(host, user=self.settings.username, connect_kwargs=self.connect)
                c.get(
                    PathMaker.client_log_file(i, id),
                    local=PathMaker.client_log_file(i, id)
                )
                c.get(
                    PathMaker.worker_log_file(i, id),
                    local=PathMaker.worker_log_file(i, id)
                )

        primary_public_ips = committee.primary_public_ips(faults)
        progress = progress_bar(primary_public_ips, prefix='Downloading primaries logs:')
        for i, host in enumerate(progress):
            c = Connection(host, user=self.settings.username, connect_kwargs=self.connect)
            c.get(
                PathMaker.primary_log_file(i),
                local=PathMaker.primary_log_file(i)
            )

        # Parse logs and return the parser. `duration` (the campaign's
        # configured run length) is the denominator for the prometheus-based
        # committed TPS (PREP FIX 3) when given.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(
            PathMaker.logs_path(), faults=faults, duration=duration
        )

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        # CAMPAIGN WINDOW: wall-clock start of the whole campaign (epoch
        # seconds, must line up with Prometheus's own timestamps) -- paired
        # with `campaign_end` at the bottom of this method for the
        # end-of-campaign `fetch_collector_metrics` call's query_range.
        campaign_start = time.time()
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # CAMPAIGN-METRICS DISCRIMINATOR: every collector export this `run()`
        # call writes (per-rate-point AND its own end-of-campaign convenience
        # export, both below) is namespaced under this subdirectory instead
        # of a flat/point-only name, so a SECOND `Bench.run()` against the
        # same testbed -- e.g. back-to-back campaigns run for comparison --
        # can never silently overwrite the first campaign's JSON
        # (`fetch_collector_metrics` opens every file with 'w'). Keyed on
        # protocol + this campaign's own start time (UTC, second resolution)
        # rather than protocol alone, so even two campaigns of the SAME
        # protocol run back-to-back stay distinct. Also passed to
        # `_record_run_window` below as its own `campaign` discriminator, for
        # the same reason applied to run-windows.json's entries.
        campaign_subdir = (
            f"{node_parameters.json.get('protocol', 'unknown')}-"
            f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(campaign_start))}"
        )

        # Select which hosts to use -- PUBLIC ips (SSH/rsync/tmux: install,
        # deploy, background-run, kill, log download all connect through
        # these, from the coordinator laptop).
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Same selection, but PRIVATE (VPC-internal) ips -- index-aligned
        # with `selected_hosts` above (same region ordering, same slicing).
        # This is what the Committee (node<->node, client<->node) gets built
        # from, so same-region traffic never crosses the public internet
        # edge. See instance.py's `internal_hosts()` for the pairing caveat.
        selected_hosts_private = self._select_hosts_config(bench_parameters)
        if not selected_hosts_private:
            Print.warn('There are not enough instances available (private IPs)')
            return

        # Update nodes.
        print(selected_hosts)
        try:
            self._update(
                selected_hosts, bench_parameters.collocate,
                bench_parameters.allow_stale_binary,
            )
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Upload all configuration files.
        try:
            committee = self._config(
                selected_hosts, selected_hosts_private,
                node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        # METRICS-COLLECTOR-PREP: deploy the dedicated metrics-collector's
        # Prometheus (scraping every validator over its PRIVATE ip, see
        # `deploy_monitoring`'s docstring). Best-effort/non-fatal -- an older
        # testbed with no collector instance, or a transient SSH hiccup while
        # installing Docker, must not abort the whole campaign: monitoring is
        # additive, not required for the benchmark itself to run.
        try:
            self.deploy_monitoring(committee.json, faults=bench_parameters.faults)
        except Exception as e:
            Print.warn(f'Failed to deploy monitoring on the metrics-collector: {e}')

        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            # CHANGE A (rate-sweep early stop on committed-TPS turnover):
            # running PEAK committed TPS seen so far in THIS node count's
            # rate sweep (reset per `n`, since saturation is a function of
            # committee size). `None` until the first rate point with a
            # usable committed-TPS reading.
            peak_committed_tps = None

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # PER-RATE-POINT PROMETHEUS WINDOW (kills the end-of-campaign
                # staleness bug, see `fetch_collector_metrics`'s docstring):
                # wall-clock bounds spanning this point's boot(s) through its
                # final kill below, used for a query_range fetch scoped to
                # exactly this point instead of the campaign-wide instant
                # query the harness used to rely on.
                point_start = time.time()

                # Run the benchmark.
                run_committed_tps = []
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            r, committee_copy, bench_parameters, debug
                        )

                        faults = bench_parameters.faults
                        logger = self._logs(
                            committee_copy, faults, bench_parameters.duration
                        )
                        logger.print(PathMaker.result_file(
                            faults,
                            n,
                            bench_parameters.workers,
                            bench_parameters.collocate,
                            r,
                            bench_parameters.tx_size,
                        ))

                        # CHANGE A: this run's committed TPS (prometheus-
                        # derived, see logs.py's `committed_tps`), for the
                        # early-stop decision below. `None`/0 (nothing
                        # committed/scraped, or no duration) is dropped, not
                        # treated as a real 0 tx/s reading -- a run that
                        # failed to produce a usable number must not look
                        # like a genuine collapse to the peak-relative check.
                        tps = logger.committed_tps()
                        if tps is not None and tps > 0:
                            run_committed_tps.append(tps)
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue

                point_end = time.time()

                # PER-RATE-POINT METRICS FETCH: query_range over exactly
                # [point_start, point_end] (5s resolution -- fine enough for
                # a 60-300s window without ballooning the JSON), into this
                # point's own subdirectory (nested under this campaign's own
                # `campaign_subdir`, see above) so per-point series never
                # overwrite another point's, AND a same-named point from a
                # different campaign against the same testbed never
                # overwrites this one's (B2 fix). Best-effort, same
                # non-fatal-to-the-sweep convention as `deploy_monitoring`'s
                # own wrapping in this method -- one point's collector
                # hiccup must not abort the remaining rate sweep. The NIC
                # verdict read-back is folded into the same try/except: it
                # depends on the fetch having just written that point's
                # bytes_sent_rate_by_host.json, so a failed fetch already
                # skips it via the exception.
                point_subdir = join(campaign_subdir, f'{n}nodes-{r}rate')
                try:
                    self.fetch_collector_metrics(
                        start=point_start, end=point_end, step='5s',
                        subdir=point_subdir,
                    )
                    self._report_nic_peak(point_subdir)
                except Exception as e:
                    Print.warn(
                        f'Failed to fetch/report per-point collector metrics '
                        f'for {n} nodes / rate={r:,}: {e}'
                    )

                # RUN-WINDOWS LOG: record this point's window regardless of
                # whether the fetch above succeeded (see
                # `_record_run_window`'s docstring) -- separate try/except so
                # a failure here never masks/duplicates the warning above.
                try:
                    self._record_run_window(
                        n, r, node_parameters.json.get('protocol'), campaign_subdir,
                        point_start, point_end,
                    )
                except Exception as e:
                    Print.warn(
                        f'Failed to record run-window for {n} nodes / '
                        f'rate={r:,}: {e}'
                    )

                # CHANGE A: peak-relative early stop. This rate's
                # representative committed TPS is the AVERAGE across its
                # `runs` (documented choice -- smooths a single noisy run
                # without discarding the others; with the campaign's default
                # `runs=1` this is just that one run's number). Skipped
                # entirely (no peak update, no stop check) if every run at
                # this rate failed to parse or produced no usable committed-
                # TPS reading -- a parse failure must never be read as a
                # throughput collapse, and must never crash the sweep.
                if run_committed_tps:
                    point_tps = mean(run_committed_tps)
                    if peak_committed_tps is None:
                        peak_committed_tps = point_tps
                    else:
                        peak_committed_tps = max(peak_committed_tps, point_tps)

                    margin = bench_parameters.early_stop_margin
                    threshold = peak_committed_tps * (1 - margin)
                    if margin > 0 and point_tps < threshold:
                        Print.heading(
                            f'Committed TPS {point_tps:,.0f} < '
                            f'{(1 - margin) * 100:.0f}% of peak '
                            f'{peak_committed_tps:,.0f} at rate={r:,} -- '
                            f'stopping sweep (remaining higher rates skipped)'
                        )
                        break

        # METRICS-COLLECTOR-PREP step 3: pull the comprehensive metrics off the
        # collector now, before `fab destroy` terminates it. `start`/`end`
        # span the WHOLE campaign (not an instant query -- see
        # `fetch_collector_metrics`'s docstring on why that goes stale) as a
        # convenience full-campaign export layered on top of the
        # per-rate-point fetches above (which already cover every point
        # precisely); this is the belt to their suspenders, e.g. for a
        # single query spanning point boundaries. `subdir=campaign_subdir`
        # (same discriminator as the per-point fetches above, B2 fix): without
        # it this call writes the FLAT collector-metrics/<name>.json, which a
        # second campaign against the same testbed would silently overwrite.
        # Best-effort/non-fatal for the same reason as the deploy step above.
        campaign_end = time.time()
        # Keep well under Prometheus's ~11,000-points-per-series query_range
        # cap regardless of how long this campaign ran: at the default 1s
        # step that cap is ~3h03m; widening the step for longer windows (10k
        # points' worth) avoids a wholesale 422 on this convenience export
        # without materially losing resolution (the per-point fetches above
        # already cover every rate point at a fixed, fine 5s step).
        campaign_step = max(1, ceil((campaign_end - campaign_start) / 10_000))
        try:
            self.fetch_collector_metrics(
                start=campaign_start, end=campaign_end, step=f'{campaign_step}s',
                subdir=campaign_subdir,
            )
        except Exception as e:
            Print.warn(f'Failed to fetch metrics from the metrics-collector: {e}')
