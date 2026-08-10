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


# PromQL queries used by `Bench.fetch_collector_metrics`.
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
    'core_wait_timer_by_proc': 'sum by (proc) (core_wait_timer)',
    'core_queue_length': 'core_queue_length',
    'core_queue_peak': 'core_queue_peak',
    'protocol_info': 'protocol_info',
    'transaction_mode_info': 'transaction_mode_info',
    # `up` reports scrape health.
    # Node labels provide process-level rates; host labels provide NIC-level
    # rates when a primary and worker share an instance.
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
    # Exclude local build output and credentials from working-tree uploads.
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
        # Fetch release binaries by default; `source_build` compiles on hosts.
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
        '''Upload the working tree, excluding build output and local data.'''
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
        '''Return a safe, best-effort NVMe mount snippet for validator stores.

        The root disk and any disk with a filesystem or mount are excluded.
        A mount failure leaves the required writable store directory check to
        `install()`.'''
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
        '''Install dependencies and prepare hosts for benchmark binaries.'''
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
        '''Verify that the release commit matches the local checkout.

        A mismatch raises `BenchError` unless `allow_stale_binary` is true.
        An unavailable commit marker warns and continues.'''
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

            # Verify the release commit before deploying binaries.
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
        '''Build and upload configuration using public SSH and private wire IPs.'''
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
        '''Deploy Prometheus and Grafana on the metrics collector.

        Uses private validator metrics addresses. Missing collectors produce a
        warning and do not stop the benchmark.'''
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

        # Share a user-defined network so Grafana resolves Prometheus by name.
        c.run('sudo docker network create monitor-net || true', hide=True)

        run_cmd = ' && '.join([
            # Keep the TSDB volume when replacing the Prometheus container.
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

        # Deploy the repository dashboard beside Prometheus.
        try:
            grafana_dir = join(self._repo_root(), 'monitoring', 'grafana')
            with open(join(grafana_dir, 'grafana-dashboard.json'), 'r') as f:
                dashboard_uid = load(f)['uid']

            c.put(join(grafana_dir, 'datasource.yaml'), 'grafana-datasource.yaml')
            c.put(join(grafana_dir, 'dashboard.yaml'), 'grafana-dashboard-provider.yaml')
            c.put(join(grafana_dir, 'grafana-dashboard.json'), 'grafana-dashboard.json')

            home = f'/home/{self.settings.username}'
            # Use a random admin password because the dashboard port is public.
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
        '''Fetch collector series as JSON.

        Pass `start` and `end` together for a range query. Without them, use
        an instant query. A failed series warns and does not stop the export.'''
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
            # Skip invalid responses and continue with the remaining series.
            except (URLError, OSError, ValueError) as e:
                Print.warn(f'Failed to fetch {name!r} ({promql!r}) from {base_url}: {e}')
                continue
            with open(PathMaker.collector_metrics_file(name, subdir), 'w') as f:
                dump(body, f, indent=2)
        Print.heading(f'Wrote collector metrics to {out_dir}')

    def _report_nic_peak(self, subdir):
        '''Report peak wire transmit rate per physical host.

        The metric file is Prometheus query-range JSON. Missing or malformed
        data produces a warning and returns.'''
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
        '''Record a rate-point time window in `run-windows.json`.

        The run identifier distinguishes repeated points and supports later
        range queries while the collector is available.'''
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

        # Use public host addresses for SSH; committee addresses are private.
        hosts = committee.public_ips()
        self.kill(hosts=hosts, delete_logs=True)

        # Remove local logs before collecting this run's metric snapshots.
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
            if bench_parameters.simulate_partition and i*tick_size == bench_parameters.partition_start:
                print('simulating partition')
                self._simulate_partition(bench_parameters, committee, faults)
            
            if bench_parameters.simulate_partition and i*tick_size == bench_parameters.partition_start + bench_parameters.partition_duration:
                print('deleting partition')
                self._delete_partition(bench_parameters, committee, faults)

            sleep(ceil(duration / 20))

        # Scrape metrics before stopping the nodes.
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
        
    def _delete_partition(self, bench_parameters, committee, faults):
        # Same PUBLIC-for-SSH note as `_simulate_partition` above.
        primary_public_ips = committee.primary_public_ips(faults)
        for i, address in enumerate(committee.primary_addresses(faults)):
            if i < bench_parameters.partition_nodes:
                partition_ips = [primary_public_ips[i]]
                cmd = ['sudo tc qdisc del dev ens4 root']
                g = Group(*partition_ips, user=self.settings.username, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)

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

        # Parse logs; `duration` is the TPS denominator when provided.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(
            PathMaker.logs_path(), faults=faults, duration=duration
        )

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        # Record the run window in Unix seconds for Prometheus range queries.
        campaign_start = time.time()
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Namespace collector exports by protocol and run start time.
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

        # Monitoring is best-effort and does not gate the benchmark.
        try:
            self.deploy_monitoring(committee.json, faults=bench_parameters.faults)
        except Exception as e:
            Print.warn(f'Failed to deploy monitoring on the metrics-collector: {e}')

        # Run benchmarks.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            # Track the peak committed TPS for this node count.
            peak_committed_tps = None

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Query metrics over this rate point's wall-clock window.
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

                        # Use committed TPS when available for early stopping.
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

                # Fetch and report this point's collector metrics.
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

                # Record the window even when metric fetching fails.
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

                # Use the mean committed TPS across successful runs.
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

        # Fetch a run-wide metrics export before the collector is destroyed.
        campaign_end = time.time()
        # Widen the step for long runs to stay below Prometheus's point limit.
        campaign_step = max(1, ceil((campaign_end - campaign_start) / 10_000))
        try:
            self.fetch_collector_metrics(
                start=campaign_start, end=campaign_end, step=f'{campaign_step}s',
                subdir=campaign_subdir,
            )
        except Exception as e:
            Print.warn(f'Failed to fetch metrics from the metrics-collector: {e}')
