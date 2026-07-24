# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os import makedirs
from os.path import basename, splitext, abspath, dirname, join
from json import dump
from urllib.error import URLError
from time import sleep
from math import ceil
from copy import deepcopy
import subprocess

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
            cmd = [
                'sudo apt-get update',
                # curl: downloads the release binaries in `_update`. ca-certificates:
                # verifies the https://github.com download. tmux: `_background_run`
                # launches every primary/worker/client inside a tmux session.
                'sudo apt-get -y install curl ca-certificates tmux',
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

    def _update(self, hosts, collocate):
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
        run_cmd = ' && '.join([
            # Idempotent: tolerates redeploying onto the same, already-running
            # collector (e.g. between sweep points in the same campaign).
            'sudo docker rm -f prometheus || true',
            'sudo docker run -d --name prometheus --restart unless-stopped '
            f'-p {InstanceManager.MONITOR_PORT}:9090 '
            f'-v /home/{self.settings.username}/prometheus.yml:/etc/prometheus/prometheus.yml '
            'prom/prometheus --config.file=/etc/prometheus/prometheus.yml '
            '--storage.tsdb.retention.time=7d --web.enable-admin-api',
        ])
        c.run(run_cmd, hide=True)
        Print.heading(
            f'Prometheus is running on the metrics-collector '
            f'(http://{collector_public_ip}:{InstanceManager.MONITOR_PORT})'
        )
        return collector_public_ip

    def fetch_collector_metrics(self, start=None, end=None, step='1s'):
        ''' METRICS-COLLECTOR-PREP step 3: pull the key series (COLLECTOR_QUERIES,
        module-level above) off the metrics-collector's Prometheus HTTP API and
        write each as JSON under logs/collector/<name>.json, so post-run
        analysis has the comprehensive metrics locally instead of re-querying
        the (about to be `fab destroy`ed) collector.

        `start`/`end` (unix seconds, both or neither): give both for a
        `query_range` covering the run window (e.g. the campaign's start time
        through now) at `step` resolution; omit both for an instant `query`
        (Prometheus's last-known value per series -- always available, no
        window bookkeeping required by the caller).

        Best-effort per series -- one query failing (collector API briefly
        unreachable, a series that was never observed into on this run) prints
        a warning and continues rather than aborting the whole export, same
        convention as `scrape_metrics`. '''
        collector = self.manager.collector_host()
        if collector is None:
            Print.warn('No metrics-collector instance found; nothing to fetch')
            return
        collector_public_ip, _ = collector
        base_url = f'http://{collector_public_ip}:{InstanceManager.MONITOR_PORT}'

        out_dir = PathMaker.collector_metrics_dir()
        makedirs(out_dir, exist_ok=True)
        for name, promql in COLLECTOR_QUERIES.items():
            try:
                body = prometheus_query(base_url, promql, start=start, end=end, step=step)
            except (URLError, OSError) as e:
                Print.warn(f'Failed to fetch {name!r} ({promql!r}) from {base_url}: {e}')
                continue
            with open(PathMaker.collector_metrics_file(name), 'w') as f:
                dump(body, f, indent=2)
        Print.heading(f'Wrote collector metrics to {out_dir}')

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
                PathMaker.db_path(i),
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
                    PathMaker.db_path(i, id),
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
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

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
            self._update(selected_hosts, bench_parameters.collocate)
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

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                # Run the benchmark.
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
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue

        # METRICS-COLLECTOR-PREP step 3: pull the comprehensive metrics off the
        # collector now, before `fab destroy` terminates it. Best-effort/
        # non-fatal for the same reason as the deploy step above.
        try:
            self.fetch_collector_metrics()
        except Exception as e:
            Print.warn(f'Failed to fetch metrics from the metrics-collector: {e}')
