# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


@task
def create(ctx, nodes=6):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=True, protocol='autobahn-optimistic', compress_network=False):
    ''' Run benchmarks on AWS.

    Phase-7 smoke test: checked-in defaults below, except `rate` set to
    50,000 tx/s (conservative for an unknown/smaller instance size than
    prior AWS runs) and `delta_ms: 150` added to node_params (passed
    through NodeParameters/serde into Parameters.delta_ms). `protocol` is
    exposed as a fab CLI arg (`--protocol=vantage`) so the same task runs
    both the autobahn-optimistic and vantage smoke passes without editing
    this file between runs.
    '''
    bench_params = {
        'faults': 0,
        'nodes': [4],
        'workers': 1,
        'co-locate': True,
        'rate': [50_000],
        'tx_size': 512,
        # METRICS-DASHBOARD-SPEC.md §8: 'random' is now the default transaction
        # mode everywhere (all-zero stays available). Guard/gate/sweep benchmarks
        # must override this back to 'all-zero' explicitly for comparability with
        # historical gate numbers (all of which are all-zero).
        'tx_mode': 'random',
        'duration': 60,
        'runs': 1,

        # Unused
        'simulate_partition': True,
        'partition_start': 5,
        'partition_duration': 5,
        'partition_nodes': 1,
    }
    node_params = {
        'timeout_delay': 5_000,  # ms
        'header_size': 32,  # bytes
        'max_header_delay': 5_000,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 5_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 20,  # ms
        'protocol': protocol,
        'use_parallel_proposals': True,
        'k': 4,
        'use_fast_path': True,
        'fast_path_timeout': 5_000,
        'use_ride_share': False,
        'car_timeout': 5_000,
        'delta_ms': 150,  # ms -- Phase 7 smoke-test setting (Vantage's AGB/control-log delta)
        # METRICS-DASHBOARD-SPEC.md §8: off by default, byte-identical framing when
        # off; `fab remote --compress-network` (or edit this literal) to enable.
        'compress_network': compress_network,

        'simulate_asynchrony': False,
        'asynchrony_start': 15_000, #ms
        'asynchrony_duration': 3_000, #ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def monitor(ctx):
    ''' METRICS-DASHBOARD-SPEC.md §4 (orchestration mode): generate
    monitoring/prometheus-remote.yaml from the last `fab remote` run's
    .committee.json (public IPs + metrics ports) so the SAME local
    monitoring/docker-compose.yml stack (grafana 3003 / prometheus 9095) can
    watch a live AWS run instead of a local-benchmark one -- just point
    docker-compose.yml's prometheus volume mount at prometheus-remote.yaml
    instead of .local-bench/prometheus.yaml (see monitoring/README.md).
    Read-only: does not touch the committee/run itself, safe to re-run any time
    after `fab install`/`fab remote` has written .committee.json.
    '''
    from json import load
    from os.path import join
    from benchmark.utils import PathMaker

    try:
        with open(PathMaker.committee_file(), 'r') as f:
            committee = load(f)
    except (OSError, IOError) as e:
        Print.error(BenchError('Failed to read committee file (run `fab remote` at least once first)', e))
        return

    targets = []
    for name, authority in committee['authorities'].items():
        targets.append((f'{name[:8]}-primary', authority['primary']['metrics']))
        for wid, worker in authority['workers'].items():
            targets.append((f'{name[:8]}-worker-{wid}', worker['metrics']))

    lines = ['global:', '  scrape_interval: 1s', 'scrape_configs:', "  - job_name: 'vantage-remote'", '    static_configs:']
    for label, addr in targets:
        lines.append(f"      - targets: ['{addr}']")
        lines.append('        labels:')
        lines.append(f"          node: '{label}'")

    out_path = join('..', 'monitoring', 'prometheus-remote.yaml')
    with open(out_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
    Print.info(f'Wrote {out_path} ({len(targets)} scrape targets)')
    Print.info('Point monitoring/docker-compose.yml\'s prometheus volume mount at '
               'prometheus-remote.yaml (instead of .local-bench/prometheus.yaml), '
               'then `docker compose -f monitoring/docker-compose.yml up -d` -- see '
               'monitoring/README.md\'s "Orchestration mode" section.')


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    plot_params = {
        'faults': [0],
        'nodes': [4],
        'workers': [1, 4, 7, 10],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [2_000, 2_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
