# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime, timezone
from os import makedirs
from os.path import join

from fabric import task

from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print, PathMaker
from benchmark.plot import Ploter, PlotError
from benchmark.instance import InstanceManager
from benchmark.remote import Bench, BenchError


def _log_cost(result):
    '''Append a timestamped cost estimate to `results/cost-log.txt`.'''
    makedirs(PathMaker.results_path(), exist_ok=True)
    path = join(PathMaker.results_path(), 'cost-log.txt')
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    with open(path, 'a') as f:
        f.write(f'--- {timestamp} ---\n{result["formatted"]}\n\n')


@task
def create(ctx, nodes=6):
    '''Create an AWS testbed.'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    '''Estimate current cost, record it, and destroy the testbed.'''
    try:
        manager = InstanceManager.make()
        result = manager.estimate_cost()
        Print.info(result['formatted'])
        _log_cost(result)
        manager.terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def cost(ctx):
    '''Print the current AWS cost estimate without changing the testbed.'''
    try:
        result = InstanceManager.make().estimate_cost()
        Print.info(result['formatted'])
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    '''Start at most `max` machines per data center.'''
    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    '''Stop all machines.'''
    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    '''Display connection information for available machines.'''
    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx, source_build=False):
    '''Install runtime dependencies; `--source-build` builds uploaded source.'''
    try:
        Bench(ctx, source_build=source_build).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=True, protocol='autobahn-optimistic', all_to_all=False,
           batch_messages=True, batch_max_bytes=65536, batch_max_delay_ms=5,
           mimic_latency_ms=0, source_build=False):
    '''Run a benchmark on AWS with the selected protocol and transport options.'''
    bench_params = {
        'faults': 0,
        'nodes': [4],
        'workers': 1,
        'co-locate': True,
        'rate': [50_000],
        'tx_size': 512,
        # Use random transaction payloads.
        'tx_mode': 'random',
        'duration': 60,
        'runs': 1,

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
        'delta_ms': 150,  # ms, Vantage AGB/control-log delay
        # `node run` interprets positive RTT as one-way link latency.
        'mimic_latency_ms': int(mimic_latency_ms),
        # `--all-to-all` enables direct communication between every pair.
        'all_to_all': all_to_all,
        # Enable per-peer transport batching by default.
        'batch_messages': batch_messages,
        'batch_max_bytes': batch_max_bytes,
        'batch_max_delay_ms': batch_max_delay_ms,

        'simulate_asynchrony': False,
        'asynchrony_start': 15_000, #ms
        'asynchrony_duration': 3_000, #ms
    }
    try:
        Bench(ctx, source_build=source_build).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def campaign(ctx, debug=False, protocol='vantage', latency='aws', mimic_latency_ms=100,
             nodes=20, duration=180, rates='50000,100000,150000,200000,250000',
             max_header_delay=50, batch_size=500_000, early_stop_margin=0.10,
             source_build=False):
    '''Run an AWS throughput and latency sweep; rates are tx/s, duration is seconds per rate, and latency selects AWS or uniform RTT.'''
    try:
        if latency not in ('aws', 'uniform'):
            raise BenchError(
                f"--latency must be 'aws' or 'uniform', got '{latency}'",
                ValueError(latency),
            )
        bench_params = {
            'faults': 0,
            'nodes': [int(nodes)],
            'workers': 1,
            'collocate': True,
            'rate': [int(r) for r in str(rates).split(',') if r.strip()],
            'tx_size': 512,
            'tx_mode': 'all_zero',
            'duration': int(duration),
            'runs': 1,
            'early_stop_margin': float(early_stop_margin),

            # Disable partition simulation.
            'simulate_partition': False,
            'partition_start': 0,
            'partition_duration': 0,
            'partition_nodes': 0,
        }
        node_params = {
            'timeout_delay': 5_000,  # ms, Autobahn core only
            'header_size': 32,  # bytes
            'max_header_delay': int(max_header_delay),  # ms
            'gc_depth': 50,  # rounds, Autobahn core only
            'vantage_gc_window_views': 50,  # retained Vantage views
            'sync_retry_delay': 5_000,  # ms -- Autobahn's Core only.
            'sync_retry_nodes': 3,  # number of nodes -- Autobahn's Core only.
            'batch_size': int(batch_size),  # bytes
            'max_batch_delay': 20,  # ms
            'protocol': protocol,
            'use_parallel_proposals': True,
            'k': 4,
            'use_fast_path': True,
            'fast_path_timeout': 5_000,
            'use_ride_share': False,
            'car_timeout': 5_000,
            'delta_ms': 150,  # ms, Vantage AGB/control-log delay
            'simulate_asynchrony': False,
            'asynchrony_start': 15_000,  # ms
            'asynchrony_duration': 3_000,  # ms
        }
        if latency == 'uniform':
            # Convert RTT to one-way latency.
            node_params['mimic_latency_ms'] = int(mimic_latency_ms)
        # AWS latency uses the default regional RTT matrix.
        Bench(ctx, source_build=source_build).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def monitor(ctx):
    '''Generate Prometheus configuration for the deployed AWS validators.'''
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
def monitor_collector(ctx):
    '''Deploy Prometheus on the metrics collector from `.committee.json`.'''
    from json import load
    from benchmark.utils import PathMaker

    try:
        with open(PathMaker.committee_file(), 'r') as f:
            committee_json = load(f)
    except (OSError, IOError) as e:
        Print.error(BenchError(
            'Failed to read committee file (run `fab remote` at least once first)', e
        ))
        return

    try:
        Bench(ctx).deploy_monitoring(committee_json)
    except BenchError as e:
        Print.error(e)


@task
def fetch_metrics(ctx, start=None, end=None):
    '''Fetch collector metrics; pass start and end as Unix seconds for a range query.'''
    try:
        if (start is None) != (end is None):
            raise BenchError(
                '--start and --end must be given together, or both omitted',
                ValueError(f'start={start!r} end={end!r}'),
            )
        Bench(ctx).fetch_collector_metrics(
            start=float(start) if start is not None else None,
            end=float(end) if end is not None else None,
        )
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    '''Plot performance from `fab remote` logs.'''
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
    '''Stop all benchmark processes.'''
    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    '''Print a summary of benchmark logs.'''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))
