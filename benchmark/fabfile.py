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
def remote(ctx, debug=True, protocol='autobahn-optimistic', compress_network=False, all_to_all=False,
           batch_messages=False, batch_max_bytes=65536, batch_max_delay_ms=5,
           mimic_latency_ms=0):
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
        # DEPLOYABLE uniform RTT (ms) mimic latency; 0 (default) = off, byte-identical
        # to prior behavior. `node run` expands >0 into a uniform NxN one-way (RTT/2)
        # latency_table at spawn -- the only way to inject WAN-shaped latency on the
        # distributed path (Parameters.latency_table is #[serde(skip)]).
        'mimic_latency_ms': int(mimic_latency_ms),
        # METRICS-DASHBOARD-SPEC.md §8: off by default, byte-identical framing when
        # off; `fab remote --compress-network` (or edit this literal) to enable.
        'compress_network': compress_network,
        # Autobahn (Giridharan et al., SOSP'24) §5.5.3: off by default, byte-identical
        # behavior when off; `fab remote --all-to-all` (or edit this literal) to enable.
        'all_to_all': all_to_all,
        # Transport-level per-peer outbound batching: off by default, byte-identical
        # wire/behavior when off; `fab remote --batch-messages` to enable.
        'batch_messages': batch_messages,
        'batch_max_bytes': batch_max_bytes,
        'batch_max_delay_ms': batch_max_delay_ms,

        'simulate_asynchrony': False,
        'asynchrony_start': 15_000, #ms
        'asynchrony_duration': 3_000, #ms
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def campaign(ctx, debug=False, protocol='vantage', mimic_latency_ms=100,
             nodes=20, duration=180, rates='50000,100000,150000,200000,250000'):
    ''' Distributed Vantage AWS throughput/latency campaign (PREP -- the
    coordinator performs the actual `fab create`/`fab remote`/`fab destroy`;
    this task only assembles + validates the config and hands it to
    `Bench(...).run`).

    Target campaign:
      - nodes = [20], workers = 1, faults = 0, collocate (default)
      - tx_size = 512 B, tx_mode = 'all-zero' (comparability with gate numbers)
      - duration = 180 s per point, runs = 1
      - rate SWEEP ascending toward saturation:
          [50k, 100k, 150k, 200k, 250k] tx/s
      - protocol = vantage, delta_ms = 150
      - UNIFORM one-region WAN mimic latency: `mimic_latency_ms` RTT (default
        100 ms RTT = 50 ms one-way) injected on every inter-authority link even
        though the 20 instances are co-located. `node run` expands this scalar
        into a uniform 20x20 one-way (RTT/2) latency_table at spawn via
        `Committee::latency_map` -- the SAME path both protocols already use for
        `node local-benchmark --mimic-latency-ms`. This scalar is the only knob
        that carries latency on the distributed path (Parameters.latency_table
        is #[serde(skip)] and never travels through parameters.json).

    Prerequisite (coordinator): a settings.json sized for 20 instances (and, if
    desired, `"instances": { ..., "spot": true }` for Spot capacity), then
    `fab create --nodes 20`, `fab install`, `fab campaign`, `fab destroy`.
    '''
    bench_params = {
        'faults': 0,
        'nodes': [int(nodes)],
        'workers': 1,
        'collocate': True,
        'rate': [int(r) for r in str(rates).split(',') if r.strip()],
        'tx_size': 512,
        'tx_mode': 'all-zero',
        'duration': int(duration),
        'runs': 1,

        # Partition simulation unused for this campaign.
        'simulate_partition': False,
        'partition_start': 0,
        'partition_duration': 0,
        'partition_nodes': 0,
    }
    node_params = {
        'timeout_delay': 5_000,  # ms -- Autobahn's Core only (VantageCore doesn't
                                  # read it, see primary/src/vantage/); left alone.
        'header_size': 32,  # bytes -- ~1 digest (32 B): a header/car fires as
                             # soon as ANY digest is ready, gated only by
                             # max_header_delay below. Already Vantage-appropriate
                             # (fast/frequent cars), unlike max_header_delay was.
        'max_header_delay': 50,  # ms -- was 5_000 (Autobahn's own default; PREP
                                  # FIX 2). `node local-benchmark`'s Vantage runs
                                  # use 50 ms (its --max-header-delay-ms CLI
                                  # default): at 5_000 ms almost every header
                                  # waited out the full 5 s timer instead of firing
                                  # on the ~50 ms cadence Vantage's AGB/car
                                  # mechanism expects, throttling committed
                                  # throughput independent of the public/private
                                  # IP fix (PREP FIX 1).
        'gc_depth': 50,  # rounds -- Autobahn's Core/garbage_collector only.
        'sync_retry_delay': 5_000,  # ms -- Autobahn's Core only.
        'sync_retry_nodes': 3,  # number of nodes -- Autobahn's Core only.
        'batch_size': 500_000,  # bytes -- matches Parameters::default() /
                                 # local-benchmark's (unexposed) worker batch
                                 # size; not a Vantage throttle.
        'max_batch_delay': 20,  # ms -- matches local-benchmark's own
                                 # --max-batch-delay-ms CLI default; unchanged.
        'protocol': protocol,
        'use_parallel_proposals': True,
        'k': 4,
        'use_fast_path': True,
        'fast_path_timeout': 5_000,
        'use_ride_share': False,
        'car_timeout': 5_000,
        'delta_ms': 150,  # ms -- Vantage AGB/control-log base delay unit
        'mimic_latency_ms': int(mimic_latency_ms),  # uniform WAN RTT (ms); node halves to one-way
        'simulate_asynchrony': False,
        'asynchrony_start': 15_000,  # ms
        'asynchrony_duration': 3_000,  # ms
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
def monitor_collector(ctx):
    ''' METRICS-COLLECTOR-PREP: (re)deploy Prometheus on the dedicated
    metrics-collector instance, reading the last run's .committee.json for
    scrape targets (every validator's primary+worker metrics endpoint, over
    its PRIVATE VPC ip). `Bench.run()` already calls this automatically right
    after `_config()`; this standalone task is for redeploying without a full
    `fab remote`/`fab campaign` (e.g. after manually editing .committee.json,
    or recovering a collector that failed to come up). Requires `fab create`
    (with the collector) and at least one prior `fab remote`/`fab campaign` to
    have written .committee.json. '''
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
def fetch_metrics(ctx):
    ''' METRICS-COLLECTOR-PREP: pull the key metrics series (committed
    transactions, transaction-committed-latency, vantage_seals, network
    message/byte counters, submitted_transactions, utilization_timer,
    core_queue_length, protocol_info/transaction_mode_info -- see
    `remote.COLLECTOR_QUERIES`) off the metrics-collector's Prometheus HTTP
    API into logs/collector/*.json. Safe to run any time after `fab remote`/
    `fab campaign` has deployed monitoring and before `fab destroy` terminates
    the collector. '''
    try:
        Bench(ctx).fetch_collector_metrics()
    except BenchError as e:
        Print.error(e)


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
