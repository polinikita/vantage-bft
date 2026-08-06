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
    ''' COST-ESTIMATE: append a timestamped cost-estimate block to
    `results/cost-log.txt` (created, with its directory, if missing) -- one
    block per `fab destroy` run, kept alongside the throughput/latency result
    files already written under `results/`. '''
    makedirs(PathMaker.results_path(), exist_ok=True)
    path = join(PathMaker.results_path(), 'cost-log.txt')
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    with open(path, 'a') as f:
        f.write(f'--- {timestamp} ---\n{result["formatted"]}\n\n')


@task
def create(ctx, nodes=6):
    ''' Create a testbed'''
    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed.

    COST-ESTIMATE: before terminating, computes + prints a deterministic AWS
    cost estimate (alive-time x price; no Cost Explorer/CloudTrail -- both
    denied for this IAM user; see `InstanceManager.estimate_cost`) and
    appends it to `results/cost-log.txt`. This MUST happen before
    `terminate_instances()`: LaunchTime (and Spot-vs-on-demand status) is
    only visible on instances that are still pending/running. '''
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
    ''' Print the current AWS cost estimate for the live testbed, without
    terminating anything -- the same computation `destroy` runs at teardown
    (see `InstanceManager.estimate_cost`), callable mid-run to check spend
    so far. Does not write to `results/cost-log.txt` (that log is one block
    per actual teardown, not a running mid-campaign log). '''
    try:
        result = InstanceManager.make().estimate_cost()
        Print.info(result['formatted'])
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
def install(ctx, source_build=False):
    ''' Install the codebase on all machines.

    Default: fetch-binary mode -- runtime deps only, `fab remote`/
    `fab campaign` download the pre-built nightly release. `--source-build`:
    old behavior -- full Rust toolchain + rsync the working tree, remote
    hosts compile from source on every `fab remote`/`fab campaign`. '''
    try:
        Bench(ctx, source_build=source_build).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, debug=True, protocol='autobahn-optimistic', all_to_all=False,
           batch_messages=True, batch_max_bytes=65536, batch_max_delay_ms=5,
           mimic_latency_ms=0, source_build=False):
    ''' Run benchmarks on AWS.

    Phase-7 smoke test: checked-in defaults below, except `rate` set to
    50,000 tx/s (conservative for an unknown/smaller instance size than
    prior AWS runs) and `delta_ms: 150` added to node_params (passed
    through NodeParameters/serde into Parameters.delta_ms). `protocol` is
    exposed as a fab CLI arg (`--protocol=vantage`) so the same task runs
    both the autobahn-optimistic and vantage smoke passes without editing
    this file between runs.

    `--source-build`: use the old rsync + remote `cargo build` deploy path
    instead of the default fetch-prebuilt-binary one (must match whatever
    `fab install` prepared the hosts for -- see `install`'s docstring).
    '''
    bench_params = {
        'faults': 0,
        'nodes': [4],
        'workers': 1,
        'co-locate': True,
        'rate': [50_000],
        'tx_size': 512,
        # METRICS-DASHBOARD-SPEC.md §8: 'random' is now the default transaction
        # mode everywhere (all_zero stays available). Guard/gate/sweep benchmarks
        # must override this back to 'all_zero' explicitly for comparability with
        # historical gate numbers (all of which are all_zero).
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
        # Autobahn (Giridharan et al., SOSP'24) §5.5.3: off by default, byte-identical
        # behavior when off; `fab remote --all-to-all` (or edit this literal) to enable.
        'all_to_all': all_to_all,
        # Transport-level per-peer outbound batching: on by default (5 ms / 64 KiB);
        # pass `fab remote --batch-messages=False` for an explicit unbatched run.
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
    ''' Distributed Vantage AWS throughput/latency campaign (PREP -- the
    coordinator performs the actual `fab create`/`fab remote`/`fab destroy`;
    this task only assembles + validates the config and hands it to
    `Bench(...).run`).

    `--source-build`: see `remote`'s docstring -- same toggle, same
    hosts-must-match-`fab install` caveat.

    `--max-header-delay` (ms) / `--batch-size` (bytes): CHANGE C -- expose the
    two node knobs behind the observed n=4 ~68k tx/s throughput ceiling
    (`max_header_delay=50ms` + `batch_size=500KB` ~= 976 tx/header) as
    campaign args, so a peak-finding rerun doesn't need a code edit. Defaults
    (50 ms / 500_000 B) are byte-identical to the prior hardcoded values.

    `--early-stop-margin` (fraction, default 0.10; 0 disables): CHANGE A --
    threaded into `bench_parameters.early_stop_margin`; see
    `config.BenchParameters` and `remote.Bench.run`'s rate loop for the
    peak-relative committed-TPS early-stop this drives.

    Target campaign:
      - nodes = [20], workers = 1, faults = 0, collocate (default)
      - tx_size = 512 B, tx_mode = 'all_zero' (comparability with gate numbers)
      - duration = 180 s per point, runs = 1
      - rate SWEEP ascending toward saturation:
          [50k, 100k, 150k, 200k, 250k] tx/s
      - protocol = vantage, delta_ms = 150
      - latency model (`--latency`), default 'aws': the real 10-AWS-region RTT
        matrix (`config::LatencyTable::aws_rtt`, ported VERBATIM from
        starfish) -- committee index i -> region i % 10. This is `node run`'s
        own DEFAULT whenever `mimic_latency_ms` is absent from
        parameters.json, so `--latency aws` simply omits that key rather than
        setting anything. All instances are pinned to a single AZ (see
        `instance.create_instances`), so this models real inter-region RTT
        between AWS regions while keeping intra-run bandwidth free.
        `--latency uniform` instead sets `mimic_latency_ms` (default 100 ms
        RTT = 50 ms one-way) as an EXPLICIT override applied uniformly to
        every inter-authority link -- the prior behavior. Either way `node
        run` expands the resulting knob into an NxN one-way latency_table at
        spawn via `Committee::latency_map` -- the SAME path both protocols
        already use for `node local-benchmark`'s `--latency-table`/
        `--mimic-latency-ms`. This is the only mechanism that carries latency
        on the distributed path (Parameters.latency_table is #[serde(skip)]
        and never travels through parameters.json).

    Prerequisite (coordinator): a settings.json sized for 20 instances (and, if
    desired, `"instances": { ..., "spot": true }` for Spot capacity), then
    `fab create --nodes 20`, `fab install`, `fab campaign`, `fab destroy`.
    '''
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
            'max_header_delay': int(max_header_delay),  # ms -- was 5_000 (Autobahn's
                                      # own default; PREP FIX 2). `node
                                      # local-benchmark`'s Vantage runs use 50 ms (its
                                      # --max-header-delay-ms CLI default, and this
                                      # arg's own default): at 5_000 ms almost every
                                      # header waited out the full 5 s timer instead
                                      # of firing on the ~50 ms cadence Vantage's
                                      # AGB/car mechanism expects, throttling
                                      # committed throughput independent of the
                                      # public/private IP fix (PREP FIX 1). Now a
                                      # `--max-header-delay` campaign arg (CHANGE C).
            'gc_depth': 50,  # rounds -- Autobahn's Core/garbage_collector only. This
                             # comment was WRONG for a while: VantageCore's internal
                             # GC used to read `gc_depth` as a count of VIEWS, so this
                             # single integer sized two unrelated windows in two
                             # different counter spaces. Vantage now has its own knob
                             # (`vantage_gc_window_views`, below) and this one is once
                             # again Autobahn-only.
            'vantage_gc_window_views': 50,  # views -- how much per-view Vantage state
                             # (AgbEngine/Frontier/ControlLog/Resolver) is retained
                             # behind the resolved prefix. Carrier bodies are held one
                             # further window back so a lagging peer can still fetch
                             # them. Sets how far a party may fall behind and still
                             # catch up, so do not shrink it without reading
                             # `ControlLog::SERVE_MARGIN_WINDOWS`.
            'sync_retry_delay': 5_000,  # ms -- Autobahn's Core only.
            'sync_retry_nodes': 3,  # number of nodes -- Autobahn's Core only.
            'batch_size': int(batch_size),  # bytes -- matches Parameters::default() /
                                     # local-benchmark's (unexposed) worker batch
                                     # size; not a Vantage throttle. Now a
                                     # `--batch-size` campaign arg (CHANGE C).
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
            'simulate_asynchrony': False,
            'asynchrony_start': 15_000,  # ms
            'asynchrony_duration': 3_000,  # ms
        }
        if latency == 'uniform':
            # EXPLICIT override: uniform WAN RTT (ms), node halves to one-way.
            node_params['mimic_latency_ms'] = int(mimic_latency_ms)
        # else ('aws'): leave 'mimic_latency_ms' absent from node_params entirely --
        # `node run` then defaults to the real 10-AWS-region RTT matrix (see docstring).
        Bench(ctx, source_build=source_build).run(bench_params, node_params, debug)
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
def fetch_metrics(ctx, start=None, end=None):
    ''' METRICS-COLLECTOR-PREP: pull the key metrics series (committed
    transactions, transaction-committed-latency, vantage_seals, network
    message/byte counters, submitted_transactions, utilization_timer,
    core_queue_length, protocol_info/transaction_mode_info, up, and the
    per-node bytes-sent/received/committed-tps breakdowns -- see
    `remote.COLLECTOR_QUERIES`) off the metrics-collector's Prometheus HTTP
    API into collector-metrics/*.json. Safe to run any time after `fab
    remote`/`fab campaign` has deployed monitoring and before `fab destroy`
    terminates the collector.

    `--start`/`--end` (unix epoch seconds, both or neither -- e.g. bounds
    read back from collector-metrics/run-windows.json, which `fab campaign`
    now writes automatically per rate point): pass both for a manual
    `query_range` re-fetch over that exact window instead of an instant
    query. Default (both omitted, unchanged prior behavior): an instant
    query -- Prometheus's last-known value per series, which goes stale
    (silently returns []) more than 5 minutes after the scraped nodes are
    gone, so this default is only meaningful called promptly after a run,
    before `fab destroy`. '''
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
