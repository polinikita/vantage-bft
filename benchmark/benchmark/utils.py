# Copyright(C) Facebook, Inc. and its affiliates.
from json import loads
from os.path import join
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen


class BenchError(Exception):
    def __init__(self, message, error):
        assert isinstance(error, Exception)
        self.message = message
        self.cause = error
        super().__init__(message)


class PathMaker:
    # NVMe-INSTANCE-STORE: throwaway-testbed hardcode -- the local instance-store
    # mount point every validator's install() (remote.py) formats + mounts (see
    # the NVMe-detect-and-mount snippet there). Rooting the RocksDB store here
    # instead of the EBS root volume was the fix for the c5.xlarge throughput
    # collapse (EBS-only, so store I/O both is slow AND competes with the
    # consensus NIC for the same network-attached bandwidth). settings.json's
    # validator `instance_type` is `c5d.xlarge` (same 4 vCPU/NIC as
    # `c5.xlarge`, plus exactly one local NVMe SSD), so `install()`'s
    # NVMe-detect step actually finds and formats that disk on every
    # validator -- this is the branch that runs in practice, not a dormant
    # fallback. This directory is a REQUIRED part of install() (not
    # best-effort): `_run_single` passes `--store` under this path
    # unconditionally, and RocksDB only creates the leaf `.db-*` directory,
    # never '/mnt/db' itself, so install() also chowns '/mnt/db' to the ssh
    # user and verifies it's writable, whether or not the NVMe mount above
    # succeeded (a plain directory on the EBS root, on a non-`*d` instance
    # type). The metrics-collector (plain `c5.xlarge`) never uses this at
    # all -- it runs no node/store.
    REMOTE_STORE_BASE = '/mnt/db'

    @staticmethod
    def binary_path():
        return join('..', 'target', 'release')

    @staticmethod
    def node_crate_path():
        return join('..', 'node')

    @staticmethod
    def committee_file():
        return '.committee.json'

    @staticmethod
    def parameters_file():
        return '.parameters.json'

    @staticmethod
    def key_file(i):
        assert isinstance(i, int) and i >= 0
        return f'.node-{i}.json'

    @staticmethod
    def db_path(i, j=None):
        assert isinstance(i, int) and i >= 0
        assert (isinstance(j, int) and i >= 0) or j is None
        worker_id = f'-{j}' if j is not None else ''
        return f'.db-{i}{worker_id}'

    @staticmethod
    def remote_db_path(i, j=None):
        ''' NVMe-INSTANCE-STORE: the remote (validator-only) RocksDB store
        path passed to `--store` -- `db_path`'s name, rooted under the
        NVMe instance-store mount (`REMOTE_STORE_BASE`) instead of the
        node's home directory (which lives on the slow, NIC-competing EBS
        root volume). No local/coordinator-side use: `db_path`'s bare name
        remains what it was for any such caller. '''
        return join(PathMaker.REMOTE_STORE_BASE, PathMaker.db_path(i, j))

    @staticmethod
    def logs_path():
        return 'logs'

    @staticmethod
    def primary_log_file(i):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(), f'primary-{i}.log')

    @staticmethod
    def worker_log_file(i, j):
        assert isinstance(i, int) and i >= 0
        assert isinstance(j, int) and i >= 0
        return join(PathMaker.logs_path(), f'worker-{i}-{j}.log')

    @staticmethod
    def client_log_file(i, j):
        assert isinstance(i, int) and i >= 0
        assert isinstance(j, int) and i >= 0
        return join(PathMaker.logs_path(), f'client-{i}-{j}.log')

    @staticmethod
    def metrics_primary_file(i):
        assert isinstance(i, int) and i >= 0
        return join(PathMaker.logs_path(), f'metrics-primary-{i}.txt')

    @staticmethod
    def metrics_worker_file(i, j):
        assert isinstance(i, int) and i >= 0
        assert isinstance(j, int) and i >= 0
        return join(PathMaker.logs_path(), f'metrics-worker-{i}-{j}.txt')

    @staticmethod
    def collector_prometheus_file():
        ''' METRICS-COLLECTOR-PREP: the generated scrape-config uploaded to the
        dedicated metrics-collector instance (local staging copy). '''
        return '.collector-prometheus.yml'

    @staticmethod
    def collector_metrics_path():
        ''' Top-level directory for metrics-collector JSON exports -- a
        SIBLING of `logs_path()`/`results_path()`, deliberately NOT nested
        under `logs/`: `CommandMaker.clean_logs()` (`rm -r logs`) runs once
        per `_run_single`, i.e. once per rate point of a rate sweep, and MUST
        NOT delete collector artifacts (or `run-windows.json`) written by an
        earlier rate point -- or an earlier campaign -- of the same coordinator
        session. '''
        return 'collector-metrics'

    @staticmethod
    def collector_metrics_dir(subdir=None):
        ''' `subdir`: optional path (may itself contain '/', e.g.
        f'{protocol}-{campaign_tag}/{n}nodes-{rate}rate' -- see remote.py's
        `Bench.run`) routing output to collector_metrics_path()/<subdir>/
        instead of the flat top-level directory, so neither a later rate
        point NOR a later campaign (different protocol/run against the same
        testbed) ever overwrites an earlier one's same-named series files.
        None (default) is the flat directory -- used by the standalone `fab
        fetch-metrics` task's ad hoc fetch, and by `_record_run_window`'s
        single run-windows.json that intentionally spans every campaign in
        the session (differentiated internally by its own protocol/nodes/rate
        fields, not by directory). '''
        base = PathMaker.collector_metrics_path()
        return join(base, subdir) if subdir else base

    @staticmethod
    def collector_metrics_file(name, subdir=None):
        assert isinstance(name, str)
        return join(PathMaker.collector_metrics_dir(subdir), f'{name}.json')

    @staticmethod
    def results_path():
        return 'results'

    @staticmethod
    def result_file(faults, nodes, workers, collocate, rate, tx_size):
        return join(
            PathMaker.results_path(),
            f'bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx_size}.txt'
        )

    @staticmethod
    def plots_path():
        return 'plots'

    @staticmethod
    def agg_file(type, faults, nodes, workers, collocate, rate, tx_size, max_latency=None):
        if max_latency is None:
            name = f'{type}-bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx_size}.txt'
        else:
            name = f'{type}-{max_latency}-bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx_size}.txt'
        return join(PathMaker.plots_path(), name)

    @staticmethod
    def plot_file(name, ext):
        return join(PathMaker.plots_path(), f'{name}.{ext}')


def scrape_metrics(address, filename, timeout=5):
    ''' Scrape a node's Prometheus /metrics endpoint (PHASE2-SPEC.md #5) and save the
    raw text-exposition body to `filename`, so results stay re-analyzable offline.
    Best-effort: a scrape failure (node down, port unreachable) prints a warning and
    writes nothing rather than raising, so one bad node doesn't abort the whole run. '''
    assert isinstance(address, str)
    assert isinstance(filename, str)
    url = f'http://{address}/metrics'
    try:
        with urlopen(url, timeout=timeout) as response:
            body = response.read().decode('utf-8')
    except (URLError, OSError) as e:
        Print.warn(f'Failed to scrape metrics from {url}: {e}')
        return
    with open(filename, 'w') as f:
        f.write(body)


def prometheus_query(base_url, promql, start=None, end=None, step='1s', timeout=10):
    ''' METRICS-COLLECTOR-PREP: query the metrics-collector's Prometheus HTTP API
    and return the parsed JSON response (same stdlib-urllib, best-effort-by-
    caller style as `scrape_metrics` above -- no `requests` dependency for a
    handful of coordinator-side reads). Instant `query` when both `start`/`end`
    are None (the default); `query_range` (stepped, over [start, end]) when
    both are given -- `start`/`end` are unix timestamps (seconds). Raises
    URLError/OSError on failure/timeout; callers decide whether that's fatal. '''
    assert isinstance(base_url, str)
    assert isinstance(promql, str)
    if start is None and end is None:
        url = f'{base_url}/api/v1/query?{urlencode({"query": promql})}'
    else:
        assert start is not None and end is not None
        params = {'query': promql, 'start': start, 'end': end, 'step': step}
        url = f'{base_url}/api/v1/query_range?{urlencode(params)}'
    with urlopen(url, timeout=timeout) as response:
        return loads(response.read().decode('utf-8'))


class Color:
    HEADER = '\033[95m'
    OK_BLUE = '\033[94m'
    OK_GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Print:
    @staticmethod
    def heading(message):
        assert isinstance(message, str)
        print(f'{Color.OK_GREEN}{message}{Color.END}')

    @staticmethod
    def info(message):
        assert isinstance(message, str)
        print(message)

    @staticmethod
    def warn(message):
        assert isinstance(message, str)
        print(f'{Color.BOLD}{Color.WARNING}WARN{Color.END}: {message}')

    @staticmethod
    def error(e):
        assert isinstance(e, BenchError)
        print(f'\n{Color.BOLD}{Color.FAIL}ERROR{Color.END}: {e}\n')
        causes, current_cause = [], e.cause
        while isinstance(current_cause, BenchError):
            causes += [f'  {len(causes)}: {e.cause}\n']
            current_cause = current_cause.cause
        causes += [f'  {len(causes)}: {type(current_cause)}\n']
        causes += [f'  {len(causes)}: {current_cause}\n']
        print(f'Caused by: \n{"".join(causes)}\n')


def progress_bar(iterable, prefix='', suffix='', decimals=1, length=30, fill='█', print_end='\r'):
    total = len(iterable)

    def printProgressBar(iteration):
        formatter = '{0:.'+str(decimals)+'f}'
        percent = formatter.format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)

    printProgressBar(0)
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)
    print()