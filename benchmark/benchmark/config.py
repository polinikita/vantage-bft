# Copyright(C) Facebook, Inc. and its affiliates.
from json import dump, load
from collections import OrderedDict


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    ''' The committee looks as follows:
        "authorities: {
            "name": {
                "stake": 1,
                "consensus: {
                    "consensus_to_consensus": x.x.x.x:x,
                },
                "primary: {
                    "primary_to_primary": x.x.x.x:x,
                    "worker_to_primary": x.x.x.x:x,
                    "metrics": x.x.x.x:x,
                },
                "workers": {
                    "0": {
                        "primary_to_worker": x.x.x.x:x,
                        "worker_to_worker": x.x.x.x:x,
                        "transactions": x.x.x.x:x,
                        "metrics": x.x.x.x:x
                    },
                    ...
                }
            },
            ...
        }

        Phase 2: each primary and each worker gains a "metrics" (Prometheus scrape)
        address, growing the per-authority port count from 6 to 8 (1 worker/authority,
        `fab remote`'s default). committee.json is regenerated every run, so there
        is no back-compat concern with older committee files.
    '''

    def __init__(self, addresses, base_port, public_hosts=None):
        ''' The `addresses` field looks as follows:
            {
                "name": ["host", "host", ...],
                ...
            }
            These MUST be the PRIVATE (VPC-internal) IPs when this committee
            is for a real (multi-instance) deployment: they become the
            node<->node and client<->node wire addresses every authority
            reads out of committee.json, and same-region traffic over public
            IPs is billed cross-instance data transfer and collapses
            throughput (routes through the internet edge instead of the
            VPC).

            `public_hosts` (optional): same shape/order as `addresses` was
            *before* this constructor consumes it (index 0 = the primary's
            physical host, 1.. = each worker's, one entry per authority) --
            the PUBLIC (internet-routable) IP of the instance that authority
            actually runs on. This is the orchestrator's own SSH/rsync/tmux
            connection-target bookkeeping (`public_ips()` and friends below);
            it is never written to committee.json and never read by a node
            binary. Omit it when `addresses` already IS the connection
            target (e.g. a purely local run with no public/private
            distinction) -- `public_ips()` then falls back to `ips()`.
        '''
        assert isinstance(addresses, OrderedDict)
        assert all(isinstance(x, str) for x in addresses.keys())
        assert all(
            isinstance(x, list) and len(x) > 1 for x in addresses.values()
        )
        assert all(
            isinstance(x, str) for y in addresses.values() for x in y
        )
        assert len({len(x) for x in addresses.values()}) == 1
        assert isinstance(base_port, int) and base_port > 1024
        if public_hosts is not None:
            assert isinstance(public_hosts, OrderedDict)
            assert list(public_hosts.keys()) == list(addresses.keys())
            assert all(
                len(public_hosts[name]) == len(addresses[name])
                for name in addresses
            )

        port = base_port
        self.json = {'authorities': OrderedDict()}
        self.public_hosts = public_hosts

        for name, hosts in addresses.items():
            # `metrics` is never dialed by a peer node -- only scraped by an
            # external observer (this orchestrator's own `scrape_metrics()`,
            # or a real Prometheus pointed at `fab monitor`'s generated
            # prometheus-remote.yaml, which is explicitly built from public
            # IPs -- see fabfile.py's `monitor` task). The node itself always
            # binds its metrics server on 0.0.0.0 regardless of the address
            # text here (see primary::Primary::spawn), so this field is free
            # to be the PUBLIC ip while every peer-dialed field below (
            # consensus/primary/worker addresses) is the PRIVATE one. Use a
            # local, poppable copy so `self.public_hosts` (consumed by the
            # public_ip accessors below) is left intact.
            pub_hosts = list(public_hosts[name]) if public_hosts is not None else None

            host = hosts.pop(0)
            pub_host = pub_hosts.pop(0) if pub_hosts is not None else host
            consensus_addr = {
                'consensus_to_consensus': f'{host}:{port}',
            }
            port += 1

            primary_addr = {
                'primary_to_primary': f'{host}:{port}',
                'worker_to_primary': f'{host}:{port + 1}',
                'metrics': f'{pub_host}:{port + 2}',
            }
            port += 3

            workers_addr = OrderedDict()
            for j, host in enumerate(hosts):
                pub_worker_host = pub_hosts[j] if pub_hosts is not None else host
                workers_addr[j] = {
                    'primary_to_worker': f'{host}:{port}',
                    'transactions': f'{host}:{port + 1}',
                    'worker_to_worker': f'{host}:{port + 2}',
                    'metrics': f'{pub_worker_host}:{port + 3}',
                }
                port += 4

            self.json['authorities'][name] = {
                'stake': 1,
                'consensus': consensus_addr,
                'primary': primary_addr,
                'workers': workers_addr
            }

    def primary_addresses(self, faults=0):
        ''' Returns an ordered list of primaries' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['primary_to_primary']]
        return addresses

    def workers_addresses(self, faults=0):
        ''' Returns an ordered list of list of workers' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            authority_addresses = []
            for id, worker in authority['workers'].items():
                authority_addresses += [(id, worker['transactions'])]
            addresses.append(authority_addresses)
        return addresses

    def primary_metrics_addresses(self, faults=0):
        ''' Returns an ordered list of primaries' Prometheus metrics addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['metrics']]
        return addresses

    def workers_metrics_addresses(self, faults=0):
        ''' Returns an ordered list of list of (id, metrics address) per authority's
        workers. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            authority_addresses = []
            for id, worker in authority['workers'].items():
                authority_addresses += [(id, worker['metrics'])]
            addresses.append(authority_addresses)
        return addresses

    def primary_public_ip(self, name):
        ''' The physical (public) host running authority `name`'s primary --
        the SSH/rsync/tmux connection target, as opposed to the (private)
        wire address `primary_addresses()` returns. Falls back to the
        private address when this committee has no public/private
        distinction (`public_hosts` unset at construction). '''
        if self.public_hosts is None:
            return self.ip(
                self.json['authorities'][name]['primary']['primary_to_primary']
            )
        return self.public_hosts[name][0]

    def worker_public_ip(self, name, worker_id):
        ''' The physical (public) host running authority `name`'s worker
        `worker_id`. See `primary_public_ip`. '''
        if self.public_hosts is None:
            return self.ip(
                self.json['authorities'][name]['workers'][worker_id]['transactions']
            )
        return self.public_hosts[name][worker_id + 1]

    def primary_public_ips(self, faults=0):
        ''' Public-host mirror of `primary_addresses()`: same order/slicing,
        one entry per (non-faulty) authority. '''
        assert faults < self.size()
        good_nodes = self.size() - faults
        names = list(self.json['authorities'].keys())[:good_nodes]
        return [self.primary_public_ip(name) for name in names]

    def workers_public_ips(self, faults=0):
        ''' Public-host mirror of `workers_addresses()`: same order/shape,
        i.e. a list (per non-faulty authority) of list of
        (worker_id, public_ip). '''
        assert faults < self.size()
        good_nodes = self.size() - faults
        result = []
        for name, authority in list(self.json['authorities'].items())[:good_nodes]:
            result.append([
                (wid, self.worker_public_ip(name, wid))
                for wid in authority['workers']
            ])
        return result

    def public_ips(self, name=None):
        ''' Returns all the physical (public) host ip(s) associated with an
        authority (in any order) -- the SSH/rsync/tmux connection targets,
        as opposed to `ips()` which returns the (private) committee/wire
        addresses. Falls back to `ips()` when this committee has no
        public/private distinction (`public_hosts` unset at construction). '''
        if self.public_hosts is None:
            return self.ips(name)

        # Scope to the LIVE authorities (self.json), not all of
        # self.public_hosts -- remove_nodes() trims the former (e.g. per
        # `committee_copy` in remote.py's `run()`, one per swept node count)
        # but leaves the latter untouched, so using public_hosts' own keys
        # here would resurrect already-removed authorities' hosts.
        names = [name] if name is not None else list(self.json['authorities'].keys())
        ips = set()
        for n in names:
            ips.update(self.public_hosts[n])
        return list(ips)

    def ips(self, name=None):
        ''' Returns all the ips associated with an authority (in any order). '''
        if name is None:
            names = list(self.json['authorities'].keys())
        else:
            names = [name]

        ips = set()
        for name in names:
            addresses = self.json['authorities'][name]['consensus']
            ips.add(self.ip(addresses['consensus_to_consensus']))

            addresses = self.json['authorities'][name]['primary']
            ips.add(self.ip(addresses['primary_to_primary']))
            ips.add(self.ip(addresses['worker_to_primary']))
            ips.add(self.ip(addresses['metrics']))

            for worker in self.json['authorities'][name]['workers'].values():
                ips.add(self.ip(worker['primary_to_worker']))
                ips.add(self.ip(worker['worker_to_worker']))
                ips.add(self.ip(worker['transactions']))
                ips.add(self.ip(worker['metrics']))

        return list(ips)

    def remove_nodes(self, nodes):
        ''' remove the `nodes` last nodes from the committee. '''
        assert nodes < self.size()
        for _ in range(nodes):
            self.json['authorities'].popitem()

    def size(self):
        ''' Returns the number of authorities. '''
        return len(self.json['authorities'])

    def workers(self):
        ''' Returns the total number of workers (all authorities altogether). '''
        return sum(len(x['workers']) for x in self.json['authorities'].values())

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    @staticmethod
    def ip(address):
        assert isinstance(address, str)
        return address.split(':')[0]


def generate_collector_scrape_config(
    committee_json, faults=0, scrape_interval='1s', job_name='vantage-collector'
):
    ''' METRICS-COLLECTOR-PREP: Prometheus scrape-config YAML for the dedicated
    metrics-collector instance -- one target per (non-faulty) authority's
    primary metrics endpoint and one per worker, all on the PRIVATE (VPC) ip.

    Operates on the RAW committee dict (`{'authorities': {name: {...}}}` --
    exactly `.committee.json`'s shape, and exactly what a live `Committee`
    exposes as `committee.json`), not a `Committee` object, so the same
    function drives both `remote.py`'s `Bench.deploy_monitoring` (called with
    the live committee right after `_config()`) and a standalone
    `fab monitor-collector` re-deploy (called with `.committee.json` loaded
    straight off disk, `fab monitor`'s own read-only pattern) -- and is
    directly unit-testable against a hand-built dict, no AWS/live Committee
    required.

    The committee only ever stores the PUBLIC ip in each 'metrics' field (see
    `Committee.__init__`'s docstring: metrics is scraped by an external
    observer, never dialed by a peer, so it's free to be the address the
    *coordinator laptop* can reach). The dedicated collector instead lives
    inside the VPC and must scrape over the PRIVATE ip (same
    cross-instance-billing/throughput reasoning as every peer-dialed
    committee field) -- so this derives the private host from the
    'primary_to_primary'/'primary_to_worker' fields (always the private ip)
    and keeps the port from 'metrics' (primary: port+2 of
    'primary_to_primary'; worker: port+3 of 'primary_to_worker' -- see
    `Committee.__init__`), rather than reading 'metrics' directly.

    Every target carries TWO labels, not one: `node` (per-PROCESS --
    '<name[:8]>-primary' or '<name[:8]>-worker-<id>', one series per
    primary/worker) and `host` (per-INSTANCE/NIC -- the same private ip this
    target's own address is built from, below). They are not interchangeable
    for aggregation: under the campaign's `collocate: True`, an authority's
    primary and its worker run as two PROCESSES on the SAME instance sharing
    ONE NIC, so `sum by (node) (...)` yields (up to) twice as many series as
    there are physical hosts, and a max/peak taken over `node` silently
    reports one process's share of the NIC's traffic instead of the
    instance's actual total. `sum by (host) (...)` collapses a collocated
    primary+worker pair onto the one series that actually corresponds to
    their shared NIC, and is therefore the ONLY one of the two labels valid
    to compare against a per-NIC bandwidth limit (see remote.py's
    `Bench._report_nic_peak`, and `remote.COLLECTOR_QUERIES`'s
    `bytes_sent_rate_by_host`/`bytes_received_rate_by_host`). `node` remains
    useful on its own for per-process (not per-NIC) breakdowns. '''
    assert faults >= 0
    authorities = committee_json['authorities']
    names = list(authorities.keys())
    assert faults < len(names)
    names = names[:len(names) - faults]

    targets = []
    for name in names:
        authority = authorities[name]
        primary_host = Committee.ip(authority['primary']['primary_to_primary'])
        primary_port = authority['primary']['metrics'].split(':')[1]
        targets.append((f'{name[:8]}-primary', primary_host, f'{primary_host}:{primary_port}'))
        for wid, worker in authority['workers'].items():
            worker_host = Committee.ip(worker['primary_to_worker'])
            worker_port = worker['metrics'].split(':')[1]
            targets.append((f'{name[:8]}-worker-{wid}', worker_host, f'{worker_host}:{worker_port}'))

    lines = [
        'global:',
        f'  scrape_interval: {scrape_interval}',
        'scrape_configs:',
        f"  - job_name: '{job_name}'",
        '    static_configs:',
    ]
    for label, host, addr in targets:
        lines.append(f"      - targets: ['{addr}']")
        lines.append('        labels:')
        lines.append(f"          node: '{label}'")
        lines.append(f"          host: '{host}'")
    return '\n'.join(lines) + '\n'


class NodeParameters:
    def __init__(self, json):
        inputs = []
        try:
            inputs += [json['timeout_delay']]
            inputs += [json['header_size']]
            inputs += [json['max_header_delay']]
            inputs += [json['gc_depth']]
            inputs += [json['sync_retry_delay']]
            inputs += [json['sync_retry_nodes']]
            inputs += [json['batch_size']]
            inputs += [json['max_batch_delay']]
        except KeyError as e:
            raise ConfigError(f'Malformed parameters: missing key {e}')

        if not all(isinstance(x, int) for x in inputs):
            raise ConfigError('Invalid parameters type')

        # Vantage / distributed WAN-mimic knobs. These are NOT required (the Rust
        # side supplies serde defaults for every one of them), but when present they
        # must be well typed -- the whole `json` dict is written verbatim into
        # parameters.json and deserialized by `config::Parameters` on each node, so a
        # typo here would only surface as a node-side parse error mid-deploy.
        #  - `protocol`: selects the node assembly; "vantage" runs Vantage (serde
        #    kebab-case of `Protocol::Vantage`), else one of the two autobahn labels.
        #  - `delta_ms`: Vantage AGB base delay unit (ms).
        #  - `mimic_latency_ms`: DEPLOYABLE uniform RTT (ms) mimic latency, an EXPLICIT
        #    OVERRIDE. `node run` expands it into a uniform NxN one-way (RTT/2)
        #    latency_table at spawn. When this key is ABSENT (the campaign's default
        #    `--latency aws`), `node run` instead defaults to the real 10-AWS-region
        #    RTT matrix (`config::LatencyTable::aws_rtt`, ported from starfish). This
        #    is the ONLY way to inject latency on the distributed path, since
        #    `Parameters.latency_table` itself is `#[serde(skip)]` and thus never
        #    travels through parameters.json.
        if 'protocol' in json and json['protocol'] not in (
            'autobahn-optimistic', 'autobahn-seamless', 'vantage'
        ):
            raise ConfigError(f"Invalid protocol '{json['protocol']}'")
        for key in ('delta_ms', 'mimic_latency_ms'):
            if key in json:
                v = json[key]
                if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                    raise ConfigError(f"'{key}' must be a non-negative integer")

        #  - `vantage_gc_window_views`: how many VIEWS of per-view internal state
        #    VantageCore retains behind its resolved prefix before pruning. Distinct
        #    from `gc_depth` below, which is a depth in Autobahn ROUNDS -- the Vantage
        #    GC originally reused `gc_depth`, so tuning Autobahn's knob silently
        #    resized Vantage's retention window. Must be >= 1 (a window of 0 puts the
        #    GC floor at the resolved watermark itself); the node clamps it too, but
        #    reject it here so the misconfiguration surfaces before deploying.
        if 'vantage_gc_window_views' in json:
            v = json['vantage_gc_window_views']
            if not isinstance(v, int) or isinstance(v, bool) or v < 1:
                raise ConfigError(
                    "'vantage_gc_window_views' must be an integer >= 1"
                )

        self.json = json

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)


class BenchParameters:
    def __init__(self, json):
        try:
            self.faults = int(json['faults'])

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes or any(x <= 1 for x in nodes):
                raise ConfigError('Missing or invalid number of nodes')
            self.nodes = [int(x) for x in nodes]

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')
            # Normalise to ASCENDING order here rather than trusting the
            # caller: remote.py's `run()` peak-relative early-stop (CHANGE A,
            # see BenchParameters.early_stop_margin below) walks self.rate in
            # list order and `break`s the FIRST point whose TPS falls too far
            # below the running peak, printing "stopping sweep (remaining
            # higher rates skipped)" -- both the early-stop logic and that
            # message assume the list is already low-to-high (matching
            # fabfile.py's own "rate SWEEP ascending toward saturation"
            # docstring). An out-of-order --rates (e.g.
            # 50000,250000,100000) would otherwise let a spurious dip at
            # 250000 `break` the sweep and silently skip the still-unrun
            # (and perfectly valid, lower) 100000 point, while claiming to
            # have skipped only "remaining higher rates".
            self.rate = sorted(int(x) for x in rate)

            self.workers = int(json['workers'])

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            # Additive: pre-Phase-2 bench params without 'tx_mode' keep the
            # upstream-equivalent all-zero payload.
            self.tx_mode = str(json['tx_mode']) if 'tx_mode' in json else 'all-zero'
            if self.tx_mode not in ('all-zero', 'random'):
                raise ConfigError(
                    f"Invalid tx_mode '{self.tx_mode}': expected 'all-zero' or 'random'"
                )

            self.duration = int(json['duration'])

            self.runs = int(json['runs']) if 'runs' in json else 1
            self.simulate_partition = bool(json['simulate_partition'])

            self.partition_nodes = int(json['partition_nodes'])
            self.partition_start = int(json['partition_start'])
            self.partition_duration = int(json['partition_duration'])

            # CHANGE A (rate-sweep early stop): fraction the running PEAK
            # committed TPS is allowed to drop by before `remote.py`'s rate
            # loop stops sweeping to higher rates (saturation reached).
            # Peak-relative (not previous-point-relative), so a single noisy
            # point that dips below its *immediate predecessor* without
            # actually falling off the true peak does not truncate the
            # sweep early -- see remote.py's `run()` for the comparison
            # itself. 0.10 (10%) default; 0 disables early-stop entirely
            # (the sweep always runs every configured rate, prior behavior).
            self.early_stop_margin = (
                float(json['early_stop_margin'])
                if 'early_stop_margin' in json else 0.10
            )
            if self.early_stop_margin < 0:
                raise ConfigError('early_stop_margin must be non-negative')

            # Opt-out for remote.py's `_update` binary-provenance check
            # (fetch-binary deploy path only -- irrelevant under
            # --source-build, which always compiles the current working
            # tree and has nothing to compare against). By default, a SHA
            # mismatch between the nightly release's commit.txt and the
            # local working tree's HEAD hard-fails the deploy (see
            # `_update`'s docstring: a stale binary silently invalidates the
            # measurement, made worse by `config::Parameters` having no
            # `#[serde(deny_unknown_fields)]`). Set true only once you've
            # deliberately confirmed the drift is immaterial for what you're
            # running -- the mismatch is then downgraded to a `Print.warn`
            # instead of aborting the deploy.
            self.allow_stale_binary = (
                bool(json['allow_stale_binary'])
                if 'allow_stale_binary' in json else False
            )
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if min(self.nodes) <= self.faults:
            raise ConfigError('There should be more nodes than faults')


class PlotParameters:
    def __init__(self, json):
        try:
            faults = json['faults']
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            workers = json['workers']
            workers = workers if isinstance(workers, list) else [workers]
            if not workers:
                raise ConfigError('Missing number of workers')
            self.workers = [int(x) for x in workers]

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            max_lat = json['max_latency']
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if len(self.nodes) > 1 and len(self.workers) > 1:
            raise ConfigError(
                'Either the "nodes" or the "workers can be a list (not both)'
            )

    def scalability(self):
        return len(self.workers) > 1
