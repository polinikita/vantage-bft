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
    '''Store committee wire addresses and public SSH hosts.'''

    def __init__(self, addresses, base_port, public_hosts=None):
        '''Build a committee from wire addresses and optional public hosts.'''
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
            # Use public hosts for metrics and private hosts for peers.
            # Copy the public list before consuming it.
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
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['primary_to_primary']]
        return addresses

    def workers_addresses(self, faults=0):
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
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for authority in list(self.json['authorities'].values())[:good_nodes]:
            addresses += [authority['primary']['metrics']]
        return addresses

    def workers_metrics_addresses(self, faults=0):
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
        '''Return the primary's public host, or its wire host without a mapping.'''
        if self.public_hosts is None:
            return self.ip(
                self.json['authorities'][name]['primary']['primary_to_primary']
            )
        return self.public_hosts[name][0]

    def worker_public_ip(self, name, worker_id):
        '''Return a worker's public host, or its wire host without a mapping.'''
        if self.public_hosts is None:
            return self.ip(
                self.json['authorities'][name]['workers'][worker_id]['transactions']
            )
        return self.public_hosts[name][worker_id + 1]

    def primary_public_ips(self, faults=0):
        assert faults < self.size()
        good_nodes = self.size() - faults
        names = list(self.json['authorities'].keys())[:good_nodes]
        return [self.primary_public_ip(name) for name in names]

    def workers_public_ips(self, faults=0):
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
        '''Return public hosts for live authorities, or wire hosts without a mapping.'''
        if self.public_hosts is None:
            return self.ips(name)

        # Use live authorities; public_hosts also contains removed authorities.
        names = [name] if name is not None else list(self.json['authorities'].keys())
        ips = set()
        for n in names:
            ips.update(self.public_hosts[n])
        return list(ips)

    def ips(self, name=None):
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
        assert nodes < self.size()
        for _ in range(nodes):
            self.json['authorities'].popitem()

    def size(self):
        return len(self.json['authorities'])

    def workers(self):
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
    '''Build Prometheus scrape configuration from committee wire addresses.'''
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

        # Validate optional protocol and latency settings.
        if 'protocol' in json and json['protocol'] not in (
            'autobahn-optimistic', 'autobahn-seamless', 'vantage'
        ):
            raise ConfigError(f"Invalid protocol '{json['protocol']}'")
        for key in ('delta_ms', 'mimic_latency_ms'):
            if key in json:
                v = json[key]
                if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                    raise ConfigError(f"'{key}' must be a non-negative integer")

        # Keep the Vantage retention window positive.
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
            self.rate = sorted(int(x) for x in rate)

            self.workers = int(json['workers'])

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            # Omit `tx_mode` for the all-zero payload default.
            self.tx_mode = str(json['tx_mode']) if 'tx_mode' in json else 'all_zero'
            # Normalize hyphens to underscores.
            self.tx_mode = self.tx_mode.replace('-', '_')
            if self.tx_mode not in ('all_zero', 'random'):
                raise ConfigError(
                    f"Invalid tx_mode '{self.tx_mode}': expected 'all_zero' or 'random'"
                )

            self.duration = int(json['duration'])

            self.runs = int(json['runs']) if 'runs' in json else 1
            self.simulate_partition = bool(json['simulate_partition'])

            self.partition_nodes = int(json['partition_nodes'])
            self.partition_start = int(json['partition_start'])
            self.partition_duration = int(json['partition_duration'])

            # Stop when committed TPS falls below the running peak.
            self.early_stop_margin = (
                float(json['early_stop_margin'])
                if 'early_stop_margin' in json else 0.10
            )
            if self.early_stop_margin < 0:
                raise ConfigError('early_stop_margin must be non-negative')

            # Permit a release SHA mismatch only when explicitly requested.
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
