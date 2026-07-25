# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        # NVMe-INSTANCE-STORE: `rm -r .db-*` alone no longer clears the store --
        # `--store` now points under `PathMaker.REMOTE_STORE_BASE` (the mounted
        # NVMe instance-store disk, see remote.py's install()/`PathMaker.
        # remote_db_path`), not the home directory. `-f` (not just `-r`) on the
        # new clause: unlike home-dir `.db-*`, this runs on hosts that never
        # got an NVMe device (the fallback path in install() still creates
        # REMOTE_STORE_BASE on the EBS root) and on the coordinator itself
        # (this same string also runs locally, see remote.py's `_config`),
        # where the glob simply never matches -- `-f` makes that a silent
        # no-op instead of a (harmless but noisy) "No such file or directory".
        return (
            f'rm -r .db-* ; rm .*.json ; '
            f'rm -rf {PathMaker.REMOTE_STORE_BASE}/.db-* ; '
            f'mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def run_primary(keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} primary')

    @staticmethod
    def run_worker(keys, committee, store, parameters, id, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(address, size, rate, nodes, mode='all-zero'):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        assert mode in ('all-zero', 'random')
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return (f'./benchmark_client {address} --size {size} --rate {rate} '
                f'--mode {mode} {nodes}')

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'