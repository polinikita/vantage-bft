# Vantage BFT testbed

[![rustc](https://img.shields.io/badge/rustc-1.95.0-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg?style=flat-square)](LICENSE)

## Overview

This repository implements and benchmarks Byzantine fault-tolerant consensus
protocols on a shared Rust runtime. The protocols use the same networking,
storage, worker, metrics, and benchmark components for comparable experiments.

The codebase is derived from the
[Autobahn SOSP 2024 artifact](https://github.com/neilgiri/autobahn-artifact).

## Protocols

| Protocol | CLI name | Description |
|---|---|---|
| Vantage | `vantage` | Vantage consensus with data availability, repair, and sequence state sync |
| Autobahn Optimistic | `autobahn-optimistic` | Autobahn with optimistic tips enabled |
| Autobahn Seamless | `autobahn-seamless` | Autobahn with optimistic tips disabled |
| Simple-IT | `simple-it` | Simple-IT with optimistic reliable broadcast |
| Simple-IT Bracha | `simple-it-bracha` | Simple-IT with Bracha reliable broadcast |

## Workspace

| Crate | Role |
|---|---|
| `node` | CLI, local benchmark, and benchmark client |
| `primary` | Consensus protocols and primary coordination |
| `worker` | Transaction batching and payload dissemination |
| `network` | Reliable peer transport |
| `store` | Persistent storage |
| `config` | Committee, protocol, and runtime configuration |
| `crypto` | Keys, signatures, and digests |
| `metrics` | Prometheus metrics |

## Requirements

- Rust 1.95.0 through `rust-toolchain.toml`
- A C/C++ toolchain, Clang, OpenSSL, and `pkg-config`
- Python 3 for benchmark launchers
- Docker Compose for container benchmarks and monitoring

On macOS:

```bash
brew install llvm openssl pkg-config
```

On Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev libssl-dev pkg-config
```

## Quick Start

```bash
cargo build --release --features benchmark
cargo test --workspace
```

### Local benchmark

```bash
./target/release/node local-benchmark \
  --nodes 4 \
  --workers 1 \
  --rate 1000 \
  --tx-size 512 \
  --protocol vantage \
  --duration 60 \
  --mimic-latency-ms 0
```

An explicit `--mimic-latency-ms 0` selects loopback latency. If no latency
option is provided, the benchmark uses the built-in AWS RTT matrix.

Run `./target/release/node local-benchmark --help` for all options.

### Local dryrun with monitoring

```bash
python3 -m pip install pyyaml
python3 local-dryrun/dryrun.py
```

- Grafana: <http://localhost:3003/d/vantage-local-benchmark>
- Prometheus: <http://localhost:9095>

Configuration is in `local-dryrun/config.yml`. See
[local-dryrun/README.md](local-dryrun/README.md).

## Benchmarks

- [Docker benchmark](docker-bench/README.md): one container per validator with
  traffic shaping and recovery scenarios.
- [AWS benchmark](benchmark/README.md): multi-instance deployment through
  Fabric.
- [Monitoring](monitoring/README.md): shared Prometheus and Grafana stack.

## License

This software is licensed under [Apache 2.0](LICENSE).
