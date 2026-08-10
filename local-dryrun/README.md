# Local Dryrun

`dryrun.py` builds and runs `node local-benchmark` with configuration from
`config.yml`. Prometheus and Grafana run in Docker; validators run in one native
process.

## Prerequisites

- Python 3 with PyYAML
- Docker with Compose
- Rust and the native build dependencies listed in the root README

```bash
python3 -m pip install pyyaml
```

## Usage

```bash
python3 local-dryrun/dryrun.py
```

Use another configuration file:

```bash
python3 local-dryrun/dryrun.py --config path/to/config.yml
```

Skip a cached build or stop monitoring when the benchmark exits:

```bash
python3 local-dryrun/dryrun.py --no-build
python3 local-dryrun/dryrun.py --down
```

Set `duration: 0` to run until Ctrl+C. A clean shutdown still prints the
benchmark summary.

## Configuration

`config.yml` defines:

- protocol, validator count, and workers per validator
- aggregate rate, transaction size, and payload mode
- duration and protocol timing
- transport batching
- startup crash count
- latency table or loopback mode
- generated data directory

`latency_table: none` selects loopback latency. A CSV path selects an
`N x N` RTT matrix. Presets for 4, 10, and 20 validators are in `latency/`.

## Monitoring

- Grafana: <http://localhost:3003/d/vantage-local-benchmark>
- Prometheus: <http://localhost:9095>

Monitoring remains active by default so the completed run can be inspected. See
[monitoring/README.md](../monitoring/README.md).

## Regression Check

The fault-free regression guard checks throughput, latency, misses, and cursor
progress against fixed thresholds:

```bash
./local-dryrun/regress.sh
```

Optional arguments are `duration`, `nodes`, `rate`, and
`wan|loopback`.
