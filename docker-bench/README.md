# Docker Benchmark

This benchmark runs one validator per container on a private Docker network.
It uses real TCP connections, `tc netem` latency, Prometheus, and Grafana.

## Prerequisites

- Docker with Compose
- Python 3
- Enough CPU and memory for two node processes per validator

On macOS, Docker Desktop or Colima must be running. `colima-up.sh` provides a
project-specific Colima configuration.

## Quick Start

```bash
./docker-bench/run.sh \
  --nodes 4 \
  --rate 1000 \
  --duration 60 \
  --protocol vantage
```

The script generates configuration, builds the image, starts validators and
monitoring, prints a live timeline and final summary, then stops validators.
Monitoring remains available after the run.

- Grafana: <http://localhost:3003/d/vantage-local-benchmark>
- Prometheus: <http://localhost:9095>

## Options

`run.sh` handles these options directly:

| Option | Default | Description |
|---|---:|---|
| `--nodes` | `4` | Number of validators |
| `--rate` | `200` | Aggregate transactions per second |
| `--duration` | `60` | Measurement duration in seconds |
| `--protocol` | `vantage` | Consensus protocol |

All other options are passed to `gen.py`:

```bash
python3 docker-bench/gen.py --help
```

Common options include `--tx-size`, `--mode`, `--no-latency`,
`--delta-ms`, `--max-header-delay-ms`, and the state-sync controls.

## Fault Injection

Disrupt a link during an active run:

```bash
./docker-bench/blip.sh 0 1 5 drop
./docker-bench/blip.sh 2 all 10 cut
```

`drop` discards traffic without closing the connection. `cut` resets the
connection and exercises reconnect handling.

Run rolling validator outages against an active cluster:

```bash
./docker-bench/chaos.sh --mode pause --cycles 6 --outage 10 --gap 20
```

Run and score the late-joiner recovery regression:

```bash
cd docker-bench
./joiner_verify.sh
```

The default run uses 10 validators and lasts 215 seconds. The tenth validator
restarts with empty in-memory consensus state, catches up through sequence sync,
and must commit every proposal it makes after recovery.

## Results

Inspect the current targets once:

```bash
python3 docker-bench/results.py
```

Watch an active run:

```bash
python3 docker-bench/results.py --watch --duration 120
```

Generated keys, configuration, logs, and stores are written to
`docker-bench/data/`. The directory is recreated for each run and is ignored by
Git.

## Cleanup

```bash
docker compose -f monitoring/docker-compose.yml down
```

This preserves the Prometheus volume. Add `-v` to remove retained metrics.
