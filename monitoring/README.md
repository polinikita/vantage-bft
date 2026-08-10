# Monitoring

The shared Prometheus and Grafana stack supports native local benchmarks,
Docker benchmarks, and AWS deployments.

## Services

| Service | URL |
|---|---|
| Grafana dashboard | <http://localhost:3003/d/vantage-local-benchmark> |
| Prometheus | <http://localhost:9095> |
| Prometheus targets | <http://localhost:9095/targets> |

Grafana allows anonymous administrator access for local development.

## Native Local Benchmark

Start the benchmark first so it writes `.local-bench/prometheus.yaml`:

```bash
cargo build --release --features benchmark
./target/release/node local-benchmark \
  --nodes 4 \
  --workers 1 \
  --rate 1000 \
  --protocol vantage \
  --duration 60 \
  --mimic-latency-ms 0
```

In another terminal:

```bash
docker compose -f monitoring/docker-compose.yml up -d
```

If `--data-dir` is changed, set `PROMETHEUS_CONFIG` to its generated
`prometheus.yaml`.

## Docker Benchmark

`docker-bench/run.sh` starts monitoring automatically and attaches Prometheus
to the validator network:

```bash
./docker-bench/run.sh --nodes 4 --rate 1000 --duration 60 --protocol vantage
```

Validators stop after the run. Monitoring remains active.

## AWS Benchmark

After `fab remote` has generated `benchmark/.committee.json`:

```bash
cd benchmark
fab monitor
cd ..
PROMETHEUS_CONFIG=../monitoring/prometheus-remote.yaml \
  docker compose -f monitoring/docker-compose.yml up -d
```

Run `fab monitor` again after the remote committee changes.

## Dashboard

The dashboard contains:

- committed throughput and transaction latency
- consensus progress and cursor lag
- network messages, bytes, and efficiency
- data publication, acknowledgments, and repair
- process availability, CPU, memory, and Vantage core utilization

Per-validator CPU and memory are available for Docker and remote deployments.
Native `local-benchmark` runs all validators in one process, so the operating
system cannot attribute those metrics to individual validators.

## Retention and Cleanup

Prometheus retains 24 hours of samples in the `prometheus_data` Docker volume.
Ordinary teardown preserves the volume:

```bash
docker compose -f monitoring/docker-compose.yml down
```

Remove the retained metrics only when they are no longer needed:

```bash
docker compose -f monitoring/docker-compose.yml down -v
```
