# Monitoring stack for `node local-benchmark`

Optional. `local-benchmark` runs (and prints its RESULTS block) with no monitoring
stack at all -- this is only for watching a run live in Grafana.

## Bring it up

From the repository root:

```
# Terminal 1: start the benchmark (writes .local-bench/prometheus.yaml on boot)
./target/release/node local-benchmark --nodes 4 --workers 1 --rate 240000 \
    --tx-size 512 --protocol autobahn-optimistic --mode all-zero --duration 60

# Terminal 2, once .local-bench/prometheus.yaml exists:
docker compose -f monitoring/docker-compose.yml up -d
```

Grafana: <http://localhost:3003> (anonymous admin, no login). Prometheus:
<http://localhost:9095>.

Host ports 3003 (grafana) / 9095 (prometheus) -- not starfish's own 3002/9093: this
machine's Docker Desktop already holds 3001/3002, and starfish's own local-dryrun
compose collides with 9093. If a future machine holds these too, edit the `ports:`
lines in `docker-compose.yml`.

## How it's wired

Nodes run **natively** in the `local-benchmark` process (not dockerized -- unlike
starfish's own compose, which also runs one `dry-run` container per authority). Only
the monitoring containers are dockerized; they reach the native nodes' metrics
endpoints via Docker Desktop's `host.docker.internal` hostname, per the target list in
the generated `<data-dir>/prometheus.yaml` (default data dir: `.local-bench/`, so the
compose file's relative volume mount assumes `local-benchmark` was run from the
repository root, same convention `fab` itself uses relative to `benchmark/`).

`docker-compose.yml` bind-mounts `../.local-bench/prometheus.yaml` directly (no
recording rules, no dashboard tied to the container-network IPs starfish's own compose
uses) -- if `--data-dir` is overridden, either symlink it to `.local-bench` or edit the
volume mount to match.

## Dashboard

`grafana/grafana-dashboard.json` is written from scratch for this project's own
metric names (committed TPS per node + total, real-latency p50/p90/p99/max, latency
misses, committed bytes rate) -- starfish's own dashboard was not ported wholesale,
since its 23 panels reference DAG/BLS/shard-reconstruction metrics this artifact
doesn't have.
