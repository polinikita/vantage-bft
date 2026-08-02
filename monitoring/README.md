# Monitoring stack for local, Docker, and remote benchmarks

Optional. `local-benchmark` runs (and prints its RESULTS block) with no monitoring
stack at all -- this is only for watching a run live in Grafana. Three flows share the
same `docker-compose.yml`/dashboard: **local mode** (a `node local-benchmark` run on
this machine), **Docker mode** (one local container per validator), and
**orchestration mode** (a live `fab remote` run on AWS).

## Local mode

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

## Docker mode (`docker-bench`)

`docker-bench/run.sh` starts this monitoring stack automatically, waits until both
the primary and worker target for every validator are healthy, and prints the direct
dashboard URL. Its Compose override attaches Prometheus to the private validator
network; the generated `docker-bench/data/prometheus.yaml` names each process target.
The validator stack stops after the run, but monitoring remains up so the run can
still be inspected. From the repository root:

```
./docker-bench/run.sh --nodes 20 --rate 200 --duration 120 --protocol vantage \
    --ack-watermarks --digest-statements
# Dashboard: http://localhost:3003/d/vantage-local-benchmark
docker compose -f monitoring/docker-compose.yml down   # when finished inspecting
```

## Orchestration mode (AWS, `fab remote`)

After `fab install`/`fab remote` has written `benchmark/.committee.json` (the
committee's real public IPs + metrics ports):

```
cd benchmark
fab monitor              # writes ../monitoring/prometheus-remote.yaml
cd ..
PROMETHEUS_CONFIG=../monitoring/prometheus-remote.yaml \
    docker compose -f monitoring/docker-compose.yml up -d
```

Same containers, same dashboard, same ports (3003/9095) -- `fab monitor` just
regenerates the Prometheus target list from whichever committee `fab remote` most
recently deployed (re-run it after every `fab create`/`fab remote`, since the
committee's IPs change per instance). `PROMETHEUS_CONFIG` is the only difference from
local mode: it overrides which target-list file the `prometheus` container mounts
(defaults to `../.local-bench/prometheus.yaml`, i.e. plain `docker compose up -d`
with no env var is unchanged local-mode behavior). No container network
`host.docker.internal` indirection needed here -- the targets are real public IPs,
reachable directly.

Security-group note (PHASE7-PREP-NOTES.md §remote): the committee's metrics port
range is already open to the orchestrator (verified end-to-end for both protocols),
but scraping FROM YOUR OWN MACHINE (rather than the orchestrator host) additionally
requires your IP to reach those same ports -- covered by the same `[base_port,
base_port+2000]` security-group range `instance.py`/`gcp_instance.py` already open to
all sources (0.0.0.0/0), so no additional security-group change is needed.

## Dashboard

`grafana/grafana-dashboard.json`, five rows (a `node` template variable, multi-select
with "All", filters every panel -- populated from Prometheus's own `up{node=...}`
label, which comes from each scrape target's static `labels:` block, not from the
app):

- **Overview**: a prominent protocol/mode stat panel (`protocol_info`/
  `transaction_mode_info`, METRICS-DASHBOARD-SPEC.md §8), committed TPS (one
  worker-0 series per validator + total stat), committed bytes rate, per-validator
  ordering and materialized p50 transaction latency (both visually capped at one
  second), seal-route rate (total + a fallback-route stat as a degradation signal),
  and latency misses.
- **Consensus** (Vantage-only -- empty/no-data on the two Autobahn paths, which never
  observe into these): view entry/seal/anchor rates, cursor lag (`entered_view -
  cursor_next_view`), control round, frontier `a_i`, control delivered-log
  len/consume pos.
- **Network**: messages/s and bytes/s sent, stacked by traffic category (the §2
  category map, encoded directly as per-category Prometheus `type=~"..."` regex
  queries -- one legend line per category; whichever protocol is actually running
  populates its own lines, the other protocol's lines are simply flat at zero, not
  wrong), overhead-bytes-per-sequenced-byte, bandwidth efficiency (starfish's
  512B-normalized formula).
- **Data plane**: blocks published/received, acks sent/received, repairs
  requested/served (all Vantage-only), batches/s (both protocols), submitted vs.
  sequenced (committed) transactions/s, proposed block size (p50/p90/p99/max bytes,
  Vantage only).
- **Node health**: Prometheus's own `up` (scrape status) by node; per-validator CPU
  and resident memory summed across that validator's standalone primary and worker
  processes; `VantageCore` utilization by section (§3's four
  `utilization_timer{proc}` labels, as % busy); and `VantageCore` inbound-queue
  depth (`core_queue_length`).  CPU is percent of one logical core and may exceed
  100%; the client load generator is deliberately excluded.

Panels for metrics a given protocol never observes into (e.g. every Vantage-only
panel, when an Autobahn run is live) simply show no data -- by design ("panels show
what exists", METRICS-DASHBOARD-SPEC.md §4), not an error.

## How it's wired (local mode)

Nodes run **natively** in the `local-benchmark` process (not dockerized -- unlike
starfish's own compose, which also runs one `dry-run` container per authority). Only
the monitoring containers are dockerized; they reach the native nodes' metrics
endpoints via Docker Desktop's `host.docker.internal` hostname, per the target list in
the generated `<data-dir>/prometheus.yaml` (default data dir: `.local-bench/`, so the
compose file's relative volume mount assumes `local-benchmark` was run from the
repository root, same convention `fab` itself uses relative to `benchmark/`).

`docker-compose.yml` bind-mounts `${PROMETHEUS_CONFIG:-../.local-bench/prometheus.yaml}`
(no recording rules; category grouping is done panel-side via label regexes instead,
see "Dashboard" above) -- if `--data-dir` is overridden, either symlink it to
`.local-bench` or set `PROMETHEUS_CONFIG` to match.

Because native `local-benchmark` runs every validator as tasks inside one PID, the
operating system cannot attribute CPU or resident memory to an individual validator.
The per-node CPU/memory panels therefore intentionally show no data in that mode.
They populate for Linux standalone-process deployments such as Docker/Colima and
remote runs, where each primary and worker has its own process collector; the panel
groups those process series back into one validator line.
