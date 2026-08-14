# AWS Benchmarks

Fabric tasks in this directory create AWS instances, deploy the binaries, run
benchmarks, collect logs, and plot results.

For local experiments, use `node local-benchmark` or
[`docker-bench`](../docker-bench/README.md).

## Local Five-Protocol Publication Study

`local_five_protocol_sweep.py` compares Vantage, Autobahn optimistic with
all-to-all enabled, Autobahn seamless, Simple-IT/Opt-RBC, and
Simple-IT/Bracha-RBC in one controlled `n=10` experiment:

```bash
python3 benchmark/local_five_protocol_sweep.py
```

The primary two-panel figure contrasts a clean offered-load sweep with the
same sweep when Byzantine nodes 0–2 send original data-lane headers and
batches only within their own three-node group and refuse subsequent
certificate, header, and batch repair to the rest of the committee. Consensus
traffic remains enabled. Each unavailable lane therefore contributes roughly
one tenth of offered load but cannot materialize at the seven honest nodes.
The local isolation study defaults to a uniform 50 ms honest-link RTT; pass
`--honest-rtt-ms 0` to use the built-in ten-region AWS RTT matrix instead.
Defaults use a 10-second warmup and a 30-second measured window. A curve stops
after the first point below 95% of reachable offered load or with backlog
growth beyond the unavoidable rate of transactions submitted to unavailable
Byzantine lanes; boundary points are repeated three times. The harness also
produces a receiver-width sweep for
`K=0,3,6,7`, raw logs, CSV/JSON measurements, and provenance.

With the default `Delta = 200 ms`, both Autobahn variants use the paper's
proof-calibrated `10 * Delta = 2 s` consensus timeout. Their separate 500 ms
fast-path wait is a performance tuning, not a view-change timeout.

### Mandatory crash regression

The protocol CI runs all five implementations in release mode with `n=7`, two
validators permanently absent from genesis, 200 offered tx/s, and the built-in
AWS RTT matrix over a 30-second measured window. Each implementation must
report zero panics, at least 85% committed throughput, and materialized p50
latency below five seconds. Autobahn uses its proof-calibrated 10 × Δ round
timer (2 seconds for the harness's Δ = 200 ms). The script keeps the larger
`n=20`, six-crash, 1,000 tx/s all-protocol liveness matrix as its default for
local or dedicated benchmark runners. That larger stress gate rejects panics,
throughput below 50%, and materialized p50 above 15 seconds; these bounds
detect stalls without turning a maximal-fault smoke test into a capacity claim:

```bash
python3 benchmark/check_protocol_regressions.py --binary target/release/node
```

To reproduce the smaller CI gate locally:

```bash
python3 benchmark/check_protocol_regressions.py \
  --binary target/release/node --protocols all \
  --nodes 7 --crash 2 --rate 200 --duration 30 \
  --min-throughput-pct 85 --max-p50-ms 5000
```

Use `--protocols vantage` (or a comma-separated subset) for a focused run.

The heavier post-feature control uses one Docker container per validator and
real `tc netem` WAN delays. It compares clean, six-from-genesis-crash, and six
Byzantine lane-withholding cases for all five protocol modes at `n=20` and
1,000 tx/s, recording p50/p90/p99 latency and throughput:

```bash
python3 docker-bench/check_protocol_controls.py
```

See [`docker-bench/README.md`](../docker-bench/README.md) for the exact fault
model and gates. This matrix is deliberately local/dedicated rather than part
of the normal hosted CI job; CI retains the smaller release-mode `n=7`, `f=2`
permanent-crash matrix above.

## Local Leader-Relay Stress

The leader-relay experiment keeps useful demand fixed on correct lanes while
silent Byzantine authors add uncounted, narrowcast payload. The background
transactions traverse the complete data path but are excluded from offered and
committed goodput. For the `n=20` mapping used in the study, six publishers are
spread with publisher stride 7; each omits six receivers with stride 19. Every
correct leader therefore holds four faulty lanes and must recover two.

One point can be reproduced with:

```bash
docker-bench/run.sh \
  --nodes 20 --rate 5000 --duration 40 \
  --protocol autobahn-optimistic --all-to-all \
  --withhold 6 --withhold-publisher-stride 7 \
  --withhold-count 6 --withhold-stride 19 \
  --withhold-batches-only --withhold-repair \
  --correct-load-only --adversarial-rate 20000
```

Use `--protocol vantage` or `--protocol simple-it` for the matched controls;
the latter derives its `8 * Delta` timeout automatically. Plot a CSV containing `protocol`,
`adversarial_tps`, `useful_tps`, `p50_ms`, and `p99_ms` with:

```bash
python3 benchmark/plot_leader_burden.py measurements.csv
```

After a run, generate the presentation-oriented capacity/latency summary with:

```bash
python3 benchmark/plot_drop_summary.py benchmark/results/<study-directory>
```

Use `--quick` for a short end-to-end check, optionally with `--protocols
vantage`. Run databases are discarded after each point unless `--keep-data`
is supplied.

## Prerequisites

- An AWS account with EC2 access
- AWS credentials in `~/.aws/credentials`
- The same EC2 SSH key name in every configured region
- Python 3

```bash
python3 -m pip install -r benchmark/requirements.txt
```

## Configuration

Copy the example and edit the local file:

```bash
cp benchmark/settings.example.json benchmark/settings.json
```

The local `benchmark/settings.json` is ignored by Git.

```json
{
  "key": {
    "name": "aws-key-name",
    "path": "/absolute/path/to/aws-key.pem"
  },
  "port": 6000,
  "repo": {
    "name": "vantage",
    "url": "https://github.com/OWNER/vantage-bft.git",
    "branch": "main",
    "release_repo": "OWNER/vantage-bft"
  },
  "username": "ubuntu",
  "instances": {
    "type": "c5d.xlarge",
    "regions": ["eu-west-1"],
    "spot": true
  }
}
```

`release_repo` is used to download the `nightly` release binaries. Omit it
only when using `--source-build`.

## Usage

Run commands from `benchmark/`:

```bash
cd benchmark
fab --list
fab create --nodes=1
fab install
fab remote --protocol=vantage
```

`--nodes` on `fab create` is the number of instances created in each
configured region.

Use the source tree instead of release binaries when testing unpublished code:

```bash
fab install --source-build
fab remote --protocol=vantage --source-build
```

The install and run modes must match.

## Campaigns

`fab remote` runs the benchmark matrix defined in `fabfile.py`.
`fab campaign` exposes the main Vantage sweep parameters:

```bash
fab campaign \
  --protocol=vantage \
  --nodes=20 \
  --duration=180 \
  --rates=50000,100000,150000
```

Use `fab kill` to stop running benchmark processes. Logs and parsed results are
stored under `benchmark/logs` and `benchmark/results`.

Generate plots with:

```bash
fab plot
```

## Monitoring

After a remote run has generated `.committee.json`:

```bash
fab monitor
cd ..
PROMETHEUS_CONFIG=../monitoring/prometheus-remote.yaml \
  docker compose -f monitoring/docker-compose.yml up -d
```

- Grafana: <http://localhost:3003/d/vantage-local-benchmark>
- Prometheus: <http://localhost:9095>

See [monitoring/README.md](../monitoring/README.md).

## Testbed Management

```bash
fab info
fab cost
fab stop
fab start
fab destroy
```

`fab destroy` terminates the instances and records a final cost estimate. Run
it when the testbed is no longer needed.
