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
  --nodes 7 --crash 2 --rate 200 --duration 45 \
  --min-throughput-pct 85 --max-p50-ms 5000
```

The 45-second window amortizes the bounded tail left by crashed-leader
timeouts without weakening the 85% in-window throughput gate.

Use `--protocols vantage` (or a comma-separated subset) for a focused run.

The heavier post-feature control uses one Docker container per validator and
real `tc netem` WAN delays. It compares clean, six-from-genesis-crash, and six
Byzantine-cohort-isolation cases for all five protocol modes at `n=20` and
1,000 tx/s, recording p50/p90/p99 latency and throughput. The isolation case
keeps the faulty bytes inside the Byzantine cohort; it is distinct from the
outside-holder leader-relay experiment below:

```bash
python3 docker-bench/check_protocol_controls.py
```

See [`docker-bench/README.md`](../docker-bench/README.md) for the exact fault
model and gates. This matrix is deliberately local/dedicated rather than part
of the normal hosted CI job; CI retains the smaller release-mode `n=7`, `f=2`
permanent-crash matrix above.

## Local Leader-Relay Stress

The leader-relay experiment increases one uniform total workload. For `n=20`,
six Byzantine authors each receive the same `1/20` load share as every correct
author. Their transactions exercise the full data path but are excluded from
useful-throughput and latency metrics, making the honest target exactly 70% of
total offered load. Every Byzantine batch remains at its author and is
narrowcast only to a correct group of five validators fixed for that lane:
exactly `f=6` direct holders, one below the `f+1=7` PoA threshold. It is not
sent to the other Byzantine validators. The six fixed lane groups are
staggered by `f-1`, so together they cover every correct leader while each
holder retains a complete prefix of its lanes. Selected Byzantine publishers
aggregate their uniform input share into one batch per `Delta`. Headers remain
visible and Byzantine authors refuse repair. To isolate the honest-leader
relay cost,
a selected Byzantine publisher uses its certified cut whenever it is itself a
consensus leader; it does not deliberately stall that view with an optimistic
tip it will refuse to serve. Autobahn's selected Byzantine cars carry
ordinary payload capacity, so queued batches share the next sub-PoA car;
relay dissemination may subsequently let that car obtain a PoA and advance the
lane. Vantage and Simple-IT keep their ordinary block-construction rules.

An optimistic Autobahn leader includes those locally held uncertified tips,
making itself the repair source for the remaining validators before they can
vote. At low load that relay finishes on the fast path. Increasing load first
loses the 500 ms fast path and eventually the `10 Delta = 2 s` round. Vantage
does not make optional tip materialization vote-critical, while Simple-IT
admits only availability-qualified data. Strict Autobahn seamless is an
optional diagnostic control: every Prepare entry, including the leader's own,
is Genesis or PoA-certified, so Prepare voting never waits for data.
The worker exports `committed_uncounted_transactions`: it proves how much
Byzantine marker-2 payload was committed and materialized while keeping that
payload out of the useful-throughput and latency statistics.

One point can be reproduced with:

```bash
docker-bench/run.sh \
  --nodes 20 --rate 10000 --duration 70 \
  --protocol autobahn-optimistic --all-to-all \
  --withhold 6 --leader-relay --egress-mbps 1000
```

Run the complete geometric load sweep and generate the publication figure with:

```bash
python3 benchmark/leader_relay_sweep.py
```

The default primary series are Autobahn optimistic all-to-all, Vantage, and
Simple-IT/Opt-RBC. Add the strict seamless diagnostic with
`--protocols autobahn-optimistic,vantage,simple-it,autobahn-seamless`.
The runner uses the Docker AWS-RTT netem matrix, a disclosed 1 Gbit/s
per-validator egress cap, 70-second runs (the first 10 seconds are excluded
from rate calculation), and three repetitions around each observed knee. It
enforces a strict Optimistic regression through 10,000 total TPS: at least 80%
of the offered Byzantine share must appear in
`committed_uncounted_tps`, in addition to the honest-goodput gate. It
writes raw logs, configurations, JSON/CSV measurements, provenance, and
`leader-relay.{png,pdf}` below `benchmark/results/`.

To replot retained measurements:

```bash
python3 benchmark/plot_leader_burden.py \
  benchmark/results/<study-directory>/measurements.csv
```

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
