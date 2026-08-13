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
same sweep when nodes 0–2 omit original data-lane headers and batches to the
fixed receiver group nodes 3–5. Repairs and control traffic remain normal.
Each omitted block therefore still has seven direct holders. The local
isolation study defaults to a uniform 50 ms honest-link RTT; pass
`--honest-rtt-ms 0` to use the built-in ten-region AWS RTT matrix instead.
Defaults use a 10-second warmup and a 30-second measured window. A curve stops
after the first point below 95% of
offered load or with a growing latter-half backlog; boundary points are
repeated three times. The harness also produces a receiver-width sweep for
`K=0,1,2,3`, raw logs, CSV/JSON measurements, and provenance.

Both Autobahn variants use the deployment configuration's 5-second consensus
and fast-path timeouts.

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

Use `--protocol vantage` or `--protocol simple-it --timeout-delay-ms 1600`
for the matched controls. Plot a CSV containing `protocol`,
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
