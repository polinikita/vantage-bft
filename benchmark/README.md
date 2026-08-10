# AWS Benchmarks

Fabric tasks in this directory create AWS instances, deploy the binaries, run
benchmarks, collect logs, and plot results.

For local experiments, use `node local-benchmark` or
[`docker-bench`](../docker-bench/README.md).

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
