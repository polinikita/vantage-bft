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

## All-protocol control matrix

After a major protocol change, run the standard `n=20`, 1,000 tx/s control:

```bash
python3 docker-bench/check_protocol_controls.py
```

It runs Vantage, Autobahn optimistic, Autobahn seamless, Simple-IT/Opt-RBC,
and Simple-IT/Bracha-RBC under the AWS RTT matrix implemented with
per-destination `tc netem`. Autobahn optimistic always enables its defining
all-to-all communication mode. Round timeouts are derived from `--delta-ms`:
Autobahn uses 10x, Simple-IT/Opt-RBC 8x, and Simple-IT/Bracha-RBC 5x. The
three scenarios are:

1. `clean`: all 20 validators run and the reachable target is 1,000 tx/s.
2. `crash`: validators 0--5 are never started; 1,000 tx/s is redistributed over
   the 14 live validators.
3. `withhold`: validators 0--5 send lane data only among themselves and refuse
   repair to validators 6--19; the materializable target is 700 tx/s.

The third case is Byzantine-cohort isolation, not the optimistic leader-relay
profile. Use `--leader-relay` (or `benchmark/leader_relay_sweep.py`) when the
bytes must reach `f-1` honest holders and Optimistic is expected to commit the
Byzantine share by relaying it through honest leaders.

Each point records submitted and committed throughput, real and materialized
p50/p90/p99 latency, panic matches, the exact command, and its full log.
`records.json`, `records.csv`, and `summary.md` are written below
`benchmark/results/docker-controls-<timestamp>/`. By default a point must
commit at least 85% of its reachable target, remain panic-free, and keep
materialized p50 within 2x of its protocol's clean baseline or 500 ms above it,
whichever allowance is larger.

Use `--protocols`, `--scenarios`, or `--duration` for focused diagnostics.
`--reuse-image` skips the first image build when the current source is already
present as `vantage-docker-bench:latest`.

## Options

`run.sh` handles these options directly:

| Option | Default | Description |
|---|---:|---|
| `--nodes` | `4` | Number of validators |
| `--rate` | `200` | Aggregate transactions per second |
| `--duration` | `60` | Measurement duration in seconds |
| `--protocol` | `vantage` | Consensus protocol |
| `--crash` | `0` | Leave validators `0..N-1` absent from genesis (at most `f`) |
| `--no-build` | off | Reuse the existing `vantage-docker-bench:latest` image |

All other options are passed to `gen.py`:

```bash
python3 docker-bench/gen.py --help
```

Common options include `--tx-size`, `--mode`, `--no-latency`,
`--delta-ms`, `--max-header-delay-ms`, and the state-sync controls.
Use `--withhold N --withhold-count K` to make the first `N` validators omit
each payload broadcast to `K` staggered peers.
Use `--withhold-publisher-stride S` to spread those Byzantine publishers over
committee order instead of selecting a consecutive prefix.
Add `--withhold-fixed-receivers` to use one disjoint receiver group and
`--withhold-batches-only` to keep lane headers flowing while permanently
dropping only the heavy transaction batches on those links.
Add `--withhold-repair` to make the selected Byzantine publishers ignore all
lane-header, certificate, and batch repair requests after narrowcasting.
Use `--leader-relay` for the optimistic leader-capacity experiment. The total
offered rate remains uniformly distributed across all validators; the normal
share submitted to each selected Byzantine author is marked uncounted so the
reported throughput and latency cover honest transactions only. Each
Byzantine batch remains at its author and is sent only to an `(f-1)`-wide
correct group fixed for that Byzantine lane. These `f` direct holders are
exactly one below the `f+1` PoA threshold; the other Byzantine validators do
not receive the bytes.
Lane headers stay visible, Byzantine authors refuse repair, and all Byzantine
lane groups are staggered by `f-1` positions across the correct validators.
Selected Byzantine publishers aggregate one batch per `Delta`; the fixed group
therefore retains the lane's complete payload prefix. Across the Byzantine
lanes, the staggered groups exercise every correct leader. The mode implies
batch-only withholding from all other peers. Selected Byzantine Autobahn
publishers retain ordinary car capacity,
keeping a single sub-PoA tip active until dissemination later supplies another
holder; honest lanes and the other protocols retain their normal payload
capacity and block-construction rules. A selected
Byzantine publisher proposes its certified cut when it is the consensus leader,
so its refusal to serve does not add a separate, intentional Byzantine-leader
timeout to the honest-leader capacity experiment.
`--egress-mbps R` adds the same aggregate egress cap to every container while
retaining the AWS RTT `tc netem` matrix.
The final JSON reports `committed_uncounted_tps` separately. For this experiment
it is the committed Byzantine payload rate; it is deliberately excluded from
`committed_tps`, which remains honest useful goodput.

Vantage carries positional availability claims on AGB echoes by default.
`--no-echo-avail-claims` selects periodic watermarks; `--no-ack-watermarks`
selects per-block acknowledgements.
Vantage also uses one-byte committee identifiers by default;
`--no-compact-ids` selects full public keys.

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
