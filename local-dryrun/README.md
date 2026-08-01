# Local dryrun launcher

`dryrun.py` (METRICS-DASHBOARD-SPEC.md §5): one config file, one command, to build,
monitor, and run a `node local-benchmark` session with a live Grafana dashboard.

```
python3 local-dryrun/dryrun.py
```

edits go in `config.yml` (protocol, node/worker count, rate, tx size, mode, duration,
Δ, batch/header delays, crash count, latency table, data dir -- every run parameter
in one place, commented defaults = the n=10 / 1000 tx/s WAN-shaped latency
experiment).

## What it does

1. Reads `config.yml` (or `--config <path>`).
2. `CARGO_BUILD_JOBS=4 cargo build --release --features benchmark` (skip with
   `--no-build` if already built).
3. Pre-generates `<data_dir>/prometheus.yaml` from the configured node/worker count
   (replicates `config::Committee::local_benchmark`'s deterministic port allocation
   in Python -- so the file has real content before either Docker or `node
   local-benchmark` itself has run; `node local-benchmark` overwrites it identically
   on its own boot, a no-op).
4. Brings up `monitoring/docker-compose.yml` (`PROMETHEUS_CONFIG` pointed at the file
   from step 3) -- idempotent, waits for Grafana's health endpoint.
5. Opens the dashboard in the default browser (`open`, macOS, best-effort -- on other
   platforms it just prints the URL).
6. Execs `node local-benchmark` with `config.yml`'s parameters, streaming its output
   live (including the RESULTS block at the end).
7. On exit or Ctrl-C: prints where RESULTS/logs/stores ended up. The monitoring
   stack is left running by default (so you can keep inspecting the dashboard after
   the run) -- pass `--down` to tear it down instead.

`--duration 0` in `config.yml` means run until Ctrl-C; `node local-benchmark` handles
its own clean shutdown (same process group, same SIGINT) and still prints a final
RESULTS block from whatever it observed up to that point.

## Why native processes, not Docker containers per node

Deliberate Phase-2 §8 deviation from starfish's own `local-dryrun` (a bash script
that builds a `starfish` Docker image and runs one container per validator): here,
primaries/workers/clients run as **native OS processes** in one `node
local-benchmark` invocation -- no Dockerfile, no image rebuild on every code change,
much faster edit-run cycles during protocol development. Only `prometheus`+`grafana`
(the monitoring stack itself) are dockerized, reaching the native processes' metrics
endpoints via Docker Desktop's `host.docker.internal` hostname.

Fully-dockerized nodes (one container per authority, closer to starfish's own setup)
remain available as a future option if ever needed (e.g. to test under a more
realistic per-container resource/network isolation) -- not implemented here, since it
would reintroduce the build-image-per-change cost this deviation exists to avoid, and
nothing in this task called for it.

## Requirements

Python 3, stdlib + `pyyaml` only (`pip install pyyaml` if your interpreter doesn't
have it already). Docker, for the monitoring stack (skip steps 3-5 by editing
`dryrun.py` if you only want the raw benchmark with no dashboard -- or just ignore
the dashboard and read the RESULTS block, monitoring is optional).

## Dependency on `../monitoring/`

Reuses `monitoring/docker-compose.yml`/`monitoring/grafana/*` unmodified (the same
stack `node local-benchmark` documents on its own, and the same one `fab monitor`
points at a live AWS run in orchestration mode) -- see `../monitoring/README.md`.
