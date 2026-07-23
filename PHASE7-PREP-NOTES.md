## §remote — `fab remote` on AWS: harness repair, smoke test attempted, blocked on a genuine compile break

**Status: smoke test PROVISIONED and RUN once (autobahn-optimistic), FAILED at compile
(gate), instances TERMINATED and verified at 0. No vantage run was attempted** (the
ordered flow was "run autobahn-optimistic; if clean, run vantage" — it wasn't clean).
See "§remote — smoke test attempt" below for the full account. Sections 1–5 below
(harness repair) are unchanged from the pre-provisioning pass and all held up in
practice; section 6 records the coordinator's ruling on the deploy-mechanism question
and the rsync implementation.

### 1. Harness inventory: fabfile's `remote` task was wired to GCP, not AWS

`benchmark/benchmark/gcp_instance.py` was added alongside the original AWS
`instance.py`, and at some point `remote.py` was repointed at it:

- `benchmark/fabfile.py`'s `create`/`destroy`/`start`/`stop`/`info` tasks import
  `benchmark.instance.InstanceManager` (AWS) directly — unaffected.
- `benchmark/benchmark/remote.py`'s `Bench` class (used by the `install`,
  `remote`, and `kill` tasks — i.e. everything `fab remote` actually needs)
  imported `from benchmark.gcp_instance import InstanceManager`. So `fab remote`
  was exercising the GCP (`google-cloud-compute`) path, not AWS, regardless of
  what `create`/`destroy` did.
- `benchmark/benchmark/settings.py`'s `Settings` class had also drifted to a
  GCP shape: it took an `aws_regions` constructor arg but stored it only as
  `self.gcp_zones`, and required `project_id`/`instances.templates`/`username`
  unconditionally. AWS's `instance.py` reads `settings.aws_regions` — that
  attribute didn't exist, so even the still-AWS-wired `create`/`destroy` tasks
  were broken.

**Fix applied** (`benchmark/benchmark/remote.py`, `benchmark/benchmark/settings.py`):
- `remote.py`: import `InstanceManager` from `benchmark.instance` (AWS) instead
  of `benchmark.gcp_instance`.
- `settings.py`: `Settings` now populates **both** `self.aws_regions` and
  `self.gcp_zones` from the same `instances.regions` list, and
  `project_id`/`templates`/`username` are optional (default `None`/`[]`/`"ubuntu"`)
  so one `Settings` class serves either provider's `InstanceManager` without
  GCP placeholders in an AWS settings file. `gcp_instance.py` untouched —
  still importable/usable if someone points `remote.py` back at it later.

### 2. Ubuntu AMI resolution was dead

`instance.py::_get_ami` filtered on the literal description
`"Canonical, Ubuntu, 20.04 LTS, amd64 focal image build on 2020-10-26"`.
Confirmed empirically (eu-west-1): **0 images match** that description today —
it's long deregistered. Replaced with a resolution by Canonical's owner id
(`099720109477`) + name glob for Ubuntu 22.04 LTS (`ubuntu-jammy-22.04-amd64-server-*`,
state `available`, `ebs`/`hvm`), sorted by `CreationDate` descending, newest
first, with an explicit `BenchError` if nothing matches. Verified in eu-west-1:
resolves to `ami-0325bf62e3737cec6` (`...-20260714`), `RootDeviceName` =
`/dev/sda1`, matching the existing `BlockDeviceMappings` in
`create_instances`. No other AMI-adjacent fix needed.

### 3. Security groups / metrics ports — already sufficient, no fix needed

Both `instance.py` and `gcp_instance.py` already open **one wide TCP range**,
`[base_port, base_port + 2000]`, plus 22 for SSH. The Phase-2 committee port
layout (`benchmark/benchmark/config.py::Committee.__init__`) is 3 ports per
primary (consensus_to_consensus, primary_to_primary, worker_to_primary+metrics
— actually 1 + 2, with `metrics` folded into the primary block) + 4 ports per
worker (primary_to_worker, transactions, worker_to_worker, metrics), all
allocated sequentially starting at `base_port`. For a 4-node, 1-worker-each
smoke test that's `4 × (3 + 4) = 28` ports total — comfortably inside the
existing 2000-port-wide range. **No security-group change was required**; the
metrics scrape added in Phase 2 (`remote.py::_run_single`, calls
`scrape_metrics` against `committee.primary_metrics_addresses()` /
`workers_metrics_addresses()`) falls inside the already-open range as long as
`base_port + 2000` isn't exceeded, which it isn't at this scale.

### 4. Python venv

The venv this task's prompt referenced
(`.../scratchpad/fabenv`) did not exist. Created it and installed
`fabric==3.2.3`, `boto3` (latest, 1.43.54), and `matplotlib` (transitively
required just to import `fabfile.py`, since `fabfile.py` unconditionally
imports `benchmark.plot`, which imports `matplotlib.pyplot`). No fabric-3 API
incompatibilities found in `remote.py`/`instance.py` (the `Connection`,
`ThreadingGroup as Group`, `ctx.connect_kwargs.pkey`, `GroupException` usage
is all still valid Fabric-3 API). `fab -l` lists all tasks cleanly after the
above fixes.

### 5. Settings chosen (not secrets)

Wrote `benchmark/settings.json` (repo convention: `InstanceManager.make()`
defaults to this filename with no CLI override in the current fabfile, so it
must hold the live config). The prior checked-in content was GCP placeholders
(`key.name: "gcp"`, `<PROJECT_ID>` etc., not real credentials) — overwritten in
the working tree only, not committed, and trivially restorable.

- `instances.regions`: `["eu-west-1"]` — single region, matches one of
  starfish's regions (`crates/orchestrator/assets/settings.yml`: `eu-west-1`,
  read-only reference).
- `instances.type`: `c5.xlarge` (4 vCPU / 8 GiB) — **smaller** than starfish's
  reference `c5d.2xlarge` (8 vCPU / 16 GiB + NVMe); no NVMe needed since
  `create_instances` already provisions a 200 GiB gp2 EBS root volume.
- `key`: dedicated new EC2 RSA key pair `vantage-smoke-20260723170010`
  (RSA required — `remote.py` loads it via `paramiko.RSAKey.from_private_key_file`),
  private key written directly to
  `.../scratchpad/vantage-smoke.pem` (mode 600), never printed to any log/command
  output. This is a fresh key pair scoped to this smoke test, not the
  starfish-shared `~/.ssh/aws`/`nikitapolianskii-iota-starfish` key.
- `port` (base_port): `6000`.
- `username`: `ubuntu` (default for Canonical's Ubuntu AMIs).

Verified end-to-end (read-only): `Settings.load()`, `InstanceManager.make()`
(client construction for eu-west-1), and `_get_ami()` all succeed against
real AWS with these settings. No `create_instances()` call was made.

### 6. Deploy-mechanism decision — RULED, implemented: working-tree rsync (Option A)

`fab remote`'s deploy step (`Bench._update`, and `Bench.install`) originally did
`git clone {settings.repo_url}` (or `git pull` if already cloned) on each remote host,
then compiled. That can't work here: the only configured remote is `upstream` →
`https://github.com/neilgiri/autobahn-artifact.git` (the original third-party author's
repo, no push access, wrong target regardless), `main` is untracked by any remote, and
`git status` showed 117 changed/untracked paths — the entire Phase 2–6 protocol work,
uncommitted. Cloning from any reachable remote would fetch stale/unrelated history.

**Coordinator ruling: Option A — rsync the working tree instead of `git clone`/`git
pull`, contained to the deploy step.** Rationale given: tests exactly the audited
working tree; the user's standing workflow is that only they commit; the **final,
citable paper campaign should still run from a tagged, committed revision** (git
clone) for provenance — this rsync path is for the smoke test and other pre-campaign
runs against a dirty tree, not the audited campaign itself.

**Implementation** (`benchmark/benchmark/remote.py`, `Bench` class):
- New `Bench.RSYNC_EXCLUDES` class constant: `.git/`, `target/`, `benchmark/logs/`,
  `benchmark/results/`, `benchmark/data/`, `__pycache__/`, `*.pyc`, `.venv/`, `venv/`,
  `fabenv/`, `*.pem` — build artifacts, VCS metadata, prior run output, and
  venv/scratch/key material never cross the wire; hosts still compile from source.
- New `Bench._repo_root()` (two `..` up from `remote.py`'s own path),
  `Bench._ssh_opts()` (`-i <key>`, `StrictHostKeyChecking=accept-new`,
  `UserKnownHostsFile=/dev/null`, `LogLevel=ERROR` — Fabric's own `Connection`/`Group`
  already auto-add unknown host keys via `AutoAddPolicy` unconditionally
  (`fabric/connection.py`), but the raw `ssh`/`rsync` subprocess calls here bypass
  Fabric entirely and need the same handling spelled out), and `Bench._sync_tree(ips)`
  (per-host `mkdir -p <repo_name>` over plain `ssh`, then `rsync -az --delete
  <excludes> -e "<ssh_opts>" <root>/ <user>@<ip>:<repo_name>/`; raises `ExecutionError`
  — already handled by every caller's existing `except (GroupException,
  ExecutionError)` — on either step's non-zero exit). Incremental: repeat runs only
  ship deltas.
- `Bench.install()`: keeps the apt/rust/build-essential/cmake/clang steps unchanged;
  the final `git clone || git pull` line is replaced by a call to `_sync_tree` after
  the `Group.run` for the system setup.
- `Bench._update()`: the `git fetch -f && git checkout -f {branch} && git pull -f`
  chain is replaced by `_sync_tree(ips)`, then unchanged `source
  $HOME/.cargo/env && cd {repo}/node && cargo build ... && alias_binaries(...)`.
- Everything else (`_config`, `_run_single`, `_logs`, security groups, AMI
  resolution) is untouched — the patch is contained to the two deploy functions plus
  the three new helpers, all clearly commented as the working-tree-deploy variant
  with a pointer back to this note.

Verified: `fab -l`/import sanity clean; `fab install` successfully rsynced and ran
apt/rust setup on all 4 hosts (see smoke-test section below); `fab remote`'s own
`_update` rsync + compile step ran correctly per-host (confirmed by direct SSH
inspection of one host: `~/vantage` present, rsync excludes honored — no `.git/`,
`target/`, or `.pem` transferred).

### Instance-hours consumed this session: **~3.2** (4× `c5.xlarge`, eu-west-1, ~48 min
each, launch 2026-07-23 15:09:51 UTC → terminated 2026-07-23 ~15:58 UTC). See the
smoke-test section below for what that bought.

### Cleanup / current resource state
- **0 EC2 instances** (verified via `describe_instances`, eu-west-1, all
  non-terminated states, both before and after the smoke-test attempt below).
- **Kept** per coordinator ruling: EC2 key pair `vantage-smoke-20260723170010`
  (eu-west-1) — needed for further smoke-test attempts, zero cost while unused.
- **Kept** per coordinator ruling: `benchmark/settings.json` holds the real AWS smoke
  config (region/instance-type/key-NAME/username — no secret material; double-checked
  by direct read before finishing, see below). Not committed (working tree only).

---

## §remote — smoke test attempt: PROVISIONED, ran, FAILED at compile (gate), TERMINATED

**Sequence actually run**: `fab create --nodes=4` (4× `c5.xlarge`, eu-west-1 — exactly
4 instances, confirmed via `describe_instances`) → `fab install` (apt/rust setup +
working-tree rsync to all 4 hosts — succeeded, "Initialized testbed of 4 nodes") →
`fab remote` (protocol `autobahn-optimistic`, rate 50,000 tx/s, tx-size 512, duration
60s, `delta_ms: 150` in `node_params` — see `fabfile.py`'s `remote` task, updated to
take `protocol` as a fab CLI kwarg so the same task serves both protocol passes without
editing the file between runs) → **FAILED** during log parsing, after the 60s window
elapsed → `fab destroy` → verified 0 instances.

### What actually happened, in order

1. Instances booted, `fab install` succeeded on all 4 (apt/rust + rsync).
2. `fab remote` re-synced (idempotent), uploaded per-host config
   (`.committee.json`/`.node-N.json`/`.parameters.json`), then ran the remote build
   step (`_update`'s `cargo build --quiet --release --features benchmark` in
   `node/`) — **this returned exit 0** (no `GroupException` raised, harness proceeded
   normally: booted clients/primaries/workers, ran the full 60s window, attempted the
   Prometheus scrape, downloaded logs).
3. Local `LogParser.process(...)` then threw: `ParseError: Failed to parse clients'
   logs: 'NoneType' object has no attribute 'group'` — a downloaded `client-*.log`
   only ever printed `Node address / Transactions size / Transactions rate /
   Transaction mode / Waiting for all nodes to be online...` and nothing further (no
   "Start ..." line the parser regex needs), meaning the client never observed the
   nodes coming online within the run.
4. Direct (read-only) SSH into one still-running host before teardown found why:
   `~/node` was a **dangling symlink** (`~/node -> ./vantage/target/release/node`,
   target absent) while `~/benchmark_client` (the client, a *separate* `[[bin]]`
   target in the same `node` package) had compiled and linked correctly
   (`target/release/benchmark_client` present, 4.2 MB). The primary/worker binary
   never existed, so `tmux new -d ... "./node ..."` failed immediately on every host
   (`bash: line 1: ./node: No such file or directory` — this is the literal, only
   content of every downloaded `primary-*.log`/`worker-*.log`), so the actual protocol
   never ran on any node, only the clients (which then sat waiting forever for nodes
   that never started, hence never printed their own "Start" line either).
5. Reproduced directly (non-piped, so the real exit code was captured):
   `cd ~/vantage/node && cargo build --quiet --release --features benchmark` on that
   host now consistently returns **exit 101**:
   ```
   error[E0425]: cannot find function `set_mimic_latency_ms` in crate `network`
     --> node/src/local_benchmark.rs:92:18
      |
   92 |         network::set_mimic_latency_ms(mimic_latency_ms);
      |                  ^^^^^^^^^^^^^^^^^^^^ not found in `network`
   error: could not compile `node` (bin "node") due to 1 previous error
   ```
   `grep`-confirmed: `set_mimic_latency_ms` does not exist anywhere in the `network`
   crate (only `mimic_latency_ms`-named local variables in `local_benchmark.rs`
   itself). `node/Cargo.toml` has exactly one explicit `[[bin]]` (`benchmark_client`,
   `required-features = ["benchmark"]`) — `node`'s own binary comes from Cargo's
   implicit `src/main.rs` auto-discovery, which is a **separate build unit** from
   `benchmark_client` within the same `cargo build` invocation; one failing while the
   other succeeds is exactly what a single stale call site inside `main`'s own binary
   graph (not `benchmark_client`'s) produces.

**This is a real, reproducible source-code compile break, not a harness/deploy bug.**
It sits in `node/src/local_benchmark.rs` (line 92, in the branch alongside the
`LatencyTable::uniform(...)`-based construction a few lines below at 106) — squarely
inside the concurrent local-CPU-measurement work covered by §findingA/§findingB/
§optional above (the `--mimic-latency-ms`/`LatencyTable` upgrade documented there
explicitly superseded an earlier "global `AtomicU64`" `network`-level setter; line 92
reads as a leftover call to that superseded API that wasn't updated at every call site
when the crate-level setter was removed). Per this task's scope (benchmark-harness
Python only, no protocol/source edits, and explicitly not stepping on the concurrent
local-measurement work), **this was not fixed here** — flagging it back rather than
patching `node/src/local_benchmark.rs` myself.

Why the very first remote compile (during `_update`, step 2 above) apparently returned
0 despite this being reproducible immediately afterward is not fully resolved — the
leading theory is a timing race with concurrent edits to this exact file/crate
happening in the shared working tree at rsync time (this repo is being actively edited
by the local-measurement work in parallel with this session), i.e. the rsync'd
snapshot briefly compiled, then the tree changed again before the diagnostic re-run
moments later. This is a real, first-order risk of the rsync-working-tree deploy
mechanism (Option A) specifically when another agent is concurrently editing the same
files — worth keeping in mind for any repeat attempt: **re-sync and compile
immediately before a run, don't assume a build validated minutes earlier still holds**.

### Outcome
- **No RESULTS block for either protocol** — autobahn-optimistic never produced one
  (compile/parse failure gate); vantage was not attempted (ordered flow: only run it
  if autobahn-optimistic was clean).
- **No metrics scrape to verify** — moot this round (nodes never started to expose
  `/metrics`); the security-group port range was not the blocker (scrape attempts got
  `Connection refused`, not a network-level timeout/drop, i.e. the security group let
  the connection attempt through to a closed port — consistent with the port range
  being open as designed, just nothing was listening).
- **No vantage seal-route distribution** — not obtained this round.
- 4× `c5.xlarge` created, ran ~48 min, **terminated**; verified **0** non-terminated
  instances in eu-west-1 via `describe_instances` after `fab destroy`.

### Recommended next step (not taken this session)
Once `node/src/local_benchmark.rs`'s stale `network::set_mimic_latency_ms` call is
cleaned up (or the local-measurement work concludes) so `cargo build --release
--features benchmark` succeeds for the `node` binary specifically (verify with exactly
that command — `benchmark_client` alone compiling is not sufficient signal), the
smoke test can be re-run with the harness as-is: no further AWS/Python-side changes
are expected to be needed. Re-sync (`fab install` or letting `fab remote`'s own
`_update` do it) immediately before compiling, given the concurrent-edit risk noted
above.

---

## §findingA — crash-fault collapse: root cause found, orchestrator's WISH hypothesis REFUTED

**Repro**: `./target/release/node local-benchmark --protocol vantage --nodes 4 --workers
1 --crash 1 --rate 180000 --tx-size 512 --duration 60 --delta-ms 150
--max-batch-delay-ms 20 --max-header-delay-ms 50 --timeline`. Observed **470 tx/s**
(same order of magnitude/phenomenon as the ~660 tx/s originally reported; exact number
is run-to-run sensitive to which early dead views luckily resolve before the backlog
described below runs away — see "root cause"). `data-dir` used:
`.local-bench-findingA*` (left in place, gitignored, harmless to delete).

### Instrumentation added (metrics-only, always-on)

Six flat `IntGauge`s (no labels), same registration pattern as the existing
`vantage_seals` counter, sampled once/sec by a new, effect-free tick in
`VantageCore::run`'s own select loop (`metrics/src/metrics.rs`, `metrics/src/snapshot.rs`
+ `read_vantage_progress`, `primary/src/vantage/node.rs::sample_metrics`):

- `vantage_entered_view` — `Pacemaker::entered_view` (new accessor; W5's own largest
  formally-entered view).
- `vantage_frontier_a_i` — `Frontier::a_i` (already had a public accessor).
- `vantage_cursor_next_view` — `Cursor::next_view` (already had a public accessor).
- `vantage_control_round` — `ControlLog::curr_round` (new accessor).
- `vantage_control_delivered_len` / `vantage_control_consume_pos` —
  `ControlLog::delivered_log.len()` / `.consume_pos` (new accessors).

`node local-benchmark` gained `--timeline` (off by default, verbose): prints one line
per live node per second reading these six gauges straight from each node's own
in-process `Registry` (`node/src/local_benchmark.rs`).

Also added three **diagnostic `log::info!` lines** (no behavior change, same class as
the pre-existing `log::info!("Committed vantage block ...")` in `cursor.rs`), gated at
`info` level so they're silent unless `RUST_LOG` asks for them:
- `resolve.rs::decide` — every time a recovery entry is actually attached to a
  proposal: `"recovery target u={u} attached at carrier turn w={w}"`.
- `control.rs::pump_log` — every FIRST-occurrence anchor application:
  `"anchor applied for u={u} via carrier w={w} at control round={round}"`.
- `control.rs::on_completion_reportable` — every M-carrying view this node itself
  completes and is about to report: `"own CompReport for carrier w={w}"`.

### The timeline (condensed; full log at
`/private/tmp/claude-501/.../scratchpad/findingA-timeline.log`)

```
T+1  node-0  entered=5964   a_i=5965   cursor=3   round=3    delivered=0    consume=0
T+1  node-2  entered=1      a_i=1      cursor=1   round=1    delivered=0    consume=0
T+2  node-0  entered=9472   a_i=9473   cursor=7   round=7    delivered=2    consume=2
T+3  node-0  entered=12095  a_i=12094  cursor=11  round=15   delivered=8    consume=8
T+4  node-0  entered=14048  a_i=14048  cursor=11  round=19   delivered=11   consume=11
T+10 node-0  entered=20628  a_i=20628  cursor=11  round=43   delivered=29   consume=29
T+30 node-0  entered=29963  a_i=29962  cursor=11  round=119  delivered=86   consume=86
T+60 node-0  entered=33956  a_i=33957  cursor=11  round=215  delivered=158  consume=158
```
(node-1/node-2 track node-0 within a few hundred views throughout — not shown per-row
for brevity, see the full log.)

**Reading this**: `entered`/`a_i` (the WISH pacemaker's own entry frontier / the
responsive proposal frontier) race to **~34,000 views in 60s** — thousands of views per
second, nowhere near "gated every 4th view." `cursor` (the output linearizer) reaches
**11 by T+3 and then never advances again for the remaining 57 seconds** of the run,
while `round`/`delivered`/`consume` (the resolution control-log's own internal state)
keep climbing steadily throughout (`round`: 3→215, ~3.6 rounds/s average) — **the
control log is not globally stuck**, it just never produces the one specific anchor
`cursor` is blocked on. `delivered` and `consume` track each other exactly at every
sample (no persistent fetch-pending gap) — `pump_log` is never blocked waiting on a
missing `B_w`; it's processing every delivered entry immediately, just not entries that
resolve view 11.

Final `SUMMARY` for this run: `Node {0,1,2} seal routes: anchor_skip=2,
direct_full=25505` — only **2** dead views (views 3 and 7, the ones before cursor's
stuck point) were ever resolved via the anchor route in the whole 60s run, out of the
~8,500 dead views a crash-every-4th-view pattern implies by the time `entered` reaches
34,000.

### This REFUTES the orchestrator's WISH-round-trip hypothesis

The analysis to verify predicted: dead-view gating makes every subsequent view
WISH/entry-gated, ~θR (900ms at Δ=150) serial cost per dead view, steady state ≈ 4
views/s. The data shows the opposite at the entry/frontier layer: WISH's
"floor `a_i` to `v−1` on formal entry" (W5(c), `Frontier::enter`) does exactly what
PHASE5-SPEC.md documents — it lets `a_i` (and hence new proposals) skip cleanly past a
dead view's own slot in a few hundred milliseconds, not stall on it. The AGB/WISH layer
is not the bottleneck at all; it is, if anything, running far too fast **relative to**
a completely different, downstream component.

### Root cause: unbounded redundant-recovery-carrier flood overwhelms the control log's smallest-view-first delivery queue

A second, shorter (15s) run with the three new diagnostic logs enabled
(`RUST_LOG=error,primary::vantage::resolve=info,primary::vantage::control=info`, full
log at `.../scratchpad/findingA-resolverlog.log`) shows the mechanism directly. In that
run the first dead view was `u=1`:

- `"recovery target u=1 ..."` fires **1,242** times over about 1 second (T≈26.232s→
  27.138s wall clock), then **3** `"anchor applied for u=1 via carrier w=5156 at
  control round=7"` lines (one per live node) — u=1 resolves quickly, while the queue
  is still small.
- Immediately after, the target shifts to `u=5` (the next dead view): `"recovery target
  u=5 ..."` fires **5,664** times over the remaining ~13-14s of the run, with **zero**
  `"anchor applied for u=5"` lines ever — u=5 never resolves in the run's duration.
- `"own CompReport for carrier w=..."` fires **20,718** times in 15s (≈1,381/s) — this
  is the rate at which NEW, almost-entirely-redundant M-carrying reports are minted.
- The control log's own round-advance rate is ≈3.6-3.65 rounds/s in both runs (`round`
  climbing roughly linearly: 3→7→15→...→215 over 60s in the first run) — bounded by the
  Bracha-RBC/Simple-IT round's own message round trips (echo, ready, deliver, commit),
  **not** by the 6Δ=900ms timeout (rounds are completing well under that, via genuine
  quorum, not fallback).

Putting it together: every correct proposer's OWN turn independently runs `Resolver::
decide`, which (per PHASE6-SPEC.md §4, correctly implemented) targets "the FIRST
unresolved view ≤ w−3" on every OTHER qualifying turn. As long as some view (here u=1,
then u=5, ...) stays the oldest unresolved one, EVERY such qualifying turn — and
qualifying turns occur in lockstep with the WISH-unthrottled, thousands-of-views/s
`a_i`/`entered` race documented above — mints a **brand-new, entirely redundant**
recovery attempt for the SAME target, each riding on a different carrier view `w`
(round-robin proposer schedule gives every view a distinct proposer). Each carrier's
own genuine completion generates its own `CompReport`, entering the control log's
`reports` census as an independently "submittable" candidate. `ControlLog::
pick_submittable_value` (§5, correctly implementing the paper's rule) always proposes
the **globally smallest-numbered** still-undelivered submittable pair — a safety-
motivated rule with no way to distinguish "this carrier's target is still open" from
"this carrier is now moot, its target was already anchored by an earlier, smaller `w`".
Because new (moot) carriers are generated at ~1,000+/s while the control log's own
delivery throughput is fixed at ~3.6/s, the backlog of pending-but-moot carriers for
the *previous* target grows without bound, and the control log spends its entire
(tiny, fixed) delivery bandwidth working through them — one smallest-`w` moot carrier
per round — falling further and further behind ever reaching a still-useful carrier
for the *current* unresolved view. This is not merely slow: it is a **self-reinforcing,
effectively unbounded queue-growth pathology**. The faster the (uncapped) AGB fast path
races ahead, the more redundant carriers per second get minted, the deeper the backlog
for whichever view is currently stuck, and the *less* likely resolution ever completes
within any realistic run length — exactly matching u=1 resolving in ~1s (queue still
near-empty) and u=5 (and, in the 60s run, u=11 and everything after it) never resolving
at all.

A secondary, smaller contributing factor: the control log's own ~270ms/round latency on
a loopback benchmark (no real Δ=150 network delay) is itself higher than a bare
Bracha-RBC round trip "should" cost at true zero latency, suggesting some scheduling
contention from the single `VantageCore::run` task queue being saturated with the
fast-path's own thousands-of-messages/s traffic — see Finding B's note on CPU
saturation for the same class of effect. This is secondary: even an arbitrarily fast
control log still eventually loses this race, since the carrier-generation rate itself
grows with the (also uncapped) AGB entry rate.

### Ranked candidate remedies (diagnosis only — none implemented; all need confirmation before any code change)

1. **(ii) implementation-only, deterministic-equivalent — suppress redundant in-flight
   recovery attempts per target.** Track, per node, whether a recovery entry for the
   CURRENT globally-oldest unresolved target `u` is already "in flight" (some carrier's
   `CompReport` already broadcast for it, not yet anchored); while in flight, `Resolver
   ::decide` should not mint another duplicate attempt for the SAME `u` (the alternation
   bit/pointer machinery is otherwise unaffected, and still applies normally to whatever
   DIFFERENT target becomes oldest next). This directly attacks the >1,000:1
   attempt-to-success ratio measured above. "Deterministic-equivalent" because it
   changes *how many* redundant copies of the same, deterministically-agreed-upon entry
   get created, not *which* entry ever gets chosen/justified — but PHASE6-SPEC.md §4's
   literal phrasing ("at OUR proposer turn... the bit selects data-only vs recovery")
   is per-turn, not per-outstanding-target, so this is a re-interpretation the paper
   author should confirm doesn't weaken any liveness argument that assumed every
   qualifying turn independently attempts recovery (e.g., as redundancy against a
   single carrier's own proposer being — impossible here, since proposers are already
   distinct per view, but flag it anyway).

2. **(ii) implementation-only, deterministic-equivalent — let control-log delivery
   skip already-moot submittable pairs.** Before `pick_submittable_value` treats a
   reported pair as a delivery candidate, first check (cheaply — `proposal.m`'s
   `target_view()` against `self.anchored`) whether its own resolution target is
   already anchored, and if so, drop it from consideration for delivery entirely
   (`delivered_set`/`in_chain` bookkeeping still needs to mark it as "seen", but it need
   not consume a full control round). Cheaper/more surgical than #1 (touches only the
   delivery-selection heuristic, not the resolver's own per-turn decision), same
   paper-author sign-off need (this is "smallest still-useful view" rather than the
   paper's literal "smallest view", a semantic change to §5's rule even though the
   final delivered-log content is presumably unaffected).

3. **(iii) protocol-semantic — bound how many nodes may concurrently attempt recovery
   for the same target.** E.g. tie eligibility to attempt recovery for `u` to the
   control log's own round-robin control-leader schedule (already exists for §5) rather
   than "every correct proposer's own turn, independently" — ties the *rate* at which
   new recovery attempts are minted to the control log's own throughput by
   construction, removing the structural mismatch between an unthrottled AGB entry rate
   and a throughput-bounded resolution log. This is the fix that scales (the other two
   only reduce backlog, they don't cap its growth rate), but it changes §4's specified
   per-turn mechanism as literally read — needs the paper author's ruling, out of scope
   for this diagnosis.

No **(i) benchmark-config** remedy exists: nothing in `local-benchmark`'s current CLI
surface throttles the AGB fast path's own view-production rate independently of the
transaction `--rate` (and throttling via `--rate` would only delay, not fix, the same
eventual unbounded-backlog collapse — it doesn't change the exponent, just the
constant).

---

## §findingB — fault-free `anchor_skip` under CPU saturation: hypothesis NOT reproduced on this host; methodology rule adopted anyway

**Repro**: `./target/release/node local-benchmark --protocol vantage --nodes 4
--workers 1 --rate 240000 --tx-size 512 --duration 60/30` fault-free (`--crash 0`,
default), once at `--delta-ms 150` (×3 trials) and once at `--delta-ms 1000`.

| Δ (ms) | Trial | Consensus TPS | Avg latency | Seal routes (summed) |
|---|---|---|---|---|
| 150 | 1 (60s) | 240,753 tx/s | 53.73 ms | `direct_full=54,454 fast_full=162,322` |
| 150 | 2 (30s) | 240,847 tx/s | 48.34 ms | `direct_full=41,498 fast_full=123,441` |
| 150 | 3 (30s) | 241,045 tx/s | 48.23 ms | `direct_full=40,717 fast_full=124,478` |
| 1000 | 1 (60s) | 175,696 tx/s | 2,301.07 ms | `direct_full=119,451 fast_full=76,761` |

**None of the four trials ever produced an `anchor_core`/`anchor_skip`/`direct_core`
entry** — every view in every trial sealed via the two happy-path routes
(`fast_full`/`direct_full`), zero fallback timers ever fired. This host has 14 logical
cores and evidently enough headroom that Δ=150 fault-free simply doesn't trip the
θE/θR fallback deadlines here, in 3/3 trials — the specific "occasional refusal-deadline
trip" the orchestrator's analysis predicted was **not reproduced on this machine**.
This doesn't refute the underlying mechanism (Δ models a network+processing bound;
150ms is tight for a real testbed and is only as safe locally as the host's own
scheduling jitter permits), it just means this diagnosis session's hardware happened to
have enough slack. A more loaded host, a laptop under thermal throttling, or a CI
runner with fewer/shared cores could still trip it — the mechanism is real even though
this run's numbers are clean.

**An unexpected, useful corollary**: Δ=1000 is not merely "safer", it is also
**substantially slower fault-free** (175.7k vs 240.8k tx/s, avg latency ~2.3s vs
~50ms) — a ~27% throughput drop and ~40× latency increase, with **zero** stddev in the
Δ=1000 latency (`stddev 0.00`), suggesting the happy path itself is bottlenecked on a
Δ-scaled fixed wait somewhere (some lock-release/positive-gate timing window appears to
scale with Δ even when quorum is available immediately), not just the fallback
timeouts. So "just set Δ very large to be safe" is not a free methodology choice — it
directly costs throughput/latency even in a completely fault-free, uncontended run.

### Methodology rule (adopted; not a code change)

- **Capacity probes run locally must use a Δ large enough that fallback timers (θE/θR,
  control-round timeout) structurally cannot trip from ordinary local scheduling
  jitter** — i.e., large relative to this host's own observed scheduling variance, not
  a fixed universal number. On this diagnosis host, Δ=150 already happened to be clean,
  but that is host-specific luck, not a property to rely on generally.
- **Δ=150 (or any network-realistic Δ) is for the remote testbed, or for local runs
  explicitly measuring latency-under-realistic-Δ below saturation** — not for local
  peak-throughput capacity probes, where the goal is measuring the protocol's ceiling,
  not incidentally exercising the fallback path due to a laptop's own jitter.
- **Do not treat "a large Δ" as strictly conservative/safe-and-otherwise-free**: this
  run shows a large Δ has its own, non-trivial throughput/latency cost fault-free.
  Recommend picking the *smallest* Δ that stays clean (zero anchor/core routes) across
  a few repeated trials on the actual measurement host, rather than reflexively
  maximizing Δ "to be safe."

---

## §optional — per-pair latency TABLE (upgrade, supersedes the earlier uniform-only `--mimic-latency-ms`)

**Coordinator upgrade, mid-session**: the initial uniform-flat-delay `--mimic-latency-ms`
(global `AtomicU64`, one number for every link) was superseded by a proper per-pair
latency TABLE — starfish-style, applied to both protocols identically (the fairness
point for WAN-shaped local runs before the AWS campaign). Findings A and B were both
already complete cleanly at the time of the upgrade. Reference (read-only):
`~/code/starfish/crates/starfish-core/src/network.rs`'s `generate_latency_table` +
per-connection `extra_connection_latency` field/application.

### Design

- **`config/src/lib.rs`** — new `LatencyTable` type: an n×n one-way-ms matrix, indexed
  by committee order (`Committee::index_of`, the SAME deterministic
  `BTreeMap<PublicKey, _>` order `Pacemaker`/`ControlLog::control_leader`/`Resolver`
  already rely on for their own party-indexed state — a CSV's rows/columns line up
  with `committee.json` identically on every node). Two constructors:
  - `LatencyTable::from_rtt_csv(path, n)` — parses an n×n **round-trip**-ms CSV (no
    header row, blank lines skipped), halving every cell to the one-way value this
    table stores; errors (wrong dimensions, non-numeric cells) via the existing
    `ConfigError::ImportError`.
  - `LatencyTable::uniform(n, rtt_ms)` — the trivial table `--mimic-latency-ms` builds:
    every off-diagonal cell = `rtt_ms` (same halving convention, so the flag is defined
    as *exactly* equivalent to a uniform CSV of that value — one mental model for both
    flags).
  - `LatencyTable::one_way(i, j) -> Duration`.
- **`config::Committee`** gains three new methods: `index_of(name)` (committee-order
  position); `addresses_of(name)` (every socket address an authority's primary +
  workers listen on — latency is modeled per AUTHORITY pair, not per individual
  service port); `latency_map(myself, table) -> HashMap<SocketAddr, Duration>` (every
  OTHER authority's addresses mapped to `table.one_way(index_of(myself),
  index_of(other))` — resolved relative to whichever node calls it).
- **`config::Parameters`** gains `latency_table: Option<Arc<LatencyTable>>`,
  `#[serde(skip)]` (never round-trips through `parameters.json`/`fab`; `None` — also
  what every EXISTING parameter file deserializes to — means zero injected delay,
  byte-identical current behavior, satisfying invariant 4).
- **`network` crate** (`ReliableSender`/`SimpleSender`): each gains an OPTIONAL
  `latency: HashMap<SocketAddr, Duration>` field (default empty) and a
  `with_latency(map)` builder. `spawn_connection` looks up the destination's entry
  ONCE at connection-spawn time and passes it into the `Connection`, which stores it
  as `extra_latency` and `sleep`s it (no-op when zero/absent) immediately before every
  real socket write for that connection's whole life — per-connection, so unrelated
  links (any address not in the map — every address, by default) are completely
  unaffected, and it's applied inside the SAME dedicated per-destination task that
  already serializes that link's own message stream (strict per-link ordering,
  unaffected by this addition — see the important caveat below on why this was kept
  fully serial rather than mirroring starfish's own concurrent-per-message design).
- **`primary/src/core.rs`** (Autobahn) and **`primary/src/vantage/node.rs`**
  (`VantageCore`) each resolve their OWN `committee.latency_map(&name, table)` (from
  `parameters.latency_table`, when set) and call `.with_latency(map)` on their
  `ReliableSender`/`SimpleSender` — the exact same call, for both protocols, the
  fairness point. `Core::spawn` gained one new trailing parameter (`latency_map:
  HashMap<SocketAddr, Duration>`, resolved by its caller `Primary::spawn` since
  `Core::spawn`, unlike `VantageCore::spawn`, doesn't otherwise take a `Parameters`);
  `VantageCore::spawn`'s signature is unchanged (it already took `parameters`).
- **CLI** (`node/src/main.rs`, `node/src/local_benchmark.rs`): `--latency-table <PATH>`
  (n×n RTT-ms CSV, takes precedence) and `--mimic-latency-ms <u64>` (uniform
  shorthand, RTT-ms, default `0` = off) on `local-benchmark`.
- **`node/Cargo.toml`**: `network` added as a direct dependency (previously reached
  only transitively).

### Verification

- **Default-off / invariant 4 (Autobahn regression, the required check)**:
  `autobahn-optimistic`, 4 nodes, `--rate 240000 --tx-size 512 --duration 60`, no
  latency flags → **241,095 tx/s**, avg 56.08 ms. TPS matches the recorded gate range
  (239,786–240,997 tx/s) within noise — the empty-map default path is confirmed
  behavior-preserving. (Avg latency here reads lower than PHASE4-NOTES.md's originally
  recorded ~110 ms; TPS — the actual invariant this check exists for, "did anything
  touching `Primary::spawn` change Autobahn behavior" — matches tightly, and every
  vantage run this session at the same rate/Δ also lands in the same 46–56 ms band, so
  this reads as host/session-level noise on the latency figure, not a regression from
  this change, which is a no-op on the default empty-map path by construction: one
  extra `HashMap::get` + `is_zero()` check per send, zero sleeps ever executed.)
- **CSV path**: a hand-written 4×4 RTT=10ms-off-diagonal CSV, loaded via
  `--latency-table`, produced results matching the equivalent `--mimic-latency-ms 10`
  run closely (226.84 ms vs 233.41 ms avg) — confirms the CSV parse → `LatencyTable` →
  `Committee::latency_map` → `with_latency` chain works end to end. A malformed
  (wrong-dimension) CSV fails cleanly with a descriptive error before any node spawns.
- **"Shifts by roughly the injected amount" (the requested loose-bound check) — holds
  only in a small-value regime; does NOT hold at WAN-realistic values, for a
  structural reason documented below.** Four vantage, fault-free, `--rate 240000
  --delta-ms 150` trials (baseline avg 46.26 ms, 241,281 tx/s):

  | `--mimic-latency-ms` (RTT) | one-way | Consensus TPS | Avg latency | vs. baseline |
  |---|---|---|---|---|
  | 0 (baseline) | 0 | 241,281 tx/s | 46.26 ms | — |
  | 1 | 0.5 ms | 241,600 tx/s | 56.25 ms | +10 ms — throughput-preserving, roughly proportional |
  | 10 | 5 ms | 238,795 tx/s | 233.41 ms | +187 ms — throughput-preserving, but already ~40× the raw one-way value |
  | 50 | 25 ms | **1,793 tx/s** | **1,423.51 ms** | throughput **collapsed 134×**, latency +1,377 ms |

  (A parallel pair of low-nominal-rate, Δ=1000 trials shows the same pattern: 754.87 ms
  → 5,512.82 ms, a 7.3× jump, at the same 25 ms one-way value — confirming the effect
  tracks the underlying *consensus message rate*, which is largely independent of
  the client transaction rate, not transaction volume.)

### The structural reason, and why it was NOT "fixed" to force a clean number

Starfish's own reference (`handle_write_stream`, `~/code/starfish/crates/starfish-core/
src/network.rs` lines ~536–685) explicitly does **not** block its single write task on
each message's delay — for the latency-simulation path it spawns a bounded (`JoinSet`,
`MAX_IN_FLIGHT` cap), *concurrent* per-message task that sleeps-then-writes, with an
explicit comment: *"preserve the many in-flight messages behavior... but keep
concurrency bounded"*. That decouples a link's *bandwidth* (how many messages/sec it
can carry) from its *latency* (how long each one takes), matching how a real network
behaves — and starfish's own `generate_latency` adds random jitter per message, so
strict message-arrival ORDER on a link is explicitly not preserved there either.

This implementation instead keeps each connection's send loop **fully serial** (sleep,
then write, one message at a time, in submission order) — a deliberate choice, not an
oversight: `ReliableSender`'s ack-correlation protocol (`keep_alive`'s
`pending_replies.pop_front()`) assumes strict FIFO correspondence between sent messages
and the acks that come back for them. Spawning concurrent per-message send tasks (even
with an identical, non-jittered fixed delay) has no language-level guarantee of
preserving that ordering under the scheduler, which would risk silently corrupting the
ack-matching invariant — a correctness bug, not merely a fidelity gap. Starfish's own
raw byte-stream protocol has no equivalent invariant to protect, which is presumably
why it could take the concurrent-task approach safely.

**Consequence**: a fully serial per-connection model makes the fixed one-way delay a
hard **throughput ceiling** of `1 / one_way_delay` messages/sec on that link, not
merely an added latency — and Vantage's own happy path generates on the order of
thousands of consensus messages/sec per peer connection even fault-free at Δ=150 (per
Finding A's own timeline). Once the configured one-way delay pushes that ceiling below
the connection's actual offered message rate, the connection's internal channel queues
up and total latency/throughput degrades far more than the raw injected number would
suggest — exactly what the 10 ms→50 ms RTT jump shows crossing that threshold. This is
a genuine, useful finding for Phase-7 WAN-shaped-run planning, not a bug to silently
patch around: it means **peak-throughput capacity probes and WAN-realistic latency
values are in tension** under the current one-TCP-connection-per-peer, strictly-ordered
design, the same general class of "a fixed serial resource can't keep up with an
unthrottled producer" issue Finding A's crash-fault diagnosis surfaced independently.

**Methodology guidance** (mirrors Finding B's rule): use `--latency-table`/
`--mimic-latency-ms` for latency-shaped runs at a `--rate` low enough that the
per-connection consensus message rate stays well under `1 / one_way_delay`; do not use
it for peak-throughput capacity probes, where it will produce a throughput collapse
dominated by this queueing effect rather than a clean, realistic WAN-latency picture.

---

## §tests — full suite, after every change above

`CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 -- --test-threads=4`, re-run
immediately before writing this section (so it reflects every file listed in §changes
below): **all green**, identical per-crate counts to the pre-existing baseline recorded
in PHASE6-NOTES.md — `config` 0, `crypto` 7/7, `metrics` 0, `network` 6/6, `node` 0,
`primary` 159 passed / 6 ignored (pre-existing, unchanged), `store` 4/4, `worker` 6/6.
**0 failed anywhere**, both before this session's edits and after.

Also rebuilt `cargo build --workspace` (both with and without `--features benchmark`)
clean (pre-existing warnings only, no new ones) before every benchmark run.

---

## §changes — everything touched this session (diagnosis-only per the hard rules; no protocol-semantic edits, no git writes)

Metrics-only / benchmark-harness-only, as required:

- `metrics/src/metrics.rs` — six new `IntGauge` fields on `Metrics` (Finding A), always
  registered, same pattern as the pre-existing Phase-3/6 counters.
- `metrics/src/snapshot.rs` — `VantageProgress` struct + `read_vantage_progress`.
- `metrics/src/lib.rs` — re-exports for the above.
- `primary/src/vantage/pacemaker.rs` — new `pub fn entered_view(&self) -> View`
  accessor (metrics-only; mirrors the existing `#[cfg(test)]` one).
- `primary/src/vantage/control.rs` — new `pub fn curr_round`/`delivered_log_len`/
  `consume_pos` accessors (metrics-only); three diagnostic `log::info!` lines (no
  behavior change, see §findingA).
- `primary/src/vantage/resolve.rs` — one diagnostic `log::info!` line (no behavior
  change, see §findingA).
- `primary/src/vantage/node.rs` — `VantageCore` keeps one more `Arc<Metrics>` clone; a
  new 1s `tokio::time::interval` tick in the run loop calling a new, effect-free
  `sample_metrics(&self)` method. No change to any existing branch's behavior/ordering.
- `node/src/main.rs` — new `--timeline`, `--mimic-latency-ms`, `--latency-table` flags
  on `local-benchmark` (all off/0/unset by default; existing flags/behavior unchanged).
- `node/src/local_benchmark.rs` — `--timeline` prints a once/sec progress line per live
  node instead of a single silent sleep (only when the flag is passed); parses
  `--latency-table`/`--mimic-latency-ms` into a `LatencyTable`, set on the in-memory
  `Parameters` every node's `Primary::spawn` receives.
- `node/Cargo.toml` — added `network` as a direct dependency (previously transitive
  only).
- `network/src/lib.rs`, `network/src/reliable_sender.rs`,
  `network/src/simple_sender.rs` — the per-pair latency injection point: `with_latency`
  builder + per-connection `extra_latency` field/sleep (see §optional; no-op with the
  default empty map).
- `config/src/lib.rs` — `LatencyTable` type (`uniform`/`from_rtt_csv`/`one_way`);
  `Committee::index_of`/`addresses_of`/`latency_map`; `Parameters.latency_table`
  (`#[serde(skip)]`, `None` default).
- `primary/src/core.rs` — `Core::spawn` gained one trailing parameter (`latency_map:
  HashMap<SocketAddr, Duration>`), applied via `.with_latency(..)` on its
  `ReliableSender`. New `use std::net::SocketAddr` / `std::time::Duration` imports.
- `primary/src/primary.rs` — resolves `parameters.latency_table` into a `latency_map`
  and passes it to `Core::spawn` (Autobahn arm); the `Protocol::Vantage` arm needed no
  change here (`VantageCore::spawn` already took `parameters` and resolves its own map
  internally).
- `primary/src/vantage/node.rs` — resolves its own `latency_map` from
  `parameters.latency_table` + `committee`, applied to both `network` (`ReliableSender`)
  and `worker_network` (`SimpleSender`) via `.with_latency(..)`.
- `primary/src/tests/core_tests.rs` — the 12 real (non-commented-out) `Core::spawn(...)`
  call sites each gained one trailing `HashMap::new()` argument (empty = current
  behavior, matching every test's existing expectations) to match the new signature; a
  further 3 call sites inside a large pre-existing `/* ... */`-commented-out test
  (`process_special_header` et al., already dead/non-compiling code predating this
  session) were left untouched (not part of the compiled surface either before or
  after).

No `tex-projects` file touched. No `starfish` file touched (read-only reference only,
via `grep`/`Read`). No git command run (`git status`/`log`/`branch` for orientation
only). All benchmark runs used `CARGO_BUILD_JOBS=4` builds; no two builds/benchmark
processes run concurrently. `.local-bench-findingA*`, `.local-bench-findingB-*`,
`.local-bench-lt-*`, `.local-bench-latency-baseline*`, `.local-bench-autobahn-
regression`, `.local-bench-mimic-smoke` data dirs left on disk under the repo root
(gitignored, harmless, safe to delete) — full stdout logs for every run are under
`/private/tmp/claude-501/.../scratchpad/{finding,lt,autobahn,latency,mimic}*.log`.

---

## §remedies — coordinator rulings on Finding A + the latency-table fix, implemented

Three rulings on Finding A's ranked remedies, one mandatory fix for the latency-table
serial-FIFO ceiling, one investigate-only item. All implemented below except remedy #3
(explicitly not sanctioned) and the Δ=1000 investigation (report only, per
instruction).

### D7-1 — Remedy #1, SANCTIONED WITH A MANDATORY TIME BOUND

**Ruling**: in-flight suppression of redundant recovery attempts must be
time-bounded (expiry = 12Δ), never open-ended — `thm:resolution-live`'s structure
needs every fixed, oldest-qualifying target attempted infinitely often; O3 only
guarantees anchoring for proposals that complete at EVERY correct party, so a carrier
that completed non-universally could look permanently "in flight" to this party, and
open-ended suppression would then extinguish the attempt stream for that target — a
liveness loss. A bounded expiry keeps attempts infinitely-often in the limit (only
throttles the mint rate, never which entries are ever chosen), so the liveness
argument survives; still flagged for the paper author.

**Implementation** (`primary/src/vantage/resolve.rs`):
- `Resolver` gains `in_flight: HashMap<View, Instant>` + `expiry: Duration` (`12 *
  delta_ms`, so `Resolver::new` now takes `delta_ms: u64` — one new call-site
  parameter, threaded through `VantageCore::spawn` from `parameters.delta_ms`, same
  pattern `AgbEngine`/`ControlLog` already use).
- `decide`'s scan gains one more skip condition, `is_in_flight(u, now)`, checked
  exactly like the pre-existing "empty candidates" skip — a suppressed target never
  blocks a later one, and the alternation bit is untouched when every candidate is
  either resolved, empty, or suppressed (`decide` gained a `now: Instant` parameter for
  this — threaded from the caller's own `now`, same as every other Vantage timer/effect
  call already does).
- `decide`'s own successful recovery-attempt branch inserts `in_flight[u] = now`
  immediately (our own attempt is itself fresh in-flight evidence).
- New `pub fn note_carrier_report(&mut self, u, now)` — called from
  `VantageCore::execute`'s `Effect::CompletionReportable` handler (extracting
  `proposal.m`'s `target_view()` when `Some`) BEFORE forwarding to
  `ControlLog::on_completion_reportable` — this is the "observed CompReport for a
  carrier resolving u" evidence the ruling names: the FIRST genuine completion of ANY
  carrier (ours or another party's) with `M` targeting `u`, independent of whether this
  party's own `decide()` ever attempted `u` itself.
- Two new unit tests (`primary/src/vantage/tests/resolve_tests.rs`):
  `d7_1_in_flight_suppression_blocks_reattempt_then_expires` (attempt → immediate
  reattempt suppressed, bit untouched → past-expiry reattempt succeeds, refreshing the
  marker) and `d7_1_note_carrier_report_suppresses_like_our_own_attempt` (externally
  noted evidence suppresses identically to a self-made attempt).
- All pre-existing `resolve_tests.rs`/`harness.rs`/`crash_fault_tests.rs`/
  `byzantine_tests.rs` call sites updated for the new `Resolver::new`/`decide`
  signatures: unrelated unit tests use `delta_ms=0` (expiry=0 → suppression provably
  never triggers, since `duration_since(t) < 0` is never true — a clean way to say "not
  exercising D7-1 here" without changing those tests' own assertions/semantics); the
  shared harness (`Node::try_propose_effects`) and the crash-fault/byzantine direct
  `Resolver::decide` call sites thread their own already-in-scope `Instant`
  (`TEST_DELTA_MS=100`, matching `ControlLog`'s own test constant).

### D7-2 — Remedy #2, SANCTIONED AS-IS

**Ruling**: leader-side "smallest still-useful view" selection is safe as proposed —
which submittable pair a control leader proposes is already a free local choice (any
submitted pair is valid; Bracha + the echo validity gate carry safety regardless), so
this changes leader heuristics only.

**Implementation** (`primary/src/vantage/control.rs`, `pick_submittable_value`): one
new skip condition — a reported pair's own `proposal.m` target already in
`self.anchored` is moot (an earlier, smaller-view carrier already resolved it) and is
now excluded from consideration entirely, rather than winning the "smallest view"
comparison and burning a leader's own limited per-round proposal bandwidth
re-delivering a no-op. `delivered_set`/`in_chain` bookkeeping and `pump_log`'s own
`self.anchored.contains(&u)` skip are both unchanged — a moot pair some other
(unpatched, or simply earlier) leader still proposes is handled exactly as before.

### D7-3 — mandatory fix for the latency-table serial-FIFO throughput ceiling

**Ruling**: switch to starfish-style concurrent delayed delivery — schedule each
message's delivery at `send_time + one_way_delay` via a per-connection delay queue;
since every message on a link gets the SAME delay, FIFO is preserved naturally by
construction (no jitter needed, unlike starfish's own per-message concurrent-task
version), so `ReliableSender`'s ack correlation is unaffected while messages pipeline
like a real network.

**Implementation** (`network/src/reliable_sender.rs`, `network/src/simple_sender.rs`):
replaced the earlier "synchronous `sleep` before each write, inside the single
per-connection task" version (which capped a link's throughput at `1 / one_way_delay`
messages/sec — the finding that motivated this fix) with a plain FIFO `VecDeque<
(Instant, Bytes, ..)>` delay queue: arrivals are scheduled (`Instant::now() +
extra_latency`) IMMEDIATELY (cheap, no send, so a new arrival is never blocked behind
an earlier message's still-pending delay), and the connection's own `tokio::select!`
gets a new branch that fires exactly when the queue's FRONT becomes due, at which
point (and only then) the real `writer.send(...)` happens. Because the delay is
IDENTICAL for every message on a given link (no jitter), arrival order strictly implies
release-order (`t2 > t1 => t2+d > t1+d` for constant `d`), so this plain queue
preserves strict per-link FIFO order by construction — `ReliableSender`'s
`pending_replies` ack-correlation invariant (which assumes writes and their acks
correspond in strict send order) is therefore unaffected, unlike starfish's own
per-message-task-with-jitter design, which would have carried that risk. The
zero-latency default path (`extra_latency.is_zero()`) is kept as a byte-identical,
completely separate code path (`keep_alive_immediate`/the original `SimpleSender::run`
body unchanged) — no scheduling overhead at all when no latency table is configured,
not even one extra `Instant::now()` call.

**Verification** (vantage, fault-free, `--rate 240000 --delta-ms 150`, 20s):

| Config | Consensus TPS | Avg latency |
|---|---|---|
| baseline (no latency table) | 241,281 tx/s | 46.26 ms |
| `--mimic-latency-ms 50` (25 ms one-way), OLD serial-sleep version | **1,793 tx/s** | **1,423.51 ms** |
| `--mimic-latency-ms 50` (25 ms one-way), NEW delay-queue version (D7-3) | **240,498 tx/s** | **122.88 ms** |

D7-3 fully eliminates the throughput collapse: TPS matches the no-latency baseline
almost exactly (240,498 vs 241,281), and the latency shift (+76.6 ms) is now in a sane,
explainable range for a 25 ms one-way delay riding along a handful of message hops in
the echo/ready/quorum round trip (roughly 3 hops × 25 ms ≈ 75 ms, matching closely).
`network` crate's own 6/6 tests still pass; Autobahn regression re-confirmed clean
after these changes (240,562 tx/s, 30s run) — invariant 4 holds.

---

## §delta-1000-investigation — mechanism found: an O(n)-scanned, never-pruned timer queue whose steady-state size scales with Δ (report only, no fix applied)

**The question**: why does fault-free Δ=1000 cost ~27% throughput and ~40× latency
(with `stddev 0.00`) relative to Δ=150 at the same offered rate, and why does the
`fast_full`/`direct_full` seal-route mix flip?

**Ruled out first** (targeted diagnostic logging, `primary::vantage::agb`, gated
`log::info!`, no behavior change — see §changes-2): added a log line at BOTH echo
emission sites — the organic grade-1 positive-gate echo (`recheck_gate`) and the
Δ-scaled fallback grade-0 echo (`on_echo_fallback_timer`, armed at `min(entry+Δ,
entry+θE)` on every genuine proposal fixing, PHASE5-SPEC.md W5(b)'s carry-over fix).
At Δ=1000, both at a non-saturating rate (20,000 tx/s, 8s: 69,364 organic echoes, **0**
fallback echoes, normal ~65 ms latency) and at the saturating rate used in Finding B
(240,000 tx/s, 8s: 66,313 organic echoes, **0** fallback echoes, normal ~57 ms latency,
`fast_full`-dominant) — **the Δ-scaled fallback path never fires**. So the happy
path's own echo/ready logic (`positive_gate_holds`/`tip_ok`/`core_ok`, all pure
local-data-availability checks, zero Δ dependence) is not the mechanism.

**The actual signal**: the SAME configuration (240,000 tx/s, Δ=1000) that showed
normal behavior in an 8-second snapshot showed the anomaly (2,301 ms avg, `stddev
0.00`, `direct_full`-dominant) over a 60-second run in Finding B, and — decisively — a
160,000 tx/s run (offered rate BELOW the ~176k tx/s the 8s/60s snapshots both showed as
the apparent ceiling) still degraded to 114,364 tx/s sustained over 60s with the exact
same `2,456.70 ms avg, stddev 0.00` signature. This is NOT a fixed per-view Δ-scaled
delay on the happy path (ruled out above) — it is a **time-dependent degradation that
worsens the longer the run continues, even below the apparent short-run ceiling**.

**Root cause, confirmed directly** (one more diagnostic log, `primary::vantage::node`,
logging `self.timers.len()`/`self.control_timers.len()`/`self.cancel_handlers.len()`
once/sec via the existing metrics tick): `VantageCore::run`'s main loop recomputes,
**on every single message/effect processed** (i.e. every iteration of the top-level
`loop`, not once per timer):

```rust
let next_deadline = self.timers.iter().map(|(d, _, _)| *d).min();             // O(timers.len())
let next_control_deadline = self.control_timers.iter().map(|(d, _)| *d).min(); // O(control_timers.len())
```

`timers: Vec<(Instant, View, TimerKind)>` (and `control_timers` identically) is a
plain, linearly-scanned `Vec` with exactly one removal path — `self.timers.retain(|(d,
..)| *d > now)`, run only when an entry's OWN deadline has already elapsed. Every
entered view arms TWO timer entries (`EchoAbsolute` at `entry+θE=5Δ`, `ReadyAbsolute`
at `entry+θR=6Δ`) that sit in the `Vec` for their FULL θE/θR duration **regardless of
whether the corresponding echo/ready already happened organically, far earlier, via
the quorum-driven happy path** — there is no proactive pruning of a superseded timer.
So the `Vec`'s steady-state size is proportional to `(view-entry rate) × (θE + θR) =
(view-entry rate) × 11Δ`, and Vantage's own view-entry rate is already high (thousands
of views/s, per Finding A). At Δ=150, `11Δ ≈ 1.65s` keeps the steady-state `Vec` small;
at Δ=1000, `11Δ = 11s` — roughly 6.7× the reap window for the SAME view rate — and the
observed run confirms it directly:

```
T+0.24s   timers.len()=3
T+1.24s   timers.len()=12,105    control_timers.len()=1,384
T+2.25s   timers.len()=16,861    control_timers.len()=2,338
T+3.27s   timers.len()=20,583    control_timers.len()=3,069
T+5.30s   timers.len()=26,637    control_timers.len()=4,156     (peak region)
T+8.30s   timers.len()=18,872    control_timers.len()=2,215     (view-entry rate has
                                                                  already throttled
                                                                  down from the growing
                                                                  per-event scan cost,
                                                                  so reaping starts
                                                                  winning)
```

`timers.len()` reaches the tens of thousands within ~5 seconds. Since the O(n) min-scan
is paid on EVERY message the node processes (not just once per view), and Vantage
processes many messages per view (echoes, readies, wishes, acks), the CUMULATIVE cost
compounds: a larger `Vec` makes every subsequent message slower to process, which
throttles view-entry rate, but the already-armed backlog still has to be worked
through at its own Δ-scaled pace — a self-reinforcing pathology structurally identical
IN KIND to Finding A's unbounded-queue-growth finding (a linearly-scanned,
never-proactively-pruned collection whose effective size scales with a protocol
constant), just in a completely different subsystem (the event-loop's own timer
scheduling, not the resolution pipeline).

This directly explains both symptoms: (1) throughput/latency degrade over TIME (as the
`Vec` grows) rather than being a fixed per-view cost, matching the 8s-clean/60s-degraded
contrast and the sub-ceiling-still-degrades result; (2) the `direct_full`-over-
`fast_full` route flip is a side effect of the SAME CPU contention — `fast_full`
requires unanimous n-of-n matching echoes to all arrive/process before `try_seal`'s
first-acceptance lock-in, and a node whose event loop is bogged down scanning a 20k+
entry `Vec` on every message is more likely to be the "straggler" that loses the race
to the quorum-only (`2f+1`) `direct_full`/`direct_core` path.

**Classification** (per the standing rubric; NOT implemented, report only per
instruction):
- **(ii) implementation-only, deterministic-equivalent** — replace both `timers: Vec<
  ..>` and `control_timers: Vec<..>` with a proper priority-queue keyed by deadline
  (e.g. `BinaryHeap<Reverse<(Instant, ..)>>` or a `BTreeMap<Instant, ..>`), giving O(1)
  peek-min and O(log n) insert instead of the current O(n) scan on every single event.
  Zero semantic change — the identical timers still fire at the identical deadlines
  producing the identical effects; only the internal data structure's algorithmic
  complexity changes. This is the clean, low-risk fix if sanctioned; no protocol logic
  is touched at all.
- A secondary, smaller observation: proactively CANCELING a timer once its underlying
  event has already happened organically (e.g. drop `EchoAbsolute`'s entry once
  `echo_sent` becomes true, rather than waiting for the deadline to elapse and then
  no-op-ing inside the handler) would shrink the steady-state `Vec` size directly,
  independent of the data-structure choice — a complementary, also implementation-only,
  deterministic-equivalent improvement, but a larger diff (needs a lookup/removal path
  by `(view, kind)`, not just insert+scan-min).
- Not investigated further per instruction: whether `Duration::from_millis(12 *
  delta_ms)` (D7-1's own expiry) or `control_round_timeout = 6Δ` could show an
  analogous, smaller compounding effect under the SAME kind of unpruned-collection
  pattern elsewhere — flagged as worth a follow-up scan if Δ=1000-class runs are ever
  needed for the Phase-7 evaluation.

---

## §findingA-rerun — crash-fault repro with D7-1 + D7-2: qualitative fix confirmed, quantitative bottleneck shifts to the control log's own round latency

**Repro**: identical to the original Finding-A run — `--protocol vantage --nodes 4
--workers 1 --crash 1 --rate 180000 --tx-size 512 --duration 60 --delta-ms 150
--max-batch-delay-ms 20 --max-header-delay-ms 50 --timeline`, now with D7-1 + D7-2
compiled in.

**The permanent stall is GONE.** Condensed timeline (full log at
`.../scratchpad/findingA-fixed-timeline.log`):

```
T+1   node-0  entered=5566   a_i=5566   cursor=1    round=5    delivered=0    consume=0
T+2   node-0  entered=8866   a_i=8867   cursor=9    round=9    delivered=3    consume=3
T+10  node-0  entered=19770  a_i=19770  cursor=73   round=41   delivered=27   consume=27
T+20  node-0  entered=25074  a_i=25074  cursor=153  round=81   delivered=57   consume=57
T+30  node-0  entered=28906  a_i=28906  cursor=233  round=121  delivered=87   consume=87
T+40  node-0  entered=31874  a_i=31875  cursor=313  round=161  delivered=117  consume=117
T+50  node-0  entered=34282  a_i=34283  cursor=385  round=197  delivered=144  consume=144
T+60  node-0  entered=35898  a_i=35898  cursor=465  round=237  delivered=174  consume=174
```

`cursor` **never stalls again** — it advances essentially every second, for the entire
60 s run, reaching view 465 (vs. permanently stuck at 11 before). Final routes: `Node
{0,1,2}: anchor_skip=116, direct_full≈26,922` — **116 dead views successfully
anchor-resolved** over the run (vs. 2 before), tracking `cursor`'s own progress almost
exactly (`465 / 4 ≈ 116`, matching the crash-every-4th-view pattern precisely).
`delivered` (174) vs. anchors (116) shows only a modest ~1.5× redundancy now (a handful
of extra carriers per target within the 12Δ=1.8s suppression window), utterly unlike
the >1,000:1 ratio Finding A measured before D7-1.

**Throughput, however, is still far from "tens of thousands"**: **600 tx/s** (avg
3,686.32 ms, highly variable — `stddev 4,462.96 ms`, p50/p90/p99 1,751/11,017/15,826
ms; 36,000 committed txs over 60 s). This is the honest, unmet part of the
expectation, worth stating plainly rather than rounding up: eliminating the
redundant-carrier flood fixed the QUALITATIVE pathology (unbounded backlog → permanent
stall) but exposed a DIFFERENT, now-binding constraint underneath it — **every dead
view (1 in 4, this crash pattern) still has to pass through the control log's
resolution pipeline once, and that pipeline's own per-round latency (driven by real
Bracha-RBC/Simple-IT round trips, not a fixed timeout — the original Finding-A
write-up noted control rounds completing in ~270 ms via genuine quorum, not the 900
ms=6Δ fallback) is now the pacing constraint on `cursor`'s advance, since Cursor only
ever advances in strict view order and 1-in-4 views require this step.** `cursor`
advancing ~464 views in 60 s (~7.7 views/s) is consistent with resolving one dead view
roughly every ~520 ms — in the right neighborhood of, though somewhat slower than, the
previously-measured ~270 ms/round figure (plausibly because D7-1's own bookkeeping, or
simply more contention from 116 successful anchor cycles instead of 2, adds some
overhead; not investigated further here).

**This is a genuine, distinct follow-on finding, not covered by D7-1/D7-2**: those two
remedies fixed the redundant-attempt VOLUME, not the underlying PER-ROUND LATENCY of
the resolution mechanism itself. Getting to "tens of thousands of tx/s" under a
sustained 1-in-4 crash pattern would additionally require either (a) a much faster
control-log round (implementation-level, if the ~270-500 ms/round figure has genuine
slack rather than being an inherent Bracha-RBC-over-real-message-round-trips floor —
not established either way here), or (b) a protocol-level change to how much of the
committed-transaction pipeline is gated on a single dead view's resolution (e.g.
allowing later, already-sealed views' content to commit independently of an earlier
still-open one — a materially different linearization/output rule, squarely
protocol-semantic, not something this session touched or recommends without the paper
author). Flagged for the coordinator's next ruling; not implemented.

---

## §tests-2 — full suite after D7-1/D7-2/D7-3

`CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 -- --test-threads=4`: **all green** —
`config` 0, `crypto` 7/7, `metrics` 0, `network` 6/6, `node` 0, `primary` **161** passed
(159 baseline + the 2 new D7-1 tests) / 6 ignored (unchanged), `store` 4/4, `worker`
6/6. **0 failed anywhere.** Also re-confirmed the Autobahn regression benchmark clean
after the `network` crate changes (240,562 tx/s, 30 s run — matches the gate range).

---

## §changes-2 — everything touched in this round (still diagnosis + two sanctioned remedies + one mandated fix; no other protocol-semantic edits; no git writes)

- `primary/src/vantage/resolve.rs` — D7-1: `in_flight`/`expiry` fields,
  `Resolver::new`'s new `delta_ms` parameter, `decide`'s new `now` parameter + the
  in-flight skip, `note_carrier_report`.
- `primary/src/vantage/node.rs` — `Resolver::new(committee.size(), parameters.
  delta_ms)`; `decide(agb, view, now, ..)`; `Effect::CompletionReportable`'s handler
  calls `resolver.note_carrier_report`; one new diagnostic `log::info!` in
  `sample_metrics` (`timers.len()`/`control_timers.len()`/`cancel_handlers.len()`, the
  Δ=1000 investigation).
- `primary/src/vantage/control.rs` — D7-2: `pick_submittable_value`'s new
  already-anchored skip.
- `primary/src/vantage/agb.rs` — two new diagnostic `log::info!` lines (organic
  grade-1 echo, Δ-fallback grade-0 echo) for the Δ=1000 investigation; no behavior
  change.
- `primary/src/vantage/tests/resolve_tests.rs` — two new D7-1 tests; every pre-existing
  `Resolver::new`/`decide` call site updated for the new signatures (`delta_ms=0`
  where D7-1 is irrelevant to the test, `Instant::now()` for `now`).
- `primary/src/vantage/tests/harness.rs`, `crash_fault_tests.rs`, `byzantine_tests.rs`
  — same signature updates, threading each test's own already-in-scope `Instant`
  (`TEST_DELTA_MS` for the harness's own `Resolver::new`).
- `network/src/reliable_sender.rs` — D7-3: `keep_alive` split into
  `keep_alive_immediate` (the original, byte-identical, default path) and
  `keep_alive_delayed` (the new FIFO delay-queue path).
- `network/src/simple_sender.rs` — D7-3: the same FIFO delay-queue pattern applied to
  `Connection::run` (no ack correlation to protect here, but the same fix regardless).

No `tex-projects` file touched. No `starfish` file touched (read-only reference only).
No git command run. `CARGO_BUILD_JOBS=4` builds throughout; no two builds/benchmark
processes run concurrently. New data dirs (`.local-bench-d73-*`,
`.local-bench-delta1000-*`, `.local-bench-findingA-fixed`, `.local-bench-timerlen-*`)
left on disk (gitignored). New logs under
`/private/tmp/claude-501/.../scratchpad/{d73,delta1000,findingA-fixed,timerlen}*.log`.
