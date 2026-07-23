## §remote — `fab remote` on AWS: harness repair status and a blocking decision

**Status: STOPPED before provisioning any AWS instances.** Zero EC2 instances were
created. The only AWS-side resource created is one throwaway RSA key pair
(`vantage-smoke-20260723170010`, eu-west-1) — zero cost, trivially deletable.
Verified via `describe_instances` (eu-west-1, all non-terminated states):
**0 instances**, before and after this session's work.

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

### 6. BLOCKING ISSUE — stopped here, needs your decision before any spend

`fab remote`'s deploy step (`Bench._update`, and `Bench.install`) does
`git clone {settings.repo_url}` (or `git pull` if already cloned) on each
remote host, then compiles. This is how code reaches the EC2 instances — there
is no other transfer mechanism in the current harness.

Checked `/Users/nikitapolianskii/code/vantage`'s git state:

- The only configured remote is `upstream` →
  `https://github.com/neilgiri/autobahn-artifact.git` — the **original
  third-party author's repo**, tracked only by a stray `overview` branch. We
  have no push access there, and pushing there would be wrong regardless of
  access.
- `main` (current branch, HEAD `6d77e03`) **is not tracked by any remote at
  all** — it's local-only.
- `git status` shows **117 changed/untracked paths**: this is the entire
  Phase 2–6 protocol work (`primary/src/vantage/`, `config/src/lib.rs`
  `delta_ms` etc., `metrics/`, `monitoring/`, new `node/src/client.rs` /
  `local_benchmark.rs`) plus wholesale deletion of the old `hotstuff/`/
  `sailfish/`/`consensus/` crates — **none of it committed**, all of it only
  in the working tree.

So `git clone {repo_url}` from any reachable remote (`upstream`, or a
hypothetical personal fork) **cannot** deliver the actual code under test —
it would fetch stale/unrelated `autobahn-artifact` history, and the smoke
test would silently benchmark the wrong thing. Getting the real tree onto EC2
via the standard flow requires a commit somewhere reachable by the remote
hosts, and:

- This session's hard rule is **no git writes** — no commit, anywhere,
  including a purely local one, is something I'll do unilaterally.
- `gh auth status` shows GitHub push access is available (`repo` scope) if a
  personal fork were the intended path, but that still requires a commit
  first, so it doesn't change the constraint.

**I did not pick a way around this.** Two ways forward, need your call before
I spend any AWS budget:

- **(A)** I patch `Bench._update`/`Bench.install` (Python-only, no git
  involved) to `rsync`/`tar`-and-`scp` the local working tree to the instances
  instead of `git clone`/`git pull`, for this smoke test. Keeps "no git
  writes" intact; deviates from "the fabfile's standard flow" as literally
  specified in the task, in exactly the one step that's affected.
- **(B)** You (or another agent, outside this task's "no git writes" rule)
  commit and push the current tree somewhere reachable (a personal fork is
  already auth'd via `gh`), and I proceed with the standard git-based
  `_update`/`install` flow unmodified.

Everything else (harness code paths, venv, AMI resolution, settings, key
pair, security groups) is ready to go the moment one of these is chosen —
picking up from here should take only the time to apply whichever fix and
run the actual `create`/`install`/`remote`/`destroy` cycle.

### Instance-hours consumed: 0 (no instances created).

### Cleanup already done / pending your decision
- No instances exist (verified above).
- One EC2 key pair (`vantage-smoke-20260723170010`, eu-west-1) exists,
  unused, zero cost — left in place pending the decision above (delete it
  yourself with `aws ec2 delete-key-pair --key-name vantage-smoke-20260723170010
  --region eu-west-1`, or tell me to and I will).
- `benchmark/settings.json` in the working tree now holds the real AWS smoke
  config above instead of the original GCP placeholder content (not
  committed). Say the word and I'll restore the original placeholder JSON.

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

## §optional — `--mimic-latency-ms` (starfish-style artificial inter-node delay)

Added (Findings A and B both completed cleanly first, per the task's gate). Reference:
starfish's `mimic_extra_latency`/`uniform_latency_ms` pattern in
`~/code/starfish/crates/starfish-core/src/network.rs` (read-only; not copied — that
version builds a full per-connection geodistributed latency table with adversarial
ramp/jitter, well beyond what a single diagnostic knob needs).

Implementation, deliberately minimal (harness-only, default off = byte-identical
current behavior):
- `network/src/lib.rs`: one process-wide `static MIMIC_LATENCY_MS: AtomicU64`, plus
  `pub fn set_mimic_latency_ms(ms: u64)` and a crate-private `mimic_latency() ->
  Duration` reader. Global rather than threaded through every `ReliableSender`/
  `SimpleSender`/`Primary::spawn`/`Worker::spawn` signature — a single benchmark
  process has exactly one intended mimic delay, so threading it through every spawn
  path for a diagnostic-only knob seemed like unwarranted surface area.
  `set_mimic_latency_ms` is called once at `local-benchmark` startup, before any node
  is spawned.
- `network/src/reliable_sender.rs` / `network/src/simple_sender.rs`: each `Connection`'s
  send loop reads `mimic_latency()` and `tokio::time::sleep`s it (no-op when zero)
  immediately before the real `writer.send(...)`. Applies uniformly to every outbound
  send on both senders (primary-to-primary via `ReliableSender`, primary-to-worker /
  worker-sync via `SimpleSender`).
- `node/src/main.rs` / `node/src/local_benchmark.rs`: new `--mimic-latency-ms <INT>`
  flag on `local-benchmark`, default `"0"` (off); parses, and if `>0` calls
  `network::set_mimic_latency_ms` and prints a one-line confirmation.
- `node/Cargo.toml`: added the (already-existing, unmodified-otherwise) `network` crate
  as a direct dependency of `node` (it previously reached `network` only transitively
  through `primary`/`worker`).

**Smoke-tested**: `autobahn-optimistic`, 4 nodes, `--rate 20000 --duration 8
--mimic-latency-ms 20` → confirmation line printed, avg latency jumped to ~1.9s (vs a
sub-second baseline at this rate with no mimic latency) — the injection is live and has
the expected directional effect. Not used in Findings A/B above (both were run with the
flag at its default 0/off).

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
- `node/src/main.rs` — new `--timeline` and `--mimic-latency-ms` flags on
  `local-benchmark` (both off/0 by default; existing flags/behavior unchanged).
- `node/src/local_benchmark.rs` — `--timeline` prints a once/sec progress line per live
  node instead of a single silent sleep (only when the flag is passed); parses and
  applies `--mimic-latency-ms`.
- `node/Cargo.toml` — added `network` as a direct dependency (previously transitive
  only).
- `network/src/lib.rs`, `network/src/reliable_sender.rs`,
  `network/src/simple_sender.rs` — the optional `--mimic-latency-ms` injection point
  (see §optional; no-op at the default 0).

No `tex-projects` file touched. No `starfish` file touched (read-only reference only,
via `grep`/`Read`). No git command run (`git status`/`log`/`branch` for orientation
only). All benchmark runs used `CARGO_BUILD_JOBS=4` builds; no two builds/benchmark
processes run concurrently. `.local-bench-findingA*`, `.local-bench-findingB-*`,
`.local-bench-mimic-smoke` data dirs left on disk under the repo root (gitignored,
harmless, safe to delete) — full stdout logs for every run are under
`/private/tmp/claude-501/.../scratchpad/finding{A,B}*.log` and `mimic-smoke.log`.
