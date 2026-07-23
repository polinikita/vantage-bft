# Vantage + Autobahn: shared implementation plan

## Status (updated 2026-07-22)

| Phase | Scope | Effort share | Progress |
|---|---|---|---|
| 0 | Bootstrap: fork, branches, plan | 2% | ✅ done |
| 1 | Dependency modernization + cleanup | 4% | ✅ done — audited; fab local green (240k tx/s sustained, 4 nodes) |
| 2 | Multi-protocol layout + Starfish parity (blake3, tx modes, real latency metric, rocksdb tuning, local-benchmark + grafana) | 12% | ✅ done — audited 2026-07-23 (PHASE2-NOTES.md); gate numbers: 240k tx/s reproduced, real-latency metric live |
| 3 | Vantage data plane (chains, ACKs, retention, fetch) | 15% | ✅ done — two-pass audit closed 2026-07-23 (PHASE3-NOTES.md); §3.4 network wiring deferred to Phase-4 preamble (dispatch-identity finding) |
| 4 | AGB happy path (manifests, R1–R4, grades, linearization) | 20% | ✅ done — two-pass audit closed 2026-07-23 (PHASE4-NOTES.md §13); vantage runs live: 48k tx/s, 4 nodes, real-latency metric |
| 5 | Pacemaker (WISH) + degraded paths | 12% | ✅ done — two-pass audit closed 2026-07-23 (PHASE5-NOTES.md); crash-fault + convergence tests green; benchmarks at parity |
| 6 | Resolution + sparse log + Byzantine suite | 20% | ✅ done — two-pass audit closed 2026-07-23 (PHASE6-NOTES.md §12); 240k parity confirmed; Byzantine suite green |
| 7 | Evaluation (LAN/WAN, headline Byzantine experiment) | 15% | not started |

Overall: ~85% by effort weight (phases 0–6 closed — ALL protocol phases complete and
audited). Remaining: phase 7 (evaluation), which needs user decisions: cloud
credentials/testbed, TCP-vs-MAC channel auth, Δ/timeout normalization for the headline
Byzantine experiment, and the two deferred performance findings (fault-free anchor_skip
under CPU saturation; crash-fault cursor serialization).

## Provenance

Fork of the Autobahn SOSP'24 artifact ([neilgiri/autobahn-artifact](https://github.com/neilgiri/autobahn-artifact), Apache-2.0).
`main` branches from `upstream/autobahn` (Narwhal-HotStuff lineage, ~20K LOC Rust).
Upstream branches are kept read-only for reference: `bullshark`, `vanilla-hs`, `batched-hs`
(+ `-blips` variants for the seamlessness methodology) and `overview`
(authors' `experiment-configs/` and raw `paper-results/` — the validation anchor).

## Goal

One binary, protocol selected by config:

| Protocol | Meaning | Status in upstream |
|---|---|---|
| `autobahn-optimistic` | Autobahn as shipped/evaluated (`use_optimistic_tips = true`) | implemented |
| `autobahn-seamless` | Certified-tips-only cut formation (`use_optimistic_tips = false`) | `TODO` in upstream (`config/src/lib.rs:86`) — we implement |
| `vantage` | Signature-free AGB protocol (paper: `tex-projects/signature-free`) | new |

Shared substrate across all three: `network`, `store`, `crypto` (unused on the vantage
hot path), `config`, `worker`, benchmark client, fab harness. Identical harness ⇒ fair
head-to-head.

## Workflow

- **Fable** (main session): per-phase specs, adversarial audits.
- **Opus** subagents: implementation.
- **User**: reviews and commits (agents never commit or push).
- Protocol-critical phases (3–6) require **two consecutive clean adversarial audit
  passes** before the gate closes; the audit checks code against the paper's rules
  (R1–R4, tip anchoring, resolution, WISH), not just tests.
- **Reuse-first**: every phase spec starts from an inventory of existing Autobahn
  modules to adapt in place — lane/car chain streaming, vote/ack plumbing, aggregator
  quorum counting, synchronizer/fetch, timers, worker batching. New code only for
  genuinely novel logic (AGB grade machine, tip anchoring, WISH, resolution, sparse
  log). No parallel reimplementations of machinery that already exists.
- **Simplification pass closes every phase**: once tests are green, simplify the
  codebase (deduplicate, collapse abstractions, delete paths made dead by the phase),
  re-run tests, and only then run the audit — so audits always see the final,
  simplified form. The phase gate includes this pass.

## Invariants during modernization (Phases 0–1)

After the Phase-1 gate validates our build against upstream numbers, invariants 1–3
relax for Phase 2's measurement upgrades (Rust and harness changes land together;
the upstream sample-based metric is kept in parallel for cross-checking). Invariant 4
holds for Autobahn protocol logic throughout the project.

1. **CLI compatibility**: `node` and `benchmark_client` flags stay byte-identical —
   `benchmark/fabfile.py` and `benchmark/benchmark/commands.py` construct these
   invocations.
2. **Log-format compatibility**: `benchmark/benchmark/logs.py` parses node logs with
   regexes; log message strings must not change.
3. **Wire stability**: keep bincode 1.x; message layouts unchanged.
4. **No protocol-logic edits**: modernization diffs must be behavior-preserving; any
   forced semantic choice is documented in `MODERNIZATION-NOTES.md`.

## Phases

### Phase 0 — Bootstrap ✅ (2026-07-22)
Clone with full history, `main` from `upstream/autobahn`, remote renamed to `upstream`,
this plan document added. Baseline build/test status is recorded at the start of Phase 1.

### Phase 1 — Dependency modernization (Opus; mechanical)
Upstream ships **no `Cargo.lock`**, so dependencies already float within semver; the
real work is the majors. Target state:

- `rust-toolchain.toml` pinned to installed stable (1.95.0); edition **2021** all crates
  (2024 deferred); commit a `Cargo.lock`.
- Consolidate shared deps into `[workspace.dependencies]`.
- Majors:
  - `rocksdb 0.16 → 0.23.x` — confined to `store` crate wrapper. This is the expected
    arm64-macOS build unblocker.
  - `ed25519-dalek 1.0.1 → 2.x` — confined to `crypto` crate internals
    (`Keypair → SigningKey`/`VerifyingKey`, batch API move); **the `crypto` crate's
    public API stays unchanged** so call sites in `primary`/`hotstuff`/`worker` are
    untouched.
  - `rand 0.7 → 0.8` (must align with dalek 2's `rand_core 0.6`; **not** 0.9).
  - `clap 2 → 4` — exact flag surface preserved (invariant 1).
  - `base64 0.13 → 0.22` (Engine API) inside `crypto` display/parse paths.
  - `tokio → latest 1.x`, `tokio-util → 0.7` (feature unions preserved).
- Keep: `bincode 1.3`, `log`/`env_logger` (invariant 2), `anyhow`, `thiserror 1.x`.
- Do **not**: touch `benchmark/` Python, chase clippy lints, reformat unrelated code.
- Cleanup — Autobahn and nothing else (after the upgraded build is green): delete
  `sailfish/` (dead, not a workspace member) and `CODE_OF_CONDUCT.md` /
  `CONTRIBUTING.md`; remove the legacy `consensus` crate if nothing references it
  (verify by build+tests after removal); replace the stale Sailfish `README.md` with a
  minimal stub (provenance, license, pointer to this plan). `LICENSE` and `benchmark/`
  stay. Whole-file/directory removals only — intra-crate dead-code surgery is Phase 2.
- Verification: record upstream baseline `cargo build`/`test` outcome first; after
  upgrade, `cargo build` (debug+release) and `cargo test --all` with no *new* failures;
  attempt `fab local` (venv from `benchmark/requirements.txt`; needs tmux) and document
  the result either way in `MODERNIZATION-NOTES.md`.
- **Gate**: build+tests green on 1.95; Fable audit confirms the diff is
  behavior-preserving; local bench numbers comparable to baseline (or to
  `paper-results/` if upstream never built locally).

### Phase 2 — Multi-protocol layout + Starfish-parity substrate
`protocol` enum in `config` (`autobahn-optimistic` | `autobahn-seamless` | `vantage`)
selecting the node assembly in `node`; fab harness gains a protocol parameter.
Implement the missing non-optimistic path (proposer references the last *certified* car
instead of the tip). Remove intra-crate dead code unreachable under the three protocol
assemblies.

Starfish-parity substrate, applied identically to all three protocols (reference:
`~/code/starfish`, `iotaledger/starfish`):

- **blake3** (1.5.x) replaces the legacy hasher behind `crypto::Digest` (stays
  `[u8; 32]`); where the repos overlap, dependency choices follow starfish. This also
  retires the Phase-1 F1/Path-A remnant (MODERNIZATION-NOTES.md): primary/worker's direct
  `ed25519_dalek::Sha512` imports are exactly the call sites blake3 replaces, so the
  legacy dalek-1 stack drops out here at zero extra cost.
- **Transaction payload modes** mirroring starfish `TransactionMode`: every transaction
  is `8 B UTC-millis timestamp + 8 B counter/random word + remainder`, with the
  remainder zero-padded (`all-zero`, upstream-equivalent) or random bytes (`random`,
  the honest mode — defeats accidental compression/dedup anywhere in the stack);
  `transaction_size > 16` enforced; mode is a `benchmark_client` config/CLI knob.
- **Real transaction latency**: submission-to-commit computed from the embedded
  timestamp of *every* committed transaction — full distribution (avg/p50/p99,
  squared-sums for stddev) over a steady-state window — replacing upstream's sparse
  "sample transaction" markers (mean-only, parsed by `logs.py`) as the headline metric.
  The Narwhal-lineage harness (Tusk/Bullshark/this artifact) never measured this;
  starfish did, and the paper's evaluation will use the honest definition. Upstream's
  sample metric stays available for cross-validation against `paper-results/`. Assumes
  NTP-grade clock sync across machines (same as starfish). Vehicle (user decision,
  2026-07-22): starfish-style prometheus — ported `PreciseHistogram`/`HistogramSender` +
  per-node axum scrape endpoint; the fab harness scrapes at run end (details in
  `PHASE2-SPEC.md` §5).
- **Local benchmarking vehicle** (user decision, 2026-07-23): `fab local` is rejected
  for local use. Starfish-style `node local-benchmark` subcommand — one process,
  in-memory committee/parameters, in-process RESULTS from the metrics registries — plus
  a `monitoring/` docker-compose stack (stock prometheus+grafana, live dashboard,
  starfish's local-dryrun pattern; host ports 3003/9095). The fab harness remains for
  remote runs only (Phase 7). Details: `PHASE2-SPEC.md` §8.
- **RocksDB tuning** ported from starfish `rocks_store.rs` (user decision, 2026-07-23):
  DB-wide options (LZ4 + bottommost-Zstd, 256 MiB ×6 write buffers, pipelined writes,
  fd-limit raise, bloom filters + pinned L0 index in block tables) with starfish's
  metadata/data profile split mapped onto the artifact's per-component DBs — primary
  store = metadata profile (level compaction), worker stores = data profile (universal
  compaction). Details: `PHASE2-SPEC.md` §7.

**Gate**: `autobahn-optimistic`
reproduces Phase-1 numbers; `autobahn-seamless` shows the expected latency penalty and
zero tip references in cuts.

### Phase 3 — Vantage data plane
Adapt Autobahn's existing machinery in place — cars already form per-lane hash chains,
so the chain layer is reuse, not new code. Modifications: `Vote` → unsigned position
ACK, fan-out widened from car-author-only to all-to-all; aggregator quorum counting →
per-author first-hand availability state (q-available sets per author-position) instead
of PoA certificate assembly (deleted on this path); retention rule added;
`synchronizer`/`header_waiter`/`helper` fetch-by-hash reused with ackers as sources. No
signing anywhere on this path. **Gate**: unit/integration tests for chain integrity,
ack accounting, fetch; simplification pass; two-pass audit.

### Phase 4 — AGB happy path
Author-indexed C/E manifests; core = proposer's first-hand 2f+1-acked entries;
TipAnchored hash-walk (tip strictly extends the same author's core entry; possession
required); R1–R4 echo/ready state machine with grades (gfull/gcore/gskip); one pipelined
instance per view; homogeneous-grade delivery; deterministic linearization into the
output log. **Gate**: 4-node local run showing 3δ core seal and 1-hop tip admission;
two-pass audit against the paper's rule block.

### Phase 5 — Pacemaker and degraded paths
WISH view synchronization (bounded high-watermark), Δ-window grade-0 fallback, proposal
frontier, leader timeout/skip. **Gate**: crash-fault tests; view convergence after
partition heal; two-pass audit.

### Phase 6 — Resolution and sparse control log
Mixed-grade tip settlement: later anchored proposal checked against each correct
echoer's own immutable earlier response; quorum-intersection first-applicable rule;
background log = validated Bracha broadcast + non-speculative Simple-IT over anchor
hashes. Byzantine fault injection built here (upstream has none): withheld-tip author,
forked author chain, equivocating leader, forced mixed grades. **Gate**: Byzantine suite
green; two-pass audit.

### Phase 7 — Evaluation
Fab LAN/WAN runs: fault-free latency/throughput; headline one-Byzantine-withheld-tip
author (vantage grade degradation vs `autobahn-optimistic` cut-skip + 2Δ sync vs
`autobahn-seamless`); crash faults; scaling in n. Metrics include
publication-to-sequencing latency (the paper's 4δ/5δ claims). Optional: blips
methodology from `-blips` branches; appendix cross-check against `paper-results/`.
Parked decision: plain TCP (identical to upstream) vs per-link MACs for the
signature-free fairness story.
