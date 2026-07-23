# Modernization notes (Phase 1)

Strictly behavior-preserving dependency modernization of the Autobahn SOSP'24 artifact.
Host: macOS arm64 (darwin 24.6.0), Homebrew clang 19.1.7.
Toolchain: `rustc 1.95.0` (pinned via `rust-toolchain.toml`).

No protocol logic, message layouts, config schema, `Parameters` defaults, log strings, or
CLI surface were changed. No git commits were made (working tree left dirty for review).

---

## 1. Baseline status (untouched tree, before any edit)

- `rustc --version` → `rustc 1.95.0 (59807616e 2026-04-14)`; `cargo 1.95.0`.
- **`cargo build` FAILED** — exactly the expected arm64/modern-clang blocker:

  ```
  error: failed to run custom build command for `librocksdb-sys v6.20.3`
    thread 'main' panicked at bindgen-0.59.2/src/ir/context.rs:878:9:
    "enum_(unnamed_at_rocksdb/include/rocksdb/c_h_854_1)" is not a valid Ident
  ```

  `crypto`, `config`, `network` (and all non-store deps, incl. `ed25519-dalek 1.0.1`,
  `curve25519-dalek 3.2.1`, `tokio 1.53.1`) compiled fine; the build died on
  `rocksdb 0.16`'s `librocksdb-sys 6.20.3` build script (bindgen 0.59 cannot handle an
  anonymous C enum ident emitted by clang 19). The build never reached `primary`,
  `worker`, `hotstuff`, `consensus`, or any test target.
- `cargo test` was therefore never run at baseline (build is a prerequisite).
- **Consequence:** there is no baseline pass/fail set to diff against. Per the plan, each
  post-upgrade failure below is classified as *pre-existing* (masked by the rocksdb build
  failure) or *upgrade-induced* (my responsibility — none found).

Baseline resolved versions (`cargo metadata`, which succeeds without compiling):
`rocksdb 0.16.0 / librocksdb-sys 6.20.3`, `ed25519-dalek 1.0.1 / curve25519-dalek 3.2.1`,
`rand 0.7.3 / rand_core 0.5.1`, `base64 0.13.1`, `clap 2.34.0`,
`tokio 1.53.1`, `tokio-util {0.6.10, 0.7.19}`, `bincode 1.3.3`, `sha2 0.9.9`,
`env_logger 0.7.1`, `serde 1.0.229`, `thiserror 1.0.69`, `anyhow 1.0.104`.

---

## 2. Per-dependency changes and API notes

### Toolchain / workspace
- Added `rust-toolchain.toml` pinning `channel = "1.95.0"`.
- `edition = "2021"` set on every workspace member manifest (was `2018`).
- Added `resolver = "2"` to the virtual `[workspace]` (required by edition-2021 members;
  also silences cargo's virtual-manifest resolver warning). See forced choice F7.
- Shared external deps consolidated into `[workspace.dependencies]`; members use
  `dep = { workspace = true }`, adding per-crate `features = [...]` at the use site so every
  crate's original feature set is preserved exactly (e.g. `tokio` "full" in node,
  `["sync","rt","macros","time"]` in primary; `tokio-util` `["codec","time"]` in primary vs
  `["codec"]` elsewhere; `rand` `small_rng` in network). Path deps left inline.
- `Cargo.lock` is now generated and committed-ready; removed the `Cargo.lock` line from
  `.gitignore` (the file's own comment already advised this for executable workspaces).

### rocksdb 0.16 → 0.23.0  (store crate)  — the arm64 unblocker
- **Zero source changes.** The `store` wrapper only uses `rocksdb::{Error, DB::open_default,
  DB::put, DB::get}`, whose signatures are identical across 0.16→0.23
  (`open_default(path)->Result<DB>`, `put(&self,k,v)->Result<()>`,
  `get(&self,k)->Result<Option<Vec<u8>>>`). `store`'s public API is unchanged.
- 0.23 ships `librocksdb-sys 0.17` with a modern bindgen that compiles cleanly under
  clang 19 on arm64. This is what unblocks the whole workspace.

### ed25519-dalek 1.0.1 → 2.2.0  (crypto crate ONLY — see forced choice F1)
Confined to `crypto/src/lib.rs`; `crypto`'s public API (type names, constructors, trait
impls, Display/Debug, custom base64 serde, and all byte encodings) is unchanged, so
`config`/`primary`/`hotstuff`/`worker`/`node` call sites are untouched. API migration:
- `Keypair::generate` → `SigningKey::generate` (needs the `rand_core` feature; enabled).
- `keypair.public.to_bytes()` → `signing_key.verifying_key().to_bytes()`.
- `keypair.to_bytes()` (64 B seed‖public) → `signing_key.to_keypair_bytes()` (same 64 B
  layout). `Keypair::from_bytes(&[u8;64])` → `SigningKey::from_keypair_bytes(&[u8;64])`.
  ed25519 key derivation is deterministic/standard, so pubkeys, the 64-byte secret
  encoding, and signatures are byte-identical across the bump (old keyfiles remain valid).
- `PublicKey` → `VerifyingKey`; `PublicKey::from_bytes` → `VerifyingKey::from_bytes`.
- `ed25519::signature::Signature::from_bytes(&self.flatten())?` →
  `dalek::Signature::from_bytes(&self.flatten())` — in ed25519 2.x this is inherent and
  **infallible** (`&[u8;64] -> Signature`), so the `?` was dropped (F4).
- `verify_batch` unchanged in shape (behind the `batch` feature); its `keys` vec element
  type changed `PublicKey` → `VerifyingKey`.
- Features enabled on crypto's dalek dep: `["batch", "rand_core"]`. Dalek's `serde` feature
  is **not** enabled — crypto serializes keys via its own base64 impls, not dalek's.
- Crate features `serde`/`Verifier`/`Signer` re-export paths (`ed25519_dalek::ed25519`,
  `ed25519_dalek::Signer`) still resolve under 2.x; kept as-is.

### rand 0.7 → 0.8.7
- Workspace `rand = "0.8"` (aligns with dalek 2's `rand_core 0.6`; **not** 0.9 per spec).
- **Zero source changes** anywhere: `OsRng`, `StdRng::from_seed`, `SmallRng::from_entropy`,
  `SeedableRng`, `SliceRandom`, `thread_rng().gen()` all keep the same paths/APIs in 0.8.
  `small_rng` feature retained in `network`. NOTE: `StdRng`'s cipher changed
  ChaCha20 (0.7) → ChaCha12 (0.8) (verified in vendored sources), so seeded streams
  *differ* across the bump. No behavioral impact here: seeded `StdRng` appears only in
  tests, which assert sign/verify round-trips rather than key bytes (all green), and
  production keygen uses `OsRng`. (Corrected in audit — this note originally claimed
  both versions used ChaCha12.)

### base64 0.13 → 0.22.1  (crypto crate ONLY — Engine API)
- Added `use base64::prelude::{Engine as _, BASE64_STANDARD};`.
- `base64::encode(x)` → `BASE64_STANDARD.encode(x)`, `base64::decode(s)` →
  `BASE64_STANDARD.decode(s)`. `BASE64_STANDARD` = standard alphabet + padding =
  byte-identical output to old `base64::encode`. Confined to `crypto` (no other crate names
  `base64::*`; `config` round-trips keys through serde, never touching base64 directly).
- `DecodeError::InvalidLength` gained a `usize` field in 0.22 (F2).

### clap 2.34 → 4.6.4  (node: main.rs + benchmark_client.rs)
Migrated builder API `App`/`SubCommand`/`args_from_usage`/`AppSettings` →
`Command`/`Arg`/`ArgAction`, and accessors `occurrences_of`/`value_of`/`values_of`/
`subcommand()` → `get_count`/`get_one::<String>`/`get_many::<String>`/`Some((name, m))`.
Enabled clap's `cargo` feature for `crate_name!`/`crate_version!` (F8).
**CLI surface preserved byte-for-byte** against the invocations in
`benchmark/benchmark/commands.py`:
- `./node generate_keys --filename F`
- `./node -vv|-vvv run --keys F --committee F --store P --parameters F primary`
- `./node -vv|-vvv run ... worker --id N`
- `./benchmark_client <ADDR> --size N --rate N [--nodes A B C ...]`
Verbosity `-v...` → `ArgAction::Count` + `get_count("v")` (0..→error/warn/info/debug/trace).
`--nodes` → `num_args(1..)` so one flag consumes multiple space-separated values, matching
`--nodes {" ".join(nodes)}`. Positional `<ADDR>`, required options, and optional
`--parameters`/`--nodes` all preserved.

### tokio → "1" (1.53.1) / tokio-util → 0.7.19
- `tokio` unified to workspace `"1"` (resolves to 1.53.1, already the latest 1.x). Per-crate
  feature sets preserved (node keeps `full`).
- `tokio-util` unified to `0.7` (was mixed 0.6.x + 0.7.4). Feature union `codec`(+`time` in
  primary) preserved. Zero source changes.

### Kept unchanged (per spec)
`bincode 1.3.3` (wire format), `log 0.4` / `env_logger 0.7.1` (log-format regexes in
`benchmark/benchmark/logs.py` depend on the exact `[<ts>Z <lvl> <tgt>] msg` shape),
`anyhow 1.0`, `thiserror 1.0`, `serde 1.0`, `async-trait 0.1`. `async-recursion` left at
`0.3` (compiles fine; not bumped).

### sha2 (new, crypto dev-dependency only)
`crypto`'s own tests imported `ed25519_dalek::{Sha512, Digest}`, dropped as re-exports in
dalek 2.x. Migrated those test imports to `sha2::{Sha512, Digest}` and added `sha2` as a
crypto **dev**-dependency. SHA-512 output is identical (fixed standard). This is
crypto-internal (in scope). NOTE: the analogous imports in hotstuff/primary/worker were
**not** touched — see F1.

---

## 3. Forced choices (where the new API compelled a decision)

- **F1 — ed25519-dalek version split (Path A).** `hotstuff/src/messages.rs`,
  `primary/src/messages.rs`, `worker/src/{batch_maker,processor}.rs` (+ their tests) import
  `ed25519_dalek::{Sha512, Digest}` directly (a re-export dalek 2.x removed). The spec
  requires the dalek bump be "confined to the crypto crate internals" and that
  "primary/hotstuff/worker/node compile unchanged." To honor both literally I kept those
  three crates on `ed25519-dalek = "1.0.1"` (zero source/behavior change) and migrated only
  `crypto` to 2.2.0. **Cost:** the old crypto stack lingers as transitive deps —
  `ed25519-dalek 1.0.1`, `curve25519-dalek 3.2.1`, `sha2 0.9.9`, `rand 0.7.3`
  (dalek 1.0.1's default `rand` feature) coexist with the 2.x stack in `Cargo.lock`. No dalek
  type crosses a crate boundary (crypto exposes only its own byte-array newtypes), so this
  is sound.
  **Recommended follow-up (needs approval — it edits those three crates, which the spec said
  to leave unchanged): Path B** — replace their `use ed25519_dalek::{Sha512, Digest}` with
  `use sha2::{Sha512, Digest}` and drop `ed25519-dalek` from their manifests. `sha2 0.10.9`
  is already in the tree (via dalek 2.x), so Path B removes the entire legacy stack while
  adding nothing, making dalek 2.x the sole ed25519 dependency (the cleanest end state).
- **F2 — base64 `DecodeError::InvalidLength`.** Now `InvalidLength(usize)`; supplied
  `bytes.len()`. This arm is effectively dead (the preceding `bytes[..N]` panics first on
  short input, matching prior behavior), so the value is cosmetic; behavior preserved.
- **F4 — infallible `Signature::from_bytes`.** Dropped the `?` (ed25519 2.x returns
  `Signature`, not `Result`). No semantic change.
- **F7 — `resolver = "2"`.** Chosen over leaving the resolver unset (which warns on every
  build under edition 2021). Feature unification was verified not to drop any needed
  feature: all builds and dependency-compatible tests are green.
- **F8 — clap `cargo` feature.** Required for `crate_name!`/`crate_version!` (used verbatim
  by the original code).

---

## 4. Build / test results

### Before
`cargo build` fails at `librocksdb-sys 6.20.3` (see §1). Nothing downstream built.

### After (rustc 1.95.0)
The **real Autobahn build set** — `node` + `benchmark_client` + all their library deps
(`config`, `store`, `crypto`, `primary`, `worker`, `network`) — is fully green:

| Command | Result |
|---|---|
| `cargo build -p node` (debug) | OK |
| `cargo build -p node --features benchmark` (debug, builds `benchmark_client`) | OK |
| `cargo build --release -p node --features benchmark` (release, both bins) | OK (51 s) |
| `cargo test -p config -p crypto -p store -p network -p node` | OK — 17 pass, 0 fail |

Test detail: crypto 7/7 (incl. signature + batch verify), network 6/6, store 4/4
(rocksdb 0.23), config 0, node 0; all doc-tests pass.

`primary`'s consensus source — `primary/src/messages.rs`, `core.rs`, `committer.rs` — is
**byte-identical to HEAD** (`git diff` clean; only `primary/Cargo.toml` changed). All `.rs`
edits in the whole task are confined to `crypto` (lib + tests) and `node`
(main.rs + benchmark_client.rs); `hotstuff`/`worker`/`primary` source is untouched.

### Not green — all PRE-EXISTING, none upgrade-induced (masked by the baseline rocksdb failure)
- **`consensus` (lib): 14 errors**, stale `Certificate` API (`.round()`, `.header` gone —
  fields are now `author, header_digest, height, votes`).
- **`hotstuff` (lib): 17 errors**, identical stale-`Certificate`-API breakage.
- **`primary` (lib test): 12 × E0061** — `tests/core_tests.rs` calls `Core::spawn` with 24
  args; `core.rs:146` defines 27 (3 missing: `bool, u64, u64`, i.e. Autobahn `Parameters`
  added to source but never propagated to tests).
- **`worker` (lib test): 2 × E0308** — `tests/batch_maker_tests.rs` expects
  `QuorumWaiterMessage` but the current `batch_maker` sends `Vec<u8>` (the struct is now
  "never constructed"). Test/source drift.

All four are plain application/test drift involving only in-crate types and Rust primitives
— no bumped dependency is implicated. They prove `consensus`/`hotstuff` and the
`primary`/`worker` test suites could not have compiled in the authors' own tree either.
Consequently a fully-green `cargo test --workspace` / `cargo build --workspace --all-targets`
is **not achievable by dependency modernization alone** — it additionally requires repairing
(or removing) this rotted test/dead code, which is out of scope for Phase 1 (test surgery)
and partly blocked (see §6).

---

## 5. Dead-crate cleanup — evidence trail (for the "Autobahn and nothing else" step)

Both crates are dead HotStuff/Tusk-lineage leftovers, unreferenced and long-broken:

**`consensus`**
- No workspace member lists it as a dependency (grep of all `Cargo.toml`).
- No `use consensus::` / `extern crate consensus` anywhere (only a commented-out
  `/*Consensus::spawn(...)*/` block in `node/src/main.rs`).
- Does not compile (14 stale-`Certificate`-API errors) → was already dead upstream.

**`hotstuff`**
- Referenced only by the workspace `members` list and the **commented-out**
  `#hotstuff = { path = "../hotstuff" }` line in `node/Cargo.toml`.
- Zero `use hotstuff::` use-sites outside the crate.
- Does not compile (17 stale-`Certificate`-API errors) → dead upstream.
- Autobahn's real consensus lives inside `primary` (ConsensusMessage/CommitQC/AcceptVote in
  `primary/src/core.rs` + `committer.rs`); `hotstuff` is not on any live path.

**Intended action (ratified by the coordinator):** remove both from `[workspace] members`
and delete the `consensus/` and `hotstuff/` directories, then confirm `cargo build
--workspace` / `cargo test --workspace` (the latter still gated on the §4 test rot).

**STATUS: DONE (2026-07-22, user-authorized).** The subagent's removals were denied by the
auto-mode permission classifier; the user explicitly authorized them in-session and the
main session executed: deleted `consensus/`, `hotstuff/`, `sailfish/` (49 tracked files),
`CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`; dropped `consensus`/`hotstuff` from
`[workspace] members`; removed the dead commented `#hotstuff`/`#sailfish` path-dep lines
from `node/Cargo.toml`. Post-deletion verification: `cargo build --workspace` green in
debug, and the exact fab compile command (`cargo build --quiet --release --features
benchmark`) green at workspace root; `cargo test --workspace` fails with exactly the §4
pre-existing set (12×E0061 primary tests, 2×E0308 worker tests) and nothing else.

Done in this pass: `README.md` rewritten to a minimal accurate stub (provenance, Apache-2.0,
pointers to `IMPLEMENTATION-PLAN.md` and `benchmark/`); `Cargo.lock` un-ignored in
`.gitignore`. `LICENSE` and `benchmark/` retained.

---

## 6. Local-bench (`fab local`) attempt

- **GREEN (2026-07-22, post-deletion).** 4 nodes × 1 co-located worker, 240,000 tx/s
  input, 512 B txs, 60 s: Consensus TPS 240,040 / latency 5 ms; End-to-end TPS 240,004 /
  latency 14 ms (this is upstream's *sample-based* latency metric — replaced as headline
  in Phase 2). Input rate fully sustained.
- Environment accommodations (documented deviations):
  - `benchmark/benchmark/local.py`: `BASE_PORT` 3000 → **4000** — the only `benchmark/`
    Python edit in Phase 1. On this machine Docker Desktop listens on 3001 and an ssh
    forward on 3002; primary-0's receivers died at boot (`AddrInUse` panics at
    `network/src/receiver.rs:51`) and the first run stalled at zero commits. Range
    4000–4023 verified free. Port numbers enter no measurement or log regex.
  - `benchmark/requirements.txt` is stale: the pins (`fabric==2.6.0`,
    `matplotlib==3.3.4`) don't install on Python 3.12, and `benchmark/gcp_instance.py`
    imports `google-cloud-compute`, which it never listed. Ran from a venv with current
    `fabric`/`boto3`/`matplotlib`/`google-cloud-compute`; fabric 3.x runs this fabfile
    unmodified. Refreshing `requirements.txt` is a Phase-2 harness item.
- Transport note for later fairness discussions: the `network` crate never calls
  `set_nodelay`, so Nagle stays ON (starfish disables it). The substrate is identical for
  all three protocols in this repo, so internal head-to-head comparisons are unaffected;
  remember it when comparing absolute latencies against starfish-paper numbers.

---

## 7. Open issues for audit

1. **[Permission-blocked] Dead-crate removal + cleanup deletions** (§5). Needs user
   authorization. Until then `cargo build --workspace` and `fab local` cannot go green.
2. **[Pre-existing, out-of-scope] Test/dead-code rot** (§4): `consensus`/`hotstuff` libs and
   `primary`/`worker` test suites don't compile against current source. Independent of this
   upgrade. Blocks `cargo test --workspace` and `--all-targets` regardless of #1. Decide
   whether Phase 1 repairs the `primary`/`worker` tests or defers.
3. **[Decision] ed25519-dalek Path A vs Path B** (F1). Path A shipped (spec-literal, zero
   downstream diff, but old dalek/curve25519/sha2/rand stacks linger). Path B (recommended)
   removes the legacy stack at the cost of a 2-line import swap + manifest edit in
   hotstuff/primary/worker.
4. **[Confirm]** `resolver = "2"` (F7) — flagged in case strict feature-parity with the
   implicit baseline resolver is desired (builds/tests are green, so no observed impact).

---

## 8. Audit (Fable, 2026-07-22) — task #11, first pass

Independently verified:
- `git diff` over `primary/src worker/src hotstuff/src consensus/src config/src
  network/src store/src` → **empty**. All `.rs` edits confined to `crypto` + `node`,
  as claimed; invariant 4 holds.
- `crypto` diff: `BASE64_STANDARD` = old default alphabet+padding (encodings
  byte-identical); 64-byte keypair layout preserved via
  `to_keypair_bytes`/`from_keypair_bytes`; `verify_strict`/`verify_batch` reject sets
  unchanged at the crate boundary (dalek 2 moves the canonical-`s` check from
  `Signature::from_bytes` to verify time; callers see `Result` either way). One real
  tightening: dalek 2's `from_keypair_bytes` validates that the public half matches the
  secret half (dalek 1 did not) — reachable only with a corrupted keyfile; panics via the
  same `expect` either way.
- `node` clap-4 diff: flag surface matches every invocation constructed in
  `benchmark/benchmark/commands.py` (generate_keys `--filename`; counted `-v`; run
  `--keys/--committee/--store` required, `--parameters` optional; `worker --id`; client
  positional `<ADDR>`, `--size/--rate`, multi-value `--nodes` via `num_args(1..)`).
- Manifests: per-crate feature sets preserved under `[workspace.dependencies]`;
  `primary` keeps dalek 1.0.1 (Path A); both dalek versions present in `Cargo.lock`.
- Re-ran `cargo test -p config -p crypto -p store -p network -p node`: **17/17 pass**;
  release `node` + `benchmark_client` binaries present.
- Corrected the §rand StdRng cipher claim (above); conclusion unaffected.

**Verdict: PASS (final, 2026-07-22).** All conditions met post-deletion: `cargo build
--workspace` green in debug and via the exact fab compile command; `cargo test
--workspace` shows precisely the pre-existing §4 failure set and nothing else;
`fab local` green with the input rate fully sustained (§6). Dispositions: **F1 Path B deferred to Phase 2** — the blake3 migration rewrites
exactly the `Sha512` call sites that keep primary/worker on dalek 1, so the legacy stack
dies there at zero extra cost. **§4 test rot deferred to Phases 2–3**, which rewrite the
vote/PoA flows those tests cover; repairing them now would touch primary/worker sources
and muddy this phase's byte-untouched guarantee.
