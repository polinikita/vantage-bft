# Phase 4 notes — deviations, decisions, inventory

Companion to `PHASE4-SPEC.md`, same role `PHASE3-NOTES.md` played for Phase 3. No git
commits were made (working tree left dirty for review, per standing instruction).
Written after implementation, in spec order.

---

## 0. Summary

Implemented: the §1 wiring preamble (`vantage::node::VantageCore`, a single spawned
task owning `LaneManager` + `Repairer` + `AgbEngine` + `Frontier` + `Cursor`;
`Primary::spawn`'s new `Protocol::Vantage` arm; `node run`/`node local-benchmark`'s D3
bail lifted); the five new §2 wire variants; `Formed_v`/`proposer(v)` (§3); the
responsive frontier/genesis/activation wrapper (`vantage::frontier::Frontier`, §4); the
Direct-AGB per-view engine (`vantage::agb::AgbEngine`) covering R2 echo (§5), R3 ready
(§6), R4 completion/direct-seal + the try-seal arbiter (§7), and the fast seal +
optimistic lock (§8); the output cursor (`vantage::cursor::Cursor`, §9) with the
Committed-metric reuse; §10's timers; the full §12 test suite (49 new tests, on top of
Phase 3's 26 — 75 vantage tests total, all passing).

Gate: workspace tests all green (86 `primary` / 6 ignored, unchanged Autobahn count from
Phase 3; 7 `crypto`, 6 `network`, 4 `store`, 6 `worker`, 0 elsewhere); Autobahn
regression benchmark reproduces the Phase-2/3 throughput number; a real 4-node
`local-benchmark --protocol vantage` run sustains commits at the configured rate with
zero misses and reports the real-latency metric. Full numbers in §12 below.

Two consecutive self-review passes done (§13); the phase's own two-pass **adversarial**
audit bar is Fable's, run against this note and the diff, same as every prior phase.

---

## 1. §1 — wiring preamble

`primary/src/vantage/node.rs` — new module. `VantageCore` owns `LaneManager`,
`Repairer`, `AgbEngine`, `Frontier`, `Cursor`, a `ReliableSender` (primary-to-primary),
a `SimpleSender` (to our own workers), and the §10 timer queue (`Vec<(Instant, View,
TimerKind)>`, drained via a biased `tokio::select!` that always checks message channels
before firing a due timer, per §5's tie-break rule). One `execute(effects)` loop (a
`VecDeque`-backed worklist, not recursive `async fn` calls — Rust can't size a
self-recursive `Future`) drains every effect, including the ones each component's own
handling of an earlier effect produces (e.g. `Fixed` → `Frontier::record_fixed` →
`AgbEngine::activate` → possibly `BroadcastEcho`), so a single inbound message can
legitimately cascade through several views in one call (this is exactly the
"responsive, no pacemaker needed" property §4 describes).

`Primary::spawn` gained a `match parameters.protocol` with a `Protocol::Vantage` arm
(spawns `VantageCore` + a `VantageReceiverHandler`-based `NetworkReceiver` on the
primary-to-primary port + the unchanged `WorkerReceiverHandler`/`PayloadReceiver`) and
an `AutobahnOptimistic | AutobahnSeamless` arm that is **byte-identical** to the
pre-Phase-4 function body (verified by diff — every line under that arm is an exact
copy, just re-indented one level into the `match`). `VantageReceiverHandler` is a new,
separate `MessageHandler` impl — Autobahn's `PrimaryReceiverHandler` is untouched.

Dispatch routing (`VantageReceiverHandler::dispatch` → `Inbound` → `VantageCore`):
`Header(h, false)` → `Inbound::Publish(h.author, h)` (claimed-by-author, per the D4
ruling recorded in PHASE3-NOTES §5/§11); `Header(h, true)` → `Inbound::Serve`;
`HeadersRequest` → `Inbound::HeadersRequest`; `VantageAck` → `Inbound::Ack`;
`VantagePropose`/`VantageEcho`/`VantageEchoSkip`/`VantageReady`/`VantageNoReady` → their
`Inbound` counterparts. `node/src/main.rs` and `local_benchmark.rs`'s D3 bails
(`"Vantage protocol is implemented in Phase 3..."` / the `local-benchmark` equivalent)
are removed; nothing else in either file changed.

Own-lane publication cadence: `rx_our_digests` accumulates `(Digest, WorkerId)` pairs;
`lm.publish_own(...)` fires on `payload_size >= header_size` or the `max_header_delay`
timer — the same trigger shape as Autobahn's `Proposer::run`, ported to `VantageCore`'s
select loop (§1: "same pattern as `Proposer`").

D1's `SyncBatches`/`store.notify_read` wiring: since `LaneManager::set_payload_ready`
(Phase 3) is documented as "call once **all** of a header's missing batches have
arrived" but unconditionally marks payload-ready the instant it's called (no internal
re-check), `VantageCore` tracks outstanding `(digest, worker_id)` keys per header digest
(`pending_payload: HashMap<Digest, HashSet<(Digest, WorkerId)>>`) and only calls
`set_payload_ready` once the last key for that header resolves — see §5.1 below for why
this needed a small `Effect::SyncBatches` shape change.

---

## 2. §2 — wire messages

`primary/src/primary.rs`'s `PrimaryMessage` gained, appended after `VantageAck` (bincode
wire-compat: variant index, never inserted elsewhere):
`VantagePropose(ViewProposal)`, `VantageEcho(Echo)`, `VantageEchoSkip(View, PublicKey)`,
`VantageReady(Ready)`, `VantageNoReady(View, PublicKey)`.

**Placement deviation**: `ViewProposal`/`Echo`/`Ready`/`ReadyGrade`/`Manifest` live in
`vantage::agb` (imported into `primary.rs`), not `messages.rs`. Phase 3 put `Ack` in
`messages.rs` (that file's existing convention), but the module plan (§11) explicitly
assigns the AGB engine and its types to `vantage/agb.rs`, and every one of these types
is otherwise only ever touched by vantage code (unlike `Ack`, which Autobahn's
`messages_tests` module sits alongside). Kept them where the spec's own module plan put
them rather than growing the already-very-large legacy `messages.rs` file further.

`Manifest = Vec<(PublicKey, Height, Digest)>` reuses `vantage::block::BlockRef` directly
(same tuple shape, no parallel type). `proposal_digest` is
`domain_hash(b"view-proposal", sid, bincode(ViewProposal))`, reusing Phase 3's
`vantage::block::domain_hash` helper exactly as instructed.

---

## 3. §3 — `Formed_v` / `proposer(v)`

`vantage::agb::formed` — pure, syntactic: per-manifest strictly-increasing-author
ordering (which also rejects a duplicate author, since a repeat can never be strictly
greater than itself), height ≥ 1, positive committee stake, and a single
cross-manifest `HashSet` pass rejecting any hash repeated across `C ∪ T`.
`vantage::agb::proposer` — round-robin over `Committee::authorities`'s already-sorted
`BTreeMap` key order, index `(v-1) mod n` (D4-2).

---

## 4. §4 — responsive frontier / genesis / activation

`vantage::frontier::Frontier` owns exactly three things: `a_i`, which views are
"active" (`HashSet<View>`, populated via the proposal-chain advance **or** `enter(v)`),
and which views we've already proposed for (R1's "not yet proposed" guard). It
deliberately does **not** duplicate `AgbEngine`'s per-view `fixed: ViewProposal`
storage — `AgbEngine::on_propose` reports only the single bit `Frontier` actually needs
(well-formed y/n) via `Effect::Fixed(view, bool)`; `Frontier::record_fixed` stores that
bit and recomputes the contiguous well-formed prefix from `a_i`'s current value,
returning every view newly activated by the call (handles the "buffered proposal
activates later" case in one pass: a view fixed out of order sits in
`fixed_well_formed` until the missing prefix arrives, then the whole run of newly-
qualifying views activates in the same `record_fixed` call).

`Frontier::try_propose` implements the R1 trigger for `view = a_i + 1` only — the only
view whose trigger could newly hold as a *direct* consequence of the current `a_i`
(checked once at genesis boot for view 1, and after every `record_fixed` advance for
the new `a_i + 1`, per `VantageCore`'s wiring). `build_manifests` reads
`LaneManager::c_candidate`/`t_candidate` in canonical (committee) author order,
deduping by hash across the union as it goes (module plan §11's "skip any hash already
listed under an earlier index" — defensive; a genuine collision between two different
authors' registers is cryptographically negligible, so this never actually fires in
practice, same status as the `formed()` debug_assert in `Frontier::build_manifests`).

Genesis floor: only the value 0 exists in Phase 4 (`Frontier::new` starts `a_i` at 0);
entering view 1 (`enter(1)`, called once at `VantageCore` boot) activates view 1 for the
positive-gate check but, per §4's own text, does **not** itself raise `a_i` — the
frontier still only advances through the well-formed proposal chain.

---

## 5. §§5-8 — `AgbEngine` (R2/R3/R4, try-seal arbiter, fast seal)

`vantage::agb::AgbEngine`: one instance per node, per-view state map
(`fixed`/`echo_sent`/`ready_sent`/`completed`/`directed`/`sealed`/`fastsealed`/
`active`/`entered`/`entry_instant`/`first_proposal_instant`/`echo_statements`/
`ready_statements`/`lock` — every field named in §5's "per-view engine state" list, plus
the R4/§7/§8 fields the later sections add). Every method is synchronous and
effect-returning, exactly like Phase 3's `LaneManager`/`Repairer` — no network/store/
clock access; callers supply `now: Instant` and mutable references to the `LaneManager`/
`Repairer` whose queries/authorization the gate and completion logic need.

R2 (`on_propose`, `recheck_gate`/`recheck_all`, `on_echo_fallback_timer`,
`on_echo_absolute_timer`, `on_echo`, `on_echo_skip`): implements the sticky first-direct-
proposal rule, the positive gate (`CoreOK_i(C) ∧ TipOK_i(C,T)`, using
`LaneManager::author_ok`/`holds_prefix`/`prefix_contains` — the last made `pub` in place,
per the reuse rule, since R2 needs it against arbitrary received manifests, not just
this node's own N5 registers), the fast-seal lock recording, and both fallback
deadlines. `recheck_all` is the "re-evaluated whenever local state changes" hook —
`VantageCore` calls it after every `BlockCached` wakeup.

R3 (`recheck_ready`, `on_ready_timer`, `on_ready`, `on_noready`): per-proposal-digest
echo tallies (stake-weighted, since acking/quorum thresholds are all
`Stake`-denominated in this codebase — same reading as Phase 3's N4), one-shot ready
emission with the grade computed from the tally at emission time.

R4 (`recheck_completion_and_direct`, `try_seal`, `outcomes_compatible`): per-proposal-
digest ready tallies drive completion (any grade, ≥ Q, once, hands `(C, T)` to the
cursor and authorizes C's lane prefix) and the direct result (`gfull`/`gcore` at a
homogeneous ≥ Q grade quorum), both submitted to the try-seal arbiter, which records the
first outcome and `debug_assert`s later submissions are compatible rather than
re-emitting.

§8 (`record_lock`, `recheck_fastseal`): D4-3's party-count (not stake) thresholds —
`f_plus_1_parties = (n-1)/3 + 1`, `n = committee.size()` — computed once in
`AgbEngine::new`. Lock birth checks the non-matching count *at that instant*; recheck
runs after every counted echo-stage statement (grade-1/0 echo, or skip), deactivating
(sticky) at ≥ f+1 non-matching parties, else firing `fastseal → gfull` once all n
parties are counted matching.

### 5.1 Effect enum extensions (deviations, all additive/backward-compatible)

The module plan's §11 Effect list (`BroadcastPropose`, `BroadcastEcho`,
`BroadcastEchoSkip`, `BroadcastReady`, `BroadcastNoReady`, `Sealed`, `ArmTimer`) is
implemented verbatim, plus these necessary, minimal additions:

- **`Effect::Fixed(View, bool)`** — the channel from `AgbEngine::on_propose` to
  `Frontier::record_fixed` (§4's frontier-advance rule needs exactly this bit; nothing
  in §11's list carries it).
- **`Effect::Completed(View, Manifest, Manifest)`** — the channel for R4's "hand (C,T)
  to the cursor as this view's manifests, state `gopen`" (§7). Completion is not itself
  a terminal/wire event and isn't `Sealed`, so it needed its own effect.
- **`Effect::ArmTimer(View, TimerKind, Instant)`** — carries the computed deadline
  directly (module plan: `ArmTimer(View, TimerKind)`). Without the `Instant`, the
  caller (`VantageCore`) would need to re-derive Δ/θE/θR arithmetic and each view's own
  `entry_instant`/`first_proposal_instant` independently, duplicating state that only
  `AgbEngine` should own.
- **`Effect::SyncBatches`'s third field** (the header digest) — Phase 3's
  `SyncBatches(PublicKey, Vec<(Digest, WorkerId)>)` doesn't name *which* header the
  missing batches belong to; the production waiter (§5 above) needs that digest to
  correlate resolved keys back to `LaneManager::set_payload_ready`. `lanes.rs`'s single
  emission site already had the digest in scope; one line added it to the effect.

None of these change any *existing* effect's meaning; `SyncBatches`'s one call site and
its one test assertion were updated to match (`primary/src/vantage/tests/ack_tests.rs`).

### 5.2 Interpretive decisions

- **D4 extended to `VantagePropose`'s sender.** `ViewProposal` (§2) carries no `sender`
  field, unlike `Echo`/`Ready`/`Ack`/`HeadersRequest`, and — per PHASE3-NOTES §5/§11 —
  there is no channel identity available at dispatch either. §13's own standing note
  ("D4 ... now also covers propose/echo/ready statements") plus the Header precedent
  ("provenance is claimed-by-author") together read as: production trusts any received
  `VantagePropose` for view `v` as if it came from `proposer(v)` (computed, not
  read off the wire) — `VantageReceiverHandler`/`VantageCore::dispatch_inbound` do
  exactly this. `AgbEngine::on_propose`'s own `sender == proposer(view)` check stays
  fully meaningful for direct/unit-test callers (it's exercised by
  `agb_echo_tests`), and becomes load-bearing again once Phase 7 adds real channel
  identity. Not escalated as a fresh gap — it's the same class of issue Phase 3 already
  flagged and this phase's own §13 explicitly re-affirmed as covering propose/echo/ready.
- **R3's `NoReady` guard read as "ready still pending," not literally "positive gate
  hasn't fired."** §6's text ("If entered and the positive gate hasn't fired by
  e_i + θR: broadcast VantageNoReady") sits directly before "One ready-stage statement
  per view, ever" — and R3 explicitly has no own-echo/positive-gate guard for going
  ready at all (a party can ready purely on others' echoes). Read literally, a party
  whose *own* gate never fired could send `NoReady` at θR even after already having
  sent a real `Ready` via others' echoes reaching quorum earlier — which would violate
  the one-shot rule the very next sentence states. Implemented as: the θR timer fires
  `NoReady` iff `ready_sent` is still false (i.e., `NoReady` occupies the same one-shot
  slot as a real `Ready`, which is the only self-consistent reading).

---

## 6. §9 — output cursor + commit metric

`vantage::cursor::Cursor`: `next_view` (strictly increasing), `output: HashSet<Digest>`
(`D`, seeded with the genesis digest), `output_log: Vec<Digest>` (emission order, used
by the integration test's byte-equality assertion), `core_emitted: HashSet<View>`, and a
`BTreeMap<View, ViewInput>` buffering `Completed`/`Sealed` inputs per view until they
can be processed in order.

`Expand_D` (`Cursor::expand`) reuses a new `BlockCache::collect_verified_chain` (Phase
3's `verified_prefix_through_genesis` is now a one-line wrapper over it — no walk logic
duplicated) that returns the actual genesis-to-tip hash list instead of a bare bool, so
the cursor can emit it. Traversal is per-manifest-entry in the manifest's own order
(already author-sorted, §2/§3), genesis excluded, deduped against `self.output` — which,
because `emit()` updates `self.output` immediately, doubles as the "D + K" dedup base
for a Full seal's `T̂` expansion without needing a second explicit accumulator.

`pump()` is the state machine: at a not-yet-core-emitted view with a `Completed` input,
emit `K` and stop (open); once `Sealed` arrives, `Full` emits `K` (if not already) then
`T̂` and advances, `Core` emits `K` (if not already) and advances without `T̂`, `Skip`
emits nothing and advances (implemented, unreachable — Direct-AGB never produces
`gskip`). A missing/unverified prefix makes `expand` return `None`, and the whole
`pump()` loop simply stops (waits); `Cursor::retry()` (called by `VantageCore` on every
`BlockCached`) re-attempts.

Commit metric: `emit()` logs `info!("Committed {}", header)` per block (reading the
header back out of `BlockCache` for the `Display` impl) and returns
`Effect::NotifyCommitted(commit_millis, by_worker)`; `VantageCore` forwards each
`(WorkerId, Vec<Digest>)` group to the corresponding local worker as
`PrimaryWorkerMessage::Committed` — the exact Phase-2 message shape/observe path, so
`committed_transactions`/the real-latency histogram work unchanged for vantage.

**Scope cut (not gate-required, flagged rather than silently added):** the cursor does
not forward committed blocks to `tx_output`/`analyze()` — `node/src/main.rs`'s
`analyze` is a no-op on every existing assembly today, and §12's gate criteria (sustained
commits, identical output logs, real-latency metric) don't depend on it. Recommend
wiring it (look the digest up via `BlockCache`, `tx_output.send(header)`) whenever a
real downstream consumer needs the committed stream; the digest is already on hand at
every call site that would need it.

---

## 7. §10 — timers and parameters

`config::Parameters::delta_ms: u64` (D4-5, default 1000, `#[serde(default)]`).
`AgbEngine::theta_echo()`/`theta_ready()` compute 5Δ/6Δ from it; both are hard paper
constants, not separately configurable. `VantageCore`'s timer queue is a plain
`Vec<(Instant, View, TimerKind)>`; the run loop computes the minimum deadline each
iteration and uses `tokio::time::sleep_until` in a `biased` `select!` arm ordered after
every message-receiving arm (§5's tie-break rule).

---

## 8. Module plan / layout

`primary/src/vantage/{agb,frontier,cursor,node}.rs` — matches the module plan exactly
(§11), reusing Phase 3's `block.rs`/`lanes.rs`/`repair.rs` as specified. GC/`Cleanup`
remains unwired for the vantage assembly (module plan: "still not wired (N8
discipline; retention unbounded, documented)") — same accepted exposure as Phase 3,
unchanged.

---

## 9. Test coverage map (49 new tests; 75 vantage tests total)

All in `primary/src/vantage/tests/`, cross-referenced against §12's checklist:

| Spec item | Test(s) |
|---|---|
| R1: construction determinism | `frontier_tests::construction_determinism_from_register_state` |
| R1: skip-dedup across author indices | `agb_echo_tests::formed_rejects_duplicate_hash_across_c_and_t` (dedup rule itself); `Frontier::build_manifests`'s defensive dedup has no independently-forceable natural scenario (see §5.2/§4 doc comment) — reviewed by inspection |
| R1: proposes exactly once | `frontier_tests::r1_proposes_exactly_once_for_its_own_turn` |
| R1: frontier boundary (a_i = v-2 ⇒ no propose) | `frontier_tests::frontier_trigger_boundary_a_i_v_minus_2_means_no_propose` |
| R1: non-proposer never triggers | `frontier_tests::non_proposer_never_triggers_r1` |
| R2: positive gate fires on exact satisfaction | `agb_echo_tests::positive_gate_fires_on_exact_predicate_satisfaction` |
| R2: C entry not author_ok blocks gate | `agb_echo_tests::core_entry_not_author_ok_blocks_gate` |
| R2: T entry acked-but-not-held blocks gate | `agb_echo_tests::tip_acked_but_not_held_blocks_gate` |
| R2: tip not strictly containing C blocks gate | `agb_echo_tests::tip_not_strictly_containing_core_blocks_gate` |
| R2: equal-height tip excluded | `agb_echo_tests::equal_height_tip_excluded` |
| R2: malformed → sticky Reject, later ignored, frontier not advanced | `agb_echo_tests::malformed_proposal_sticky_reject_later_versions_ignored`; `frontier_tests::malformed_fixed_proposal_never_advances_frontier` |
| R2: buffered proposal activates on contiguous prefix | `frontier_tests::buffered_proposal_activates_when_contiguous_prefix_arrives` |
| R2: grade-0 fallback / echo-skip at deadlines (injected clock) | `agb_echo_tests::grade0_fallback_fires_at_t1_when_core_ok_but_gate_never_holds`, `echo_skip_at_t1_when_core_not_ok`, `echo_skip_at_absolute_deadline_with_no_fixed_proposal` |
| R2: proposal after θE ignored | `agb_echo_tests::proposal_delivered_after_theta_echo_is_ignored` |
| R2: echo-stage one-shot | `agb_echo_tests::echo_stage_one_shot_after_positive_gate` |
| R3: grade One/Zero/Mix | `ready_tests::ready_fires_at_quorum_with_grade_one`, `ready_grade_zero_when_quorum_all_grade0`, `ready_grade_mix_when_split` |
| R3: Q boundary exact | `ready_tests::q_boundary_exact` |
| R3: no own-echo guard | `ready_tests::ready_without_own_echo_or_fixed_proposal` |
| R3: ready one-shot | `ready_tests::ready_one_shot_per_view` |
| R3: noready at θR (and never after) | `ready_tests::noready_fires_when_ready_pending_at_theta_r_and_never_after`, `noready_is_noop_once_ready_already_sent` |
| R4: completion on mixed-grade quorum | `completion_tests::completion_fires_once_on_mixed_grade_quorum` |
| R4: direct seals on homogeneous quorums | `completion_tests::direct_seal_full_on_homogeneous_grade1_quorum`, `direct_seal_core_on_homogeneous_grade0_quorum` |
| R4: late homogeneous quorum after completion still seals | `completion_tests::late_homogeneous_quorum_after_completion_still_seals` |
| R4: arbiter first-wins + later ignored | `completion_tests::arbiter_first_submission_wins_later_compatible_submission_ignored` (genuinely *different*-outcome collision is structurally unreachable under an honest/consistent dataset — see note below) |
| Fast seal: all-n matching → fastseal → arbiter | `fastseal_tests::fastseal_fires_on_all_n_matching_echoes` |
| Fast seal: lock recorded before matching echo | `fastseal_tests::lock_is_recorded_before_our_matching_echo_is_sent` |
| Fast seal: lock deactivates at f+1 non-matching, never reactivates | `fastseal_tests::lock_deactivates_at_f_plus_1_nonmatching_and_never_reactivates` |
| Fast seal: no complete/direct side effects | `fastseal_tests::fastseal_produces_no_completion_or_direct_side_effects` |
| Cursor: expansion order + cross-view dedup | `cursor_tests::expansion_order_and_cross_view_dedup` |
| Cursor: core-prefix-of-full | `cursor_tests::core_prefix_of_full_property` |
| Cursor: open tip blocks later views | `cursor_tests::open_tip_blocks_later_views_payload` |
| Cursor: gcore skips T̂ | `cursor_tests::gcore_skips_t_hat` |
| Cursor: idempotent duplicate seal | `cursor_tests::idempotent_duplicate_seal` |
| Cursor: missing-prefix wait then emit | `cursor_tests::missing_prefix_wait_then_emit` |
| `Formed_v`/`proposer(v)` themselves | `agb_echo_tests::formed_rejects_unsorted_or_duplicate_author`, `formed_accepts_well_formed_disjoint_manifests`, `proposer_round_robins_over_committee_in_sorted_order` |
| §4 `enter(v)` also activates | `frontier_tests::enter_also_activates_independent_of_frontier_advance` |
| Integration: 4 in-proc engines, ≥3 consecutive views, identical output logs | `integration_tests::four_party_happy_path_three_consecutive_views_identical_output` |

**Note on the arbiter's "incompatible outcome" branch:** under a fully honest, internally
-consistent dataset (the only kind Phase 4's happy-path scope constructs — Byzantine
fault injection is explicitly Phase 6), fast seal's all-n-matching precondition forces
every echo for that exact `B` to be grade-1, which forces R3/R4's own homogeneous-quorum
path to independently agree on the same `Full(C,T)` — so two *genuinely different*
`Outcome` values can never both reach `try_seal` for the same view from two honest-
consistent inputs. The `debug_assert`'s dead branch is exercised only by feeding a
deliberately inconsistent (self-contradicting) `Ready.grade`, which is a Byzantine-
injection scenario out of Phase 4's scope; not added as a test for that reason (see
`completion_tests.rs`'s existing "first submission wins" tests for what *is* covered:
the idempotent-guard behavior that prevents a second `Sealed` effect).

Autobahn's Phase-3 26-test suite (N1-N9 coverage table, PHASE3-NOTES §7) is unchanged
and still green (`chain_tests`/`ack_tests`/`registers_tests`/`repair_tests`/
`retention_tests`/`metrics_tests`).

---

## 10. Gate

`CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 -- --test-threads=4`: **all green** —
`primary` 86 passed (11 Autobahn + 75 vantage) / 6 ignored (pre-existing, unchanged),
`crypto` 7/7, `network` 6/6, `store` 4/4, `worker` 6/6, `config`/`metrics`/`node` 0
tests (unchanged), 0 failed anywhere.

**Autobahn regression** (release build, `./target/release/node local-benchmark --nodes
4 --workers 1 --rate 240000 --tx-size 512 --protocol autobahn-optimistic --mode
all-zero --duration 60`): **240,075 tx/s**, avg latency 110.11 ms (p50/p90/p99
105/199/244 ms), 14,392,775 txs, 0 misses — matches the Phase-2/3 gate range
(239,786-240,997 tx/s, ~109 ms avg) within run-to-run noise. Vantage tasks are never
constructed on this assembly; this is a pure "did anything touching the shared
`Primary::spawn` function change Autobahn behavior" check, and it didn't (the Autobahn
`match` arm is a byte-identical copy of the pre-Phase-4 function body).

**Vantage 4-node** (release build, `--protocol vantage --rate 50000 --tx-size 512
--duration 60`): **48,221 tx/s sustained**, 2,893,284 committed transactions, **0
misses**, avg latency 489.78 ms (p50/p90/p99 486/881/1139 ms) — the higher latency
than Autobahn's is expected at Phase 4 (an extra echo/ready round trip vs. Autobahn's
direct QC path; still well under any θE/θR fallback timeout, i.e. this run is riding the
happy-path positive-gate/fast-seal path, not the fallback timers). Real-latency metric
reports correctly (Phase-2 parity confirmed). "Identical output logs across parties" is
verified at the unit level by
`integration_tests::four_party_happy_path_three_consecutive_views_identical_output`
(byte-equality assertion across 4 in-proc engines over ≥ 3 views); the live 4-process
benchmark run's zero-miss, sustained-throughput result is corroborating evidence at
production scale, not a byte-level cross-node log diff (the harness has no such
instrumentation point today — recommend adding one, e.g. an admin/debug endpoint
exposing `Cursor::output_log()`, before Phase 5's larger-scale runs need it).

**A debug-build-only, pre-existing (not touched by this phase) Autobahn note found
during gate testing:** `primary/src/core.rs`'s `try_prepare_waiting_slots`/
`is_prepare_ticket_ready` path computes `slot + 1 - self.k` unconditionally once
`self.k > 1`; for `slot + 1 < k` (always true near genesis, `k` defaults to 4) this is
an unsigned underflow that panics under a debug build's overflow checks (release
silently wraps and the guarded `contains_key` call simply finds nothing, so it's benign
there). Confirmed via `git stash`/rebuild that this exists identically on the
pre-Phase-4 tree — not a regression, not touched, flagged only because it means the
gate's Autobahn regression run **must** use `--release` (debug hangs/panics within
seconds of any run, independent of Vantage). Recommend a `checked_sub`/guard fix
whenever Autobahn is next touched; out of scope here per "Autobahn paths: zero semantic
changes, ever."

Simplification pass done before writing this file: removed two now-dead test-only
accessors (`AgbEngine::is_active_for_test`, `AgbEngine::delta()`) and an unused test
fixture (`tests::common::test_genesis`) that were never referenced once the test suite
settled. Re-ran the full vantage suite after; unaffected (still 75/75 passing).

---

## 11. Deviations summary (quick index)

- Wire types (`ViewProposal`/`Echo`/`Ready`/`ReadyGrade`/`Manifest`) live in
  `vantage::agb`, not `messages.rs` (§2 above).
- `Effect::Fixed(View, bool)` — new, necessary (§5.1).
- `Effect::Completed(View, Manifest, Manifest)` — new, necessary (§5.1).
- `Effect::ArmTimer` carries `Instant`, not just `(View, TimerKind)` (§5.1).
- `Effect::SyncBatches` gained a `Digest` (header) field (§5.1, §1).
- D4 read as covering `VantagePropose`'s (fieldless) sender the same way it covers
  `Header`'s (§5.2) — interpretive, not a fresh gap, per §13's own standing note.
- R3's `NoReady` guard read as "ready still pending" rather than literally "the
  positive gate hasn't fired" (§5.2) — interpretive, resolves an internal
  inconsistency in the literal reading.
- `tx_output`/`analyze()` not fed from the vantage cursor — explicit, documented, not
  gate-required scope cut (§6).
- GC/`Cleanup` unwired for vantage — inherited Phase-3 stance, unchanged, module-plan-
  sanctioned (§8).

No Autobahn semantics were touched anywhere in this phase (the one pre-existing issue
noted in §10 was found, not introduced, and is left alone per the hard rule).

---

## 12. Fable audit — pass 1 (findings addressed; two-clean-pass counter restarted)

Scope: `agb.rs`, `frontier.rs`, `cursor.rs`, `node.rs`, `collect_verified_chain`, the §9
coverage map. Ruling: engine logic, the frontier completeness argument, cursor
determinism, and `collect_verified_chain`'s author pinning (the P1-1-style
cross-author-graft check, carried over from Phase 3) all check out. Four findings, all
fixed.

**P4-4 — LIVENESS DEFECT, fixed: R2's positive gate wasn't re-polled on two of its three
enabling events.** `VantageCore`'s wiring re-ran `AgbEngine::recheck_all` after
`Effect::BlockCached` (a fresh publish/serve) but not after (a) `Inbound::Ack` →
`LaneManager::process_ack` (an ack crossing the f+1/2f+1 threshold for a C/T entry
that's only `author_ok` via `is_q_available`, never actually published to us) or (b)
`rx_payload_ready` → `LaneManager::set_payload_ready` (a block already directly
published but missing its payload, where the *payload's* arrival — not a fresh block —
is what flips `direct_pub`/`author_ok` true). If the *last* event that would have
satisfied the gate for a given view was an ack or a payload arrival rather than a fresh
block, the echo would never fire and that view would silently stall forever — masked in
the local-benchmark gate run (§10) only because continuous own-lane publication kept
producing `BlockCached` wakeups that happened to also cover it.

Fixed in `primary/src/vantage/node.rs`: both dispatch sites now extend their effects
with `self.agb.recheck_all(now, &mut self.lm, &mut self.rep)`, mirroring the
`BlockCached` arm exactly —
`dispatch_inbound`'s `Inbound::Ack` arm (after `lm.process_ack`) and the `rx_payload_ready`
branch in `run()` (after `lm.set_payload_ready`, still gated on "every outstanding key
for this header resolved," §1/§5's D1 bookkeeping — unchanged).

Regression tests (drive `AgbEngine` + `LaneManager` directly, reproducing the wiring's
exact call sequence at each site — `primary/src/vantage/tests/agb_echo_tests.rs`):
`positive_gate_fires_when_final_enabling_event_is_an_ack` (gate blocked on a
never-published C entry; 2 first-hand acks cross `is_q_available`; `recheck_all` fires
the echo) and `positive_gate_fires_when_final_enabling_event_is_a_payload_ready` (gate
blocked on a directly-published-but-payload-missing entry; the batch marker is written,
`set_payload_ready` then `recheck_all` fires the echo).

**P4-1 — minor, fixed (N9 hygiene): a malformed echo grade was silently folded into the
grade-0 tally.** `AgbEngine::on_echo` counted any `grade` byte other than 0/1 as if it
were a legal grade-0 echo (occupying the sender's one-shot echo-stage slot, entering the
R3 grade-0 tally, and counting as "non-matching" for fast-seal lock deactivation) —
behaviorally harmless (equivalent to a legal grade-0 statement everywhere it mattered)
but a genuine N9 violation: the model never counts a malformed message at all. Fixed:
`on_echo` now returns immediately (no effects, no counting) when `echo.grade > 1`, before
touching `count_echo_statement`. Regression test:
`echo_with_out_of_range_grade_is_dropped_not_counted` — a grade-2 echo produces no
effects and does *not* occupy the sender's slot (proven by driving that same sender's
follow-up legal grade-1 echo, plus two more senders, all the way to R3's quorum; had the
malformed echo consumed the slot, quorum would never be reached).

**P4-2 — minor, fixed (memory): a late/duplicate `Completed`/`Sealed` input for a view
already advanced past leaked a `pending` entry forever.** `Cursor::on_completed`/
`on_sealed` unconditionally did `self.pending.entry(view).or_default()...` before
`pump()`; for `view < self.next_view` (a late seal-arbiter/completion signal arriving
after the cursor already advanced past that view — the arbiter's own first-wins/
later-ignored discipline doesn't prevent a late *duplicate* signal from reaching the
cursor at all, only from producing a second `Sealed` effect upstream) this recreated a
`pending` entry that `pump()` would never look at again (it only ever inspects
`self.next_view`), leaking one `ViewInput` per such late arrival indefinitely. Fixed:
both methods now check `view < self.next_view` first and return `Vec::new()` immediately
with no `pending` mutation at all. `cursor_tests::idempotent_duplicate_seal`'s existing
second `on_sealed` call for the same (by-then-already-advanced-past) view now exercises
exactly this new early-return path (previously it happened to be harmless-in-effect via
`pump()`'s own idempotency, but did leak the entry) — re-ran, still green, same
assertions.

**P4-3 — minor, fixed (resource, honest-traffic leak): `VantageCore::cancel_handlers`
grew without bound.** Every `broadcast`/`send_to` call appended its `CancelHandler`s
(`oneshot::Receiver<Bytes>`) and nothing ever removed one, even after the underlying
message was long since ack'd — unlike Autobahn's `Core`, which GCs its
`cancel_handlers`/`consensus_cancel_handlers` maps by round. Fixed with a
`prune_cancel_handlers` pass at the top of `VantageCore::run`'s main loop (every
iteration, cheap — `Vec::retain_mut` over what is, at steady state, a small set): a
handler is kept only while `handler.try_recv()` returns `Err(TryRecvError::Empty)`
(still genuinely in flight); `Ok(_)` (ack'd) or `Err(Closed)` (connection died, will
never resolve) both mean it's done and it's dropped. This preserves
`ReliableSender`'s retransmit-until-ack semantics exactly: dropping a `CancelHandler`
that is still `Empty` would cancel that retry (per `network::reliable_sender`'s
`Connection::keep_alive`, which checks `handler.is_closed()` before every send/retry
attempt), so the fix only ever drops handles that can no longer affect delivery either
way. Chosen over a per-view/per-digest keying scheme because it needs no new
bookkeeping keyed by protocol state (views, digests) that would have to be kept in sync
with the cursor/AGB's own view-advance logic — the oneshot's own resolution state is
already the exact signal needed, and pruning once per loop iteration is O(handlers)
against a set that stays small under normal operation (each handler is alive only for
the time between a send and its ack, not the life of the view).

Two items recorded per instruction, no code change:

1. **Phase-5 precondition, added to the §5 TODO surface:** `AgbEngine::enter` does not
   arm `EchoFallback` when a proposal is already fixed at entry time (only `on_propose`
   arms it, at the moment `ρ_i` first becomes known) — unreachable in Phase 4 because
   entry only ever happens once, at boot, strictly before any proposal could possibly
   have arrived yet. Once Phase 5's WISH pacemaker allows entering a view *after* its
   proposal may already be fixed (e.g. re-entering on a view-change), `enter` will need
   to also arm `EchoFallback` (using the already-known `first_proposal_instant`) if
   `fixed` is already `Proposal(_)` and echo is still pending at that point — today
   `enter`'s call to `activate` only re-checks the *positive* gate, it does not arm the
   fallback deadline. Flagging now so Phase 5's implementer doesn't have to rediscover
   it from scratch.
2. **Accepted deviation, cursor log line:** `Cursor::emit` logs
   `info!("Committed vantage block {}", entry.block)` (using `Header`'s own `Display`,
   which renders as `B{height}({author})`) rather than literally reproducing Autobahn's
   `"Committed {header}"` shape verbatim. No harness/tool parses this specific line
   today (unlike the `#[cfg(feature = "benchmark")]` `"Created {} -> {:?}"` /
   `"Committed {} -> {:?}"` lines the fab log-parsing pipeline actually consumes, which
   this phase never touches), so this is a cosmetic difference with no functional
   effect; noted for awareness in case a future phase's tooling ever wants to grep for
   an exact string.

**Post-fix verification:** `CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 --
--test-threads=4` — all green. `primary`: 89 passed (11 Autobahn + 78 vantage, up from
75: +3 new regression tests for P4-4(i)/P4-4(ii)/P4-1) / 6 ignored (unchanged) / 0
failed; `crypto` 7/7; `network` 6/6; `store` 4/4; `worker` 6/6; `config`/`metrics`/`node`
0 tests (unchanged). No other files touched beyond `agb.rs` (P4-1),
`cursor.rs` (P4-2), `node.rs` (P4-3, P4-4), and one new test module addition to
`agb_echo_tests.rs` (P4-1/P4-4 regression tests).

---

## 13. Fable audit — gate closed (2026-07-23)

Restarted two-pass sequence after the P4-1..P4-4 round:

- **Pass 1 (post-fix): CLEAN.** All four fix hunks verified (Ack + payload-ready arms
  now repoll the R2 gate; grade>1 echoes dropped before counting; cursor stale-view
  guards on both entry points; prune_cancel_handlers retains only in-flight oneshots,
  preserving retry-until-ack). Three new regression tests genuine. Independent run:
  primary 89 passed / 6 ignored / 0 failed.
- **Pass 2: CLEAN.** Fresh §§4–9 conformance sweep: R1 trigger completeness (a_i+1
  argument), R2 one-shot + gate-repoll event set now complete (block-cached, direct
  mark, payload, ack — every author_ok input has a hook), R3/R4 tallies and one-shots,
  fast-seal lock lifecycle, cursor determinism + D+K dedup + stale guard, wiring
  self-delivery completeness, effect-cascade termination (all one-shot/monotone).

**Phase-4 gate: CLOSED** (two consecutive clean adversarial passes). Carried into
Phase 5: (a) `AgbEngine::enter` must arm `EchoFallback` when a proposal is already
fixed (recorded §12); (b) formal entry/WISH per paper sec:pacemaker; (c) crash-fault
liveness runs. Suggested Phase-4 commit split: (1) Header/wire + agb.rs engine +
tests, (2) frontier/cursor/node wiring, (3) config param + node assembly, (4) docs.
