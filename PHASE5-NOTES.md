# Phase 5 notes — deviations, decisions, inventory

Companion to `PHASE5-SPEC.md`, same role `PHASE3-NOTES.md`/`PHASE4-NOTES.md` played for
their phases. No git commits were made (working tree left dirty for review, per
standing instruction). Written after implementation, in spec order.

---

## 0. Summary

Implemented: the WISH pacemaker (`vantage::pacemaker::Pacemaker`, new module) covering
W1 (genesis wish(2) + ω-array init), W2 (receipt/amplification/formal-entry-target
advance, strict order), W3 (the two-response wish trigger, surfaced by
`AgbEngine::two_response_wish_target`), W4 (piggyback outside identity, on all four
response messages + a standalone `VantageWish`), W5 (entry semantics: θE/θR arming,
the Phase-4 carry-over `EchoFallback` fix, the `Frontier` formal-entry floor), and W6
(retention — already true, documented, no code change); the §2 wire additions;
`VantageCore`'s wish routing/absorption/stamping and `Effect::Enter` execution; formal
entry now live for every view (not just genesis's view 1); the full §4 test suite (24
new tests, on top of Phase 4's 89 — 113 `primary` tests total, all passing).

Gate: workspace tests all green (113 `primary` / 6 ignored, unchanged Autobahn count
from Phase 4; 7 `crypto`, 6 `network`, 4 `store`, 6 `worker`, 0 elsewhere); Autobahn
regression benchmark reproduces the Phase 2-4 throughput number; a real 4-node
`local-benchmark --protocol vantage` run sustains ~49.3k tx/s at zero misses — matches
Phase 4's ~48.2k within run-to-run noise (wish piggybacking does not measurably
regress it). Full numbers in §7 below.

---

## 1. §1 — WISH rules (`vantage::pacemaker::Pacemaker`)

New module. Owns the n-slot `omega` array (D5-1: order statistics by **party count**,
not stake — `f_plus_1_parties = (n-1)/3 + 1`, `two_f_plus_1_parties =
2*((n-1)/3) + 1`, matching `AgbEngine`'s own `f_plus_1_parties` formula exactly), the
own-wish high-watermark, the current entry target (`omega_q`'s high-water mark), and
`largest_entered_view` (the local monotonic entry counter).

- **W1**: `Pacemaker::genesis()` — called once at boot, immediately after
  `VantageCore::enter_view_effects(1, boot)` (the existing boot behavior). Records
  `largest_entered_view = 1` (so W2's "missing views through target" bookkeeping never
  re-schedules view 1), then raises the own wish to 2 via `raise_own_wish` (own-slot
  update *is* "self-delivery immediate" — no separate self-addressed round trip) and
  returns `[Effect::BroadcastWish(2), ...raise_own_wish's own effects]`.
- **W2**: `Pacemaker::on_wish(sender, x)` — `omega[j] = max(omega[j], x)`; recompute
  `omega_plus` (rank `f_plus_1_parties`) and, only if it increased past the own
  watermark, amplify (`Effect::BroadcastWish(omega_plus)`, update own slot +
  watermark) — **then** recompute `omega_q` (rank `two_f_plus_1_parties`, over the
  *possibly just-updated* array) and, if it increased past the entry target, raise the
  target and emit `Effect::Enter(v)` for every `v` in `(largest_entered_view,
  entry_target]`, ascending. A stale wish (`x <=` the sender's current slot) leaves
  the array unchanged, so both statistics are unchanged too and every check is a
  natural no-op — no special-cased "is this stale" branch needed.
- **W3**: `Pacemaker::raise_own_wish(x)` — unconditional own-slot raise (no-op if `x`
  isn't larger than the current watermark), never itself broadcasts (the raised
  watermark instead piggybacks on the very response effect about to be emitted, via
  `Effect::RaiseWish`, see §2 below) but can still return `Effect::Enter` if the raise
  alone crosses `omega_q`'s threshold (independent of amplification, per the spec's
  "the two updates are independent").
- **W4/D5-2**: piggyback carried on `Echo`/`Ready`/`EchoSkip`/`NoReady` + a standalone
  `VantageWish` (§2).
- **W5**: `Pacemaker` itself does not implement (a)-(d) — those live in
  `AgbEngine::enter`/`Frontier::enter` (§3 below); `Pacemaker` only decides *when* to
  enter (`Effect::Enter`), not what entering does.
- **W6**: unchanged from Phase 4's stance — the ω array is bounded (one slot per
  author, `Vec<View>` of length n); per-view AGB state for far-future views remains
  unbounded (documented Phase-6 concern, same class as before — no new exposure, just
  a new bounded structure alongside the existing per-view maps).

Tests: `pacemaker_tests.rs` (8, pure — no wiring, per the suggested implementation
order).

---

## 2. §2 — wire

- `Echo`/`Ready` (`vantage::agb`) gain a `wish: View` field (outside `proposal_digest`
  identity — never read by any counting/dedup logic).
- `PrimaryMessage::VantageEchoSkip(View, PublicKey)` →
  `VantageEchoSkip(View, PublicKey, View /* wish */)`; `VantageNoReady` likewise.
- `PrimaryMessage::VantageWish(View, PublicKey)` appended last (after
  `VantageNoReady`) — bincode wire-compat rule, same as every prior Vantage-only
  variant.
- `vantage::node::Inbound` mirrors all of the above (`EchoSkip`/`NoReady` gain a
  trailing wish field; new `Wish(View, PublicKey)` variant).

**D5-3, implemented as specified, no deviation**: `AgbEngine` constructs `Echo`/`Ready`
with `wish: 0` (a placeholder — never read before being overwritten) and
`BroadcastEchoSkip`/`BroadcastNoReady` still carry no wish field at all (unchanged
shape, just a `View`); `VantageCore::execute`'s `Effect::BroadcastEcho`/
`BroadcastReady`/`BroadcastEchoSkip`/`BroadcastNoReady` arms stamp
`self.pacemaker.own_watermark()` into the outgoing message immediately before
serializing. The engine never touches `Pacemaker`.

---

## 3. §3 — implementation map

- **`Pacemaker`**: §1 above.
- **`AgbEngine`**:
  - `enter(v, ...)` gains the W5(b) carry-over fix: if a proposal is already fixed for
    `v` and the echo is still pending at entry time, arm `EchoFallback` at
    `min(max(e_i(v), rho_i(v)) + Delta, e_i(v) + theta_E)` — the exact formula
    `on_propose` uses, with `e_i(v) = now` (entry is happening this instant) and
    `rho_i(v) = first_proposal_instant` (already recorded by `on_propose`, whenever it
    ran, before or after entry).
  - `two_response_wish_target(view, stage) -> Option<View>` (private; `ResponseStage`
    enum, `Echo`/`Ready`) — the W3 hook, a pure query over already-recorded
    `echo_sent`/`ready_sent` flags. Consulted (via the `wish_effect` helper, which
    wraps a `Some` result as `Effect::RaiseWish`) at all five response-emission call
    sites: `recheck_gate` (positive-gate echo), `on_echo_fallback_timer` (grade-0
    echo/echo-skip), `on_echo_absolute_timer` (echo-skip), `recheck_ready` (ready),
    `on_ready_timer` (no-ready) — pushed immediately before that site's own response
    effect.
  - W1's genesis-response boundary (views `<= 0` are fixed, always-sent-by-everyone
    conventions) is consulted exactly once: the `Echo` stage's `u - 1` reference when
    `u = 1` (`Ready`'s `u + 1` reference is always `>= 2`, so it never needs the
    boundary).
- **`Frontier::enter(v)`** (W5(c)): now floors `a_i` to `max(a_i, v-1)` and re-runs the
  contiguous well-formed-prefix advance from the new floor (the same loop
  `record_fixed` uses), on top of the existing "also activates" behavior. Return shape
  changed from `Option<View>` to `Vec<View>` (matching `record_fixed`'s existing
  shape) — every view newly activated by the call, ascending. This is a deliberate
  loosening of `a_i`'s invariant: it is no longer purely "the verified contiguous
  well-formed prefix length," but also a liveness floor independent of actual
  completion (exactly the spec's stated intent).
- **`VantageCore`**:
  - New `pacemaker: Pacemaker` field, constructed in `spawn`.
  - New `enter_view_effects(view, now)` helper — `AgbEngine::enter` +
    `Frontier::enter` (+ activating whatever `Frontier::enter`'s floor newly unlocks,
    exactly like `Effect::Fixed`'s existing handling) + `try_propose_effects`. Used by
    both the genesis boot call (view 1) and `Effect::Enter`'s execution — the two call
    sites the spec names are now one shared function, not duplicated.
  - Boot sequence: `enter_view_effects(1, boot)` then `pacemaker.genesis()`, executed
    together (W1's stated order).
  - `dispatch_inbound`: every response arm (`Echo`/`EchoSkip`/`Ready`/`NoReady`) now
    absorbs its piggybacked wish via `pacemaker.on_wish(sender, wish)` **before**
    handing the response to `AgbEngine` (amplification/entry, then the response's own
    processing — the spec's stated ordering is fine either way since they're
    independent, but this keeps all five wish-bearing arms symmetric); `Inbound::Wish`
    absorbs only.
  - `execute`: new `Effect::BroadcastWish` (serialize + broadcast `VantageWish`),
    `Effect::Enter` (→ `enter_view_effects`), `Effect::RaiseWish` (→
    `pacemaker.raise_own_wish`) arms; the four response-broadcast arms now stamp the
    watermark (D5-3, §2 above) before serializing.
- No new timers — confirmed: WISH is purely event-driven, and θE/θR now arm for every
  entered view for free (they always did, per-view; Phase 4 just never called `enter`
  for anything but view 1).

---

## 4. New `Effect` variants (deviations, all additive)

- `Effect::BroadcastWish(View)` — W2 amplification's standalone wire message.
- `Effect::Enter(View)` — W2's formal-entry-target-advance step; spec-named
  (implementation map §3's own phrasing), not a deviation.
- `Effect::RaiseWish(View)` — **necessary, minimal channel**, not spec-named: the only
  way for `AgbEngine`'s pure `two_response_wish_target` query to actually reach
  `Pacemaker::raise_own_wish` without giving the engine a `Pacemaker` dependency
  (D5-3's separation). Pushed immediately before the response effect it feeds, so
  `VantageCore::execute`'s FIFO `VecDeque` always processes it first — the same
  pattern Phase 4 used for `Effect::Fixed`/`Effect::Completed` (necessary,
  minimal, additive cross-component channels, each documented as a deviation there
  too).

---

## 5. Test coverage map (24 new tests; 113 `primary` tests total)

All in `primary/src/vantage/tests/`, cross-referenced against §4's checklist. New
shared harness (`harness.rs`) generalizes Phase 4's `integration_tests.rs`-local
`Node`/`drain_local`/`run_to_quiescence` into a reusable module (adds `Pacemaker`
wiring, a per-node timer queue + `fire_due_timers`/`advance_time` for injected-clock
tests, `alive` for crash simulation, and `wish_partitioned`/`held_wishes` for the
convergence test) — shared by `integration_tests.rs` (updated in place, same test),
`wish_trigger_tests.rs`, `crash_fault_tests.rs`, and `convergence_tests.rs`.

| Rule | Test(s) | File |
|---|---|---|
| W1 (genesis wish(2), self-delivery, ω init) | `w1_omega_initializes_to_zero_for_every_author`, `w1_genesis_sets_own_wish_to_2_and_broadcasts_with_self_delivery` | `pacemaker_tests.rs` |
| W2 (ω⁺/ω^Q boundaries at exactly f+1/2f+1) | `w2_omega_plus_boundary_exactly_f_plus_1_senders`, `w2_omega_q_boundary_exactly_two_f_plus_1_senders_independent_of_amplification` | `pacemaker_tests.rs` |
| W2 (amplify-then-entry order; increasing-order entry) | `w2_amplification_precedes_entry_and_entry_is_recorded_in_increasing_order` | `pacemaker_tests.rs` |
| W2 (a wish for x supports every view ≤ x) | `w2_a_wish_for_x_supports_every_view_up_to_x` | `pacemaker_tests.rs` |
| W2 (stale wish → no transition) | `w2_stale_wish_causes_no_transition` | `pacemaker_tests.rs` |
| W3 (raise_own_wish never broadcasts, no-op below watermark) | `raise_own_wish_never_broadcasts_and_is_a_no_op_below_current_watermark` | `pacemaker_tests.rs` |
| W3 (echo completes pair → wish ≥ u+2, order) | `w3_echo_stage_completes_pair_raises_wish_to_u_plus_2`, `w3_echo_stage_without_the_pairing_ready_never_raises` | `wish_trigger_tests.rs` |
| W3 (ready completes pair → wish ≥ u+3, order) | `w3_ready_stage_completes_pair_raises_wish_to_u_plus_3`, `w3_ready_stage_without_the_pairing_echo_never_raises` | `wish_trigger_tests.rs` |
| W4 (duplicate statement counts once, wish absorbed both times) | `w4_duplicate_response_counted_once_but_wish_absorbed_both_times` | `wish_trigger_tests.rs` |
| W4 (piggyback alone drives entry, zero standalone wishes) | `w4_piggybacked_wish_alone_drives_entry_with_no_standalone_wish_messages` | `wish_trigger_tests.rs` |
| W4 (piggyback on all four response types) | `w4_piggyback_rides_on_all_four_response_types` | `wish_trigger_tests.rs` |
| W5 (entry arms θE/θR) | `w5_entry_arms_echo_and_ready_absolute_timers` | `agb_echo_tests.rs` |
| W5 (entry never re-enters) | `w5_entry_never_re_enters` | `agb_echo_tests.rs` |
| W5(b) (carry-over regression, PHASE4-NOTES §12) | `w5b_entry_after_already_fixed_pending_proposal_arms_echo_fallback_carry_over_regression` | `agb_echo_tests.rs` |
| W5(c) (frontier floor: raises, never lowers, re-runs contiguous advance) | `enter_floors_a_i_to_v_minus_1`, `enter_floor_never_lowers_a_i`, `enter_floor_re_runs_contiguous_advance_from_new_floor` | `frontier_tests.rs` |
| W5(c) (floor enables R1 without having seen v-1) | `enter_floor_enables_r1_without_having_seen_v_minus_1` | `frontier_tests.rs` |
| Crash-fault (kill proposer(v); entry/echo-skip/no-ready continue; cursor blocks) | `crash_fault_dead_proposer_view_blocks_output_but_entry_and_later_views_proceed` | `crash_fault_tests.rs` |
| Convergence (partition one party's inbound wishes, release, rejoin) | `convergence_partitioned_party_enters_all_missed_views_on_release` | `convergence_tests.rs` |

Existing Phase 3/4 suites unaffected in count except where the wire-shape change
required a mechanical fixup (added `wish: 0`/`wish` fields to pre-existing `Echo`/
`Ready` literals and helper constructors in `fastseal_tests.rs`, `ready_tests.rs`,
`completion_tests.rs`; `Frontier::enter`'s return-type change from `Option<View>` to
`Vec<View>` updated in `frontier_tests.rs`'s one existing caller) — no assertions in
those files changed in substance.

---

## 6. Deviations summary (quick index)

- `Effect::RaiseWish(View)` — new, necessary (§4).
- `Frontier::enter`'s return type: `Option<View>` → `Vec<View>` (§3) — a genuine
  behavior extension (the floor can now activate more than one view per call), not
  just a signature change; the module plan's phrase "existing return shape" is read as
  referring to `record_fixed`'s `Vec<View>` shape, which `enter` now shares.
  Interpretive, not a fresh gap.
- `VantageCore::enter_view_effects` — a new private helper, not named by the spec,
  factoring out the "AgbEngine::enter + Frontier::enter + activate cascade +
  try_propose" sequence the spec's §3 describes twice (genesis boot, `Effect::Enter`)
  into one function. Reduces duplication; no behavior change from doing it inline
  twice.
- No Autobahn semantics were touched anywhere in this phase.

---

## 7. Gate

`CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 -- --test-threads=4`: **all green** —
`primary` 113 passed (11 Autobahn + 102 vantage, up from 89: +24 new) / 6 ignored
(pre-existing, unchanged) / 0 failed; `crypto` 7/7; `network` 6/6; `store` 4/4;
`worker` 6/6; `config`/`metrics`/`node` 0 tests (unchanged).

**Autobahn regression** (release build, `--features benchmark`, `./target/release/node
local-benchmark --nodes 4 --workers 1 --rate 240000 --tx-size 512 --protocol
autobahn-optimistic --mode all-zero --duration 60`): **239,878 tx/s**, avg latency
109.30 ms (p50/p90/p99 106/200/242 ms), 14,392,673 txs, 0 misses — matches the
Phase 2-4 gate range (239,786-240,997 tx/s, ~109-110 ms avg) within run-to-run noise.
Vantage tasks are never constructed on this assembly; nothing in this phase touches
`Primary::spawn`'s Autobahn arm at all.

**Vantage 4-node** (release build, `--protocol vantage --rate 50000 --tx-size 512
--duration 60`): **49,297 tx/s sustained**, 2,957,844 committed transactions, **0
misses**, avg latency 516.32 ms (p50/p90/p99 510/924/1211 ms) — matches Phase 4's
48,221 tx/s within run-to-run noise; wish piggybacking (an extra `View` on every
response, plus the occasional standalone `VantageWish`) does not measurably regress
throughput. Latency is slightly higher than Phase 4's 489.78 ms average, consistent
with the small additional per-message serialization cost of the wish field family
(within noise; not a regression pattern — misses stayed at 0 and TPS held).

A benign, pre-existing (not touched by this phase) teardown-time panic pattern was
observed *after* the benchmark's summary line prints, during process shutdown
(`worker`/`store`/`network` background tasks hitting closed channels as the process
exits) — cosmetic, does not affect the measured result, and is orthogonal to anything
this phase changed (the same shutdown path exists on Autobahn assemblies too).

Simplification pass done before writing this file: verified zero new compiler
warnings anywhere touched by this phase (`cargo build -p primary --tests` diffed
against the pre-existing warning set — the only vantage-adjacent warning present,
an unnecessary `mut` in `agb_echo_tests.rs`'s pre-existing
`echo_with_out_of_range_grade_is_dropped_not_counted` test, predates this phase and
is unrelated to the `wish` field it happens to sit next to).

---

## 8. Flagged decisions (status)

- **D5-1**: implemented as specified — party-count order statistics, stake-independent
  (§1).
- **D5-2**: implemented as specified — piggyback field on all four response messages +
  standalone `VantageWish` (§2).
- **D5-3**: implemented as specified, no deviation — watermark stamped at
  serialization time in `VantageCore`; `AgbEngine` never touches `Pacemaker` (§2, §3).

No open questions were found that required stopping to ask; nothing in this phase
touched Autobahn protocol semantics.

---

## Fable audit — gate closed (2026-07-23)

- **Pass 1: CLEAN.** pacemaker.rs W1/W2 verified (strict update→amplify→enter order;
  the raise-own-wish amplification invariant holds: raising the own slot bounds the new
  ω⁺ by the new watermark, so no unamplified state is reachable; stale wishes provably
  no-op). W3 arithmetic exact both directions, including the genesis edge (R(0) counts
  as the fixed genesis response). All five response-emission sites hooked. enter()'s
  W5(b) EchoFallback arm matches the paper's t = max(entry, ρ) window. Frontier floor +
  contiguous re-advance correct. Node wiring absorbs piggybacked wishes before engine
  processing; Enter effects ascend. Crash-fault test asserts the cursor blocks exactly
  at the dead view. Independent run: primary 113 passed / 6 ignored / 0 failed.
- **Pass 2: CLEAN.** RaiseWish precedes the response broadcast in every emission site's
  effect vector; serialization-time stamping therefore carries the raised watermark
  (W3's "before emitting" requirement). Wish field verified outside counting identity
  (it lives on Echo/Ready, not ViewProposal; proposal_digest unaffected). Entry cascade
  terminates (strictly-increasing largest_entered_view bounds it). Enter→activate
  idempotence race-free. Deviations (RaiseWish effect, Frontier::enter Vec return,
  enter_view_effects helper) all accepted.

**Phase-5 gate: CLOSED** (two consecutive clean adversarial passes). Suggested commit
split: (1) pacemaker.rs + wire/piggyback, (2) agb/frontier/node W3/W5 changes,
(3) test harness + new test files, (4) docs.
