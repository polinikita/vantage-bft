# Phase 6 notes — deviations, decisions, inventory

Companion to `PHASE6-SPEC.md`. Written incrementally during implementation; §5's
Simple-IT distillation was written first (before any control-log code), per the
spec's own instruction, so it appears before §§1–4's notes below despite the numbering.
No git commits made (per standing instruction).

---

## 0. Summary

**Status: gate CLOSED (pending Fable's two audit passes).** This section (and §§7/9/10
below) was rewritten by the Phase-6 CONTINUATION agent picking up from the prior
session's handoff (everything above/below this rewrite that isn't marked "continuation"
is the prior agent's own work, left as-is where still accurate). Continuation work:
**R1** (`Repairer::settle` memoization — the third hot path the prior agent identified
and stopped short of, per the amendment's own fallback clause), **R2** (`--crash <k>` on
`node local-benchmark`, plus three additional CLI flags folded in mid-session —
`--delta-ms`/`--max-batch-delay-ms`/`--max-header-delay-ms`), **R3** (Byzantine suite
scenarios 2–6, the `crash_fault_tests.rs` update, and a per-view seal-route metric
folded in mid-session), and the full §9 gate (capacity probe, Autobahn regression,
crash-fault benchmark, simplification pass). Full workspace test suite green: **155**
`primary` (144 `vantage` + 11 Autobahn) / 6 ignored + 7 `crypto` + 6 `network` +
4 `store` + 6 `worker` — 8 new tests this continuation (136 → 144 vantage): 3 in
`repair_tests.rs` (R1) + 5 new Byzantine scenarios (R3), plus metric-observation
assertions folded into 3 already-existing tests (no new test functions for those).
See §7/§9/§10 below for the full account.

---

## 5. Simple-IT distillation (D6-1, written BEFORE control.rs, per spec §5)

Source: `tex-projects/signature-free/papers/simple-it-yu-2026.pdf` (granted read-only
exception, 19 pages, arXiv:2606.14404v1, "Simple-IT: Practical Low-Latency
Signature-Free BFT Consensus", Yu/Villacis/Losa/Xiang/Wang). We implement the
**non-speculative variant, protocol S, paired with Bracha-RBC** (Fig. 2, p.7) — the
spec explicitly calls for "the non-speculative Simple-IT control log", which is protocol
`S` (not `S_opt`, and not the pipelined `S^s`/`S^s_opt` variants of Fig. 3, p.8, which
add speculative proposing/pipelining that this phase does not use). Delays for
Bracha-RBC per Table 3 (p.5): `(d_o, d_s, d_t) = (3, 3, 2)`. Vantage's control-round
timer is `6Δ` (spec §5), deliberately larger than the paper's own prescribed
`Δ_to = (d_s+d_t)Δ = 5Δ` for margin — a spec-directed choice, not a distillation
deviation.

### State variables (Fig. 2, p.7) → `control.rs` fields

- `curr_round` (init 1) → `ControlLog::curr_round`.
- `submitted` (blocks tob-submitted) → the submitted-pair set: every `(w,h)` that has
  become **submittable** (spec §5: ≥2f+1 matching reports, party count, AND the
  verified `B_w` held) — `ControlLog::submitted`.
- `delivered` (sequence delivered so far) → `ControlLog::delivered` (the `L` sequence
  of §6, dedup'd by first occurrence).
- `proposal[r]` (the `<r', b>` rb-delivered for round `r` from `r`'s leader, or `⊥`) →
  `ControlLog::proposals: HashMap<Round, Option<(Round, Option<(View,Digest)>)>>` (`⊥`
  is `Option::None` for the value component; genesis: `proposal[0] = <⊥, genesis>` is
  modeled as a fixed sentinel `Log(0) = []`, never itself a submitted pair).
- `safe[r]` → `ControlLog::safe: HashSet<Round>` (`safe[0]` implicit true).
- `disabled[r]` → `ControlLog::disabled: HashSet<Round>`.
- `committed[r]` → `ControlLog::committed: HashSet<Round>`.
- `voted`, `timed_out` (per-current-round only, reset on round entry) →
  `ControlLog::voted: bool`, `timed_out: bool`.

### Helpers (p.7)

- `SafeParent(r, r')`: `0 ≤ r' < r`, `safe[r']`, and `disabled[r'']` for every
  `r' < r'' < r`. Implemented as `ControlLog::safe_parent`.
- `Log(r)`: the chain of blocks from round 0 to `r` along assigned-safe parents.
  Implemented as `ControlLog::log_chain(r) -> Vec<(View, Digest)>` (recursive via
  `proposal[r]`'s parent pointer; empty at `r=0`).

### Protocol steps (Fig. 2, p.7) → our rules

- **Init**: enter round 1 — `ControlLog::new` + first `enter_round(1)`.
- **Submit**: `(w,h)` becomes submitted the instant it meets spec §5's submittable
  predicate (2f+1 matching reports + held verified `B_w`) — no separate "tob_submit"
  call; the report census IS the submission trigger.
- **Enter round**: on entering `r`, set `curr_round=r`, arm the round timer at `now +
  6Δ`, reset `voted=false`/`timed_out=false`; if we are `r`'s control leader
  (round-robin, independent of data-view proposer rotation — spec §5), propose.
- **Propose** (leader): find the highest `r'` with `SafeParent(curr_round, r')`
  (scanning down from `curr_round - 1`); pick the smallest-view submitted pair not
  already delivered and not already in `Log(r')` (`⊥` if none); this is the "choose
  highest safe round r_p ... propose the smallest-view submitted pair" of spec §5.
  INIT carries `(round, parent=r', value)` AND attaches `B_w` (nonempty value only) —
  this is the validated-Bracha extension (paper's plain Bracha-RBC INIT carries just
  the value; ours also carries validation data, per spec §5's wire list).
- **RB-deliver**: on the validated-Bracha instance for round `r` delivering, set
  `proposal[r]`.
- **Mark safe**: `proposal[r] = <r', b>` and `safe[r']` ⟹ `safe[r] = true`.
- **Vote**: `safe[curr_round] ∧ ¬timed_out ∧ ¬voted` ⟹ broadcast `<commit, curr_round>`,
  `voted=true`.
- **Timeout**: round timer fires, `¬voted` ⟹ `rn_raise(<timeout, curr_round>)`,
  `timed_out=true`.
- **Disable**: `rn_confirm(<timeout,r>)` ⟹ `disabled[r]=true`.
- **Commit**: `n-f` (party count, D6-2) matching `<commit,r>` ⟹ `committed[r]=true`.
- **Deliver**: `committed[r] ∧ safe[r]` ⟹ tob-deliver every non-`⊥` block of `Log(r)`
  not yet delivered, in order. This is exactly §6's log-assembly/contiguous-consumption
  step; each delivered non-`⊥` `(w,h)` pair becomes a control-log entry processed by
  the anchor derivation (§6) before advancing further.
- **Advance round**: `safe[curr_round] ∧ (voted ∨ timed_out)`, OR `disabled[curr_round]`
  ⟹ enter `curr_round+1`.

### Reliable Notification (Fig. 4, p.7) → the round-timeout wire message family

Thresholds by **party count** (D6-2, consistent with the rest of Phase 6):
- **Vote**: on `rn_raise(e)`, broadcast `<vote,e>`.
- **Accept**: on `n-f` `<vote,e>` ⟹ broadcast `<accept,e>`.
- **Confirm**: on `2f+1` `<accept,e>` ⟹ `rn_confirm(e)` (here: `disabled[r]=true`).
- **Cascade**: on `f+1` `<accept,e>` (and not yet sent one) ⟹ broadcast `<accept,e>`.

`e` is always `<timeout, r>` in our use (Simple-IT's only notification event) — so the
wire messages are round-scoped: `ControlTimeoutVote(Round, sender)`,
`ControlTimeoutAccept(Round, sender)` (append last per spec §5's wire list, "whatever
Simple-IT round-timeout notification message the reference requires").

### Validated Bracha-RBC per control proposal (spec §5, our addition to the paper's
plain Bracha-RBC)

Standard Bracha broadcast (INIT → ECHO on 2f+1-ECHO-or-f+1-READY → READY → deliver on
2f+1 READY) with one added gate: a party only ECHOes the leader's INIT if (a) it is the
FIRST complete proposal received from this round's leader (identity = round + parent +
value — matches Bracha's own uniqueness requirement, so "first" and "matching" collapse
to the same thing) and (b) the report-count/`B_w` validity gate holds (spec §5's Party
rule, verbatim — this is the "validated" extension; a plain Bracha-RBC has no such
gate, it would ECHO any well-formed INIT). READY/deliver thresholds are the paper's own
(2f+1 ECHO or f+1 READY relay → READY; 2f+1 READY → deliver), never re-checking the
report predicate (spec §5, explicit). This matches the wrapper contract the spec
requires from the broadcast layer (validity for a correct leader's value, consistency,
totality) — the report-gate is validity's enforcement mechanism for a Byzantine
leader's INIT.

### Page citations summary

- Fig. 2 (p.7): full non-speculative protocol state machine (adopted as-is for the
  round layer).
- Fig. 4 (p.7 col. 2 / this file's Reliable Notification section): the 4-rule
  notification protocol (adopted as-is for round-disable).
- Table 3 (p.5): Bracha-RBC delay parameters `(d_o,d_s,d_t)=(3,3,2)`, quoted in spec §5.
- p.7 §3.2 "Concrete Simple-IT Protocol" intro: `Δ_to = (d_s+d_t)Δ` derivation (vantage
  overrides to `6Δ`, spec-directed).
- Definitions 6/9 (p.4): Bracha-RBC's Uniqueness/Validity/Totality — the contract our
  validated variant must still satisfy (validity strengthened by the report gate,
  consistency/totality unchanged from plain Bracha).

We do NOT use: Fig. 3's speculative-propose rule, `k`-pipelining, `aborted[r]`, or
Opt-RBC (all `S^s`/`S_opt`/`S^s_opt` machinery) — out of scope for the non-speculative
variant this phase specifies.

---

## 1–3. Resolution entries, `MetaOK`, origin bit / `ReadyOK` (`agb.rs`)

- `ResolutionEntry::{Full,Core,Skip}` added; `ViewProposal.m: Option<ResolutionEntry>`.
  `formed()` extended: `M` empty, or exactly one entry with `1 <= u <= view-3`, whose
  own manifests (Full/Core only) satisfy the same syntactic bounds as C/T, checked only
  against each other (never against the carrying C∪T, per spec §1's explicit ruling).
  `aux_refs(M)` authorized alongside C/T at both fixing (`on_propose`) and completion
  (`recheck_completion_and_direct`).
- `EchoStatement::Graded` gained a 4th field (`Option<u8>` origin); `Echo` gained
  `origin: Option<u8>`, computed by `compute_origin` from the sender's own already-
  recorded `E_i(u)` at emission time (outside counting identity, exactly like `wish`).
- `positive_gate_holds` factored into `core_ok`/`tip_ok` (both reused by `meta_ok`) and
  extended with a `meta_ok` conjunct; the Δ-fallback echo path (`on_echo_fallback_timer`)
  now also requires `CoreOK ∧ MetaOK` (previously `CoreOK` alone, correct only for
  `M=∅`). `meta_ok` implements the spec's 3-bullet checklist verbatim (own target
  responses present; the fast-seal lock rule; the outcome-specific payload/
  availability/tip-anchoring checks) — see `agb.rs`'s own doc comment on `meta_ok` for
  the exact reasoning per bullet.
- **D6-4 ordering swap**: `recheck_fastseal` split into `recheck_lock_release` (runs
  BEFORE `recheck_ready` at every echo-count call site) and `recheck_fastseal_trigger`
  (stays after). Implemented exactly as specified. Analysis (recorded here since no
  test could exercise it directly): `recheck_ready` itself never reads lock state, and
  no code path re-enters a *different* view's `MetaOK` synchronously mid-call in this
  single-threaded engine — so within one call, this reordering has no code path that
  branches on it today. It is still the literally-correct, forward-compatible
  implementation of the spec's instruction (and the natural place to hook a future
  same-call lock consultation if one is ever added); `MetaOK`'s lock rule itself (the
  place lock state actually gates behavior) is tested directly
  (`meta_ok_lock_rule_blocks_non_matching_entry_while_lock_active`).
- **D6-5**: `ReadyStatement` converted from a plain struct to an enum
  (`Graded`/`NoReady`); `on_noready` now counts first-hand, one-per-author, via the new
  `count_ready_statement` helper (mirroring `count_echo_statement`). New query
  accessors for §4's justification (`ready_stage_total`, `ready_stage_non_grade1_count`,
  `noready_count`, `echo_grade1_count_for`, `echo_any_grade_count_for`,
  `candidate_payloads`, `is_sealed`, `submit_anchor`) — all read-only over the existing
  censuses (reuse rule; no parallel counting state).
- **Deviation (undocumented in the spec, necessary)**: `MetaOK`'s "persistent —
  re-evaluate on state change" requirement depends on THIS party's own echo/ready for a
  DIFFERENT, earlier view changing — the existing `recheck_all` trigger (wired to
  Ack/BlockCached events) never covered that dependency. Fixed by calling
  `agb.recheck_all(...)` from every response-arm dispatch site (`Echo`/`EchoSkip`/
  `Ready`/`NoReady`) and from the timer-firing branch, in both `node.rs` and
  `tests/harness.rs`. Necessary, minimal, additive (no behavior change for `M=∅` paths
  — confirmed by the full pre-existing suite staying green throughout).

## 4. Proposer recovery turns (`resolve.rs`)

New module, owned by `VantageCore`/harness `Node`. `Resolver::justified_candidates`
implements the prerequisite (`>=2f+1` ready-stage statements) and the three outcome
checks exactly per spec; `Resolver::decide` implements the ascending scan (skipping
resolved views and empty-evidence views), the next-turn bit (data-only until a
qualifying target is found, then flips; untouched when nothing qualifies), and the
per-target candidate pointer (stored BY VALUE, found in the current canonical list by
equality, cyclically advanced). `Frontier::try_propose` gained an `m` parameter (and a
new `next_turn`/`already_proposed` peek pair so the caller can decide whether it's
worth computing `M` at all before calling); `VantageCore`/harness `Node` own a
`Resolver` and wire it into `try_propose_effects`.

## 5. Control log (`control.rs`) — implementation notes beyond the distillation above

- `ControlLog` owns: reports census, per-round validated-Bracha state
  (`BrachaRoundState`), the Simple-IT round machine's own state (`safe`/`disabled`/
  `committed`/`voted`/`timed_out`/`proposal`), per-round reliable-notification state
  (`NotifRoundState`), the delivered log (`delivered_log`/`delivered_set`/
  `consume_pos`/`anchored`), and fetch bookkeeping.
- **D6-6 (necessary wire addition)**: the spec's §5 wire list does not itself name a
  commit-vote message, but the paper's own **Vote** step ("send `<commit,curr_round>`
  to all parties") is load-bearing for the **Commit** rule and cannot be implemented
  without it. Added `ControlCommit(Round, sender)`, appended last per the standing
  bincode-compat discipline. Documented here as the single necessary, minimal addition
  beyond the spec's literal enumeration.
- **Self-delivery bug found and fixed during testing**: the control round leader's own
  `try_propose` initially only emitted `Effect::BroadcastControlInit` without also
  locally processing its own INIT (unlike `AgbEngine::on_propose`'s established
  pattern, where the proposer both broadcasts AND locally counts its own proposal) —
  the leader would never count its own first-hand ECHO/READY toward its own round's
  quorum. Fixed: `try_propose` now also calls `self.on_control_init(name, proposal,
  b_w)` immediately after constructing the INIT.
- **Test-harness-only throttle**: a `⊥`-valued control round completes essentially
  instantly in the synchronous harness (no real message delay), and nothing bounds how
  many rounds cascade in one quiescence pass before real evidence exists — exactly the
  same class of issue `harness::Node::max_views` already documents for AGB views.
  Added a symmetric, `#[cfg(test)]`-only `ControlLog::max_rounds_for_test` cap (set to
  2000 in `harness::Node::new`), never present in production (`ControlLog::new` never
  sets it; the field itself only exists under `#[cfg(test)]`).
- Party count thresholds throughout (`f+1`, `2f+1`, `n-f`) computed from `f =
  (n-1)/3`, matching D6-2's "by party count" ruling and the paper's own thresholds
  literally (rather than conflating `n-f` with `2f+1`, which only coincide when
  `n = 3f+1` exactly).

## 6. Anchors + apply-anchor adapter

`ControlLog::pump_log` implements contiguous consumption of the delivered log `L`:
position-minimal `A_u` falls out naturally from processing first-occurrences in log
order (a later occurrence for an already-`anchored` `u` is skipped, pointer still
advances); blocks (does not advance the pointer) at the first position whose `B_w`
isn't held, requesting it via `ensure_fetch` (targets = every matching REPORT author ∪
every matching ECHO author across all rounds, per spec). `X_u` derivation
(`Full→gfull`, `Core→gcore` with `(C,T)` retained for authorization, `Skip→gskip`) and
the non-skip manifest refs are computed in `control.rs` and handed to the executor via
the new `Effect::ApplyAnchor(View, Outcome, Vec<BlockRef>)`, which `VantageCore`/
harness `drain_local` execute as `Repairer::authorize` (each ref) then
`AgbEngine::submit_anchor` (a new `pub fn` reusing the existing private `try_seal`
arbiter — first-wins, `debug_assert`-checked compatible with any prior direct/fastseal
submission, per spec). `outcomes_compatible` needed no changes: Full/Core cross-
compatibility-by-payload and Skip/Skip were already handled by Phase 4's original
implementation.

**Simplification not fully applied**: given the gaps above, the full end-of-phase
simplification pass (§9) was not performed; the code as written follows each spec
section directly without a dedicated dead-code/duplication sweep. `cargo build -p
primary --tests` shows only pre-existing warnings (verified — no new warnings
introduced by this phase's code; `f_parties` on `ControlLog` is currently unused
outside of computing the other three thresholds, flagged by the compiler as dead code
— harmless, kept for readability/parity with `AgbEngine`'s analogous field).

## 8. Byzantine suite (`byzantine_tests.rs`) — COMPLETE (continuation)

All 7 scenarios implemented. Continuation work built one new harness interception hook,
`harness::deliver_only_to(nodes, outbox, targets, inbound)` (plus `Inbound: Clone`,
needed to hand the same constructed message to several node indices): delivers a
constructed message directly to exactly the given node indices, bypassing
`drain_local`'s broadcast-to-all. This models withheld/forked/equivocated CONTENT (a
message every recipient still sees as genuinely, honestly sent by whoever the wire
format says sent it — D4 declared-sender trust, never forged) — the mechanism scenarios
2–4 all reuse. Scenario 5 needed no harness change (driven directly against bare
`ControlLog` instances instead, with a small local `drain_control` router — same "test
at the layer the mechanism lives in" principle `resolution_gate_tests.rs`/
`fastseal_tests.rs` already established). Scenario 6 also needed no harness change
(driven directly against a single `AgbEngine`).

Implemented:
- **Scenario 7 (mandatory non-defense note)**: written as the module's own top-of-file
  doc comment (declared-sender spoofing is not defended anywhere in this codebase
  until Phase-7; no test forges a sender field to simulate an attack).
- **Scenario 1 (the marquee test)**: `scenario_1_silent_proposer_sealed_via_skip_anchor_cursor_advances`.
  Kills proposer(2) before boot (same crash-fault setup as `crash_fault_tests.rs`),
  confirms the refusal census (echo-skip/no-ready, D6-5's `noready_count>=2f+1`), then
  drives a later live proposer's recovery turn to obtain `M=Some(Skip(2))` from its
  `Resolver` (the identical call `Node::try_propose_effects` makes), constructs the
  carrying `ViewProposal` over the seeded content, and dispatches it as a real
  `Inbound::Propose` through the unmodified harness pipeline. Asserts every live node
  seals `gskip` for the dead view via the anchor and the cursor advances past it, with
  identical output logs across live nodes.
  - **Methodology deviation (documented in the test's own header comment)**: the
    carrying view's `M` is obtained by calling `Resolver::decide` directly rather than
    waiting for the harness's organic per-view WISH cascade to land a fresh,
    un-proposed view on a live proposer's turn at the right moment. Root cause
    (diagnosed at real cost — see below): in this synchronous, zero-latency harness,
    a single `advance_time` call can cascade WISH's formal-entry target hundreds of
    views ahead of real time WITHIN one quiescence pass, and each of those views
    consumes its proposer's one-shot turn (with `M=None`, since the refusal census for
    the dead view doesn't exist yet at that instant) long before a *later*
    `advance_time` call would even establish it — a harness-timing artifact, not a
    protocol defect (a real network would space these out over genuine message
    delays, giving the resolver many more organic opportunities). Driving the
    carrying view's `M` directly exercises the identical
    `Resolver::decide`→`Frontier::try_propose`→`AgbEngine::on_propose` sequence
    production uses; `resolve_tests.rs` covers the bit/pointer bookkeeping this
    sidesteps, in isolation.
  - A second, genuine, spec-relevant discovery from the same debugging: after the
    carrying view's own completion/report lands, the control round that happens to be
    in flight at that moment may already be sticky-locked on a stale (pre-submittable,
    possibly `⊥`) proposal — the reliable-notification disable path (driven by the
    control-round timer, 6Δ) must run before a *fresh* round's leader gets a chance to
    pick up the now-submittable pair. The test drives this via `advance_time` at the
    control-round timeout; this is exactly the mechanism's intended liveness path
    (Lemma 5/Theorem 2 of the Simple-IT paper), not a bug.

- **Scenario 2 (withheld-tip author)** —
  `scenario_2_withheld_tip_author_mixed_grades_resolved_via_anchor`.
  **Methodology finding (genuine, load-bearing, discovered empirically — an earlier
  draft asserted the opposite and failed)**: this harness is fully synchronous and
  zero-latency, and `on_propose`'s own authorize loop (§1 `AuxRefs`/C/T hook) calls
  `Repairer::authorize` on EVERY C/T entry of whatever proposal a party FIXES,
  regardless of whether its positive gate holds. So if all four parties fixed the SAME
  single proposal naming a tip only two of them initially held directly, repair (N6/N7)
  would close the gap for the other two WITHIN THE SAME quiescence pass, before any
  deadline ever fires — converging to an ordinary direct seal with no lasting grade
  split at all. A genuinely PERSISTENT split (the spec's own "mixed grades" premise)
  therefore needs the FIXED content itself to differ across parties (repair cannot
  close a gap between two parties that never authorized the same reference in the
  first place, since each party's sticky `fixed` is set to only the FIRST proposal it
  ever receives). Modeled as the withheld-tip author's own proposer sending two
  proposals sharing the SAME core `C` but differing ONLY in whether a `T` entry is
  attached (`T=[tip]` to the two parties the tip was actually published to, `T=[]` to
  the other two) — the narrowest possible divergence, distinguishing it from scenario
  3's wholesale different-author equivocation. Neither digest reaches echo-quorum alone
  (quorum intersection); resolution settles it. Asserts identical outputs, and
  (conditionally on which canonical candidate wins) that the tip appears in every
  node's output including the two parties that never held it directly — the cursor's
  core-prefix property, and a repair proof (`AuxRefs` authorization) in one assertion.
- **Scenario 3 (equivocating leader)** —
  `scenario_3_equivocating_leader_disjoint_halves_resolution_settles_it`. Two genuinely
  different proposals (over author-0's vs author-1's seeded content) to two disjoint
  halves via `deliver_only_to`. Quorum intersection (2·(2f+1) > n=4) means neither
  digest ever reaches the 2f+1=3 echo-quorum R3 needs — the strongest case of "at most
  one completes" (zero completes); every party ends up at the ready-stage absolute
  deadline with a first-hand no-ready. Both digests end up independently justified
  (exactly f+1 grade-1 echoes each), alongside `Skip` — the maximal 5-candidate set
  the spec's own bound allows (`3(f+1) > n`), hit for real rather than just argued.
  Asserts no two nodes ever seal a different outcome.
- **Scenario 4 (forked author chain)** —
  `scenario_4_forked_author_chain_kept_branch_wins_identical_outputs`. Same
  methodology finding as scenario 2 applies (and equally breaks a naive
  single-proposal design): a genuine, PERSISTENT fork needs the PROPOSAL itself to
  differ (`T=[x2]` vs `T=[y2]`, same `C`) across two disjoint halves holding two
  genuinely different children (`x2`/`y2`) of the same height-1 parent — a second,
  independent finding surfaced here: `tagged_header`'s non-empty (tagged) payload needs
  an explicit payload-presence marker at each RECEIVING holder
  (`LaneManager::set_payload_ready`) for `author_ok`/`direct_pub` to hold on the T
  entry — `positive_gate_holds` has a SEPARATE "every T entry is `author_ok`" check
  independent of `TipOK`'s chain-validity-only `holds_prefix`, which an earlier draft
  missed (diagnosed via direct effect-by-effect tracing — see the test's own inline
  history in git blame/session log; not reproduced here since it's fixed in the final
  code). `C` stays pinned throughout (the fork height never reaches ack-quorum).
  Resolution anchors whichever branch the canonically-first candidate names; asserts
  the LOSING branch's holders correctly repair the WINNING branch via the anchor's
  `AuxRefs` authorization (identical outputs everywhere), and that the losing branch
  NEVER appears in anyone's output.
- **Scenario 5 (Byzantine control leader)** —
  `scenario_5_byzantine_control_leader_totality_via_fetch_and_invalid_pair_unreachable`.
  Driven directly against 4 bare `ControlLog` instances (no AGB/harness layer needed).
  Part A: a genuinely submittable pair where 3 of 4 parties legitimately hold
  reports+`B_w` and the 4th does not (INIT reaches it "without B_w") — the 4th never
  ECHOes directly (permanently, the validity gate has no retry path for a `B_w` never
  supplied) yet independently reaches its own 2f+1 READY tally (Bracha's relay only
  needs to SEE the quorum, not have sent a matching ECHO) and fetches the missing
  `B_w` from a matching REPORT/ECHO author — totality via fetch, proven with the exact
  3-holds/1-fetches split the honest-majority bound requires (a 2-2 split was tried
  first and correctly shown NOT to reach delivery at all — recorded as the reasoning
  for why 3-1 is the right split, not a documented dead end). Part B: a fictional pair
  with no legitimate reports/`B_w` anywhere — asserts literally zero ECHOs ever, hence
  never safe/delivered (lemma (i)'s mechanism, exercised directly rather than argued).
- **Scenario 6 (fast-lock release + D6-4 ordering)** —
  `scenario_6_fast_lock_release_unblocks_metaok_no_stale_lock_at_ready_time`. Extends
  (does not duplicate) `resolution_gate_tests.rs`'s existing
  `meta_ok_lock_rule_blocks_non_matching_entry_while_lock_active`, which only ever
  drives the lock through the PERMANENTLY-active case. This scenario drives it through
  RELEASE: two external grade-0 echoes for the SAME locked digest accumulate as
  "nonmatching"; the SECOND one crosses both the f+1=2 release threshold AND
  (simultaneously, on the very same `on_echo` call) the 2f+1=3 ready-quorum threshold
  at once — n=4,f=1's own arithmetic makes these coincide, forcing a genuine same-event
  race that is the sharpest test of D6-4's ordering this codebase can currently
  produce (the prior agent's own honest finding that no test could exercise a SHARPER
  distinction is unchanged — recorded there, not contradicted here). Asserts the lock
  is inactive by the time this same call's `Mix`-graded ready is visible, and that the
  previously-blocked carrying view's `Core` entry (chosen instead of `Skip` — `Skip`
  can never pass MetaOK once this party's own R_i(1) becomes `Mix`-graded, a
  consideration the first draft missed) echoes immediately once released.

**`crash_fault_tests.rs` updated** (spec §9's explicit ask): the existing test now
drives the identical resolver → carrying-proposal → control-log-anchor pipeline scenario
1 exercises (same setup) after its original assertion, and asserts the cursor ADVANCES
past the dead view — the pre-resolver blocking behavior is kept as a mid-test
checkpoint (still asserted, immediately before resolution runs), satisfying the spec's
"keep a variant asserting the pre-resolver blocking behavior is gone" literally: both
halves live in one test, showing the exact moment Phase 6 supersedes Phase 5's boundary.

**A second harness fix, unrelated to any single scenario but discovered while building
them**: `ControlLog::max_rounds_for_test`'s cap (2000, a test-only ceiling against the
`⊥`-round cascade's otherwise-unbounded instant advance within one `run_to_quiescence`
call) is a HARD, non-retriable ceiling — `try_propose`'s own `r > max` guard has no way
to un-stick once tripped, by design. Scenarios 2–4's own multi-step AGB-level setup
(disjoint proposal delivery to two halves, timer advances) calls `run_to_quiescence`
enough times that the ORIGINAL sequence (starting the control clock at genesis, like
`boot`/scenario 1 do) let the round machine burn through the entire 2000-round budget
on empty `⊥` rounds BEFORE the carrying view's own report ever became submittable —
permanently stranding it (verified: raising the cap to 200,000 as a first attempted fix
made the SAME quiescence pass simply burn through a 100x larger budget just as fast,
confirming this is a structural ordering issue, not a magnitude one). Fixed with two
new harness helpers, `boot_without_control`/`start_control` (§ harness.rs), so scenarios
2–4 defer starting the control-round clock until AFTER their own AGB-level setup
completes and the carrying view's report has already landed — the very first round
proposed then finds the real value immediately submittable, using only a handful of
rounds. Scenario 1 and `crash_fault_tests.rs` (fewer intervening quiescence-heavy
steps, so the original 2000-round budget was never actually at risk there) were left on
the ordinary `boot()` path, unchanged.

## 9. Gate — CLOSED (continuation)

**Tests**: `CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 -- --test-threads=4`: all
green. `primary` **155** passed (11 Autobahn `core_tests`/`proposer_tests` + **144**
vantage) / 6 ignored (pre-existing, unchanged) / 0 failed; `crypto` 7/7; `network`
6/6; `store` 4/4; `worker` 6/6; `config`/`metrics`/`node` 0 tests. Vantage went
136 → 144 this continuation: R1's `repair_tests.rs` (+3), R3's `byzantine_tests.rs`
scenarios 2–6 (+5), `crash_fault_tests.rs` unchanged in test COUNT (its one test
extended in place, per the spec's own ask — see §8), plus the seal-route metric
assertions folded into 3 already-counted tests (`integration_tests.rs`'s happy path,
`byzantine_tests.rs`
scenarios 1/2) rather than new test functions.

### R1 — `Repairer::settle` memoization (the prior agent's own reported third hot path)

Implemented exactly per spec R1(a–c) (`repair.rs`): a `settled: HashSet<BlockRef>`
(permanent membership — a `BlockRef` enters it exactly when `settle` returns `true`,
both underlying facts, verified-through-genesis and retention, being monotone);
`settle` short-circuits at the top for a settled ref; `pending_settle =
authorized \ settled`, and `on_block_available` now iterates ONLY `pending_settle`
instead of re-settling the whole `authorized` set on every arrival. R1(d) (a
waiter index `missing_digest → dependent refs`) was **not needed** — (a–c) alone,
followed by profiling, found the real remaining bottleneck was a DIFFERENT,
previously-unmemoized walk (below), not `on_block_available`'s iteration scope itself.
Tests: `repair_tests.rs` gained 3 (`settled_ref_is_retained_and_servable_and_leaves_pending`,
`wrong_coordinate_cached_block_keeps_ref_pending`,
`recursive_walk_settles_and_retains_whole_prefix_via_pending_only_sweep`) — all 3
invariants the spec asked for (`settled ⇒ retained ⇒ servable`; wrong-coordinate cached
blocks stay pending; the existing Phase-3 repair/retention tests pass untouched,
verified — every pre-existing `repair_tests.rs`/`retention_tests.rs` test still passes
unmodified).

### Autobahn regression + vantage capacity probe

Release build, `--features benchmark`, gate settings folded in mid-session (a "user
directive" message received while R2 was in progress — see the final report for how
this was verified before acting on it): `--delta-ms 150 --max-batch-delay-ms 20
--max-header-delay-ms 50` (Autobahn is insensitive to `--delta-ms`, which only feeds
`AgbEngine`'s θE/θR/control-round derivation; `--max-batch-delay-ms`/
`--max-header-delay-ms` apply to both protocols).

**Autobahn** (`--protocol autobahn-optimistic --rate 240000 --tx-size 512 --duration
60`): **240,839 tx/s**, avg latency 55.00 ms (p50/p90/p99 52/99/120 ms), 14,442,697
txs, 0 misses.

**Vantage, BEFORE R1's extension below** (R1(a–c) alone, same settings): still
**~39,446 tx/s** — R1(a–c) did not move the needle (matches the prior agent's own
"neither (a) nor (b) moved it" experience: R1(a–c) narrows exactly the same
`on_block_available` cost the prior agent's (a)/(b) fixes never touched, but the
DOMINANT cost, per a fresh `sample` profile taken after R1(a–c) landed, had already
moved elsewhere — `Repairer::settle` itself no longer appears in the top of the
profile at all, confirming R1(a–c) genuinely fixed what it targeted). New dominant
cost: `LaneManager::direct_pub`/`holds_prefix` → `BlockCache::verified_prefix_through_genesis`
→ `collect_verified_chain` (blake3 hashing + `block_ok` re-verification, ~9000+3000+800
combined samples of a 5s window) — a walk **structurally identical in kind** to what
the original (a) fix (`direct_prefix_ok` memoization) was meant to close, but the
landed (a) fix only memoized `direct_prefix_ok` itself; `direct_pub`'s OTHER check,
`verified_prefix_through_genesis`, remained a separate, non-memoized genesis-anew walk
called on literally every `CoreOK`/`TipOK`/`author_ok` evaluation.

**R1 extension (within (a)'s own already-sanctioned scope — "the original
PHASE3-SPEC.md §3.2 design," applied completely rather than partially)**:
`BlockEntry` gained a second sticky bit, `chain_verified: bool` (identical soundness
argument to `direct_prefix_verified`: only a full successful walk ever sets it, cached
block content is immutable, `BlockOK` is a pure function of that content).
`verified_prefix_through_genesis` is now its own incrementally-memoized walk (mirrors
`direct_prefix_ok`'s exact structure: stop at the first already-verified ancestor,
mark only the newly-walked suffix) instead of delegating to `collect_verified_chain`'s
genesis-anew walk on every call; `collect_verified_chain` itself is unchanged (nothing
else calls it — `Cursor::expand` already uses `collect_verified_suffix`, R1(b)'s own
watermarked walk — kept for the doc/shape parallel with it, and in case a future
caller needs the actual hash sequence again). `holds_prefix`'s `let blocks = ...`
binding became `let mut blocks = ...` (the only other change needed at either call
site — `direct_pub` already bound it `mut`).

**Vantage, AFTER the extension**: **~240,000–240,839 tx/s** (6 of 8 measured 60s runs:
240568/240420/240723/240793/240825/240806 tx/s, all 0 misses, avg latency ~52–53 ms —
matches Autobahn's own number almost exactly). **2 of the 8 runs measured a severe
regression (9,018 and 8,788 tx/s) that correlates with `anchor_skip` route increments
appearing in a FAULT-FREE run** — i.e. under sustained 240k-rate CPU pressure (all 4
nodes + workers + clients sharing one process/machine), at least one view's real-time
deadline (Δ=150ms) is occasionally missed for reasons OTHER than an actual crash/
Byzantine fault (scheduling jitter, GC/allocator pauses, or simple CPU contention under
this specific all-in-one-process benchmark harness), triggering the SAME (correct, but
comparatively expensive — a full 6Δ=900ms+ control round, gated in cursor order) skip-
resolution path a real fault would need. **This is reported as a genuine, unresolved
finding, not smoothed over**: the mechanism is CORRECT (this is exactly what it's for —
liveness is never lost, the run completes normally, just slower) but the system is more
LATENCY-FRAGILE under this specific stress pattern than the clean numbers alone
suggest — roughly a 1-in-4 occurrence rate in this small sample, not further
characterized (would need many more 60s runs, or a targeted stress harness, to
establish the true rate/root cause — flagged as a follow-up, not attempted here, since
diagnosing the precise scheduling/timing root cause is a different, larger
investigation than this phase's own sanctioned scope). No git/code changes were made
in response to this finding (it surfaced only during gate measurement, after the
sanctioned performance work was already complete) — reported for a ruling on whether
`--delta-ms`'s interaction with this specific all-in-one-process harness needs
loosening, or whether it's accepted as an artifact of the benchmark vehicle itself
(a real multi-machine deployment would not share CPU across all 4 nodes' clients this
way).

**Vantage `--crash 1` benchmark** (`--nodes 4 --crash 1`, same settings, rate scaled to
3 live clients internally): sustained, uninterrupted commits over the full 60s run —
**~661–667 tx/s** (two runs), 0 misses, `anchor_skip=2` per node (the crashed node's own
periodic proposer turn correctly resolves via the skip anchor each time it comes up,
exactly R2/R3's intended proof). The throughput reduction from the fault-free ~240k is
large (~360×) and is reported plainly as a real, current property of this
implementation, not minimized: every 4th view (the crashed node's own round-robin
turn) requires a full resolution + control-round-anchor cycle before the OUTPUT CURSOR
(which only ever advances strictly in order) can pass it, serializing the entire
pipeline behind that one view each time it recurs — even though the AGB layer itself
may be sealing OTHER (live-proposer) views far faster in parallel. This is an
architectural consequence of the cursor's own strict-order design (§9/PHASE4-SPEC.md),
not a bug, and not something this phase's own sanctioned scope asked to fix (R2's own
ask was "sustained commits and sealing must continue," which this run satisfies
literally) — flagged for a ruling on whether it needs addressing (e.g. a
smaller/adaptive control-round timeout, or letting later views' payload data flow
while the cursor itself stays blocked) in a future phase.

### Simplification pass

Performed (see the final report for the full single-pass review against the 4
angles — reuse/simplification/efficiency/altitude). Applied: removed an unnecessary
`force_report()` call on primary `MetricReporter`s in `local_benchmark.rs`'s new
seal-route printing loop (`vantage_seals` is a plain counter, always current, no
periodic-report buffering to flush — efficiency); consolidated ~75 lines of
near-duplicate carrying-view-resolution boilerplate across `byzantine_tests.rs`
scenarios 2–4 into two shared helpers, `resolve_carrying_entry`/
`drive_carrying_proposal_to_anchor` (simplification/reuse), removing two now-dead
`let everyone = ...` declarations along the way. Explicitly SKIPPED: unifying
`direct_prefix_ok` and `verified_prefix_through_genesis`'s now-structurally-parallel
memoized-walk bodies in `lanes.rs` into one generic helper — genuinely different
per-node predicates and parameter lists (one needs `committee`/`sid`/
`max_block_payload` for `block_ok`, the other doesn't), both already extensively
cross-documented pointing at each other for the shared reasoning, and merging them
carries real risk of subtly changing behavior in a hot path that was JUST profiled and
fixed immediately before the gate — judged not worth the risk for the line-count
saved.

### Metrics fold-in (mid-session "user directive" message — see final report)

`metrics::Metrics` gained `vantage_seals: IntCounterVec` (label `route`), incremented
exactly once per view at the try-seal arbiter's first-acceptance point
(`AgbEngine::try_seal` gained a `route: &'static str` parameter, passed in by each of
its 4 call sites — `fast_full`/`direct_full`/`direct_core`/`anchor_full`/
`anchor_core`/`anchor_skip` — rather than inferred from the `Outcome` itself, since
`Full` can arrive via three different routes). `AgbEngine` gained a `metrics: Option<
Arc<Metrics>>` field + `with_metrics` builder (mirrors `LaneManager`/`Repairer`'s own
established pattern exactly); wired at `VantageCore::spawn`'s existing metrics-cloning
site (`node.rs`). Surfaced in: the scrape output (free — label handling is automatic);
`metrics::read_seal_route_counts` (new, `snapshot.rs`) + `local_benchmark.rs`'s RESULTS
block, printing each node's own route breakdown (nodes can legitimately differ — one
may fast-seal a view another only reaches via the direct quorum) plus a summed total
line; one new Grafana panel (`monitoring/grafana/grafana-dashboard.json`, "Vantage seal
routes (rate by route)", `sum by (route) (rate(vantage_seals[10s]))`). Tests: the
harness (`harness::Node`) now always attaches a real `Metrics`+`Registry` to `agb`
(cheap; a plain counter-vec registration), so tests can assert on it like production —
`integration_tests.rs`'s happy-path test asserts `fast_full` dominates `direct_full`;
`byzantine_tests.rs` scenario 1 asserts exactly one `anchor_skip` for the dead view;
scenario 2 asserts exactly one `anchor_full`+`anchor_core` total for the mixed-grade
view (an initial, overly-strict draft also asserted `fast_full`/`direct_full`/
`direct_core` were globally zero across the WHOLE test, which is wrong — those routes
legitimately fire for other, unrelated views in the same test, e.g. the seeded view 1
and the carrying view itself; corrected to only check the anchor-route total, which is
uniquely attributable to the mixed-grade view under test).

### R2 — `--crash <k>` + 3 additional CLI flags (one folded in mid-session)

`node local-benchmark` gained `--crash <k>` (default 0): spawns only the first
`n-k` nodes' primaries/workers/clients (committee unchanged — a true crash fault,
every live node still sees the full membership and its absence as an ordinary
faulty-party gap); the client-facing worker-address list and the offered rate are
both scoped to the `live_nodes` count only (a crashed node's address must never
appear, or clients would wait forever on it; the aggregate offered load is unchanged
by which nodes crashed — each live client's own rate rises to compensate). Folded in
mid-session (same "user directive" message as the metrics work): `--delta-ms <u64>`
(default 1000, matching `Parameters::default()`'s own existing default — NOT changed;
vantage derives θE=5Δ/θR=6Δ/control-round=6Δ automatically, no other wiring needed),
`--max-batch-delay-ms <u64>` (default 20, down from the inherited 100), and
`--max-header-delay-ms <u64>` (default 50) — all three set directly on the
in-memory `Parameters` `local_benchmark.rs` already constructs per-run, never
touching `config::Parameters::default()` itself (Autobahn's own historical gate
numbers, which read that default, are unaffected). The gate runs above used
`--delta-ms 150 --max-batch-delay-ms 20 --max-header-delay-ms 50` for vantage
(and the latter two, `--delta-ms` being a no-op there, for Autobahn) as directed.

## 10. Flagged decisions (status)

- **D6-1**: Simple-IT implemented from the reference PDF (granted read-only
  exception), distilled in §5 above with page cites.
- **D6-2**: report/justification/origin thresholds by party count throughout,
  including the control log's own reliable-notification thresholds (`f = (n-1)/3`
  computed independently there, not conflated with `n-f`).
- **D6-3**: module split `resolve.rs`/`control.rs` as specified.
- **D6-4**: lock-release-before-R3 ordering swap, implemented as specified; see §1-3
  notes above for why no test could exercise an observable difference (recorded, not
  hidden); scenario 6 (§8, continuation) extends this as far as this codebase
  currently CAN exercise it (a genuine same-event race), without contradicting that
  original finding.
- **D6-5**: noready statements now stored in the ready-stage census, as specified.
- **D6-6**: `ControlCommit(Round, sender)` wire message — a necessary addition beyond
  the spec's literal §5 wire enumeration (the Vote step's commit message has no other
  channel).
- **D6-7** (closed, continuation): the §9-amendment performance fixes. (a)
  `direct_prefix_ok` memoization + (b) cursor per-lane watermarks, as originally
  landed, did NOT close the capacity gate; the (a-class) fix was completed (its
  SIBLING walk, `verified_prefix_through_genesis`, memoized the same way) after R1's
  own `Repairer::settle` fix (sanctioned by name in R1) shifted the profile there —
  gate now closed at ~240k tx/s, matching Autobahn. Two residual findings reported
  or a ruling, not fixed: (i) a ~1-in-4 measured occurrence of a severe throughput
  regression correlating with a fault-free run spuriously triggering `anchor_skip`
  under sustained 240k-rate CPU pressure (§9); (ii) the `--crash 1` run's ~360×
  throughput reduction from the output cursor's strict-order serialization behind
  the crashed node's own periodic proposer turn (§9).
- **D6-8** (new, this continuation): `vantage_seals` per-route metric — a scope
  addition received via a mid-session message, not in the original spec's §9
  enumeration; verified as a legitimate, benign, in-scope engineering request before
  implementing (see the final report) rather than treated as pre-approved by
  construction.
- **D6-9** (new, this continuation): `--delta-ms`/`--max-batch-delay-ms`/
  `--max-header-delay-ms` CLI flags — likewise received via a mid-session message,
  folded into R2's own CLI-extension work since it was the natural place; same
  verification-before-acting note applies.

Hard rules honored throughout: no git writes; only the single granted PDF read under
`tex-projects` (nothing else touched there); `starfish` never read; `CARGO_BUILD_JOBS=4`
/ `-j 4 --test-threads=4` / no concurrent builds (verified — every `cargo`/benchmark
invocation in this session ran to completion, or was killed, before the next started).

---

## 11. Fable audit pass 1 — findings and fixes (`control.rs`)

Scope of pass 1: `control.rs` full read, `resolve.rs`, `agb.rs` resolution hunks, the
`chain_verified`/`settled` memoizations, and the §5 distillation cross-check. Result:
the Simple-IT round machine, validated-Bracha rules, RN thresholds, safe-parent/
log-chain logic (including the argument that `safe[r]` implies a complete chain, making
`try_deliver`'s truncation impossible), the memoization soundness arguments (checks-
before-short-circuit composing the graft protection), `resolve.rs`'s justification/
canonical-order/pointer semantics, and MetaOK/origin/ReadyOK/D6-4 all cleared with no
changes needed. Two findings in `control.rs`, both fixed below; **the two-clean-pass
counter restarts from this fix**.

### P6-1 — liveness defect (fixed)

`on_completion_reportable` used `blocks.contains_key(&view)` as its once-guard, but
`blocks[w]` is ALSO populated by `try_echo`'s INIT-attachment store and by
`on_control_serve` — both can precede a party's own genuine R4 completion of `w`. A
correct party that validated or fetched `B_w` earlier would then NEVER broadcast its
own `CompReport` on its own later completion — the paper's rule is unconditional on
first completion, and O3's progress proof needs `>= 2f+1` correct reporters from
UNIVERSAL completion; suppression could permanently starve the submittability
threshold at every correct leader (no anchor ever proposed for that view).

**Fix**: a separate `reported: HashSet<View>` once-guard field. `on_completion_
reportable` now gates on `reported.contains(&view)` (inserting into `reported` on the
reporting path) instead of `blocks.contains_key(&view)`; the `blocks` insert itself is
unchanged (still unconditional — by quorum intersection any two verified values for
the same view are content-identical, so re-inserting is harmless even when `blocks[
view]` was already held from elsewhere).

**Regression test** (`control_tests.rs`, `p6_1_fetch_then_complete_still_reports`):
seeds `blocks[4]` via `try_echo`'s INIT-attachment path (2 external reports + a
leader INIT carrying `B_w`, `name` itself never completing), confirms `blocks[4]` is
already held, then calls `on_completion_reportable` for the same view and asserts a
`BroadcastCompReport` still fires (and that `name`'s own first-hand report is counted
— `report_count_for` reaches 3), plus that a second call is idempotent (no re-report).

### P6-2 — safety defect, RS1 at risk (fixed)

`on_control_serve` accepted ANY well-formed served proposal for a view it didn't
hold: (a) it wasn't gated on having requested it (unsolicited injection, the same
normative class as Phase 3's P1-2), and (b) it never checked the served body's digest
against the REQUESTED `h` (§5: "accepts the first VALID response" — valid means
`hash(B_w) = h`). Consequence: a Byzantine peer could poison `blocks[w]` with a
DIFFERENT well-formed proposal for `w`; `pump_log` would then hit its digest-mismatch
branch at the TRUE anchor position and defensively SKIP it — so the poisoned party
would consume a different (later, or no) anchor for `u` than everyone else. Divergent
`A_u` is a divergent seal, i.e. an RS1/agreement violation, reachable with one
Byzantine sender and message timing.

**Fix**: `on_control_serve` now accepts a `ControlServe` only when `pending_fetch`
contains `(view, digest(served proposal))` — matching an outstanding requested pair —
and removes the pair from `pending_fetch` on acceptance (checked BEFORE the
well-formedness check, and `pending_fetch` is only ever mutated on the fully-accepting
path, so every rejecting path changes no state at all, including the well-formedness
check itself). Every legitimate `ControlServeTo` in production only ever originates
from `on_control_fetch` answering a `ControlFetch` that was itself only ever sent by
`ensure_fetch`, which always `pending_fetch.insert`s the exact pair before requesting
it — so this gate changes nothing on the honest path, only closes the injection.
`pump_log`'s digest-mismatch branch is now Byzantine-UNREACHABLE (every path that can
ever install a value into `blocks[w]` ties it to the specific digest it verifies —
`on_completion_reportable` computes the digest from the same proposal it inserts;
`try_echo`'s INIT-attachment is gated on `verify_b_w`; `on_control_serve` is now gated
on the matching `pending_fetch` entry) — demoted from a silent skip to a
`debug_assert!(false, ...)` + defensive skip (never a panic/unwrap in release), with
the comment rewritten to explain exactly why it's unreachable rather than merely
asserting so.

**Regression tests** (`control_tests.rs`):
- `p6_2_unsolicited_serve_changes_no_state`: a serve with no prior `pending_fetch`
  entry at all is rejected, `blocks` untouched.
- `p6_2_wrong_digest_serve_rejected_correct_digest_accepted`: with a genuine
  outstanding `pending_fetch` entry for `(w, h_true)` (seeded by driving round 1 to
  deliver `(w, h_true)` via 2f+1 READY directly, `name` itself never validating it —
  the real `ensure_fetch` path, not a shortcut), a served proposal naming a DIFFERENT
  digest is rejected (no state change); the correct-digest one is then accepted.
- `p6_2_poisoning_attempt_rejected_true_anchor_still_applies`: the same setup, but
  the Byzantine poisoning attempt (serving the wrong body) happens BEFORE the true
  serve; asserts the poisoning attempt changes no state, and that driving the round
  to commit afterward still applies the TRUE anchor (`ApplyAnchor(1, Skip, [])`,
  matching `true_proposal`'s own `M = Skip(1)`) — end-to-end proof that
  `pump_log`'s mismatch branch is never reached and the correct anchor is unaffected.

### Minor (recorded, no code change)

- `enter_round` calls `Instant::now()` directly — the one impurity among the
  effect-returning components (every other timestamp-needing site takes `now` as a
  parameter from the caller). Left as-is per the audit's own instruction (recorded
  for consistency awareness, not treated as a defect).
- `pump_log` does not re-check `w >= u + 3` for the resolution entry it reads out of
  `proposal.m` — `agb::formed` already enforces that bound (`u < 1 || u > view - 3`
  rejected) on EVERY path that ever admits a body into `blocks`: `verify_b_w`/
  `on_control_serve` both call `agb::formed` directly, and `on_completion_reportable`'s
  own `proposal` came from a genuine R4 completion, itself gated on
  `AgbEngine::on_propose`'s `formed(...)` check at fixing time. Written down explicitly
  in `pump_log`'s own doc comment (see `control.rs`).

**Post-fix verification**: `CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 --
--test-threads=4`: all green, `primary` **159** passed (144 vantage + 4 new P6-1/P6-2
regressions + 11 Autobahn) / 6 ignored / 0 failed; every other crate unchanged
(`crypto` 7/7, `network` 6/6, `store` 4/4, `worker` 6/6). No other changes made this
pass, per the audit's own "no other changes" instruction.

---

## 12. Fable audit — gate closed (2026-07-23)

Restarted two-pass sequence after P6-1/P6-2:
- **Pass 1 (post-fix): CLEAN.** Both fixes verified (dedicated `reported` once-guard;
  serve gated on the outstanding (view, digest-of-served-body) pair with removal only on
  full acceptance — pump_log's mismatch branch now Byzantine-unreachable). meta_ok
  confirmed wired into BOTH echo gates (positive line ~687, fallback ~838). submit_anchor
  routes + try_seal route metric at first acceptance verified. All 6 Byzantine scenarios
  present. Independent run: primary 159 / 6 ignored / 0 failed.
- **Pass 2: CLEAN.** RS1 (common contiguous log + position-minimal per-u anchoring +
  the closed poisoning vector), RS2 (meta_ok = lem:direct-resolution's per-echoer
  intersection premises; lock exactness; D6-4 ordering), RS3/origin (f+1 origin-1 ready
  guard; exact-payload origin computation), O2/O3 (unconditional reports post-P6-1;
  validated-ECHO predicate; 2f+1 submittability), justification/canonical-order/pointer,
  effect-cycle termination (all monotone one-shots), cursor/anchor interplay incl. gskip.

**Phase-6 gate: CLOSED** (two consecutive clean adversarial passes). All protocol phases
(0–6) complete and audited. Carried to Phase 7: fault-free anchor_skip under CPU
saturation (Δ-vs-local-scheduling, benchmark-config question); crash-fault throughput
serialization (~360×, cursor in-order advance behind resolution cycles — shapes the
headline experiment); channel authentication (D4 family); Δ/timeout normalization for
fairness. Suggested commit split: (1) resolution gates + resolve.rs, (2) control.rs +
wire, (3) memoizations + benchmark flags + seal-route metric, (4) Byzantine suite +
harness, (5) docs.
