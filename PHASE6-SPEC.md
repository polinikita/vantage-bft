# PHASE6-SPEC — Resolution, sparse control log, Byzantine suite

**Status: FINAL (work order). Precondition: Phase-5 gate closed (it is — PHASE5-NOTES.md).**
Self-contained except one explicitly granted reference (§5). Builds on
`primary/src/vantage/*`; reuse rule applies. Protocol-critical: two consecutive clean
adversarial audit passes (Fable). This phase restores **output liveness under faults**:
the views Phase 5's cursor correctly blocks on get sealed here.

## 0. Scope and non-goals

In: resolution entries in proposal metadata (M ≠ ∅) with `MetaOK`/origin-bit/ready-guard
hooks (the Phase-4 `AuxOK`/`Ann`/`ReadyOK` trivial forms become real); proposer recovery
turns (alternation bit + per-target candidate pointer); completion reports; the
validated-Bracha + non-speculative Simple-IT control log; first-applicable-anchor
consumption and the `apply-anchor` adapter; the Byzantine fault-injection suite.
Non-goals: channel authentication (Phase 7 — the suite must state explicitly that
declared-sender spoofing is NOT defended yet, per the standing D4 rulings); checkpoint
GC (out of model); WAN/perf evaluation (Phase 7).

## 1. Resolution entries (extends §2/§3 of Phase 4)

`ViewProposal` gains `m: Option<ResolutionEntry>` where

```rust
pub enum ResolutionEntry {
    Full(View /* u */, Manifest /* C_u */, Manifest /* T_u */),
    Core(View, Manifest, Manifest),   // payload retains T for identity/compat checks
    Skip(View),
}
```

`Formed_v` extension: M empty, or exactly one entry targeting `u ≤ w − 3` whose
manifests (if any) satisfy the same syntactic bounds as C/T (≤1 entry/author, sorted,
distinct hashes — also globally distinct from the carrying C ∪ T? No: the paper bounds
only the entry's own manifests syntactically; cross-checks are semantic, not Formed).
`m` is inside `ViewProposal`, hence inside `proposal_digest` — echo/ready statements
automatically agree on the entry. `AuxRefs(M)` = the non-skip entry's manifests: on
fixing a proposal AND on completion, authorize those references too (extend the two
existing authorize loops).

## 2. MetaOK — the real `AuxOK` (gates R2's positive AND grade-0 fallback echoes)

`MetaOK_i(w, M)`, evaluated at echo decision time; ∅ → true. For one entry targeting u:
1. Both own target responses `E_i(u)` (echo-stage) and `R_i(u)` (ready-stage) are
   already emitted (either still pending → false for this attempt; persistent —
   re-evaluate on state change like the rest of the gate).
2. **Lock rule**: if `fastLock_u = (C,T)` exists and is NOT released, only the exact
   entry `Full(u, C, T)` can pass; every core/skip/different-payload entry fails.
3. Outcome-specific:
   - `Full(u, C_u, T_u)`: own `R_i(u)` is neither a grade-0 proposal-ready nor a
     proposal-ready naming a payload ≠ (C_u,T_u) (noready, mix, or grade-1 same-payload
     are fine); every entry of C_u and T_u is **locally available**
     (`LaneManager::locally_available`); the R2 tip-anchoring walk holds for
     (C_u, T_u) (paired tips held, strictly containing the C entry — reuse
     `positive_gate_holds`'s anchoring section against the ENTRY's manifests).
   - `Core(u, C_u, T_u)`: own `R_i(u)` is neither a grade-1 proposal-ready nor a
     proposal-ready for a different payload; every C_u entry locally available.
   - `Skip(u)`: own `R_i(u)` is noready.

Wire the predicate into BOTH echo paths: the positive gate (grade-1) now requires
`CoreOK ∧ TipOK ∧ MetaOK`; the Δ-fallback grade-0 echo requires `CoreOK ∧ MetaOK`
(Phase 4's fallback checked CoreOK only — correct for M=∅, must change now); failing
entries at the fallback deadline → echo-skip, exactly the existing else-branch.

**Ordering change (D6-4)**: the fast-seal lock-release check must run BEFORE R3's
ready recheck on the same newly counted echo-stage response (paper's coherence
convention: never emit a grade-0/different-payload ready while a contradictory lock is
active). Phase 4 orders `recheck_ready` before `recheck_fastseal` at the count sites —
swap: release evaluation first (split `recheck_fastseal` if needed: release part before
`recheck_ready`, all-n fastseal trigger part can stay after).

## 3. Origin bit (`Ann`) + ready guard (`ReadyOK`)

`Echo` gains `origin: Option<u8>` (0/1; `None` for skip entries or empty M — Phase 4
reserved this field conceptually). Set at emission, immutable, OUTSIDE counting
identity (like the wish field):
- entry `Full(u, P)`: origin = 1 iff this party's own `E_i(u)` was a grade-1 echo for
  exactly payload P; else 0.
- entry `Core(u, P)`: origin = 1 iff own `E_i(u)` was a proposal echo (grade 0 or 1)
  for exactly P; else 0.

`ReadyOK(M, ℰ)` in R3 (`recheck_ready`): for a full/core entry, require ≥ **f+1
(party count)** counted proposal echoes *for the carrying proposal* with origin = 1;
skip/empty always passes. Monotone; evaluated over the tally at emission time.

## 4. Proposer recovery turns (extends R1 / `Frontier::try_propose`)

Persistent proposer state (new `vantage/resolve.rs`, owned by `VantageCore`, consulted
by `build_manifests`' caller): a **next-turn bit** (initially data-only) and a
**per-target candidate pointer** (names a candidate, never a list index).

At our proposer turn for view w:
1. Compute justified candidates for each unsealed, un-anchor-resolved view u ≤ w−3,
   scanning ascending, skipping views with empty justified sets (an older no-evidence
   view never blocks a later target). Prerequisite for ANY candidate: ≥ 2f+1 counted
   ready-stage statements for u (any kind, noready included). Justification (all from
   the engine's per-view first-hand censuses; **party counts** — D6-2):
   - `Full(P)`: ≥ f+1 counted grade-1 echoes for view-u proposals with payload P.
   - `Core(P)`: some 2f+1-subset of counted ready-stage statements for u containing NO
     grade-1 proposal-ready (i.e. #(counted ready-stage statements that are not
     grade-1 readies) ≥ 2f+1), plus ≥ f+1 counted echoes (any grade) for payload P.
   - `Skip`: ≥ 2f+1 counted noready responses for u. (Phase 5 must have recorded
     received `NoReady`s — Phase 4/5's `on_noready` is a no-op; NOW it must count
     first-hand noready statements into the ready-stage census one-per-author. They
     already occupy the one-shot slot conceptually; store them.)
   At most 2 payloads can be justified (3(f+1) > n); candidate set ⊆ 5 elements.
2. If no target qualifies → data-only proposal (M = None), bit unchanged.
3. Else: the bit selects data-only vs recovery, then flips. A recovery proposal targets
   the FIRST qualifying view and carries the target's pointer candidate; the pointer
   initializes to the first candidate in canonical order (payloads sorted
   lexicographically by `bincode(C, T)`; Full before Core per payload; Skip last) and
   advances cyclically over the CURRENT canonical justified list after every recovery
   attempt for that target.

## 5. Completion reports + validated control broadcast + Simple-IT

**Reports.** On the FIRST genuine R4 `complete(w) → B_w` with `M_w ≠ ∅` (fast seal
alone creates NO report): broadcast `CompReport(w, proposal_digest(B_w))` and retain +
serve `B_w` indefinitely (store it; a fetch-server answers below). Count only the first
report per author per view, first-hand; never relayed.

**Submitted pair.** `(w, h)` becomes submittable at a party once it has ≥ 2f+1 matching
reports (party count) AND holds the verified `B_w` (digest matches, well-formed,
`M_w ≠ ∅`).

**Control log = non-speculative Simple-IT over validated Bracha RBC.** Rounds and
round-robin control leaders independent of data views; control-round timer = **6Δ**
(> (d_s + d_t)Δ = 5Δ). **Granted reference exception (D6-1): you may READ
`/Users/nikitapolianskii/code/tex-projects/signature-free/papers/simple-it-yu-2026.pdf`
(read-only, this one file, nothing else under tex-projects) for the Simple-IT round /
safe-parent / reliable-disable / timeout rules — implement the non-speculative variant,
and distill the exact rules you implemented into PHASE6-NOTES.md with page cites so the
audit can check code against your distillation.** The wrapper contract Simple-IT needs
from the broadcast layer: validity for a correct leader's value, consistency, totality,
delays (d_s, d_t) = (3, 2).

Validated Bracha per control proposal (identity covers round + parent + block x):
- Leader: choose highest safe round r_p (every intervening round reliably disabled);
  propose the smallest-view submitted pair not already delivered or in `Log(r_p)`
  (its safe parent chain), else ⊥. INIT carries the full control proposal AND attaches
  `B_w` as validation data (nonempty only).
- Party: store the FIRST complete proposal received from the control leader; ECHO only
  that one, and only after counting ≥ f+1 matching reports AND verifying the attached
  B_w (view w, well-formed, digest = h, M ≠ ∅); ⊥ passes immediately. READY on 2f+1
  matching ECHOs or f+1 matching READYs; deliver on 2f+1 matching READYs. READY relay
  and delivery do NOT re-check the report predicate. ECHO/READY carry the short
  proposal only (never B_w).
- Missing validation data at delivery time: request `B_w` once from every matching
  REPORT and ECHO author; accept the first valid response; a holder answers at most
  once per requester–(w,h) pair; do not re-impose the f+1-report predicate after
  delivery; process the anchor before advancing to the next log position.

**Wire (appended after `VantageWish`, in order):** `CompReport(View, Digest, sender)`,
`ControlInit(ControlProposal, Option<ViewProposal> /* B_w */)`,
`ControlEcho(ControlProposal, sender)`, `ControlReady(ControlProposal, sender)`,
`ControlFetch(View, Digest, sender)`, `ControlServe(View, ViewProposal)` — plus
whatever Simple-IT round-timeout notification message the reference requires (append
last; document shape in notes). All unsigned, declared sender (D4 class).

## 6. Anchors and the apply-anchor adapter

`L` = the delivered nonempty control values in log order, deduplicating a repeated
(w,h) after first occurrence. Consume only the contiguous prefix; obtain B_w before
processing ("observe"). **A_u** = the anchor at the smallest LOG POSITION whose
carrying-proposal view is ≥ u+3 and whose entry resolves u (position-minimal, so later
log growth never changes it). On observing A_u for a view u not yet sealed locally:
derive X_u from the entry (`Full → gfull(C,T)`, `Core → gcore(C)` with backing payload
(C,T) retained, `Skip → gskip`), authorize the non-skip manifests, and submit X_u to
the existing try-seal arbiter (`Outcome::Skip` finally becomes reachable — the cursor's
gskip arm goes live). Later anchors targeting u are ignored. Compatibility with
fast/direct submissions needs no side conditions (paper lemmas); the arbiter's
`outcomes_compatible` debug_assert must accept `Full/Core-vs-anchor` agreement by
payload (extend it for Skip-vs-Skip; a genuinely incompatible pair remains
Byzantine-unreachable and assert-worthy).

Cursor: a non-skip anchor observed BEFORE local completion supplies the manifests for
the same emit procedure (feed `Completed`-equivalent input from the anchor); gskip →
emit nothing, advance.

## 7. Module plan

- `vantage/resolve.rs` — MetaOK, origin computation, justification/candidate pointer,
  alternation bit (state persisted in-memory; crash-restart out of model).
- `vantage/control.rs` — reports census, validated RBC state machine, Simple-IT rounds
  (`ControlRound` timer via the existing timer queue), log assembly, contiguous
  consumption, A_u derivation, fetch server/client. Effect-returning like everything.
- `agb.rs` — Formed/AuxRefs extension, MetaOK in both echo gates, origin stamping,
  ReadyOK in recheck_ready, noready counting into the ready census, D6-4 ordering
  swap, completion hook → `Effect::CompletionReportable(View, ViewProposal)` when
  M ≠ ∅.
- `frontier.rs`/`node.rs` — recovery-turn construction plumbing; new Inbound/Effect
  variants; control timer arm.

## 8. Byzantine suite (in-proc harness; extend Phase 5's `harness.rs` with per-node
message interception/drop/forge hooks)

Each test names the defense it exercises AND the trust boundary it does not:
1. **Silent/withheld proposer (the marquee test)**: proposer(u) never proposes →
   refusals at deadlines → a later correct proposer's recovery turn carries Skip(u) →
   control log anchors it → **every node seals gskip and the cursor advances past u**
   (this un-blocks Phase 5's crash-fault scenario end-to-end; assert output resumes
   and logs stay identical).
2. **Withheld-tip author**: author publishes chain to some parties only → mixed grades
   (some grade-1, some grade-0 echoes) → completion gopen → recovery carries
   Core/Full per justification → anchor seals; assert core-prefix property and
   identical outputs.
3. **Equivocating leader**: proposer(u) sends different proposals to disjoint halves →
   at most one completes (quorum intersection); resolution settles the rest; assert
   no two nodes seal different outcomes.
4. **Forked author chain across views**: Byzantine author forks its lane; canonical
   expansion + first-occurrence dedup keeps outputs identical; C-pinning (Phase-3 N5)
   keeps T on the kept branch.
5. **Byzantine control leader**: INIT without/with-corrupt B_w → honest parties don't
   ECHO; totality via fetch when only a subset initially validated; a delivered anchor
   for an invalid pair is impossible (assert lemma (i)'s mechanism: no 2f+1 ECHOs).
6. **Forced mixed grades + fast-lock interaction**: lock active at some parties →
   MetaOK rejects non-matching entries until release; assert a grade-0 ready is never
   emitted while a contradictory active lock exists (D6-4 ordering).
7. **Explicit non-defense note in the suite's mod.rs doc**: declared-sender spoofing
   (publish provenance, ack/echo/ready/wish/report sender fields) is NOT defended
   until Phase-7 channel auth — tests must not forge senders except where explicitly
   modeling that documented gap.

## 9. Gate

Full workspace tests green (only the 6 documented Autobahn ignores); Byzantine suite
green; the Phase-5 crash-fault test UPDATED: cursor now advances past the dead view via
the skip anchor (output liveness restored — keep a variant asserting the pre-resolver
blocking behavior is gone); Autobahn regression benchmark; **vantage capacity probe
(user directive, 2026-07-23): the prior 48–49k tx/s numbers were OFFERED-rate artifacts
(--rate 50000), not a measured ceiling — run the vantage fault-free 4-node
local-benchmark at --rate 240000. Target: sustained ≥ ~240k tx/s, parity with Autobahn
(nothing in the design caps below it — payload bytes flow through the same worker
layer). If it does not sustain, profile and fix the known quadratic hot paths, all
performance-only and deterministic-equivalent: (a) incremental `direct_prefix_ok`
memoization on `BlockEntry` (the original PHASE3-SPEC §3.2 design) replacing the
per-event O(height) walks in `refresh_author`/`recompute_registers`; (b) per-lane
emitted-height watermarks in the cursor replacing genesis-anew `collect_verified_chain`
walks at each seal; (c) if still short, report the profile and stop for a ruling before
any structural change (e.g. splitting VantageCore). Re-run the full test suite after
each optimization — semantics must be unchanged, and my audit passes will treat these
as first-class review targets. Report rate-sustained numbers at 240k for both
protocols side by side**; control log on the happy path is near-idle — data-only
proposals produce no reports, so overhead should be ≈ 0 there; one vantage 4-node local-benchmark run WITH one silent node (n=4 tolerates
f=1): sustained commits and sealing must continue (rate scaled to 3 clients);
simplification pass; PHASE6-NOTES.md (deviations + Simple-IT distillation + coverage
map); then two consecutive clean Fable audit passes.

## 10. Flagged decisions

- **D6-1** Simple-IT implemented from the reference PDF (granted read-only exception),
  distilled in notes for audit.
- **D6-2** report/justification/origin thresholds by party count.
- **D6-3** module split resolve.rs/control.rs as above.
- **D6-4** lock-release-before-R3 ordering swap at echo-count sites.
- **D6-5** noready statements now stored in the ready-stage census (they always
  occupied the one-shot slot; Phase 4 discarded the content).

Hard rules unchanged: no git writes; tex-projects untouchable EXCEPT the single
granted read-only PDF above; starfish read-only; CARGO_BUILD_JOBS=4 /
`-j 4 --test-threads=4` / no concurrent builds; deviations → PHASE6-NOTES.md; STOP and
ask on anything uncovered or Autobahn-semantic.
