# PHASE5-SPEC — WISH pacemaker, formal entry, degraded paths

**Status: FINAL (work order). Precondition: Phase-4 gate closed (it is — PHASE4-NOTES.md
§13).** Self-contained (source: paper sec:pacemaker; do not open tex-projects). Builds
on Phase 4's `vantage/{agb,frontier,cursor,node}.rs` — extend in place, reuse rule
applies. Protocol-critical: two consecutive clean adversarial audit passes (Fable).

## 0. Scope and non-goals

In: the WISH view-synchronization service (Direct AGB's `enter` implementation),
formal entry for all views, the two Phase-4 carry-overs (enter-arms-EchoFallback;
frontier formal-entry floor), response piggybacking, degraded paths live in production
(θE/θR now fire for every entered view), crash-fault and convergence tests.
Non-goals: resolution/`gskip` and **output liveness under faults** — a crashed
proposer's view stays `gopen` and the cursor correctly blocks there until Phase 6's
resolver closes it (spell this out in tests: entry/wish liveness continues past dead
proposers, output does not — that is protocol-correct at this phase); Byzantine wish
attacks beyond unit sanity (Phase 6); channel auth (Phase 7).

## 1. WISH rules (normative; tests cite W1–W6)

**W1 — state + genesis.** Per author j: `ω[j]` = largest wish received first-hand from
j (including piggybacked). Own-wish high-watermark and largest-entered-view, initially
0; all `ω[j] = 0`. Genesis: enter view 1 (existing boot behavior), then set
`ω[i][i] = 2`, own watermark 2, broadcast `wish(2)` (self-delivery immediate).
Responses for views ≤ 0 are fixed genesis responses treated as received from every
party (already the Phase-4 convention; nothing on the wire).

**W2 — receipt, amplification, entry (strict order).** On first-hand `wish(x)` from
`p_j`: `ω[j] = max(ω[j], x)`; recompute the order statistics `ω⁺` = (f+1)-st largest
and `ω^Q` = (2f+1)-st largest entry of the ω array (party-count statistics over the n
per-author values — D5-1). Then, FIRST: if `ω⁺` > own watermark, broadcast `wish(ω⁺)`,
update `ω[i]` + own watermark, and recompute both statistics. THEN: if `ω^Q` increased,
raise the entry target to `ω^Q` and **record formal entry to every missing view through
the target, immediately and in increasing order**. Stale wishes cause no transition.
A wish for x supports every view ≤ x; the two updates are independent (target
advancement never waits for `ω^Q = ω⁺`).

**W3 — two-response wish trigger** (Starfish-style two-response progress): *before*
emitting whichever of `E_i(v−1)` (echo-stage response) and `R_i(v−2)` (ready-stage
response) completes that pair, raise own wish to at least `v+1`. Mechanically: when
about to emit the echo-stage response for view u and the ready-stage response for u−1
is already sent → raise wish to ≥ u+2 first; when about to emit the ready-stage
response for view u and the echo-stage response for u+1 is already sent → raise wish to
≥ u+3 first. "Raise" = update `ω[i]` + watermark (+ recompute statistics, which may
trigger W2's entry step); the raised watermark rides out on the very response being
emitted (W4).

**W4 — piggyback, outside identity.** Every response message (`VantageEcho`,
`VantageEchoSkip`, `VantageReady`, `VantageNoReady`) carries the sender's current
own-wish watermark as an extra field **outside the immutable response identity**: it is
excluded from `proposal_digest`-based counting identity, does not affect the
one-statement-per-author rule, and two copies of a response differing only in the wish
field are the same statement (first received wins; its wish is still absorbed via W2).
Amplification (W2) may send a standalone `VantageWish`. A wish only schedules views —
it is never an echo, ready, ack, origin bit, or resolution justification, and entry/
frontier/wish state is never quorum evidence.

**W5 — entry semantics.** Entry is strictly increasing locally (record each view once,
in order). Recording entry to v: (a) arms both fallback schedulers (θE at e+θE, θR at
e+θR); (b) **Phase-4 carry-over fix** — if a proposal is already fixed for v and the
echo is pending, also arm `EchoFallback` at `min(max(e_i(v), ρ_i(v)) + Δ, e_i(v) +
θE)` (PHASE4-NOTES §12's recorded gap; regression test mandatory); (c) raises the
responsive frontier `a_i` to at least `v−1` (the formal-entry floor — `Frontier`
currently only activates on `enter`; it must now also floor `a_i`, which can newly
enable R1 `try_propose` and re-run the contiguous-prefix advance from the new floor);
(d) activates v (existing behavior). Entry never waits for an older response window —
pending older responses close at their own deadlines.

**W6 — retention.** Future-view messages are retained (already true: per-view maps
create state on receipt). The §6.3 unbounded-Byzantine-spam exposure now includes the
ω array (bounded: one slot per author) and per-view AGB state for far-future views
(unbounded, documented — Phase 6 concern, same class as before).

## 2. Wire

`VantageWish(View, /* sender */ PublicKey)` appended after `VantageNoReady` (sender
declared, D4 trust — same as every vantage statement). `Echo`/`Ready` structs gain
`wish: View`; `VantageEchoSkip`/`VantageNoReady` variants gain a `View` wish field
(D5-2). Encoding changes are fine (whole-cluster rebuild; no cross-version interop).

## 3. Implementation map

- New `vantage/pacemaker.rs` — `Pacemaker`, effect-returning like everything else:
  owns ω array + watermarks; `on_wish(sender, x) -> Vec<Effect>` implementing W2
  (returns `BroadcastWish(View)` and `Enter(View)` effects in order);
  `raise_own_wish(x)` for W3/genesis. New `Effect::BroadcastWish(View)` and
  `Effect::Enter(View)`.
- `AgbEngine`: `enter(v, ...)` gains the W5(b) EchoFallback arming; a
  `two_response_wish_target(view_of_emission, stage) -> Option<View>` hook (or
  equivalent) consulted at every response-emission site BEFORE pushing the broadcast
  effect, feeding `Pacemaker::raise_own_wish`; response-emission effects carry the
  current watermark for piggybacking (simplest: `VantageCore` stamps the watermark
  into the outgoing message at serialization time — the engine stays watermark-free;
  D5-3, flag if you deviate).
- `Frontier::enter(v)`: floor `a_i` to `max(a_i, v−1)` + re-run the contiguous advance
  + report newly activated views (existing return shape).
- `VantageCore`: route `VantageWish` → pacemaker; absorb piggybacked wish fields from
  every inbound response BEFORE handing the response to the engine (W2 ordering:
  amplification, then entry, then the response's own processing is fine — wish
  processing is independent of statement counting); execute `Enter(v)` effects as
  `agb.enter(v, ...)` + `frontier.enter(v)` in increasing view order.
- No new timers: WISH is purely event-driven; the existing θE/θR timers now arm for
  every entered view (they already exist — Phase 4 armed them only for view 1).

## 4. Tests (cite W-rules) and gate

- W1: genesis wish(2) broadcast + self-delivery; ω initialization.
- W2: ω⁺/ω^Q order-statistics boundaries (exactly f+1 / 2f+1 senders); amplification
  fires before entry processing and updates own slot; stale wish no-op; entry recorded
  for every missing view through target, in order, strictly increasing; a wish for x
  supports all views ≤ x (entry to v from a quorum of wishes for x > v).
- W3: both trigger arithmetic cases (echo completes pair → wish ≥ u+2; ready completes
  pair → wish ≥ u+3); wish raised BEFORE the response effect is emitted (order within
  the returned effect vec / watermark visible in the piggyback).
- W4: piggybacked wish outside identity — two same-sender responses differing only in
  wish: statement counted once, wish still absorbed; piggybacked wish drives entry with
  zero standalone wish messages.
- W5: entry arms θE/θR; **the carry-over regression** — enter(v) with an
  already-fixed pending proposal arms EchoFallback and the grade-0/skip fallback then
  fires at the right deadline; frontier floor raise enables R1 for v (proposer of v
  proposes after entering v without having seen v−1's proposal); entry never re-enters.
- Crash-fault integration (in-proc, 4 engines, injected clocks): kill proposer(v) —
  correct parties enter v via wishes, echo-skip at θE, noready at θR, **enter v+1 and
  beyond** (lemma (a) inductive step observable), later views with live proposers
  complete and seal normally; the cursor correctly BLOCKS at the dead view (assert no
  output past it — that is the documented Phase-6 boundary).
- Convergence: delay one party's inbound wishes (partition), release — it enters all
  missed views in order and rejoins (entries converge; the 2δ bound is the lemma's,
  asserted qualitatively: entry happens within the test's release window).
- Autobahn regression (tests + one optimistic local-benchmark run); vantage fault-free
  4-node local-benchmark run reproduces Phase-4 numbers (~48k tx/s; wish piggybacking
  must not measurably regress it).
- Simplification pass → PHASE5-NOTES.md → two consecutive clean Fable audit passes.

## 5. Flagged decisions

- **D5-1** ω order statistics by party count over the n-slot array (paper's per-author
  construction; stake-independent).
- **D5-2** piggyback as an extra field on all four response messages + standalone
  `VantageWish` for amplification.
- **D5-3** watermark stamping at serialization time in `VantageCore` (engine stays
  watermark-free) — flag if implemented differently.

Hard rules unchanged: no git writes; no tex-projects; starfish read-only;
CARGO_BUILD_JOBS=4 / `-j 4 --test-threads=4` / no concurrent builds; deviations →
PHASE5-NOTES.md; STOP and ask on anything uncovered or Autobahn-semantic.
