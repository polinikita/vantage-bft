# PHASE4-SPEC — AGB happy path (manifests, R1–R4, grades, fast seal, output cursor)

**Status: FINAL (work order). Precondition: Phase-3 gate closed (it is — PHASE3-NOTES.md
§12).** Self-contained: all protocol rules are transcribed below (source: paper
sec:agb-interface / sec:agb-rules / expansion contract; do not open `tex-projects`).
Builds directly on Phase 3's `primary/src/vantage/{block,lanes,repair}.rs` — reuse those
APIs; extend, never duplicate (user's standing reuse rule).

## 0. Scope and non-goals

In scope: the deferred Phase-3 §3.4 production wiring; the Direct-AGB per-view state
machine (rules R1–R4 below) with **M = ∅ everywhere** (empty auxiliary field); the
responsive proposal frontier and genesis bootstrap; the Vantage fast seal + optimistic
lock recording; the caller-owned try-seal arbiter; the output cursor (deterministic
linearization) for `gfull`/`gcore`; a runnable `Protocol::Vantage` node assembly
(`node run` + `node local-benchmark`) with the Phase-2 real-latency metric working on it;
the §12 test suite. Gate: two consecutive clean adversarial audit passes (Fable).

Non-goals: WISH pacemaker and formal entry for views > 1 (Phase 5 — see §4 for why the
happy path doesn't need them); resolution entries / non-empty M / `Formed`'s aux clause /
`AuxOK`/`Ann`/`ReadyOK` beyond their trivial empty-M forms (Phase 6); `noready`
consumption and lock *use* by the resolver (Phase 6 — Phase 4 only records lock state);
`gskip` production (never a Direct-AGB output; the cursor's gskip arm is implemented but
unreachable); Byzantine fault injection (Phase 6); crash-fault liveness runs (Phase 5).
Autobahn paths: zero semantic changes, ever.

## 1. Wiring preamble (Phase-3 §3.4, deferred here by audit ruling)

One new task, `vantage::node::VantageCore` (D4-1: a **single** spawned task owning
`LaneManager` + `Repairer` + `AgbEngine` + `Frontier` + `Cursor` and executing their
returned `Effect`s — the Phase-3 components are synchronous effect-returning state
machines, so one owning loop avoids shared locks entirely; the shared `BlockCache`
mutex stays as-is).

- `Primary::spawn` gains the `Protocol::Vantage` arm: spawns `VantageCore` plus the
  existing worker-facing receivers (`WorkerReceiverHandler` unchanged) and the metrics
  server; does NOT spawn Core/Proposer/HeaderWaiter/Helper/consensus tasks. Autobahn
  arms byte-identical to today.
- `PrimaryReceiverHandler::dispatch` on the vantage assembly routes:
  `Header(h, false)` → publish path (`LaneManager::process_publish(h.author, h)` —
  provenance is **claimed-by-author** per the recorded D4 ruling: no channel identity
  exists until the Phase-7 authenticated-channels decision; documented, load-bearing);
  `Header(h, true)` → `Repairer::on_serve`; `HeadersRequest(digests, requestor)` →
  `Repairer::on_request` per digest; `VantageAck(a)` → `LaneManager::process_ack(a.sender,
  a.reference())`; the five new §2 variants → `AgbEngine`. Channels into `VantageCore`.
- Effect execution: `BroadcastPublish`/`BroadcastAck`/AGB broadcasts → ReliableSender to
  `others_primaries` (self-delivery: loop the message back into the engine directly —
  counted first-hand, model convention); `RequestTo`/`ServeTo` → ReliableSender unicast;
  `SyncBatches(author, missing)` → `PrimaryWorkerMessage::Synchronize(digests, author)`
  to own workers + spawn a `store.notify_read` waiter per missing key that sends the
  block digest back into the loop → `LaneManager::set_payload_ready`; `BlockCached(d)` →
  `Repairer::on_block_available(d)` (and re-poll the AGB gate, §5).
- Own-lane publication: driven by `rx_our_digests` + `max_header_delay`/`header_size`
  cadence (same pattern as `Proposer`), calling `LaneManager::publish_own`.
- `node/src/main.rs` and `local-benchmark` stop bailing for vantage (D3 lifts).

## 2. Wire messages (appended after `VantageAck`, in this order; all unsigned, sender
declared per D4)

```rust
VantagePropose(ViewProposal),
VantageEcho(Echo),
VantageEchoSkip(View, /* sender */ PublicKey),
VantageReady(Ready),
VantageNoReady(View, /* sender */ PublicKey),
```

`type View = u64`. `Manifest = Vec<(PublicKey, Height, Digest)>` — entries in strictly
increasing author order (the paper's canonical "encoded vector order").
`ViewProposal { view: View, c: Manifest, t: Manifest }` (M is structurally absent in
Phase 4 — adding it later appends a field, wire compat is not required across phases).
`Echo { proposal: ViewProposal, grade: u8 /* 0|1 */, sender: PublicKey }` (the origin
annotation `o` is empty for M = ∅ and therefore not carried yet).
`Ready { proposal: ViewProposal, grade: ReadyGrade /* Zero|One|Mix */, sender: PublicKey }`.
Echoes/readies restate the full proposal (paper: statements name `B_v = (C,T,M)`; readies
must deliver manifests to parties that never saw the proposal). Counting identity =
`proposal_digest = blake3(b"view-proposal" ‖ sid ‖ bincode(ViewProposal))` (domain-tagged
per the model's encoding rules; use `vantage::block::domain_hash`).

## 3. Formed_v (deterministic, syntactic) and proposer selection

`Formed_v(C, T)` (M = ∅ implicit): each of C and T has ≤ 1 entry per author and is
sorted strictly increasing by author; every hash across C ∪ T is distinct; every entry
has height ≥ 1 and an author with stake in the committee. Malformed ⇒ not echoable (R2).

`proposer(v)` (D4-2): round-robin over the committee's authorities in their canonical
sorted order — index `(v - 1) mod n`.

## 4. Responsive frontier, genesis, activation (Vantage wrapper)

- `a_i` = responsive proposal frontier (a `View`). Genesis convention: every party boots
  with frontier 0, formally **entered into view 1** (`e_i(1)` = boot time — arms view 1's
  fallback deadlines). Views ≤ 0 have fixed genesis responses (never sent on the wire; no
  state needed beyond the conventions above). WISH broadcasting is Phase 5.
- Frontier advance: when the party has a **well-formed fixed proposal received directly
  from proposer(u)** for every view up to `u` (contiguous prefix; formal-entry floors
  also raise the frontier — in Phase 4 only the genesis floor of 0 exists), the frontier
  is `u`. Implementation: buffer fixed proposals by view; on each fix or entry, advance
  through the contiguous prefix in view order.
- **`activate(v)` fires exactly when processing the fixed proposal advances the frontier
  to `v`** (a buffered proposal activates later, when its missing prefix arrives).
  `enter(v)` (Phase 4: only v = 1) also activates. A stored proposal is *active* once
  activated (either route) — the R2 positive gate runs only for active proposals.
- A **malformed** fixed proposal cannot advance the frontier (and is not echoable), but
  it is sticky: the first direct proposal from proposer(v) is fixed forever, even as
  `reject`; later versions are ignored.
- R1 trigger: `p_i == proposer(v)` ∧ `a_i ≥ v − 1` ∧ not yet proposed for `v`.
- Consequence for the happy path (why no pacemaker is needed): the positive echo and
  ready paths never require formal entry — activation flows through the contiguous
  proposal chain from genesis. Fallback timers (θE/θR) exist only for entered views —
  i.e. only view 1 in Phase 4. This is paper-conformant: all fallbacks are conditioned
  on entry.

## 5. R2 — echo stage (one-shot; per-view immutable response)

Per-view engine state: `fixed ∈ {⊥, Reject, Proposal}`, `echo ∈ {Pending, Sent}`,
`ready ∈ {Pending, Sent}`, `completed: Option<ViewProposal>`, `directed:
Option<Outcome>`, `activated/active: bool`, `e_i: Option<Instant>`, `ρ_i:
Option<Instant>`, first-hand counted maps (≤ 1 echo-stage and ≤ 1 ready-stage statement
per author per view — the FIRST received; later/conflicting ones ignored; self-delivery
counted immediately), `lock: Option<Lock>`.

- On the first direct `VantagePropose` from proposer(v) (sender == proposer(v)) while
  `fixed = ⊥` and echo pending: set `ρ_i`; if malformed → `fixed = Reject`; else
  `fixed = B`, store it, **authorize** every reference named by C and T
  (`Repairer::authorize` per entry — typed repair, never provenance), process in the
  frontier (§4), maybe activate.
- Positive gate — re-evaluated whenever local state changes (ack counts, payload
  arrivals, block cached, activation), while echo pending ∧ active:
  - `CoreOK_i(C)` = `LaneManager::author_ok(entry)` for **every** C entry;
  - `TipOK_i(C,T)` = `author_ok(entry)` for every T entry **and tip anchoring**: for
    every author present in **both** manifests, the party *holds* the T entry's full
    lane prefix (`holds_prefix` — a pure local hash walk; counted acks never substitute
    for a paired tip) and that prefix **strictly contains** the author's C entry
    (`t.height > c.height ∧ prefix_contains(t, c)`).
  - When the gate first holds: `echo = Sent`, record the fast-seal lock (§8) immediately
    before sending, broadcast `Echo { B, grade: 1 }`. May happen before formal entry.
- Fallback (only when `e_i(v)` is set — view 1 in Phase 4): with `t = max(e_i, ρ_i)`, at
  deadline `min(t + Δ, e_i + θE)`, if echo still pending and `fixed = B`: broadcast
  `Echo { B, grade: 0 }` if `CoreOK_i(C)` holds (AuxOK trivial), else `VantageEchoSkip`.
  At the absolute deadline `e_i + θE` with no active well-formed fixed proposal:
  `VantageEchoSkip`. A proposal delivered after that deadline is ignored. Deadline ties
  favor messages and positive gates (drain message queues before taking a timer branch).
- One echo-stage statement per view, ever (grade-1 echo, grade-0 echo, or echo-skip).

## 6. R3 — ready stage

While ready pending, on every counted-echo change: if some `B` has **Q = 2f+1
(quorum_threshold, stake)** counted proposal echoes (any grades, identity by
proposal_digest) — ReadyOK is trivial for M = ∅ — broadcast
`Ready { B, grade: g }` where, over all echoes counted at emission: `g = One` if Q
counted echoes for B have grade 1, `Zero` if Q have grade 0, else `Mix`. **No entry,
fixed-proposal, or own-echo guard** — a party can go ready purely on others' echoes.
If entered (view 1) and the positive gate hasn't fired by `e_i + θR`: broadcast
`VantageNoReady`. One ready-stage statement per view, ever.

## 7. R4 — completion, direct seal, try-seal arbiter

On every counted-ready change, for each `B` named by counted proposal-ready statements:
- If not completed and ≥ Q readies (grades disregarded) name B: `completed = B`, store
  it, authorize C's lane prefixes, fire `complete(v) → B` (the core becomes irrevocable;
  hand (C,T) to the cursor as this view's manifests, state `gopen`).
- If not directed and ≥ Q **grade-1** readies name B: `directed = gfull(C,T)`, submit to
  the arbiter. Else if not directed and ≥ Q **grade-0** readies name B: `directed =
  gcore(C)`, submit.
- Ready counting continues after completion — a late homogeneous quorum still produces
  the direct result. Neither completion nor direct results wait for entry.

**Try-seal arbiter** (caller-owned, per view): first submission wins and emits the
terminal `seal(v) → X` (drives the cursor); every later submission is ignored
(`debug_assert!` compatibility: same outcome, per the paper's compatibility guarantee).

## 8. Vantage fast seal + optimistic lock (M = ∅ wrapper; caller-side, R1–R4 unchanged)

- A *matching response* for the exact data-only proposal `B` = a grade-1 proposal echo
  for exactly B (proposal_digest equality).
- Immediately before sending our own matching response, record lock `L_i(v, B)`. The
  lock is *active* exactly while fewer than **f+1 parties** (D4-3: party count, not
  stake — unanimity/threshold counting here follows the paper's "all n parties" /
  "f+1 responses" phrasing; with uniform stake they coincide) have been counted with
  echo-stage responses for v that are NOT matching responses for B. A lock may be born
  inactive; once inactive it never reactivates. Lock state persists (Phase-6 resolver
  input); Phase 4 only records and updates it.
- Upon counting matching responses from **all n parties**: emit `fastseal(v) → gfull(C,T)`
  once and submit it to the arbiter. It does NOT fire complete/direct-seal, expose a
  core, or create a completion report; R3/R4 continue normally.

## 9. Output cursor (deterministic linearization) + commit metric

Views processed in strictly increasing cursor order; `D` = set of block hashes already
output, initialized `{genesis_digest}`.

- `Expand_D(X)`: traverse manifest X in encoded vector order (increasing author); for
  each entry, traverse its lane prefix **from genesis toward the named frontier**
  (genesis itself never output); omit hashes in D or seen earlier in the same traversal
  (dedup by block hash); emit only after obtaining and verifying hashes, author
  coordinates, heights, predecessor links, and BlockOK for the whole prefix (Phase-3
  walks; missing prefixes are already authorized — wait on `BlockCached` wakeups).
- `K_{v,D} = Expand_D(C_v)`; `T̂_{v,D} = Expand_{D+K}(T_v)`; `Full = K ‖ T̂`;
  `Core = K` — the completed core is literally a prefix of the full view expansion.
- At a locally **completed but open** cursor view: emit `K_{v,D}` (request missing core
  prefixes first), record core-emitted, do NOT advance (tip open). On seal:
  `gfull` → emit K if not yet, then append `T̂`, advance; `gcore` → emit K if needed,
  advance without T; `gskip` → emit nothing, advance (arm implemented, unreachable in
  Phase 4). A fast full seal already names both manifests — the cursor uses them
  directly. Late compatible completions/seals are idempotent — never reopen a view or
  duplicate output. Later AGB instances run concurrently and buffer; **no payload from a
  later view crosses an open cursor position.**
- **Commit metric (Phase-2 parity):** for every block appended to the output log, send
  `PrimaryWorkerMessage::Committed(now_millis, batch_digests)` to own workers grouped by
  WorkerId — the existing worker-side observe path does the rest; `committed_transactions`
  counters and real-latency histograms work unchanged, so `local-benchmark` RESULTS and
  the Grafana dashboard work for vantage with zero harness changes. Also emit the
  existing `info!("Committed {header}")`-shaped log line per block (harmless for fab).

## 10. Timers and parameters

New `Parameters` field: `delta_ms: u64`, `#[serde(default)]` = 1000 (Δ). θE = 5Δ,
θR = 6Δ (hard constants, paper). Timers exist only for entered views (view 1 in
Phase 4). Implementation: `tokio::time::sleep_until` futures in the `VantageCore`
select loop; drain message channels before firing a due timer (tie-favoring rule, §5).

## 11. Module plan

- `vantage/agb.rs` — `AgbEngine`: per-view state map, R2/R3/R4 + fast seal + arbiter;
  effect-returning like Phase 3. New `Effect` variants: `BroadcastPropose(ViewProposal)`,
  `BroadcastEcho(Echo)`, `BroadcastEchoSkip(View)`, `BroadcastReady(Ready)`,
  `BroadcastNoReady(View)`, `Sealed(View, Outcome)`, `ArmTimer(View, TimerKind)`.
- `vantage/frontier.rs` — frontier + proposal buffer + R1 trigger (reads the N5
  registers `c_candidate`/`t_candidate` for construction; process authors in canonical
  order, skipping any hash already listed under an earlier index).
- `vantage/cursor.rs` — §9, including the Committed notifications
  (`Effect::NotifyCommitted(u64, Vec<(WorkerId, Vec<Digest>)>)`).
- `vantage/node.rs` — the §1 wiring task (`VantageCore::spawn`).
- GC/`Cleanup`: still not wired (N8 discipline; retention unbounded, documented).

## 12. Tests (each cites its rule) and gate

- R1: construction determinism from register state; skip-dedup across author indices;
  proposes exactly once; frontier trigger boundary (a_i = v−2 ⇒ no propose).
- R2: positive gate fires on exact predicate satisfaction; each negative independently
  blocks (C entry not author_ok; T entry acked-but-not-held — counted acks never
  substitute for possession; tip not strictly containing C; equal-height tip excluded;
  malformed proposal → sticky Reject, later versions ignored, frontier not advanced);
  buffered proposal activates when the contiguous prefix arrives; grade-0 fallback and
  echo-skip at the right deadlines (view 1, injected clock); proposal after θE ignored;
  echo-stage one-shot immutability.
- R3: grade One/Zero/Mix over crafted echo sets; Q boundary exact; no own-echo guard
  (ready without having echoed); ready one-shot; noready at θR.
- R4: completion on mixed-grade quorum; direct seals on homogeneous quorums; late
  homogeneous quorum after completion still seals; arbiter first-wins + later-ignored.
- Fast seal: all-n matching → fastseal → arbiter (and R4's later direct result ignored);
  lock recorded before matching echo; lock deactivates at f+1 non-matching and never
  reactivates; no complete/direct side effects from fastseal.
- Cursor: expansion order + cross-view dedup (a block output in v never re-output in
  v′>v); core-prefix-of-full property; open tip blocks later views' payload; gcore skips
  T̂; idempotent duplicate seal; missing-prefix wait then emit.
- Integration: 4 in-proc engines wired over channels, full happy path for ≥ 3
  consecutive views: propose → all grade-1 echoes → fast seal at 2δ AND ordinary gfull
  via readies; output logs identical across parties (assert byte equality).
- **Gate**: all green; Autobahn regression (workspace tests + one `local-benchmark`
  optimistic run reproducing Phase-2/3 throughput); **4-node
  `local-benchmark --protocol vantage` run: sustained commits at the configured rate,
  identical output logs, real-latency metric reporting** (this is the plan's "3δ core
  seal / 1-hop tip admission" demonstration); simplification pass; then TWO consecutive
  clean adversarial audit passes (Fable) against §§4–9.

## 13. Flagged decisions (user review)

- **D4-1** single `VantageCore` task executing effect loops (vs. task-per-component).
- **D4-2** proposer(v) = sorted-key round-robin.
- **D4-3** fast-seal thresholds count parties (unanimity semantics), not stake.
- **D4-4** echoes/readies restate full manifests on the wire (≤ n entries each; identity
  via domain-tagged proposal_digest).
- **D4-5** `delta_ms` default 1000.
- Standing: D4 (declared sender identity) now also covers propose/echo/ready statements
  — first-hand counting trusts the declared `sender` until the Phase-7 channel-auth
  decision; recorded as load-bearing in PHASE3-NOTES §9.

Hard rules for the implementer: never git commit/add/push; never touch tex-projects;
starfish read-only; CARGO_BUILD_JOBS=4, `cargo test -j 4 -- --test-threads=4`, no
concurrent builds; deviations → PHASE4-NOTES.md; STOP and ask on anything the spec
doesn't cover or that touches Autobahn semantics.
