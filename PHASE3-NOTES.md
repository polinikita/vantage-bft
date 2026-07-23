# Phase 3 notes — deviations, decisions, inventory

Companion to `PHASE3-SPEC.md`, same role `PHASE2-NOTES.md`/`MODERNIZATION-NOTES.md`
played for earlier phases. No git commits were made (working tree left dirty for
review, per standing instruction). Written after implementation, in spec order.

---

## 0. Summary

Implemented: `Header`'s `Option` fields + mechanical Autobahn adaptation (§3.1),
`vantage::block` (session id / genesis digest / `BlockOK`, §1/§3.1/§6.1),
`vantage::lanes::LaneManager` (N1–N5, §3.2), `vantage::repair::Repairer` (N6–N8, §3.3),
the `VantageAck` wire variant (§5), `Ack` message type (§3.1), §6.4 metrics counters,
and the full §7 unit/integration test suite (26 vantage tests, all passing). Both
Autobahn assemblies remain green (11 pass / 6 pre-existing-ignored, unchanged from the
Phase-2 gate) and the local-benchmark regression reproduces Phase-2 throughput.

**Not implemented, flagged rather than improvised: §3.4 production wiring
(`Primary::spawn`'s `Protocol::Vantage` arm).** See §5 below — this is the one
deliberate scope cut, and it is a "STOP and flag" item per the work order's own hard
rules, not an oversight.

---

## 1. §3.1 — `Header` Option fields (mechanical Autobahn adaptation)

`primary/src/messages.rs`:
- `signature: Signature` → `signature: Option<Signature>`; `None` on vantage, `Some(_)`
  everywhere on Autobahn (both existing constructors — the async `Header::new` and
  `Header::new_from_key` — updated to wrap `Some`).
- new `sid: Option<Digest>` field; `None` on Autobahn (falls out of `#[derive(Default)]`
  for every `..Header::default()`/`..Self::default()` construction site — nothing to
  touch there), `Some(sid)` on vantage.
- `Hash for Header`: appended a presence-byte-then-content fold for `sid` after every
  existing fold, per spec (fold order of existing fields untouched).
- `Header::verify` (Autobahn-only, unchanged call sites): `self.signature.as_ref().ok_or(...)?`
  before `.verify(...)` — same behavior for every existing caller, since Autobahn
  headers are always `Some`.
- New `Header::new_vantage(author, height, payload, prev_digest, sid)` — sync (no
  signature service), builds the empty-votes `parent_cert` pointer.

**Mechanical adaptation footprint** (everywhere the compiler pointed, once the field
type changed): `primary/src/tests/common.rs` (4 `Header` fixture constructors, wrap
`signature` in `Some(...)`), `primary/src/tests/core_tests.rs` (1 full-field-literal
Header construction, add `sid: None`). No other call site in `primary/src`, `worker/src`,
`node/src` touches `Header.signature` or constructs a `Header` with an explicit field
list (grep-verified) — everything else already went through `..Header::default()` or
never read the field. **Zero Autobahn test regressions**: `cargo test -p primary --lib`
stayed at 11 passed / 6 ignored (identical to the Phase-2 gate number) throughout.

`primary/src/primary.rs`: appended `VantageAck(Ack)` as the new last variant of
`PrimaryMessage` (bincode wire-compat rule — same reasoning as Phase 2's
`PrimaryWorkerMessage::Committed`).

`config/src/lib.rs`: added `Parameters::max_block_payload: usize` (`#[serde(default)]`
= 16, so pre-Phase-3 parameter files keep deserializing) and one new `Parameters::log()`
line (new line, not a changed one — Phase-2's log-format-compatibility invariant is
about not *changing* existing strings, which this doesn't).

---

## 2. §3.1 — `Ack` message type

Added to `messages.rs` alongside `Header`: `Ack { author, height, digest, sender }`,
unsigned, with `.reference() -> (PublicKey, Height, Digest)` for keying. Currently
unused outside the vantage module and its tests (see §5 — no production wiring calls
`LaneManager::process_ack` with a real network-derived `Ack` yet), but it is real,
tested wire-shape code (round-trips through `bincode`/`serde` like every other
`PrimaryMessage` payload), not a stub.

---

## 3. Module layout and one deliberate architectural simplification

`primary/src/vantage/{mod,block,lanes,repair}.rs` + `tests/`. Reuse-first, per the
project's stated preference:

- `block.rs`: `BlockRef` type alias, `session_id`/`genesis_digest`/`domain_hash`,
  `block_ok`. No new struct for "block" — it's the `Header` from §3.1, exactly as
  §3.1 mandates ("No parallel `DataBlock` struct").
- `lanes.rs`: `BlockCache` (the shared, protocol-level block index) + `LaneManager`
  (N1–N5: ack accounting, C/T registers, own-lane publication).
- `repair.rs`: `Repairer` (N6–N8: Authorize walk, request/serve bookkeeping).

**Simplification vs. the spec's literal "two independent tasks" framing (§3.2/§3.3
each say "One task ... owning" their own state):** `LaneManager` and `Repairer` share
one `BlockCache` (`Arc<Mutex<BlockCache>>`, `SharedBlocks`) instead of each keeping a
disjoint cache and message-passing block bytes across a channel between them. Reasoning:
`Repairer`'s serving (N7) must answer a request uniformly regardless of *how* a block
was obtained — directly published (`LaneManager`'s doing) or repaired
(`Repairer`'s doing) — so the two "tasks" fundamentally need a common view of "every
block we've ever obtained, and whether it's retained." Splitting that into two disjoint
caches with a synchronization channel between them would be a parallel
reimplementation of the same state for no behavioral gain (the module plan itself
already reuses `store.write`/`store.read` as one shared body-storage backend for both
paths, §3.3's last sentence) — this just does the equivalent for the in-memory index.
Each side's *own* bookkeeping stays fully disjoint as specified:
`LaneManager` alone owns `ack_senders`/`acked`/`c_candidate`/`t_candidate`;
`Repairer` alone owns `authorized`/`requested`/`pendingReq`/`answered`. Cross-notification
(a block becoming cached via one side wakes the other) is an explicit, narrow
`Effect::BlockCached` / `Repairer::on_block_available` hook, not open shared mutable
access to each other's private sets.

Both structs are synchronous-effect-returning (`Vec<Effect>`), not literal `tokio::spawn`
task loops with their own `run()`. This is what let the §7 test suite drive them
directly, exactly as the spec's test-style note asks ("no full node"), without a mocked
network/store harness per test. Wrapping them in a `run()` loop that reads channels and
dispatches `Effect`s to the real network/store is exactly what §3.4 wiring would add
(see §5) — the current shape is the correct base for that, not a dead end.

---

## 4. Interpretive decisions (spec ambiguities resolved, not silently guessed)

**4.1 What "hash(b)"/`H("data-block" || enc(b))` means for a real (non-genesis) block.**
§1's object-map row states the paper's block hash as a domain-tagged blake3 over the
block's *encoding*. Read completely literally, that would mean recomputing a *second*,
different digest per block (distinct from `Header.id`/`digest()`) purely for vantage
reference purposes. §3.1 simultaneously instructs modifying the *existing* `Hash for
Header` impl (the incremental, per-field fold already used for `Header.id` everywhere
in Autobahn — store keys, `Certificate.header_digest`, votes, etc.) by appending the new
`sid` fold "after the existing folds." Given `Header.id` is already the codebase's
universal block-identity convention, and §6.1 uses the *same* domain-tag helper only for
the **genesis** digest (which is not a real `Header` at all — there's nothing to
`Header::digest()`), I read row 1's "H(...)" as the paper-level description of what a
content hash conceptually is, realized concretely as: `Header::digest()`/`.id` for real
blocks (the incremental fold, now folding `sid` in), and the standalone
`domain_hash`/`genesis_digest` helper only for the genesis placeholder and (per its own
doc comment) future phases' non-`Header` hashing needs. Implemented that way; a single
`domain_hash` helper backs both interpretations so nothing is lost if this needs
revisiting.

**4.2 "Canonical bincode" (`BlockOK`'s first clause, N9's "non-canonical" rejection).**
By the time a `Header` reaches any vantage check, the network dispatcher
(`PrimaryReceiverHandler::dispatch`) has already decoded the whole `PrimaryMessage` once
via `bincode::deserialize` — there is no raw-byte re-parse opportunity at the
`LaneManager`/`Repairer` layer, and bincode 1.x's `deserialize` does not itself reject
trailing/unconsumed bytes. `block_ok` therefore checks canonicality the same way
Autobahn's `Header::verify` already does: `header.digest() == header.id` — the header's
*declared* identity must match its recomputed one. This is not vacuous: it's exactly
what prevents a block whose `id` (used everywhere as its content address — repair
coordinates, `store` keys, parent pointers) is inconsistent with its own fields, which
would otherwise let two nodes disagree about "the same" block's identity. A raw-bytes
canonical-encoding check would only add anything beyond this if the vantage hash used
raw wire bytes directly, which (per §4.1) it doesn't.

**4.3 "q distinct parties" (N4) realized as stake, not raw count.** N4's prose says
"acks from ≥ q distinct parties"; §3 explicitly instructs reusing
`committee.quorum_threshold()`/`validity_threshold()`, which are `Stake`-denominated
(not authority-count-denominated) in this codebase. Implemented `ack_stake` (sum of
`committee.stake(sender)` over first-hand ack senders) compared against those two
`Stake` thresholds — the literal-count reading and the stake reading coincide exactly
under the fixture/`local_benchmark`'s uniform stake-1 committees, and the stake reading
is the one the spec's own named helper functions require in the weighted-stake general
case. Not a deviation from the spec so much as resolving which of two coincident
readings the named helpers pin down.

**4.4 Serve acceptance is not gated on having actually requested `h`.** N6's prose is
"On `serve(h,b)` **for a requested `h`**"; `Repairer::on_serve` does not check `h` against
`requested` before validating and caching. Reasoning: gating would only change behavior
for *unsolicited* serves, and accepting a hash-correct, `BlockOK` body from anyone is
strictly beneficial (it's exactly what "coordinate-independent" caching already
embraces for *mis*-addressed serves) with no safety cost — the acceptance criteria
(`hash(b) == h`, `BlockOK`) are the only things that matter for whether cached data can
ever be trusted, not who sent it or whether we asked. Since the production fan-out
(§6.2, D2) always asks *every* other party anyway, this is not reachable in practice
through the honest path; it only matters for a party that serves without being asked,
which this implementation tolerates rather than rejects.

**4.5 A genuine hang-safety gap found and fixed during self-review (not spec-driven,
a bug caught before it shipped).** The first implementation of
`BlockCache::direct_prefix_ok`/`verified_prefix_through_genesis` and
`LaneManager::retain_prefix`/`prefix_contains` walked purely by following
`parent_cert.header_digest` pointers until reaching the genesis digest. `BlockOK` only
checks a block's *own* internal consistency (`parent_cert.height + 1 == height`) —
never that the block actually found at `parent_cert.header_digest` really has that
height. A block honestly published at height 10 with its predecessor pointer aimed at
some other **real, already-cached, unrelated** block sitting at height 3 (not 9) is
individually `BlockOK` (self-consistent) and would, under the pointer-only walk, either
falsely appear to verify (if that unrelated block's own chain happens to reach genesis)
or — worse — never terminate if the reachable graph among cached blocks contains a
directed cycle. (A *literal* two-block mutual-parent cycle turns out to be
cryptographically infeasible to construct, since each block's `id` is a hash of its own
`parent_cert.header_digest`, so two blocks can't consistently name each other as parent
without inverting blake3 — but the height-skipping attack needs no such inversion: it
just points at an arbitrary already-existing real block.) Fixed by tracking an expected
height that strictly decrements by exactly one per step, checked against each visited
block's actual `height` field; this simultaneously (a) enforces the "consecutive
heights" clause of "valid lane prefix" (§1) that the original code was missing outright,
and (b) bounds every walk to at most the starting height's worth of iterations,
independent of what any adversarial cached data looks like. Covered by
`chain_tests::rejects_non_consecutive_real_predecessor`.

**4.6 N8(ii) retention-on-check, found and fixed during self-review.**
`holds_prefix` (§4's query) can be `true` via chain validity alone, without the block
ever having gone through the ack path (N3, which requires payload availability too,
D1) or the repair path's own genesis-completion retention (N6/N8(iii)) — e.g. a
directly-published, fully chain-linked prefix whose payload for some ancestor simply
hasn't arrived yet. N8(ii) requires that "every prefix whose local holding a rule relies
on in a local-availability/AuthorOK/tip-anchoring check" be retained. The initial
implementation left `holds_prefix` a pure, non-mutating query, so such a prefix could be
chain-valid *forever* without ever becoming `retained`, meaning `Repairer::try_serve`
(which gates on `entry.retained`) would refuse to serve it even though this node
genuinely holds a complete, valid copy. Fixed: `holds_prefix` is now `&mut self` and
retains the whole verified prefix as a side effect of a successful check.
`direct_pub`/`author_ok`'s success does *not* need the same fix — it's already retained
via N3's `refresh_author` pipeline, which runs after every relevant mutation. Covered by
`retention_tests`.

---

## 5. §3.4 wiring — deliberately not built; flagged per the work order's own rule

The module plan's §3.4 describes `Primary::spawn` gaining a `Protocol::Vantage` arm that
spawns `LaneManager`+`Repairer` as real tasks, with `PrimaryReceiverHandler::dispatch`
routing `Header`/`HeadersRequest`/`VantageAck` to them.

Building this surfaced a gap the transcribed spec text does not address:
**`network::MessageHandler::dispatch` (the trait every `NetworkReceiver` handler
implements) receives no peer identity at all** — not even the raw `SocketAddr`
(`Receiver::spawn_runner` has it locally but never passes it to `handler.dispatch`).
N1's core rule — "a recipient treats a block as author publication only when the
*channel sender* and the block's encoded author are both `p_a`" — is literally
impossible to evaluate against a live `Header` publish message under the current
transport surface: there is no channel-sender information available at the point
`PrimaryMessage::Header` is dispatched. (D4's acks/requests sidestep this because those
messages carry an explicit, spec-mandated *declared* sender/requestor field trusted the
same insecure way `HeadersRequest.requestor` already is — but `Header`/`publish` has no
such field; its only identity is `author`, which is exactly the thing N1 needs to
compare *against* something external.)

Making N1 realizable in production would require extending `MessageHandler::dispatch`'s
signature to thread the peer's address (or a resolved identity) through — a change that
necessarily touches every existing implementor, including both of Autobahn's
(`PrimaryReceiverHandler`, `WorkerReceiverHandler`) and worker's own handler, plus
`network::Receiver`/`network/src/tests` and every test fixture across `primary`/`worker`
that constructs a `MessageHandler`. That is a cross-cutting network-layer change not
mentioned anywhere in the transcribed §§3–6, and per the work order's own hard rule
("anything touching Autobahn protocol semantics [or infrastructure the spec doesn't
cover] — STOP and message me instead of improvising"), I did not make it.

**Consequence:** `Primary::spawn` is unchanged; `Protocol::Vantage` still isn't
constructible from `node run`/`node local-benchmark` (both already bail for it — D3,
predates this phase, unaffected). `LaneManager`/`Repairer` are complete, tested,
production-*quality* library code — just not yet wired to a live network. This is the
one place §3.4's letter isn't satisfied; recommend deciding the dispatch-identity
question (thread `SocketAddr` through `MessageHandler::dispatch`, mechanically adapted
across both Autobahn handlers as a no-op parameter, versus some other mechanism) before
Phase 4/5 wiring depends on `LaneManager`/`Repairer` being reachable from a running node.

---

## 6. §6.4 metrics

Added to `metrics::Metrics` (always registered, same pattern as every existing field —
only observed into on the vantage path): `vantage_blocks_published`,
`vantage_blocks_received`, `vantage_acks_sent`, `vantage_acks_received`,
`vantage_repairs_requested`, `vantage_repairs_served`, `vantage_retained_bytes`.
`LaneManager`/`Repairer` take an optional `Arc<Metrics>` via `.with_metrics(...)`
(`None` by default — most unit tests skip it); `vantage::tests::metrics_tests` verifies
every counter actually increments at the right call site, not just that it compiles.

---

## 7. §7 test gate — N-rule coverage map

All in `primary/src/vantage/tests/`, 26 tests, all passing
(`cargo test -p primary --lib vantage`):

| N-rule | Test(s) |
|---|---|
| N1 (own-lane publish, self-delivery, author==sender) | `chain_tests::accepts_valid_direct_publish`, `relay_then_authentic_upgrades` |
| N2 (publish validation, upgrade-on-authentic-republish) | `chain_tests::rejects_wrong_sid`, `rejects_non_canonical`, `rejects_oversized_payload`, `rejects_wrong_predecessor`, `rejects_non_consecutive_height`, `rejects_non_consecutive_real_predecessor`, `relay_then_authentic_upgrades` |
| N3 (ack trigger, once-per-tuple, repair-only never acks) | `ack_tests::acks_exactly_once_per_tuple`, `repaired_only_prefix_never_acked`, `ack_withheld_until_payload_arrives` (D1 gate) |
| N4 (first-hand counting, dedup, q-available boundaries) | `ack_tests::per_sender_ack_dedup`, `q_available_exact_boundaries` |
| N5 (C/T registers: tiebreak, fork rule, no-C anchoring, strict containment) | `registers_tests::newest_tiebreak_by_smallest_digest`, `fork_pins_one_branch_for_t`, `no_c_entry_any_direct_tip_qualifies` |
| N6 (Authorize walk: recursion, fan-out-once, false-coordinate, corrupted serve) | `repair_tests::recursive_walk_over_served_blocks`, `request_fanout_all_parties_once`, `false_coordinate_cached_not_advanced`, `corrupted_serve_ignored` |
| N7 (pendingReq before possession, answer-once) | `repair_tests::pending_request_answered_once_on_retention` |
| N8 (retention: acked, late request after retention, no discard) | `retention_tests::acked_prefix_served_to_later_requester`, `late_request_still_served_after_retention`, `no_discard_on_local_events` |
| N9 (session hygiene / malformed rejection) | `chain_tests::rejects_wrong_sid`, `rejects_non_canonical`, `rejects_oversized_payload` (folded into N2's tests above — the same checks are §2's N9 clause) |
| §6.4 metrics | `metrics_tests::lane_manager_counters_observe`, `repairer_counters_observe` |

Autobahn regression: `cargo test --workspace` — `primary` lib: 37 passed (11 Autobahn +
26 vantage), 6 ignored (pre-existing, unchanged from Phase 2), 0 failed; `worker`: 6/6;
`crypto`: 7/7; `config`/`store`/`network`: 0 tests (unchanged). Full command used
throughout (per the resource cap): `CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 --
--test-threads=4`.

`node local-benchmark --nodes 4 --workers 1 --rate 240000 --tx-size 512 --protocol
autobahn-optimistic --mode all-zero --duration 60`: **240,977 tx/s, 14,458,620 committed
transactions, 0 misses** — reproduces the Phase-2 gate number (239,786–240,997 tx/s
across that phase's runs) closely; vantage tasks are never spawned on this assembly
(§3.4/D3), so this is a pure "did the mechanical `Option` adaptation change Autobahn
behavior" check, and it didn't. Latency (109 ms avg) is the known in-process
self-hosting artifact documented in PHASE2-NOTES.md §8/R3, not a Phase-3 concern.

---

## 8. Simplification pass

Done before the audit read below, per the standing per-phase rule. Changes: removed a
redundant second `recompute_registers` call inside `on_direct_pub_confirmed` (the only
caller, `refresh_author`, already calls it once after its loop); removed an unused
`is_genesis_ref` helper and an unused `Repairer::is_authorized` test accessor; removed
one now-dead `Hash as _` import. Re-ran the full suite after each; all green throughout
(see §7's numbers, taken post-simplification).

## 9. Known limitations (documented exposure, not defects)

- **§3.4 wiring gap** — see §5. The largest open item.
- **`Repairer::settle`'s recursion depth scales with chain height** (genuine Rust call
  stack, not the height-bounded *iterative* walks in `lanes.rs`, which use an explicit
  counter instead of recursion for the same reason). Bounded and correct for Phase 3's
  scale; a very deep, fully-offline-then-repaired chain could in principle be a stack
  concern. Not tested at that scale here — flagging for whoever first needs repair over
  chains of that depth (likely Phase 6's Byzantine/partition-heal scenarios).
- **§6.3's documented exposure stands as specified**: `requested`/`pendingReq`/
  `answered`/per-tuple ack sets are unbounded against a Byzantine peer spamming distinct
  *tuples* (not just distinct messages for the same tuple, which are already
  deduplicated). Explicitly out of scope per the spec (Phase 6 concern).
- **D4's sender-spoofing gap is unchanged and, per §5, now load-bearing for more of the
  protocol than D4's own text anticipated** — not just acks/requests (as D4 says) but
  N1's direct-publication distinction for `Header` itself has no realizable enforcement
  mechanism at all yet in this transport. Byzantine-suite claims (Phase 6) must account
  for this being wider than D4 as written suggests.

## 10. Self-review status (two-pass audit bar)

I ran two of my own adversarial read-throughs of `vantage/{block,lanes,repair}.rs`
against every N-rule in §2, back to back, before writing this file:

- **Pass 1** found and fixed two real defects (§4.5's hang/consecutive-heights gap,
  §4.6's N8(ii) retention gap), plus the minor simplification-pass items in §8. Both
  fixes have dedicated regression tests (§7's coverage table).
- **Pass 2**, after those fixes and re-running the full suite, found nothing further.

This satisfies the *mechanics* of the phase's two-pass bar as far as a single
implementing session can; the phase gate's own text specifies this as an audit *by the
user/Fable* against a separate session's diff, which this note-writing session cannot
substitute for. Recommend that adversarial pass before treating the Phase-3 gate as
formally closed, same as every prior phase.

---

## 11. Fable audit — pass 1 (findings addressed; two-clean-pass counter restarted)

Three findings, all fixed; rulings recorded on every self-flagged item from §§4–9.

**P1-1 — DEFECT, fixed: cross-author graft.** `BlockCache::verified_prefix_through_genesis`
and `direct_prefix_ok` (and, for defense-in-depth, `LaneManager::prefix_contains`) never
checked that every visited block along a walk shares the *same author* as the walk's
starting reference — §1's "valid lane prefix" definition's first clause ("one author
index") was silently unenforced. `BlockOK` only checks a block's own height arithmetic
(`parent_cert.height + 1 == height`) against *itself*, never that the block actually
found at `parent_cert.header_digest` belongs to the same author. Concretely: a
Byzantine author `A` publishes a block at height `k` whose predecessor pointer names
author `B`'s genuine, cached, direct+payload_ok, chain-valid block at height `k-1`; both
walks then validate straight down `B`'s real chain to genesis, so `DirectPub_i(A,k,h)`
would incorrectly hold and get acked — forging a first-hand availability statement for
a chain `A` never actually built. Fixed by pinning `author = start.block.author` at the
top of each walk and rejecting any visited block whose `author` differs, in all three
functions (`repair.rs`'s `settle` was already immune — it checks `b.author == author`
per step). Regression tests: `chain_tests::rejects_cross_author_graft` (direct_pub/ack
path (a) and `holds_prefix` (b), both via one grafted block, at an author-consistent
past-genesis height so the check is actually exercised rather than short-circuited by
running out of height first) and
`registers_tests::cross_author_graft_never_selected_as_t_candidate` (c: confirms the
graft never wins "newest wins T" against the author's own genuine, shorter tip).

**P1-2 — reverted deviation §4.4: `Repairer::on_serve` now gates on requested hashes.**
Added `requested_hashes: HashSet<Digest>` (populated everywhere `settle` fans out
`request(h)`); `on_serve` early-returns before any cache mutation if the served block's
`id` isn't in it. The paper's "on serve(h,b) **for a requested h**" clause is normative,
not incidental — my original "strictly beneficial, no safety cost" argument for
accepting unsolicited hash-correct bodies missed that it lets any peer bulk-inject
unbounded valid blocks *of its own lane* into the shared cache without us ever asking,
which is a free-memory attack outside §6.3's documented (attacker-cost-proportional)
exposure, and pollutes `by_author`/`refresh_author` scans with data nobody asked for.
Coordinate-independent caching is unchanged for hashes we *did* solicit (§3.3's
false-coordinate-serve behavior). Regression test:
`repair_tests::unsolicited_serve_changes_no_state`.

  **Second-order fix this required, found while writing the P1-2 regression tests, not
  itself something Fable's pass 1 flagged:** gating `on_serve` on `requested_hashes`
  means each ancestor along a repair walk is now only ever served *after* the walk
  itself has requested it specifically — so ancestors necessarily arrive one at a time,
  each via its own separate `on_serve`/`on_block_available` call, rather than
  (as the original tests assumed) potentially all already sitting in the cache by the
  time the top block resolves. `Repairer::on_block_available` originally re-settled
  *only* the exact `(author, height, digest)` matching the just-arrived block. That is
  insufficient once ancestors arrive strictly one at a time: `settle`'s retention
  propagation only unwinds back up its *own* call stack, so when (say) `h3` is served
  first, `settle` recurses into `h2`, finds it uncached, and returns; when `h2` is later
  served, a *fresh, separate* `settle(h2)` call resolves `h2`'s own parent (`h1`) but has
  no way to reach back into the long-gone `h3` stack frame from earlier — `h3` would
  simply never become retained. Fixed by having `on_block_available` re-settle *every*
  currently-`authorized` reference on each arrival (not just the one matching the new
  digest): since by the time the last ancestor arrives every block in the chain is
  already cached, re-settling the topmost reference walks the whole prefix down to
  genesis in one nested call and retains it correctly.
  `repair_tests::recursive_walk_over_served_blocks` was rewritten to serve blocks in the
  realistic causal order this gate now enforces (top hash requested first; each serve
  triggers the next ancestor's request) instead of the old out-of-order/unsolicited
  scenario, and now also asserts the intermediate `RequestTo` effects at each step.

**P1-3 — minor, done: `try_serve` now removes answered `pendingReq` entries.** One line
(`self.pending_req.remove(&(peer, h.clone()))` alongside the `answered` insert) so the
set doesn't grow forever; `answered` already made repeat entries inert, this is purely
about not leaking memory for something we'll never need again.

**Rulings recorded (no code changes needed for these):**
- §5 (production wiring deferral) — **accepted**. Resolves with the Phase-7
  authenticated-channels decision; production wiring moves to the Phase-4 preamble.
  Until link auth exists, a publish-typed `Header` is treated as claimed-by-author under
  D4's documented trust model. §9's note that this widens D4's stated scope (beyond
  acks/requests, to `Header` publish itself) stands as written.
- §4.1 (hash interpretation: `Header::digest()`+`sid` fold as block identity;
  `domain_hash` reserved for genesis) — **accepted**. Noted: the type-domain separation
  between "real block" and "genesis placeholder" rests on differing *input shapes*
  (a full `Header` vs. the literal bytes `b"genesis"`), not on distinct domain tags —
  acceptable for this artifact, flagged for awareness rather than as an action item.
- §4.3 (stake-weighted `q-available`, not raw party count) — **accepted**.
- §4.6 (`holds_prefix` retains-on-query, N8(ii)) — **accepted**.
- **Non-blocking efficiency note (recorded, not acted on):** `refresh_author`/
  `recompute_registers` re-walk O(height) per reference per event — quadratic per lane
  over time. Fine at Phase-3 scale (confirmed by the local-benchmark regression and the
  full unit suite's runtime); when §3.4 wiring lands, consider realizing §3.2's original
  design intent — an incremental `direct_prefix_ok: bool` memoized directly on
  `BlockEntry`, updated once per new block rather than recomputed by walking every time
  — instead of the current from-scratch-per-query walk. Not implemented now, per
  instruction.

**Post-fix verification:** `CARGO_BUILD_JOBS=4 cargo test --workspace -j 4 --
--test-threads=4` — primary: 40 passed (11 Autobahn + 29 vantage, up from 26: +3 new
regression tests for P1-1(a/b), P1-1(c), P1-2), 6 ignored (pre-existing, unchanged), 0
failed; worker 6/6; crypto 7/7; all else unchanged. No other files touched.

---

## 12. Fable audit — gate closed (2026-07-23)

Restarted two-pass sequence after the P1-1/P1-2/P1-3 round:

- **Pass 1 (post-fix): CLEAN.** Re-read of lanes.rs walks (author pinning correct in all
  three; pinned to the start block's author, which exact_coordinate ties to the ref),
  repair.rs (requested_hashes gate before any mutation; re-settle-all-authorized is
  correct and effect-idempotent — retained/answered/requested dedups absorb repeats;
  pendingReq removal in place), new regression tests verified genuine (the graft test is
  correctly calibrated to reach the author check rather than exhausting the height
  budget). Independent test run: primary 40 passed / 6 ignored / 0 failed.
- **Pass 2: CLEAN.** messages.rs (`sid` fold injective and appended after existing
  folds; every non-digest-bound Header field — parent_cert.height, votes, signature,
  consensus metadata — is pinned by `block_ok`, so same-digest malleability attacks
  cannot enter the vantage cache; `new_vantage` sound), vantage/mod.rs Effect enum,
  ack/registers/retention test files (D1 payload gate, N4 exact boundaries at f+1/2f+1,
  fork pinning, no-discard). Full N1–N9 conformance sweep against PHASE3-SPEC §2:
  conformant.

**Phase-3 gate: CLOSED** (two consecutive clean adversarial passes). Deferred into
Phase 4's preamble: §3.4 production wiring (per §5's dispatch-identity finding —
publish provenance runs on D4's declared-identity trust until the Phase-7
authenticated-channels decision). Working tree remains uncommitted for the user;
suggested Phase-3 commit split: (1) messages.rs Header Option fields + mechanical
Autobahn adaptation, (2) vantage module + tests, (3) config/metrics additions, (4) docs.
