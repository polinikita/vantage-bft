# PHASE3-SPEC — Vantage data plane (chains, ACKs, repair, retention)

**Status: FINAL (work order). Precondition: the Phase-2 gate is closed (audit passed).
Line anchors below were taken 2026-07-23 pre-Phase-2-completion and may shift slightly —
anchor by symbol name.**

Implements the paper's §"Data Dissemination" (sec:agb-diss) as the `vantage` protocol's
data plane inside the Autobahn artifact. Paper source (READ-ONLY, do not open unless told):
`signature-free.tex` lines 646–786 + `algorithms/dissemination.tex`. This spec is
self-contained — every normative rule is transcribed below; implement from here, not from
the paper.

Depends on Phase 2 being closed: `Protocol::Vantage` enum arm exists, blake3 is the only
hasher, metrics crate available. Phase-2 invariants that keep holding here: user commits
everything (never run `git commit/add/push`); wire enums are append-only; existing log
strings unchanged; `~/code/starfish` read-only; never touch `~/code/tex-projects`.

## 0. Scope and non-goals

In scope: per-author hash-chained data blocks over the existing lane/car machinery;
all-to-all unsigned ACKs; first-hand availability accounting (q-available, AuthorOK,
C/T candidate registers); authorized recursive repair (request/serve); the retention rule;
unit/integration tests. All of it lives behind `Protocol::Vantage` — the two Autobahn
assemblies must be byte-identically unaffected (their code paths untouched or provably
equivalent-gated).

Non-goals (later phases): manifests/`Formed`/proposals, echo/ready state machine, grades
(Phase 4); WISH pacemaker, `activate`/`enter` (Phase 5); resolution, control log (Phase 6);
fab benchmark runs of `vantage` (Phase 7 — node `main.rs` keeps bailing for vantage until
Phase 4/5 makes it runnable; Phase-3 components are driven by tests). No signing anywhere
on the vantage path — `crypto` is used only for `Digest`/blake3.

## 1. Object map (paper → implementation)

| Paper | Implementation |
|---|---|
| party `p_i`, `Π`, `n = 3f+1` | committee entry (PublicKey as identity only; no key ops), committee size |
| session id `sid` | 32-byte session id derived from the committee (see §6.1) |
| data block `b_i^k = (sid, i, k, h_i^{k-1}, x)` | the existing `Header` struct with `Option` fields added (`signature: Option<Signature>` = `None`, `sid: Option<Digest>` = `Some`); predecessor = `parent_cert.header_digest` with empty votes (pure pointer) — §3.1 |
| `H("data-block" ‖ enc(b))` | blake3 over domain tag `b"data-block"` ‖ sid ‖ bincode(b) — domain-tag helper shared with later phases |
| `⟨publish, b⟩` | existing `PrimaryMessage::Header(h, false)` broadcast to all primaries (transport §6.2) |
| `⟨ack, a, k, h⟩` | appended `PrimaryMessage::VantageAck(Ack)`, unsigned, broadcast all-to-all |
| `⟨request, h⟩` / `⟨serve, h, b⟩` | existing `HeadersRequest(digests, requestor)` / `Header(h, true)` — the sync flag *is* the paper's publish/serve type distinction; vantage handler semantics differ (§3.3) |
| `BlockOK` | deterministic, state-independent: canonical encoding, payload-size cap, per-block digest-count cap (config), correct sid |
| valid lane prefix | chain check: one author index, consecutive heights, matching predecessor hashes back to genesis, every block BlockOK |

**D1 (decision, flagged for user review): two-tier payload.** The paper's `x` is carried
as worker-batch digests (Autobahn's existing worker layer disseminates the batch bytes).
`DirectPub_j(a,k,h)` is implemented as: (i) every block of the prefix through `h` was
received via `publish` directly from `p_a` on the authenticated channel (sender ==
encoded author), (ii) the chain and BlockOK checks pass, and (iii) the referenced worker
batches for every block in the prefix are present in the local workers' stores — reusing
Autobahn's existing payload-availability gate that already precedes voting. Consequence:
an ACK asserts possession of chain *and* payload bytes, so an acked prefix is fully
servable — this is the paper's intent (ack ⇒ retrievability). Batch repair between
workers reuses the existing worker synchronizer unchanged.

## 2. Normative rules (transcribed; the tests in §7 cite these by number)

**N1 — publication.** A correct party extends only its current lane frontier: next block
has height `k = frontier+1`, predecessor = frontier digest. On creating `b_i^k`: store it,
process it as a direct publication from self (self-delivery counts first-hand), broadcast
`publish(b_i^k)` to all. A recipient treats a block as *author publication* only when the
channel sender and the block's encoded author are both `p_a`. Anyone else's `publish` of
`p_a`'s block establishes nothing (may still be cached as bytes, without provenance).

**N2 — publish validation.** On direct `publish(b)` from `p_a`: accept iff `b` is
canonical, `sid` correct, encoded author = `p_a`, payload within caps, BlockOK. Then store
and set the direct-publication mark for `(a,k,hash(b))`. A later `publish` may *upgrade*
bytes previously cached via repair (adds the provenance mark).

**N3 — ACK trigger.** When `DirectPub_i(a,k,h)` (per D1) first becomes true and `(a,k,h)`
has not been acked before: retain the valid lane prefix through `h` (N8), add to `acked`,
broadcast unsigned `ack(a,k,h)` to all (including self-delivery). At most one ack per
tuple ever. A prefix obtained only through repair is held and served but **never acked**.

**N4 — first-hand counting.** Count an `ack(a,k,h)` only from its own channel sender,
once per (sender, exact tuple). No relaying, no transferable evidence. Definitions:
*q-available* at a party ⇔ acks from ≥ q distinct parties counted for the exact tuple;
*locally available* ⇔ holds the valid lane prefix ∨ (f+1)-available; *AuthorOK_i(a,k,h)*
⇔ DirectPub_i(a,k,h) ∨ (f+1)-available. For a manifest (Phase 4), each predicate must
hold per entry; expose per-reference queries now.

**N5 — C/T candidate registers (feeds Phase-4 propose).** Maintain deterministically from
first-hand state, per author `a`:
- `c_candidate(a)` = newest reference with DirectPub_i **and** ≥ 2f+1 counted acks;
- `t_candidate(a)` = newest directly published lane tip whose valid prefix **strictly
  contains** `c_candidate(a)` (an author with no C entry is anchored at genesis: any
  directly published tip qualifies).
*Newest* = greatest verified lane height, ties broken by lexicographically smallest hash.
Fork rule: the C entry pins the kept branch below itself; a directly published tip on any
other branch is never a T candidate. (Registers only in Phase 3 — manifest assembly is
Phase 4.)

**N6 — authorized repair (Authorize walk).** `Authorize(a,k,h)` is the only fetch entry
point (Phase 4/6 call it for proposal/completion/seal/anchor references; tests call it
directly). Semantics per the paper's algorithm:
- add `(a,k,h)` to `authorized`;
- if a cached block `b = (sid,a,k,h',x)` matches exactly — `hash(b)=h`, BlockOK, size
  caps, and (`k>1` ∨ `h'` = genesis hash) — recurse `Authorize(a,k-1,h')`;
- else send `request(h)` once to **every** party, recording `(peer,h)` in `requested`
  (at most one request per (peer, hash), ever — reliable channels make retransmission
  unnecessary; **no retry timers on this path**).
- On `serve(h,b)` for a requested `h`: if `b` canonical, `hash(b)=h`, size caps, BlockOK —
  cache as repaired data **without** provenance mark, *coordinate-independent* (cache even
  if encoded author/height differ from the currently authorized coordinate; only an exact
  coordinate match advances the walk). Failed hash/body check: no state change, hash not
  marked obtained.
- Whenever a cached block matches an authorized exact coordinate (arriving via publish
  *or* serve), the walk advances (recursion above).
- When a walk verifies a prefix through genesis: retain every block in it (N8) and
  try-serve their hashes (N7).

**N7 — serving.** On direct `request(h)` from `p_j`: record `(j,h)` in `pendingReq` if
not already answered — **even when the block is not held yet** — then try-serve. On a
block becoming retained: try-serve its hash. Try-serve: if a retained block matches `h`,
answer each pending `(j,h)` not yet answered with `serve(h,b)`, marking `(j,h)` answered
(at most one answer per requester–hash pair, ever).

**N8 — retention.** Retain permanently (hold + serve indefinitely): (i) every prefix
acked (N3); (ii) every prefix whose local holding a rule relies on in a local-availability
/ AuthorOK / tip-anchoring check; (iii) every prefix obtained by request for such a check,
even when the prompting window has closed by arrival; (iv) hash-correct repair bodies stay
cached so a later exact coordinate can consume them. Local sealing/output is **not** a
discard signal. No GC in the model (checkpointing out of scope) — but see §6.3 for the
practical memory note. Retained worker batches follow their retaining block (D1).

**N9 — session hygiene.** Every vantage message carries/derives sid; wrong-session
messages are rejected before storing or counting. Malformed (non-canonical, over caps)
messages are never counted and change no state.

## 3. Module plan — adapt-in-place inventory

New task code lives in `primary/src/vantage/` (module `vantage`, gated by
`Protocol::Vantage` at spawn time). **User decision (2026-07-23): wire byte-compat with
upstream is NOT required — shared structs may gain `Option` fields.** The whole cluster
always runs one rebuilt binary, so changing `Header`'s encoding is safe; what must stay
intact is Autobahn *behavior* (invariant 4) — mechanical adaptations of Autobahn code to
the new field shapes (e.g. wrapping in `Some`) are fine, semantic changes are not, and
the §7 fab regression re-validates both Autobahn assemblies. Shared infra reused as-is:
`network::{ReliableSender, Receiver}`, `store::Store` (`write`/`read`/`notify_read`),
`config::{Committee, Parameters}`, `committee.quorum_threshold()` (=2f+1) and
`validity_threshold()` (=f+1), the whole worker crate, and the `metrics` crate from
Phase 2.

**3.1 Block = the existing `Header` with `Option` fields (user-ratified).** No parallel
`DataBlock` struct. Changes to `Header` (messages.rs:420):
- `signature: Signature` → `signature: Option<Signature>` — `None` on the vantage path;
  Autobahn constructors/`verify` adapt mechanically (`Some(sig)`, `ensure!` presence
  before verifying).
- new `sid: Option<Digest>` field — `Some` on vantage (§6.1), `None` on Autobahn paths.
- `Hash for Header` (messages.rs:568): fold each new `Option` injectively — a presence
  byte (`0`/`1`) then the content when `Some` — appended after the existing folds so the
  fold order of current fields is untouched. (`signature` stays outside the digest, as
  today.)
- chain link: **reuse `parent_cert.header_digest` as the predecessor pointer** with
  `parent_cert = Certificate { author, header_digest: prev, height: k-1, votes: vec![] }`
  — an empty-votes certificate acting as a pure pointer. It is never `Certificate::verify`d
  on the vantage path; N2's chain check is `parent_cert.height + 1 == height` plus digest
  match — the same shape `process_header` enforces at core.rs:325.
- consensus metadata (`consensus_messages`, `num_active_instances`, `special`):
  empty/`0`/`false` on vantage.
- constructor `Header::new_vantage(author, height, payload, prev_digest, sid)` — no
  signature service; sets `id = digest()`.
- `Ack { author: PublicKey, height: Height, digest: Digest, sender: PublicKey }` —
  unsigned; `sender` per D4.
- `BlockOK` = canonical bincode, `sid == Some(our sid)`, `payload.len() <=
  max_block_payload` (new `Parameters` field, default = worker count ×4; validated like
  the other int params by config.py), height ≥ 1, **and no smuggled Autobahn artifacts**:
  `signature.is_none()`, `parent_cert.votes.is_empty()`, consensus metadata empty.

**3.2 `vantage/lanes.rs` — lane state + ACK trigger (N1–N5).** One task
(`LaneManager`) owning:
- per-author block index: `HashMap<PublicKey, BTreeMap<Height, HashMap<Digest,
  BlockEntry>>>` where `BlockEntry { block, direct: bool, repaired: bool, retained:
  bool, payload_ok: bool, direct_prefix_ok: bool }` — forks representable (multiple
  digests per height).
- `direct_prefix_ok` computed incrementally (direct mark + parent's flag), the D1 payload
  gate via the primary-store probe Autobahn's `Synchronizer::missing_payload`
  (synchronizer.rs:53) uses: key `[batch_digest ‖ worker_id LE]`, written by
  `PayloadReceiver` (payload_receiver.rs:24) when our workers report `OthersBatch`.
  Missing batches: send `PrimaryWorkerMessage::Synchronize(missing, block.author)` to our
  own workers (exactly what `HeaderWaiter::SyncBatches` does, header_waiter.rs:165) and
  park the block on `store.notify_read` of the missing keys — the worker
  `Synchronizer`'s existing request/retry machinery (worker/src/synchronizer.rs:101)
  fills them; on wake, re-run the ack check (N3).
- ack sets per exact tuple: `HashMap<(PublicKey, Height, Digest), HashSet<PublicKey>>`
  (first-hand: keyed by `sender`, insert-once — N4) + own-ack dedup set (`acked`).
- registers `c_candidate`/`t_candidate` per author (N5), updated on every ack-set or
  direct-mark change; exposes the §4 queries over a small `watch`/mpsc API.
- own-lane publication: a `VantageProposer` arm (pattern: proposer.rs:130) driven by
  `header_size`/`max_header_delay` and `rx_our_digests` — but **no certificate wait**:
  height advances immediately on self-creation (lanes are ack-independent, unlike
  Autobahn's `last_parent` gate at proposer.rs:241). Self-delivery counts (N1): process
  own block as direct publication and count own ack.

**3.3 `vantage/repair.rs` — Authorize walk + serving (N6–N8).** One task
(`Repairer`) owning `authorized`, `requested: HashSet<(PublicKey /*peer*/, Digest)>`,
`pendingReq: HashSet<(PublicKey, Digest)>`, `answered: HashSet<(PublicKey, Digest)>`,
and the coordinate-independent repair cache `HashMap<Digest, Header>`. Shape follows
`HeaderWaiter`+`Helper` (request-out / answer-in split) with the paper's differences:
requests fan out to **all** other primaries once (no `sync_retry_delay`, no
`TIMER_RESOLUTION` poll loop, no `lucky_broadcast` escalation — D2); serving answers
**retained** blocks only and keeps `pendingReq` for blocks not yet held (Autobahn's
`Helper` drops unknown digests, helper.rs:77 — we must not); answer-once per
(requester, hash). Walk advancement and genesis-complete retention per N6/N8; block
bodies also `store.write(block_digest, bincode)` for crash-tolerant serving parity with
Autobahn (in-memory sets are authoritative; store is body storage, mirroring how
`process_header` stores at core.rs:378).

**3.4 Wiring.** `Primary::spawn` gains a `Protocol` parameter (from
`parameters.protocol`): for `Vantage` it spawns `LaneManager` + `Repairer` +
the existing worker-facing receivers (`WorkerReceiverHandler` unchanged — `OurBatch` →
vantage proposer arm, `OthersBatch` → `PayloadReceiver` as today) and does **not** spawn
`Core`/`Proposer`/`HeaderWaiter`/`Helper`/consensus tasks; for the two Autobahn variants
the spawn set is identical to today. `PrimaryReceiverHandler::dispatch` (primary.rs:288)
routes per assembly: on vantage, `Header`/`HeadersRequest`/`VantageAck` go to
`LaneManager`/`Repairer`; on Autobahn assemblies dispatch is unchanged. `Cleanup`/GC is
**not** wired on the vantage path (N8: no discard); `gc_depth` unused here.

**3.5 Deleted on this path:** nothing in Phase 3 (Autobahn paths stay live for the other
two protocols). Dead-code deletion inside the vantage module itself happens in the
simplification pass.

## 4. API surface exposed to Phase 4 (crate-internal, primary)

## 4. API surface exposed to Phase 4 (crate-internal, primary)

- `authorize(a, k, h)` (N6 entry), idempotent.
- Queries, all first-hand-deterministic: `is_q_available(ref, q)` (q ∈ {f+1, 2f+1}),
  `author_ok(ref)`, `locally_available(ref)`, `c_candidate(author)`,
  `t_candidate(author)`, `holds_prefix(ref)`.
- Event stream (channel) for "reference became 2f+1-available" and "prefix verified
  through genesis" — Phase 4's proposal/echo gates subscribe; tests consume it now.

## 5. Wire changes

One variant **appended after** `ProposalHeadersRequest` (the current last variant) of
`PrimaryMessage` (primary.rs:42):

```rust
    // ... existing 11 variants unchanged in order ...
    VantageAck(Ack),
```

publish/serve/request reuse the existing vocabulary: `Header(h, false)` = publish,
`Header(h, true)` = serve (the sync flag is the paper's "distinctly typed" serve — a
served block never carries provenance, N2/N6), `HeadersRequest(digests, requestor)` =
request (requester identity per D4). A node runs exactly one protocol, so dispatch is
unambiguous: the vantage assembly routes these variants to `LaneManager`/`Repairer`
instead of Core/Helper. The `Header` struct's encoding changes (§3.1 `Option` fields) —
permitted per the user's 2026-07-23 decision; there is no cross-version interop, the
cluster always rebuilds whole. `PrimaryWorkerMessage`/`WorkerPrimaryMessage` unchanged
beyond Phase 2's appended `Committed`. All vantage messages unsigned. Transport:
ReliableSender (per-message ack+retransmit) for publish/ack/request/serve — this carries
the paper's reliable-channel assumption that licenses D2's no-retry rule.

## 6. Implementation notes

**6.1 sid + genesis.** `sid = blake3(b"vantage-sid" ‖ canonical committee encoding)`
(committee already binds membership + epoch). Genesis digest = `blake3(b"data-block" ‖
sid ‖ b"genesis")`; height-0 is implicit — no genesis block on the wire. `Parameters`
gains only `max_block_payload` (§3.1). Deviation (record in PHASE3-NOTES.md): the paper
puts sid on *every* message; we carry it explicitly only inside blocks
(`Header.sid: Option<Digest>`) — acks/requests/serves bind the session transitively
through the digest (which hashes sid). A foreign-session ack accumulates under a tuple that is never authorized or
counted for any local decision; the residual cost is bounded-set memory, covered by the
§6.3 exposure note.

**6.2 Transport.** publish/ack/request/serve use the reliable sender (per-message
ack+retransmit) — the paper's reliable-channel assumption is what licenses "no
application-level retries" (N6). The vantage path arms **no** sync-retry timers; the
Autobahn `sync_retry_delay` machinery stays untouched on its own paths.

**6.3 Memory.** Retention is unbounded by design (paper model). Sets (`requested`,
`pendingReq`, `answered`, per-tuple ack sets) are bounded per honest peer but not against
Byzantine spam of *distinct* tuples; cap enforcement is a Phase-6 concern (Byzantine
suite) — Phase 3 documents the exposure in PHASE3-NOTES.md and moves on.

**6.4 Metrics.** Counters via the Phase-2 `metrics` crate: blocks published/received,
acks sent/received, repairs requested/served, retained bytes. Names prefixed `vantage_`.
No new scrape plumbing.

## 7. Tests (the gate)

Unit/integration in the style the repo already uses (tokio, common.rs fixtures), driving
the Phase-3 components directly (no full node). Every test cites the rule it checks:

- **Chain integrity (N1/N2/N9):** reject wrong sid, non-canonical bytes, oversized
  payload, wrong predecessor, non-consecutive height, author≠sender; accept and mark a
  valid direct publish; a relayed publish (sender≠author) yields cache without provenance;
  later authentic publish upgrades it (N2).
- **ACK discipline (N3/N4):** ack fires exactly once per tuple, only after the *entire*
  prefix is directly published and worker batches present (D1 — withhold one batch ⇒ no
  ack until it arrives); repaired-only prefix never acked; per-sender dedup (same ack
  twice counts once); q-available thresholds at f+1/2f+1 exact boundaries.
- **Registers (N5):** height/lex-tiebreak newest selection; fork: two branches acked by
  disjoint senders — C pins one branch, T never from the other branch; author without C ⇒
  any direct tip qualifies for T; strict-containment (T=C height) excluded.
- **Repair (N6/N7):** recursive walk to genesis over served blocks; request fan-out =
  all parties, exactly once per (peer,hash) across repeated Authorize calls;
  false-coordinate serve is cached but does not advance the walk, and a later exact
  reference consumes the cached body without a new request; corrupted serve (hash
  mismatch) ignored, hash stays un-obtained; pendingReq recorded before possession and
  answered exactly once on retention (N7 answer-once).
- **Retention (N8):** acked ⇒ served to a later requester; prefix fetched for an
  AuthorOK check arriving "late" is still retained and served; no discard on any local
  event.
- **Autobahn regression:** both Autobahn assemblies compile and their test suites pass
  after mechanical `Option` adaptations only (`Some(signature)` wrapping, `sid: None` —
  no semantic edits); one `node local-benchmark` run (optimistic, all-zero, 240k, 60 s)
  reproduces Phase-2 throughput/count (latency is not comparable in-process — documented
  §8 limitation; vantage tasks never spawned on those assemblies).

Gate: all of the above green; simplification pass (dedupe, delete dead vantage-path
scaffolding, re-run); **two consecutive clean adversarial audit passes** (protocol-critical
phase) checking code against §2's rules; PHASE3-NOTES.md lists every deviation.

## 8. Decisions flagged for user review

- **D1** two-tier payload / ack gated on worker-batch possession (§1). Corollary: batch
  *bytes* may arrive from non-author workers via the existing worker retry path;
  provenance (DirectPub) applies to blocks, possession of payload is content-addressed.
- **D2** repair fan-out to all parties with pendingReq/answered bookkeeping replaces the
  Autobahn waiter's timer-based re-request on the vantage path (§6.2).
- **D3** node assembly: `vantage` keeps bailing in `main.rs` until Phase 4/5; Phase 3 is
  library + tests only (§0).
- **D4** sender identity: vantage messages carry an explicit sender/requester field
  (`Ack.sender`, `VantageRequest` requester), trusted the same way Autobahn trusts
  `HeadersRequest`'s `requestor` today — i.e. the artifact's TCP links are unauthenticated
  and first-hand counting relies on the declared field. The paper's model assumes
  authenticated channels; per-link MACs (or TLS) remain the parked Phase-7 decision. Until
  then the Byzantine suite (Phase 6) must not claim defense against sender spoofing.
  Recorded as a known model gap in PHASE3-NOTES.md.
- **D5** sid carried only inside blocks (`Header.sid`, §6.1).
- **D6 (user-ratified 2026-07-23)** vantage blocks reuse the `Header` struct with
  `Option` fields; upstream wire byte-compat dropped; publish/serve/request reuse the
  existing `Header(h, sync)`/`HeadersRequest` message vocabulary (§§3.1, 5).
