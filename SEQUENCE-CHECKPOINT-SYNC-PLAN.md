# Vantage sequence-checkpoint state-sync plan

**Status: Phase A implemented (record-only shadow mode). Phases B/C specified below and
not yet proved. No protocol proof audit has been run.**

Phase ordering matters and is easy to misread, so it is stated once here:

- **Phase A** builds the local chain and nothing else. It cannot fix a straggler.
- **Phase A.1** repairs observability and harness configuration so Phase A's gate can
  actually be scored on AWS.
- **Phase B** announces, fetches, and verifies. It **deliberately installs nothing**, so
  it *also* cannot make a lagging node recover. Its purpose is to prove that remote
  sequence state can be certified and downloaded correctly before anything is allowed to
  mutate the cursor.
- **Phase C** is the first phase capable of making a delayed node rejoin, because it is
  the first that installs.

Anyone hoping this work fixes the n=100 straggler tail is waiting on Phase C, not B.

## 0. Decision

Add a hash-chained sequence log and a first-hand `f+1` checkpoint announcement rule.
A party that directly receives the same `(view, sequence_head)` from `f+1` distinct
committee members may use that head as a recovery anchor. At least one matching
announcer is correct. Correct parties announce a head only after their output cursor has
terminally advanced through that view and while they retain everything required to serve
the corresponding state.

This is state transfer for finalized history. It is deliberately separate from live AGB
and does not reconstruct historical ECHO, READY, ACK, WISH, resolution, or control-log
quorums. After validating the announced sequence chain, the recovering party downloads
the exact ordered output and installs it directly.

The mechanism is signature-free:

- checkpoint announcements count only when received first-hand over an authenticated
  channel from their encoded sender;
- an announcement is not a transferable certificate and is never forwarded as evidence;
- hashes bind sequence records and downloaded content, but do not establish authorship;
- no signatures, threshold signatures, PKI, or transferable MAC vectors are introduced.

## 1. Motivation

Vantage currently has three different recovery units:

1. data blocks are hash-linked per author and repaired by digest;
2. digest-named AGB statements can fetch a proposal body, but still require first-hand
   ECHO/READY statements to reconstruct a quorum;
3. reconnect replay replays retained one-shot messages from a coarse WISH-watermark
   floor.

That is sufficient in the paper's indefinite-retention/fair-delivery model, but it is a
poor state-transfer interface. A party a few seconds behind may process thousands of old
statements, arm a burst of old-view timers, recursively repair lane prefixes, and still
remain blocked at the serial output cursor. If history has fallen below an outbox or
component GC floor, replay cannot recover it at all.

A correct party that has already terminally sequenced view `v` knows a much stronger and
smaller fact: the exact common output prefix through `v`. Receiving the same commitment
to that prefix from `f+1` parties gives the recovering party a local correct witness.
The state-transfer path can therefore bypass historical consensus evidence without
changing how new views reach agreement.

## 2. Scope and non-goals

### In scope

- a deterministic per-view sequence record and hash chain;
- first-hand checkpoint announcements and an `f+1` matching-head collector;
- bounded, chunked transfer of sequence records and per-view output deltas;
- reuse or extension of block and worker-payload synchronization;
- atomic installation into the cursor and its per-author watermarks;
- retention, serving, scheduling, metrics, dashboard panels, tests, and rollout gates;
- a protocol proof covering checkpoint soundness, prefix compatibility, and recovery
  liveness.

### Not in scope for the first implementation

- changing AGB completion, sealing, resolution, or control-log rules;
- using checkpoints to justify a live-view ECHO/READY or availability claim;
- forwarding another party's announcement as evidence;
- application snapshots. Without a snapshot, a recovering party must still download and
  execute every ordered block it has not executed locally;
- replacing the existing author-lane hash chain;
- claiming bounded storage before retention and snapshot rules have their own proof.

## 3. Terminology and eligibility

Use **sequence checkpoint**, not **commit certificate**. In the paper, AGB completion
fixes a proposal and makes its core irrevocable, but a completed view may still have an
open tip. Such a view is not a state-transfer boundary.

A view `v` is checkpoint-eligible locally only when all of the following hold:

1. the cursor has terminally processed `Full`, `Core`, or `Skip` for `v`;
2. `cursor.next_view == v + 1` or has advanced beyond it;
3. every output block assigned by `v` has been materialized and delivered locally;
4. the sequence record, output-delta commitment, outcome body, block headers, and worker
   payloads required by the advertised retention contract remain serveable;
5. every earlier view is already represented by the local sequence chain.

The record must be created when the cursor advances, not when AGB completes or seals in
isolation. The cursor may output a completed core before the tip is terminally sealed;
those early core blocks belong to the eventual output delta for that same view and must
be accumulated until the terminal advance.

## 4. Canonical objects

All encodings are versioned, canonical bincode encodings and domain-separated by the
Vantage session id. Hash functions below use the existing Blake3-backed `Digest`.

### 4.1 Per-view output delta

For view `v`, `delta_v` is the exact ordered list of block digests newly emitted by the
cursor while processing `v`, after cross-view and within-view deduplication. It includes:

- core blocks emitted while `v` was still open;
- later tip blocks if `v` terminally becomes `Full`;
- no additional blocks for `Core` after an already-emitted core;
- an empty list for `Skip`.

Use an incremental item chain so arbitrarily large deltas can be streamed without
constructing a Merkle tree or buffering one oversized wire frame:

```text
D_v[0]   = H("vantage-sequence-delta" || sid || v || 0)
D_v[i+1] = H("vantage-sequence-item"  || sid || v || i || D_v[i]
             || delta_v[i])
```

The final commitment is `(delta_len, delta_head)`. Transfer chunks carry a starting
index and consecutive digests; the receiver verifies the item chain incrementally.

### 4.2 Terminal outcome body

Define a canonical state-transfer-only outcome body:

```rust
enum SequenceOutcome {
    Full { c: Manifest, t: Manifest },
    Core { c: Manifest },
    Skip,
}
```

Its digest is:

```text
outcome_digest = H("vantage-sequence-outcome" || sid || v
                   || canonical_encode(outcome))
```

This deliberately commits the terminal result, not merely the proposal body. A proposal
digest alone cannot distinguish `Full` from `Core`, and a silent `Skip` has no proposal.
The original proposal digest may be retained as diagnostic metadata, but recovery safety
must not depend on obtaining historical proposal traffic.

### 4.3 Sequence record and head

```rust
struct SequenceRecord {
    version: u16,
    view: View,
    previous_head: Digest,
    outcome_digest: Digest,
    delta_len: u64,
    delta_head: Digest,
}
```

```text
H_v = H("vantage-sequence-record" || sid || canonical_encode(record_v))
```

There is exactly one record for every terminally processed view, including `Skip`. The
session defines a constant `H_0` for the genesis sequence head. A valid chain for target
`(v, H_v)` contains every record from genesis, or from a locally trusted earlier head,
through `v` with no missing view.

### 4.4 Announcement

```rust
struct SequenceAnnouncement {
    version: u16,
    view: View,
    head: Digest,
    serve_floor: View,
    sender: PublicKey,
}
```

`serve_floor` is informational and cannot strengthen the claim. The receiver derives
the authoritative sender identity from the authenticated connection and rejects a
payload whose `sender` differs, as on the other Vantage first-hand paths.

Use deterministic checkpoint boundaries initially, for example every configured `K`
views. Every party that has passed boundary `b` continues announcing `(b, H_b)` until it
can announce a later boundary. Fixed boundaries make `f+1` exact matches likely even
when correct cursors differ by several current views. A current non-boundary head may be
piggybacked later as an optimization, but is not needed for version 1.

## 5. Why `f+1` is sufficient

For `n = 3f + 1`, a set of `f+1` distinct parties contains at least one correct party.
The recovery rule counts an announcement only first-hand and once per sender for a
given checkpoint view. A correct party announces `(v, H_v)` only for its actual,
terminal, serveable output prefix.

Therefore, if a recovering correct party counts `f+1` identical announcements for
`(v, H_v)`, one matching announcement came directly from a correct party. Collision
resistance binds `H_v` to the exact record chain, terminal outcomes, and ordered output
deltas that correct party sequenced. Existing Vantage safety ensures that no correct
party terminally sequences a conflicting prefix.

This argument is local. The receiver cannot hand the `f+1` messages to somebody else as
a certificate. A third party must collect its own first-hand `f+1` announcements.

The proof must explicitly establish:

1. **determinism:** correct parties that terminally process through `v` derive the same
   `H_v`;
2. **soundness:** an adopted `H_v` is the head of a sequence produced by a correct party;
3. **prefix compatibility:** the adopted sequence extends every output already delivered
   by the recovering correct party;
4. **content binding:** a different record, outcome, or output delta cannot validate
   against the adopted head without a hash collision;
5. **availability:** at least one matching correct announcer retains and eventually
   serves the advertised state;
6. **no double output:** installation never re-delivers a locally emitted block or skips
   a not-yet-delivered block;
7. **non-interference:** adopting finalized history does not create a live-view vote,
   availability acknowledgment, resolution stance, or control-log message.

## 6. Wire protocol

Append new variants to `PrimaryMessage`; do not reorder existing variants.

```rust
VantageSequenceAnnounce(SequenceAnnouncement)
VantageSequenceRequest(SequenceRequest)
VantageSequenceRecords(SequenceRecordChunk)
VantageSequenceDeltaRequest(SequenceDeltaRequest)
VantageSequenceDelta(SequenceDeltaChunk)
VantageSequenceOutcomeRequest(SequenceOutcomeRequest)
VantageSequenceOutcome(SequenceOutcomeServe)
VantageSequenceUnavailable(SequenceUnavailable)
```

Required request/response binding fields:

- requester and authenticated sender;
- target `(view, head)`;
- requested start view or `(view, delta_index)`;
- monotonically generated local transfer id;
- bounded item count and byte count;
- explicit completion marker and authoritative serve floor.

Responses that do not match an active transfer id, target head, expected view/index, and
authenticated peer are ignored. Duplicate valid chunks are idempotent. Future chunks
may be parked only within a configured byte and item cap.

The first implementation should use a dedicated state-sync sender and bounded ingress,
modeled on the existing replay sender, rather than enqueueing large chunks through the
main Vantage inbound channel.

### 6.1 Dedicated transport, resolved

This is not an optimization. The mechanism exists to relieve a node whose **main inbound
queue is already saturated** -- that is the measured n=100 failure -- so routing recovery
traffic through that same queue would deepen the exact congestion it is meant to drain,
and a large chunk arriving as one queue item would evict live consensus messages behind
it. The separation is therefore a correctness requirement of the design, not tuning.

**Egress.** A dedicated sender task per node, modeled on `wire::spawn_resume_sender`
(Mechanism A), owning its own `SimpleSender` and its own bounded command channel. It runs
OFF the `VantageCore` run loop for the same reason the resume sender does: serving a
range must never occupy the turn that live consensus needs. `VantageCore` hands it
`(peer, request)` descriptors, never materialized chunk bytes, so no large allocation
crosses the loop boundary.

**Ingress.** A dedicated bounded channel (`sequence_sync_inbound_capacity`, default
**256** frames) separate from the main Vantage inbound channel. Overflow **drops the
newest frame and increments `vantage_sequence_sync_dropped_total{reason="ingress_full"}`**
rather than blocking the receiver: state-sync responses are idempotent and re-requestable,
so dropping one costs a retry, while blocking would propagate backpressure into the
transport and stall live traffic. This is the same reasoning that makes `SimpleSender`
best-effort, and the drain hazard in `network/src/simple_sender.rs` is the cautionary
case -- an unbounded or blocking recovery path is worse than a lossy one.

**Budgets**, all per node and enforced in the requester before a request is emitted:

| knob | default | why |
| --- | --- | --- |
| `sequence_sync_chunk_records` | 256 records | ~24 KB/frame at 96 B/record |
| `sequence_sync_chunk_digests` | 1024 digests | 32 KB/frame, below the 64 KB frame norm |
| `sequence_sync_max_in_flight_bytes` | 4 MiB | ~2 frames per source at 3 sources |
| `sequence_sync_max_sources` | 3 | `f+1` at the smallest committee this targets |
| `sequence_sync_apply_items_per_tick` | 512 | bounds one `VantageCore` turn |
| `sequence_sync_inbound_capacity` | 256 | above |

**Never a whole-range walk in one turn.** Verification and installation both yield after
`sequence_sync_apply_items_per_tick` items, so a 10,000-view catch-up cannot monopolize
the loop it is trying to unblock.

### 6.2 Phase B decisions, resolved

The plan previously left these open; Phase B cannot be implemented without them.

1. **Announcement cadence.** A standalone periodic message on a timer,
   `sequence_announce_period_ms`, default **2000 ms**, broadcast only when the local
   boundary has advanced OR `sequence_announce_repeat_ms` (default **10000 ms**) has
   elapsed since the last send for the current boundary. Repetition is required, not
   optional: a node that starts late must be able to collect `f+1` announcements for a
   boundary everyone else passed before it existed, and a strictly edge-triggered
   announcement would never reach it. Cost is `n` small frames per period -- at n=100 and
   2 s that is 50 frames/s fleet-wide, negligible beside the measured ~19 cars/s/node.
   Piggybacking is deferred: it entangles this with live-path message accounting, and the
   paper's message-complexity claims must stay unchanged while the feature is off.

2. **Retention.** In-memory only for Phases B and C, matching §16's version-1 choice.
   Records are ~96 B, so a 100,000-view session is ~10 MB -- acceptable, and it keeps
   restart semantics trivially safe: a restarted process starts from `H_0` and re-derives,
   because §9 forbids claiming a pre-restart head that has not been re-materialized.
   RocksDB persistence is deliberately NOT in this phase; it would make a restarted node
   able to announce a head it cannot serve, which is the one thing the availability
   argument may not lose.

3. **Chunk sizes.** As tabulated in §6.1. Records and digests are chunked by ITEM COUNT
   rather than bytes because both are fixed-width, so an item cap is an exact byte cap
   and needs no per-item measurement.

4. **Transfer timeout and backoff.** Per-request timeout `sequence_sync_request_timeout_ms`,
   default **5000 ms**. On timeout the request is re-issued to the NEXT source in the
   matching-announcer set, not the same one, with exponential backoff per source
   (200 ms doubling to a 2 s cap, matching `network`'s existing connect backoff so there
   is one retry idiom in the codebase). A source that times out twice consecutively is
   parked for `sequence_sync_source_park_ms` (default **30000 ms**).

5. **Source failover.** Request each outstanding chunk from up to
   `sequence_sync_max_sources` matching announcers CONCURRENTLY, accept the first valid
   copy, and cancel the rest. `f` of the `f+1` matching announcers may withhold or corrupt
   every response, so serial failover would multiply the worst-case recovery time by `f`;
   concurrent requests bound it by the one correct announcer's latency at a bandwidth cost
   of at most `max_sources`x. Corrupt chunks are counted per source
   (`vantage_sequence_sync_invalid_chunks_total{reason}`) and a source that returns two
   invalid chunks for a target is dropped for that target.

## 7. Recovery state machine

One `SequenceSync` instance is owned by `VantageCore` and has at most one installation
target at a time.

```text
Idle
  -> CollectingHeads
  -> FetchingRecords
  -> FetchingOutcomes
  -> FetchingDeltas
  -> FetchingBlocksAndPayloads
  -> ReadyToInstall
  -> Installing
  -> Idle
```

### 7.1 Head collection

- Maintain at most one first announcement per `(checkpoint_view, sender)`; a later
  conflicting head from the same sender is recorded as equivocation and ignored for
  counting.
- Bound future checkpoint views relative to the highest first-hand WISH/current fleet
  head already observed, and cap the number of retained candidate boundaries.
- A candidate becomes certified locally when `f+1` distinct committee senders announce
  the identical `(view, head)`.
- Choose the highest certified target above the locally installed sequence head whose
  records are advertised as available by at least one matching sender.
- A higher target may replace an active target only before installation, and only if
  doing so does not discard already verified reusable chunks without a configured
  benefit threshold.

### 7.2 Record synchronization

Request the lightweight `SequenceRecord` range from the local installed view plus one
through the target. Send requests to all `f+1` matching announcers, because as many as
`f` may withhold or send corrupt bytes. Accept the first valid copy of each chunk and
deduplicate the rest.

Verify all record links and terminal outcome/delta commitments through `H_v` before
executing any newly downloaded block. This makes a Byzantine peer's plausible prefix
harmless if it cannot complete the chain to the certified head.

### 7.3 Outcome and delta synchronization

For every verified record:

1. obtain a canonical `SequenceOutcome` body matching `outcome_digest`;
2. stream the output digests in index order and verify `(delta_len, delta_head)`;
3. check that a `Skip` has an empty delta;
4. check that the delta is exactly the deterministic cursor expansion of the downloaded
   outcome relative to the preceding installed per-author watermarks;
5. compare any already locally emitted partial delta for the current open view against
   the same prefix of the downloaded delta.

Step 4 is defense in depth and protects against implementation divergence: the certified
head already has a correct witness, but a local deterministic recomputation should still
reject a state-transfer encoding that does not match Vantage's output rules.

### 7.4 Block and payload synchronization

Authorize every digest in a verified delta for content recovery, but do not reinterpret a
state-transfer body as direct publication or create an ACK from it. Prefer sources in
this order:

1. matching correct-witness candidates that advertised the checkpoint;
2. the encoded author for direct lane-range publication;
3. known holders from availability claims;
4. ordinary staged repair fallback.

Add a chunked lane-range request as a follow-up optimization if existing digest repair
cannot drain the range without amplification. A range from a non-author is repair-only;
only the authenticated encoded author can establish direct-publication provenance.

Wait until every referenced header and all worker batches needed for materialization are
present before marking the target installable.

### 7.5 Atomic installation

Installation runs in the single `VantageCore` owner task:

1. re-check that the local installed head/base view has not changed incompatibly while
   the transfer was staged;
2. re-check the local partial delta, if any, against the downloaded prefix;
3. deliver not-yet-delivered headers in exact sequence-delta order, in bounded batches;
4. update the cursor output set, per-author `(height, digest)` watermarks, finalized
   sequence head, and `next_view = target_view + 1` as one logical transition;
5. retain live/future state above the target and GC obsolete AGB, timer, resolver,
   control-validation, digest-statement, and cursor-pending state at or below the target;
6. publish the newly serveable checkpoint only after the installation and local
   materialization complete.

If the ordinary cursor advances beyond the target before installation, abort the stale
transfer. If it advances partway toward the target, reuse verified records only after
revalidating them from the new local head.

## 8. Cursor changes

`Cursor` needs explicit per-view delta ownership:

- accumulate every digest passed to `emit` under the current `next_view`;
- retain that partial delta when a completed core is emitted while the view remains open;
- after `Full`, `Core`, or `Skip` terminally advances the view, emit a new internal
  effect such as:

```rust
Effect::SequenceFinalized {
    view: View,
    outcome: SequenceOutcome,
    output_delta: Vec<Digest>,
}
```

- clear the partial delta only after `VantageCore` has recorded the sequence record;
- expose a checked state-transfer installation method rather than allowing
  `SequenceSync` to mutate cursor maps directly.

The delta vector is an internal handoff, not one wire frame. `SequenceStore` converts it
to the incremental delta chain and chunked serving representation.

## 9. Storage and retention

Add `SequenceStore`, initially owned by `VantageCore`, containing:

- records indexed by view and head;
- terminal outcome bodies indexed by digest;
- ordered per-view output deltas or a compact on-disk representation;
- the current installed head and lowest fully serveable view;
- checkpoint-boundary metadata.

Correctness rule: never announce a checkpoint below the local serve floor or one whose
outcomes, deltas, headers, or required snapshot are not actually serveable.

Version 1 may retain sequence metadata indefinitely, matching the paper's current
retention model, while block storage follows the existing conservative holder floors.
Before adding bounded GC, define one of:

- a durable application snapshot at a checkpoint plus a commitment to it; or
- a proved rule ensuring at least `f+1` correct parties retain every advertised range.

Do not silently clamp a request below the serve floor and call the transfer complete.
Return `VantageSequenceUnavailable` with the authoritative floor; the requester then
tries another matching announcer or a newer snapshot-capable checkpoint.

Persistence across process restart is a separate switch. If enabled, record and delta
metadata must be written before announcement. If disabled, a restarted process must not
claim its pre-restart head until it has recovered and re-materialized it.

## 10. Scheduling and resource bounds

The mechanism is intended to relieve a congested straggler and must not compete without
bounds on the same queue that caused the lag.

- Use dedicated bounded queues for record/delta/block state-sync traffic.
- Apply byte and item budgets per peer and globally per tick.
- Keep at most one target and a bounded number of source peers active.
- Request from all matching announcers for liveness, but cancel redundant requests as
  soon as a valid chunk arrives.
- Prioritize the oldest missing sequence record/delta and the oldest cursor-blocking
  block before later history.
- Preserve bandwidth for current live consensus messages.
- Never perform a whole-range hash walk or installation in one `VantageCore` turn;
  yield after bounded work.
- Bound parked future chunks, malformed responses, candidate heads, transfer ids, and
  per-sender equivocation records.

Initial configuration fields, with conservative defaults chosen only after profiling:

```text
sequence_checkpoints                 bool
sequence_checkpoint_interval_views   u64
sequence_announce_period_ms           u64
sequence_sync_chunk_bytes             usize
sequence_sync_max_in_flight_bytes     usize
sequence_sync_apply_items_per_tick    usize
sequence_sync_candidate_windows       usize
```

The feature defaults off until the verify-only rollout is clean.

## 11. Metrics and dashboard

Add at least:

```text
vantage_sequence_head_view
vantage_sequence_checkpoint_announced_total
vantage_sequence_checkpoint_candidates
vantage_sequence_checkpoint_certified_total
vantage_sequence_checkpoint_equivocations_total
vantage_sequence_sync_target_view
vantage_sequence_sync_gap_views
vantage_sequence_sync_records_received_total
vantage_sequence_sync_delta_digests_received_total
vantage_sequence_sync_bytes_received_total{kind}
vantage_sequence_sync_invalid_chunks_total{reason}
vantage_sequence_sync_unavailable_total
vantage_sequence_sync_blocks_pending
vantage_sequence_sync_payloads_pending
vantage_sequence_sync_install_total{result}
vantage_sequence_sync_install_seconds
vantage_sequence_sync_queue_length{queue}
```

Dashboard panels:

- local cursor view, installed sequence-head view, and certified target view;
- sync gap and pending blocks/payloads by node;
- state-sync bytes/s split by records, outcomes, deltas, headers, and payloads;
- queue depths and invalid/unavailable responses;
- install duration and successful/aborted/failed installs.

## 12. Implementation map

### New modules

- `primary/src/vantage/sequence.rs`: canonical objects, hashing, `SequenceStore`, head
  collector, and unit tests.
- `primary/src/vantage/state_sync.rs`: requester/serve state machines, chunk validation,
  scheduling, and installation staging.

### Existing modules

- `primary/src/vantage/cursor.rs`: accumulate per-view deltas, emit
  `SequenceFinalized`, checked installation API.
- `primary/src/vantage/mod.rs`: exports and internal effects.
- `primary/src/vantage/node.rs`: ownership, ticks, inbound dispatch, execution, GC, and
  install orchestration.
- `primary/src/vantage/wire.rs`: dedicated bounded state-sync sender and chunk serving.
- `primary/src/primary.rs`: append-only wire variants and type names.
- `primary/src/vantage/lanes.rs`: deterministic expansion verification and optional
  range-sync helpers.
- `primary/src/vantage/repair.rs`: checkpoint-source preference without creating
  provenance.
- `primary/src/vantage/payload.rs`: materialization readiness for imported headers.
- `config/src/lib.rs`: flag and resource knobs.
- `metrics/src/metrics.rs`: counters, gauges, and histograms.
- `monitoring/grafana/grafana-dashboard.json`: state-sync panels.

## 13. Test plan

### Pure unit tests

- sequence record hashing is deterministic and domain/session separated;
- one changed outcome, delta item, index, view, or predecessor changes the head;
- delta chunks verify incrementally and reject gaps, overlap with different content,
  wrong indices, truncation, and oversized chunks;
- exactly `f+1` matching first-hand announcements certify; `f` do not;
- duplicate announcements count once; sender equivocation never counts twice;
- non-member and authenticated-sender mismatch are rejected;
- `Full`, `Core`, and `Skip` produce distinct outcome digests;
- early core emission plus later full seal produces one final ordered delta;
- partial local delta must be a prefix of the imported delta;
- request below serve floor returns unavailable, never a false completion;
- candidate and parked-chunk bounds hold under Byzantine future-view input.

### Protocol integration tests

- one node starts 3--5 seconds late, collects `f+1`, downloads through a checkpoint,
  sequences the backlog, and joins current output without replaying old AGB statements;
- a node has already emitted the core of an open view before installing a later
  checkpoint; no block is double-delivered;
- `f` Byzantine nodes announce one invalid head while correct nodes announce another;
  the invalid head never certifies;
- `f` Byzantine matching announcers withhold or corrupt every response while the one
  correct matching announcer serves; recovery completes;
- matching announcers are at different serve floors; the requester selects a complete
  source or reports the real gap;
- the sender dies between announcement and serve; another matching correct announcer
  completes the transfer;
- corrupted record, outcome, delta, header, and payload bodies change no installed
  state;
- ordinary live consensus continues while state sync is staged;
- a locally advancing cursor aborts or rebases a stale transfer safely;
- installation retains future-view messages and removes only obsolete state.

### Transport and queue regressions

- ACK/reconnect failure after frame receipt but before application dispatch cannot make
  an installed sequence head advance;
- a large state-sync response cannot enter as one unbounded main-queue item;
- chunk replay and reordering remain idempotent;
- a slow or disconnected source cannot occupy all state-sync slots;
- live consensus receives service throughout a maximum-sized recovery transfer.

## 14. Rollout and experiments

### Phase A: record-only shadow mode -- IMPLEMENTED

- build local sequence records and boundary heads;
- never announce, fetch, or install;
- compare heads across all healthy nodes at each common boundary;
- any correct-node mismatch is a release blocker and must be reduced to the first
  divergent view/delta item.

### Phase A.1: observability and harness repair -- IMPLEMENTED

Phase A shipped a gate that could not actually be scored. Fixed here:

- **the boundary HEAD is exported, not just its view.** Two divergent nodes are at
  IDENTICAL boundary heights by construction, so a view-only export makes the one failure
  this phase hunts invisible. The head is now the series identity
  (`vantage_sequence_boundary_head{head="<64 hex>"}`), with a truncated integer companion
  purely so a dashboard can draw a line;
- `docker-bench/sequence_check.py` decides the gate on full hex heads and exits nonzero on
  divergence -- and also on "nothing was compared", since a boundary reached by one node
  is not evidence of agreement;
- dashboard semantics corrected: `head_view == cursor_next_view - 1` exactly, so the panel
  plots the DIFFERENCE (which must be 0) rather than two curves that are permanently one
  apart; and delta BLOCK rate is no longer compared against `committed_transactions`,
  which counts transactions in another process and differs by the batch fan-out factor;
- `sequence_checkpoints` and `sequence_checkpoint_interval_views` are settable from
  wan-bench, which previously could not enable the feature on AWS at all;
- cursor regression added for early-core emission followed by a later terminal seal --
  the case that forces the per-view delta to be a field rather than a local.

Validation target: n=20 with one deliberately lagging validator is sufficient. The gate
needs several boundaries crossed by 2+ nodes, not scale.

### Phase B: announce and verify-only -- IMPLEMENTED

- broadcast/piggyback announcements and collect `f+1` heads;
- download and fully verify records/outcomes/deltas in the background;
- do not mutate cursor or output state;
- compare the verified remote result to ordinary local replay/cursor output.

The last bullet is the phase's actual deliverable and the gate for Phase C. Verifying a
transfer proves only that the served chain hashes to the head `f+1` peers announced; the
chain is self-referential, so one that is internally consistent but WRONG verifies
perfectly. The verified `(view, head)` is therefore retained in `sequence_verified_target`
and compared, in `record_sequence`, against the head ordinary execution derives for the
same view. That comparison is what separates "the peers agreed with each other" from "the
peers were right", and it is counted in
`vantage_sequence_verify_{match,mismatch}_total`. Mismatch must be zero.

A mismatch is deliberately non-fatal in Phase B: nothing is installed, so it cannot
corrupt state, and the run's value is the evidence it produces. Phase C must treat the
same signal as fatal to installation, since by then the bytes would be applied.

While a verified target is awaiting its comparison, no new transfer starts. Phase C
replaces that wait with an install.

### Phase C: guarded installation

Implementation order, smallest safe increment first:

1. **Staging and block fetch -- IMPLEMENTED.** A verified target names block *digests*, not
   blocks, so nothing can be installed until those blocks are local. `vantage::install`
   turns each view's outcome manifests into work for the existing `Repairer` and reports
   when a view's whole delta is in the cache.
   - The fetch instruction is the OUTCOME's manifests, not the delta: a `Manifest` entry is
     exactly a `BlockRef`, and `Repairer::authorize` already walks the named lane's prefix
     with bounded fan-out, a congestion window, and worker-payload sync on arrival. This is
     §16 decision 4's conservative choice -- reuse block repair, add no new bulk transport.
   - The delta is the completion test instead, checked against the block cache, so blocks
     arriving by ordinary dissemination count and nothing is fetched twice.
   - Announcers of the certified target seed repair's holder index (`note_holder`), which
     is the "checkpoint-source preference" half of that decision. Repair otherwise learns
     holders only from traffic it has already seen -- on a node that just fell behind,
     precisely the traffic it missed. One entry per lane, not per manifest entry.
   - Pacing: at most `sequence_install_window_views` (8) views in flight, and nothing
     admitted while `Repairer::pending_settle_len()` exceeds
     `sequence_install_settle_ceiling` (2048). The second gate matters more than the first.
     This mechanism runs on nodes that are already behind, which is exactly when repair is
     already loaded, so an installer indifferent to that backlog would recreate the regime
     that turned 60,262 received blocks into 612M settle calls.
   - Still installs nothing. Reaching "every view locally held" is observable
     (`vantage_sequence_install_ready_total`) and is the precondition the next step needs.
2. **Atomic cursor installation.** Prefix-check any locally emitted partial view, deliver
   missing blocks exactly once in sequence order, and move the cursor's watermarks, output
   set, open delta, sequence head and `next_view` together.
3. **Discard obsolete consensus work** at or below the installed view, preserving every
   future-view message.
4. **Race handling:** ordinary dissemination winning aborts the sync; a partially advanced
   cursor forces abort or rebase; a stale or incompatible target is never installed.

Then:

- enable installation only on deliberately delayed/restarted nodes;
- run deterministic in-process tests, then Docker without latency, mimic latency, and
  netem latency;
- at local scale use the largest stable committee (currently approximately 30 nodes),
  not an overloaded 40-node run as correctness evidence.

### Phase D: fleet A/B

- same binary/configuration except `sequence_checkpoints`;
- delayed-node or rolling-rejoin workload, plus the existing n=100 netem onset shape;
- score the fraction of delayed/saturated nodes that return to the fleet cursor and
  resume committing, not only the noisy final straggler count;
- compare core-queue occupancy, direct-prefix walk work, replay bytes, repair requests,
  worker-payload backlog, and time from reconnect to first/new sustained output.

Success requires all of the following:

- no correct-node sequence-head divergence;
- no checkpoint installation from fewer than `f+1` matching first-hand reports;
- exact output equality with ordinary execution;
- a delayed node can recover after old AGB messages are absent or deliberately not
  replayed;
- state-sync traffic does not pin the main Vantage inbound queue;
- recovery completes with `f` matching Byzantine sources withholding;
- no regression in fault-free steady-state throughput/latency outside measurement noise.

## 15. Paper work

The manuscript currently states that checkpointing and bounded GC are outside the model.
Do not revise that claim merely because record-only code exists. After Phase B proves
determinism and the protocol proof is audited, add a state-transfer subsection that:

1. defines the sequence record and checkpoint announcement;
2. states the local `f+1` recovery rule and its non-transferability;
3. proves checkpoint soundness and prefix-compatible installation;
4. states the retention/fair-serving assumption used for recovery liveness;
5. distinguishes downloading/executing an output suffix from snapshot installation;
6. updates the recovery section so one-shot replay is the near-history path and sequence
   checkpoints are the finalized-history fallback;
7. keeps live AGB safety, latency, and message-complexity claims unchanged unless the
   implementation adds announcements to the counted live protocol path.

Run two consecutive adversarial proof audits before enabling installation by default.

## 16. Open decisions before implementation

1. **Checkpoint interval:** fixed number of terminal views versus a byte/time hybrid.
2. **Announcement vehicle:** periodic standalone message, piggyback on existing traffic,
   or both with an idle fallback.
3. **Storage:** in-memory initial artifact versus RocksDB-backed records/deltas from the
   first implementation.
4. **Block transfer:** reuse staged digest repair first or implement lane-range chunks in
   the same phase.
5. **Application snapshot boundary:** worker/database snapshot format and deterministic
   state root, required before a node can skip executing the downloaded suffix.
6. **GC rule:** indefinite sequence retention initially, or a proved minimum number of
   correct checkpoint retainers.
7. **Partial-view installation:** support a locally emitted open core in version 1, or
   permit installation only when the local cursor has no partial delta.

The conservative version-1 choices are: fixed boundaries, standalone announcements plus
later piggybacking, indefinite sequence metadata retention, existing block repair with
checkpoint-source preference, no application snapshots, and full support for checking a
local partial delta before installation.
