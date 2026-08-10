# Late-joiner contribution: findings, requirements, open problem

Handoff document. Everything below is measured on docker-bench netem, n=21, 1000 tx/s,
node-20 stopped for 60 s then given 120 s to recover, unless stated otherwise. Fleet view
rate ~13 views/s; the healthy fleet's own node-to-node cursor spread is **17–21 views**,
which is the parity target any recovering node should be judged against.

## 1. Requirement

A validator that fell behind must, **after catching up**:

1. immediately resume contributing — its own round-robin **consensus proposals** must be
   committed, not merely its lane data blocks (those carry client transactions and are
   committed by everyone, so they say nothing about consensus participation);
2. **never need to catch up again** — state sync must stop and stay stopped. Perpetual
   recovery means the protocol is doing something wrong.

## 2. How to reproduce and score

```
./docker-bench/late_joiner.sh --nodes 21 --rate 1000 --down 60 --settle 120 \
    --interval 50 --sequence-sync-min-gap-views 100 --sequence-sync-shed-gap-views 100
```

Score with `check_latch.py <start> <end>` (in the session scratchpad; reproduced logic
below). Five conditions, judged on the **last third** of the window — "never again" only
means anything in the tail:

| # | condition | metric |
|---|---|---|
| 1 | recovery latched off | `vantage_sequence_sync_recovered` = 1 throughout tail |
| 2 | transfers stopped | `rate(vantage_sequence_sync_verified_total)` = 0 in tail |
| 3 | reached parity | median lag ≤ 2× **peer-only** spread (exclude the joiner, else circular) |
| 4 | data blocks committed | `vantage_committed_by_author` on a peer / joiner's published |
| 5 | **own proposals committed** | `vantage_own_proposals_committed_total` / `vantage_own_proposer_turns_total` |

**Instrument trust.** Conditions 1, 2, 3, 5 are plain per-node counters and are reliable.
Condition 4 is NOT: it resolves the author label from `data/node-N/key.json`, which
`gen.py` regenerates every run, and it returned 1.00 and 0.00 on runs with no relevant
change between them. Do not reason from condition 4 until the mapping is fixed.

Two earlier measurement traps, both of which cost several wasted iterations:

- `vantage_own_blocks_committed_total` counts what a node **locally observes** committing.
  A node that is behind has not reached the views where its own blocks were committed, so
  it reads 0 regardless of whether they were. Contribution must be read on a caught-up peer.
- Final lag in a joiner run flatters itself, because the client workload expires before the
  run ends and the fleet stops advancing. Judge the tail, not the last sample.

## 3. The core open finding

**The catcher proposes, but its proposals are not adopted.** Latest measurement (latch7):

```
proposals: made 129, proposer turns 117, committed 49  ->  0.42   (peer median 1.00)
median lag 84 views          peer-only spread 18 views
transfers 0.14/s, never stopping;  recovered never latches
```

So it is not failing to propose — it proposes for essentially all its turns. Peers decline to
ADOPT the proposal, and those views seal as `Skip`. 58–60 % of its proposer turns are lost,
reproducibly (0.42 in latch7, 0.40 in latch9).

### A structural fact, which turned out NOT to be the cause

**Nothing advances the AGB view on install.** `apply_sequence_install` raises the output cursor
and calls `resolver.note_installed_through(target)`, but `enter_view_effects` is reached only
from boot and `Effect::Enter`, so the AGB view advances solely through the WISH pacemaker.
Consensus position therefore trails output position. Directly observed: node-20 echoing views
2773–2815 while node-0 echoed 2795–2877.

This is true, and it is *not* what breaks the proposals — see the refuted fixes below. Recorded
because it is a real asymmetry someone will rediscover and assume is the cause, as I did.

### Two candidate fixes MEASURED AND REFUTED (latch9)

Both are in the tree. Both are semantically right and neither helps; do not spend time
re-deriving them.

1. **Retain `Inbound::Wish` while shedding** — so the catcher keeps learning where the fleet
   is instead of discarding the signal.
2. **`RaiseWish(target + 1)` on install completion** — so consensus position follows output
   position rather than walking behind it.

Measured together against latch7's baseline:

| | latch7 (neither) | latch9 (both) |
|---|---|---|
| proposals committed | 0.42 (49/117) | **0.40 (44/109)** |
| median lag | 84 | 86 |

Unchanged within noise. So the AGB view is **not** the binding constraint, and the catcher's
own view is evidently not what makes its proposals unusable. Note it *does* propose for
essentially all its turns (105 made / 109 turns), so this is not a timing-of-proposal problem
in the naive sense either.

### Where I would look next

The failure is that peers do not ADOPT the catcher's proposal, not that it fails to send one.
Since view position is ruled out, the remaining candidates concern proposal *content* and the
catcher's own availability bookkeeping:

1. **The catcher cannot vouch for its own lane.** Confirmed while chasing the CPU problem: its
   own lane prefix is absent from `BlockCache` after restart, so `direct_pub` fails for its own
   blocks indefinitely and `pending_direct` never drains for its own author. A proposal built
   from `Frontier::try_propose`/`LaneManager` state under that condition may name a manifest
   peers will not accept, or may be near-empty. This is the strongest remaining lead and it is
   the same root cause as the `header_seal` blow-up.
2. **Seed the cache with the node's own lane prefix from the store on restart.** Headers are
   persisted; the cache is memory-only. This would fix (1) at the source, and would also let
   `pending_direct` drain.
3. **Do not let a lagging node hold a proposer turn.** Independently of the cause: 60 % of the
   catcher's turns produce `Skip`, and a `Skip` costs the whole fleet a view, not just the
   joiner. Yielding the turn is worth considering on its own merits.

### What a Skip actually costs here

Worth quantifying before optimising: at n=21 the catcher owns 1 view in 21, and loses ~60 % of
those, so ~2.9 % of all views seal empty because of it. That is the fleet-level cost of one
recovering node.

## 4. Why sync never stops (condition 1/2)

`recovery_active` is `target - local >= sync_gate`. At equilibrium the catcher sits ~84
views behind with checkpoint boundaries every 50 views, so `target - local` is ~100–137 —
permanently at or above a gate of 100. The gate is re-evaluated against every newly
certified boundary, so it re-arms forever. Raising the gate to 200 made everything worse
(lag 84 → 154), consistent with the sweep result that a **low** shed gate wins.

A `sequence_sync_recovered` latch plus a `sequence_sync_rearm_gap_views` (800) re-arm
threshold is implemented. It has never been observed to hold, because the gap never falls
inside the gate. **The latch cannot work until the underlying lag is fixed** — it is
downstream of section 3, not an alternative to it.

Two latch bugs already found and fixed, worth not reintroducing:

- A **level** check latches at boot (`recovery_active` is trivially false there, no install
  staged) and disables state sync from birth: measured "RECOVERED at view=0" 59 ms after
  start, 0 transfers, joiner stuck at view 1. Must be **edge-triggered** on leaving recovery.
- The gauge was set to 1 and never cleared on re-arm, so it asserted "recovered" while the
  node was actively resyncing.

## 5. Parameter sweep results (measured)

| interval | sync gate | shed gate | lag | tail closed by participation | transfers |
|---|---|---|---|---|---|
| 20 | 30 | 50 | 48 | 4.1 % | 78 |
| 10 | 20 | 50 | 59 | 1.7 % | 121 |
| 50 | 80 | 100 | 59 | **11.6 %** | 31 |
| 20 | 30 | 300 | 270 | 1.4 % | 55 |

- The **shed gate dominates** lag; keep it low (50–100). 300 is the only arm that failed.
- The **checkpoint interval does not** set a lag floor — halving it made lag slightly worse.
  An earlier "one interval per transfer cycle" model was wrong: transfers cover variable
  multi-interval ranges via retargeting.
- **Larger intervals are cheaper**, not slower: interval 50 gave the same lag with 31
  transfers instead of 121 and the highest participation share. Matters at n=100.
- The sync gate must sit **above** the achievable lag or it never fires.

## 6. CPU findings

The catcher was **core-saturated**, which is why it could not close the gap: 0.88 cores vs a
peer's 0.14 (6.5×), on a single-threaded core loop.

**Fixed:** `refresh_author` was quadratic. `direct_pub` walks the lane to genesis **twice**
(`verified_prefix_through_genesis` + `direct_prefix_ok`), and after a restart the node has no
cache entries for its own lane prefix — `BlockCache` is memory-only — so none of its blocks
confirm until repair refills that prefix. `pending_direct` grew without bound and every
publish re-tested all of it. `header_seal` was burning **478 ms of every second, 1,078× a
peer**. Now a bounded rotating scan (8 walks/call): **478,493 → 10,558 µs/s, 45× better.**

Do **not** "optimise" this by stopping at the lowest blocked height. Prefix failure is
monotone in height on the canonical chain, but `pending_direct` also holds refs from
*abandoned* branches, which are permanently unconfirmable; one at a low height then blocks
every higher ref forever. Measured: own blocks committed fell to zero.

Remaining after the fix — total 0.67 cores vs peer 0.16:

| stage | joiner | peer | ratio |
|---|---|---|---|
| `effect_execution` | 343,462 µs/s | 120,953 | 2.8× |
| `inbound_dispatch` | 267,278 µs/s | 48,737 | 5.5× |
| `header_seal` | 10,558 µs/s | 452 | 23× |

`effect_execution` **tripled** after the `refresh_author` fix (103k → 343k) because
confirmations now actually fire. It is the largest consumer and has not been investigated.

## 7. Other changes in the tree from this work

- Split `sequence_sync_min_gap_views` into a **sync gate** and a **shed gate**
  (`sequence_sync_shed_gap_views`); one constant previously drove three decisions.
- `install_replaces_inbound` releases its claim on the view range once inside the sync gate
  (holding it starved ordinary participation of exactly the traffic that could close the tail).
- `Cursor::apply_watermarks` now derives watermarks from the **delivered delta**, not the
  manifest. Deriving from the manifest let an installing node adopt a watermark an executing
  node never adopts, and then emit forked blocks the other dropped — **divergent committed
  logs between two correct nodes**. General rule: watermarks are a function of delivery,
  never of intent.
- `lanes::SuffixWalk::{Ready, Pending, Forked}`; `Cursor::expand` drops a `Forked` manifest
  entry instead of waiting forever on it (that wait wedged the output cursor of every honest
  node when one validator forked its lane).
- Lane frontier persistence (`OWN_FRONTIER_KEY`), restored before anything can publish, in
  both `vantage` and `simpleit`. Note `Store::write` only queues into a 100-slot channel
  flushed every 50 ms with `sync=false`, so this is **not** durable against SIGKILL.
- O(gap²) → monotone cursor in `first_missing_outcome`/`first_missing_delta`;
  `views_in_flight()` lifted out of `admit`'s loop condition.
- `late_joiner.sh` duration trimmed from `30 + down + settle + 90` to `15 + down + settle + 20`.

## 8. Audit findings still open

See the two Opus audits (safety and liveness/scale). Identity/lane spoofing is **out of
scope — this is a testbed.** Still open and relevant here:

- a stalled install can never be abandoned: one unavailable block ⇒ permanent halt of
  committed output, because the staged target blocks every newer target and holds shedding on;
- `SequenceStore` freezes if a single `SequenceFinalized` effect is lost, and two
  `abort_install` call sites discard effects while keeping the state mutation;
- retention is 19.4 KB/view at n=100 = **911 MB/h per node**, against the plan's estimate of
  10 MB per 100k views (~190× off) — the item most likely to break the AWS n=100 gate;
- `serve_sequence_headers` has no per-request bound; every sibling serve path clamps.
