// PHASE3-SPEC.md/PHASE4-SPEC.md -- Vantage: per-author hash-chained blocks (the
// existing `Header` with `Option` fields, §3.1), all-to-all unsigned ACKs, first-hand
// availability accounting (`lanes::LaneManager`, §3.2), authorized recursive repair
// (`repair::Repairer`, §3.3), the Direct-AGB per-view engine (`agb::AgbEngine`,
// PHASE4-SPEC.md §§3-8), the responsive proposal frontier (`frontier::Frontier`, §4),
// the output cursor (`cursor::Cursor`, §9), and the production wiring task
// (`node::VantageCore`, §1). Gated behind `Protocol::Vantage`; the two Autobahn
// assemblies never construct anything in this module.

pub mod agb;
pub mod block;
pub mod cursor;
pub mod frontier;
pub mod lanes;
pub mod node;
pub mod pacemaker;
pub mod payload;
pub mod repair;
pub mod resolve;
pub mod threshold;
pub mod wire;

pub use agb::{
    AgbEngine, Echo, Manifest, Outcome, Ready, ReadyGrade, ResolutionEntry, TimerKind, ViewProposal,
};
pub use block::BlockRef;
pub use cursor::Cursor;
pub use frontier::Frontier;
pub use lanes::{AvailEntry, BlockCache, BlockEntry, LaneManager, SharedBlocks};
pub use node::VantageCore;
pub use pacemaker::Pacemaker;
pub use repair::Repairer;
pub use resolve::Resolver;
pub use threshold::Thresholds;

use crate::messages::{Ack, Header};
use crate::primary::View;
use config::WorkerId;
use crypto::{Digest, PublicKey};
use std::time::Instant;

/// Outbound side-effects produced by `LaneManager`/`Repairer`/`AgbEngine`/`Frontier`/
/// `Cursor` methods. All five are pure (no direct network/worker I/O) so that tests can
/// drive them without a live network -- callers (the production wiring, or a test)
/// execute these against the real transport/worker/timer channels.
#[derive(Debug, Clone)]
pub enum Effect {
    /// N1: broadcast our own freshly-created block to every primary.
    BroadcastPublish(Header),
    /// N3: broadcast an unsigned ack for a tuple that just became `DirectPub`.
    BroadcastAck(Ack),
    /// D1: ask our own workers to sync these missing batches for `author`'s block
    /// (named by its own digest -- PHASE4-SPEC.md §1's production wiring correlates a
    /// resolved `store.notify_read` waiter back to `LaneManager::set_payload_ready` via
    /// this digest; a minimal extension of the Phase-3 shape, documented in
    /// PHASE4-NOTES.md).
    SyncBatches(
        PublicKey, /* author */
        Digest,    /* header digest */
        Vec<(Digest, WorkerId)>,
    ),
    /// D2/N6: send `request(h)` to `peer` (fan-out is one `Effect` per peer, emitted at
    /// most once per (peer, h) ever -- see `Repairer::requested`).
    RequestTo(PublicKey, Digest),
    /// N7: send `serve(h, b)` to `peer` in answer to its request.
    ServeTo(PublicKey, Header),
    /// A block was just cached (via publish or serve) -- production wiring forwards
    /// this to `Repairer::on_block_available` so a walk stuck waiting on this exact
    /// digest can advance (§3.3: "whenever a cached block matches an authorized exact
    /// coordinate ... the walk advances"). Tests forward it manually too.
    BlockCached(Digest),

    // --- PHASE4-SPEC.md §§3-8 (AGB engine) ---
    /// R1: broadcast our own freshly-constructed view proposal.
    BroadcastPropose(ViewProposal),
    /// R2: broadcast a (grade-1 or grade-0) proposal echo.
    BroadcastEcho(Echo),
    /// R2 fallback/absolute deadline: broadcast an echo-skip for `view` (sender is
    /// filled in by the caller, which always knows its own identity -- symmetric with
    /// `BroadcastNoReady` below).
    BroadcastEchoSkip(View),
    /// R3: broadcast a proposal ready (grade One/Zero/Mix).
    BroadcastReady(Ready),
    /// R3 absolute deadline: broadcast a no-ready for `view`.
    BroadcastNoReady(View),
    /// §5's `Fixed` transition outcome for `view` (`true` = well-formed proposal now
    /// fixed, `false` = malformed/Reject) -- not itself a spec-named wire effect, but a
    /// necessary, minimal channel from `AgbEngine::on_propose` to `Frontier::record_fixed`
    /// (§4's frontier-advance rule reads exactly this bit); documented as a deviation in
    /// PHASE4-NOTES.md.
    Fixed(View, bool),
    /// R4 completion (`complete(v) -> B`): the core becomes irrevocable; hand `(C, T)`
    /// to the cursor as this view's manifests, state `gopen`. Not itself a spec-named
    /// wire effect (completion produces no network message) but a necessary, minimal
    /// channel from `AgbEngine` to `Cursor` (§7's "hand (C,T) to the cursor"); documented
    /// as a deviation in PHASE4-NOTES.md.
    Completed(View, Manifest, Manifest),
    /// The try-seal arbiter's terminal, first-wins result for `view` -- drives the
    /// cursor (§9).
    Sealed(View, Outcome),
    /// Arm a timer at the given deadline (§10; `Instant` computed by the engine at arm
    /// time from Δ/θE/θR and the view's own entry/first-proposal instants -- carried
    /// directly here, a minimal extension of the module plan's `ArmTimer(View,
    /// TimerKind)` signature so the caller (`VantageCore`) needn't duplicate that
    /// arithmetic; documented as a deviation in PHASE4-NOTES.md).
    ArmTimer(View, TimerKind, Instant),

    // --- PHASE4-SPEC.md §9 (output cursor) ---
    /// Commit metric (Phase-2 parity): for every block just appended to the output log,
    /// notify our own workers (grouped by `WorkerId`) of the batches committed.
    /// Third field (PHASE7-PREP-NOTES.md, paying down the PHASE4-NOTES.md §6 scope
    /// cut): the committed blocks' own `Header`s, in commit order, already looked up
    /// from `BlockCache` by the cursor at emit time -- so `VantageCore` only has to
    /// forward them to `tx_output`, matching the Autobahn `Committer`'s output-channel
    /// shape without VantageCore needing its own `BlockCache` handle.
    NotifyCommitted(
        u64, /* commit UTC-millis */
        Vec<(WorkerId, Vec<Digest>)>,
        Vec<Header>,
    ),

    // --- PHASE5-SPEC.md §1-3 (WISH pacemaker) ---
    /// W2 amplification: broadcast a standalone `VantageWish` (sender filled in by the
    /// caller at serialization time, symmetric with `BroadcastEchoSkip`/
    /// `BroadcastNoReady`).
    BroadcastWish(View),
    /// W2's formal-entry-target-advance step: record entry to `view` -- executed as
    /// `AgbEngine::enter(view, ...)` + `Frontier::enter(view)`, in increasing order
    /// (`Pacemaker` already emits these in ascending order, one per newly-covered view).
    Enter(View),
    /// W3's two-response wish trigger, surfaced by `AgbEngine::two_response_wish_target`
    /// (a pure query -- the engine itself never touches `Pacemaker`, keeping D5-3's
    /// module separation): raise our own wish to `View` before the response effect
    /// immediately following it in the same batch is executed (`VantageCore::execute`'s
    /// FIFO queue always processes this one first). Not itself a spec-named wire effect
    /// -- a necessary, minimal channel from `AgbEngine` to `Pacemaker::raise_own_wish`,
    /// documented as a deviation in PHASE5-NOTES.md.
    RaiseWish(View),

    // --- PHASE6-SPEC.md §5 (completion reports + control log) ---
    /// The FIRST genuine R4 completion for `view` with `M != None` -- a necessary,
    /// minimal channel from `AgbEngine` to `control::ControlLog` (mirrors `Completed`'s
    /// own role for the cursor); carries the full `ViewProposal` (`B_w`) since
    /// `control::ControlLog` both counts the report AND retains/serves `B_w`.
    CompletionReportable(View, ViewProposal),
    /// Broadcast our own `CompReport` (sender filled in by the caller).
    BroadcastCompReport(View, Digest),
    /// The control round's leader step: broadcast `ControlInit` (the control proposal,
    /// plus `B_w` as validation data when the value is non-empty).
    BroadcastControlInit(control::ControlProposal, Option<ViewProposal>),
    /// Broadcast our own `ControlEcho` (sender filled in by the caller).
    BroadcastControlEcho(control::ControlProposal),
    /// Broadcast our own `ControlReady` (sender filled in by the caller).
    BroadcastControlReady(control::ControlProposal),
    /// Broadcast our own `ControlCommit` vote for `Round` (sender filled in by the
    /// caller). PHASE6-SPEC.md §5's wire list does not itself name a commit message,
    /// but the paper's own **Vote** step ("send `<commit, curr_round>` to all
    /// parties") is load-bearing for the **Commit** rule -- a necessary, minimal,
    /// documented addition (D6-6, PHASE6-NOTES.md), appended last on the wire per the
    /// same bincode-compat discipline as every other Vantage-only variant.
    BroadcastControlCommit(Round),
    /// The round-timeout notification's **Vote** step (Fig. 4): broadcast
    /// `ControlTimeoutVote` (sender filled in by the caller).
    BroadcastControlTimeoutVote(Round),
    /// The round-timeout notification's **Accept**/**Cascade** step: broadcast
    /// `ControlTimeoutAccept` (sender filled in by the caller).
    BroadcastControlTimeoutAccept(Round),
    /// Fan-out request for a still-missing `B_w`, one `Effect` per target peer (mirrors
    /// `RequestTo`'s shape).
    ControlFetchTo(PublicKey, View, Digest),
    /// Answer a peer's `ControlFetch` with our own held, verified `B_w`.
    ControlServeTo(PublicKey, View, ViewProposal),
    /// Arm the control-round timer (`6Δ`, §5) at the given deadline.
    ArmControlTimer(Round, Instant),

    // --- PHASE6-SPEC.md §6 (anchors + apply-anchor adapter) ---
    /// `control::ControlLog`'s log-consumption pump, upon observing `A_u` for a
    /// not-yet-anchored view `u`: `Outcome` is `X_u` (`gfull`/`gcore`/`gskip`, already
    /// derived from the resolution entry); the `Vec<BlockRef>` is the non-skip
    /// manifest references to authorize BEFORE submitting to the try-seal arbiter (the
    /// executor, `VantageCore`, owns both `Repairer::authorize` and
    /// `AgbEngine::submit_anchor` -- `control::ControlLog` itself touches neither).
    ApplyAnchor(View, Outcome, Vec<BlockRef>),
}

pub mod control;
pub use control::{ControlLog, ControlProposal, Round};

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
