// Simple-IT cut-consensus effects -- the outputs of `engine::CutEngine`. Upstream's
// `Core` (primary/src/core.rs) performs network sends and committer channel sends
// directly from inside its `process_*`/`try_*` methods; `CutEngine` never does either,
// so every one of those call sites becomes a value pushed onto a `Vec<CutEffect>`
// instead, for the caller (the not-yet-written production core, or a test) to actually
// execute. See engine.rs's module doc comment for the full architecture rationale.

use crate::simpleit::messages::{
    Cut, CutCertificate, CutProposal, CutRound, CutVote, Decide, Timeout, TimeoutAccept,
};
use crypto::{Digest, PublicKey};
use std::time::Instant;

/// The wire-message payloads `CutEngine` ever broadcasts. One variant per upstream
/// `PrimaryMessage::{CutProposal,CutVote,CutCertificate,Decide,Timeout,TimeoutAccept}`
/// arm actually constructed by one of the 25 ported methods -- `TimeoutCert` is
/// deliberately absent (upstream never broadcasts one; it is purely a local
/// certificate `handle_timeout_accept_action` verifies and acts on).
#[derive(Clone, Debug)]
pub enum CutOut {
    CutProposal(CutProposal),
    CutVote(CutVote),
    CutCertificate(CutCertificate),
    Decide(Decide),
    Timeout(Timeout),
    TimeoutAccept(TimeoutAccept),
}

/// Everything a `CutEngine` method can ask its caller to do. Three shapes, matching the
/// three kinds of "reach outside the state machine" upstream's `Core` had:
/// `self.network.broadcast(...)`, `self.cut_timer_futures.push(...)` (a relative-delay
/// sleep future), and `self.tx_committer.send(...)`.
#[derive(Clone, Debug)]
pub enum CutEffect {
    /// Broadcast `CutOut` to every other primary (upstream: a `ReliableSender::broadcast`
    /// call). The sender never needs to be named -- every `CutOut` variant already
    /// carries its own author/proposer field.
    Broadcast(CutOut),
    /// Arm a one-shot deadline for `round` (upstream: `schedule_cut_timer` pushing a
    /// `sleep(timeout_delay)` future onto `cut_timer_futures`). `deadline` is computed
    /// by the engine at arm time (`Instant::now() + timeout_delay`), matching
    /// `control::ControlLog`'s own `ArmControlTimer` shape (an absolute `Instant`, not
    /// a bare duration) rather than `agb::AgbEngine`'s threaded-`now` style -- see
    /// `CutEngine::schedule_cut_timer`'s doc comment for why. The caller is expected to
    /// eventually feed back `Inbound::TimerFired(round)` once `deadline` elapses (unless
    /// superseded, exactly like every other timer in this codebase).
    ArmTimer { round: CutRound, deadline: Instant },
    /// Emit a commit for `round`, carrying the committed cut's tips. Upstream:
    /// `emit_commit_to_committer`'s `self.tx_committer.send(ConsensusMessage::Commit {
    /// round, proposals })`. Deliberately its own variant, NOT a reuse of Autobahn's
    /// `ConsensusMessage::Commit` -- that enum also carries Autobahn's dead
    /// `Prepare`/`Confirm` arms, which this port does not inherit.
    Commit { round: CutRound, proposals: Cut },

    // --- Cut-proposal repair (not upstream -- see engine.rs's module doc comment and
    // `CutEngine::ensure_cut_fetch`/`on_cut_fetch`/`on_cut_serve` for the liveness gap
    // this closes) ---
    /// Ask `peer` for the `CutProposal` identified by `(round, cut_id)`. Mirrors
    /// `vantage::Effect::ControlFetchTo`'s identical role for Vantage's own carrier
    /// bodies (fan-out is one `CutEffect` per target peer, emitted at most once per
    /// `(round, cut_id)` pair every `CutEngine::FETCH_RETRY_ROUNDS` cut rounds -- see
    /// `CutEngine::ensure_cut_fetch`). The requester is never named here -- exactly
    /// like `ControlFetchTo`, the caller always asks on this node's own behalf, so
    /// the production wiring (`SimpleItCore::execute_cut`) fills in `self.name` at
    /// send time.
    FetchTo {
        peer: PublicKey,
        round: CutRound,
        cut_id: Digest,
    },
    /// Answer a peer's fetch with our own held `CutProposal`. Mirrors
    /// `vantage::Effect::ControlServeTo`'s identical role.
    ServeTo {
        peer: PublicKey,
        proposal: CutProposal,
    },
}
