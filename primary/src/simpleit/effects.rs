// Effects returned by `CutEngine`. The caller performs network, timer, and
// committer actions.
use crate::simpleit::messages::{
    Cut, CutProposal, CutReady, CutRound, CutVote, Decide, Timeout, TimeoutAccept,
};
use crypto::{Digest, PublicKey};
use std::time::Instant;

/// Wire messages emitted by the state machine. `TimeoutCert` and safe state are local;
/// only individually verified votes and ready messages can advance them.
#[derive(Clone, Debug)]
pub enum CutOut {
    CutProposal(CutProposal),
    CutVote(CutVote),
    Decide(Decide),
    Timeout(Timeout),
    TimeoutAccept(TimeoutAccept),
    CutReady(CutReady),
}

/// Actions that the caller executes for network sends, timers, commits, and repair.
#[derive(Clone, Debug)]
pub enum CutEffect {
    /// Broadcast `CutOut` to every other primary.
    Broadcast(CutOut),
    /// Arm a one-shot deadline for `round`. The caller feeds back
    /// `Inbound::TimerFired(round)` after it elapses.
    ArmTimer { round: CutRound, deadline: Instant },
    /// Emit a commit for `round`, carrying the committed cut's tips.
    Commit { round: CutRound, proposals: Cut },

    /// Ask `peer` for the proposal identified by `(round, cut_id)`.
    FetchTo {
        peer: PublicKey,
        round: CutRound,
        cut_id: Digest,
    },
    /// Answer a peer's fetch with a held proposal.
    ServeTo {
        peer: PublicKey,
        proposal: CutProposal,
    },
}
