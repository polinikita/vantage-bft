pub mod agb;
pub mod avail;
pub mod block;
pub mod claim;
pub mod cursor;
pub mod frontier;
pub mod install;
pub mod lanes;
pub mod node;
pub mod outbox;
pub mod pacemaker;
pub mod payload;
#[cfg(feature = "pipeline-tracing")]
mod pipeline;
pub mod repair;
pub mod resolve;
pub mod resume;
pub mod sequence;
pub mod threshold;
pub mod wire;

pub use agb::{
    AgbEngine, BatchViewProposal, DigestStatements, Echo, EchoBatch, EchoDigest, EchoOut, Manifest,
    Outcome, ProposalOut, Ready, ReadyBatch, ReadyDigest, ReadyGrade, ReadyOut, ResolutionEntry,
    TimerKind, ViewProposal,
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

/// Side effects emitted by Vantage state machines for the runtime to execute.
#[derive(Debug, Clone)]
pub enum Effect {
    /// Broadcasts a locally created block.
    BroadcastPublish(Header),
    /// Broadcasts an unsigned availability acknowledgment.
    BroadcastAck(Ack),
    /// Requests missing batches from local workers for `(author, header digest)`.
    SyncBatches(
        PublicKey, // Block author.
        Digest,    // Header digest.
        Vec<(Digest, WorkerId)>,
    ),
    RequestTo(PublicKey, Digest),
    ServeTo(PublicKey, Header),
    /// Reports a newly cached block so exact-digest repair walks can resume.
    BlockCached(Digest),

    BroadcastPropose(ProposalOut),
    BroadcastEcho(EchoOut),
    BroadcastEchoSkip(View),
    /// Quarantines non-quorum tips carried by a locally emitted READY-mix.
    QuarantineTips(Manifest),
    BroadcastReady(ReadyOut),
    BroadcastNoReady(View),
    /// Broadcasts a skip vote after the local no-ready and `2f + 1` skip predicates.
    BroadcastSkipVote(View),
    /// Reports whether the fixed proposal for a view is well formed.
    Fixed(View, bool),
    /// Reports irrevocable completion with the core and tail manifests.
    Completed(View, Manifest, Manifest),
    /// Reports the first terminal outcome selected for a view.
    Sealed(View, Outcome),
    /// Arms a view timer at an absolute deadline.
    ArmTimer(View, TimerKind, Instant),

    /// Carries `(UTC milliseconds, batches by worker, headers in commit order)`.
    NotifyCommitted(
        u64, // UTC milliseconds.
        Vec<(WorkerId, Vec<Digest>)>,
        Vec<Header>,
    ),

    BroadcastWish(View),
    Enter(View),
    /// Raises the local wish before the following response effect is executed.
    RaiseWish(View),

    /// Reports terminal view output in emission order.
    SequenceFinalized {
        view: View,
        outcome: sequence::SequenceOutcome,
        output_delta: Vec<Digest>,
    },
    /// Restores this node's lane from a checkpoint-certified committed header.
    RecoverOwnLane(Header),

    /// Reports the first genuine completion of a proposal carrying recovery entries.
    CompletionReportable(View, ProposalOut),
    BroadcastCompReport(View, Digest),
    BroadcastControlInit(control::ControlProposal, Option<ProposalOut>),
    BroadcastControlEcho(control::ControlProposal),
    BroadcastControlReady(control::ControlProposal),
    /// Broadcasts the control protocol's commit vote.
    BroadcastControlCommit(Round),
    BroadcastControlTimeoutVote(Round),
    BroadcastControlTimeoutAccept(Round),
    ControlFetchTo(PublicKey, View, Digest),
    /// Serves a held, verified control proposal body.
    ControlServeTo(PublicKey, View, ProposalOut),
    /// Arms a control-round timer at an absolute deadline.
    ArmControlTimer(Round, Instant),

    /// Applies an anchor outcome and its authorized non-skip block references.
    ApplyAnchor(View, Outcome, Vec<BlockRef>),

    BodyFetchTo(PublicKey, View, Digest),
    /// Serves a requested AGB proposal body.
    BodyServeTo(PublicKey, View, ViewProposal),

    /// Serves a replayed block to the requesting peer.
    ResumeServeTo(PublicKey, Header),
    /// Carries resolved claims; `true` means the digest came from the proposal tip.
    AvailClaimed(PublicKey, Vec<(BlockRef, bool)>),
}

pub mod control;
pub use control::{ControlLog, ControlProposal, Round};

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
