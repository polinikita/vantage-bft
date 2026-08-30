pub mod agb;
pub mod avail;
pub mod block;
pub mod claim;
pub mod cursor;
pub mod direct_resolution;
pub mod frontier;
pub(crate) mod index;
pub mod install;
pub mod lanes;
mod legacy_control_wire;
pub mod node;
pub mod outbox;
pub mod pacemaker;
pub mod payload;
#[cfg(feature = "pipeline-tracing")]
mod pipeline;
pub mod repair;
pub mod resolution_evidence;
pub mod resume;
pub mod sequence;
pub mod threshold;
pub mod wire;

pub use agb::{
    AgbEngine, BatchViewProposal, DigestStatements, DirectVoteDecision, Echo, EchoBatch,
    EchoDigest, EchoOut, Manifest, Outcome, ProposalOut, Ready, ReadyBatch, ReadyDigest,
    ReadyGrade, ReadyOut, ResolutionEntry, TimerKind, ViewProposal,
};
pub use block::BlockRef;
pub use cursor::Cursor;
pub use direct_resolution::{
    DirectResolutionDone, DirectResolutionEffect, DirectResolutionPhase, DirectResolutionProof,
    DirectResolutionProposal, DirectResolutionStatement, DirectResolutionSuggest,
    DirectResolutionTimerKind, DirectResolutionValueFetch, DirectResolutionValueServe,
    DirectResolutionVote, DirectResolutionWish, DirectResolutionWitness, DirectResolver,
    DirectResolverView,
};
pub use frontier::Frontier;
pub use lanes::{AvailEntry, BlockCache, BlockEntry, LaneManager, SharedBlocks};
pub use legacy_control_wire::LegacyControlProposal;
pub use node::VantageCore;
pub use pacemaker::Pacemaker;
pub use repair::Repairer;
pub use resolution_evidence::ResolutionEvidence;
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
    /// Quarantines non-quorum tips after a READY quorum completes without either grade quorum.
    QuarantineTips(Manifest),
    BroadcastReady(ReadyOut),
    BroadcastNoReady(View),
    /// Broadcasts a skip vote after the local no-ready and `Q = n - f` skip predicates.
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

    /// Executes one step of the target-local resolver.
    DirectResolution(DirectResolutionEffect),

    BodyFetchTo(PublicKey, View, Digest),
    /// Serves a requested AGB proposal body.
    BodyServeTo(PublicKey, View, ViewProposal),

    /// Serves a replayed block to the requesting peer.
    ResumeServeTo(PublicKey, Header),
    /// Carries authenticated positional claims for local ancestry resolution.
    AvailClaimed(PublicKey, Vec<claim::ClaimRef>),
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
