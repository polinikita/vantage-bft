// Simple-IT cut-consensus modules.
//
// Messages and aggregators define protocol state. The engine returns effects instead
// of performing I/O and prunes round-indexed state with `prune_below`.
pub mod aggregators;
pub mod effects;
pub mod engine;
pub mod messages;
pub mod node;

pub use aggregators::{
    CutReadyAggregator, CutVoteAggregator, DecideAggregator, TimeoutAcceptAggregator,
    TimeoutAggregator,
};
pub use effects::{CutEffect, CutOut};
pub use engine::{CutEngine, Inbound, TipOracle, Variant};
pub use messages::{
    Cut, CutProposal, CutReady, CutRound, CutVote, Decide, Timeout, TimeoutAccept, TimeoutCert,
};
// `node::SimpleItCore` only -- `node::SimpleItReceiverHandler`/`node::Inbound` are
// reached via their full path (`crate::simpleit::node::...`), exactly mirroring
// `vantage`'s own `pub use node::VantageCore;` (not `VantageReceiverHandler`). Note
// `node::Inbound` differs from `engine::Inbound` re-exported above. It is the
// wire/channel type that contains the latter alongside Simple-IT's data-plane
// messages) -- see `node.rs`'s own doc comment on `Inbound`.
pub use node::SimpleItCore;
