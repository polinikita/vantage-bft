// Simple-IT cut-consensus protocol -- a port from the upstream
// `simpleit/Opt-Mempool-Simple-IT-Failure` branch (fetched as remote `simpleit`, never
// checked out/merged/applied -- see each submodule's doc comment for exact upstream
// line ranges).
//
// Stage 1 (`messages`, `aggregators`) covers the wire types and the vote aggregators.
// Stage 2 (`engine`, `effects`) covers the state machine itself: `engine::CutEngine`
// ports the cut-consensus half of upstream's `Core` as a pure, network-free state
// machine that returns `effects::CutEffect`s instead of touching a socket/store/
// channel directly -- see engine.rs's module doc comment for the full architecture.
// Required deviations from upstream, applied throughout every submodule:
//   1. Every digest hashes with `Blake3Hasher`, never `Sha512`.
//   2. `CutRound` (`= u64`, in `messages`) is the one cut-round type; `Slot`/`View`
//      (Autobahn) and `crate::vantage::control::Round` (an unrelated control-round
//      counter) are not used here, with one narrow, explicit exception: `engine.rs`'s
//      `leader_for_round` converts a `CutRound` to a `View` at the single call site
//      where it calls `crate::vantage::agb::proposer` (deviation 1 -- upstream's own
//      `leader.rs`/`LeaderElector` is not ported).
//   3. Prunable state (stage 2 only -- stage 1 defines no state) is `BTreeMap`/
//      `BTreeSet`, keyed so a single `split_off` prunes it; see `CutEngine::
//      prune_below`.
//   4. `Committee` thresholds are called by name (`quorum_threshold`,
//      `optimistic_threshold`, `validity_threshold`), never reimplemented inline; see
//      aggregators.rs for the `optimistic_threshold` free-function exception this
//      repo's `Committee` forces.
//
// Autobahn types (`QC`, `TC`, `CommitQC`, `ConsensusMessage`, `Slot`, Autobahn's own
// `View`) are never constructed here. Vantage is used in exactly the one place named
// in point 2 above (`agb::proposer`, a pure leader-schedule function) -- no other
// `crate::vantage::*` item (no `LaneManager`, no `Effect`, no engine) is ever
// constructed or imported here.

pub mod aggregators;
pub mod effects;
pub mod engine;
pub mod messages;

pub use aggregators::{
    CutVoteAggregator, DecideAggregator, TimeoutAcceptAggregator, TimeoutAggregator,
};
pub use effects::{CutEffect, CutOut};
pub use engine::{CrashSim, CutEngine, Inbound, TipOracle};
pub use messages::{
    Cut, CutCertificate, CutProposal, CutRound, CutVote, Decide, Timeout, TimeoutAccept,
    TimeoutCert,
};
