// Simple-IT cut-consensus protocol -- stage 1 of a port from the upstream
// `simpleit/Opt-Mempool-Simple-IT-Failure` branch (fetched as remote `simpleit`, never
// checked out/merged/applied -- see each submodule's doc comment for exact upstream
// line ranges).
//
// Stage 1 covers only the wire types (`messages`) and the vote aggregators
// (`aggregators`) -- no state machine, no dispatch wiring, nothing reachable from
// `core.rs` yet. Required deviations from upstream, applied throughout both
// submodules:
//   1. Every digest hashes with `Blake3Hasher`, never `Sha512`.
//   2. `CutRound` (`= u64`, in `messages`) is the one cut-round type; `Slot`/`View`
//      (Autobahn) and `crate::vantage::control::Round` (an unrelated control-round
//      counter) are not used here.
//   3. No collection in either submodule is keyed by cut round, so the
//      BTreeMap-over-HashMap GC rule has no concrete target yet -- it applies to the
//      not-yet-ported state machine's per-round maps (stage 2).
//   4. `Committee` thresholds are called by name (`quorum_threshold`,
//      `optimistic_threshold`), never reimplemented inline; see aggregators.rs for the
//      `optimistic_threshold` free-function exception this repo's `Committee` forces.
//
// Autobahn types (`QC`, `TC`, `CommitQC`, `ConsensusMessage`, ...) and Vantage
// (`crate::vantage::*`) are never constructed here.

pub mod aggregators;
pub mod messages;

pub use aggregators::{
    CutVoteAggregator, DecideAggregator, TimeoutAcceptAggregator, TimeoutAggregator,
};
pub use messages::{
    Cut, CutCertificate, CutProposal, CutRound, CutVote, Decide, Timeout, TimeoutAccept,
    TimeoutCert,
};
