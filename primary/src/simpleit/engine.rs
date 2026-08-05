// Simple-IT cut-consensus state machine (stage 2 of a port from the upstream
// `simpleit/Opt-Mempool-Simple-IT-Failure` branch's primary/src/core.rs). Ports exactly
// 25 named methods (the cut-consensus half of upstream's `Core`); everything else in
// that file -- header/vote/certificate processing, the Autobahn view-based consensus
// (`process_consensus_*`), the run loop, every network/store/channel field -- is
// Autobahn residue or data-plane wiring and is not ported. See each method's doc
// comment below for its exact upstream line range -- EXCEPT where the FIGURE-2 REWRITE
// paragraph below says otherwise: this module now implements the paper's Figure 2
// directly, and where that disagrees with upstream's own implementation, Figure 2
// governs. Upstream line citations on individual methods below are retained as
// provenance (what upstream had at that call site), not as an implied claim that the
// method still behaves as upstream did.
//
// FIGURE-2 REWRITE (this revision, arXiv:2606.14404 Fig. 2): upstream's cut-consensus
// design -- ported here unchanged until now -- broadcasts a `CutCertificate` (a
// `Vec<PublicKey>` of claimed voters) once any party's own vote count crosses
// `mint_threshold`, and every OTHER party accepts that certificate on the strength of
// `CutCertificate::verify` alone: checking only that the LISTED names are distinct,
// stake-bearing committee members -- never that they actually voted. This protocol is
// signature-free, so a vote carries no transferable proof of its own authorship; any
// party could therefore assert a certificate for any `cut_id`, forging a notarization
// that advances `cut_round`/`highest_safe_cut` (formerly `highest_certified_cut`) for a
// round it never itself established. Commit itself stayed protected (`try_commit_round`
// requires the locally-recorded leader proposal), but a forged certificate still let a
// party stall the chain -- see the task history for the full liveness-attack argument.
//
// Figure 2 itself has NO certificate message anywhere. `safe[r]` is established purely
// by rb-delivering `proposal[r]` and checking `SafeParent` (no vote-counting at all, in
// the paper's own text -- that guarantee comes for free from the reliable broadcast's
// totality property, which this port's `CutProposal`+`CutVote` pair only
// APPROXIMATES, see the REPAIR paragraph below), and `committed[r]` by each party
// counting `⟨commit, r⟩` (this engine's `Decide`) from n - f parties FIRST-HAND -- see
// the module doc comment on `Decide` (messages.rs) for why it is exactly Fig. 2's Vote
// step. This module's own design already has parties count `CutVote`s into a
// `mint_threshold`-thresholded aggregator as its chosen stand-in for "the proposal is
// corroborated enough to trust" (playing the role a real RBC's echo/ready phases would
// play, at the message-pattern level this codebase works at); this revision's fix is
// narrower than re-deriving that from scratch: keep the same threshold, but make EVERY
// party evaluate it over votes it individually verified and counted itself
// (`process_cut_vote` -> `mark_cut_safe`), and delete the one step that let a party
// skip its own counting by trusting a peer's relayed aggregate instead
// (`process_cut_certificate`'s old certificate-accept path, and the certificate
// broadcast that fed it). No `CutOut`/`Inbound`/`PrimaryMessage` variant can carry a
// notarization anymore -- the removal is enforced by the type system, not merely by
// this module's own logic (see `mod tests`' `inbound_has_no_certificate_shaped_variant`
// below for why that is a compile-time, not run-time, guarantee).
//
// FIELD MAPPING (old upstream-derived name -> Fig. 2's state variable -> this module's
// name), for every field this revision renames:
//   cut_certificates: BTreeMap<CutRound, CutCertificate>  -- safe[r] (+ its proposal)
//     -> safe: BTreeMap<CutRound, Digest>  (key presence IS safe[r]; the value IS
//        proposal[r]'s id -- the two are always established together here, so one map
//        carries both, exactly as upstream's own bool-map/proposal-map pair would if
//        upstream had used one map for each too)
//   highest_certified_cut: Digest  -- no single Fig.-2 variable names this; it is this
//     engine's own cache of "which safe cut should the next round's proposal parent
//     onto", playing `make_cut_proposal`'s convenience role
//     -> highest_safe_cut: Digest
//   sent_timeouts: BTreeSet<CutRound>  -- timed_out
//     -> timed_out: BTreeSet<CutRound>  (Fig. 2: one per-current-round bool, reset on
//        round entry; here, one persistent latch per round, since this engine tracks
//        many rounds' in-flight messages concurrently rather than only curr_round)
//   decides_by_round: BTreeMap<CutRound, Decide>  -- committed[r]
//     -> committed: BTreeMap<CutRound, Decide>  (key presence IS committed[r]; the
//        value is the quorum-crossing `Decide` itself, needed by `try_commit_round`'s
//        cut-id comparison against `leader_cut_by_round`)
//   sent_decide_rounds: BTreeSet<CutRound>  -- voted ("has this party sent its own
//     ⟨commit, r⟩ yet" -- `Decide` IS the paper's commit message)
//     -> voted: BTreeSet<CutRound>
//   voted_cut_rounds: BTreeSet<CutRound>  -- NO Fig.-2 equivalent: this is this
//     engine's OWN CutVote-sent latch, standing in for RBC-echo participation, which
//     the paper's `rb-broadcast` primitive absorbs and never names as a party-visible
//     step at all
//     -> sent_cut_votes: BTreeSet<CutRound>  (renamed FROM `voted_cut_rounds`
//        specifically to stop colliding with the real `voted` above, which means
//        something else -- see this module's own task report for the conflict this
//        surfaced and why both names changed together)
// Left deliberately UNCHANGED (not among the names the task asked to align -- `safe`,
// `voted`, `timed_out`, `committed`): `certified_timed_out` (closest Fig.-2 analogue:
// `disabled`), `sent_commit_rounds` (Fig. 2 names no flag for this at all -- it is the
// **Deliver** action's own local, one-shot latch, distinct from `committed[r]` itself),
// `cut_round_by_id`, `leader_cut_by_round`, `cut_proposals`, `pending_cut_children`,
// every timeout-ladder/aggregator/repair field.
//
// Architecture: `CutEngine` is a pure state machine. It never holds a `ReliableSender`,
// a `Wire`, a `LaneManager`, a `Store`, or any channel -- upstream's `Core` holds all of
// these, and removing them is the entire point of this port. Every public method
// returns `Vec<CutEffect>` (see effects.rs) in place of upstream's direct
// `self.network.broadcast(...)`/`self.tx_committer.send(...)` calls; the caller (a
// not-yet-written production core, or this module's own tests) executes those effects
// against the real transport/timer/committer. This mirrors
// `primary/src/vantage/agb.rs`'s `AgbEngine` and `primary/src/vantage/control.rs`'s
// `ControlLog` -- both effect-returning, network-free engines already in this crate --
// rather than upstream's own style.
//
// Two oracles are *given* to the engine rather than held by it:
//   1. `tips: &Cut` -- upstream's `current_cut()` reads `self.current_certified_tips`,
//      an Autobahn-DAG field this engine does not have. The caller builds this fresh,
//      per call, from `LaneManager::c_candidate(author)` for each committee member
//      (`BlockRef = (PublicKey, Height, Digest)` maps onto `(author, Proposal
//      { header_digest, height })`) -- `CutEngine` never imports `LaneManager`.
//   2. `oracle: &dyn TipOracle` -- the f+1 tip-availability gate (deviation 3 below)
//      needs to ask "have I seen enough evidence for this tip", which is also
//      `LaneManager` state. The trait is defined here; the not-yet-written production
//      core implements it over `LaneManager::is_q_available(r, validity_threshold())`.
//      `&dyn` (not `&impl`) deliberately: nearly every one of the 25 methods can
//      transitively reach a vote or a re-propose decision (see `try_propose_cut_for_
//      current_round`'s callers), so `tips`/`oracle` thread through almost the entire
//      call graph -- a generic parameter would have to repeat on every one of those
//      signatures for no benefit, since the engine only ever calls one method on it.
//
// Required deviations from upstream (beyond the two oracle-passing changes above,
// which are themselves required -- see the task brief):
//   1. Leader schedule: `leader_for_round` below, not `leader.rs`/`LeaderElector`.
//   2. Prunable state is `BTreeMap`/`BTreeSet`, never `HashMap`/`HashSet`; `prune_below`
//      is the one `split_off`-based GC entry point (no `retain` anywhere).
//   3. The f+1 tip-availability gate in `process_cut_proposal`, behind `gate_tips`
//      (defaults to `true`, paper-faithful; upstream never checks `proposal.tips` at
//      all -- `gate_tips: false` reproduces that).
//   4. `process_cut_proposal`'s internal queue no longer aborts (and silently drops
//      every already-dequeued sibling) on one bad proposal -- see that method's doc
//      comment.
//   5. No Autobahn types anywhere (`Slot`/`View`/`QC`/`TC`/`CommitQC`/`ConsensusMessage`);
//      `CutRound` (from `simpleit::messages`) is the one round type. `agb::proposer`
//      takes a `View` -- `leader_for_round` is the one, explicitly documented place
//      that type ever appears, as a same-width conversion (`View = CutRound = u64`).
//
// None of the ported methods are `async fn`, unlike upstream. Every reason upstream's
// versions were async is gone here: network broadcasts and the committer channel send
// are replaced by effect values (no `.await` left to perform), and the three trivial
// `Timeout::new`/`TimeoutAccept::new`/`Decide::new` upstream `async fn` constructors
// (stage 1, primary/src/simpleit/messages.rs) are bypassed via plain struct literals
// (their fields are all `pub`) rather than requiring an async context for a
// constructor that does no actual `.await` work. This matches `AgbEngine`/`ControlLog`,
// neither of which has a single `async fn` either.
//
// REPAIR (not upstream, not the paper either -- new machinery, added after the port to
// close a liveness gap this port's OWN message pattern creates that a real reliable
// broadcast would not have): the paper's `rb-broadcast`/`rb-deliver` gives every
// correct party the SAME `proposal[r]` once any correct party has it at all (RBC's
// totality property). This port's stand-in for that -- `CutProposal` broadcast plus
// all-to-all `CutVote` echo (`process_cut_vote`) -- carries no such guarantee on its
// own: a `CutVote` names only a `round` and `cut_id`, so a party can cross
// `mint_threshold` (and so call `mark_cut_safe`) for a round whose own `CutProposal`
// it never received -- `safe_cut_parent` cannot then resolve a citing child's parent
// through `cut_round_by_id`, so the chain stalls rather than committing (see this
// module's own test `missing_proposal_stalls_the_chain_rather_than_skipping_a_round`).
// This liveness gap predates the Figure-2 rewrite above and is UNCHANGED by it (before
// the rewrite, the identical gap existed one step later: a party could accept a PEER's
// certificate for round r without ever having received round r's own `CutProposal`
// either) -- only the trigger that surfaces it moves, from certificate-acceptance to
// locally reaching `safe`. `ensure_cut_fetch`/`on_cut_fetch`/`on_cut_serve` restore the
// missing property operationally, by pull rather than by broadcast -- mirroring
// `vantage::control::ControlLog`'s own `ensure_fetch`/`on_control_fetch`/
// `on_control_serve` for its carrier bodies exactly (see each method's own doc comment
// for the parallel, and `mark_cut_safe`/`process_cut_proposal` for the two triggers).
// The fetch TARGET set at `mark_cut_safe`'s trigger is exactly the `mint_threshold`-many
// voters THIS party itself counted (`CutVoteAggregator::append`'s own returned
// `voters`) -- the closest available analogue to the removed certificate's `votes`
// list, but (unlike that list) never transmitted or trusted from a peer: it is this
// party's own first-hand record of who it received a vote from, so asking exactly
// those peers for the proposal carries no less assurance than the old design did.
//
// BRACHA VARIANT (separate task, separate upstream branch, `simpleit/
// Bracha-Mempool-Simple-IT` -- fetched as the same `simpleit` remote, read-only, never
// checked out/merged/applied; primary/src/core.rs there, cited per method below):
// arXiv:2606.14404 Table 1/2 + Corollary 5 name TWO cut-consensus variants -- "Opt"
// (everything above this paragraph) and "S" (Bracha-RBC), the latter trading Opt's
// larger `mint_threshold` first-hand census for a second, plain-`quorum_threshold`
// echo round, in exchange for never needing more than `quorum_threshold`-many live
// authors to make progress (Opt's `mint_threshold` can exceed `quorum_threshold` at
// large committees -- see `aggregators::mint_threshold`'s own doc comment). `variant:
// Variant` (below) selects between them; `Variant::Opt` is the default and is
// byte-for-byte the engine described above -- every method already documented above
// is unchanged by this addition, and reading them, `variant` never appears.
//
// Mechanism (`Variant::Bracha`): `process_cut_vote`'s own first-hand `CutVote` census
// -- the SAME `cut_vote_aggregators` map `Variant::Opt` uses, just thresholded at
// plain `quorum_threshold` instead of `mint_threshold` (see `CutVoteAggregator::
// append`'s `threshold` parameter) -- crossing threshold broadcasts one-shot
// `CutReady(round, cut_id, author)` (`broadcast_cut_ready`, latched once per round by
// `sent_cut_ready`, mirroring `sent_cut_votes`'s identical shape) instead of calling
// `mark_cut_safe` directly. Every party then counts `CutReady`s first-hand, exactly
// like `CutVote`s, into its own `cut_ready_aggregators` (`process_cut_ready`); crossing
// `quorum_threshold` there calls `mark_cut_safe` with the `CutReady` senders as the
// fetch-witness set -- the identical REPAIR mechanism above, just fed from the second
// echo round's own census instead of the first's. `mark_cut_safe` itself, and every
// downstream step it reaches (self-Decide, `try_commit_round`, the timeout ladder,
// `try_propose_cut_for_current_round`, `schedule_cut_timer`), is IDENTICAL for both
// variants -- neither reads `self.variant` anywhere.
//
// Upstream citations: `Core::process_cut_vote` (primary/src/core.rs:417-433 there)
// crossing `quorum_threshold` calls `broadcast_cut_ready` (:435-459), which -- unlike
// this port -- unconditionally re-broadcasts (no per-round latch upstream); `Core::
// process_cut_ready` (:646-679) mints+broadcasts a `CutCertificate` on crossing
// `quorum_threshold` at its `CutReadyAggregator` (primary/src/aggregators.rs:59-65,
// 138-182 there) -- the exact same Fig.-2-rewrite defect the module doc comment above
// already fixed for the Opt variant's own certificate, fixed here identically (first-
// hand `mark_cut_safe`, no certificate ever minted or broadcast). Upstream's own
// `CutVoteAggregator` on THIS branch (:54-57, 114-136) is a separately-shaped type
// from the Opt branch's (returns a bare `bool`, no witness list -- Bracha's
// certificate design never needed one); this port's `CutVoteAggregator` unifies both
// into one type via the `threshold` parameter, as noted above.
//
// NOT reproduced (deliberate, flagged in the accompanying task report, not a defect
// in the port): upstream's `Core::process_cut_ready` has no f+1-ready amplification
// step at all -- unlike its OWN `handle_timeout_accept_action`'s f+1-triggered
// re-broadcast of `TimeoutAccept` (ported above, unchanged, and shared by both
// variants), a `CutReady` census that reaches f+1 (short of the full
// `quorum_threshold` needed to mark safe) never causes upstream to do anything at all
// -- no re-broadcast, no amplification, nothing. This port reproduces that absence
// faithfully rather than inventing an amplification step upstream itself does not
// have.

use crate::error::{DagError, DagResult};
use crate::messages::Proposal;
use crate::simpleit::aggregators::{
    mint_threshold, CutReadyAggregator, CutVoteAggregator, DecideAggregator,
    TimeoutAcceptAggregator, TimeoutAggregator,
};
use crate::simpleit::effects::{CutEffect, CutOut};
use crate::simpleit::messages::{
    Cut, CutProposal, CutReady, CutRound, CutVote, Decide, Timeout, TimeoutAccept, TimeoutCert,
};
use crate::vantage::agb;
use config::{Committee, Stake};
use crypto::{Digest, Hash as _, PublicKey};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

/// The messages `CutEngine` consumes, plus the one non-message input (a previously
/// armed timer firing). Wiring this to the real wire enum (`PrimaryMessage`) and to
/// `vantage::node::Inbound` is a separate, later task -- this type is deliberately not
/// related to either.
///
/// No certificate-shaped variant exists here (see the module doc comment's "FIGURE-2
/// REWRITE" paragraph) -- this is this engine's ENTIRE inbound message surface, so the
/// absence is a compile-time guarantee, not merely a run-time one: nothing outside this
/// enum can ever reach `CutEngine::handle` at all, and `handle`'s own match over it is
/// exhaustive with no wildcard arm (see `mod tests`'
/// `inbound_has_no_certificate_shaped_variant` for a test that fails to COMPILE, not
/// merely to pass, if a certificate-shaped variant is ever added back here). `CutReady`
/// (BRACHA VARIANT ADDITION, see the module doc comment's "BRACHA VARIANT" paragraph)
/// is not certificate-shaped either -- same single-named-author shape as `CutVote` --
/// and is included in that same exhaustive, no-wildcard guarantee.
#[derive(Clone, Debug)]
pub enum Inbound {
    CutProposal(CutProposal),
    CutVote(CutVote),
    Decide(Decide),
    Timeout(Timeout),
    TimeoutAccept(TimeoutAccept),
    /// Bracha variant only (`Variant::Bracha`) -- Bracha-RBC's own second echo round.
    /// Never constructed under `Variant::Opt`; see `CutEngine::process_cut_ready`.
    CutReady(CutReady),
    /// A previously `CutEffect::ArmTimer`-requested deadline for this round has
    /// elapsed. Corresponds to upstream's `cut_timer_futures` yielding a round.
    TimerFired(CutRound),
    /// A peer's request for the `CutProposal` identified by `(round, cut_id)`.
    /// Repair machinery, not upstream (upstream has no equivalent -- see this
    /// module's doc comment). The requester is carried explicitly, mirroring
    /// `vantage::node::Inbound::ControlFetch`.
    CutFetch(CutRound, Digest, /* requester */ PublicKey),
    /// A peer's answer to our own fetch. Mirrors `vantage::node::Inbound::
    /// ControlServe`.
    CutServe(CutProposal),
}

/// Tip-availability oracle for the f+1 gate (deviation 3): "has this party itself seen
/// at least f+1 evidence for `tip`, authored by `author`". `CutEngine` never
/// implements this -- the production core implements it over
/// `LaneManager::is_q_available(&(author, tip.height, tip.header_digest), committee.
/// validity_threshold())`; a test implements it directly (see `mod tests` below).
pub trait TipOracle {
    fn available_at_validity(&self, author: &PublicKey, tip: &Proposal) -> bool;
}

/// BRACHA VARIANT ADDITION -- see the module doc comment's "BRACHA VARIANT" paragraph
/// for the full mechanism. Selects which cut-consensus census/echo shape
/// `CutEngine::process_cut_vote` (and, for `Bracha`, `process_cut_ready`) runs;
/// `CutEngine::new`'s default (`Opt`) is byte-for-byte the engine as it existed before
/// this addition. `config::Protocol::SimpleItBracha` selects `Bracha`; every other
/// protocol selects (or defaults to) `Opt` -- see `simpleit::node::SimpleItCore::build`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Variant {
    /// arXiv:2606.14404's "Opt" cut-consensus variant: a single first-hand `CutVote`
    /// census, thresholded at `mint_threshold`, reaches `mark_cut_safe` directly. Every
    /// method documented in the module doc comment ABOVE the "BRACHA VARIANT"
    /// paragraph describes this variant.
    #[default]
    Opt,
    /// arXiv:2606.14404's "S" cut-consensus variant (Bracha-RBC, Table 1/2 +
    /// Corollary 5): a first-hand `CutVote` census, thresholded at plain
    /// `quorum_threshold`, broadcasts `CutReady`; a second first-hand `CutReady`
    /// census, also at `quorum_threshold`, reaches `mark_cut_safe`.
    Bracha,
}

/// The Simple-IT cut-consensus state machine. See the module doc comment above for the
/// architecture; see each method below for its upstream provenance.
pub struct CutEngine {
    name: PublicKey,
    committee: Committee,
    /// Upstream `Core::timeout_delay`: the relative delay `schedule_cut_timer` arms
    /// each round's timeout deadline for.
    timeout_delay: u64,
    /// Deviation 3: gates `process_cut_proposal`'s vote on f+1 tip availability when
    /// `true` (the paper-faithful default). `false` reproduces upstream exactly (votes
    /// without ever inspecting `proposal.tips`).
    gate_tips: bool,
    /// BRACHA VARIANT ADDITION -- see `Variant`'s own doc comment. `CutEngine::new`'s
    /// default (`Variant::Opt`) is byte-for-byte the engine as it existed before this
    /// field was added.
    variant: Variant,

    /// Upstream `Core::cut_round`. Starts at 1 (confirmed by reading upstream's own
    /// `Core::spawn` constructor -- NOT 0; round 0 is reserved as `safe_cut_parent`'s
    /// genesis-parent sentinel and is never an actual contested round; see
    /// `leader_for_round`'s doc comment for why this discrepancy from the task brief
    /// does not affect the round -> leader mapping either way).
    cut_round: CutRound,
    /// Upstream `Core::highest_certified_cut`, renamed `highest_safe_cut`: the paper
    /// (Fig. 2) names no single variable for "the cut the next round's proposal should
    /// parent onto", but this field's role is unchanged by the Fig.-2 rewrite -- only
    /// what sets it changed (`mark_cut_safe`'s own local threshold-crossing, never a
    /// received certificate; see the module doc comment's field-mapping table).
    highest_safe_cut: Digest,
    /// The floor `prune_below` was last called with (0 if never). Doubles as
    /// `sanitize_timeout_accept`'s staleness floor -- see that method's doc comment for
    /// why this replaces upstream's Autobahn-typed `gc_round: Height`.
    gc_floor: CutRound,

    /// Upstream `cut_vote_aggregators: HashMap<Digest, CutVoteAggregator>`. Re-keyed
    /// per deviation 2: every read site (`process_cut_vote`) already has `vote.round`
    /// in hand, so keying by `(CutRound, Digest)` costs nothing at the lookup sites and
    /// makes this `prune_below`-able by `split_off`. Round-prunable; covered.
    cut_vote_aggregators: BTreeMap<(CutRound, Digest), CutVoteAggregator>,
    /// BRACHA VARIANT ADDITION (`Variant::Bracha` only -- see the module doc
    /// comment's "BRACHA VARIANT" paragraph): first-hand `CutReady` census, mirroring
    /// `cut_vote_aggregators` exactly (same key shape, same `split_off`-pruning
    /// reason). Never populated under `Variant::Opt` -- that variant never
    /// constructs or accepts a `CutReady` at all (see `process_cut_ready`'s own
    /// variant guard). Round-prunable; covered.
    cut_ready_aggregators: BTreeMap<(CutRound, Digest), CutReadyAggregator>,
    /// Upstream `timeouts_aggregators: HashMap<u64, TimeoutAggregator>`. Already
    /// round-keyed; container swap only. Round-prunable; covered.
    timeouts_aggregators: BTreeMap<CutRound, TimeoutAggregator>,
    /// Upstream `timeout_accept_aggregators: HashMap<u64, TimeoutAcceptAggregator>`.
    /// Already round-keyed; container swap only. Round-prunable; covered.
    timeout_accept_aggregators: BTreeMap<CutRound, TimeoutAcceptAggregator>,
    /// Upstream `cut_proposals: HashMap<Digest, CutProposal>`. Re-keyed per deviation 2
    /// exactly as `cut_vote_aggregators` above: every read site
    /// (`process_cut_proposal`'s dedup check, `emit_commit_to_committer`) already has
    /// the round in hand. Round-prunable; covered. `prune_below` also uses this map's
    /// own split_off to drive `cut_round_by_id`'s cleanup (see that field).
    cut_proposals: BTreeMap<(CutRound, Digest), CutProposal>,
    /// Upstream `pending_cut_children: HashMap<Digest, Vec<CutProposal>>` (keyed by the
    /// *parent* cut's digest). Re-keyed as `(CutRound, Digest)` where the round is each
    /// buffered *child* proposal's own round and the digest is still the cited parent
    /// -- a strict refinement of upstream's grouping (splits one upstream bucket into
    /// one bucket per distinct child round citing that parent; every buffered proposal
    /// upstream would ever have held is still held, just possibly in a different
    /// bucket alongside fewer/other siblings), not a behavior change. This makes
    /// `prune_below` a clean `split_off` (a child whose own round is already GC'd can
    /// never be validly processed anyway). The cost: reparenting when a parent becomes
    /// known (`process_cut_proposal`) can no longer do a single `HashMap::remove`,
    /// since the round component of a pending child's key is not known in advance from
    /// the parent's digest alone -- it scans this map's (bounded-by-current-backlog)
    /// keys for matches. That scan is a lookup, not the GC-pruning operation the "no
    /// retain" rule targets, so it is not a violation of that rule. Round-prunable;
    /// covered.
    pending_cut_children: BTreeMap<(CutRound, Digest), Vec<CutProposal>>,
    /// Upstream `cut_round_by_id: HashMap<Digest, u64>`. Its one read site
    /// (`safe_cut_parent`) looks up a round FROM a digest -- the opposite direction
    /// from every other map above, so it cannot itself be re-keyed by round without
    /// destroying that lookup's whole purpose (the round is exactly the unknown being
    /// looked up). Kept `Digest`-keyed (switched to `BTreeMap` for consistency, not for
    /// `split_off`-ability on its own key). NOT independently round-prunable by its own
    /// key -- see `prune_below`'s doc comment for how it is still covered, by riding
    /// `cut_proposals`' split_off (the two maps are populated together, one insert
    /// each, only by `record_cut_proposal`).
    cut_round_by_id: BTreeMap<Digest, CutRound>,
    /// Upstream `leader_cut_by_round: HashMap<u64, Digest>`. Already round-keyed.
    /// Round-prunable; covered.
    leader_cut_by_round: BTreeMap<CutRound, Digest>,
    /// Upstream `cut_certificates: HashMap<u64, CutCertificate>`, renamed `safe` and
    /// re-typed `BTreeMap<CutRound, Digest>` -- Fig. 2's `safe[r]` state variable: key
    /// presence IS `safe[r] = true`; the value is `proposal[r]`'s id (Fig. 2 tracks
    /// that separately, in `proposal[r]`, but the two are only ever established
    /// together in this engine -- see `mark_cut_safe`). Populated ONLY by
    /// `mark_cut_safe`, reached ONLY from `process_cut_vote`'s own
    /// `CutVoteAggregator` crossing `mint_threshold` on first-hand-verified votes --
    /// never from a received message asserting a round is safe (there is no such
    /// message; see the module doc comment's "FIGURE-2 REWRITE" paragraph). Already
    /// round-keyed. Round-prunable; covered.
    safe: BTreeMap<CutRound, Digest>,
    /// Upstream `decide_aggregators: HashMap<(u64, Digest), DecideAggregator>`. Already
    /// tuple-keyed by round; container swap only. Round-prunable; covered.
    decide_aggregators: BTreeMap<(CutRound, Digest), DecideAggregator>,
    /// Upstream `decides_by_round: HashMap<u64, Decide>`, renamed `committed` -- Fig.
    /// 2's `committed[r]` state variable: key presence IS `committed[r] = true`; the
    /// value is the quorum-crossing `Decide` itself, needed by `try_commit_round`'s
    /// cut-id comparison against `leader_cut_by_round`. Already round-keyed.
    /// Round-prunable; covered.
    committed: BTreeMap<CutRound, Decide>,
    /// Upstream `voted_cut_rounds: HashSet<u64>`, renamed `sent_cut_votes`. NOT Fig.
    /// 2's `voted` (that is the field below, `voted`) -- this is this engine's OWN
    /// CutVote-sent latch, standing in for RBC-echo participation, a step the paper's
    /// `rb-broadcast` primitive absorbs and never names. Renamed specifically to stop
    /// colliding with the real `voted` below now that the Fig.-2 alignment makes the
    /// distinction load-bearing -- see the module doc comment's field-mapping table.
    /// Round-prunable; covered.
    sent_cut_votes: BTreeSet<CutRound>,
    /// BRACHA VARIANT ADDITION (`Variant::Bracha` only): this party's own one-shot
    /// `CutReady`-broadcast latch per round, mirroring `sent_cut_votes` immediately
    /// above exactly (same per-round-not-per-current-round-bool shape, same
    /// rationale). Never populated under `Variant::Opt`. Round-prunable; covered.
    sent_cut_ready: BTreeSet<CutRound>,
    /// Upstream `proposed_cut_rounds: HashSet<u64>`. Round-prunable; covered.
    proposed_cut_rounds: BTreeSet<CutRound>,
    /// Upstream `sent_decide_rounds: HashSet<u64>`, renamed `voted` -- Fig. 2's `voted`
    /// flag ("has this party sent its own `⟨commit, r⟩` yet"; `Decide` IS the paper's
    /// commit message, per messages.rs's own doc comment on `Decide`). Fig. 2 resets
    /// this to `false` on every round entry (it is a single per-current-round bool
    /// there); here it is one persistent latch per round, for the same reason
    /// `timed_out` below is (this engine tracks many rounds' in-flight messages
    /// concurrently, not only `curr_round`). Round-prunable; covered.
    voted: BTreeSet<CutRound>,
    /// Upstream `sent_commit_rounds: HashSet<u64>`. Fig. 2 names no flag for this at
    /// all -- it is the **Deliver** action's own one-shot local latch (guards
    /// `emit_commit_to_committer`), distinct from `committed[r]` itself (this engine's
    /// `committed`, above): a round can be `committed` for a while before this engine
    /// has locally resolved enough to deliver it (see `node.rs`'s `commit_queue`).
    /// Round-prunable; covered.
    sent_commit_rounds: BTreeSet<CutRound>,
    /// Upstream `sent_timeouts: HashSet<u64>`, renamed `timed_out` -- Fig. 2's
    /// `timed_out` flag ("has this party raised its own timeout flag for the current
    /// round"). Same per-round-latch-vs.-per-current-round-bool distinction as `voted`
    /// above. Round-prunable; covered.
    timed_out: BTreeSet<CutRound>,
    /// Upstream `sent_timeout_accepts: HashSet<u64>`. Round-prunable; covered.
    sent_timeout_accepts: BTreeSet<CutRound>,
    /// Upstream `certified_timed_out: HashSet<u64>`. Closest Fig.-2 analogue:
    /// `disabled[r]` (set upon `rn_confirm(⟨timeout, r⟩)`, i.e. exactly this engine's
    /// own timeout-CERTIFIED state) -- deliberately NOT renamed to `disabled`: the
    /// task's paper-alignment list names `safe`/`voted`/`timed_out`/`committed`
    /// specifically, not `disabled`, and this name already says precisely what it
    /// tracks (the TimeoutCert-backed certified state, as opposed to `timed_out`
    /// above, this party's own un-certified raised flag) without the risk of a
    /// same-named-but-not-quite field pair the `voted`/`sent_cut_votes` split above
    /// was written to avoid. Round-prunable; covered.
    certified_timed_out: BTreeSet<CutRound>,
    /// Upstream `scheduled_cut_timers: HashSet<u64>`. Round-prunable; covered.
    scheduled_cut_timers: BTreeSet<CutRound>,

    // --- Cut-proposal repair (not upstream -- see the module doc comment) ---
    /// Outstanding `CutProposal` fetches, mapped to the cut round (this engine's own
    /// retry clock, `self.cut_round`) we last fanned the request out in. Mirrors
    /// `control::ControlLog::pending_fetch` exactly -- see that field's doc comment
    /// for why this must be retryable (a one-shot latch left a request permanently
    /// stuck if its one round of targets never answered) -- keyed by `(CutRound,
    /// Digest)` so `split_off` prunes it in `prune_below`, same as every other
    /// round-keyed field above.
    pending_cut_fetch: BTreeMap<(CutRound, Digest), CutRound>,
    /// Per-requester fetch-answered dedup, mirroring `control::ControlLog::
    /// fetch_answered` exactly. Round-prunable; covered.
    fetch_answered: BTreeSet<(CutRound, Digest, PublicKey)>,
}

impl CutEngine {
    pub fn new(name: PublicKey, committee: Committee, timeout_delay: u64) -> Self {
        Self {
            name,
            committee,
            timeout_delay,
            gate_tips: true,
            variant: Variant::default(),
            cut_round: 1,
            highest_safe_cut: Digest::default(),
            gc_floor: 0,
            cut_vote_aggregators: BTreeMap::new(),
            cut_ready_aggregators: BTreeMap::new(),
            timeouts_aggregators: BTreeMap::new(),
            timeout_accept_aggregators: BTreeMap::new(),
            cut_proposals: BTreeMap::new(),
            pending_cut_children: BTreeMap::new(),
            cut_round_by_id: BTreeMap::new(),
            leader_cut_by_round: BTreeMap::new(),
            safe: BTreeMap::new(),
            decide_aggregators: BTreeMap::new(),
            committed: BTreeMap::new(),
            sent_cut_votes: BTreeSet::new(),
            sent_cut_ready: BTreeSet::new(),
            proposed_cut_rounds: BTreeSet::new(),
            voted: BTreeSet::new(),
            sent_commit_rounds: BTreeSet::new(),
            timed_out: BTreeSet::new(),
            sent_timeout_accepts: BTreeSet::new(),
            certified_timed_out: BTreeSet::new(),
            scheduled_cut_timers: BTreeSet::new(),
            pending_cut_fetch: BTreeMap::new(),
            fetch_answered: BTreeSet::new(),
        }
    }

    /// Deviation 3's switch. Defaults to `true` (paper-faithful); pass `false` to
    /// reproduce upstream's blind-vote behavior.
    pub fn with_gate_tips(mut self, gate_tips: bool) -> Self {
        self.gate_tips = gate_tips;
        self
    }

    /// BRACHA VARIANT ADDITION's switch -- see `Variant`'s own doc comment. Defaults
    /// to `Variant::Opt` (byte-for-byte the engine as it existed before this method
    /// was added); pass `Variant::Bracha` to run the Bracha-RBC variant instead.
    pub fn with_variant(mut self, variant: Variant) -> Self {
        self.variant = variant;
        self
    }

    /// Single dispatch entry point over `Inbound` -- not itself one of the 25 ported
    /// methods (upstream has no equivalent single function; its `run()` select-loop,
    /// not ported, plays this role there), but required by the "engine consumes
    /// `Inbound`" architecture. Mirrors upstream's own dispatch shape exactly:
    /// `TimeoutAccept` is `sanitize_timeout_accept`-checked before `process_timeout_
    /// accept` (matching upstream's sanitize-then-process pattern), a wire `Timeout`
    /// goes through `handle_timeout` (not `process_timeout` directly -- that direct
    /// path is reserved for `process_cut_timer`'s own locally-generated timeout,
    /// exactly as upstream keeps the two named entry points distinct).
    pub fn handle(
        &mut self,
        inbound: Inbound,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        match inbound {
            Inbound::CutProposal(p) => self.process_cut_proposal(p, tips, oracle),
            Inbound::CutVote(v) => self.process_cut_vote(v, tips, oracle),
            Inbound::CutReady(r) => self.process_cut_ready(r, tips, oracle),
            Inbound::Decide(d) => self.process_decide(d),
            Inbound::Timeout(t) => self.handle_timeout(t, tips, oracle),
            Inbound::TimeoutAccept(a) => {
                if self.sanitize_timeout_accept(&a).is_err() {
                    return Vec::new();
                }
                self.process_timeout_accept(a, tips, oracle)
            }
            Inbound::TimerFired(r) => self.process_cut_timer(r, tips, oracle),
            Inbound::CutFetch(round, cut_id, requester) => {
                self.on_cut_fetch(requester, round, cut_id)
            }
            Inbound::CutServe(proposal) => self.on_cut_serve(proposal, tips, oracle),
        }
    }

    /// Deviation 1: upstream's `leader.rs`/`LeaderElector`/`fixed_leader_order` are not
    /// ported; every leader lookup in this engine goes through this one function.
    ///
    /// The mapping: `agb::proposer(committee, view)` computes `names[(view - 1) % n]`
    /// over `committee.authorities.keys()` (`BTreeMap` order, i.e. raw `PublicKey`
    /// byte order) and requires `view >= 1`. Upstream computes `leaders[round % n]`
    /// over a *separately sorted* `Vec` (by `node_id` if every authority has one, else
    /// also raw `PublicKey` order -- `fixed_leader_order`), with `round` always >= 1 in
    /// practice (confirmed by reading upstream's `Core::spawn`: `cut_round: 1`, and it
    /// only ever increases; `round` is never 0 for an actual contested round, only as
    /// `safe_cut_parent`'s parent-round sentinel).
    ///
    /// Choosing `view = round + 1` makes `(view - 1) % n == round % n` an *identity*
    /// (not merely mod-equivalent -- `view - 1` is literally `round`), for every
    /// `round >= 0`. So for every round upstream would ever actually query, this
    /// reproduces the exact same 0-indexed slot into whichever list is in play -- only
    /// the list's *ordering* differs (raw key order here vs upstream's node-id-aware
    /// order there), which is exactly deviation 1's "use ours, not theirs" instruction,
    /// never the arithmetic. `View` (upstream's `u64` alias, from `primary::primary`)
    /// never appears as a named type anywhere in this module -- `round + 1` is a
    /// `CutRound` (`= u64`) value that coerces to `agb::proposer`'s `View` (`= u64`)
    /// parameter because the two are the same underlying type; this call site is the
    /// only place that coercion ever happens.
    fn leader_for_round(&self, round: CutRound) -> PublicKey {
        agb::proposer(&self.committee, round + 1)
    }

    /// `pub` (production wiring, `simpleit::node::SimpleItCore`): a read-only accessor
    /// so the wiring layer can compute its own GC floor (`cut_round.saturating_sub
    /// (gc_window)`) for `prune_below` below -- neither `cut_round` itself nor any
    /// other progress indicator was otherwise exposed. Not one of the 25 ported
    /// methods; upstream has no equivalent (its own GC, if any, would have lived
    /// inside the un-ported `run()` loop with direct field access).
    pub fn cut_round(&self) -> CutRound {
        self.cut_round
    }

    /// GC floor: `split_off`-prune every round-prunable structure at `floor`. No-op if
    /// `floor` is at or behind the current floor (matching `AgbEngine::gc_below`/
    /// `ControlLog::gc_below`'s own monotonic-guard shape). Not one of the 25 ported
    /// methods -- upstream has no GC for this state at all; this is the standing
    /// project rule ("prunable structures... `split_off` at floor, never `retain`
    /// scans") applied proactively.
    ///
    /// `cut_round_by_id` is the one field with no `split_off` of its own (see that
    /// field's doc comment for why: it is keyed by `Digest`, needed for
    /// `safe_cut_parent`'s digest -> round lookup). Instead of a `.retain` scan over
    /// it, this reads the digests `cut_proposals`' own split_off is about to discard --
    /// `record_cut_proposal` populates both maps together for the same cut_id, so the
    /// two are always in lockstep -- and removes exactly those keys: cost proportional
    /// to what is being dropped, not to what survives.
    pub fn prune_below(&mut self, floor: CutRound) {
        if floor <= self.gc_floor {
            return;
        }

        let kept_cut_proposals = self.cut_proposals.split_off(&(floor, Digest::default()));
        for (_, digest) in self.cut_proposals.keys() {
            self.cut_round_by_id.remove(digest);
        }
        self.cut_proposals = kept_cut_proposals;

        self.cut_vote_aggregators = self
            .cut_vote_aggregators
            .split_off(&(floor, Digest::default()));
        // BRACHA VARIANT ADDITION: always spliced, even under `Variant::Opt` (where
        // both maps/sets stay empty the whole run) -- a no-op `split_off` on an empty
        // map, mirroring how every OTHER round-prunable field here is unconditional
        // regardless of which upstream deviation populates it.
        self.cut_ready_aggregators = self
            .cut_ready_aggregators
            .split_off(&(floor, Digest::default()));
        self.pending_cut_children = self
            .pending_cut_children
            .split_off(&(floor, Digest::default()));
        self.decide_aggregators = self
            .decide_aggregators
            .split_off(&(floor, Digest::default()));

        self.timeouts_aggregators = self.timeouts_aggregators.split_off(&floor);
        self.timeout_accept_aggregators = self.timeout_accept_aggregators.split_off(&floor);
        self.leader_cut_by_round = self.leader_cut_by_round.split_off(&floor);
        self.safe = self.safe.split_off(&floor);
        self.committed = self.committed.split_off(&floor);
        self.sent_cut_votes = self.sent_cut_votes.split_off(&floor);
        self.sent_cut_ready = self.sent_cut_ready.split_off(&floor);
        self.proposed_cut_rounds = self.proposed_cut_rounds.split_off(&floor);
        self.voted = self.voted.split_off(&floor);
        self.sent_commit_rounds = self.sent_commit_rounds.split_off(&floor);
        self.timed_out = self.timed_out.split_off(&floor);
        self.sent_timeout_accepts = self.sent_timeout_accepts.split_off(&floor);
        self.certified_timed_out = self.certified_timed_out.split_off(&floor);
        self.scheduled_cut_timers = self.scheduled_cut_timers.split_off(&floor);

        self.pending_cut_fetch = self
            .pending_cut_fetch
            .split_off(&(floor, Digest::default()));
        self.fetch_answered =
            self.fetch_answered
                .split_off(&(floor, Digest::default(), PublicKey::default()));

        self.gc_floor = floor;
    }

    /// Re-fan an unanswered `CutProposal` fetch every this many cut rounds -- mirrors
    /// `control::ControlLog::FETCH_RETRY_ROUNDS` exactly: cut rounds are this
    /// engine's own natural clock (`self.cut_round`), exactly as control rounds are
    /// `ControlLog`'s, and the identical retry rationale applies verbatim (a
    /// one-shot latch left a request permanently unanswered if its one round of
    /// targets never answered -- re-asking is cheap next to that).
    const FETCH_RETRY_ROUNDS: CutRound = 8;

    /// Every other committee member (never `self.name`) -- the fallback fetch-target
    /// set for `process_cut_proposal`'s "parent unknown" trigger, which (unlike
    /// `mark_cut_safe`'s vote-driven trigger, which asks exactly the witnesses it
    /// counted) has no per-voter evidence to narrow to. See that call site's own
    /// comment for the full justification.
    fn all_other_committee_members(&self) -> Vec<PublicKey> {
        self.committee
            .authorities
            .keys()
            .filter(|k| **k != self.name)
            .copied()
            .collect()
    }

    /// Request the `CutProposal` identified by `(round, cut_id)` from every one of
    /// `targets`, at most once every `FETCH_RETRY_ROUNDS` cut rounds for that exact
    /// pair -- mirrors `control::ControlLog::ensure_fetch` exactly (see its doc
    /// comment for the retry rationale). No-op if we already hold the proposal
    /// (`cut_round_by_id`) or `round` is already pruned. Called from both repair
    /// triggers: `mark_cut_safe` (an exact round, read directly off the just-crossed
    /// vote threshold, with `targets` the exact witnesses counted) and
    /// `process_cut_proposal`'s buffering branch (a best-effort round guess -- see
    /// that call site for why an exact round isn't available there).
    fn ensure_cut_fetch(
        &mut self,
        round: CutRound,
        cut_id: &Digest,
        targets: Vec<PublicKey>,
    ) -> Vec<CutEffect> {
        if round < self.gc_floor || self.cut_round_by_id.contains_key(cut_id) {
            return Vec::new();
        }
        let key = (round, cut_id.clone());
        match self.pending_cut_fetch.get(&key) {
            Some(&last) if self.cut_round.saturating_sub(last) < Self::FETCH_RETRY_ROUNDS => {
                return Vec::new();
            }
            _ => {}
        }
        self.pending_cut_fetch.insert(key, self.cut_round);
        targets
            .into_iter()
            .map(|peer| CutEffect::FetchTo {
                peer,
                round,
                cut_id: cut_id.clone(),
            })
            .collect()
    }

    /// Was upstream primary/src/core.rs:448-478 (minted+broadcast a `CutCertificate`
    /// once THIS party's own vote count crossed `mint_threshold`). Now Fig. 2's
    /// Mark-safe step, evaluated FIRST-HAND: every `CutVote` is individually verified
    /// (`vote.verify`) and fed to this party's OWN `CutVoteAggregator` -- never a
    /// peer's relayed claim. Under `Variant::Opt` (unchanged from before the Bracha
    /// addition), once weight crosses `mint_threshold`, this party transitions
    /// straight into `mark_cut_safe` with the exact witnesses it counted. No
    /// certificate is minted, broadcast, or received by anyone, anywhere -- see the
    /// module doc comment's "FIGURE-2 REWRITE" paragraph for why this is the fix, not
    /// merely a rename.
    ///
    /// BRACHA VARIANT ADDITION: under `Variant::Bracha`, this is instead Bracha-RBC's
    /// own FIRST echo round -- the SAME `CutVoteAggregator`/`cut_vote_aggregators` map,
    /// just thresholded at plain `quorum_threshold` (see `CutVoteAggregator::append`'s
    /// `threshold` parameter) rather than `mint_threshold`, and crossing it broadcasts
    /// a `CutReady` (`broadcast_cut_ready`) rather than calling `mark_cut_safe`
    /// directly -- `witnesses` (the vote senders) are not used as a fetch-witness set
    /// at this step; see `broadcast_cut_ready`/`process_cut_ready` below for the
    /// second echo round that actually reaches `mark_cut_safe`. See the module doc
    /// comment's "BRACHA VARIANT" paragraph for the full mechanism and upstream
    /// citation.
    pub fn process_cut_vote(
        &mut self,
        vote: CutVote,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if vote.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = vote.round;
        let cut_id = vote.cut_id.clone();
        let key = (round, cut_id.clone());
        let threshold = match self.variant {
            Variant::Opt => mint_threshold(&self.committee),
            Variant::Bracha => self.committee.quorum_threshold(),
        };
        let aggregator = self.cut_vote_aggregators.entry(key).or_default();
        let Ok(Some(witnesses)) = aggregator.append(&vote, &self.committee, threshold) else {
            return Vec::new();
        };
        match self.variant {
            Variant::Opt => self.mark_cut_safe(round, cut_id, witnesses, tips, oracle),
            Variant::Bracha => self.broadcast_cut_ready(round, cut_id, tips, oracle),
        }
    }

    /// BRACHA VARIANT ADDITION (`Variant::Bracha` only): reached when this party's own
    /// first-hand `CutVote` census (`process_cut_vote` above) crosses
    /// `quorum_threshold` -- Bracha-RBC's own first echo-to-ready transition (arXiv:
    /// 2606.14404 Table 1/2 + Corollary 5, variant S). One-shot per round
    /// (`sent_cut_ready`), mirroring `sent_cut_votes`'s identical latch shape exactly
    /// -- upstream has the identical one-shot guard on its own `broadcast_cut_ready`
    /// (`sent_ready_rounds.insert(round)`, primary/src/core.rs:436-438 there), so this
    /// part is faithfully mirrored, not a deviation. Broadcasts, then immediately
    /// processes its own `CutReady` locally through the SAME
    /// first-hand path a peer's `CutReady` would take (`process_cut_ready`) -- mirrors
    /// `process_cut_proposal`'s own self-vote dispatch and `mark_cut_safe`'s own
    /// self-Decide dispatch: never a shortcut that credits this party's own message
    /// without also counting it.
    ///
    /// Upstream (`simpleit/Bracha-Mempool-Simple-IT` branch) primary/src/core.rs:
    /// 435-459 (`broadcast_cut_ready`) is the direct ancestor of this method plus
    /// `process_cut_ready` below combined -- upstream inlines the self-processing call
    /// at the end of its own `broadcast_cut_ready`; split here only by which method
    /// each half lives in, mirroring this port's existing `mark_cut_safe`/
    /// `process_decide` split for the identical self-Decide pattern.
    fn broadcast_cut_ready(
        &mut self,
        round: CutRound,
        cut_id: Digest,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if !self.sent_cut_ready.insert(round) {
            return Vec::new();
        }
        let ready = CutReady {
            round,
            cut_id,
            author: self.name,
        };
        let mut effects = vec![CutEffect::Broadcast(CutOut::CutReady(ready.clone()))];
        effects.extend(self.process_cut_ready(ready, tips, oracle));
        effects
    }

    /// BRACHA VARIANT ADDITION: `Inbound::CutReady`'s entry point -- Bracha-RBC's own
    /// second echo round (see `broadcast_cut_ready`'s doc comment for the first).
    /// Mirrors `process_cut_vote`'s shape exactly: individually verifies, dedups via
    /// this party's OWN first-hand `CutReadyAggregator`, and on crossing
    /// `quorum_threshold` transitions straight into `mark_cut_safe` with the exact
    /// `CutReady` senders counted as the fetch-witness set -- see `mark_cut_safe`'s
    /// own doc comment for how `witnesses` is used there (`ensure_cut_fetch`'s target
    /// set). No certificate is minted or broadcast anywhere -- see the module doc
    /// comment's "FIGURE-2 REWRITE" paragraph; this is that identical fix applied to
    /// the separate soundness gap upstream's OWN Bracha branch has at this exact
    /// point (`Core::process_cut_ready`, primary/src/core.rs:646-679 there, which
    /// minted+broadcast a `CutCertificate` on crossing `quorum_threshold` -- see the
    /// module doc comment's "BRACHA VARIANT" paragraph for the upstream citation and
    /// the "NOT reproduced" note on upstream's own missing f+1-ready amplification).
    ///
    /// No-op for `Variant::Opt` (defensive: an Opt-configured engine never constructs
    /// a `CutReady` itself, and no correct Bracha-configured peer ever sends one to an
    /// Opt-configured committee -- `config::Protocol` selects exactly one variant for
    /// an entire run -- but a stray/Byzantine `CutReady` should not be able to make an
    /// Opt engine take a Bracha-shaped path it was never configured to run).
    pub fn process_cut_ready(
        &mut self,
        ready: CutReady,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if self.variant != Variant::Bracha {
            return Vec::new();
        }
        if ready.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = ready.round;
        let cut_id = ready.cut_id.clone();
        let key = (round, cut_id.clone());
        let aggregator = self.cut_ready_aggregators.entry(key).or_default();
        let Ok(Some(witnesses)) = aggregator.append(&ready, &self.committee) else {
            return Vec::new();
        };
        self.mark_cut_safe(round, cut_id, witnesses, tips, oracle)
    }

    /// Upstream primary/src/core.rs:480-544.
    ///
    /// Deviation 4: upstream's `while let Some(proposal) = queue.pop_front()` uses
    /// `?`/`ensure!` inside the loop for the verify check and the leader-authenticity
    /// check. Either failing aborts `process_cut_proposal` entirely via `Err`, which
    /// (since `queue` was already extended with every sibling `pending_cut_children`
    /// had buffered for the cut that just became known, and `record_cut_proposal`
    /// already ran for the just-recorded parent before the loop even reaches a bad
    /// sibling) silently drops every valid sibling still sitting in `queue` -- they are
    /// simply never dequeued. Fixed by rejecting a bad proposal individually (`continue`)
    /// without discarding the rest of the queue; no check below accepts anything
    /// upstream would have rejected, or rejects anything upstream would have accepted --
    /// only the "one bad item takes the whole batch down with it" failure mode is gone.
    /// (`retry_pending_cut_proposals`'s own `for proposal in ready { ...await?... }`
    /// loop had the identical failure mode one level up; making this function infallible
    /// fixes that call site too, for free, as a consequence of the effect-returning
    /// design rather than a second, separate fix.)
    ///
    /// Deviation 3: the f+1 tip-availability gate sits immediately before the one
    /// voting decision (`sent_cut_votes.insert`), gating only the vote -- recording
    /// the proposal and reparenting its own pending children happen unconditionally,
    /// exactly as upstream does, since the paper's gate is about "casting a vote", not
    /// about learning of/relaying a proposal. A gate failure does not consume the
    /// per-round vote latch (`sent_cut_votes`), unlike upstream's original passing
    /// case (which always consumes it): this is deliberate, so a proposal that fails
    /// the gate leaves the round eligible to vote later if some other satisfying event
    /// re-drives processing for it -- this engine does not itself invent such a retry
    /// (none was asked for), it only avoids permanently foreclosing one.
    pub fn process_cut_proposal(
        &mut self,
        proposal: CutProposal,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let mut effects = Vec::new();
        let mut queue = VecDeque::from([proposal]);
        while let Some(proposal) = queue.pop_front() {
            if proposal.verify(&self.committee).is_err() {
                continue;
            }
            let round = proposal.round;
            if self.certified_timed_out.contains(&round) {
                continue;
            }
            if proposal.proposer != self.leader_for_round(round) {
                continue;
            }

            if !self.safe_cut_parent(round, &proposal.parent_cut) {
                let parent_cut = proposal.parent_cut.clone();
                // REPAIR (see the module doc comment): fetch only when the parent is
                // genuinely UNKNOWN (never recorded) -- NOT when it's known but
                // simply not yet safe (an in-flight pipeline wait on intermediate
                // rounds' own timeout certification, or a malformed parent_round >=
                // round), which `safe_cut_parent` would also reject but which
                // fetching cannot help. Target set: unlike `mark_cut_safe`'s trigger,
                // no per-voter evidence exists for this specific digest (if it did,
                // this party would itself already have crossed `mint_threshold` for
                // it, which would already have triggered a fetch) -- there is no
                // narrower evidence than "ask everyone", so this asks the full
                // committee. Round guess: `round - 1`, exact whenever no
                // timeout-skipped round sits between parent and child (the
                // common/optimistic case: `make_cut_proposal` always cites
                // `highest_safe_cut`, which a leader updates only immediately upon
                // marking safe the round it then builds on). A wrong guess (a skipped
                // round sits between them) is not fatal: `ensure_cut_fetch` simply
                // never matches a genuine answer for the true parent's real round, so
                // it goes unanswered rather than corrupting state, and
                // `mark_cut_safe`'s own trigger remains the fully general backstop --
                // it fires with the EXACT round once this party independently crosses
                // `mint_threshold` for the true parent itself.
                if parent_cut != Digest::default()
                    && !self.cut_round_by_id.contains_key(&parent_cut)
                {
                    let targets = self.all_other_committee_members();
                    effects.extend(self.ensure_cut_fetch(
                        round.saturating_sub(1),
                        &parent_cut,
                        targets,
                    ));
                }
                self.pending_cut_children
                    .entry((round, parent_cut))
                    .or_default()
                    .push(proposal);
                continue;
            }

            let cut_id = proposal.id();
            if self.cut_proposals.contains_key(&(round, cut_id)) {
                continue;
            }

            let tips_ok = !self.gate_tips
                || proposal
                    .tips
                    .iter()
                    .all(|(author, tip)| oracle.available_at_validity(author, tip));

            let cut_id = self.record_cut_proposal(proposal);
            self.leader_cut_by_round
                .entry(round)
                .or_insert_with(|| cut_id.clone());

            // Reparent: any proposals buffered while `cut_id` was unknown, across
            // whichever round(s) named it as `parent_cut` -- see `pending_cut_children`'s
            // doc comment for why this is a bounded scan rather than a single removal.
            let waiting_rounds: Vec<CutRound> = self
                .pending_cut_children
                .keys()
                .filter(|(_, parent)| *parent == cut_id)
                .map(|(r, _)| *r)
                .collect();
            for r in waiting_rounds {
                if let Some(children) = self.pending_cut_children.remove(&(r, cut_id.clone())) {
                    queue.extend(children);
                }
            }

            if tips_ok && self.sent_cut_votes.insert(round) {
                let vote = CutVote {
                    round,
                    cut_id: cut_id.clone(),
                    author: self.name,
                };
                effects.push(CutEffect::Broadcast(CutOut::CutVote(vote.clone())));
                effects.extend(self.process_cut_vote(vote, tips, oracle));
            }

            effects.extend(self.try_commit_round(round));
        }
        effects
    }

    /// Upstream primary/src/core.rs:546-576.
    pub fn retry_pending_cut_proposals(
        &mut self,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if self.pending_cut_children.is_empty() {
            return Vec::new();
        }

        let pending = std::mem::take(&mut self.pending_cut_children);
        let mut still_pending = BTreeMap::new();
        let mut ready = Vec::new();

        for ((round, parent_cut), proposals) in pending {
            let mut deferred = Vec::new();
            for proposal in proposals {
                if self.safe_cut_parent(proposal.round, &proposal.parent_cut) {
                    ready.push(proposal);
                } else {
                    deferred.push(proposal);
                }
            }
            if !deferred.is_empty() {
                still_pending.insert((round, parent_cut), deferred);
            }
        }

        self.pending_cut_children = still_pending;

        let mut effects = Vec::new();
        for proposal in ready {
            effects.extend(self.process_cut_proposal(proposal, tips, oracle));
        }
        effects
    }

    /// Upstream primary/src/core.rs:609-615. Upstream builds a fresh `BTreeMap` from
    /// `self.current_certified_tips` (a `HashMap`); since the caller now hands us the
    /// cut directly as a `BTreeMap` already (see the module doc comment's oracle-1),
    /// this is just that value, cloned.
    fn current_cut(&self, tips: &Cut) -> Cut {
        tips.clone()
    }

    /// Upstream primary/src/core.rs:617-624.
    fn make_cut_proposal(&self, round: CutRound, parent_cut: Digest, tips: &Cut) -> CutProposal {
        CutProposal {
            round,
            proposer: self.name,
            parent_cut,
            tips: self.current_cut(tips),
        }
    }

    /// Upstream primary/src/core.rs:632-639. Upstream also writes
    /// `self.cut_parents.insert(cut_id, proposal.parent_cut)` here -- `cut_parents` is
    /// dead (write-only upstream; read only from code already commented out there) and
    /// is deliberately not ported at all, per the task brief.
    fn record_cut_proposal(&mut self, proposal: CutProposal) -> Digest {
        let cut_id = proposal.id();
        let round = proposal.round;
        self.cut_round_by_id.insert(cut_id.clone(), round);
        self.cut_proposals.insert((round, cut_id.clone()), proposal);
        cut_id
    }

    /// Was upstream primary/src/core.rs:652-687 (`process_cut_certificate`, verifying
    /// and accepting a RECEIVED `CutCertificate`). Now Fig. 2's Mark-safe step
    /// immediately followed by Vote ("send `⟨commit, curr_round⟩` to all parties"),
    /// reached ONLY from `process_cut_vote` -- exclusively when THIS party's own
    /// `CutVoteAggregator` crosses `mint_threshold` on votes it individually verified.
    /// There is no longer a verify step here (there is nothing left to verify: every
    /// vote that got `round`/`cut_id`/`witnesses` here already passed `CutVote::
    /// verify` and the aggregator's own dedup, one at a time, in `process_cut_vote`).
    /// `witnesses` is `CutVoteAggregator::append`'s own returned voter list -- this
    /// party's first-hand record of who it received a vote from, used below only as
    /// `ensure_cut_fetch`'s target set (see the module doc comment's REPAIR
    /// paragraph), never transmitted anywhere.
    fn mark_cut_safe(
        &mut self,
        round: CutRound,
        cut_id: Digest,
        witnesses: Vec<PublicKey>,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if self.certified_timed_out.contains(&round) {
            return Vec::new();
        }
        self.safe.entry(round).or_insert_with(|| cut_id.clone());
        if round + 1 >= self.cut_round {
            self.highest_safe_cut = cut_id.clone();
        }
        self.cut_round = self.cut_round.max(round + 1);
        self.advance_timed_out_cut_rounds();

        // REPAIR (see the module doc comment): crossing `mint_threshold` names only a
        // `cut_id` -- if we never independently received/recorded round `round`'s own
        // `CutProposal`, nothing else will ever ask for it (`safe_cut_parent`'s
        // "parent unknown" branch only BUFFERS a citing child, it never fetches on
        // its own -- that is `process_cut_proposal`'s OWN, separate trigger, for when
        // a child arrives before this party ever crosses the threshold itself). Every
        // one of `witnesses` sent us a `CutVote` naming this `cut_id`, i.e. claimed,
        // by voting, to have seen the proposal -- see `ensure_cut_fetch`'s doc
        // comment.
        let mut effects = self.ensure_cut_fetch(round, &cut_id, witnesses);
        if self.voted.insert(round) {
            let decide = Decide {
                id: cut_id,
                round,
                author: self.name,
            };
            effects.push(CutEffect::Broadcast(CutOut::Decide(decide.clone())));
            effects.extend(self.process_decide(decide));
        }

        effects.extend(self.try_propose_cut_for_current_round(tips, oracle));
        effects.extend(self.schedule_cut_timer(self.cut_round));
        effects
    }

    /// Upstream primary/src/core.rs:689-712. `Decide` is Fig. 2's `⟨commit, r⟩`; this
    /// is the **Commit** step ("Upon receiving `⟨commit, r⟩` from n - f parties for
    /// some round r, set `committed[r] ← true`"), counted FIRST-HAND exactly like
    /// `process_cut_vote` above -- every `Decide` is individually verified
    /// (`decide.verify`) and fed to this party's OWN `DecideAggregator`.
    pub fn process_decide(&mut self, decide: Decide) -> Vec<CutEffect> {
        if decide.verify(&self.committee).is_err() {
            return Vec::new();
        }
        if self.committed.contains_key(&decide.round) {
            return Vec::new();
        }

        let key = (decide.round, decide.id.clone());
        let aggregator = self.decide_aggregators.entry(key).or_default();
        let Ok(Some(quorum_decide)) = aggregator.append(&decide, &self.committee) else {
            return Vec::new();
        };
        let round = quorum_decide.round;
        self.committed.entry(round).or_insert(quorum_decide);
        self.try_commit_round(round)
    }

    /// Upstream primary/src/core.rs:714-727.
    fn try_commit_round(&mut self, round: CutRound) -> Vec<CutEffect> {
        let Some(decide) = self.committed.get(&round) else {
            return Vec::new();
        };
        let Some(leader_cut) = self.leader_cut_by_round.get(&round) else {
            return Vec::new();
        };

        if decide.id == *leader_cut {
            let leader_cut = leader_cut.clone();
            return self.emit_commit_to_committer(round, &leader_cut);
        }
        Vec::new()
    }

    /// Upstream primary/src/core.rs:729-751. Upstream only marks `sent_commit_rounds`
    /// after a successful `self.tx_committer.send(...).await` (skipping it on a send
    /// error, e.g. a dropped receiver); there is no channel here to fail, so once this
    /// engine decides to emit the effect it is unconditionally considered sent -- the
    /// caller's own delivery of the effect is outside the state machine's concern,
    /// exactly as every other `CutEffect::Broadcast` already is.
    fn emit_commit_to_committer(&mut self, round: CutRound, cut_id: &Digest) -> Vec<CutEffect> {
        if self.sent_commit_rounds.contains(&round) {
            return Vec::new();
        }
        let Some(cut) = self.cut_proposals.get(&(round, cut_id.clone())) else {
            return Vec::new();
        };
        let proposals = cut.tips.clone();
        self.sent_commit_rounds.insert(round);
        vec![CutEffect::Commit { round, proposals }]
    }

    /// Upstream primary/src/core.rs:753-788.
    pub fn try_propose_cut_for_current_round(
        &mut self,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let round = self.cut_round;
        if self.name != self.leader_for_round(round) {
            return Vec::new();
        }
        if !self.safe_cut_parent(round, &self.highest_safe_cut) {
            return Vec::new();
        }
        if !self.proposed_cut_rounds.insert(round) {
            return Vec::new();
        }

        let parent_cut = self.highest_safe_cut.clone();
        let proposal = self.make_cut_proposal(round, parent_cut, tips);
        let mut effects = vec![CutEffect::Broadcast(CutOut::CutProposal(proposal.clone()))];
        effects.extend(self.process_cut_proposal(proposal, tips, oracle));
        effects
    }

    /// Upstream primary/src/core.rs:808-822. `ArmTimer`'s deadline is computed here
    /// (`Instant::now() + timeout_delay`), matching `control::ControlLog::
    /// enter_round_core`'s own `Effect::ArmControlTimer(r, Instant::now() + ...)` style
    /// -- not `agb::AgbEngine`'s threaded-`now` parameter style -- because upstream's own
    /// `schedule_cut_timer(&mut self, round: u64)` took no `now`-like parameter either;
    /// this preserves that signature exactly rather than introducing one upstream never
    /// had.
    ///
    /// `pub` (production wiring, `simpleit::node::SimpleItCore`): every OTHER caller of
    /// this method is internal (`mark_cut_safe`/`handle_timeout_accept_action`,
    /// both only ever reachable after `cut_round` has already advanced PAST round 1), so
    /// round 1 itself never gets a timer armed this way -- the production wiring calls
    /// this directly, once, at boot (`schedule_cut_timer(1)`), exactly mirroring how
    /// `try_propose_cut_for_current_round` (already `pub`) must also be called directly
    /// at boot for the round-1 leader to ever propose. Zero behavior change: same
    /// one-shot-per-round latch (`scheduled_cut_timers`), same effect.
    pub fn schedule_cut_timer(&mut self, round: CutRound) -> Vec<CutEffect> {
        if self.scheduled_cut_timers.insert(round) {
            log::info!(
                "BENCH event=round_start round={} leader={:?} node={:?}",
                round,
                self.leader_for_round(round),
                self.name
            );
            let deadline = Instant::now() + Duration::from_millis(self.timeout_delay);
            return vec![CutEffect::ArmTimer { round, deadline }];
        }
        Vec::new()
    }

    /// Upstream primary/src/core.rs:824-838.
    fn safe_cut_parent(&self, round: CutRound, parent_cut: &Digest) -> bool {
        let parent_round = if *parent_cut == Digest::default() {
            0
        } else if let Some(parent_round) = self.cut_round_by_id.get(parent_cut) {
            *parent_round
        } else {
            return false;
        };

        if parent_round >= round {
            return false;
        }

        ((parent_round + 1)..round).all(|r| self.certified_timed_out.contains(&r))
    }

    /// Upstream primary/src/core.rs:840-848.
    fn advance_timed_out_cut_rounds(&mut self) -> bool {
        let old_cut_round = self.cut_round;
        while self.certified_timed_out.contains(&self.cut_round)
            && self.safe_cut_parent(self.cut_round + 1, &self.highest_safe_cut)
        {
            self.cut_round += 1;
        }
        self.cut_round != old_cut_round
    }

    /// Upstream primary/src/core.rs:850-878.
    pub fn process_cut_timer(
        &mut self,
        round: CutRound,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if round != self.cut_round
            || self.safe.contains_key(&round)
            || self.certified_timed_out.contains(&round)
            || !self.timed_out.insert(round)
        {
            return Vec::new();
        }

        log::info!(
            "BENCH event=timeout_sent round={} node={:?}",
            round,
            self.name
        );
        let timeout = Timeout {
            round,
            author: self.name,
        };
        let mut effects = vec![CutEffect::Broadcast(CutOut::Timeout(timeout.clone()))];
        effects.extend(self.process_timeout(timeout, tips, oracle));
        effects
    }

    /// Upstream primary/src/core.rs:880-900.
    pub fn process_timeout(
        &mut self,
        timeout: Timeout,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        if timeout.verify(&self.committee).is_err() {
            return Vec::new();
        }
        let round = timeout.round;
        if self.certified_timed_out.contains(&round) || self.safe.contains_key(&round) {
            return Vec::new();
        }

        let aggregator = self.timeouts_aggregators.entry(round).or_default();
        let Ok(Some(())) = aggregator.append(timeout, &self.committee) else {
            return Vec::new();
        };

        let (mut effects, maybe) = self.send_timeout_accept(round);
        if let Some((weight, timeout_cert)) = maybe {
            effects.extend(self.handle_timeout_accept_action(
                round,
                weight,
                timeout_cert,
                tips,
                oracle,
            ));
        }
        effects
    }

    /// Upstream primary/src/core.rs:902-917.
    fn broadcast_timeout_accept(&self, accept: &TimeoutAccept) -> Vec<CutEffect> {
        vec![CutEffect::Broadcast(CutOut::TimeoutAccept(accept.clone()))]
    }

    /// Upstream primary/src/core.rs:919-930. Returns the broadcast effect (if this call
    /// is the first-ever send for `round` -- upstream's `sent_timeout_accepts.insert`
    /// one-shot latch) alongside the `(weight, maybe-cert)` `record_timeout_accept`
    /// produced for our own accept; `None` in the second position exactly when upstream
    /// would have returned `Ok(None)` (already sent).
    fn send_timeout_accept(
        &mut self,
        round: CutRound,
    ) -> (Vec<CutEffect>, Option<(Stake, Option<TimeoutCert>)>) {
        if !self.sent_timeout_accepts.insert(round) {
            return (Vec::new(), None);
        }
        let accept = TimeoutAccept {
            round,
            author: self.name,
        };
        let effects = self.broadcast_timeout_accept(&accept);
        let result = self.record_timeout_accept(accept);
        (effects, Some(result))
    }

    /// Upstream primary/src/core.rs:932-950.
    fn record_timeout_accept(&mut self, accept: TimeoutAccept) -> (Stake, Option<TimeoutCert>) {
        if accept.verify(&self.committee).is_err() {
            return (0, None);
        }
        let round = accept.round;
        if self.certified_timed_out.contains(&round) || self.safe.contains_key(&round) {
            return (0, None);
        }

        let aggregator = self.timeout_accept_aggregators.entry(round).or_default();
        aggregator
            .append(accept, &self.committee)
            .unwrap_or((0, None))
    }

    /// Upstream primary/src/core.rs:952-957.
    pub fn process_timeout_accept(
        &mut self,
        accept: TimeoutAccept,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let round = accept.round;
        let (weight, timeout_cert) = self.record_timeout_accept(accept);
        self.handle_timeout_accept_action(round, weight, timeout_cert, tips, oracle)
    }

    /// Upstream primary/src/core.rs:959-982.
    fn handle_timeout_accept_action(
        &mut self,
        round: CutRound,
        weight: Stake,
        mut timeout_cert: Option<TimeoutCert>,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let mut effects = Vec::new();
        if weight >= self.committee.validity_threshold() {
            let (amplify_effects, maybe) = self.send_timeout_accept(round);
            effects.extend(amplify_effects);
            if let Some((_, own_cert)) = maybe {
                timeout_cert = timeout_cert.or(own_cert);
            }
        }

        if let Some(timeout_cert) = timeout_cert {
            if timeout_cert.verify(&self.committee).is_err() {
                return effects;
            }
            if self.certified_timed_out.insert(round) {
                effects.extend(self.retry_pending_cut_proposals(tips, oracle));
                if self.advance_timed_out_cut_rounds() {
                    effects.extend(self.try_propose_cut_for_current_round(tips, oracle));
                    effects.extend(self.schedule_cut_timer(self.cut_round));
                }
            }
        }
        effects
    }

    /// Upstream primary/src/core.rs:1015-1017. A thin wrapper, kept distinct from
    /// `process_timeout` because upstream keeps it distinct -- see `handle`'s doc
    /// comment for which call site uses which name.
    pub fn handle_timeout(
        &mut self,
        timeout: Timeout,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        self.process_timeout(timeout, tips, oracle)
    }

    /// Upstream primary/src/core.rs:1023-1029. Upstream checks `self.gc_round <=
    /// accept.round`, where `gc_round: Height` is an Autobahn header-height GC floor --
    /// a different unit entirely from a cut round, and not ported (see `gc_floor`'s
    /// doc comment on `CutEngine`). This checks the same shape of thing
    /// (`accept.round` is not older than our own GC floor) against `gc_floor`, this
    /// engine's own analogous floor.
    pub fn sanitize_timeout_accept(&self, accept: &TimeoutAccept) -> DagResult<()> {
        ensure!(
            self.gc_floor <= accept.round,
            DagError::CertificateTooOld(accept.digest(), accept.round)
        );
        Ok(())
    }

    // ============================================================ Cut-proposal repair
    //
    // Not upstream -- upstream has no equivalent. Closes the liveness gap where a
    // party locally marks round r safe (crossing `mint_threshold` on votes naming only
    // a `cut_id`) without ever having received round r's own `CutProposal` -- see the
    // module doc comment and `mark_cut_safe`/`process_cut_proposal` for the two
    // triggers that call `ensure_cut_fetch` above. Mirrors `control::ControlLog`'s own
    // carrier-body fetch/serve (`on_control_fetch`/`on_control_serve`) exactly.

    /// A peer's request for `(round, cut_id)` -- answer with our own held
    /// `CutProposal` if we have it and haven't already answered this requester for
    /// this exact pair. Gated on `gc_floor`, this engine's one and only retention
    /// floor (unlike `ControlLog`'s split `min_live_view`/`min_serve_view`:
    /// `CutEngine::prune_below` drops `cut_proposals` at the same floor as every
    /// other round-keyed field, so there is no wider serve-only window to gate on
    /// separately here).
    pub fn on_cut_fetch(
        &mut self,
        requester: PublicKey,
        round: CutRound,
        cut_id: Digest,
    ) -> Vec<CutEffect> {
        if round < self.gc_floor {
            return Vec::new();
        }
        let answered_key = (round, cut_id.clone(), requester);
        if self.fetch_answered.contains(&answered_key) {
            return Vec::new();
        }
        let Some(proposal) = self.cut_proposals.get(&(round, cut_id)) else {
            return Vec::new();
        };
        let proposal = proposal.clone();
        self.fetch_answered.insert(answered_key);
        vec![CutEffect::ServeTo {
            peer: requester,
            proposal,
        }]
    }

    /// A peer's answer to our own fetch -- accept only if it hash-matches a pair we
    /// actually requested: `proposal.id()` (`== CutProposal::digest()`, which hashes
    /// `round` among its other fields) keyed together with `proposal.round` against
    /// `pending_cut_fetch` is exactly that pair, so this single lookup checks BOTH
    /// "is this cut_id one we asked for" AND "at the round we asked for it" in one
    /// step. Mirrors `control::ControlLog::on_control_serve`'s RS1-class defense:
    /// "valid" means hash-matching a REQUESTED pair, not merely well-formed.
    /// Structural verification (leader authenticity, safety, dedup, the f+1 tip
    /// gate, ...) is deliberately NOT duplicated here -- accepting hands off
    /// entirely to `process_cut_proposal`, which already performs every one of
    /// those checks for a directly-received proposal, so recording, reparenting of
    /// `pending_cut_children`, voting, and `try_commit_round` all happen exactly as
    /// they would have. Every rejecting path below changes no state
    /// (`pending_cut_fetch` is only ever removed on the accepting path, after the
    /// hash-match check has already passed).
    pub fn on_cut_serve(
        &mut self,
        proposal: CutProposal,
        tips: &Cut,
        oracle: &dyn TipOracle,
    ) -> Vec<CutEffect> {
        let key = (proposal.round, proposal.id());
        if !self.pending_cut_fetch.contains_key(&key) {
            return Vec::new(); // unsolicited, or answers a pair we never requested
        }
        self.pending_cut_fetch.remove(&key);
        self.process_cut_proposal(proposal, tips, oracle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> PublicKey {
        PublicKey([byte; 32])
    }

    /// `n` committee members, `key(1)..=key(n)`, equal stake -- ascending byte value so
    /// `committee.authorities.keys()` (BTreeMap order) yields exactly `key(1), key(2),
    /// ..., key(n)`, letting tests reason about `leader_for_round`'s output directly.
    fn committee_of(n: u8) -> (Committee, Vec<PublicKey>) {
        let keys: Vec<PublicKey> = (1..=n).map(key).collect();
        let info = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    *k,
                    1u32,
                    format!("127.0.0.1:{}", 9000 + i as u16).parse().unwrap(),
                )
            })
            .collect();
        (Committee::new(info), keys)
    }

    fn sample_tips(keys: &[PublicKey]) -> Cut {
        keys.iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    *k,
                    Proposal {
                        header_digest: Digest([i as u8 + 1; 32]),
                        height: 1,
                    },
                )
            })
            .collect()
    }

    struct AllAvailable;
    impl TipOracle for AllAvailable {
        fn available_at_validity(&self, _author: &PublicKey, _tip: &Proposal) -> bool {
            true
        }
    }

    struct DenyAuthor(PublicKey);
    impl TipOracle for DenyAuthor {
        fn available_at_validity(&self, author: &PublicKey, _tip: &Proposal) -> bool {
            *author != self.0
        }
    }

    fn find_proposal(effects: &[CutEffect]) -> Option<CutProposal> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::CutProposal(p)) => Some(p.clone()),
            _ => None,
        })
    }

    fn find_vote_for_round(effects: &[CutEffect], round: CutRound) -> Option<CutVote> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::CutVote(v)) if v.round == round => Some(v.clone()),
            _ => None,
        })
    }

    fn find_decide_for_round(effects: &[CutEffect], round: CutRound) -> Option<Decide> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::Decide(d)) if d.round == round => Some(d.clone()),
            _ => None,
        })
    }

    fn find_commits(effects: &[CutEffect], round: CutRound) -> Vec<(CutRound, Cut)> {
        effects
            .iter()
            .filter_map(|e| match e {
                CutEffect::Commit {
                    round: r,
                    proposals,
                } if *r == round => Some((*r, proposals.clone())),
                _ => None,
            })
            .collect()
    }

    fn find_fetches(effects: &[CutEffect]) -> Vec<(PublicKey, CutRound, Digest)> {
        effects
            .iter()
            .filter_map(|e| match e {
                CutEffect::FetchTo {
                    peer,
                    round,
                    cut_id,
                } => Some((*peer, *round, cut_id.clone())),
                _ => None,
            })
            .collect()
    }

    /// BRACHA VARIANT ADDITION: mirrors `find_vote_for_round`/`find_decide_for_round`
    /// exactly, for the new `CutOut::CutReady` broadcast.
    fn find_ready_for_round(effects: &[CutEffect], round: CutRound) -> Option<CutReady> {
        effects.iter().find_map(|e| match e {
            CutEffect::Broadcast(CutOut::CutReady(r)) if r.round == round => Some(r.clone()),
            _ => None,
        })
    }

    /// Test 1: happy path end to end, at both n=4 and n=10 -- proposal -> votes to
    /// `mint_threshold` -> this party marks the round `safe` (and, in the SAME step,
    /// sends its own `Decide` -- no certificate round-trip in between: one fewer
    /// message delay than the removed certificate-broadcast design) -> decides to
    /// `quorum_threshold` -> commit emitted exactly once for the round.
    fn happy_path_commit(n: u8) {
        let (committee, keys) = committee_of(n);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2); // round 1 -> view 2, see leader_for_round

        let mut engine = CutEngine::new(leader, committee, 1_000);

        // Propose: the leader broadcasts its own proposal and immediately self-votes
        // (upstream: the leader also processes its own proposal locally).
        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();
        assert!(
            find_vote_for_round(&effects, round).is_some(),
            "leader self-votes for its own proposal"
        );
        assert!(!engine.safe.contains_key(&round));

        // Votes: bring in other committee members' votes for the same cut_id until
        // this party's own count crosses `mint_threshold` and it marks the round safe
        // locally -- no certificate is minted or broadcast anywhere on this path.
        let mut others = keys.iter().filter(|k| **k != leader);
        loop {
            if engine.safe.contains_key(&round) {
                break;
            }
            let author = *others
                .next()
                .expect("committee is large enough to reach mint_threshold");
            let vote = CutVote {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_vote(vote, &tips, &oracle);
        }
        assert_eq!(engine.safe.get(&round), Some(&cut_id));
        assert!(
            find_decide_for_round(&effects, round).is_some(),
            "the vote that crosses mint_threshold broadcasts this party's own Decide \
             in the SAME step -- one fewer message delay than the old \
             certificate-broadcast design"
        );

        // Decides: bring in other committee members' decides for the same (round,
        // cut_id) until commit appears. The safe-crossing call above already produced
        // our own self-decide.
        let mut others = keys.iter().filter(|k| **k != leader);
        let mut commits = find_commits(&effects, round);
        while commits.is_empty() {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let decide = Decide {
                id: cut_id.clone(),
                round,
                author,
            };
            effects = engine.process_decide(decide);
            commits = find_commits(&effects, round);
        }

        assert_eq!(
            commits.len(),
            1,
            "commit should be emitted exactly once for the round"
        );
        assert_eq!(commits[0].1, proposal.tips);

        // Re-delivering the same decide-quorum-crossing event again must not emit a
        // second commit (the per-round `sent_commit_rounds` latch).
        let repeat = engine.try_commit_round(round);
        assert!(find_commits(&repeat, round).is_empty());
    }

    #[test]
    fn happy_path_commit_n4() {
        happy_path_commit(4);
    }

    #[test]
    fn happy_path_commit_n10() {
        happy_path_commit(10);
    }

    /// BRACHA VARIANT ADDITION -- Test 1's Bracha analogue, at both n=4 and n=10:
    /// proposal -> votes to `quorum_threshold` -> this party broadcasts+self-processes
    /// its own `CutReady` -> readys to `quorum_threshold` -> safe (and, in the SAME
    /// step, this party's own `Decide`) -> decides to `quorum_threshold` -> commit
    /// emitted exactly once for the round.
    fn happy_path_commit_bracha(n: u8) {
        let (committee, keys) = committee_of(n);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2);

        let mut engine = CutEngine::new(leader, committee, 1_000).with_variant(Variant::Bracha);

        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();
        assert!(
            find_vote_for_round(&effects, round).is_some(),
            "leader self-votes for its own proposal"
        );
        assert!(
            find_ready_for_round(&effects, round).is_none(),
            "a single self-vote is not enough to cross quorum_threshold"
        );
        assert!(!engine.safe.contains_key(&round));

        // Votes: bring in other committee members' votes until this party's own
        // CutVote census crosses quorum_threshold -- this broadcasts (and
        // self-processes) our own CutReady in the SAME step, but does NOT yet mark
        // the round safe.
        let mut others = keys.iter().filter(|k| **k != leader);
        loop {
            if find_ready_for_round(&effects, round).is_some() {
                break;
            }
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let vote = CutVote {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_vote(vote, &tips, &oracle);
        }
        assert!(
            !engine.safe.contains_key(&round),
            "crossing the FIRST echo (vote) threshold only broadcasts CutReady -- it \
             does not mark the round safe directly"
        );

        // Readys: bring in other committee members' CutReadys until this party's own
        // CutReady census crosses quorum_threshold and it marks the round safe
        // locally. The vote-threshold crossing above already produced our own
        // self-ready.
        let mut others = keys.iter().filter(|k| **k != leader);
        while !engine.safe.contains_key(&round) {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let ready = CutReady {
                round,
                cut_id: cut_id.clone(),
                author,
            };
            effects = engine.process_cut_ready(ready, &tips, &oracle);
        }
        assert_eq!(engine.safe.get(&round), Some(&cut_id));
        assert!(
            find_decide_for_round(&effects, round).is_some(),
            "the CutReady that crosses quorum_threshold broadcasts this party's own \
             Decide in the SAME step, exactly like the Opt variant's mark_cut_safe"
        );

        // Decides: bring in other committee members' decides for the same (round,
        // cut_id) until commit appears -- identical to the Opt variant (DecideAggregator
        // is shared, unmodified by the variant).
        let mut others = keys.iter().filter(|k| **k != leader);
        let mut commits = find_commits(&effects, round);
        while commits.is_empty() {
            let author = *others
                .next()
                .expect("committee is large enough to reach quorum_threshold");
            let decide = Decide {
                id: cut_id.clone(),
                round,
                author,
            };
            effects = engine.process_decide(decide);
            commits = find_commits(&effects, round);
        }

        assert_eq!(
            commits.len(),
            1,
            "commit should be emitted exactly once for the round"
        );
        assert_eq!(commits[0].1, proposal.tips);

        let repeat = engine.try_commit_round(round);
        assert!(find_commits(&repeat, round).is_empty());
    }

    #[test]
    fn happy_path_commit_bracha_n4() {
        happy_path_commit_bracha(4);
    }

    #[test]
    fn happy_path_commit_bracha_n10() {
        happy_path_commit_bracha(10);
    }

    /// BRACHA VARIANT ADDITION: `broadcast_cut_ready` broadcasts at most once per
    /// round, even if the internal trigger (crossing `quorum_threshold` on the vote
    /// census) somehow fires again for a DIFFERENT `cut_id` in the same round (e.g. an
    /// equivocating leader) -- `sent_cut_ready` is a per-ROUND latch, mirroring
    /// `sent_cut_votes`'s identical round-only shape, not a per-(round, cut_id) one.
    #[test]
    fn bracha_cut_ready_broadcasts_at_most_once_per_round() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let mut engine = CutEngine::new(keys[0], committee, 1_000).with_variant(Variant::Bracha);

        let cut_id_a = Digest([1; 32]);
        let effects = engine.broadcast_cut_ready(1, cut_id_a.clone(), &tips, &oracle);
        match effects.first() {
            Some(CutEffect::Broadcast(CutOut::CutReady(r))) => {
                assert_eq!(
                    r.cut_id, cut_id_a,
                    "the first call for a round broadcasts CutReady"
                );
            }
            other => panic!("expected a CutReady broadcast first, got {other:?}"),
        }

        let cut_id_b = Digest([2; 32]);
        let effects = engine.broadcast_cut_ready(1, cut_id_b, &tips, &oracle);
        assert!(
            effects.is_empty(),
            "a second CutReady for the same round must not be sent, even for a \
             different cut_id: {effects:?}"
        );
    }

    /// BRACHA VARIANT ADDITION: `process_cut_ready`'s own `CutReadyAggregator` census
    /// dedups by author exactly like `CutVoteAggregator` does (see
    /// `safe_is_reached_only_by_counting_distinct_votes_never_below_quorum`'s
    /// identical final assertion for votes) -- `safe` is reached at exactly
    /// `quorum_threshold` distinct `CutReady`s, and a replay from an already-counted
    /// author manufactures no additional weight.
    #[test]
    fn bracha_cut_ready_census_dedups_by_author_and_reaches_safe_at_quorum() {
        let (committee, keys) = committee_of(10);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let mut engine =
            CutEngine::new(keys[0], committee.clone(), 1_000).with_variant(Variant::Bracha);
        let cut_id = Digest([9; 32]);
        let round: CutRound = 1;

        let quorum = committee.quorum_threshold();
        let mut counted = 0u32;
        for author in keys.iter().copied() {
            if engine.safe.contains_key(&round) {
                break;
            }
            engine.process_cut_ready(
                CutReady {
                    round,
                    cut_id: cut_id.clone(),
                    author,
                },
                &tips,
                &oracle,
            );
            counted += 1;
        }
        assert!(
            engine.safe.contains_key(&round),
            "n=10 has enough distinct authors to reach quorum_threshold"
        );
        assert_eq!(
            counted, quorum,
            "safe should be reached at exactly quorum_threshold distinct CutReadys"
        );

        let before = engine.safe.get(&round).cloned();
        let repeat_author = keys[0];
        engine.process_cut_ready(
            CutReady {
                round,
                cut_id: cut_id.clone(),
                author: repeat_author,
            },
            &tips,
            &oracle,
        );
        assert_eq!(
            engine.safe.get(&round),
            before.as_ref(),
            "a replayed CutReady changes nothing"
        );
    }

    /// BRACHA VARIANT ADDITION: `process_cut_ready` is a no-op under `Variant::Opt` --
    /// a stray/Byzantine `CutReady` cannot make an Opt-configured engine take the
    /// Bracha-shaped path it was never configured to run.
    #[test]
    fn bracha_cut_ready_is_a_no_op_under_variant_opt() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let mut engine = CutEngine::new(keys[0], committee, 1_000); // Variant::Opt (default)

        let effects = engine.process_cut_ready(
            CutReady {
                round: 1,
                cut_id: Digest([1; 32]),
                author: keys[1],
            },
            &tips,
            &oracle,
        );
        assert!(effects.is_empty());
        assert!(engine.cut_ready_aggregators.is_empty());
        assert!(!engine.safe.contains_key(&1));
    }

    /// BRACHA VARIANT ADDITION -- the motivating case: at n=20, Opt's own
    /// `mint_threshold` (15) exceeds the number of live authors in this scenario (14),
    /// so no Opt-variant engine could ever reach `safe` here, no matter how long it
    /// waited. Bracha's own threshold (plain `quorum_threshold`, 14 at n=20) is exactly
    /// the number of live authors -- a round reaches `safe` and commits under Bracha
    /// using ONLY messages from those 14 live authors (which include the round-1
    /// leader); the other 6 committee members ("crashed") never contribute a single
    /// message anywhere in this test.
    #[test]
    fn bracha_reaches_safe_and_commits_with_only_fourteen_of_twenty_live_authors() {
        let (committee, keys) = committee_of(20);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round: CutRound = 1;
        let leader = agb::proposer(&committee, 2);

        assert_eq!(
            committee.quorum_threshold(),
            14,
            "test setup assumes n=20's quorum_threshold is exactly 14"
        );
        assert_eq!(
            mint_threshold(&committee),
            15,
            "test setup assumes n=20's mint_threshold is exactly 15 -- one MORE than \
             the number of live authors below, which is exactly why Opt could never \
             reach safe in this scenario"
        );

        let live: Vec<PublicKey> = keys.iter().copied().take(14).collect();
        assert!(
            live.contains(&leader),
            "test setup requires the round-1 leader to be among the live authors"
        );

        let mut engine = CutEngine::new(leader, committee, 1_000).with_variant(Variant::Bracha);

        let mut effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let proposal = find_proposal(&effects).expect("leader broadcasts a cut proposal");
        let cut_id = proposal.id();

        // Votes from the 13 OTHER live authors (the leader's own self-vote already
        // landed above) -- never from any of the 6 crashed authors.
        for author in live.iter().filter(|k| **k != leader) {
            effects = engine.process_cut_vote(
                CutVote {
                    round,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
        }
        assert!(
            find_ready_for_round(&effects, round).is_some(),
            "14 live authors is exactly quorum_threshold -- the vote census should \
             have crossed it and broadcast our own CutReady"
        );
        assert!(!engine.safe.contains_key(&round));

        // Readys from the same 13 other live authors (our own self-ready already
        // landed via broadcast_cut_ready's self-processing) -- again, never from a
        // crashed author.
        for author in live.iter().filter(|k| **k != leader) {
            if engine.safe.contains_key(&round) {
                break;
            }
            effects = engine.process_cut_ready(
                CutReady {
                    round,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
        }
        assert_eq!(
            engine.safe.get(&round),
            Some(&cut_id),
            "14 live authors are exactly quorum_threshold under Bracha"
        );

        // Decides from live authors only, until commit (quorum_threshold = 14, exactly
        // the number of live authors -- the leader's own self-decide already landed
        // via mark_cut_safe above).
        let mut commits = find_commits(&effects, round);
        for author in live.iter().filter(|k| **k != leader) {
            if !commits.is_empty() {
                break;
            }
            effects = engine.process_decide(Decide {
                id: cut_id.clone(),
                round,
                author: *author,
            });
            commits = find_commits(&effects, round);
        }

        assert_eq!(
            commits.len(),
            1,
            "the round should commit exactly once, using only the 14 live authors' \
             own messages"
        );
        assert_eq!(commits[0].1, proposal.tips);
    }

    /// REGRESSION (audit fix, see `aggregators::mint_threshold`); ADAPTED for the
    /// Fig.-2 rewrite -- was `minted_certificate_passes_its_own_verify_at_small_
    /// committees`. There is no certificate to verify anymore: each party marks a
    /// round safe by counting its OWN `CutVote`s to `mint_threshold =
    /// max(optimistic_threshold, quorum_threshold)`, with no separate verify step
    /// downstream. Before the clamp, `optimistic_threshold` alone was strictly
    /// smaller for f <= 2 (n = 4, 5, 6, 8, 9, 12 -- and n=4 is `fab remote`'s
    /// default); under the OLD certificate design that meant the minting party
    /// rejected the certificate it had just built (mint < verify), so no `Decide` was
    /// ever sent and the round could never commit. Under THIS design there is no
    /// verify step to catch an unclamped threshold at all -- `mint_threshold` alone
    /// stands between "some party thinks it's safe" and actual safety -- so this
    /// sweeps exactly the sizes where an unclamped threshold would fail silently
    /// rather than loudly.
    #[test]
    fn party_reaches_safe_at_the_correctly_clamped_local_vote_threshold() {
        for n in [4u8, 5, 6, 8, 9] {
            let (committee, keys) = committee_of(n);
            let tips = sample_tips(&keys);
            let oracle = AllAvailable;
            let leader = agb::proposer(&committee, 2);
            let mut engine = CutEngine::new(leader, committee.clone(), 1_000);

            let effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
            let cut_id = find_proposal(&effects).expect("leader proposes").id();
            assert!(
                !engine.safe.contains_key(&1),
                "n={n}: self-vote alone is not enough"
            );

            let mut others = keys.iter().filter(|k| **k != leader);
            while !engine.safe.contains_key(&1) {
                let author = *others
                    .next()
                    .expect("committee large enough to reach mint_threshold");
                engine.process_cut_vote(
                    CutVote {
                        round: 1,
                        cut_id: cut_id.clone(),
                        author,
                    },
                    &tips,
                    &oracle,
                );
            }

            assert_eq!(
                engine.safe.get(&1),
                Some(&cut_id),
                "n={n}: safe[1] should hold exactly the cut this party's own votes converged on"
            );
            assert!(
                engine.voted.contains(&1),
                "n={n}: reaching safe should have sent this party's own Decide (Fig. 2's Vote step)"
            );
        }
    }

    /// REQUIRED (task): a party reaches `safe[r]` purely by counting votes, with no
    /// certificate message ever crossing the wire -- proved at the type level, not
    /// merely asserted at run time. `mint_threshold` (n=10: quorum_threshold = 7) is
    /// never reached below `quorum_threshold` distinct votes -- so there is no
    /// shortcut that marks a round safe on fewer than a quorum's worth of
    /// first-hand-counted, individually-verified votes.
    #[test]
    fn safe_is_reached_only_by_counting_distinct_votes_never_below_quorum() {
        let (committee, keys) = committee_of(10);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let leader = agb::proposer(&committee, 2);
        let mut engine = CutEngine::new(leader, committee.clone(), 1_000);

        let effects = engine.try_propose_cut_for_current_round(&tips, &oracle);
        let cut_id = find_proposal(&effects).expect("leader proposes").id();
        assert!(
            !engine.safe.contains_key(&1),
            "the leader's own self-vote alone is not enough"
        );

        let quorum = committee.quorum_threshold();
        let mut voted = 1u32; // the leader's own self-vote, above
        for author in keys.iter().filter(|k| **k != leader) {
            if engine.safe.contains_key(&1) {
                break;
            }
            engine.process_cut_vote(
                CutVote {
                    round: 1,
                    cut_id: cut_id.clone(),
                    author: *author,
                },
                &tips,
                &oracle,
            );
            voted += 1;
            if !engine.safe.contains_key(&1) {
                assert!(
                    voted <= quorum,
                    "still not safe with {voted} distinct votes counted (quorum is {quorum}) \
                     -- mint_threshold should never exceed n"
                );
            }
        }

        assert!(
            engine.safe.contains_key(&1),
            "committee is large enough to reach mint_threshold"
        );
        assert_eq!(engine.safe.get(&1), Some(&cut_id));
        assert!(
            voted >= quorum,
            "safe was reached with only {voted} distinct votes, fewer than quorum_threshold \
             ({quorum}) -- mint_threshold's clamp is not holding"
        );

        // A single corrupt (or merely noisy) party resending its OWN already-counted
        // vote cannot manufacture additional weight: `CutVoteAggregator`'s dedup
        // (`used: HashSet<PublicKey>`) rejects a repeat from the same author, so
        // crossing `mint_threshold` categorically requires that many DISTINCT
        // authors, never fewer authors voting more times.
        let repeat_author = keys[0];
        let before = engine.safe.get(&1).cloned();
        engine.process_cut_vote(
            CutVote {
                round: 1,
                cut_id: cut_id.clone(),
                author: repeat_author,
            },
            &tips,
            &oracle,
        );
        assert_eq!(
            engine.safe.get(&1),
            before.as_ref(),
            "a replayed vote changes nothing"
        );
    }

    /// REQUIRED (task): a forged notarization is impossible -- there is no message a
    /// party can send that makes ANOTHER party mark a round safe without that party
    /// itself counting `mint_threshold` distinct votes. This is asserted
    /// STRUCTURALLY, not merely at run time: `Inbound` is `CutEngine`'s entire
    /// message surface (see `handle`'s own match over it), and the match below has NO
    /// wildcard arm -- if a certificate-shaped variant is ever added back to
    /// `Inbound`, this test module fails to COMPILE, not merely to pass, until it is
    /// explicitly handled here. The only path that ever populates `safe` is
    /// `process_cut_vote` -> `mark_cut_safe`, reached exclusively by this party's own
    /// `CutVoteAggregator` crossing `mint_threshold` (see the two tests above) --
    /// never by trusting a relayed aggregate asserted by another party.
    #[test]
    fn inbound_has_no_certificate_shaped_variant() {
        fn assert_exhaustive_with_no_certificate_arm(inbound: Inbound) {
            match inbound {
                Inbound::CutProposal(_)
                | Inbound::CutVote(_)
                // BRACHA VARIANT ADDITION: `Inbound::CutReady` is exactly the kind of
                // new variant this test's own doc comment describes -- adding it here
                // is REQUIRED (the match would fail to COMPILE otherwise, per this
                // test's stated purpose), not a behavior change to the test itself.
                // Same single-named-author shape as `CutVote`, so it belongs on this
                // side of the match, not a certificate-shaped arm this test would need
                // to reject.
                | Inbound::CutReady(_)
                | Inbound::Decide(_)
                | Inbound::Timeout(_)
                | Inbound::TimeoutAccept(_)
                | Inbound::TimerFired(_)
                | Inbound::CutFetch(_, _, _)
                | Inbound::CutServe(_) => {}
            }
        }
        // Any one instance suffices: the exhaustiveness check above is performed by
        // the compiler against the LIVE `Inbound` definition, not by this call.
        assert_exhaustive_with_no_certificate_arm(Inbound::TimerFired(0));
    }

    /// AUDIT (ADAPTED for the Fig.-2 rewrite -- the trigger was a hand-built
    /// `CutCertificate`, now `mint_threshold`-many `CutVote`s): what actually happens
    /// to a party that counts enough votes to mark round r safe LOCALLY but never
    /// itself received round r's own PROPOSAL. Establishes whether the failure mode
    /// is a divergent commit order (unsafe) or a stall (a liveness gap) -- unchanged
    /// by the rewrite, since a `CutVote` names only a `round` and `cut_id`, exactly
    /// as a `CutCertificate` did.
    #[test]
    fn missing_proposal_stalls_the_chain_rather_than_skipping_a_round() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        // An observer that leads neither round, so it only ever *receives*.
        let observer = *keys
            .iter()
            .find(|k| **k != round1_leader && **k != round2_leader)
            .expect("n=4 has a non-leader");
        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        // Round 1's cut, as some other party would have built it. This engine never
        // sees the proposal itself -- only enough of its peers' CutVotes to cross
        // mint_threshold locally.
        let round1_cut = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let round1_id = round1_cut.id();
        let voters: Vec<PublicKey> = keys
            .iter()
            .take(committee.quorum_threshold() as usize) // == mint_threshold at n=4
            .copied()
            .collect();
        let mut effects = Vec::new();
        for author in voters {
            effects = engine.process_cut_vote(
                CutVote {
                    round: 1,
                    cut_id: round1_id.clone(),
                    author,
                },
                &tips,
                &oracle,
            );
        }
        assert_eq!(
            engine.safe.get(&1),
            Some(&round1_id),
            "mint_threshold distinct votes mark round 1 safe locally"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, CutEffect::Broadcast(CutOut::Decide(d)) if d.round == 1)),
            "reaching safe locally produces a Decide for round 1"
        );

        // Round 2's proposal chains onto round 1's cut, whose digest this engine has
        // never recorded (`record_cut_proposal` is the only writer of cut_round_by_id).
        let round2 = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: round1_id,
            tips: tips.clone(),
        };
        let effects = engine.process_cut_proposal(round2, &tips, &oracle);
        assert!(
            effects.is_empty(),
            "round 2 cannot be recorded or voted: its parent is unknown here"
        );
        assert!(
            !engine.pending_cut_children.is_empty(),
            "round 2 is buffered pending round 1's proposal"
        );

        // Even a full quorum of Decides for round 2 cannot commit it, because
        // try_commit_round requires leader_cut_by_round[2], set only when the proposal
        // is recorded. So the chain STALLS -- it never emits round 2 ahead of round 1.
        for author in keys.iter().copied() {
            let effects = engine.process_decide(Decide {
                id: round2_leader_cut_id(&tips, round2_leader),
                round: 2,
                author,
            });
            assert!(
                find_commits(&effects, 2).is_empty(),
                "round 2 must never commit while round 1's proposal is missing"
            );
        }
    }

    fn round2_leader_cut_id(tips: &Cut, leader: PublicKey) -> Digest {
        CutProposal {
            round: 2,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        }
        .id()
    }

    /// AUDIT FOLLOWUP (cut-proposal repair; ADAPTED for the Fig.-2 rewrite -- was
    /// `certificate_with_unknown_proposal_triggers_fetch_to_its_voters`): the same
    /// missing-proposal scenario as `missing_proposal_stalls_the_chain_rather_than_
    /// skipping_a_round` above, now asserting the repair fix -- locally crossing
    /// `mint_threshold` for round 1's cut_id emits a fetch for its own `(round,
    /// cut_id)` addressed to exactly the witnesses THIS party itself counted (every
    /// one of them sent a `CutVote` naming this cut_id, i.e. claimed, by voting, to
    /// have seen the proposal -- see `mark_cut_safe`'s own call to `ensure_cut_fetch`
    /// and `CutVoteAggregator::append`'s returned voter list).
    #[test]
    fn local_safe_with_unknown_proposal_triggers_fetch_to_its_witnesses() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let observer = *keys
            .iter()
            .find(|k| **k != round1_leader && **k != round2_leader)
            .expect("n=4 has a non-leader");
        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        let round1_cut = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let round1_id = round1_cut.id();
        let voters: Vec<PublicKey> = keys
            .iter()
            .take(committee.quorum_threshold() as usize) // == mint_threshold at n=4
            .copied()
            .collect();

        let mut effects = Vec::new();
        for author in voters.clone() {
            effects = engine.process_cut_vote(
                CutVote {
                    round: 1,
                    cut_id: round1_id.clone(),
                    author,
                },
                &tips,
                &oracle,
            );
        }
        assert!(
            engine.safe.contains_key(&1),
            "test setup must actually cross mint_threshold"
        );

        let fetches = find_fetches(&effects);
        assert!(
            fetches.iter().all(|(_, r, id)| *r == 1 && *id == round1_id),
            "every fetch should name round 1's own cut_id: {fetches:?}"
        );
        let mut fetch_targets: Vec<PublicKey> = fetches.iter().map(|(p, _, _)| *p).collect();
        fetch_targets.sort();
        let mut expected = voters;
        expected.sort();
        assert_eq!(
            fetch_targets, expected,
            "the fetch should be addressed to exactly the witnesses whose votes were counted"
        );
        assert_eq!(
            engine.pending_cut_fetch.get(&(1, round1_id)),
            Some(&engine.cut_round),
            "the fetch should be latched for retry bookkeeping"
        );
    }

    /// AUDIT FOLLOWUP (cut-proposal repair, additional coverage beyond the task's
    /// required list -- see the report): the OTHER trigger -- a proposal citing a
    /// parent this engine has never heard of AT ALL (no certificate either) still
    /// gets buffered exactly as before, but now ALSO fans a fetch out to the full
    /// committee, since there is no narrower evidence available for who holds it
    /// (see `process_cut_proposal`'s own comment at this call site).
    #[test]
    fn buffered_child_with_unknown_parent_triggers_fetch_to_committee() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round2_leader = agb::proposer(&committee, 3);
        let mut engine = CutEngine::new(keys[0], committee.clone(), 1_000);

        let unknown_parent = Digest([42; 32]);
        let round2_cut = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: unknown_parent.clone(),
            tips: tips.clone(),
        };

        let effects = engine.process_cut_proposal(round2_cut, &tips, &oracle);

        assert!(
            !engine.pending_cut_children.is_empty(),
            "the proposal should still be buffered, exactly as before this fix"
        );
        let fetches = find_fetches(&effects);
        assert!(
            fetches
                .iter()
                .all(|(_, r, id)| *r == 1 && *id == unknown_parent),
            "the fetch should name round 1 (the best-effort round - 1 guess) and the \
             unknown parent digest: {fetches:?}"
        );
        let mut fetch_targets: Vec<PublicKey> = fetches.iter().map(|(p, _, _)| *p).collect();
        fetch_targets.sort();
        let mut expected: Vec<PublicKey> =
            keys.iter().filter(|k| **k != keys[0]).copied().collect();
        expected.sort();
        assert_eq!(
            fetch_targets, expected,
            "with no narrower evidence, the fetch should go to every other committee member"
        );
    }

    /// A served proposal that hash-matches an outstanding request unblocks a
    /// buffered child: the parent is recorded, the buffered round-2 child is
    /// reparented and voted, and (its Decide quorum already in, exactly as in the
    /// stall scenario) round 2 now commits.
    #[test]
    fn served_proposal_matching_request_unblocks_reparents_and_commits() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let observer = *keys
            .iter()
            .find(|k| **k != round1_leader && **k != round2_leader)
            .expect("n=4 has a non-leader");
        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        let round1_cut = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let round1_id = round1_cut.id();
        let round2_cut = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: round1_id.clone(),
            tips: tips.clone(),
        };
        let round2_id = round2_cut.id();

        // Seed the exact state the stall scenario reaches (see
        // `missing_proposal_stalls_the_chain_rather_than_skipping_a_round`): round 2
        // buffered pending round 1's still-unknown proposal, and an outstanding
        // fetch for it (as `mark_cut_safe`'s own trigger would have set up -- seeded
        // directly here to isolate `on_cut_serve`'s own accept/dispatch behavior,
        // mirroring `queue_with_invalid_sibling_still_processes_valid_one`'s
        // identical direct-seeding style).
        engine
            .pending_cut_children
            .insert((2, round1_id.clone()), vec![round2_cut.clone()]);
        engine
            .pending_cut_fetch
            .insert((1, round1_id.clone()), engine.cut_round());

        // A full quorum of round-2 Decides, exactly as the stall scenario feeds --
        // these land BEFORE the parent is ever known and (per that scenario) cannot
        // commit yet, since `leader_cut_by_round[2]` isn't set until the proposal is
        // recorded.
        for author in keys.iter().copied() {
            let effects = engine.process_decide(Decide {
                id: round2_id.clone(),
                round: 2,
                author,
            });
            assert!(find_commits(&effects, 2).is_empty());
        }
        assert!(engine.committed.contains_key(&2));

        // The serve arrives.
        let effects = engine.on_cut_serve(round1_cut, &tips, &oracle);

        assert_eq!(
            engine.cut_round_by_id.get(&round1_id),
            Some(&1),
            "the served proposal should have been recorded"
        );
        assert!(
            !engine
                .pending_cut_children
                .contains_key(&(2, round1_id.clone())),
            "the buffered round-2 child should have been reparented"
        );
        assert!(
            find_vote_for_round(&effects, 2).is_some(),
            "the reparented round-2 proposal should have been voted on"
        );
        assert_eq!(
            find_commits(&effects, 2),
            vec![(2, tips.clone())],
            "round 2's already-quorate Decide should now commit"
        );
        assert!(
            !engine.pending_cut_fetch.contains_key(&(1, round1_id)),
            "the satisfied fetch should be cleared"
        );
    }

    /// A served proposal that does NOT hash-match any outstanding request is
    /// rejected and changes no engine state -- including when a DIFFERENT pending
    /// fetch happens to share the same digest but a different round (`CutProposal::
    /// id()` hashes `round` too, so `(2, cut_id)` and `(1, cut_id)` are distinct
    /// pairs).
    #[test]
    fn served_proposal_not_matching_any_request_is_rejected() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;
        let leader = agb::proposer(&committee, 2);
        let mut engine = CutEngine::new(keys[0], committee, 1_000);

        let proposal = CutProposal {
            round: 1,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let cut_id = proposal.id();

        // An outstanding request exists, but for a DIFFERENT round.
        engine.pending_cut_fetch.insert((2, cut_id.clone()), 1);

        let effects = engine.on_cut_serve(proposal, &tips, &oracle);

        assert!(
            effects.is_empty(),
            "a serve matching no requested pair must produce no effects"
        );
        assert!(
            !engine.cut_round_by_id.contains_key(&cut_id),
            "an unmatched serve must not be recorded"
        );
        assert!(engine.cut_proposals.is_empty());
        assert!(
            engine.pending_cut_fetch.contains_key(&(2, cut_id)),
            "the unrelated pending entry must be untouched"
        );
    }

    /// `on_cut_fetch` answers when the proposal is held, answers a given requester
    /// only once for the same pair, and answers nothing once the round has been
    /// pruned below the GC floor.
    #[test]
    fn on_cut_fetch_answers_when_held_once_per_requester_and_respects_gc_floor() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let leader = agb::proposer(&committee, 2);
        let mut engine = CutEngine::new(keys[0], committee, 1_000);

        let proposal = CutProposal {
            round: 3,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let cut_id = proposal.id();
        engine.record_cut_proposal(proposal); // held directly -- isolates on_cut_fetch

        let requester = keys[1];
        let effects = engine.on_cut_fetch(requester, 3, cut_id.clone());
        match effects.as_slice() {
            [CutEffect::ServeTo {
                peer,
                proposal: served,
            }] => {
                assert_eq!(*peer, requester);
                assert_eq!(served.id(), cut_id);
            }
            other => panic!("expected exactly one ServeTo effect, got {other:?}"),
        }

        // Same requester, same pair -- already answered, no repeat.
        let effects = engine.on_cut_fetch(requester, 3, cut_id.clone());
        assert!(
            effects.is_empty(),
            "the same requester must not be answered twice"
        );

        // A DIFFERENT requester for the same pair is still owed its own answer.
        let other_requester = keys[2];
        let effects = engine.on_cut_fetch(other_requester, 3, cut_id.clone());
        assert_eq!(
            effects.len(),
            1,
            "a different requester gets its own answer"
        );

        // Below the GC floor: nothing, even for a still-fresh requester.
        engine.prune_below(4);
        let fresh_requester = keys[3];
        let effects = engine.on_cut_fetch(fresh_requester, 3, cut_id);
        assert!(
            effects.is_empty(),
            "a round pruned below the GC floor must not be served"
        );
    }

    /// Retry backoff: `ensure_cut_fetch` does not re-emit a fetch for the same
    /// `(round, cut_id)` pair before `FETCH_RETRY_ROUNDS` cut rounds have elapsed
    /// since the last fan-out, and does re-emit once they have.
    #[test]
    fn cut_fetch_retry_backoff_holds_until_the_window_elapses() {
        let (committee, keys) = committee_of(4);
        let mut engine = CutEngine::new(keys[0], committee, 1_000);
        let cut_id = Digest([7; 32]);
        let targets = vec![keys[1], keys[2]];

        let first = engine.ensure_cut_fetch(1, &cut_id, targets.clone());
        assert_eq!(first.len(), 2, "the first call fans out to every target");

        // Immediately retried (still within FETCH_RETRY_ROUNDS of cut_round==1):
        // no-op.
        let again = engine.ensure_cut_fetch(1, &cut_id, targets.clone());
        assert!(again.is_empty(), "retried too soon -- must not re-fan");

        // Advance the engine's own retry clock short of the threshold: still no-op.
        engine.cut_round = 1 + CutEngine::FETCH_RETRY_ROUNDS - 1;
        let still_too_soon = engine.ensure_cut_fetch(1, &cut_id, targets.clone());
        assert!(
            still_too_soon.is_empty(),
            "one round short of the window -- still no-op"
        );

        // Advance to exactly the threshold: re-fans.
        engine.cut_round = 1 + CutEngine::FETCH_RETRY_ROUNDS;
        let retried = engine.ensure_cut_fetch(1, &cut_id, targets);
        assert_eq!(retried.len(), 2, "past the retry window -- fans out again");
    }

    /// Test 2: timeout path -- leader silent, timer fires, `Timeout` reaches quorum,
    /// `TimeoutAccept` amplifies at f+1 and certifies at quorum, the round is marked
    /// timed-out, `cut_round` advances, and a pending child whose parent was skipped is
    /// retried.
    #[test]
    fn timeout_path_advances_round_and_retries_pending_child() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;

        // An observer distinct from round 2's leader (see the assertions below for
        // why that separation matters).
        let observer = agb::proposer(&committee, 2); // round 1's leader, used only as
                                                     // this engine's own identity
        let round2_leader = agb::proposer(&committee, 3);
        assert_ne!(
            observer, round2_leader,
            "test setup needs two distinct leaders"
        );

        let mut engine = CutEngine::new(observer, committee.clone(), 1_000);

        // Round 1's leader stays silent: nobody ever calls try_propose/process_cut_proposal
        // for round 1 on this engine.

        // Before round 1 certifies as timed out, a round-2 proposal citing the
        // (still-genesis) parent is not yet `safe_cut_parent` -- it gets buffered.
        let pending_child = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let effects = engine.process_cut_proposal(pending_child.clone(), &tips, &oracle);
        assert!(
            effects.is_empty(),
            "round 2 is not yet safe, nothing to do yet"
        );
        assert!(
            !engine.pending_cut_children.is_empty(),
            "the round-2 proposal should be buffered pending round 1's resolution"
        );

        // The round-1 timer fires.
        let mut effects = engine.process_cut_timer(1, &tips, &oracle);
        assert!(matches!(
            effects.as_slice(),
            [CutEffect::Broadcast(CutOut::Timeout(t))] if t.round == 1
        ));

        // Bring in other committee members' timeouts until quorum_threshold.
        let mut others = keys.iter().filter(|k| **k != observer);
        loop {
            if effects.iter().any(
                |e| matches!(e, CutEffect::Broadcast(CutOut::TimeoutAccept(a)) if a.round == 1),
            ) {
                break;
            }
            let author = *others.next().expect("enough members to reach quorum");
            let timeout = Timeout { round: 1, author };
            effects = engine.process_timeout(timeout, &tips, &oracle);
        }
        assert!(!engine.certified_timed_out.contains(&1));

        // Bring in other committee members' timeout-accepts. f+1 should amplify our own
        // accept (already sent above, so a no-op) and quorum should certify.
        let mut others = keys.iter().filter(|k| **k != observer);
        let mut saw_cert = false;
        while !saw_cert {
            let author = *others.next().expect("enough members to reach quorum");
            let accept = TimeoutAccept { round: 1, author };
            effects = engine.process_timeout_accept(accept, &tips, &oracle);
            saw_cert = engine.certified_timed_out.contains(&1);
        }

        assert!(
            engine.certified_timed_out.contains(&1),
            "round 1 is certified timed out"
        );
        assert_eq!(
            engine.cut_round, 2,
            "cut_round should advance past the timed-out round"
        );
        assert!(
            engine.pending_cut_children.is_empty(),
            "the pending round-2 child should have been retried"
        );
        assert!(
            find_vote_for_round(&effects, 2).is_some(),
            "the retried round-2 proposal should have been voted on"
        );
    }

    /// Test 3: the f+1 gate. With `gate_tips: true` and one tip unavailable, no vote is
    /// emitted; with `gate_tips: false`, a vote IS emitted for the same input.
    #[test]
    fn gate_tips_blocks_vote_when_tip_unavailable() {
        let (committee, keys) = committee_of(4);
        let leader = agb::proposer(&committee, 2);
        let unavailable_author = keys.iter().find(|k| **k != leader).copied().unwrap();
        let proposal_tips = sample_tips(&keys);
        let proposal = CutProposal {
            round: 1,
            proposer: leader,
            parent_cut: Digest::default(),
            tips: proposal_tips,
        };
        let oracle = DenyAuthor(unavailable_author);
        let dummy_tips = Cut::new();

        let mut gated = CutEngine::new(keys[2], committee.clone(), 1_000);
        assert_ne!(keys[2], leader);
        let effects = gated.process_cut_proposal(proposal.clone(), &dummy_tips, &oracle);
        assert!(
            find_vote_for_round(&effects, 1).is_none(),
            "gate_tips defaults to true and one tip is unavailable -- no vote"
        );

        let mut ungated = CutEngine::new(keys[2], committee, 1_000).with_gate_tips(false);
        let effects = ungated.process_cut_proposal(proposal, &dummy_tips, &oracle);
        assert!(
            find_vote_for_round(&effects, 1).is_some(),
            "gate_tips: false reproduces upstream's blind vote for the same input"
        );
    }

    /// Test 4: deviation-4's fix. A `pending_cut_children` bucket with one invalid and
    /// one valid sibling still processes the valid one once the shared parent becomes
    /// known.
    ///
    /// Note on construction: upstream's verify()/leader-authenticity checks both
    /// precede the "is the parent known yet" buffering step, so an item that would
    /// fail either check is rejected on first contact and never actually reaches
    /// `pending_cut_children` organically -- anything that *is* buffered has, by
    /// construction, already passed both checks once, and since neither depends on
    /// anything that can change while this engine runs (the proposal's own fields, and
    /// the fixed committee), it will pass them again when dequeued. This test isolates
    /// the loop's own per-item-rejection behavior (what deviation 4 actually changes)
    /// from that fact by seeding the pending bucket directly -- exactly as
    /// `prune_below_is_exact` seeds other fields directly -- rather than relying on
    /// two separate `process_cut_proposal` calls to organically buffer both siblings.
    #[test]
    fn queue_with_invalid_sibling_still_processes_valid_one() {
        let (committee, keys) = committee_of(4);
        let tips = sample_tips(&keys);
        let oracle = AllAvailable;

        let round1_leader = agb::proposer(&committee, 2);
        let round2_leader = agb::proposer(&committee, 3);
        let not_round2_leader = keys.iter().copied().find(|k| *k != round2_leader).unwrap();

        let mut engine = CutEngine::new(round1_leader, committee, 1_000);

        let parent = CutProposal {
            round: 1,
            proposer: round1_leader,
            parent_cut: Digest::default(),
            tips: tips.clone(),
        };
        let parent_id = parent.id();

        let invalid_child = CutProposal {
            round: 2,
            proposer: not_round2_leader, // wrong leader for round 2 -- rejected at dequeue
            parent_cut: parent_id.clone(),
            tips: tips.clone(),
        };
        let valid_child = CutProposal {
            round: 2,
            proposer: round2_leader,
            parent_cut: parent_id.clone(),
            tips: tips.clone(),
        };
        let valid_child_id = valid_child.id();

        engine
            .pending_cut_children
            .insert((2, parent_id.clone()), vec![invalid_child, valid_child]);

        // The parent arrives: both seeded children move into the internal queue,
        // invalid one first (insertion order). Without the deviation-4 fix, rejecting
        // the invalid one would abort the whole call and the valid sibling -- already
        // removed from `pending_cut_children` at that point -- would be lost.
        let effects = engine.process_cut_proposal(parent, &tips, &oracle);

        assert!(
            !engine.pending_cut_children.contains_key(&(2, parent_id)),
            "both siblings should have been drained from the pending queue"
        );
        let vote =
            find_vote_for_round(&effects, 2).expect("the valid sibling should have been voted on");
        assert_eq!(vote.cut_id, valid_child_id);
    }

    /// Test 5: `prune_below` removes exactly the entries strictly below the floor and
    /// nothing at or above it, across every round-prunable field.
    #[test]
    fn prune_below_is_exact() {
        let (committee, _keys) = committee_of(4);
        let mut engine = CutEngine::new(key(1), committee, 1_000);

        let d1 = Digest([1; 32]);
        let d2 = Digest([2; 32]);
        let proposal_at = |round: CutRound| CutProposal {
            round,
            ..CutProposal::default()
        };

        engine
            .cut_vote_aggregators
            .insert((1, d1.clone()), CutVoteAggregator::new());
        engine
            .cut_vote_aggregators
            .insert((2, d2.clone()), CutVoteAggregator::new());
        engine
            .timeouts_aggregators
            .insert(1, TimeoutAggregator::new());
        engine
            .timeouts_aggregators
            .insert(2, TimeoutAggregator::new());
        engine
            .timeout_accept_aggregators
            .insert(1, TimeoutAcceptAggregator::new());
        engine
            .timeout_accept_aggregators
            .insert(2, TimeoutAcceptAggregator::new());
        engine.cut_proposals.insert((1, d1.clone()), proposal_at(1));
        engine.cut_proposals.insert((2, d2.clone()), proposal_at(2));
        engine
            .pending_cut_children
            .insert((1, d1.clone()), vec![proposal_at(1)]);
        engine
            .pending_cut_children
            .insert((2, d2.clone()), vec![proposal_at(2)]);
        engine.cut_round_by_id.insert(d1.clone(), 1);
        engine.cut_round_by_id.insert(d2.clone(), 2);
        engine.leader_cut_by_round.insert(1, d1.clone());
        engine.leader_cut_by_round.insert(2, d2.clone());
        engine.safe.insert(1, d1.clone());
        engine.safe.insert(2, d2.clone());
        engine
            .decide_aggregators
            .insert((1, d1.clone()), DecideAggregator::new());
        engine
            .decide_aggregators
            .insert((2, d2.clone()), DecideAggregator::new());
        engine.committed.insert(
            1,
            Decide {
                id: d1.clone(),
                round: 1,
                author: key(1),
            },
        );
        engine.committed.insert(
            2,
            Decide {
                id: d2.clone(),
                round: 2,
                author: key(1),
            },
        );
        for set in [
            &mut engine.sent_cut_votes,
            &mut engine.proposed_cut_rounds,
            &mut engine.voted,
            &mut engine.sent_commit_rounds,
            &mut engine.timed_out,
            &mut engine.sent_timeout_accepts,
            &mut engine.certified_timed_out,
            &mut engine.scheduled_cut_timers,
        ] {
            set.insert(1);
            set.insert(2);
        }
        engine.pending_cut_fetch.insert((1, d1.clone()), 1);
        engine.pending_cut_fetch.insert((2, d2.clone()), 2);
        engine.fetch_answered.insert((1, d1.clone(), key(1)));
        engine.fetch_answered.insert((2, d2.clone(), key(1)));

        engine.prune_below(2);

        assert!(!engine.cut_vote_aggregators.contains_key(&(1, d1.clone())));
        assert!(engine.cut_vote_aggregators.contains_key(&(2, d2.clone())));
        assert!(!engine.timeouts_aggregators.contains_key(&1));
        assert!(engine.timeouts_aggregators.contains_key(&2));
        assert!(!engine.timeout_accept_aggregators.contains_key(&1));
        assert!(engine.timeout_accept_aggregators.contains_key(&2));
        assert!(!engine.cut_proposals.contains_key(&(1, d1.clone())));
        assert!(engine.cut_proposals.contains_key(&(2, d2.clone())));
        assert!(!engine.pending_cut_children.contains_key(&(1, d1.clone())));
        assert!(engine.pending_cut_children.contains_key(&(2, d2.clone())));
        assert!(
            !engine.cut_round_by_id.contains_key(&d1),
            "cut_round_by_id should be cleaned up alongside cut_proposals"
        );
        assert!(engine.cut_round_by_id.contains_key(&d2));
        assert!(!engine.leader_cut_by_round.contains_key(&1));
        assert!(engine.leader_cut_by_round.contains_key(&2));
        assert!(!engine.safe.contains_key(&1));
        assert!(engine.safe.contains_key(&2));
        assert!(!engine.pending_cut_fetch.contains_key(&(1, d1.clone())));
        assert!(engine.pending_cut_fetch.contains_key(&(2, d2.clone())));
        assert!(!engine.fetch_answered.contains(&(1, d1.clone(), key(1))));
        assert!(engine.fetch_answered.contains(&(2, d2.clone(), key(1))));
        assert!(!engine.decide_aggregators.contains_key(&(1, d1)));
        assert!(engine.decide_aggregators.contains_key(&(2, d2)));
        assert!(!engine.committed.contains_key(&1));
        assert!(engine.committed.contains_key(&2));
        for set in [
            &engine.sent_cut_votes,
            &engine.proposed_cut_rounds,
            &engine.voted,
            &engine.sent_commit_rounds,
            &engine.timed_out,
            &engine.sent_timeout_accepts,
            &engine.certified_timed_out,
            &engine.scheduled_cut_timers,
        ] {
            assert!(!set.contains(&1));
            assert!(set.contains(&2));
        }

        // gc_floor moved with it -- sanitize_timeout_accept now rejects round 1.
        assert!(engine
            .sanitize_timeout_accept(&TimeoutAccept {
                round: 1,
                author: key(1)
            })
            .is_err());
        assert!(engine
            .sanitize_timeout_accept(&TimeoutAccept {
                round: 2,
                author: key(1)
            })
            .is_ok());

        // Idempotent / monotonic: pruning to an earlier-or-equal floor is a no-op.
        engine.prune_below(1);
        assert!(engine.safe.contains_key(&2));
    }

    /// BRACHA VARIANT ADDITION: `prune_below` covers the two new round-prunable
    /// fields (`cut_ready_aggregators`, `sent_cut_ready`) exactly like
    /// `prune_below_is_exact` above covers every pre-existing one -- a separate test
    /// (rather than an addition to that one) so no pre-existing test is modified.
    #[test]
    fn prune_below_covers_bracha_ready_state() {
        let (committee, _keys) = committee_of(4);
        let mut engine = CutEngine::new(key(1), committee, 1_000).with_variant(Variant::Bracha);

        let d1 = Digest([1; 32]);
        let d2 = Digest([2; 32]);
        engine
            .cut_ready_aggregators
            .insert((1, d1.clone()), CutReadyAggregator::new());
        engine
            .cut_ready_aggregators
            .insert((2, d2.clone()), CutReadyAggregator::new());
        engine.sent_cut_ready.insert(1);
        engine.sent_cut_ready.insert(2);

        engine.prune_below(2);

        assert!(!engine.cut_ready_aggregators.contains_key(&(1, d1)));
        assert!(engine.cut_ready_aggregators.contains_key(&(2, d2)));
        assert!(!engine.sent_cut_ready.contains(&1));
        assert!(engine.sent_cut_ready.contains(&2));

        // Idempotent / monotonic, mirroring `prune_below_is_exact`'s identical check.
        engine.prune_below(1);
        assert!(engine.sent_cut_ready.contains(&2));
    }
}
