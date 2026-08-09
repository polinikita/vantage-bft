// PHASE3-SPEC.md §7 / PHASE4-SPEC.md §12 test gate. Each module below cites the
// rule(s) it covers.
mod ack_tests;
mod avail_tests;
mod chain_tests;
// AVAIL-ECHO-SPEC.md: positional availability acknowledgments.
mod claim_tests;
mod common;
mod metrics_tests;
mod registers_tests;
mod repair_tests;
mod retention_tests;
// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §13: canonical sequence objects and the local store.
mod sequence_tests;
// SEQUENCE-CHECKPOINT-SYNC-PLAN.md §10: staging a verified target for installation.
mod install_tests;

// PHASE4-SPEC.md §12
mod agb_echo_tests;
mod completion_tests;
mod cursor_tests;
mod fastseal_tests;
mod frontier_tests;
mod gc_tests;
mod harness;
mod integration_tests;
mod ready_tests;

// PHASE5-SPEC.md §4
mod convergence_tests;
mod crash_fault_tests;
mod pacemaker_tests;
mod wish_trigger_tests;

// PHASE6-SPEC.md §2/§3
mod resolution_gate_tests;

// PHASE6-SPEC.md §4
mod resolve_tests;

// PHASE6-SPEC.md §5/§6
mod control_tests;

// PHASE6-SPEC.md §8
mod byzantine_tests;

// PHASE7 (signature-free.tex's "Batched resolution entries" paragraph)
mod batched_anchors_tests;

// signature-free.tex 704fb29 -- "Grounded post-ready skip" (par:skip-seal)
mod skip_vote_tests;

// signature-free.tex sec.8.3 -- "Digest-named AGB statements"
mod digest_stmt_tests;
