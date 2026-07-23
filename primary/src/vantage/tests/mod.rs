// PHASE3-SPEC.md §7 / PHASE4-SPEC.md §12 test gate. Each module below cites the
// rule(s) it covers.
mod common;
mod chain_tests;
mod ack_tests;
mod registers_tests;
mod repair_tests;
mod retention_tests;
mod metrics_tests;

// PHASE4-SPEC.md §12
mod agb_echo_tests;
mod ready_tests;
mod completion_tests;
mod fastseal_tests;
mod frontier_tests;
mod cursor_tests;
mod harness;
mod integration_tests;

// PHASE5-SPEC.md §4
mod pacemaker_tests;
mod wish_trigger_tests;
mod crash_fault_tests;
mod convergence_tests;

// PHASE6-SPEC.md §2/§3
mod resolution_gate_tests;

// PHASE6-SPEC.md §4
mod resolve_tests;

// PHASE6-SPEC.md §5/§6
mod control_tests;

// PHASE6-SPEC.md §8
mod byzantine_tests;
