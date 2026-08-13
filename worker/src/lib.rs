// Copyright(C) Facebook, Inc. and its affiliates.
mod batch_maker;
mod helper;
mod primary_connector;
mod processor;
mod synchronizer;
mod worker;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::worker::Worker;

/// Benchmark clients use marker 2 for adversarial payload that consumes the
/// real data path but is deliberately excluded from offered/committed TPS.
/// Outside benchmark builds every opaque transaction counts normally.
fn transaction_counts_toward_goodput(transaction: &[u8]) -> bool {
    #[cfg(feature = "benchmark")]
    {
        transaction.first().is_none_or(|marker| *marker < 2)
    }
    #[cfg(not(feature = "benchmark"))]
    {
        let _ = transaction;
        true
    }
}

#[cfg(all(test, feature = "benchmark"))]
mod transaction_marker_tests {
    use super::transaction_counts_toward_goodput;

    #[test]
    fn adversarial_background_marker_is_excluded_from_goodput() {
        assert!(transaction_counts_toward_goodput(&[0]));
        assert!(transaction_counts_toward_goodput(&[1]));
        assert!(!transaction_counts_toward_goodput(&[2]));
    }
}
