// Shared party-count quorum-threshold derivation. `AgbEngine`, `Pacemaker`,
// `Resolver`, and `ControlLog` each need the same BFT constants (f, f+1, 2f+1,
// n-f) derived from the committee's PARTY COUNT `n` -- these are the paper's
// per-party thresholds (D4-3: "fast-seal thresholds count parties, not stake"),
// distinct from `config::Committee::quorum_threshold`/`validity_threshold`
// (STAKE-weighted, used by `LaneManager`/`Repairer` for `is_q_available`). Never
// conflate the two: this type only ever counts committee members, never stake.

use config::Committee;

/// `n = 3f + 1 + k` (`0 <= k < 3`) party-count BFT thresholds. Immutable once
/// constructed -- every field is a fixed function of `n`.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    /// Committee size (party count).
    pub n: usize,
    /// f: max tolerated Byzantine parties.
    pub f_parties: usize,
    /// f+1: validity/availability threshold (party count).
    pub f_plus_1_parties: usize,
    /// 2f+1: quorum threshold (party count).
    pub two_f_plus_1_parties: usize,
    /// n-f: the "all but the Byzantine parties" threshold (party count).
    pub n_minus_f_parties: usize,
}

impl Thresholds {
    /// Derives every threshold from a raw party count.
    pub fn from_party_count(n: usize) -> Self {
        let f_parties = (n - 1) / 3;
        Self {
            n,
            f_parties,
            f_plus_1_parties: f_parties + 1,
            two_f_plus_1_parties: 2 * f_parties + 1,
            n_minus_f_parties: n - f_parties,
        }
    }

    /// Derives every threshold from `committee.size()`.
    pub fn from_committee(committee: &Committee) -> Self {
        Self::from_party_count(committee.size())
    }
}
