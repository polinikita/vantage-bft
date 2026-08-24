use config::Committee;

/// Party-count BFT thresholds derived from `n`; these values are not stake weighted.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    pub n: usize,
    pub f_parties: usize,
    pub f_plus_1_parties: usize,
    pub n_minus_f_parties: usize,
}

impl Thresholds {
    /// Computes `f`, `f + 1`, and the protocol quorum `Q = n - f`.
    pub fn from_party_count(n: usize) -> Self {
        let f_parties = (n - 1) / 3;
        Self {
            n,
            f_parties,
            f_plus_1_parties: f_parties + 1,
            n_minus_f_parties: n - f_parties,
        }
    }

    pub fn from_committee(committee: &Committee) -> Self {
        Self::from_party_count(committee.size())
    }
}
