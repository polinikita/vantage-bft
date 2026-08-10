use crate::primary::{Slot, View};
use config::Committee;
use crypto::PublicKey;

pub type LeaderElector = SemiParallelRRLeaderElector;

pub(crate) struct RoundRobin {
    authorities: Vec<PublicKey>,
}

impl RoundRobin {
    pub(crate) fn new(committee: &Committee) -> Self {
        let authorities: Vec<PublicKey> = committee.authorities.keys().copied().collect();
        assert!(!authorities.is_empty(), "committee must not be empty");
        Self { authorities }
    }

    pub(crate) fn at(&self, position: u64) -> PublicKey {
        self.authorities[wrapped_index(self.authorities.len(), position)]
    }

    pub(crate) fn one_based(&self, round: u64) -> PublicKey {
        self.at(round.saturating_sub(1))
    }
}

pub(crate) fn one_based_authority(committee: &Committee, round: u64) -> PublicKey {
    let index = wrapped_index(committee.size(), round.saturating_sub(1));
    *committee
        .authorities
        .keys()
        .nth(index)
        .expect("committee must not be empty")
}

fn wrapped_index(authority_count: usize, position: u64) -> usize {
    assert!(authority_count > 0, "committee must not be empty");
    (position % authority_count as u64) as usize
}

pub struct SemiParallelRRLeaderElector {
    leaders: RoundRobin,
}

impl SemiParallelRRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self {
            leaders: RoundRobin::new(&committee),
        }
    }

    pub fn get_leader(&self, slot: Slot, view: View) -> PublicKey {
        self.leaders.at(view + slot)
    }
}
