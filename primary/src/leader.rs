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
    leader_count: u64,
    /// Section 5.4 offsets consecutive slot schedules by f authorities.
    slot_offset: u64,
}

impl SemiParallelRRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        let leader_count = committee.size() as u64;
        Self {
            leaders: RoundRobin::new(&committee),
            leader_count,
            slot_offset: leader_count.saturating_sub(1) / 3,
        }
    }

    pub fn get_leader(&self, slot: Slot, view: View) -> PublicKey {
        // Reduce before multiplying so adversarial slot/view values cannot
        // overflow while preserving the round-robin schedule exactly.
        let slot_shift =
            (slot % self.leader_count) * (self.slot_offset % self.leader_count) % self.leader_count;
        let position = ((view % self.leader_count) + slot_shift) % self.leader_count;
        self.leaders.at(position)
    }
}

#[cfg(test)]
mod tests {
    use super::{RoundRobin, SemiParallelRRLeaderElector};

    #[test]
    fn parallel_slot_schedules_are_offset_by_f() {
        let (committee, _) = config::Committee::local_benchmark(10, 1, 18_000);
        let expected = RoundRobin::new(&committee);
        let elector = SemiParallelRRLeaderElector::new(committee);

        for slot in 1..=10 {
            assert_eq!(elector.get_leader(slot, 1), expected.at(1 + slot * 3));
            assert_eq!(elector.get_leader(slot, 2), expected.at(2 + slot * 3));
        }
    }
}
