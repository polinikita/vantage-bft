use crate::primary::{Slot, View};
use config::Committee;
use crypto::PublicKey;

pub type LeaderElector = SemiParallelRRLeaderElector;

pub struct SemiParallelRRLeaderElector {
    committee: Committee,
}

impl SemiParallelRRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self { committee }
    }

    pub fn get_leader(&self, slot: Slot, view: View) -> PublicKey {
        let keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        // TODO: Uncomment, this is strictly commented out for testing
        //keys.sort();
        let index = view + slot;
        keys[index as usize % self.committee.size()]
        //keys[1]
    }
}
