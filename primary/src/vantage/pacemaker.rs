use crate::primary::View;
use crate::vantage::{Effect, Thresholds};
use config::Committee;
use crypto::PublicKey;
use std::collections::HashMap;

pub struct Pacemaker {
    /// Largest first-hand wish received from each author.
    omega: Vec<View>,
    index_of: HashMap<PublicKey, usize>,
    own_index: usize,
    f_plus_1_parties: usize,
    two_f_plus_1_parties: usize,
    own_watermark: View,
    entry_target: View,
    largest_entered_view: View,
}

impl Pacemaker {
    pub fn new(name: PublicKey, committee: &Committee) -> Self {
        let names: Vec<PublicKey> = committee.authorities.keys().cloned().collect();
        let n = names.len();
        let thresholds = Thresholds::from_party_count(n);
        let f_plus_1_parties = thresholds.f_plus_1_parties;
        let two_f_plus_1_parties = thresholds.two_f_plus_1_parties;
        let index_of: HashMap<PublicKey, usize> =
            names.iter().enumerate().map(|(i, pk)| (*pk, i)).collect();
        let own_index = *index_of
            .get(&name)
            .expect("self must be a committee member");
        Self {
            omega: vec![0; n],
            index_of,
            own_index,
            f_plus_1_parties,
            two_f_plus_1_parties,
            own_watermark: 0,
            entry_target: 0,
            largest_entered_view: 0,
        }
    }

    pub fn own_watermark(&self) -> View {
        self.own_watermark
    }

    pub fn entry_target(&self) -> View {
        self.entry_target
    }

    pub fn omega_plus(&self) -> View {
        self.kth_largest(self.f_plus_1_parties)
    }

    pub fn omega_q(&self) -> View {
        self.kth_largest(self.two_f_plus_1_parties)
    }

    pub fn entered_view(&self) -> View {
        self.largest_entered_view
    }

    pub fn omega_of(&self, author: PublicKey) -> View {
        self.omega[self.index_of[&author]]
    }

    #[cfg(test)]
    pub(crate) fn largest_entered_view_for_test(&self) -> View {
        self.largest_entered_view
    }

    pub fn genesis(&mut self) -> Vec<Effect> {
        self.largest_entered_view = 1;
        let mut effects = vec![Effect::BroadcastWish(2)];
        effects.extend(self.raise_own_wish(2));
        effects
    }

    /// Records a first-hand wish, amplifies at `f + 1`, then advances entry at `2f + 1`.
    pub fn on_wish(&mut self, sender: PublicKey, x: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        let Some(&idx) = self.index_of.get(&sender) else {
            return effects;
        };
        if x > self.omega[idx] {
            self.omega[idx] = x;
        }

        let omega_plus = self.kth_largest(self.f_plus_1_parties);
        if omega_plus > self.own_watermark {
            effects.push(Effect::BroadcastWish(omega_plus));
            self.omega[self.own_index] = omega_plus;
            self.own_watermark = omega_plus;
        }
        let omega_q = self.kth_largest(self.two_f_plus_1_parties);
        effects.extend(self.advance_entry_target(omega_q));
        effects
    }

    pub fn raise_own_wish(&mut self, x: View) -> Vec<Effect> {
        if x <= self.own_watermark {
            return Vec::new();
        }
        self.omega[self.own_index] = x;
        self.own_watermark = x;
        let omega_q = self.kth_largest(self.two_f_plus_1_parties);
        self.advance_entry_target(omega_q)
    }

    /// Advances local entry bookkeeping to `next_live` after verified state installation.
    pub fn fast_forward_installed_entry(&mut self, next_live: View) {
        if next_live > self.entry_target {
            self.entry_target = next_live;
        }
        if next_live > self.largest_entered_view {
            self.largest_entered_view = next_live;
        }
        if next_live > self.own_watermark {
            self.omega[self.own_index] = next_live;
            self.own_watermark = next_live;
        }
    }

    fn advance_entry_target(&mut self, omega_q: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        if omega_q > self.entry_target {
            self.entry_target = omega_q;
            while self.largest_entered_view < self.entry_target {
                self.largest_entered_view += 1;
                effects.push(Effect::Enter(self.largest_entered_view));
            }
        }
        effects
    }

    /// Returns the one-based `k`th largest wish.
    fn kth_largest(&self, k: usize) -> View {
        let mut values = self.omega.clone();
        let (_, value, _) = values.select_nth_unstable_by(k - 1, |a, b| b.cmp(a));
        *value
    }
}
