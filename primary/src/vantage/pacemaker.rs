// PHASE5-SPEC.md §1/§3 -- the WISH view-synchronization pacemaker (rules W1-W6): owns
// the per-author wish array (`omega`), the own-wish high-watermark, and the formal
// entry target. Effect-returning like every other vantage state machine (`AgbEngine`/
// `Frontier`/`Cursor`/`Repairer`/`LaneManager`) -- no network/timer I/O of its own, so
// tests can drive it without a live node (PHASE5-SPEC.md §4).

use crate::primary::View;
use crate::vantage::{Effect, Thresholds};
use config::Committee;
use crypto::PublicKey;
use std::collections::HashMap;

/// The WISH pacemaker (W1-W6). One instance per node.
pub struct Pacemaker {
    /// D5-1: `omega[j]` = the largest wish received first-hand from author `j`
    /// (including piggybacked), one slot per author -- order statistics below are
    /// computed by *party count* over this n-slot array, stake-independent (the
    /// paper's per-author construction).
    omega: Vec<View>,
    index_of: HashMap<PublicKey, usize>,
    own_index: usize,
    /// (f+1) -- the 1-based rank of `omega_plus` in `omega` sorted descending.
    f_plus_1_parties: usize,
    /// (2f+1) -- the 1-based rank of `omega_q` in `omega` sorted descending.
    two_f_plus_1_parties: usize,
    /// Own-wish high-watermark (kept equal to `omega[own_index]` at all times; a
    /// separate field only so every check against it is a plain integer compare, not a
    /// HashMap-indexed `omega` re-read).
    own_watermark: View,
    /// The current formal-entry target (W2's `omega_q`, high-water-marked).
    entry_target: View,
    /// The largest view this party has itself recorded (or scheduled, via
    /// `Effect::Enter`) formal entry for -- monotonic, strictly increasing (W5: "entry
    /// is strictly increasing locally").
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

    /// R1's early-wish trigger (paper): the current amplification statistic
    /// `omega_i^+`, i.e. the `f+1`-th largest first-hand wish across `omega`. Recomputed
    /// from `omega` on every call (not cached), so it is always current with respect to
    /// the latest `on_wish`/`raise_own_wish` update -- exactly the same statistic
    /// `on_wish` itself recomputes internally before deciding whether to amplify.
    pub fn omega_plus(&self) -> View {
        self.kth_largest(self.f_plus_1_parties)
    }

    /// Metrics-only accessor: the current `2f+1`-party wish statistic `omega_q` --
    /// exactly the value `advance_entry_target` is driven by, recomputed fresh like
    /// `omega_plus` above. Exported so the `--timeline` progress table can show WHY a
    /// node's entry target is (or is not) advancing.
    pub fn omega_q(&self) -> View {
        self.kth_largest(self.two_f_plus_1_parties)
    }

    /// PHASE7-PREP-NOTES.md Finding A: metrics-only accessor -- the largest view this
    /// party has itself formally entered (W5). Distinct from the `#[cfg(test)]`
    /// accessor below only in that production code (the 1s progress-gauge sampler)
    /// needs this outside test builds too.
    pub fn entered_view(&self) -> View {
        self.largest_entered_view
    }

    /// reconnect-replay plan §2.6: OUR OWN tracked wish for `author` -- the
    /// requester-side "hint floor" a `VantageResumeHello` we send TO `author`
    /// carries (§2.4: "The requester's `omega_of(j)` floor survives only as a
    /// hint" -- v3's authoritative floor is the AUTHOR's own `pending_low`, never
    /// this). Promoted from a `#[cfg(test)]`-only accessor (`omega_of_for_test`) to
    /// a production one -- every other read-only accessor on this type (`own_
    /// watermark`/`entry_target`/`omega_plus`/`omega_q`/`entered_view`) is already
    /// `pub fn`, so this matches the type's own established visibility convention.
    pub fn omega_of(&self, author: PublicKey) -> View {
        self.omega[self.index_of[&author]]
    }

    #[cfg(test)]
    pub(crate) fn largest_entered_view_for_test(&self) -> View {
        self.largest_entered_view
    }

    /// W1: genesis bootstrap. Called once at boot, immediately after entering view 1
    /// (the existing boot behavior, performed by the caller, outside this struct) --
    /// records that view 1's entry already happened, so this pacemaker's own "missing
    /// views through target" bookkeeping (W2) starts counting from 1 and never
    /// re-schedules it. Then raises the own wish to 2 and returns the effects needed to
    /// broadcast it: `raise_own_wish`'s own-slot update *is* W1's "self-delivery
    /// immediate" (no separate self-addressed message round-trip is needed -- the local
    /// state is updated synchronously, right here).
    pub fn genesis(&mut self) -> Vec<Effect> {
        self.largest_entered_view = 1;
        let mut effects = vec![Effect::BroadcastWish(2)];
        effects.extend(self.raise_own_wish(2));
        effects
    }

    /// W2: a first-hand `wish(x)` from `p_j` (`sender` may be our own key -- a
    /// standalone `VantageWish`'s self-delivery is not otherwise distinguished from any
    /// other first-hand wish). Implements the strict order: `omega[j] = max(omega[j],
    /// x)`, then amplify (against the statistics as just updated), then check the
    /// target advance (against the statistics recomputed if amplification changed our
    /// own slot too) -- the two updates are independent (target advancement never waits
    /// for `omega_q == omega_plus`). A stale wish (`x` no larger than the sender's
    /// current slot) leaves `omega` unchanged, so both statistics below are unchanged
    /// too and every check is a natural no-op (W2: "stale wishes cause no transition").
    pub fn on_wish(&mut self, sender: PublicKey, x: View) -> Vec<Effect> {
        let mut effects = Vec::new();
        let Some(&idx) = self.index_of.get(&sender) else {
            return effects; // not a committee member -- ignore (defense in depth)
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

    /// W3 (and genesis's own initial raise): unconditionally raise our own wish to at
    /// least `x` (a no-op if `x` is not actually larger than the current watermark).
    /// Never itself broadcasts a standalone `VantageWish` -- W3's raised watermark rides
    /// out on the very response effect about to be emitted instead (piggybacked, W4);
    /// genesis's own initial broadcast is pushed separately by `genesis`, before this
    /// runs. May still return `Effect::Enter` if the raise also advances `omega_q` (W2's
    /// target-advance step is independent of amplification -- our own watermark is part
    /// of the `omega` array too).
    pub fn raise_own_wish(&mut self, x: View) -> Vec<Effect> {
        if x <= self.own_watermark {
            return Vec::new();
        }
        self.omega[self.own_index] = x;
        self.own_watermark = x;
        let omega_q = self.kth_largest(self.two_f_plus_1_parties);
        self.advance_entry_target(omega_q)
    }

    /// State-sync install has already made every view below `next_live` terminal locally.
    /// Fast-forward this pacemaker's local-entry bookkeeping to that first live view
    /// without replaying one `Enter` per historical view.
    ///
    /// This is intentionally narrower than W2's quorum-driven `advance_entry_target`:
    /// state sync is a local catch-up proof, not a first-hand wish quorum. The caller must
    /// still execute formal entry for exactly `next_live` in the AGB/frontier components;
    /// this method only makes the pacemaker agree that all skipped entry effects are no
    /// longer missing and raises the local wish watermark used for subsequent piggybacks
    /// and outbox filing.
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

    /// W2's target-advance step: if `omega_q` increased past the current entry target,
    /// raise it and record formal entry to every missing view through the new target,
    /// immediately and in increasing order (one `Effect::Enter` per view, ascending).
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

    /// The `k`-th largest of the n-slot `omega` array (`k` a 1-based party-count rank,
    /// e.g. `f_plus_1_parties`/`two_f_plus_1_parties`) -- always in `1..=n` by
    /// construction (`f_plus_1_parties <= two_f_plus_1_parties <= n` for any `n >= 1`,
    /// since `n >= 3f + 1`).
    fn kth_largest(&self, k: usize) -> View {
        let mut sorted = self.omega.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a)); // descending
        sorted[k - 1]
    }
}
