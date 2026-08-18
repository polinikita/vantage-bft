use crate::primary::Height;
use crate::vantage::BlockRef;
use config::{Committee, Stake};
use crypto::{Digest, PublicKey};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Committee positions carried by one inline bitmap.
///
/// One-byte wire identifiers cap a wire-compatible committee at this many keys; a larger
/// committee keeps its remaining positions in a list rather than losing them.
const INLINE_MEMBERS: usize = 256;

/// Committee positions in `Committee::index_of` order.
///
/// `Committee::authorities` is a `BTreeMap`, so its key order is the index order and a binary
/// search resolves the same position without hashing a key.  Every slot handed to a container
/// must come from the index that sized it.
pub(crate) struct CommitteeIndex {
    keys: Vec<PublicKey>,
    stakes: Vec<Stake>,
    quorum: Stake,
    validity: Stake,
}

impl CommitteeIndex {
    pub(crate) fn new(committee: &Committee) -> Arc<Self> {
        Arc::new(Self {
            keys: committee.authorities.keys().copied().collect(),
            stakes: committee.authorities.values().map(|a| a.stake).collect(),
            quorum: committee.quorum_threshold(),
            validity: committee.validity_threshold(),
        })
    }

    pub(crate) fn size(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn quorum_threshold(&self) -> Stake {
        self.quorum
    }

    pub(crate) fn validity_threshold(&self) -> Stake {
        self.validity
    }

    /// Resolves one key; every probe downstream indexes with the result.
    pub(crate) fn slot(&self, key: &PublicKey) -> Slot {
        Slot {
            key: *key,
            index: self.keys.binary_search(key).ok(),
        }
    }

    pub(crate) fn at(&self, index: usize) -> Slot {
        Slot {
            key: self.keys[index],
            index: Some(index),
        }
    }

    pub(crate) fn is_member(&self, key: &PublicKey) -> bool {
        self.keys.binary_search(key).is_ok()
    }

    /// Stake of a resolved slot; a key outside the committee carries none.
    pub(crate) fn stake_of(&self, slot: &Slot) -> Stake {
        slot.index.map_or(0, |index| self.stakes[index])
    }

    pub(crate) fn stake_at(&self, index: usize) -> Stake {
        self.stakes[index]
    }
}

/// A public key with its committee position, when the committee has one.
///
/// Nothing upstream of these structures rejects a reference authored by a non-member, so the
/// unresolved case keeps exactly the behaviour a public-key-keyed map had.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Slot {
    key: PublicKey,
    index: Option<usize>,
}

impl Slot {
    pub(crate) fn key(&self) -> PublicKey {
        self.key
    }

    pub(crate) fn index(&self) -> Option<usize> {
        self.index
    }
}

/// Committee positions as a bitmap.
#[derive(Clone, Default)]
pub(crate) struct MemberSet {
    words: [u64; INLINE_MEMBERS / 64],
    /// Positions of a committee wider than the inline bitmap.
    overflow: Vec<usize>,
}

impl MemberSet {
    /// Returns true only when this call adds `index`.
    pub(crate) fn insert(&mut self, index: usize) -> bool {
        let Some((word, bit)) = Self::bit(index) else {
            if self.overflow.contains(&index) {
                return false;
            }
            self.overflow.push(index);
            return true;
        };
        let added = self.words[word] & bit == 0;
        self.words[word] |= bit;
        added
    }

    /// Returns true only when this call removes `index`.
    pub(crate) fn remove(&mut self, index: usize) -> bool {
        let Some((word, bit)) = Self::bit(index) else {
            let Some(at) = self.overflow.iter().position(|held| *held == index) else {
                return false;
            };
            self.overflow.remove(at);
            return true;
        };
        let held = self.words[word] & bit != 0;
        self.words[word] &= !bit;
        held
    }

    pub(crate) fn contains(&self, index: usize) -> bool {
        match Self::bit(index) {
            Some((word, bit)) => self.words[word] & bit != 0,
            None => self.overflow.contains(&index),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let inline = self.words.iter().enumerate().flat_map(|(word, bits)| {
            let mut rest = *bits;
            std::iter::from_fn(move || {
                (rest != 0).then(|| {
                    let bit = rest.trailing_zeros() as usize;
                    rest &= rest - 1;
                    word * 64 + bit
                })
            })
        });
        inline.chain(self.overflow.iter().copied())
    }

    fn bit(index: usize) -> Option<(usize, u64)> {
        (index < INLINE_MEMBERS).then(|| (index / 64, 1u64 << (index % 64)))
    }
}

/// A set of keys, dense over committee positions and listed outside them.
#[derive(Default)]
pub(crate) struct SlotSet {
    members: MemberSet,
    outside: Vec<PublicKey>,
}

impl SlotSet {
    pub(crate) fn insert(&mut self, slot: &Slot) -> bool {
        match slot.index() {
            Some(index) => self.members.insert(index),
            None => {
                if self.outside.contains(&slot.key()) {
                    return false;
                }
                self.outside.push(slot.key());
                true
            }
        }
    }

    pub(crate) fn remove(&mut self, slot: &Slot) -> bool {
        match slot.index() {
            Some(index) => self.members.remove(index),
            None => {
                let Some(at) = self.outside.iter().position(|held| *held == slot.key()) else {
                    return false;
                };
                self.outside.remove(at);
                true
            }
        }
    }

    pub(crate) fn iter<'a>(&'a self, index: &'a CommitteeIndex) -> impl Iterator<Item = Slot> + 'a {
        self.members
            .iter()
            .map(|position| index.at(position))
            .chain(self.outside.iter().map(|key| index.slot(key)))
    }
}

/// Per-author state indexed by committee position.
pub(crate) struct ByAuthor<T> {
    index: Arc<CommitteeIndex>,
    dense: Vec<T>,
    /// Authors the committee has no position for.
    outside: HashMap<PublicKey, T>,
}

impl<T: Default> ByAuthor<T> {
    pub(crate) fn new(index: Arc<CommitteeIndex>) -> Self {
        let dense = std::iter::repeat_with(T::default)
            .take(index.size())
            .collect();
        Self {
            index,
            dense,
            outside: HashMap::new(),
        }
    }

    /// Yields `(author, value)` for every author holding state.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (Slot, &T)> + '_ {
        let dense = self
            .dense
            .iter()
            .enumerate()
            .map(|(author, value)| (self.index.at(author), value));
        let outside = self
            .outside
            .iter()
            .map(|(author, value)| (self.index.slot(author), value));
        dense.chain(outside)
    }

    pub(crate) fn get(&self, author: &Slot) -> Option<&T> {
        match author.index() {
            Some(index) => Some(&self.dense[index]),
            None => self.outside.get(&author.key()),
        }
    }

    /// Returns the author's state, creating it for an author outside the committee.
    pub(crate) fn entry(&mut self, author: &Slot) -> &mut T {
        match author.index() {
            Some(index) => &mut self.dense[index],
            None => self.outside.entry(author.key()).or_default(),
        }
    }

    /// Drops one author's state.
    pub(crate) fn clear(&mut self, author: &Slot) {
        match author.index() {
            Some(index) => self.dense[index] = T::default(),
            None => {
                self.outside.remove(&author.key());
            }
        }
    }
}

/// Per-`(sender, author)` state in author-major rows.
///
/// Author-major because every bulk operation over these tables -- a lane reset, the retry
/// after one author's block arrives -- covers one author and every sender.
pub(crate) struct ByPair<T> {
    index: Arc<CommitteeIndex>,
    /// One row per author, sized on first use.
    rows: Vec<Vec<Option<T>>>,
    /// Pairs naming a key the committee has no position for, keyed `(sender, author)`.
    outside: HashMap<(PublicKey, PublicKey), T>,
}

impl<T> ByPair<T> {
    pub(crate) fn new(index: Arc<CommitteeIndex>) -> Self {
        let rows = std::iter::repeat_with(Vec::new)
            .take(index.size())
            .collect();
        Self {
            index,
            rows,
            outside: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, sender: &Slot, author: &Slot) -> Option<&T> {
        match (sender.index(), author.index()) {
            (Some(sender), Some(author)) => self.rows[author].get(sender)?.as_ref(),
            _ => self.outside.get(&(sender.key(), author.key())),
        }
    }

    pub(crate) fn insert(&mut self, sender: &Slot, author: &Slot, value: T) {
        match (sender.index(), author.index()) {
            (Some(sender), Some(author)) => {
                let width = self.index.size();
                let row = &mut self.rows[author];
                if row.is_empty() {
                    row.resize_with(width, || None);
                }
                row[sender] = Some(value);
            }
            _ => {
                self.outside.insert((sender.key(), author.key()), value);
            }
        }
    }

    pub(crate) fn remove(&mut self, sender: &Slot, author: &Slot) {
        match (sender.index(), author.index()) {
            (Some(sender), Some(author)) => {
                if let Some(held) = self.rows[author].get_mut(sender) {
                    *held = None;
                }
            }
            _ => {
                self.outside.remove(&(sender.key(), author.key()));
            }
        }
    }

    /// Drops every pair naming `author`, including senders outside the committee.
    pub(crate) fn clear_author(&mut self, author: &Slot) {
        if let Some(index) = author.index() {
            self.rows[index].clear();
        }
        let key = author.key();
        self.outside.retain(|(_, held), _| *held != key);
    }

    /// Yields `(sender, value)` for one author.
    pub(crate) fn row<'a>(&'a self, author: &'a Slot) -> impl Iterator<Item = (Slot, &'a T)> + 'a {
        let dense = author.index().into_iter().flat_map(move |author| {
            self.rows[author]
                .iter()
                .enumerate()
                .filter_map(move |(sender, held)| {
                    held.as_ref().map(|value| (self.index.at(sender), value))
                })
        });
        let key = author.key();
        let outside = self
            .outside
            .iter()
            .filter(move |((_, held), _)| *held == key)
            .map(|((sender, _), value)| (self.index.slot(sender), value));
        dense.chain(outside)
    }

    #[cfg(test)]
    pub(crate) fn pairs(&self) -> Vec<(PublicKey, PublicKey)> {
        let dense = self.rows.iter().enumerate().flat_map(|(author, row)| {
            row.iter()
                .enumerate()
                .filter(|(_, held)| held.is_some())
                .map(move |(sender, _)| (self.index.at(sender).key(), self.index.at(author).key()))
        });
        dense.chain(self.outside.keys().copied()).collect()
    }
}

/// Per-block-reference state addressed by `(author position, height)`.
pub(crate) struct ByRef<T> {
    lanes: Vec<BTreeMap<Height, RefSlot<T>>>,
    /// References authored by a key the committee has no position for.
    outside: HashMap<BlockRef, T>,
}

/// One `(author, height)` coordinate.
///
/// A second digest at one coordinate is equivocation: adversarially rare, so the extra
/// digests are scanned linearly instead of widening every key with a digest.
struct RefSlot<T> {
    head: Digest,
    value: T,
    forks: Vec<(Digest, T)>,
}

impl<T> RefSlot<T> {
    fn get(&self, digest: &Digest) -> Option<&T> {
        if self.head == *digest {
            return Some(&self.value);
        }
        self.forks
            .iter()
            .find(|(held, _)| held == digest)
            .map(|(_, value)| value)
    }

    fn get_mut(&mut self, digest: &Digest) -> Option<&mut T> {
        if self.head == *digest {
            return Some(&mut self.value);
        }
        self.forks
            .iter_mut()
            .find(|(held, _)| held == digest)
            .map(|(_, value)| value)
    }
}

impl<T: Default> ByRef<T> {
    pub(crate) fn new(index: &CommitteeIndex) -> Self {
        Self {
            lanes: std::iter::repeat_with(BTreeMap::new)
                .take(index.size())
                .collect(),
            outside: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, author: &Slot, height: Height, digest: &Digest) -> Option<&T> {
        match author.index() {
            Some(index) => self.lanes[index].get(&height)?.get(digest),
            None => self.outside.get(&(author.key(), height, digest.clone())),
        }
    }

    pub(crate) fn get_mut(
        &mut self,
        author: &Slot,
        height: Height,
        digest: &Digest,
    ) -> Option<&mut T> {
        match author.index() {
            Some(index) => self.lanes[index].get_mut(&height)?.get_mut(digest),
            None => self
                .outside
                .get_mut(&(author.key(), height, digest.clone())),
        }
    }

    /// Returns the coordinate's state and whether this call created it.
    pub(crate) fn entry(
        &mut self,
        author: &Slot,
        height: Height,
        digest: &Digest,
    ) -> (&mut T, bool) {
        let Some(index) = author.index() else {
            let key = (author.key(), height, digest.clone());
            let mut created = false;
            let value = self.outside.entry(key).or_insert_with(|| {
                created = true;
                T::default()
            });
            return (value, created);
        };
        match self.lanes[index].entry(height) {
            Entry::Vacant(vacant) => {
                let slot = vacant.insert(RefSlot {
                    head: digest.clone(),
                    value: T::default(),
                    forks: Vec::new(),
                });
                (&mut slot.value, true)
            }
            Entry::Occupied(occupied) => {
                let slot = occupied.into_mut();
                if slot.head == *digest {
                    return (&mut slot.value, false);
                }
                match slot.forks.iter().position(|(held, _)| held == digest) {
                    Some(at) => (&mut slot.forks[at].1, false),
                    None => {
                        slot.forks.push((digest.clone(), T::default()));
                        let at = slot.forks.len() - 1;
                        (&mut slot.forks[at].1, true)
                    }
                }
            }
        }
    }

    /// Removes and returns every value authored by `author`.
    pub(crate) fn drain_author(&mut self, author: &Slot) -> Vec<T> {
        let mut drained = Vec::new();
        match author.index() {
            Some(index) => {
                for (_, slot) in std::mem::take(&mut self.lanes[index]) {
                    drained.push(slot.value);
                    drained.extend(slot.forks.into_iter().map(|(_, value)| value));
                }
            }
            None => {
                let key = author.key();
                self.outside.retain(|(held, _, _), value| {
                    if *held != key {
                        return true;
                    }
                    drained.push(std::mem::take(value));
                    false
                });
            }
        }
        drained
    }
}
