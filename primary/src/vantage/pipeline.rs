use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::{BlockRef, ProposalOut, ResolutionEntry};
use crypto::PublicKey;
use metrics::PipelineMetrics;
use parking_lot::Mutex;
use std::collections::{BTreeMap, VecDeque};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const BLOCK_LIMIT: usize = 4096;

/// Publication instants are retained across every lane in the process, so this bound
/// is a whole-committee one rather than the per-lane `BLOCK_LIMIT`.
const GLOBAL_BLOCK_LIMIT: usize = 65_536;

#[derive(Clone, Copy)]
struct BlockTiming {
    published_at: Instant,
    quorum_observed: bool,
    proposal_observed: bool,
}

#[derive(Default)]
pub struct PipelineTrace {
    header_opened_at: Option<Instant>,
    blocks: BTreeMap<Height, BlockTiming>,
    sealed_at: BTreeMap<View, Instant>,
}

impl PipelineTrace {
    pub fn note_digest(&mut self, header_is_empty: bool, now: Instant) {
        if header_is_empty {
            self.header_opened_at = Some(now);
        }
    }

    pub fn clear_header(&mut self) {
        self.header_opened_at = None;
    }

    pub fn note_publish(&mut self, header: &Header, metrics: &PipelineMetrics) {
        let Some(opened_at) = self.header_opened_at.take() else {
            return;
        };
        metrics
            .vantage_digest_to_block_publish_latency
            .observe(opened_at.elapsed());
        self.blocks.insert(
            header.height,
            BlockTiming {
                published_at: Instant::now(),
                quorum_observed: false,
                proposal_observed: false,
            },
        );
        while self.blocks.len() > BLOCK_LIMIT {
            self.blocks.pop_first();
        }
    }

    pub fn note_quorum(&mut self, reference: &BlockRef) -> Option<Duration> {
        let timing = self.blocks.get_mut(&reference.1)?;
        if timing.quorum_observed {
            return None;
        }
        timing.quorum_observed = true;
        Some(timing.published_at.elapsed())
    }

    pub fn note_proposal(
        &mut self,
        name: PublicKey,
        proposal: &ProposalOut,
        metrics: &PipelineMetrics,
    ) {
        let mut covered_height = 0;
        let mut collect = |refs: &[BlockRef]| {
            for reference in refs.iter().filter(|reference| reference.0 == name) {
                covered_height = covered_height.max(reference.1);
            }
        };
        collect(proposal.c());
        collect(proposal.t());
        for entry in proposal.entries() {
            match entry {
                ResolutionEntry::Full(_, c, t) => {
                    collect(c);
                    collect(t);
                }
                ResolutionEntry::Core(_, c, _) => collect(c),
                ResolutionEntry::Skip(_) => {}
            }
        }

        for timing in self
            .blocks
            .range_mut(..=covered_height)
            .map(|(_, timing)| timing)
        {
            if timing.proposal_observed {
                continue;
            }
            timing.proposal_observed = true;
            metrics
                .vantage_block_publish_to_proposal_latency
                .observe(timing.published_at.elapsed());
        }
    }

    pub fn note_committed(
        &mut self,
        headers: &[Header],
        name: PublicKey,
        metrics: &PipelineMetrics,
    ) {
        for header in headers.iter().filter(|header| header.author == name) {
            if let Some(timing) = self.blocks.remove(&header.height) {
                metrics
                    .vantage_block_publish_to_commit_latency
                    .observe(timing.published_at.elapsed());
            }
        }
    }

    pub fn note_sealed(&mut self, view: View) {
        self.sealed_at.entry(view).or_insert_with(Instant::now);
    }

    pub fn note_finalized(&mut self, view: View) -> Option<Duration> {
        self.sealed_at
            .remove(&view)
            .map(|started| started.elapsed())
    }

    pub fn gc_below(&mut self, view: View) {
        self.sealed_at.retain(|candidate, _| *candidate >= view);
    }
}

/// Publication instants of lane blocks, keyed by lane, shared by every validator
/// running in the current process.
///
/// The per-validator [`PipelineTrace`] can only time an author's own blocks against
/// its own proposals. This trace instead pairs an author's publication with the
/// proposal send of whichever validator first names that block, which is a
/// cross-validator measurement and therefore sound only while all of the validators
/// involved share one clock, i.e. in the single-process local benchmark.
#[derive(Default)]
struct GlobalTipNamingTrace {
    published_at: BTreeMap<(PublicKey, Height), Instant>,
    /// Insertion order of the keys above, used to bound the trace.
    inserted: VecDeque<(PublicKey, Height)>,
}

impl GlobalTipNamingTrace {
    /// Removes the entries of `author`'s lane at or below `height`, returning the
    /// publication instant of each.
    fn take_prefix(&mut self, author: PublicKey, height: Height) -> Vec<Instant> {
        let covered: Vec<_> = self
            .published_at
            .range((author, Height::MIN)..=(author, height))
            .map(|(key, _)| *key)
            .collect();
        covered
            .into_iter()
            .filter_map(|key| self.published_at.remove(&key))
            .collect()
    }
}

fn global_tip_naming() -> &'static Mutex<GlobalTipNamingTrace> {
    static TRACE: OnceLock<Mutex<GlobalTipNamingTrace>> = OnceLock::new();
    TRACE.get_or_init(|| Mutex::new(GlobalTipNamingTrace::default()))
}

/// Records that `author` published the block at `height`.
pub fn note_publish_global(author: PublicKey, height: Height) {
    let mut trace = global_tip_naming().lock();
    let key = (author, height);
    if trace.published_at.insert(key, Instant::now()).is_none() {
        trace.inserted.push_back(key);
    }
    // `published_at` never holds a key absent from `inserted`, so bounding the queue
    // bounds the map as well.
    while trace.inserted.len() > GLOBAL_BLOCK_LIMIT {
        let Some(oldest) = trace.inserted.pop_front() else {
            break;
        };
        trace.published_at.remove(&oldest);
    }
}

/// Observes, for every block whose lane prefix this proposal is the first to name in
/// `T`, the delay from its publication to this proposal's send.
pub fn note_tip_naming_global(proposal: &ProposalOut, metrics: &PipelineMetrics) {
    let mut trace = global_tip_naming().lock();
    for (author, height, _) in proposal.t() {
        for published_at in trace.take_prefix(*author, *height) {
            metrics
                .vantage_block_publish_to_tip_naming_latency
                .observe(published_at.elapsed());
        }
    }
    // A proposal's first coverage of a block is always through a tip entry, so a core
    // entry above a block still waiting here means this process never saw that block
    // named as a tip. Drop it unobserved instead of charging it a later naming.
    for (author, height, _) in proposal.c() {
        trace.take_prefix(*author, *height);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vantage::block::BlockRef;
    use crypto::Digest;
    use prometheus::Registry;
    use std::thread::sleep;

    fn reference(author: PublicKey, height: Height, byte: u8) -> BlockRef {
        (author, height, Digest([byte; 32]))
    }

    #[test]
    fn proposal_tip_covers_earlier_own_blocks_once() {
        let registry = Registry::new();
        let (metrics, reporter) = metrics::Metrics::new(&registry);
        let author = PublicKey::default();
        let mut trace = PipelineTrace::default();
        for height in 1..=3 {
            trace.blocks.insert(
                height,
                BlockTiming {
                    published_at: Instant::now(),
                    quorum_observed: false,
                    proposal_observed: false,
                },
            );
        }
        let proposal = ProposalOut::Single(crate::vantage::ViewProposal {
            view: 1,
            c: vec![reference(author, 3, 3)],
            t: Vec::new(),
            m: None,
        });

        trace.note_proposal(author, &proposal, &metrics.pipeline);
        trace.note_proposal(author, &proposal, &metrics.pipeline);
        reporter.force_report();

        let snapshot =
            metrics::read_duration_snapshot(&registry, "vantage_block_publish_to_proposal_latency")
                .unwrap();
        assert_eq!(snapshot.count, 3);
    }

    #[test]
    fn global_tip_naming_observes_each_published_block_once() {
        let registry = Registry::new();
        let (metrics, reporter) = metrics::Metrics::new(&registry);
        // A lane no other test publishes into keeps the process-wide trace isolated.
        let author = PublicKey([7; 32]);
        note_publish_global(author, 1);
        note_publish_global(author, 2);
        let proposal = ProposalOut::Single(crate::vantage::ViewProposal {
            view: 1,
            c: Vec::new(),
            t: vec![reference(author, 2, 2)],
            m: None,
        });

        note_tip_naming_global(&proposal, &metrics.pipeline);
        note_tip_naming_global(&proposal, &metrics.pipeline);
        reporter.force_report();

        let snapshot = metrics::read_duration_snapshot(
            &registry,
            "vantage_block_publish_to_tip_naming_latency",
        )
        .unwrap();
        assert_eq!(snapshot.count, 2);
    }

    #[test]
    fn quorum_and_commit_are_one_shot() {
        let registry = Registry::new();
        let (metrics, reporter) = metrics::Metrics::new(&registry);
        let author = PublicKey::default();
        let mut trace = PipelineTrace::default();
        trace.blocks.insert(
            1,
            BlockTiming {
                published_at: Instant::now(),
                quorum_observed: false,
                proposal_observed: false,
            },
        );
        let r = reference(author, 1, 1);
        assert!(trace.note_quorum(&r).is_some());
        assert!(trace.note_quorum(&r).is_none());

        sleep(Duration::from_millis(1));
        let header = Header {
            author,
            height: 1,
            ..Header::default()
        };
        trace.note_committed(std::slice::from_ref(&header), author, &metrics.pipeline);
        trace.note_committed(&[header], author, &metrics.pipeline);
        reporter.force_report();

        let snapshot =
            metrics::read_duration_snapshot(&registry, "vantage_block_publish_to_commit_latency")
                .unwrap();
        assert_eq!(snapshot.count, 1);
    }
}
