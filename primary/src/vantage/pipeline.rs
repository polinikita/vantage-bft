use crate::messages::Header;
use crate::primary::{Height, View};
use crate::vantage::{BlockRef, ProposalOut, ResolutionEntry};
use crypto::PublicKey;
use metrics::PipelineMetrics;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const BLOCK_LIMIT: usize = 4096;

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
