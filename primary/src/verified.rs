//! A digest-keyed cache of objects already verified against the static
//! committee. Verification is deterministic and idempotent, so the cache is
//! pure memoization: any eviction is sound and only costs a re-verification.
//! Negative results are never cached.
//!
//! Keys must bind the complete verified content. Certificates are keyed by
//! `evidence_digest()` (which binds the exact vote set, unlike
//! `Certificate::digest()`), cuts by `proposals_digest`, and QC-bearing
//! consensus messages by their message digest joined with the QC digest
//! (the message digest alone does not bind the QC votes). The digest domains
//! of these key families are disjoint.

use crate::error::{ConsensusResult, DagResult};
use crate::messages::{
    proposals_digest, verify_commit, verify_confirm, Certificate, ConsensusMessage, Header,
    Proposal, ProposalKind, Timeout, TC,
};
use config::Committee;
use crypto::{Blake3Hasher, Digest, Hash as _, PublicKey};
use log::debug;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// What a cached cut verdict certifies about the cut's lane entries.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum CutShape {
    /// Every lane entry is Genesis or Certified.
    AllCertified,
    /// At least one entry is an optimistic tip (with a verified parent PoA).
    HasOptimistic,
}

struct Generations {
    current: HashMap<Digest, Option<CutShape>>,
    previous: HashMap<Digest, Option<CutShape>>,
    cap: usize,
}

#[derive(Clone)]
pub struct VerifiedCache {
    inner: Arc<RwLock<Generations>>,
}

impl VerifiedCache {
    /// A comfortable default: roughly two slot periods of certificates, cuts,
    /// and consensus messages per lane.
    pub fn for_committee(committee: &Committee) -> Self {
        Self::with_capacity(committee.size().max(4) * 1024)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Generations {
                current: HashMap::new(),
                previous: HashMap::new(),
                cap: cap.max(1),
            })),
        }
    }

    fn lookup(&self, key: &Digest) -> Option<Option<CutShape>> {
        let generations = self.inner.read();
        generations
            .current
            .get(key)
            .or_else(|| generations.previous.get(key))
            .copied()
    }

    fn record(&self, key: Digest, shape: Option<CutShape>) {
        let mut generations = self.inner.write();
        if generations.current.len() >= generations.cap {
            generations.previous = std::mem::take(&mut generations.current);
        }
        generations.current.insert(key, shape);
    }

    /// Drops the older generation when the newer one is full. Called from
    /// slot-period GC so quiet periods still bound memory.
    pub fn advance_if_full(&self) {
        let mut generations = self.inner.write();
        if generations.current.len() >= generations.cap {
            generations.previous = std::mem::take(&mut generations.current);
        }
    }

    /// Verifies a PoA certificate once per unique evidence.
    pub fn check_certificate(&self, cert: &Certificate, committee: &Committee) -> DagResult<()> {
        let key = cert.evidence_digest();
        if self.lookup(&key).is_some() {
            return Ok(());
        }
        cert.verify(committee)?;
        self.record(key, None);
        Ok(())
    }

    /// Verifies a header's signature and structure once per (id, signature).
    /// The parent PoA is deliberately not covered: the header digest binds
    /// only its coordinate, so it is checked separately via
    /// [`Self::check_certificate`].
    pub fn check_header(&self, header: &Header, committee: &Committee) -> DagResult<()> {
        let mut hasher = Blake3Hasher::new();
        hasher.update(b"header-verified-v1");
        hasher.update(&header.id.0);
        match &header.signature {
            Some(signature) => hasher.update(&signature.to_bytes()),
            None => return header.verify(committee),
        };
        let key = Digest(hasher.finalize().into());
        if self.lookup(&key).is_some() {
            return Ok(());
        }
        header.verify(committee)?;
        self.record(key, None);
        Ok(())
    }

    /// The exact decision of `autobahn_cut_is_valid`, with each unique cut
    /// classified once and each unique PoA verified once.
    pub fn cut_is_valid(
        &self,
        committee: &Committee,
        allow_optimistic_tips: bool,
        proposals: &HashMap<PublicKey, Proposal>,
    ) -> bool {
        if proposals.len() != committee.size() {
            return false;
        }
        let key = proposals_digest(proposals);
        let shape = match self.lookup(&key) {
            Some(Some(shape)) => shape,
            Some(None) => unreachable!("cut keys live in their own digest domain"),
            None => {
                let mut shape = CutShape::AllCertified;
                for lane in committee.authorities.keys() {
                    let Some(proposal) = proposals.get(lane) else {
                        return false;
                    };
                    match self.check_proposal(proposal, lane, committee) {
                        Ok(ProposalKind::Genesis | ProposalKind::Certified) => {}
                        Ok(ProposalKind::Optimistic) => shape = CutShape::HasOptimistic,
                        Err(error) => {
                            debug!("invalid Autobahn cut entry for lane {}: {}", lane, error);
                            return false;
                        }
                    }
                }
                self.record(key, Some(shape));
                shape
            }
        };
        match shape {
            CutShape::AllCertified => true,
            CutShape::HasOptimistic => allow_optimistic_tips,
        }
    }

    /// `Proposal::verify` with the PoA signatures checked through the cache.
    pub fn check_proposal(
        &self,
        proposal: &Proposal,
        lane: &PublicKey,
        committee: &Committee,
    ) -> DagResult<ProposalKind> {
        let kind = proposal.classify(lane, committee)?;
        let poa = proposal
            .poa
            .as_ref()
            .expect("classified proposals carry a PoA");
        self.check_certificate(poa, committee)?;
        Ok(kind)
    }

    /// `verify_confirm` once per (message digest, QC digest).
    pub fn check_confirm(&self, message: &ConsensusMessage, committee: &Committee) -> bool {
        let ConsensusMessage::Confirm { qc, .. } = message else {
            return false;
        };
        let key = qc_bound_key(b"confirm-verified-v1", message, &qc.digest());
        if self.lookup(&key).is_some() {
            return true;
        }
        if verify_confirm(message, committee) {
            self.record(key, None);
            true
        } else {
            false
        }
    }

    /// `verify_commit` once per (message digest, QC digest).
    pub fn check_commit(&self, message: &ConsensusMessage, committee: &Committee) -> bool {
        let ConsensusMessage::Commit { qc, .. } = message else {
            return false;
        };
        let key = qc_bound_key(b"commit-verified-v1", message, &qc.digest());
        if self.lookup(&key).is_some() {
            return true;
        }
        if verify_commit(message, committee) {
            self.record(key, None);
            true
        } else {
            false
        }
    }

    /// `Timeout::verify` with the embedded Confirm checked through the cache.
    /// The timeout's own signature is cheap and re-verified per call.
    pub fn check_timeout(&self, timeout: &Timeout, committee: &Committee) -> DagResult<()> {
        timeout.verify_with(committee, &mut |message, committee| {
            self.check_confirm(message, committee)
        })
    }

    /// `TC::verify` with every embedded Confirm checked through the cache.
    #[allow(clippy::result_large_err)]
    pub fn check_tc(&self, tc: &TC, committee: &Committee) -> ConsensusResult<()> {
        tc.verify_with(committee, &mut |message, committee| {
            self.check_confirm(message, committee)
        })
    }
}

fn qc_bound_key(domain: &'static [u8], message: &ConsensusMessage, qc_digest: &Digest) -> Digest {
    let mut hasher = Blake3Hasher::new();
    hasher.update(domain);
    hasher.update(&message.digest().0);
    hasher.update(&qc_digest.0);
    Digest(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn poa_with_votes(count: usize) -> (Committee, Certificate) {
        let committee = crate::common::committee();
        let header = crate::common::header();
        let mut certificate = crate::common::certificate(&header);
        certificate.votes.truncate(count);
        (committee, certificate)
    }

    #[test]
    fn valid_certificate_is_cached_and_reused() {
        let (committee, certificate) = poa_with_votes(2);
        let cache = VerifiedCache::with_capacity(8);
        assert!(cache.check_certificate(&certificate, &committee).is_ok());
        // A second check is a pure lookup; corrupting the committee-free path
        // is not observable, so assert via the cache key instead.
        assert!(cache.lookup(&certificate.evidence_digest()).is_some());
        assert!(cache.check_certificate(&certificate, &committee).is_ok());
    }

    #[test]
    fn forged_vote_set_misses_the_cache_and_fails() {
        let (committee, certificate) = poa_with_votes(2);
        let cache = VerifiedCache::with_capacity(8);
        assert!(cache.check_certificate(&certificate, &committee).is_ok());

        // Same coordinate, different (garbage) vote set: distinct evidence
        // digest, so no cache ride-through.
        let mut forged = Certificate {
            author: certificate.author,
            header_digest: certificate.header_digest.clone(),
            height: certificate.height,
            votes: certificate.votes.clone(),
            ..Default::default()
        };
        forged.votes[0].1 = crypto::Signature::default();
        assert_ne!(forged.evidence_digest(), certificate.evidence_digest());
        assert!(cache.check_certificate(&forged, &committee).is_err());
    }

    #[test]
    fn optimistic_cut_shape_is_remembered_but_still_gated() {
        let committee = crate::common::committee();
        let cache = VerifiedCache::with_capacity(8);
        let mut cut = Header::genesis_proposals(&committee);
        let lane = *committee.authorities.keys().next().unwrap();
        let genesis_poa = Certificate::genesis_for(lane, &committee);
        let optimistic = Proposal {
            header_digest: Digest([9; 32]),
            height: 1,
            poa: Some(genesis_poa),
            ..Default::default()
        };
        cut.insert(lane, optimistic);

        // First classification populates the cache; the verdict must still
        // depend on the caller's optimistic-tip allowance.
        assert!(cache.cut_is_valid(&committee, true, &cut));
        assert!(!cache.cut_is_valid(&committee, false, &cut));
        assert!(cache.cut_is_valid(&committee, true, &cut));

        let certified = Header::genesis_proposals(&committee);
        assert!(cache.cut_is_valid(&committee, false, &certified));
        assert!(cache.cut_is_valid(&committee, true, &certified));
    }

    #[test]
    fn generations_bound_memory_and_misses_reverify() {
        let (committee, certificate) = poa_with_votes(2);
        let cache = VerifiedCache::with_capacity(1);
        assert!(cache.check_certificate(&certificate, &committee).is_ok());
        // Force two flips; the entry ages out entirely.
        cache.record(Digest([1; 32]), None);
        cache.record(Digest([2; 32]), None);
        cache.record(Digest([3; 32]), None);
        assert!(cache.lookup(&certificate.evidence_digest()).is_none());
        // A miss simply re-verifies.
        assert!(cache.check_certificate(&certificate, &committee).is_ok());
    }
}
