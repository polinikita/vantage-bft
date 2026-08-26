// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Timeout, Vote, QC, TC};
use config::{Committee, Stake};
use crypto::consensus_auth::ConsensusSignature;
use crypto::{Digest, PublicKey, Signature};
use std::collections::HashSet;

/// Aggregates votes for a particular header into a certificate.
pub struct VotesAggregator {
    dissemination_weight: Stake,
    pub votes: Vec<(PublicKey, Signature)>,
    used: HashSet<PublicKey>,
    diss_cert: Option<Certificate>,

    /// Whether the certificate has reached quorum.
    pub complete: bool,
    /// Whether the completed certificate has been returned.
    get_once: bool,
}

impl VotesAggregator {
    pub fn new() -> Self {
        Self {
            dissemination_weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
            diss_cert: None,
            complete: false,
            get_once: true,
        }
    }

    pub fn append(
        &mut self,
        vote: Vote,
        committee: &Committee,
        _header: &Header,
    ) -> DagResult<(bool, bool)> {
        if self.complete {
            return Ok((true, false));
        }
        let author = vote.author;
        ensure!(
            committee.stake(&author) > 0,
            DagError::UnknownAuthority(author)
        );
        // Count each authority once.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push((author, vote.signature));
        self.dissemination_weight += committee.stake(&author);

        if self.dissemination_weight >= committee.validity_threshold() {
            if self.diss_cert.is_none() {
                let dissemination_cert: Certificate = Certificate {
                    author: vote.origin,
                    header_digest: vote.id,
                    height: vote.height,
                    votes: self.votes.clone(),
                    ..Default::default()
                };

                self.diss_cert = Some(dissemination_cert);
            }
            self.complete = true;
            return Ok((true, true));
        }
        Ok((false, false))
    }

    pub fn get(&mut self) -> DagResult<Option<Certificate>> {
        // A passenger QC can complete before its ambassador car reaches the
        // f+1 availability threshold. Looking for the PoA at that point must
        // not consume the one-shot delivery; consume it only once a
        // certificate actually exists.
        if self.get_once && self.diss_cert.is_some() {
            self.get_once = false;
            Ok(self.diss_cert.clone())
        } else {
            Ok(None)
        }
    }
}

/// Aggregates consensus votes and checks quorum thresholds.
pub struct QCMaker {
    weight: Stake,
    pub votes: Vec<(PublicKey, ConsensusSignature)>,
    used: HashSet<PublicKey>,

    pub try_fast: bool,
    qc_dig: Digest,
    /// Whether the slow quorum was reached for the first time.
    first: bool,
    /// Whether the fast quorum completed.
    completed_fast: bool,
}

impl QCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
            try_fast: false,
            qc_dig: Digest::default(),
            first: true,
            completed_fast: false,
        }
    }

    pub fn append(
        &mut self,
        author: PublicKey,
        vote: (Digest, ConsensusSignature),
        committee: &Committee,
    ) -> DagResult<(bool, Option<QC>)> {
        let voting_rights = committee.stake(&author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(author));
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push((author, vote.1));
        self.weight += voting_rights;

        if self.try_fast {
            return self.check_fast_qc(vote.0, committee);
        }
        // Slow path.
        if self.weight >= committee.quorum_threshold() {
            // Emit the QC once.
            self.weight = 0;
            return Ok((
                true,
                Some(QC {
                    id: vote.0,
                    votes: self.votes.clone(),
                    ..Default::default()
                }),
            ));
        }

        Ok((false, None))
    }

    pub fn check_fast_qc(
        &mut self,
        vote_dig: Digest,
        committee: &Committee,
    ) -> DagResult<(bool, Option<QC>)> {
        if self.weight >= committee.fast_threshold() {
            // Emit the QC once.
            self.weight = 0;
            self.completed_fast = true;
            return Ok((
                true,
                Some(QC {
                    id: vote_dig,
                    votes: self.votes.clone(),
                    ..Default::default()
                }),
            ));
        } else if self.weight >= committee.quorum_threshold() {
            self.qc_dig = vote_dig;
            let first = self.first;
            self.first = false;
            return Ok((first, None));
        }

        Ok((false, None))
    }

    // Returns the slow QC after the fast-path timer expires.
    pub fn get_qc(&mut self) -> DagResult<(bool, Option<QC>)> {
        if self.completed_fast {
            return Ok((false, None));
        }
        ensure!(
            self.qc_dig != Digest::default(),
            DagError::InvalidSlowQCRequest
        );
        Ok((
            true,
            Some(QC {
                id: self.qc_dig.clone(),
                votes: self.votes.clone(),
                ..Default::default()
            }),
        ))
    }
}

pub struct TCMaker {
    weight: Stake,
    votes: Vec<Timeout>,
    used: HashSet<PublicKey>,
}

impl TCMaker {
    pub fn new() -> Self {
        Self {
            weight: 0,
            votes: Vec::new(),
            used: HashSet::new(),
        }
    }

    /// Try to append a signature to a (partial) quorum.
    pub fn append(&mut self, timeout: Timeout, committee: &Committee) -> DagResult<Option<TC>> {
        let author = timeout.author;

        // Count each authority once.
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        let slot = timeout.slot;
        let view = timeout.view;

        self.votes.push(timeout);
        self.weight += committee.stake(&author);
        if self.weight >= committee.quorum_threshold() {
            self.weight = 0;
            return Ok(Some(TC {
                slot,
                view,
                timeouts: self.votes.clone(),
            }));
        }
        Ok(None)
    }

    pub fn weight(&self) -> Stake {
        self.weight
    }
}

#[cfg(test)]
mod tests {
    use super::{TCMaker, VotesAggregator};
    use crate::messages::{Timeout, Vote};

    #[test]
    fn early_car_certificate_lookup_does_not_consume_later_poa() {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let header = crate::common::header();
        let mut maker = VotesAggregator::new();

        assert!(maker.get().unwrap().is_none());
        for (author, _) in keys.iter().take(committee.validity_threshold() as usize) {
            let vote = Vote {
                id: header.id.clone(),
                height: header.height,
                origin: header.author,
                author: *author,
                signature: Default::default(),
                consensus_votes: Vec::new(),
            };
            maker.append(vote, &committee, &header).unwrap();
        }

        assert!(maker.get().unwrap().is_some());
        assert!(maker.get().unwrap().is_none());
    }

    #[test]
    fn timeout_weight_exposes_the_f_plus_one_mutiny_boundary() {
        let committee = crate::common::committee();
        let keys = crate::common::keys();
        let mut maker = TCMaker::new();
        let validity = committee.validity_threshold() as usize;
        let quorum = committee.quorum_threshold() as usize;

        for (author, _) in keys.iter().take(validity) {
            assert!(maker
                .append(
                    Timeout {
                        slot: 7,
                        view: 3,
                        high_qc: None,
                        high_prop: None,
                        author: *author,
                        signature: Default::default(),
                    },
                    &committee,
                )
                .unwrap()
                .is_none());
        }
        assert_eq!(maker.weight(), committee.validity_threshold());

        let mut tc = None;
        for (author, _) in keys.iter().skip(validity).take(quorum - validity) {
            tc = maker
                .append(
                    Timeout {
                        slot: 7,
                        view: 3,
                        high_qc: None,
                        high_prop: None,
                        author: *author,
                        signature: Default::default(),
                    },
                    &committee,
                )
                .unwrap();
        }
        assert_eq!(tc.unwrap().timeouts.len(), quorum);
    }
}
