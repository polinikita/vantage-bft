// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Timeout, Vote, QC, TC};
use config::{Committee, Stake};
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
                };

                self.diss_cert = Some(dissemination_cert);
            }
            self.complete = true;
            return Ok((true, true));
        }
        Ok((false, false))
    }

    pub fn get(&mut self) -> DagResult<Option<Certificate>> {
        if self.get_once {
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
    pub votes: Vec<(PublicKey, Signature)>,
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
        vote: (Digest, Signature),
        committee: &Committee,
    ) -> DagResult<(bool, Option<QC>)> {
        ensure!(self.used.insert(author), DagError::AuthorityReuse(author));

        self.votes.push((author, vote.1));
        self.weight += committee.stake(&author);

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
}
