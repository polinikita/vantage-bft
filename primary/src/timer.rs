use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{sleep, Duration, Instant, Sleep};

use crate::messages::{ConsensusVote, Vote};
use crate::primary::{Slot, View};

//#[cfg(test)]
//#[path = "tests/timer_tests.rs"]
//pub mod timer_tests;

pub struct Timer {
    slot: Slot,
    view: View,
    duration: u64,
    sleep: Pin<Box<Sleep>>,
}

impl Timer {
    pub fn new(slot: Slot, view: View, duration: u64) -> Self {
        let sleep = Box::pin(sleep(Duration::from_millis(duration)));
        Self {
            slot,
            view,
            duration,
            sleep,
        }
    }

    pub fn reset(&mut self) {
        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(self.duration));
    }
}

impl Future for Timer {
    type Output = (Slot, View);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => Poll::Ready((self.slot, self.view)),
            Poll::Pending => Poll::Pending,
        }
    }
}

//This timer is used for FastPath and for Cars waiting to proceed without consensus
pub struct CarTimer {
    vote: Vote,
    duration: u64,
    sleep: Pin<Box<Sleep>>,
}

impl CarTimer {
    pub fn new(vote: Vote, duration: u64) -> Self {
        let sleep = Box::pin(sleep(Duration::from_millis(duration)));
        Self {
            vote,
            duration,
            sleep,
        }
    }

    pub fn reset(&mut self) {
        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(self.duration));
    }
}

impl Future for CarTimer {
    type Output = Vote;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => Poll::Ready(self.vote.clone()),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct FastTimer {
    vote: ConsensusVote,
    duration: u64,
    sleep: Pin<Box<Sleep>>,
}

impl FastTimer {
    pub fn new(vote: ConsensusVote, duration: u64) -> Self {
        let sleep = Box::pin(sleep(Duration::from_millis(duration)));
        Self {
            vote,
            duration,
            sleep,
        }
    }

    pub fn reset(&mut self) {
        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(self.duration));
    }
}

impl Future for FastTimer {
    type Output = ConsensusVote;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(_) => Poll::Ready(self.vote.clone()),
            Poll::Pending => Poll::Pending,
        }
    }
}
