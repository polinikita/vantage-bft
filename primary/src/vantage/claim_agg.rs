//! Off-core claim crediting.
//!
//! Counting echo-carried availability claims is the consensus core's single largest
//! cost: measured at 78% of effect execution, growing with the square of the committee,
//! and independent of offered load. The counting itself is commutative bookkeeping with
//! no protocol decision inside it, so it runs here, on its own task, against the shared
//! `Avail` state.
//!
//! The core keeps everything that needs its own state: it validates the claim carrier
//! (first echo per view, formed proposal, membership) before anything reaches this task,
//! and it applies the results — register refreshes and availability crediting — when the
//! event returns. In between, this task owns the expensive part.
//!
//! Ordering is preserved by construction: one command channel, processed in order,
//! against state behind one lock shared with the synchronous readers. The only semantic
//! difference from in-line crediting is latency — credits apply one channel hop later.
//! A generation stamp closes the checkpoint-reset race: events computed against state
//! from before an own-lane reset carry a stale generation and are dropped on receipt.

use crate::vantage::avail::{AvailResolver, ClaimCredits};
use crate::vantage::claim::ClaimRef;
use crypto::PublicKey;
use metrics::{Metrics, UtilizationTimer};
use parking_lot::Mutex;
use prometheus::IntCounter;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Work for the aggregator, in core order.
pub(crate) enum ClaimCmd {
    /// A validated claim carrier: credit `statements` from `sender`.
    Claims {
        generation: u64,
        sender: PublicKey,
        statements: Vec<ClaimRef>,
    },
}

/// The result of crediting one carrier, returned to the core.
pub(crate) struct ClaimEvent {
    pub generation: u64,
    pub sender: PublicKey,
    pub credits: ClaimCredits,
}

/// Runs claim crediting until the command channel closes.
///
/// Events go back unbounded: they are small, their rate is bounded by the bounded
/// command channel, and a bounded return path could deadlock against a core that is
/// itself blocked sending commands.
pub(crate) fn spawn(
    avail: Arc<Mutex<AvailResolver>>,
    mut rx: mpsc::Receiver<ClaimCmd>,
    tx: mpsc::UnboundedSender<ClaimEvent>,
    metrics: Option<Arc<Metrics>>,
) {
    tokio::spawn(async move {
        // Cached: the label lookup would otherwise run once per carrier.
        let mut timer_cache: Option<IntCounter> = None;
        while let Some(cmd) = rx.recv().await {
            match cmd {
                ClaimCmd::Claims {
                    generation,
                    sender,
                    statements,
                } => {
                    let timer = metrics.as_ref().map(|metrics| {
                        let counter = timer_cache
                            .get_or_insert_with(|| {
                                metrics
                                    .utilization_timer
                                    .with_label_values(&["avail_claims"])
                            })
                            .clone();
                        UtilizationTimer::from_counter(counter)
                    });
                    let credits = avail.lock().note_claim(sender, &statements);
                    drop(timer);
                    if tx
                        .send(ClaimEvent {
                            generation,
                            sender,
                            credits,
                        })
                        .is_err()
                    {
                        // The core is gone; nothing left to credit for.
                        return;
                    }
                }
            }
        }
    });
}
