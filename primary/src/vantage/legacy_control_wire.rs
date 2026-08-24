//! Wire-only payload retained for the retired control-log variants.
//!
//! Existing `PrimaryMessage` variant indices are part of the bincode wire
//! layout. The runtime counts and drops these messages; it does not instantiate
//! the old control state machine.

use crate::primary::View;
use crypto::Digest;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LegacyControlProposal {
    pub round: u64,
    pub parent: u64,
    pub value: Option<(View, Digest)>,
}
