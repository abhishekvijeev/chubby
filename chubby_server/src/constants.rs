use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const LEASE_EXTENSION: Duration = Duration::from_secs(12);
pub const NUM_NODES: u64 = 5;

#[derive(Clone, Deserialize, Serialize)]
pub enum LockMode {
    EXCLUSIVE,
    SHARED,
    FREE,
}
