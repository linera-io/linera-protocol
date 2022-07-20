use super::ChainGuards;
use futures::FutureExt;
use linera_base::messages::ChainId;
use std::time::Duration;
use tokio::time::sleep;

/// Test if a chain guard can be obtained again after it has been dropped.
#[tokio::test]
async fn guard_can_be_obtained_later_again() {
    let chain_id = ChainId::root(0);
    let mut guards = ChainGuards::default();
    // Obtain the guard the first time and drop it immediately
    let _ = guards.guard(chain_id).await;
    // It should be available immediately on the second time
    assert!(guards.guard(chain_id).now_or_never().is_some());
}
