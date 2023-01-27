use crate::{chain::ChainTipState, ChainManager, ChainStateView};
use async_graphql::scalar;
use linera_base::crypto::CryptoHash;
use linera_execution::ExecutionStateView;
use linera_views::common::Context;

scalar!(ChainManager);

#[async_graphql::Object]
impl<C: Sync + Send + Context> ChainStateView<C> {
    async fn execution_state_view(&self) -> &ExecutionStateView<C> {
        &self.execution_state
    }

    async fn tip_state(&self) -> &ChainTipState {
        self.tip_state.get()
    }

    async fn execution_state_hash(&self) -> &Option<CryptoHash> {
        self.execution_state_hash.get()
    }

    async fn manager(&self) -> &ChainManager {
        self.manager.get()
    }

    async fn confirmed_log(&self) -> usize {
        self.confirmed_log.count()
    }

    async fn received_log(&self) -> usize {
        self.received_log.count()
    }
}
