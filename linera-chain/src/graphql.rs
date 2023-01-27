use crate::ChainStateView;
use async_graphql::Object;
use linera_views::common::Context;

#[Object]
impl<C: Sync + Send + Context> ChainStateView<C> {
    async fn confirmed_log(&self) -> usize {
        self.confirmed_log.count()
    }

    async fn received_log(&self) -> usize {
        self.received_log.count()
    }
}
