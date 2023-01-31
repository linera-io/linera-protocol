use crate::{chain::ChainTipState, ChainManager, ChainStateView};
use async_graphql::{scalar, Error};
use linera_base::crypto::CryptoHash;
use linera_execution::ExecutionStateView;
use linera_views::{common::Context, views::ViewError};
use serde::{Deserialize, Serialize};

scalar!(ChainManager);
scalar!(Range);

#[derive(Serialize, Deserialize)]
pub struct Range {
    start: usize,
    end: usize,
}

impl From<Range> for std::ops::Range<usize> {
    fn from(range: Range) -> Self {
        std::ops::Range {
            start: range.start,
            end: range.end,
        }
    }
}

#[async_graphql::Object]
impl<C: Sync + Send + Context> ChainStateView<C>
where
    ViewError: From<C::Error>,
{
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

    async fn confirmed_log(&self, range: Option<Range>) -> Result<Vec<CryptoHash>, Error> {
        let range = range.unwrap_or(Range {
            start: 0,
            end: self.confirmed_log.count(),
        });
        Ok(self.confirmed_log.read(range.into()).await?)
    }

    async fn received_log(&self, range: Option<Range>) -> Result<Vec<CryptoHash>, Error> {
        let range = range.unwrap_or(Range {
            start: 0,
            end: self.received_log.count(),
        });
        Ok(self.received_log.read(range.into()).await?)
    }
}
