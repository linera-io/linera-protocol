// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::ReentrantCounter;
use async_trait::async_trait;
use linera_sdk::{base::WithServiceAbi, QueryContext, Service, ViewStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(ReentrantCounter);

impl WithServiceAbi for ReentrantCounter {
    type Abi = reentrant_counter::ReentrantCounterAbi;
}

#[async_trait]
impl Service for ReentrantCounter {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        _argument: (),
    ) -> Result<u64, Self::Error> {
        Ok(*self.value.get())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument in reentrant-counter app: could not deserialize GraphQL request.
    #[error(
        "Invalid query argument in reentrant-counter app: could not deserialize GraphQL request."
    )]
    InvalidQuery(#[from] serde_json::Error),
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use crate::ReentrantCounter;
    use futures::FutureExt;
    use linera_sdk::{base::ChainId, test, views::ViewStorageContext, QueryContext, Service};
    use linera_views::views::View;
    use std::sync::Arc;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        test::mock_key_value_store();
        let value = 61_098_721_u64;
        let mut counter = ReentrantCounter::load(ViewStorageContext::default())
            .now_or_never()
            .unwrap()
            .expect("Failed to load Counter");
        counter.value.set(value);
        let counter = Arc::new(counter);
        let result = counter
            .query_application(&dummy_query_context(), ())
            .now_or_never()
            .expect("Query should not await anything");

        assert_eq!(result.unwrap(), value);
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 4].into()),
        }
    }
}
