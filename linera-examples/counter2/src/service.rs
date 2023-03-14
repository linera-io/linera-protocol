// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::Counter;
use std::sync::Arc;

use async_trait::async_trait;
use linera_sdk::{
    service::system_api::ReadableWasmContext, QueryContext, Service, ViewStateStorage,
};
use linera_views::{common::Context, views::ViewError};
use thiserror::Error;

/// TODO(#434): Remove the type alias
type ReadableCounter = Counter<ReadableWasmContext>;
linera_sdk::service!(ReadableCounter);

#[async_trait]
impl<C> Service for Counter<C>
where
    C: Context + Send + Sync + Clone + 'static,
    ViewError: From<C::Error>,
{
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let value = self.value.get();
        match argument {
            &[] => Ok(bcs::to_bytes(&value).expect("Serialization should not fail")),
            _ => Err(Error::InvalidQuery),
        }
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; Counter application only supports a single (empty) query.
    #[error("Invalid query argument; Counter application only supports a single (empty) query")]
    InvalidQuery,
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::Counter;
    use futures_util::FutureExt;
    use linera_sdk::{ChainId, QueryContext, Service};
    use linera_views::{memory::create_test_context, views::View};
    use std::sync::Arc;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let value = 61_098_721_u128;
        let context = create_test_context()
            .now_or_never()
            .expect("Failed to acquire the guard");
        let mut counter = Counter::load(context)
            .now_or_never()
            .unwrap()
            .expect("Failed to load Counter");
        counter.value.set(value);
        let counter = Arc::new(counter);
        let result = counter
            .query_application(&dummy_query_context(), &[])
            .now_or_never()
            .expect("Query should not await anything");

        let expected_response =
            bcs::to_bytes(&value).expect("Counter value could not be serialized");

        assert_eq!(result, Ok(expected_response));
    }

    #[webassembly_test]
    fn invalid_query() {
        let value = 4_u128;
        let context = create_test_context()
            .now_or_never()
            .expect("Failed to acquire the guard");
        let mut counter = Counter::load(context)
            .now_or_never()
            .unwrap()
            .expect("Failed to load Counter");
        counter.value.set(value);
        let counter = Arc::new(counter);

        let dummy_argument = [2];
        let result = counter
            .query_application(&dummy_query_context(), &dummy_argument)
            .now_or_never()
            .expect("Query should not await anything");

        assert_eq!(result, Err(Error::InvalidQuery));
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 4].into()),
        }
    }
}
