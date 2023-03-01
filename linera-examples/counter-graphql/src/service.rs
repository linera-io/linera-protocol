// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::Counter;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
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
        let graphql_request: async_graphql::Request =
            serde_json::from_slice(argument).map_err(|_| Error::InvalidQuery)?;
        let schema = Schema::build(self.clone(), EmptyMutation, EmptySubscription).finish();
        let res = schema.execute(graphql_request).await;
        Ok(serde_json::to_vec(&res).unwrap())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; Counter application only supports a single (empty) query.
    #[error(
        "Invalid query argument; Counter application only supports JSON encoded GraphQL queries"
    )]
    InvalidQuery,
}

#[cfg(test)]
mod tests {
    use super::{Counter, Error};
    use async_graphql::{Request, Response};
    use futures::FutureExt;
    use linera_sdk::{ChainId, QueryContext, Service};
    use linera_views::{memory::make_test_context, views::View};
    use std::sync::Arc;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let req = Request::new("{ data }");
        let req_bytes = serde_json::to_vec(&req).unwrap();
        let value = 61_098_721_u64;
        let context = make_test_context()
            .now_or_never()
            .expect("Failed to acquire the guard");
        let mut counter = Counter::load(context)
            .now_or_never()
            .unwrap()
            .expect("Failed to load Counter");
        counter.value.set(value);
        let counter = Arc::new(counter);

        let result = counter
            .query_application(&dummy_query_context(), &req_bytes)
            .now_or_never()
            .expect("Query should not await anything")
            .unwrap();

        assert!(serde_json::from_slice::<Response>(&result).is_ok())
    }

    #[webassembly_test]
    fn invalid_query() {
        let value = 61_098_721_u64;
        let context = make_test_context()
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

        assert_eq!(result, Err(Error::InvalidQuery));
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 8].into()),
        }
    }
}
