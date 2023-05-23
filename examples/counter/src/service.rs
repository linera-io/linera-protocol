// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Counter;
use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use async_trait::async_trait;
use linera_sdk::{base::WithServiceAbi, QueryContext, Service, SimpleStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(Counter);

impl WithServiceAbi for Counter {
    type Abi = counter::CounterAbi;
}

#[async_trait]
impl Service for Counter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let schema = Schema::build(
            QueryRoot { value: self.value },
            MutationRoot {},
            EmptySubscription,
        )
        .finish();
        Ok(schema.execute(request).await)
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn increment(&self, value: u64) -> Vec<u8> {
        bcs::to_bytes(&value).unwrap()
    }
}

struct QueryRoot {
    value: u64,
}

#[Object]
impl QueryRoot {
    async fn value(&self) -> &u64 {
        &self.value
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument; could not deserialize GraphQL request.
    #[error("Invalid query argument; could not deserialize GraphQL request")]
    InvalidQuery(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::Counter;
    use async_graphql::{Request, Response, Value};
    use futures::FutureExt;
    use linera_sdk::{base::ChainId, QueryContext, Service};
    use serde_json::json;
    use std::sync::Arc;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let value = 61_098_721_u64;
        let counter = Arc::new(Counter { value });
        let request = Request::new("{ value }");

        let result = counter
            .query_application(&dummy_query_context(), request)
            .now_or_never()
            .expect("Query should not await anything");

        let expected = Response::new(Value::from_json(json!({"value" : 61_098_721})).unwrap());

        assert_eq!(result.unwrap(), expected)
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 4].into()),
        }
    }
}
