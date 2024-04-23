// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use linera_sdk::{base::WithServiceAbi, Service, ServiceRuntime, ViewStateStorage};
use thiserror::Error;

use self::state::Counter;

pub struct CounterService {
    state: Counter,
}

linera_sdk::service!(CounterService);

impl WithServiceAbi for CounterService {
    type Abi = counter::CounterAbi;
}

impl Service for CounterService {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;
    type State = Counter;
    type Parameters = ();

    async fn new(state: Self::State, _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(CounterService { state })
    }

    async fn handle_query(&self, request: Request) -> Result<Response, Self::Error> {
        let schema = Schema::build(
            QueryRoot {
                value: *self.state.value.get(),
            },
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
    use async_graphql::{Request, Response, Value};
    use futures::FutureExt;
    use linera_sdk::{
        test::mock_key_value_store,
        util::BlockingWait,
        views::{View, ViewStorageContext},
        Service,
    };
    use serde_json::json;
    use webassembly_test::webassembly_test;

    use super::{Counter, CounterService};

    #[webassembly_test]
    fn query() {
        mock_key_value_store();

        let value = 61_098_721_u64;
        let mut state = Counter::load(ViewStorageContext::default())
            .blocking_wait()
            .expect("Failed to read from mock key value store");
        state.value.set(value);

        let service = CounterService { state };
        let request = Request::new("{ value }");

        let result = service
            .handle_query(request)
            .now_or_never()
            .expect("Query should not await anything");

        let expected = Response::new(Value::from_json(json!({"value" : 61_098_721})).unwrap());

        assert_eq!(result.unwrap(), expected)
    }
}
