// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use linera_sdk::{base::WithServiceAbi, views::View, Service, ServiceRuntime};

use self::state::Counter;

// ANCHOR: service_struct
linera_sdk::service!(CounterService);

pub struct CounterService {
    state: Counter,
}
// ANCHOR_END: service_struct

// ANCHOR: declare_abi
impl WithServiceAbi for CounterService {
    type Abi = counter::CounterAbi;
}
// ANCHOR_END: declare_abi

impl Service for CounterService {
    type Parameters = ();

    // ANCHOR: new
    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = Counter::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CounterService { state }
    }
    // ANCHOR_END: new

    // ANCHOR: handle_query
    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            QueryRoot {
                value: *self.state.value.get(),
            },
            MutationRoot {},
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
    // ANCHOR_END: handle_query
}

// ANCHOR: mutation
struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn increment(&self, value: u64) -> Vec<u8> {
        bcs::to_bytes(&value).unwrap()
    }
}
// ANCHOR_END: mutation

// ANCHOR: query
struct QueryRoot {
    value: u64,
}

#[Object]
impl QueryRoot {
    async fn value(&self) -> &u64 {
        &self.value
    }
}
// ANCHOR_END: query

#[cfg(test)]
mod tests {
    use async_graphql::{Request, Response, Value};
    use futures::FutureExt as _;
    use linera_sdk::{util::BlockingWait, views::View, Service, ServiceRuntime};
    use serde_json::json;

    use super::{Counter, CounterService};

    #[test]
    fn query() {
        let value = 61_098_721_u64;
        let runtime = ServiceRuntime::<CounterService>::new();
        let mut state = Counter::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");
        state.value.set(value);

        let service = CounterService { state };
        let request = Request::new("{ value }");

        let response = service
            .handle_query(request)
            .now_or_never()
            .expect("Query should not await anything");

        let expected = Response::new(Value::from_json(json!({"value" : 61_098_721})).unwrap());

        assert_eq!(response, expected)
    }
}
