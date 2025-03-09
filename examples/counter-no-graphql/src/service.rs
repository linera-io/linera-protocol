// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};

use self::state::CounterState;

pub struct CounterService {
    state: CounterState,
}

linera_sdk::service!(CounterService);

impl WithServiceAbi for CounterService {
    type Abi = counter_no_graphql::CounterNoGraphQlAbi;
}

impl Service for CounterService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = CounterState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        CounterService {
            state,
        }
    }

    async fn handle_query(&self, _request: ()) -> u64 {
        *self.state.value.get()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::FutureExt as _;
    use linera_sdk::{util::BlockingWait, views::View, Service, ServiceRuntime};
    use serde_json::json;

    use super::{CounterService, CounterState};

    #[test]
    fn query() {
        let value = 61_098_721_u64;
        let runtime = Arc::new(ServiceRuntime::<CounterService>::new());
        let mut state = CounterState::load(runtime.root_view_storage_context())
            .blocking_wait()
            .expect("Failed to read from mock key value store");
        state.value.set(value);

        let service = CounterService { state, runtime };
        let request = ();

        let response = service
            .handle_query(request)
            .now_or_never()
            .expect("Query should not await anything");

        assert_eq!(response, value)
    }
}
