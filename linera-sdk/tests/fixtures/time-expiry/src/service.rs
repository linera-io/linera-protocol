// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{linera_base_types::WithServiceAbi, views::View, Service, ServiceRuntime};
use time_expiry::TimeExpiryAbi;

use self::state::TimeExpiryState;

pub struct TimeExpiryService {
    #[allow(dead_code)]
    state: TimeExpiryState,
}

linera_sdk::service!(TimeExpiryService);

impl WithServiceAbi for TimeExpiryService {
    type Abi = TimeExpiryAbi;
}

impl Service for TimeExpiryService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = TimeExpiryState::load(runtime.root_view_storage_context())
            .expect("Failed to load state");
        TimeExpiryService { state }
    }

    async fn handle_query(&self, _query: ()) {}
}
