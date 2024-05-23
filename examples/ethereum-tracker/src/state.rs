// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ethereum_tracker::{EndpointAndAddress, U256Cont};
use linera_sdk::views::{linera_views, MapView, RegisterView, RootView, ViewStorageContext};

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct EthereumTracker {
    pub argument: RegisterView<EndpointAndAddress>,
    pub start_block: RegisterView<u64>,
    pub accounts: MapView<String, U256Cont>,
}
