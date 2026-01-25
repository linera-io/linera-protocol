// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ethereum_tracker::U256Cont;
use linera_sdk::views::{linera_views, MapView, RegisterView, SyncRootView, ViewStorageContext};

/// The application state.
#[derive(SyncRootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct EthereumTrackerState {
    pub ethereum_endpoint: RegisterView<String>,
    pub contract_address: RegisterView<String>,
    pub start_block: RegisterView<u64>,
    pub accounts: MapView<String, U256Cont>,
}
