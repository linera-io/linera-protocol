// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use ethereum_tracker::U256Cont;
use linera_sdk::views::{
    linera_views, SyncMapView, SyncRegisterView, SyncView, ViewStorageContext,
};

/// The application state.
#[derive(SyncView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct EthereumTrackerState {
    pub ethereum_endpoint: SyncRegisterView<String>,
    pub contract_address: SyncRegisterView<String>,
    pub start_block: SyncRegisterView<u64>,
    pub accounts: SyncMapView<String, U256Cont>,
}
