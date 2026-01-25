// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, SyncRootView, ViewStorageContext};

/// The application state.
#[derive(SyncRootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct CounterState {
    pub value: RegisterView<u64>,
}
