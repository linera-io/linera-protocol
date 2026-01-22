// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, SyncRegisterView, SyncView, ViewStorageContext};

/// The application state.
#[derive(SyncView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct CounterState {
    pub value: SyncRegisterView<u64>,
}
