// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, SyncRegisterView, SyncView, ViewStorageContext};

/// The application state (empty for this test fixture, but needs at least one field).
#[derive(SyncView)]
#[view(context = ViewStorageContext)]
pub struct TimeExpiryState {
    /// A dummy field since RootView requires at least one field.
    pub dummy: SyncRegisterView<u64>,
}
