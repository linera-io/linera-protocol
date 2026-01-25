// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, SyncView, ViewStorageContext};

/// The application state (empty for this test fixture, but needs at least one field).
#[derive(SyncView)]
#[view(context = ViewStorageContext)]
pub struct TimeExpiryState {
    /// A dummy field since RootView requires at least one field.
    pub dummy: RegisterView<u64>,
}
