// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, SyncView, ViewStorageContext};

/// The application state.
#[derive(SyncView)]
#[view(context = ViewStorageContext)]
pub struct TrackInstantiationState {
    pub stats: RegisterView<u64>,
}
