// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    views::{linera_views, RegisterView, ViewStorageContext},
    RootView,
};

/// The application state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct TrackInstantiationState {
    pub stats: RegisterView<u64>,
}
