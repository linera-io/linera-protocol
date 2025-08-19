// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};
use crate::Stats;

/// The application state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct TrackInstantiationLoadOperationState {
    pub stats: RegisterView<Stats>,
}