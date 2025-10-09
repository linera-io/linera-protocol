// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, CollectionView, MapView, RootView, ViewStorageContext};

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct ComplexDataState {
    pub field4: CollectionView<String, MapView<String, u64>>,
}
