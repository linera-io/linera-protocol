// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, CollectionView, LogView, MapView, RegisterView, RootView, ViewStorageContext};

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct GraphQlQueriesState {
    pub reg: RegisterView<u64>,
    pub map_s: MapView<String, u8>,
    pub coll_s: CollectionView<String, RegisterView<u8>>,
    pub coll_log: CollectionView<Vec<String>, LogView<u16>>,
    pub coll_map: CollectionView<String, MapView<String, u64>>,
}
