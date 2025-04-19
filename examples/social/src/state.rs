// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::{
    linera_base_types::ChainId,
    views::{linera_views, CustomMapView, LogView, MapView, RootView, ViewStorageContext},
};
use social::{Key, OwnPost, Post};

/// The application state.
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = "ViewStorageContext")]
pub struct SocialState {
    /// Our posts.
    pub own_posts: LogView<OwnPost>,
    /// Posts we received from authors we subscribed to.
    pub received_posts: CustomMapView<Key, Post>,
    /// Other chains events we have already processed.
    pub processed_events: MapView<ChainId, u32>,
}
