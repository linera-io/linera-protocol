// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{CustomMapView, LogView, ViewStorageContext};
use linera_views::views::{GraphQLView, RootView};
use social::{Key, OwnPost};

/// The application state.
#[derive(RootView, GraphQLView)]
#[view(context = "ViewStorageContext")]
pub struct Social {
    /// Our posts.
    pub own_posts: LogView<OwnPost>,
    /// Posts we received from authors we subscribed to.
    pub received_posts: CustomMapView<Key, String>,
}
