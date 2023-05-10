// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    log_view::LogView,
    map_view::CustomMapView,
    views::{GraphQLView, RootView},
};
use social::{Key, OwnPost};

/// The application state.
#[derive(RootView, GraphQLView, Debug)]
pub struct Social<C> {
    /// Our posts.
    pub own_posts: LogView<C, OwnPost>,
    /// Posts we received from authors we subscribed to.
    pub received_posts: CustomMapView<C, Key, String>,
}
