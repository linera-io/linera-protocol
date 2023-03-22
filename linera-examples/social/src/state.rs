// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::base::Timestamp;
use linera_views::{
    common::Context,
    log_view::LogView,
    map_view::CustomMapView,
    views::{RootView, View},
};
use serde::{Deserialize, Serialize};
use social::Key;

/// The application state.
#[derive(RootView, Debug)]
pub struct Social<C> {
    /// Our posts.
    pub own_posts: LogView<C, OwnPost>,
    /// Posts we received from authors we subscribed to.
    pub received_posts: CustomMapView<C, Key, String>,
}

/// An effect of the application on one chain, to be handled on another chain.
#[derive(PartialEq, Serialize, Deserialize)]
pub enum Effect {
    /// The origin chain wants to subscribe to the target chain.
    RequestSubscribe,
    /// The origin chain wants to unsubscribe from the target chain.
    RequestUnsubscribe,
    /// The origin chain made a post, and the target chain is subscribed.
    /// This includes the most recent posts in reverse order, and the total count of posts by the
    /// sender. I.e. the indices of the posts in the `Vec` are `count - 1, count - 2, ...`.
    Posts { count: u64, posts: Vec<OwnPost> },
}

/// A post's text and timestamp, to use in contexts where author and index are known.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct OwnPost {
    /// The timestamp of the block in which the post operation was included.
    pub timestamp: Timestamp,
    /// The posted text.
    pub text: String,
}
