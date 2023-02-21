// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{GraphQLView, RootView, View},
};

/// The application state.
#[derive(RootView, GraphQLView, Debug)]
pub struct Counter<C> {
    pub value: RegisterView<C, u64>,
}
