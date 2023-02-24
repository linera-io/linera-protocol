// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{GraphQLView, RootView, View},
};

use linera_sdk::Service;

/// The application state.
#[derive(RootView, GraphQLView, Service, Debug)]
pub struct Counter<C> {
    pub value: RegisterView<C, u64>,
}
