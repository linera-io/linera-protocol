// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::InputObject;
use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{GraphQLView, RootView, View},
};
use serde::{Deserialize, Serialize};

/// The application state.
#[derive(RootView, GraphQLView, Debug)]
pub struct Counter<C> {
    pub value: RegisterView<C, u64>,
}

#[derive(Default, Serialize, Deserialize, Debug, InputObject)]
pub struct CounterOperation {
    pub increment: u64,
}
