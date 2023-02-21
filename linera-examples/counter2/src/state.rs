// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{RootView, View},
};

/// The application state.
#[derive(RootView, Debug)]
pub struct Counter<C> {
    pub value: RegisterView<C, u128>,
}
