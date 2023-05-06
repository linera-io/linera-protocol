// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    register_view::RegisterView,
    views::{RootView, View},
};

/// The application state.
#[derive(Debug, RootView)]
pub struct ReentrantCounter<C> {
    pub value: RegisterView<C, u128>,
}
