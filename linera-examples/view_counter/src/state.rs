// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{ContainerView, View},
};

/// The application state.
#[derive(ContainerView, Debug)]
pub struct ViewCounter<C> {
    pub value: RegisterView<C, u128>,
}
