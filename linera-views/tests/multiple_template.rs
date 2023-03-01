// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_views::{
    common::Context,
    memory::get_memory_context,
    register_view::RegisterView,
    views::{HashableView, View},
};
pub use linera_views_derive::{CryptoHashRootView, CryptoHashView, GraphQLView, RootView};

#[derive(View)]
struct TestType<C,T> {
    pub inner: RegisterView<C, T>,
}
