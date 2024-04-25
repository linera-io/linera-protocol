// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for application states.

use crate::views::{RootView, ViewStorageContext};

/// Representation of an empty persistent state.
///
/// This can be used by applications that don't need to store anything in the database.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyState;

impl crate::State for EmptyState {
    async fn load() -> Self {
        EmptyState
    }

    async fn store(&mut self) {}
}

impl<V> crate::State for V
where
    V: RootView<ViewStorageContext>,
{
    async fn load() -> Self {
        V::load(ViewStorageContext::default())
            .await
            .expect("Failed to load application state")
    }

    async fn store(&mut self) {
        self.save()
            .await
            .expect("Failed to store application state")
    }
}
