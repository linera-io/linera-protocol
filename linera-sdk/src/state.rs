// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for application states.

use std::ops::{Deref, DerefMut};

use crate::{
    util::BlockingWait,
    views::{RootView, ViewStorageContext},
    State,
};

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

/// Helper type to persist a [`State`] when it is dropped.
///
/// If this type is stored in the application contract type, it is dropped when the transaction
/// finishes. Therefore, this ensures that the state is saved at the end of the transaction.
pub struct StoreOnDrop<S>(pub S)
where
    S: State;

impl<S> From<S> for StoreOnDrop<S>
where
    S: State,
{
    fn from(state: S) -> Self {
        StoreOnDrop(state)
    }
}

impl<S> Deref for StoreOnDrop<S>
where
    S: State,
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for StoreOnDrop<S>
where
    S: State,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S> Drop for StoreOnDrop<S>
where
    S: State,
{
    fn drop(&mut self) {
        self.0.store().blocking_wait();
    }
}
