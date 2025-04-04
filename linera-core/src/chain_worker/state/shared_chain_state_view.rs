// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A wrapper around a [`ChainStateView`] that is locked for read-only access.

use std::{ops::Deref, sync::Arc};

use linera_chain::ChainStateView;
use linera_views::context::Context;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// A read-lock on a shared [`ChainStateView`].
pub struct SharedChainStateView<C>(OwnedRwLockReadGuard<ChainStateView<C>>)
where
    C: Context + Send + Sync + 'static;

impl<C> SharedChainStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    /// Creates a new [`SharedChainStateView`] by obtaining a read-lock on `view_lock`.
    pub async fn read_lock(view_lock: Arc<RwLock<ChainStateView<C>>>) -> Self {
        SharedChainStateView(view_lock.read_owned().await)
    }
}

impl<C> Deref for SharedChainStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    type Target = ChainStateView<C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
