// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A wrapper around a [`ChainStateView`] that is locked for read-only access.

use std::{ops::Deref, sync::Arc};

use linera_base::identifiers::ChainId;
use linera_chain::ChainStateView;
use linera_execution::ExecutionRuntimeContext;
use linera_views::context::Context;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tracing::debug;

/// A read-lock on a shared [`ChainStateView`].
pub struct SharedChainStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    chain_id: ChainId,
    chain_state: OwnedRwLockReadGuard<ChainStateView<C>>,
}

impl<C> SharedChainStateView<C>
where
    C: Context + Send + Sync + 'static,
    C::Extra: ExecutionRuntimeContext,
{
    /// Creates a new [`SharedChainStateView`] by obtaining a read-lock on `view_lock`.
    pub async fn read_lock(view_lock: Arc<RwLock<ChainStateView<C>>>) -> Self {
        let chain_state = view_lock.read_owned().await;
        let chain_id = chain_state.chain_id();

        SharedChainStateView {
            chain_id,
            chain_state,
        }
    }
}

impl<C> Deref for SharedChainStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    type Target = ChainStateView<C>;

    fn deref(&self) -> &Self::Target {
        &self.chain_state
    }
}

impl<C> Drop for SharedChainStateView<C>
where
    C: Context + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let chain_id = self.chain_id;
        debug!("Releasing `shared_chain_view` lock for {chain_id}");
    }
}
