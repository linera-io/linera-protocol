// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use async_graphql::Error;
use futures::lock::{Mutex, MutexGuard, OwnedMutexGuard};
use linera_base::identifiers::ChainId;
use linera_core::client::{ArcChainClient, ChainClient};

pub type ClientMapInner<P, S> = BTreeMap<ChainId, ArcChainClient<P, S>>;
pub struct ChainClients<P, S>(pub Arc<Mutex<ClientMapInner<P, S>>>);

impl<P, S> Clone for ChainClients<P, S> {
    fn clone(&self) -> Self {
        ChainClients(self.0.clone())
    }
}

impl<P, S> Default for ChainClients<P, S> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
}

impl<P, S> ChainClients<P, S> {
    async fn client(&self, chain_id: &ChainId) -> Option<ArcChainClient<P, S>> {
        Some(self.0.lock().await.get(chain_id)?.clone())
    }

    pub async fn client_lock(
        &self,
        chain_id: &ChainId,
    ) -> Option<OwnedMutexGuard<ChainClient<P, S>>> {
        Some(self.client(chain_id).await?.0.lock_owned().await)
    }

    pub async fn try_client_lock(
        &self,
        chain_id: &ChainId,
    ) -> Result<OwnedMutexGuard<ChainClient<P, S>>, Error> {
        self.client_lock(chain_id)
            .await
            .ok_or_else(|| Error::new(format!("Unknown chain ID: {}", chain_id)))
    }

    pub async fn map_lock(&self) -> MutexGuard<ClientMapInner<P, S>> {
        self.0.lock().await
    }
}
