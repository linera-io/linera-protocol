// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, sync::Arc};

use async_graphql::Error;
use futures::lock::{Mutex, MutexGuard};
use linera_base::identifiers::ChainId;
use linera_core::client::ChainClient;
use linera_storage::Storage;
use linera_views::views::ViewError;

pub type ClientMapInner<P, S> = BTreeMap<ChainId, ChainClient<P, S>>;
pub struct ChainClients<P, S>(pub Arc<Mutex<ClientMapInner<P, S>>>)
where
    S: Storage,
    ViewError: From<S::StoreError>;

impl<P, S> Clone for ChainClients<P, S>
where
    S: Storage,
    ViewError: From<S::StoreError>,
{
    fn clone(&self) -> Self {
        ChainClients(self.0.clone())
    }
}

impl<P, S> Default for ChainClients<P, S>
where
    S: Storage,
    ViewError: From<S::StoreError>,
{
    fn default() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
}

impl<P, S> ChainClients<P, S>
where
    S: Storage,
    ViewError: From<S::StoreError>,
{
    async fn client(&self, chain_id: &ChainId) -> Option<ChainClient<P, S>> {
        Some(self.0.lock().await.get(chain_id)?.clone())
    }

    pub async fn client_lock(&self, chain_id: &ChainId) -> Option<ChainClient<P, S>> {
        self.client(chain_id).await
    }

    pub async fn try_client_lock(&self, chain_id: &ChainId) -> Result<ChainClient<P, S>, Error> {
        self.client_lock(chain_id)
            .await
            .ok_or_else(|| Error::new(format!("Unknown chain ID: {}", chain_id)))
    }

    pub async fn map_lock(&self) -> MutexGuard<ClientMapInner<P, S>> {
        self.0.lock().await
    }
}
