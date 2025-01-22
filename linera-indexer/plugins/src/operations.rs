// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::{Ordering, PartialOrd},
    sync::Arc,
};

use async_graphql::{OneofObject, SimpleObject};
use axum::Router;
use linera_base::{
    crypto::CryptoHash, data_types::BlockHeight, doc_scalar, hashed::Hashed, identifiers::ChainId,
};
use linera_chain::types::ConfirmedBlock;
use linera_execution::Operation;
use linera_indexer::{
    common::IndexerError,
    plugin::{load, route, sdl, Plugin},
};
use linera_views::{
    context::{Context, ViewContext},
    map_view::MapView,
    store::KeyValueStore,
    views::RootView,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct OperationKey {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

doc_scalar!(OperationKey, "An operation key to index operations");

impl PartialOrd for OperationKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.height
                .cmp(&other.height)
                .then_with(|| self.index.cmp(&other.index)),
        )
    }
}

#[derive(Deserialize, Serialize, Clone, SimpleObject, Debug)]
pub struct ChainOperation {
    key: OperationKey,
    previous_operation: Option<OperationKey>,
    index: u64,
    block: CryptoHash,
    content: Operation,
}

#[derive(RootView)]
pub struct Operations<C> {
    last: MapView<C, ChainId, OperationKey>,
    count: MapView<C, ChainId, u64>,
    /// ChainOperation MapView indexed by their hash
    operations: MapView<C, OperationKey, ChainOperation>,
}

#[derive(OneofObject)]
pub enum OperationKeyKind {
    Key(OperationKey),
    Last(ChainId),
}

/// Implements helper functions on the `RootView`
impl<C> Operations<C>
where
    C: Context + Send + Sync + 'static + Clone,
{
    /// Registers an operation and update count and last entries for this chain ID
    async fn register_operation(
        &mut self,
        key: OperationKey,
        block: CryptoHash,
        content: Operation,
    ) -> Result<(), IndexerError> {
        let last_operation = self.last.get(&key.chain_id).await?;
        match last_operation {
            Some(last_key) if last_key >= key => Ok(()),
            previous_operation => {
                let index = self.count.get(&key.chain_id).await?.unwrap_or(0);
                let operation = ChainOperation {
                    key: key.clone(),
                    previous_operation,
                    index,
                    block,
                    content,
                };
                info!(
                    "register operation for {:?}:\n{:?}",
                    key.chain_id, operation
                );
                self.operations.insert(&key, operation.clone())?;
                self.count.insert(&key.chain_id, index + 1)?;
                Ok(self.last.insert(&key.chain_id, key.clone())?)
            }
        }
    }
}

#[derive(Clone)]
pub struct OperationsPlugin<C>(Arc<Mutex<Operations<C>>>);

static NAME: &str = "operations";

/// Implements `Plugin`
#[async_trait::async_trait]
impl<S> Plugin<S> for OperationsPlugin<ViewContext<(), S>>
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
{
    fn name(&self) -> String {
        NAME.to_string()
    }

    async fn load(store: S) -> Result<Self, IndexerError>
    where
        Self: Sized,
    {
        Ok(Self(load(store, NAME).await?))
    }

    async fn register(&self, value: &Hashed<ConfirmedBlock>) -> Result<(), IndexerError> {
        let mut plugin = self.0.lock().await;
        let chain_id = value.inner().chain_id();
        for (index, content) in value.inner().block().body.operations.iter().enumerate() {
            let key = OperationKey {
                chain_id,
                height: value.inner().height(),
                index,
            };
            match plugin
                .register_operation(key, value.hash(), content.clone())
                .await
            {
                Err(e) => return Err(e),
                Ok(()) => continue,
            }
        }
        Ok(plugin.save().await?)
    }

    fn sdl(&self) -> String {
        sdl(self.clone())
    }

    fn route(&self, app: Router) -> Router {
        route(&self.name(), self.clone(), app)
    }
}

/// Implements `ObjectType`
#[async_graphql::Object(cache_control(no_cache))]
impl<C> OperationsPlugin<C>
where
    C: Context + Send + Sync + 'static + Clone,
{
    /// Gets the operation associated to its hash
    pub async fn operation(
        &self,
        key: OperationKeyKind,
    ) -> Result<Option<ChainOperation>, IndexerError> {
        let plugin = self.0.lock().await;
        let key = match key {
            OperationKeyKind::Last(chain_id) => match plugin.last.get(&chain_id).await? {
                None => return Ok(None),
                Some(key) => key,
            },
            OperationKeyKind::Key(key) => key,
        };
        Ok(plugin.operations.get(&key).await?)
    }

    /// Gets the operations in downward order from an operation hash or from the last block of a chain
    pub async fn operations(
        &self,
        from: OperationKeyKind,
        limit: Option<u32>,
    ) -> Result<Vec<ChainOperation>, IndexerError> {
        let plugin = self.0.lock().await;
        let mut key = match from {
            OperationKeyKind::Last(chain_id) => match plugin.last.get(&chain_id).await? {
                None => return Ok(Vec::new()),
                Some(key) => Some(key),
            },
            OperationKeyKind::Key(key) => Some(key),
        };
        let mut result = Vec::new();
        let limit = limit.unwrap_or(20);
        for _ in 0..limit {
            let Some(next_key) = &key else { break };
            let operation = plugin.operations.get(next_key).await?;
            match operation {
                None => break,
                Some(op) => {
                    key.clone_from(&op.previous_operation);
                    result.push(op)
                }
            }
        }
        Ok(result)
    }

    /// Gets the number of operations registered for a chain
    pub async fn count(&self, chain_id: ChainId) -> Result<u64, IndexerError> {
        let plugin = self.0.lock().await;
        Ok(plugin
            .count
            .get(&chain_id)
            .await
            .map(|opt| opt.unwrap_or(0))?)
    }

    /// Gets the hash of the last operation registered for a chain
    pub async fn last(&self, chain_id: ChainId) -> Result<Option<OperationKey>, IndexerError> {
        let plugin = self.0.lock().await;
        Ok(plugin.last.get(&chain_id).await?)
    }
}
