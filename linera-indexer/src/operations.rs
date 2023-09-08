// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the operations indexer plugin.

use crate::common::IndexerError;
use async_graphql::{EmptyMutation, EmptySubscription, Object, OneofObject, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{extract::Extension, routing::get, Router};
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, doc_scalar, identifiers::ChainId};
use linera_chain::data_types::HashedValue;
use linera_execution::Operation;
use linera_views::{
    common::{Context, ContextFromDb, KeyValueStoreClient},
    map_view::MapView,
    value_splitting::DatabaseConsistencyError,
    views::{RootView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::{Ordering, PartialOrd},
    sync::Arc,
};
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
pub struct OperationsPluginInternal<C> {
    last: MapView<C, ChainId, OperationKey>,
    count: MapView<C, ChainId, u64>,
    /// ChainOperation MapView indexed by their hash
    operations: MapView<C, OperationKey, ChainOperation>,
}

#[derive(Clone)]
pub struct OperationsPlugin<C>(Arc<Mutex<OperationsPluginInternal<C>>>, String);

impl<C> OperationsPlugin<C>
where
    C: Context + Send + Sync + 'static + Clone,
    ViewError: From<C::Error>,
{
    /// Registers an operation and update count and last entries for this chain ID
    async fn register_operation(
        &self,
        key: OperationKey,
        block: CryptoHash,
        content: Operation,
    ) -> Result<(), IndexerError> {
        let mut plugin = self.0.lock().await;
        let last_operation = plugin.last.get(&key.chain_id).await?;
        match last_operation {
            Some(last_key) if last_key >= key => Ok(()),
            previous_operation => {
                let index = plugin.count.get(&key.chain_id).await?.unwrap_or(0);
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
                plugin.operations.insert(&key, operation.clone())?;
                plugin.count.insert(&key.chain_id, index + 1)?;
                plugin
                    .last
                    .insert(&key.chain_id, key.clone())
                    .map_err(IndexerError::ViewError)
            }
        }
    }

    /// Handler for the GraphQL server
    async fn handler(
        schema: Extension<Schema<Self, EmptyMutation, EmptySubscription>>,
        req: GraphQLRequest,
    ) -> GraphQLResponse {
        schema.execute(req.into_inner()).await.into()
    }

    /// Schema for the GraphQL server
    fn schema(&self) -> Schema<Self, EmptyMutation, EmptySubscription> {
        Schema::new(self.clone(), EmptyMutation, EmptySubscription)
    }
}

#[async_trait::async_trait]
impl<DB> crate::plugin::Plugin<DB> for OperationsPlugin<ContextFromDb<(), DB>>
where
    DB: KeyValueStoreClient + Clone + Send + Sync + 'static,
    DB::Error: From<bcs::Error>
        + From<DatabaseConsistencyError>
        + Send
        + Sync
        + std::error::Error
        + 'static,

    ViewError: From<DB::Error>,
{
    async fn register(&self, value: &HashedValue) -> Result<(), IndexerError> {
        let Some(executed_block) = value.inner().executed_block() else {
            return Ok(());
        };
        let chain_id = value.inner().chain_id();
        for (index, content) in executed_block.block.operations.iter().enumerate() {
            let key = OperationKey {
                chain_id,
                height: executed_block.block.height,
                index,
            };
            match self
                .register_operation(key, value.hash(), content.clone())
                .await
            {
                Err(e) => return Err(e),
                Ok(()) => continue,
            }
        }
        let mut plugin = self.0.lock().await;
        plugin.save().await.map_err(IndexerError::ViewError)
    }

    async fn from_context(
        context: ContextFromDb<(), DB>,
        name: &str,
    ) -> Result<Self, IndexerError> {
        let plugin = OperationsPluginInternal::load(context).await?;
        Ok(OperationsPlugin(
            Arc::new(Mutex::new(plugin)),
            name.to_string(),
        ))
    }

    fn sdl(&self) -> String {
        self.schema().sdl()
    }

    fn name(&self) -> String {
        self.1.clone()
    }

    fn route(&self, app: Router) -> Router {
        app.route(
            &format!("/{}", self.1),
            get(crate::common::graphiql).post(Self::handler),
        )
        .layer(Extension(self.schema()))
    }
}

#[derive(OneofObject)]
pub enum OperationKeyKind {
    Key(OperationKey),
    Last(ChainId),
}

#[Object(name = "OperationsRoot")]
impl<C> OperationsPlugin<C>
where
    C: Context + Send + Sync + 'static + Clone,
    ViewError: From<C::Error>,
{
    /// Gets the operation associated to its hash
    pub async fn operation(
        &self,
        key: OperationKeyKind,
    ) -> Result<Option<ChainOperation>, IndexerError> {
        let key = match key {
            OperationKeyKind::Last(chain_id) => {
                match self.0.lock().await.last.get(&chain_id).await? {
                    None => return Ok(None),
                    Some(key) => key,
                }
            }
            OperationKeyKind::Key(key) => key,
        };
        let operation = self
            .0
            .lock()
            .await
            .operations
            .get(&key)
            .await
            .map_err(IndexerError::ViewError)?;
        Ok(operation)
    }

    /// Gets the operations in downward order from an operation hash or from the last block of a chain
    pub async fn operations(
        &self,
        from: OperationKeyKind,
        limit: Option<u32>,
    ) -> Result<Vec<ChainOperation>, IndexerError> {
        let mut key = match from {
            OperationKeyKind::Last(chain_id) => {
                match self.0.lock().await.last.get(&chain_id).await? {
                    None => return Ok(Vec::new()),
                    Some(key) => Some(key),
                }
            }
            OperationKeyKind::Key(key) => Some(key),
        };
        let mut result = Vec::new();
        let limit = limit.unwrap_or(20);
        for _ in 0..limit {
            let Some(next_key) = key else { break };
            let operation = self.0.lock().await.operations.get(&next_key).await?;
            match operation {
                None => break,
                Some(op) => {
                    key = op.previous_operation.clone();
                    result.push(op)
                }
            }
        }
        Ok(result)
    }

    /// Gets the number of operations registered for a chain
    pub async fn count(&self, chain_id: ChainId) -> Result<u64, IndexerError> {
        let plugin = self.0.lock().await;
        plugin
            .count
            .get(&chain_id)
            .await
            .map_err(IndexerError::ViewError)
            .map(|opt| opt.unwrap_or(0))
    }

    /// Gets the hash of the last operation registered for a chain
    pub async fn last(&self, chain_id: ChainId) -> Result<Option<OperationKey>, IndexerError> {
        let plugin = self.0.lock().await;
        plugin
            .last
            .get(&chain_id)
            .await
            .map_err(IndexerError::ViewError)
    }
}
