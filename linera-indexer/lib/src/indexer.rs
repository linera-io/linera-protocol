// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the base component of linera-indexer.

use std::{collections::BTreeMap, sync::Arc};

use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{extract::Extension, routing::get, Router};
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::data_types::HashedCertificateValue;
use linera_views::{
    common::{Context, ContextFromStore, KeyValueStore},
    map_view::MapView,
    register_view::RegisterView,
    set_view::SetView,
    value_splitting::DatabaseConsistencyError,
    views::{RootView, View, ViewError},
};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::{
    common::{graphiql, IndexerError},
    plugin::Plugin,
    service::Listener,
};

#[derive(RootView)]
pub struct StateView<C> {
    chains: MapView<C, ChainId, (CryptoHash, BlockHeight)>,
    plugins: SetView<C, String>,
    initiated: RegisterView<C, bool>,
}

#[derive(Clone)]
pub struct State<C>(Arc<Mutex<StateView<C>>>);

type StateSchema<S> = Schema<State<ContextFromStore<(), S>>, EmptyMutation, EmptySubscription>;

pub struct Indexer<S> {
    pub state: State<ContextFromStore<(), S>>,
    pub plugins: BTreeMap<String, Box<dyn Plugin<S>>>,
}

pub enum IndexerCommand {
    Run,
    Schema,
}

#[derive(Debug)]
enum LatestBlock {
    LatestHash(CryptoHash),
    StartHeight(BlockHeight),
}

impl<S> Indexer<S>
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: From<bcs::Error>
        + From<DatabaseConsistencyError>
        + Send
        + Sync
        + std::error::Error
        + 'static,
    ViewError: From<S::Error>,
{
    /// Loads the indexer using a database backend with an `indexer` prefix.
    pub async fn load(store: S) -> Result<Self, IndexerError> {
        let context = ContextFromStore::create(store.clone(), "indexer".as_bytes().to_vec(), ())
            .await
            .map_err(|e| IndexerError::ViewError(e.into()))?;
        let state = State(Arc::new(Mutex::new(StateView::load(context).await?)));
        Ok(Indexer {
            state,
            plugins: BTreeMap::new(),
        })
    }

    /// Processes one block: registers the block in all the plugins and saves the state of
    /// the indexer.
    pub async fn process_value(
        &self,
        state: &mut StateView<ContextFromStore<(), S>>,
        value: &HashedCertificateValue,
    ) -> Result<(), IndexerError> {
        for plugin in self.plugins.values() {
            plugin.register(value).await?
        }
        let chain_id = value.inner().chain_id();
        let hash = value.hash();
        let height = value.inner().height();
        info!("save {:?}: {:?} ({})", chain_id, hash, height);
        state
            .chains
            .insert(&chain_id, (value.hash(), value.inner().height()))?;
        state.save().await.map_err(IndexerError::ViewError)
    }

    /// Processes a `NewBlock` notification: processes all blocks from the latest
    /// registered to the one in the notification in the corresponding chain.
    pub async fn process(
        &self,
        listener: &Listener,
        value: &HashedCertificateValue,
    ) -> Result<(), IndexerError> {
        let chain_id = value.inner().chain_id();
        let hash = value.hash();
        let height = value.inner().height();
        let state = &mut self.state.0.lock().await;
        if height < listener.start {
            return Ok(());
        };
        let latest_block = match state.chains.get(&chain_id).await? {
            None => LatestBlock::StartHeight(listener.start),
            Some((last_hash, last_height)) => {
                if last_hash == hash || last_height >= height {
                    return Ok(());
                }
                LatestBlock::LatestHash(last_hash)
            }
        };
        info!("process {:?}: {:?} ({})", chain_id, hash, height);

        let mut values = Vec::new();
        let mut value = value.clone();
        loop {
            let Some(block) = value.inner().block() else {
                return Err(IndexerError::InvalidCertificateValue(value.hash()));
            };
            if !value.inner().is_confirmed() {
                return Err(IndexerError::InvalidCertificateValue(value.hash()));
            };
            values.push(value.clone());
            if let Some(hash) = block.previous_block_hash {
                match latest_block {
                    LatestBlock::LatestHash(latest_hash) if latest_hash != hash => {
                        value = listener.service.get_value(chain_id, Some(hash)).await?;
                        continue;
                    }
                    LatestBlock::StartHeight(start) if block.height > start => {
                        value = listener.service.get_value(chain_id, Some(hash)).await?;
                        continue;
                    }
                    _ => break,
                }
            }
            break;
        }

        while let Some(value) = values.pop() {
            self.process_value(state, &value).await?
        }
        Ok(())
    }

    pub async fn init(&self, listener: &Listener, chain_id: ChainId) -> Result<(), IndexerError> {
        match listener.service.get_value(chain_id, None).await {
            Ok(value) => self.process(listener, &value).await,
            Err(IndexerError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Produces the GraphQL schema for the indexer or for a certain plugin
    pub fn sdl(&self, plugin: Option<String>) -> Result<String, IndexerError> {
        match plugin {
            None => Ok(self.state.clone().schema().sdl()),
            Some(plugin) => match self.plugins.get(&plugin) {
                Some(plugin) => Ok(plugin.sdl()),
                None => Err(IndexerError::UnknownPlugin(plugin.to_string())),
            },
        }
    }

    /// Registers a new plugin in the indexer
    pub async fn add_plugin(
        &mut self,
        plugin: impl Plugin<S> + 'static,
    ) -> Result<(), IndexerError> {
        let name = plugin.name();
        self.plugins
            .insert(name.clone(), Box::new(plugin))
            .map_or_else(|| Ok(()), |_| Err(IndexerError::PluginAlreadyRegistered))?;
        let mut state = self.state.0.lock().await;
        Ok(state.plugins.insert(&name)?)
    }

    /// Handles queries made to the root of the indexer
    async fn handler(schema: Extension<StateSchema<S>>, req: GraphQLRequest) -> GraphQLResponse {
        schema.execute(req.into_inner()).await.into()
    }

    /// Registers the handler to an Axum router
    pub fn route(&self, app: Option<Router>) -> Router {
        let app = app.unwrap_or_default();
        app.route("/", get(graphiql).post(Self::handler))
            .layer(Extension(self.state.clone().schema()))
            .layer(CorsLayer::permissive())
    }
}

#[derive(SimpleObject)]
pub struct HighestBlock {
    chain: ChainId,
    block: Option<CryptoHash>,
    height: Option<BlockHeight>,
}

#[Object]
impl<C> State<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    /// Gets the plugins registered in the indexer
    pub async fn plugins(&self) -> Result<Vec<String>, IndexerError> {
        let state = self.0.lock().await;
        Ok(state.plugins.indices().await?)
    }

    /// Gets the latest blocks registered for each chain handled by the indexer
    pub async fn state(&self) -> Result<Vec<HighestBlock>, IndexerError> {
        let state = self.0.lock().await;
        let chains = state.chains.indices().await?;
        let mut result = Vec::new();
        for chain in chains {
            let block = state.chains.get(&chain).await?;
            result.push(HighestBlock {
                chain,
                block: block.map(|b| b.0),
                height: block.map(|b| b.1),
            });
        }
        Ok(result)
    }
}

impl<C> State<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
{
    pub fn schema(self) -> Schema<Self, EmptyMutation, EmptySubscription> {
        Schema::build(self, EmptyMutation, EmptySubscription).finish()
    }
}
