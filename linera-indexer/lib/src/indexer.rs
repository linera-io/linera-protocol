// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the base component of linera-indexer.

use std::{collections::BTreeMap, sync::Arc};

use allocative::Allocative;
use async_graphql::{EmptyMutation, EmptySubscription, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{extract::Extension, routing::get, Router};
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
use linera_chain::types::{CertificateValue as _, ConfirmedBlock};
use linera_views::{
    context::{Context, ViewContext},
    map_view::MapView,
    set_view::SetView,
    store::{KeyValueDatabase, KeyValueStore},
    views::RootView,
};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::{
    common::{graphiql, IndexerError},
    plugin::{self, Plugin},
    service::Listener,
};

#[derive(RootView, Allocative)]
#[allocative(bound = "C")]
pub struct StateView<C> {
    chains: MapView<C, ChainId, (CryptoHash, BlockHeight)>,
    plugins: SetView<C, String>,
}

#[derive(Clone)]
pub struct State<C>(Arc<Mutex<StateView<C>>>);

type StateSchema<S> = Schema<State<ViewContext<(), S>>, EmptyMutation, EmptySubscription>;

pub struct Indexer<D>
where
    D: KeyValueDatabase,
{
    pub state: State<ViewContext<(), D::Store>>,
    pub plugins: BTreeMap<String, Box<dyn Plugin<D>>>,
}

#[derive(Debug)]
enum LatestBlock {
    LatestHash(CryptoHash),
    StartHeight(BlockHeight),
}

impl<D> Indexer<D>
where
    D: KeyValueDatabase + Clone + Send + Sync + 'static,
    D::Store: KeyValueStore + Clone + Send + Sync + 'static,
    D::Error: Send + Sync + std::error::Error + 'static,
{
    /// Loads the indexer using a database backend with an `indexer` prefix.
    pub async fn load(database: D) -> Result<Self, IndexerError> {
        let state = State(
            plugin::load::<D, StateView<ViewContext<(), D::Store>>>(database, "indexer").await?,
        );
        Ok(Indexer {
            state,
            plugins: BTreeMap::new(),
        })
    }

    /// Processes one block: registers the block in all the plugins and saves the state of
    /// the indexer.
    pub async fn process_value(
        &self,
        state: &mut StateView<ViewContext<(), D::Store>>,
        value: &ConfirmedBlock,
    ) -> Result<(), IndexerError> {
        for plugin in self.plugins.values() {
            plugin.register(value).await?
        }
        let chain_id = value.chain_id();
        let hash = value.hash();
        let height = value.height();
        info!("save {:?}: {:?} ({})", chain_id, hash, height);
        state
            .chains
            .insert(&chain_id, (hash, height))?;
        state.save().await.map_err(IndexerError::ViewError)
    }

    /// Processes a `NewBlock` notification: processes all blocks from the latest
    /// registered to the one in the notification in the corresponding chain.
    pub async fn process(
        &self,
        listener: &Listener,
        value: &ConfirmedBlock,
    ) -> Result<(), IndexerError> {
        let chain_id = value.chain_id();
        let hash = value.hash();
        let height = value.height();
        let mut state = self.state.0.lock().await;
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
            let header = &value.block().header;
            values.push(value.clone());
            let next_hash = header.previous_block_hash.filter(|&hash| match &latest_block {
                LatestBlock::LatestHash(latest_hash) => *latest_hash != hash,
                LatestBlock::StartHeight(start) => header.height > *start,
            });
            if let Some(hash) = next_hash {
                value = listener.service.get_value(chain_id, Some(hash)).await?;
            } else {
                break;
            }
        }

        while let Some(value) = values.pop() {
            self.process_value(&mut state, &value).await?
        }
        Ok(())
    }

    pub async fn init(&self, listener: &Listener, chain_id: ChainId) -> Result<(), IndexerError> {
        match listener.service.get_value(chain_id, None).await {
            Err(IndexerError::NotFound(_)) => Ok(()),
            result => self.process(listener, &result?).await,
        }
    }

    /// Produces the GraphQL schema for the indexer or for a certain plugin
    pub fn sdl(&self, plugin: Option<String>) -> Result<String, IndexerError> {
        if let Some(plugin_name) = plugin {
            self.plugins
                .get(&plugin_name)
                .map(|plugin| plugin.sdl())
                .ok_or(IndexerError::UnknownPlugin(plugin_name))
        } else {
            Ok(self.state.clone().schema().sdl())
        }
    }

    /// Registers a new plugin in the indexer
    pub async fn add_plugin(
        &mut self,
        plugin: impl Plugin<D> + 'static,
    ) -> Result<(), IndexerError> {
        let name = plugin.name();
        if self
            .plugins
            .insert(name.clone(), Box::new(plugin))
            .is_some()
        {
            return Err(IndexerError::PluginAlreadyRegistered);
        }
        let mut state = self.state.0.lock().await;
        state.plugins.insert(&name)?;
        Ok(())
    }

    /// Handles queries made to the root of the indexer
    async fn handler(
        schema: Extension<StateSchema<D::Store>>,
        req: GraphQLRequest,
    ) -> GraphQLResponse {
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

#[async_graphql::Object(cache_control(no_cache))]
impl<C> State<C>
where
    C: Context + Clone + Send + Sync + 'static,
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
            let (block, height) = match state.chains.get(&chain).await? {
                Some((hash, h)) => (Some(hash), Some(h)),
                None => (None, None),
            };
            result.push(HighestBlock { chain, block, height });
        }
        Ok(result)
    }
}

impl<C> State<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    pub fn schema(self) -> Schema<Self, EmptyMutation, EmptySubscription> {
        Schema::build(self, EmptyMutation, EmptySubscription).finish()
    }
}
