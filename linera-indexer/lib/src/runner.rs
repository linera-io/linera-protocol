// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the trait for indexer runners.

use linera_base::identifiers::ChainId;
use linera_views::{
    common::{AdminKeyValueStore, KeyValueStore},
    value_splitting::DatabaseConsistencyError,
    views::ViewError,
};
use tokio::select;
use tracing::{info, warn};

use crate::{common::IndexerError, indexer::Indexer, plugin::Plugin, service::Listener};

#[derive(clap::Parser, Debug, Clone)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
pub enum IndexerCommand {
    Schema {
        plugin: Option<String>,
    },
    Run {
        #[command(flatten)]
        listener: Listener,
        /// The port of the indexer server
        #[arg(long, default_value = "8081")]
        port: u16,
        /// Chains to index (default: the ones on the service wallet)
        chains: Vec<ChainId>,
    },
}

#[derive(clap::Parser, Debug, Clone)]
pub struct IndexerConfig<Config: clap::Args> {
    #[command(flatten)]
    pub client: Config,
    #[command(subcommand)]
    pub command: IndexerCommand,
}

pub struct Runner<DB, Config: clap::Args> {
    pub store: DB,
    pub config: IndexerConfig<Config>,
    pub indexer: Indexer<DB>,
}

impl<DB, Config> Runner<DB, Config>
where
    Self: Send,
    Config: Clone + std::fmt::Debug + Send + Sync + clap::Parser + clap::Args,
    DB: AdminKeyValueStore<Error = <DB as KeyValueStore>::Error>
        + KeyValueStore
        + Clone
        + Send
        + Sync
        + 'static,
    <DB as KeyValueStore>::Error: From<bcs::Error>
        + From<DatabaseConsistencyError>
        + Send
        + Sync
        + std::error::Error
        + 'static,
    ViewError: From<<DB as KeyValueStore>::Error>,
{
    /// Loads a new runner
    pub async fn new(config: IndexerConfig<Config>, store: DB) -> Result<Self, IndexerError>
    where
        Self: Sized,
    {
        let indexer = Indexer::load(store.clone()).await?;
        Ok(Self {
            store,
            config,
            indexer,
        })
    }

    /// Registers a new plugin to the indexer
    pub async fn add_plugin(
        &mut self,
        plugin: impl Plugin<DB> + 'static,
    ) -> Result<(), IndexerError> {
        self.indexer.add_plugin(plugin).await
    }

    /// Runs a server from the indexer and the plugins
    async fn server(port: u16, indexer: &Indexer<DB>) -> Result<(), IndexerError> {
        let mut app = indexer.route(None);
        for plugin in indexer.plugins.values() {
            app = plugin.route(app);
        }
        axum::serve(
            tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await?,
            app,
        )
        .await?;
        Ok(())
    }

    /// Runs a server and the chains listener
    pub async fn run(&mut self) -> Result<(), IndexerError> {
        let config = self.config.clone();
        match config.clone().command {
            IndexerCommand::Schema { plugin } => {
                println!("{}", self.indexer.sdl(plugin)?);
                Ok(())
            }
            IndexerCommand::Run {
                chains,
                listener,
                port,
            } => {
                info!("config: {:?}", config);
                let chains = if chains.is_empty() {
                    listener.service.get_chains().await?
                } else {
                    chains
                };
                let initialize_chains = chains
                    .iter()
                    .map(|chain_id| self.indexer.init(&listener, *chain_id));
                futures::future::try_join_all(initialize_chains).await?;
                let connections = {
                    chains
                        .into_iter()
                        .map(|chain_id| listener.listen(&self.indexer, chain_id))
                };
                select! {
                    result = Self::server(port, &self.indexer) => {
                        result.map(|()| warn!("GraphQL server stopped"))
                    }
                    (result, _, _) = futures::future::select_all(connections.map(Box::pin)) => {
                        result.map(|chain_id| {
                            warn!("Connection to {:?} notifications websocket stopped", chain_id)
                        })
                    }
                }
            }
        }
    }
}
