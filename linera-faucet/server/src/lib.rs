// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The server component of the Linera faucet.

use std::{
    collections::HashMap, future::IntoFuture, io, net::SocketAddr, path::PathBuf, sync::Arc,
};

use anyhow::Context as _;
use async_graphql::{EmptySubscription, Error, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, Router};
use futures::{lock::Mutex, FutureExt as _};
use linera_base::{
    crypto::{CryptoHash, ValidatorPublicKey},
    data_types::{Amount, ApplicationPermissions, ChainDescription, Timestamp},
    identifiers::{AccountOwner, ChainId},
    ownership::ChainOwnership,
};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext},
    config::GenesisConfig,
};
use linera_core::data_types::ClientOutcome;
#[cfg(feature = "metrics")]
use linera_metrics::prometheus_server;
use linera_storage::{Clock as _, Storage};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::info;

/// Returns an HTML response constructing the GraphiQL web page for the given URI.
pub(crate) async fn graphiql(uri: axum::http::Uri) -> impl axum::response::IntoResponse {
    axum::response::Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint(uri.path())
            .subscription_endpoint("/ws")
            .finish(),
    )
}

#[cfg(test)]
mod tests;

/// The root GraphQL query type.
pub struct QueryRoot<C> {
    context: Arc<Mutex<C>>,
    genesis_config: Arc<GenesisConfig>,
    chain_id: ChainId,
}

/// The root GraphQL mutation type.
pub struct MutationRoot<C> {
    chain_id: ChainId,
    context: Arc<Mutex<C>>,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
    faucet_storage: Arc<Mutex<FaucetStorage>>,
    storage_path: PathBuf,
}

/// The result of a successful `claim` mutation.
#[derive(SimpleObject)]
pub struct ClaimOutcome {
    /// The ID of the new chain.
    pub chain_id: ChainId,
    /// The hash of the parent chain's certificate containing the `OpenChain` operation.
    pub certificate_hash: CryptoHash,
}

#[derive(Debug, Deserialize, SimpleObject)]
pub struct Validator {
    pub public_key: ValidatorPublicKey,
    pub network_address: String,
}

/// Persistent mapping of account owners to chain descriptions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct FaucetStorage {
    /// Maps account owners to their corresponding chain descriptions
    owner_to_chain: HashMap<AccountOwner, ChainDescription>,
}

impl FaucetStorage {
    /// Loads the faucet storage from disk, creating a new one if it doesn't exist
    async fn load(storage_path: &PathBuf) -> Result<Self, io::Error> {
        match tokio::fs::read(storage_path).await {
            Ok(data) => serde_json::from_slice(&data).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to deserialize faucet storage: {}", e),
                )
            }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Saves the faucet storage to disk
    async fn save(&self, storage_path: &PathBuf) -> Result<(), io::Error> {
        let data = serde_json::to_vec_pretty(self).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize faucet storage: {}", e),
            )
        })?;

        // Create parent directory if it doesn't exist
        if let Some(parent) = storage_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(storage_path, data).await
    }

    /// Gets the chain description for an owner if it exists
    fn get_chain(&self, owner: &AccountOwner) -> Option<&ChainDescription> {
        self.owner_to_chain.get(owner)
    }

    /// Stores a new mapping from owner to chain description
    fn store_chain(&mut self, owner: AccountOwner, description: ChainDescription) {
        self.owner_to_chain.insert(owner, description);
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> QueryRoot<C>
where
    C: ClientContext + 'static,
{
    /// Returns the version information on this faucet service.
    async fn version(&self) -> linera_version::VersionInfo {
        linera_version::VersionInfo::default()
    }

    /// Returns the genesis config.
    async fn genesis_config(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(&*self.genesis_config)?)
    }

    /// Returns the current committee's validators.
    async fn current_validators(&self) -> Result<Vec<Validator>, Error> {
        let client = self.context.lock().await.make_chain_client(self.chain_id);
        let committee = client.local_committee().await?;
        Ok(committee
            .validators()
            .iter()
            .map(|(public_key, validator)| Validator {
                public_key: *public_key,
                network_address: validator.network_address.clone(),
            })
            .collect())
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<C> MutationRoot<C>
where
    C: ClientContext + 'static,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, owner: AccountOwner) -> Result<ChainDescription, Error> {
        self.do_claim(owner).await
    }
}

impl<C> MutationRoot<C>
where
    C: ClientContext,
{
    async fn do_claim(&self, owner: AccountOwner) -> Result<ChainDescription, Error> {
        // Check if this owner already has a chain
        {
            let storage = self.faucet_storage.lock().await;
            if let Some(existing_description) = storage.get_chain(&owner) {
                return Ok(existing_description.clone());
            }
        }

        let client = self.context.lock().await.make_chain_client(self.chain_id);

        if self.start_timestamp < self.end_timestamp {
            let local_time = client.storage_client().clock().current_time();
            if local_time < self.end_timestamp {
                let full_duration = self
                    .end_timestamp
                    .delta_since(self.start_timestamp)
                    .as_micros();
                let remaining_duration = self.end_timestamp.delta_since(local_time).as_micros();
                let balance = client.local_balance().await?;
                let Ok(remaining_balance) = balance.try_sub(self.amount) else {
                    return Err(Error::new("The faucet is empty."));
                };
                // The tokens unlock linearly, e.g. if 1/3 of the time is left, then 1/3 of the
                // tokens remain locked, so the remaining balance must be at least 1/3 of the start
                // balance. In general:
                // start_balance / full_duration <= remaining_balance / remaining_duration.
                if multiply(u128::from(self.start_balance), remaining_duration)
                    > multiply(u128::from(remaining_balance), full_duration)
                {
                    return Err(Error::new("Not enough unlocked balance; try again later."));
                }
            }
        }

        let result = client
            .open_chain(
                ChainOwnership::single(owner),
                ApplicationPermissions::default(),
                self.amount,
            )
            .await;
        self.context.lock().await.update_wallet(&client).await?;
        let description = match result? {
            ClientOutcome::Committed((description, _certificate)) => description,
            ClientOutcome::WaitForTimeout(timeout) => {
                return Err(Error::new(format!(
                    "This faucet is using a multi-owner chain and is not the leader right now. \
                    Try again at {}",
                    timeout.timestamp,
                )));
            }
        };

        // Store the new mapping and save to disk
        {
            let mut storage = self.faucet_storage.lock().await;
            storage.store_chain(owner, description.clone());
            if let Err(e) = storage.save(&self.storage_path).await {
                tracing::warn!("Failed to save faucet storage: {}", e);
            }
        }

        Ok(description)
    }
}
/// Multiplies a `u128` with a `u64` and returns the result as a 192-bit number.
fn multiply(a: u128, b: u64) -> [u64; 3] {
    let lower = u128::from(u64::MAX);
    let b = u128::from(b);
    let mut a1 = (a >> 64) * b;
    let a0 = (a & lower) * b;
    a1 += a0 >> 64;
    [(a1 >> 64) as u64, (a1 & lower) as u64, (a0 & lower) as u64]
}

/// A GraphQL interface to request a new chain with tokens.
pub struct FaucetService<C>
where
    C: ClientContext,
{
    chain_id: ChainId,
    context: Arc<Mutex<C>>,
    genesis_config: Arc<GenesisConfig>,
    config: ChainListenerConfig,
    storage: <C::Environment as linera_core::Environment>::Storage,
    port: u16,
    #[cfg(feature = "metrics")]
    metrics_port: u16,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
    faucet_storage: Arc<Mutex<FaucetStorage>>,
    storage_path: PathBuf,
    /// Temporary directory handle to keep it alive (if using temporary storage)
    _temp_dir: Option<Arc<tempfile::TempDir>>,
}

impl<C> Clone for FaucetService<C>
where
    C: ClientContext + 'static,
{
    fn clone(&self) -> Self {
        Self {
            chain_id: self.chain_id,
            context: Arc::clone(&self.context),
            genesis_config: Arc::clone(&self.genesis_config),
            config: self.config.clone(),
            storage: self.storage.clone(),
            port: self.port,
            #[cfg(feature = "metrics")]
            metrics_port: self.metrics_port,
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
            faucet_storage: Arc::clone(&self.faucet_storage),
            storage_path: self.storage_path.clone(),
            _temp_dir: self._temp_dir.clone(),
        }
    }
}

pub struct FaucetConfig {
    pub port: u16,
    #[cfg(feature = "metrics")]
    pub metrics_port: u16,
    pub chain_id: ChainId,
    pub amount: Amount,
    pub end_timestamp: Timestamp,
    pub genesis_config: Arc<GenesisConfig>,
    pub chain_listener_config: ChainListenerConfig,
    pub storage_path: Option<PathBuf>,
}

impl<C> FaucetService<C>
where
    C: ClientContext + 'static,
{
    /// Creates a new instance of the faucet service.
    pub async fn new(
        config: FaucetConfig,
        context: C,
        storage: <C::Environment as linera_core::Environment>::Storage,
    ) -> anyhow::Result<Self> {
        let client = context.make_chain_client(config.chain_id);
        let context = Arc::new(Mutex::new(context));
        let start_timestamp = client.storage_client().clock().current_time();
        client.process_inbox().await?;
        let start_balance = client.local_balance().await?;

        // Create storage path: use provided path or create temporary directory
        let (storage_path, temp_dir) = match config.storage_path {
            Some(path) => (path, None),
            None => {
                let temp_dir = tempfile::tempdir()
                    .context("Failed to create temporary directory for faucet storage")?;
                let storage_path = temp_dir.path().join("faucet_storage.json");
                (storage_path, Some(Arc::new(temp_dir)))
            }
        };

        // Load the faucet storage
        let faucet_storage = FaucetStorage::load(&storage_path)
            .await
            .context("Failed to load faucet storage")?;
        let faucet_storage = Arc::new(Mutex::new(faucet_storage));

        Ok(Self {
            chain_id: config.chain_id,
            context,
            genesis_config: config.genesis_config,
            config: config.chain_listener_config,
            storage,
            port: config.port,
            #[cfg(feature = "metrics")]
            metrics_port: config.metrics_port,
            amount: config.amount,
            end_timestamp: config.end_timestamp,
            start_timestamp,
            start_balance,
            faucet_storage,
            storage_path,
            _temp_dir: temp_dir,
        })
    }

    fn schema(&self) -> Schema<QueryRoot<C>, MutationRoot<C>, EmptySubscription> {
        let mutation_root = MutationRoot {
            chain_id: self.chain_id,
            context: Arc::clone(&self.context),
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
            faucet_storage: Arc::clone(&self.faucet_storage),
            storage_path: self.storage_path.clone(),
        };
        let query_root = QueryRoot {
            genesis_config: Arc::clone(&self.genesis_config),
            context: Arc::clone(&self.context),
            chain_id: self.chain_id,
        };
        Schema::build(query_root, mutation_root, EmptySubscription).finish()
    }

    #[cfg(feature = "metrics")]
    fn metrics_address(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.metrics_port))
    }

    /// Runs the faucet.
    #[tracing::instrument(name = "FaucetService::run", skip_all, fields(port = self.port, chain_id = ?self.chain_id))]
    pub async fn run(self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let port = self.port;
        let index_handler = axum::routing::get(graphiql).post(Self::index_handler);

        #[cfg(feature = "metrics")]
        prometheus_server::start_metrics(self.metrics_address(), cancellation_token.clone());

        let app = Router::new()
            .route("/", index_handler)
            .route("/ready", axum::routing::get(|| async { "ready!" }))
            .route_service("/ws", GraphQLSubscription::new(self.schema()))
            .layer(Extension(self.clone()))
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        let chain_listener =
            ChainListener::new(self.config, self.context, self.storage, cancellation_token).run();
        let tcp_listener =
            tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
        let server = axum::serve(tcp_listener, app).into_future();
        futures::select! {
            result = Box::pin(chain_listener).fuse() => result?,
            result = Box::pin(server).fuse() => result?,
        };

        Ok(())
    }

    /// Executes a GraphQL query and generates a response for our `Schema`.
    async fn index_handler(service: Extension<Self>, request: GraphQLRequest) -> GraphQLResponse {
        let schema = service.0.schema();
        schema.execute(request.into_inner()).await.into()
    }
}
