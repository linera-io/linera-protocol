// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{EmptySubscription, Error, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, Router};
use futures::lock::Mutex;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, ApplicationPermissions, Timestamp},
    identifiers::{ChainId, MessageId},
    ownership::ChainOwnership,
};
use linera_client::{
    chain_clients::ChainClients,
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext},
    config::GenesisConfig,
};
use linera_core::{
    data_types::ClientOutcome,
    node::{ValidatorNode, ValidatorNodeProvider},
};
use linera_execution::committee::ValidatorName;
use linera_storage::{Clock as _, Storage};
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::util;

#[cfg(test)]
#[path = "unit_tests/faucet.rs"]
mod tests;

/// The root GraphQL query type.
pub struct QueryRoot<P, S>
where
    S: Storage,
{
    genesis_config: Arc<GenesisConfig>,
    clients: ChainClients<P, S>,
    chain_id: ChainId,
}

/// The root GraphQL mutation type.
pub struct MutationRoot<P, S, C>
where
    S: Storage,
{
    clients: ChainClients<P, S>,
    chain_id: ChainId,
    context: Arc<Mutex<C>>,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

/// The result of a successful `claim` mutation.
#[derive(SimpleObject)]
pub struct ClaimOutcome {
    /// The ID of the message that created the new chain.
    pub message_id: MessageId,
    /// The ID of the new chain.
    pub chain_id: ChainId,
    /// The hash of the parent chain's certificate containing the `OpenChain` operation.
    pub certificate_hash: CryptoHash,
}

#[derive(Debug, Deserialize, SimpleObject)]
pub struct Validator {
    pub name: ValidatorName,
    pub network_address: String,
}

#[async_graphql::Object(cache_control(no_cache))]
impl<P, S> QueryRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
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
        let client = self.clients.try_client_lock(&self.chain_id).await?;
        let committee = client.local_committee().await?;
        Ok(committee
            .validators()
            .iter()
            .map(|(name, validator)| Validator {
                name: *name,
                network_address: validator.network_address.clone(),
            })
            .collect())
    }
}

#[async_graphql::Object(cache_control(no_cache))]
impl<P, S, C> MutationRoot<P, S, C>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    <P as ValidatorNodeProvider>::Node: Sync,
    S: Storage + Clone + Send + Sync + 'static,
    C: ClientContext<ValidatorNodeProvider = P, Storage = S> + Send + 'static,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, public_key: PublicKey) -> Result<ClaimOutcome, Error> {
        self.do_claim(public_key).await
    }
}

impl<P, S, C> MutationRoot<P, S, C>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    <P as ValidatorNodeProvider>::Node: Sync,
    S: Storage + Clone + Send + Sync + 'static,
    C: ClientContext<ValidatorNodeProvider = P, Storage = S> + Send + 'static,
{
    async fn do_claim(&self, public_key: PublicKey) -> Result<ClaimOutcome, Error> {
        let client = self.clients.try_client_lock(&self.chain_id).await?;

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
                if Self::multiply(u128::from(self.start_balance), remaining_duration)
                    > Self::multiply(u128::from(remaining_balance), full_duration)
                {
                    return Err(Error::new("Not enough unlocked balance; try again later."));
                }
            }
        }

        let ownership = ChainOwnership::single(public_key);
        let result = client
            .open_chain(ownership, ApplicationPermissions::default(), self.amount)
            .await;
        self.context.lock().await.update_wallet(&client).await?;
        let (message_id, certificate) = match result? {
            ClientOutcome::Committed(result) => result,
            ClientOutcome::WaitForTimeout(timeout) => {
                return Err(Error::new(format!(
                    "This faucet is using a multi-owner chain and is not the leader right now. \
                    try again at {}",
                    timeout.timestamp,
                )));
            }
        };
        let chain_id = ChainId::child(message_id);
        Ok(ClaimOutcome {
            message_id,
            chain_id,
            certificate_hash: certificate.hash(),
        })
    }
}

impl<P, S, C> MutationRoot<P, S, C>
where
    S: Storage,
{
    /// Multiplies a `u128` with a `u64` and returns the result as a 192-bit number.
    fn multiply(a: u128, b: u64) -> [u64; 3] {
        let lower = u128::from(u64::MAX);
        let b = u128::from(b);
        let mut a1 = (a >> 64) * b;
        let a0 = (a & lower) * b;
        a1 += a0 >> 64;
        [(a1 >> 64) as u64, (a1 & lower) as u64, (a0 & lower) as u64]
    }
}

/// A GraphQL interface to request a new chain with tokens.
pub struct FaucetService<P, S, C>
where
    S: Storage,
{
    clients: ChainClients<P, S>,
    chain_id: ChainId,
    context: Arc<Mutex<C>>,
    genesis_config: Arc<GenesisConfig>,
    config: ChainListenerConfig,
    storage: S,
    port: NonZeroU16,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

impl<P, S: Clone, C> Clone for FaucetService<P, S, C>
where
    S: Storage,
{
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
            chain_id: self.chain_id,
            context: self.context.clone(),
            genesis_config: self.genesis_config.clone(),
            config: self.config.clone(),
            storage: self.storage.clone(),
            port: self.port,
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
        }
    }
}

impl<P, S, C> FaucetService<P, S, C>
where
    P: ValidatorNodeProvider + Send + Sync + Clone + 'static,
    <<P as ValidatorNodeProvider>::Node as ValidatorNode>::NotificationStream: Send,
    S: Storage + Clone + Send + Sync + 'static,
    C: ClientContext<ValidatorNodeProvider = P, Storage = S> + Send + 'static,
{
    /// Creates a new instance of the faucet service.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        port: NonZeroU16,
        chain_id: ChainId,
        context: C,
        amount: Amount,
        end_timestamp: Timestamp,
        genesis_config: Arc<GenesisConfig>,
        config: ChainListenerConfig,
        storage: S,
    ) -> anyhow::Result<Self> {
        let clients = ChainClients::<P, S>::from_context(&context).await;
        let context = Arc::new(Mutex::new(context));
        let client = clients.try_client_lock(&chain_id).await?;
        let start_timestamp = client.storage_client().clock().current_time();
        client.process_inbox().await?;
        let start_balance = client.local_balance().await?;
        Ok(Self {
            clients,
            chain_id,
            context,
            genesis_config,
            config,
            storage,
            port,
            amount,
            end_timestamp,
            start_timestamp,
            start_balance,
        })
    }

    pub fn schema(&self) -> Schema<QueryRoot<P, S>, MutationRoot<P, S, C>, EmptySubscription> {
        let mutation_root = MutationRoot {
            clients: self.clients.clone(),
            chain_id: self.chain_id,
            context: self.context.clone(),
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
        };
        let query_root = QueryRoot {
            genesis_config: self.genesis_config.clone(),
            clients: self.clients.clone(),
            chain_id: self.chain_id,
        };
        Schema::build(query_root, mutation_root, EmptySubscription).finish()
    }

    /// Runs the faucet.
    pub async fn run(self) -> anyhow::Result<()> {
        let port = self.port.get();
        let index_handler = axum::routing::get(util::graphiql).post(Self::index_handler);

        let app = Router::new()
            .route("/", index_handler)
            .route("/ready", axum::routing::get(|| async { "ready!" }))
            .route_service("/ws", GraphQLSubscription::new(self.schema()))
            .layer(Extension(self.clone()))
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        ChainListener::new(self.config.clone(), self.clients.clone())
            .run(self.context.clone(), self.storage.clone())
            .await;

        axum::serve(
            tokio::net::TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?,
            app,
        )
        .await?;

        Ok(())
    }

    /// Executes a GraphQL query and generates a response for our `Schema`.
    async fn index_handler(service: Extension<Self>, request: GraphQLRequest) -> GraphQLResponse {
        let schema = service.0.schema();
        schema.execute(request.into_inner()).await.into()
    }
}
