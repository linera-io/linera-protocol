// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};

use async_graphql::{EmptySubscription, Error, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{Extension, Router};
use futures::lock::Mutex;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ApplicationPermissions, Timestamp},
    identifiers::{ChainId, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_client::{
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext},
    config::GenesisConfig,
};
use linera_core::data_types::ClientOutcome;
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
impl<C> QueryRoot<C>
where
    C: ClientContext,
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
        let client = self.context.lock().await.make_chain_client(self.chain_id)?;
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
impl<C> MutationRoot<C>
where
    C: ClientContext,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, owner: Owner) -> Result<ClaimOutcome, Error> {
        self.do_claim(owner).await
    }
}

impl<C> MutationRoot<C>
where
    C: ClientContext,
{
    async fn do_claim(&self, owner: Owner) -> Result<ClaimOutcome, Error> {
        let client = self.context.lock().await.make_chain_client(self.chain_id)?;

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

        let ownership = ChainOwnership::single(owner);
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

impl<C> MutationRoot<C> {
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
pub struct FaucetService<C>
where
    C: ClientContext,
{
    chain_id: ChainId,
    context: Arc<Mutex<C>>,
    genesis_config: Arc<GenesisConfig>,
    config: ChainListenerConfig,
    storage: C::Storage,
    port: NonZeroU16,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

impl<C> Clone for FaucetService<C>
where
    C: ClientContext,
{
    fn clone(&self) -> Self {
        Self {
            chain_id: self.chain_id,
            context: Arc::clone(&self.context),
            genesis_config: Arc::clone(&self.genesis_config),
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

impl<C> FaucetService<C>
where
    C: ClientContext,
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
        storage: C::Storage,
    ) -> anyhow::Result<Self> {
        let client = context.make_chain_client(chain_id)?;
        let context = Arc::new(Mutex::new(context));
        let start_timestamp = client.storage_client().clock().current_time();
        client.process_inbox().await?;
        let start_balance = client.local_balance().await?;
        Ok(Self {
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

    pub fn schema(&self) -> Schema<QueryRoot<C>, MutationRoot<C>, EmptySubscription> {
        let mutation_root = MutationRoot {
            chain_id: self.chain_id,
            context: Arc::clone(&self.context),
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
        };
        let query_root = QueryRoot {
            genesis_config: Arc::clone(&self.genesis_config),
            context: Arc::clone(&self.context),
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

        ChainListener::new(self.config.clone())
            .run(Arc::clone(&self.context), self.storage.clone())
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
