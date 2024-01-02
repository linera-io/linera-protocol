// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{EmptySubscription, Error, Object, Schema, SimpleObject};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{http::StatusCode, response, response::IntoResponse, Extension, Router, Server};
use futures::lock::Mutex;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, Timestamp},
    identifiers::{ChainId, MessageId},
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    data_types::ClientOutcome,
    node::ValidatorNodeProvider,
};
use linera_execution::{committee::ValidatorName, ChainOwnership};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde::Deserialize;
use serde_json::json;
use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};
use thiserror::Error as ThisError;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::{config::GenesisConfig, util};

#[cfg(test)]
#[path = "unit_tests/faucet.rs"]
mod tests;

/// The root GraphQL query type.
pub struct QueryRoot<P, S> {
    genesis_config: Arc<GenesisConfig>,
    client: Arc<Mutex<ChainClient<P, S>>>,
}

/// The root GraphQL mutation type.
pub struct MutationRoot<P, S> {
    client: Arc<Mutex<ChainClient<P, S>>>,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

#[derive(Debug, ThisError)]
#[error(transparent)]
struct FaucetError(#[from] ChainClientError);

impl IntoResponse for FaucetError {
    fn into_response(self) -> response::Response {
        let code = StatusCode::INTERNAL_SERVER_ERROR;
        let json = json!({"error": self.0.to_string()});
        (code, json.to_string()).into_response()
    }
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

#[Object]
impl<P, S> QueryRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Returns the genesis config.
    async fn genesis_config(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(&*self.genesis_config)?)
    }

    /// Returns the current committee's validators.
    async fn current_validators(&self) -> Result<Vec<Validator>, Error> {
        let mut client = self.client.lock().await;
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

#[Object]
impl<P, S> MutationRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, public_key: PublicKey) -> Result<ClaimOutcome, Error> {
        self.do_claim(public_key).await
    }
}

impl<P, S> MutationRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    async fn do_claim(&self, public_key: PublicKey) -> Result<ClaimOutcome, Error> {
        let mut client = self.client.lock().await;

        if self.start_timestamp < self.end_timestamp {
            let local_time = client.storage_client().await.current_time();
            if local_time < self.end_timestamp {
                let full_duration = self
                    .end_timestamp
                    .saturating_diff_micros(self.start_timestamp);
                let remaining_duration = self.end_timestamp.saturating_diff_micros(local_time);
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
        let (message_id, certificate) = match client.open_chain(ownership, self.amount).await? {
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

impl<P, S> MutationRoot<P, S> {
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
#[derive(Clone)]
pub struct FaucetService<P, S> {
    client: Arc<Mutex<ChainClient<P, S>>>,
    genesis_config: Arc<GenesisConfig>,
    port: NonZeroU16,
    amount: Amount,
    end_timestamp: Timestamp,
    start_timestamp: Timestamp,
    start_balance: Amount,
}

impl<P, S> FaucetService<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + Clone + 'static,
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new instance of the faucet service.
    pub async fn new(
        port: NonZeroU16,
        mut client: ChainClient<P, S>,
        amount: Amount,
        end_timestamp: Timestamp,
        genesis_config: Arc<GenesisConfig>,
    ) -> anyhow::Result<Self> {
        let start_timestamp = client.storage_client().await.current_time();
        let start_balance = client.local_balance().await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            genesis_config,
            port,
            amount,
            end_timestamp,
            start_timestamp,
            start_balance,
        })
    }

    pub fn schema(&self) -> Schema<QueryRoot<P, S>, MutationRoot<P, S>, EmptySubscription> {
        let mutation_root = MutationRoot {
            client: self.client.clone(),
            amount: self.amount,
            end_timestamp: self.end_timestamp,
            start_timestamp: self.start_timestamp,
            start_balance: self.start_balance,
        };
        let query_root = QueryRoot {
            genesis_config: self.genesis_config.clone(),
            client: self.client.clone(),
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

        Server::bind(&SocketAddr::from(([0, 0, 0, 0], port)))
            .serve(app.into_make_service())
            .await?;

        Ok(())
    }

    /// Executes a GraphQL query and generates a response for our `Schema`.
    async fn index_handler(service: Extension<Self>, request: GraphQLRequest) -> GraphQLResponse {
        let schema = service.0.schema();
        schema.execute(request.into_inner()).await.into()
    }
}
