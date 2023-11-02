// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{connection::EmptyFields, EmptySubscription, Error, Object, Schema};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{http::StatusCode, response, response::IntoResponse, Extension, Router, Server};
use futures::lock::Mutex;
use linera_base::{
    crypto::PublicKey,
    data_types::Amount,
    identifiers::{ChainId, MessageId},
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    node::ValidatorNodeProvider,
};
use linera_execution::{
    system::{Recipient, UserData},
    ChainOwnership,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use serde_json::json;
use std::{net::SocketAddr, num::NonZeroU16, sync::Arc};
use thiserror::Error as ThisError;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::util;

/// The root GraphQL mutation type.
pub struct MutationRoot<P, S> {
    client: Arc<Mutex<ChainClient<P, S>>>,
    amount: Amount,
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

#[Object]
impl<P, S> MutationRoot<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new chain with the given authentication key, and transfers tokens to it.
    async fn claim(&self, public_key: PublicKey) -> Result<MessageId, Error> {
        let ownership = ChainOwnership::single(public_key);
        let mut client = self.client.lock().await;
        let (message_id, _) = client.open_chain(ownership).await?;
        let chain_id = ChainId::child(message_id);
        let recipient = Recipient::chain(chain_id);
        let _certificate = client
            .transfer(None, self.amount, recipient, UserData::default())
            .await?;
        Ok(message_id)
    }
}

/// A GraphQL interface to request a new chain with tokens.
#[derive(Clone)]
pub struct FaucetService<P, S> {
    client: Arc<Mutex<ChainClient<P, S>>>,
    port: NonZeroU16,
    amount: Amount,
}

impl<P, S> FaucetService<P, S>
where
    P: ValidatorNodeProvider + Send + Sync + Clone + 'static,
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    /// Creates a new instance of the faucet service.
    pub fn new(port: NonZeroU16, client: ChainClient<P, S>, amount: Amount) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            port,
            amount,
        }
    }

    pub fn schema(&self) -> Schema<EmptyFields, MutationRoot<P, S>, EmptySubscription> {
        let mutation_root = MutationRoot {
            client: self.client.clone(),
            amount: self.amount,
        };
        Schema::build(EmptyFields, mutation_root, EmptySubscription).finish()
    }

    /// Runs the faucet.
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let port = self.port.get();
        let index_handler = axum::routing::get(util::graphiql).post(Self::index_handler);

        let app = Router::new()
            .route("/", index_handler)
            .route("/ready", axum::routing::get(|| async { "ready!" }))
            .route_service("/ws", GraphQLSubscription::new(self.schema()))
            .layer(Extension(self.clone()))
            .layer(CorsLayer::permissive());

        info!("GraphiQL IDE: http://localhost:{}", port);

        Server::bind(&SocketAddr::from(([127, 0, 0, 1], port)))
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
