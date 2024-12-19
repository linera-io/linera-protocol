// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the trait for indexer plugins.

use std::sync::Arc;

use async_graphql::{EmptyMutation, EmptySubscription, ObjectType, Schema};
use axum::Router;
use linera_base::hashed::Hashed;
use linera_chain::types::ConfirmedBlock;
use linera_views::{context::ViewContext, store::KeyValueStore, views::View};
use tokio::sync::Mutex;

use crate::common::IndexerError;

#[async_trait::async_trait]
pub trait Plugin<S>: Send + Sync
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
{
    /// Gets the name of the plugin
    fn name(&self) -> String
    where
        Self: Sized;

    /// Loads the plugin from a store
    async fn load(store: S) -> Result<Self, IndexerError>
    where
        Self: Sized;

    /// Main function of the plugin: registers the information required for a hashed value
    async fn register(&self, value: &Hashed<ConfirmedBlock>) -> Result<(), IndexerError>;

    /// Produces the GraphQL schema for the plugin
    fn sdl(&self) -> String;

    /// Registers the plugin to an Axum router
    fn route(&self, app: Router) -> Router;
}

async fn handler<Q: ObjectType + 'static>(
    schema: axum::extract::Extension<Schema<Q, EmptyMutation, EmptySubscription>>,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

fn schema<Q: ObjectType + 'static>(query: Q) -> Schema<Q, EmptyMutation, EmptySubscription> {
    Schema::new(query, EmptyMutation, EmptySubscription)
}

pub fn sdl<Q: ObjectType + 'static>(query: Q) -> String {
    schema(query).sdl()
}

pub fn route<Q: ObjectType + 'static>(name: &str, query: Q, app: axum::Router) -> axum::Router {
    app.route(
        &format!("/{}", name),
        axum::routing::get(crate::common::graphiql).post(handler::<Q>),
    )
    .layer(axum::extract::Extension(schema(query)))
    .layer(tower_http::cors::CorsLayer::permissive())
}

pub async fn load<S, V: View<ViewContext<(), S>>>(
    store: S,
    name: &str,
) -> Result<Arc<Mutex<V>>, IndexerError>
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
{
    let root_key = name.as_bytes().to_vec();
    let store = store
        .clone_with_root_key(&root_key)
        .map_err(|_e| IndexerError::CloneWithRootKeyError)?;
    let context = ViewContext::create_root_context(store, ())
        .await
        .map_err(|e| IndexerError::ViewError(e.into()))?;
    let plugin = V::load(context).await?;
    Ok(Arc::new(Mutex::new(plugin)))
}
