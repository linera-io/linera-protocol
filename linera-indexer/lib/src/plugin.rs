// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the trait for indexer plugins.

use crate::common::IndexerError;
use axum::Router;
use linera_chain::data_types::HashedValue;
use linera_views::{
    common::{ContextFromDb, KeyValueStoreClient},
    views::ViewError,
};

#[async_trait::async_trait]
pub trait Plugin<DB>: Send + Sync
where
    DB: KeyValueStoreClient + Clone + Send + Sync + 'static,
    DB::Error: From<bcs::Error> + Send + Sync + std::error::Error + 'static,
    ViewError: From<DB::Error>,
{
    /// Loads the plugin from a context
    async fn from_context(context: ContextFromDb<(), DB>) -> Result<Self, IndexerError>
    where
        Self: Sized;

    /// Main function of the plugin: registers the information required for a hashed value
    async fn register(&self, value: &HashedValue) -> Result<(), IndexerError>;

    /// Produces the GraphQL schema for the plugin
    fn sdl(&self) -> String;

    /// Gets the name of the plugin
    fn name(&self) -> String
    where
        Self: Sized,
    {
        Self::static_name()
    }

    /// Defines the name of the plugin
    fn static_name() -> String
    where
        Self: Sized;

    /// Registers the plugin to an Axum router
    fn route(&self, app: Router) -> Router;

    /// Loads the plugin from a client
    async fn load(client: DB) -> Result<Self, IndexerError>
    where
        Self: Sized,
    {
        let context = ContextFromDb::create(client, Self::static_name().as_bytes().to_vec(), ())
            .await
            .map_err(|e| IndexerError::ViewError(e.into()))?;
        Self::from_context(context).await
    }
}
