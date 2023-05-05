// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Object, Schema};
use async_trait::async_trait;
use linera_sdk::{
    base::ChainId, service::system_api::ReadOnlyViewStorageContext, QueryContext, Service,
    ViewStateStorage,
};
use linera_views::views::ViewError;
use social::Operation;
use state::Social;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(Social<ReadOnlyViewStorageContext>);

#[async_trait]
impl Service for Social<ReadOnlyViewStorageContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let graphql_request: async_graphql::Request =
            serde_json::from_slice(argument).map_err(Error::InvalidQuery)?;
        let schema = Schema::build(self.clone(), MutationRoot, EmptySubscription).finish();
        let res = schema.execute(graphql_request).await;
        Ok(serde_json::to_vec(&res).unwrap())
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn subscribe(&self, chain_id: ChainId) -> Vec<u8> {
        bcs::to_bytes(&Operation::RequestSubscribe(chain_id)).unwrap()
    }

    async fn unsubscribe(&self, chain_id: ChainId) -> Vec<u8> {
        bcs::to_bytes(&Operation::RequestUnsubscribe(chain_id)).unwrap()
    }

    async fn post(&self, text: String) -> Vec<u8> {
        bcs::to_bytes(&Operation::Post(text)).unwrap()
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query.
    #[error("Invalid query")]
    InvalidQuery(serde_json::Error),

    /// Serialization error.
    #[error(transparent)]
    Serialization(#[from] bcs::Error),

    /// View error.
    #[error(transparent)]
    View(#[from] ViewError),
}
