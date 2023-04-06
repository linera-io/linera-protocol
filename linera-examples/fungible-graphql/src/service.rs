// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::FungibleToken;
use async_graphql::{EmptySubscription, Object, Schema};
use async_trait::async_trait;
use fungible_graphql::Transfer;
use linera_sdk::{
    service::system_api::ReadOnlyViewStorageContext, QueryContext, Service, ViewStateStorage,
};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(FungibleToken<ReadOnlyViewStorageContext>);


#[async_trait]
impl Service for FungibleToken<ReadOnlyViewStorageContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let graphql_request: async_graphql::Request =
            serde_json::from_slice(argument).map_err(|_| Error::InvalidQuery)?;
        let schema = Schema::build(self.clone(), MutationRoot {}, EmptySubscription).finish();
        let res = schema.execute(graphql_request).await;
        Ok(serde_json::to_vec(&res).unwrap())
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    #[allow(unused)]
    async fn execute_operation(&self, transfer: Transfer) -> Vec<u8> {
        bcs::to_bytes(&transfer).unwrap()
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; could not deserialize GraphQL request.
    #[error(
        "Invalid query argument; Counter application only supports JSON encoded GraphQL queries"
    )]
    InvalidQuery,
}
