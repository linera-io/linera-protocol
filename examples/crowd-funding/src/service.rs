// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_graphql::{EmptySubscription, Object, Schema};
use async_trait::async_trait;
use crowd_funding::Operation;
use fungible::AccountOwner;
use linera_sdk::{base::Amount, QueryContext, Service, ViewStateStorage};
use state::CrowdFunding;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(CrowdFunding);

#[async_trait]
impl Service for CrowdFunding {
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
    async fn pledge_with_transfer(&self, owner: AccountOwner, amount: Amount) -> Vec<u8> {
        bcs::to_bytes(&Operation::PledgeWithTransfer { owner, amount }).unwrap()
    }

    async fn collect(&self) -> Vec<u8> {
        bcs::to_bytes(&Operation::Collect {}).unwrap()
    }

    async fn cancel(&self) -> Vec<u8> {
        bcs::to_bytes(&Operation::Cancel {}).unwrap()
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query argument in crowd-funding app: could not deserialize GraphQL request.
    #[error("Invalid query argument in crowd-funding app: could not deserialize GraphQL request.")]
    InvalidQuery,
}
