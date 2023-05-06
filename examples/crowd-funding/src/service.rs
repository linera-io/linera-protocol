// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use crowd_funding::Query;
use linera_sdk::{
    base::Amount, service::system_api::ReadOnlyViewStorageContext, QueryContext, Service,
    ViewStateStorage,
};
use state::CrowdFunding;
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(CrowdFunding<ReadOnlyViewStorageContext>);

#[async_trait]
impl Service for CrowdFunding<ReadOnlyViewStorageContext> {
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
    async fn pledge_with_transfer(
        &self,
        owner: AccountOwner,
        amount: Amount,
    ) -> Vec<u8> {
        bcs::to_bytes(&Operation::PledgeWithTransfer {
            owner,
            amount,
        })
        .unwrap()
    }
    async fn collect(
        &self,
    ) -> Vec<u8> {
        bcs::to_bytes(&Operation::Collect { }).unwrap()
    }

    async fn cancel(
        &self,
    ) -> Vec<u8> {
        bcs::to_bytes(&Operation::Cancel { }).unwrap()
    }
 }


impl CrowdFunding<ReadOnlyViewStorageContext> {
    /// Returns the total amount of tokens pledged to this campaign.
    pub async fn pledged(&self) -> Amount {
        let mut total = Amount::zero();
        self.pledges
            .for_each_index_value(|_, value| {
                total.saturating_add_assign(value);
                Ok(())
            })
            .await
            .expect("view iteration should not fail");
        total
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid account query.
    #[error("Invalid account specified in query parameter")]
    InvalidQuery(#[from] bcs::Error),
}
