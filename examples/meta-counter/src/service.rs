// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::MetaCounter;
use async_graphql::{Request, Response};
use async_trait::async_trait;
use linera_sdk::{
    base::{ApplicationId, WithServiceAbi},
    service::system_api,
    QueryContext, Service, SimpleStateStorage,
};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(MetaCounter);

impl MetaCounter {
    fn counter_id() -> Result<ApplicationId, Error> {
        let parameters = system_api::current_application_parameters();
        serde_json::from_slice(&parameters).map_err(|_| Error::Parameters)
    }
}

impl WithServiceAbi for MetaCounter {
    type Abi = meta_counter::MetaCounterAbi;
}

#[async_trait]
impl Service for MetaCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let argument = serde_json::to_vec(&request).unwrap();
        let value = system_api::query_application(Self::counter_id()?, &argument)
            .await
            .map_err(|_| Error::InternalQuery)?;
        let response = serde_json::from_slice(&value).unwrap();
        Ok(response)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Internal query failed")]
    InternalQuery,

    #[error("Invalid application parameters")]
    Parameters,

    /// Invalid query argument in meta-counter app: could not deserialize GraphQL request.
    #[error("Invalid query argument in meta-counter app: could not deserialize GraphQL request.")]
    InvalidQuery(#[from] serde_json::Error),
}
