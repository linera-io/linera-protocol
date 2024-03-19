// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::MetaCounter;
use async_graphql::{Request, Response};
use linera_sdk::{base::WithServiceAbi, Service, ServiceRuntime, SimpleStateStorage};
use thiserror::Error;

pub struct MetaCounterService {
    _state: MetaCounter,
}

linera_sdk::service!(MetaCounterService);

impl WithServiceAbi for MetaCounterService {
    type Abi = meta_counter::MetaCounterAbi;
}

impl Service for MetaCounterService {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;
    type State = MetaCounter;

    async fn new(state: Self::State) -> Result<Self, Self::Error> {
        Ok(MetaCounterService { _state: state })
    }

    async fn handle_query(
        &self,
        _runtime: &ServiceRuntime,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let counter_id = Self::parameters()?;
        Self::query_application(counter_id, &request)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Internal query failed: {0}")]
    InternalQuery(String),

    #[error("Invalid application parameters")]
    Parameters,

    /// Invalid query argument in meta-counter app: could not deserialize GraphQL request.
    #[error("Invalid query argument in meta-counter app: could not deserialize GraphQL request.")]
    InvalidQuery(#[from] serde_json::Error),
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Self::InternalQuery(s)
    }
}
