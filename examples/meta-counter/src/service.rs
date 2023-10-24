// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::MetaCounter;
use async_graphql::{Request, Response};
use async_trait::async_trait;
use linera_sdk::{base::WithServiceAbi, QueryContext, Service, SimpleStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(MetaCounter);

impl WithServiceAbi for MetaCounter {
    type Abi = meta_counter::MetaCounterAbi;
}

#[async_trait]
impl Service for MetaCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn handle_query(
        self: Arc<Self>,
        _context: &QueryContext,
        request: Request,
    ) -> Result<Response, Self::Error> {
        let counter_id = Self::parameters()?;
        Self::query_application(counter_id, &request).await
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
