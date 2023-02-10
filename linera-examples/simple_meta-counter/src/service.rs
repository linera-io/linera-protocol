// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::SimpleMetaCounter;
use async_trait::async_trait;
use linera_sdk::{service::system_api, QueryContext, Service, SimpleStateStorage};
use thiserror::Error;

linera_sdk::service!(SimpleMetaCounter);

#[async_trait]
impl Service for SimpleMetaCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let value = system_api::query_application(self.counter_id.unwrap(), argument)
            .await
            .map_err(|_| Error::InternalQuery)?;
        Ok(value)
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    #[error("Internal query failed")]
    InternalQuery,
}
