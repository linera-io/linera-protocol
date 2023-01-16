// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(target_arch = "wasm32")]

mod state;

use self::state::{ApplicationState, Counter};
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service};
use thiserror::Error;

#[async_trait]
impl Service for Counter {
    type Error = Error;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        match argument {
            &[] => Ok(bcs::to_bytes(&self.value).expect("Serialization should not fail")),
            _ => Err(Error::InvalidQuery),
        }
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; Counter application only supports a single (empty) query.
    #[error("Invalid query argument; Counter application only supports a single (empty) query")]
    InvalidQuery,
}

#[path = "../boilerplate/service/mod.rs"]
mod boilerplate;

#[cfg(test)]
mod tests {
    use super::Counter;
    use futures::FutureExt;
    use linera_sdk::{ChainId, QueryContext, Service};
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let value = 61_098_721_u128;
        let counter = Counter { value };

        let result = counter
            .query_application(&dummy_query_context(), &[])
            .now_or_never()
            .expect("Query should not await anything");

        let expected_response =
            bcs::to_bytes(&value).expect("Counter value could not be serialized");

        assert_eq!(result, Ok(expected_response));
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 8].into()),
        }
    }
}
