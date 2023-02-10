// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::SimpleCounter;
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service, SimpleStateStorage};
use thiserror::Error;

linera_sdk::service!(SimpleCounter);

#[async_trait]
impl Service for SimpleCounter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

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
    /// Invalid query argument; SimpleCounter application only supports a single (empty) query.
    #[error(
        "Invalid query argument; SimpleCounter application only supports a single (empty) query"
    )]
    InvalidQuery,
}

#[cfg(test)]
mod tests {
    use super::{Error, SimpleCounter};
    use futures::FutureExt;
    use linera_sdk::{ChainId, QueryContext, Service};
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let value = 61_098_721_u128;
        let simple_counter = SimpleCounter { value };

        let result = simple_counter
            .query_application(&dummy_query_context(), &[])
            .now_or_never()
            .expect("Query should not await anything");

        let expected_response =
            bcs::to_bytes(&value).expect("SimpleCounter value could not be serialized");

        assert_eq!(result, Ok(expected_response));
    }

    #[webassembly_test]
    fn invalid_query() {
        let value = 4_u128;
        let simple_counter = SimpleCounter { value };

        let dummy_argument = [2];
        let result = simple_counter
            .query_application(&dummy_query_context(), &dummy_argument)
            .now_or_never()
            .expect("Query should not await anything");

        assert_eq!(result, Err(Error::InvalidQuery));
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 8].into()),
        }
    }
}
