// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use self::state::Counter;
use async_graphql::{EmptySubscription, Object, Schema};
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service, SimpleStateStorage};
use std::sync::Arc;
use thiserror::Error;

linera_sdk::service!(Counter);

#[async_trait]
impl Service for Counter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        match argument {
            &[] => Ok(bcs::to_bytes(&self.value).expect("Serialization should not fail")),
            bytes => {
                let graphql_request: async_graphql::Request =
                    serde_json::from_slice(bytes).map_err(|_| Error::InvalidQuery)?;
                let schema = Schema::build(
                    QueryRoot { value: self.value },
                    MutationRoot {},
                    EmptySubscription,
                )
                .finish();
                let res = schema.execute(graphql_request).await;
                Ok(serde_json::to_vec(&res).unwrap())
            }
        }
    }
}

struct MutationRoot;

#[Object]
impl MutationRoot {
    #[allow(unused)]
    async fn execute_operation(&self, operation: u64) -> Vec<u8> {
        bcs::to_bytes(&operation).unwrap()
    }
}

struct QueryRoot {
    value: u64,
}

#[Object]
impl QueryRoot {
    #[allow(unused)]
    async fn value(&self) -> &u64 {
        &self.value
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; Counter application only supports a single (empty) query.
    #[error("Invalid query argument; Counter application only supports a single (empty) query")]
    InvalidQuery,
}

#[cfg(test)]
mod tests {
    use super::{Counter, Error};
    use futures::FutureExt;
    use linera_sdk::{base::ChainId, QueryContext, Service};
    use std::sync::Arc;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let value = 61_098_721_u64;
        let counter = Arc::new(Counter { value });

        let result = counter
            .query_application(&dummy_query_context(), &[])
            .now_or_never()
            .expect("Query should not await anything");

        let expected_response =
            bcs::to_bytes(&value).expect("Counter value could not be serialized");

        assert_eq!(result, Ok(expected_response));
    }

    #[webassembly_test]
    fn invalid_query() {
        let value = 4_u64;
        let counter = Arc::new(Counter { value });

        let dummy_argument = [2];
        let result = counter
            .query_application(&dummy_query_context(), &dummy_argument)
            .now_or_never()
            .expect("Query should not await anything");

        assert_eq!(result, Err(Error::InvalidQuery));
    }

    fn dummy_query_context() -> QueryContext {
        QueryContext {
            chain_id: ChainId([0; 4].into()),
        }
    }
}
