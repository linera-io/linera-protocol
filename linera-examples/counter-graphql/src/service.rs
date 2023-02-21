// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::Counter;
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_trait::async_trait;
use linera_sdk::{QueryContext, Service, SimpleStateStorage};
use thiserror::Error;

linera_sdk::service!(Counter);

struct DummyObject;

#[async_graphql::Object]
impl DummyObject {
    async fn hello(&self) -> String {
        "Hello World!".to_string()
    }
}

#[async_trait]
impl Service for Counter {
    type Error = Error;
    type Storage = SimpleStateStorage<Self>;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let graphql_request: async_graphql::Request =
            serde_json::from_slice(argument).map_err(|_| Error::InvalidQuery)?;
        let dummy = DummyObject;
        let schema = Schema::build(dummy, EmptyMutation, EmptySubscription).finish();
        let res = schema.execute(graphql_request).await;
        Ok(serde_json::to_vec(&res).unwrap())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; Counter application only supports a single (empty) query.
    #[error(
        "Invalid query argument; Counter application only supports JSON encoded GraphQL queries"
    )]
    InvalidQuery,
}

#[cfg(test)]
mod tests {
    use super::{Counter, Error};
    use async_graphql::{Request, Response};
    use futures::FutureExt;
    use linera_sdk::{ChainId, QueryContext, Service};
    use serde_json::json;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let req = Request::new("{ hello }");
        let req_bytes = serde_json::to_vec(&req).unwrap();
        let counter = Counter { value: 0 };

        let result = counter
            .query_application(&dummy_query_context(), &req_bytes)
            .now_or_never()
            .expect("Query should not await anything")
            .unwrap();

        let response: Response = serde_json::from_slice(&result).unwrap();
        let expected_response = Response::new("Hello World!".to_string());
        // create expected response by hand
        // match response.data {
        //     ConstValue::String(s) => {
        //         assert_eq!(s, "Hello World!".to_string())
        //     },
        //     v => panic!("was expecting string 'Hello World!'. instead got: {}", v)
        // }
    }

    #[webassembly_test]
    fn invalid_query() {
        let value = 4_u128;
        let counter = Counter { value };

        let dummy_argument = [2];
        let result = counter
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
