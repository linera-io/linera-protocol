// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use self::state::ViewCounter;

use async_trait::async_trait;
use linera_sdk::{
    service::system_api::HostServiceWasmContext, QueryContext, Service, ViewStateStorage,
};
use linera_views::{common::Context, memory::MemoryContext, views::ViewError};
use thiserror::Error;

/// Alias to the application type, so that the boilerplate module can reference it.
pub type ApplicationState = ViewCounter<HostServiceWasmContext>;
pub type ApplicationStateTest = ViewCounter<MemoryContext<()>>;
linera_sdk::service!(ApplicationState);

#[async_trait]
impl<C: Context + Send + Sync> Service for ViewCounter<C>
where
    ViewError: From<<C as linera_views::common::Context>::Error>,
{
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        &self,
        _context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        let value = self.value.get();
        match argument {
            &[] => Ok(bcs::to_bytes(&value).expect("Serialization should not fail")),
            _ => Err(Error::InvalidQuery),
        }
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Error {
    /// Invalid query argument; ViewCounter application only supports a single (empty) query.
    #[error(
        "Invalid query argument; ViewCounter application only supports a single (empty) query"
    )]
    InvalidQuery,
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::ApplicationStateTest;
    use futures_util::FutureExt;
    use linera_sdk::{ChainId, QueryContext, Service};
    use linera_views::{memory::get_memory_context, views::View};
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn query() {
        let value = 61_098_721_u128;
        let context = get_memory_context()
            .now_or_never()
            .expect("Failed to acquire the guard");
        let mut view_counter = ApplicationStateTest::load(context)
            .now_or_never()
            .unwrap()
            .expect("Failed to load view_Counter");
        view_counter.value.set(value);
        let result = view_counter
            .query_application(&dummy_query_context(), &[])
            .now_or_never()
            .expect("Query should not await anything");

        let expected_response =
            bcs::to_bytes(&value).expect("ViewCounter value could not be serialized");

        assert_eq!(result, Ok(expected_response));
    }

    #[webassembly_test]
    fn invalid_query() {
        let value = 4_u128;
        let context = get_memory_context()
            .now_or_never()
            .expect("Failed to acquire the guard");
        let mut view_counter = ApplicationStateTest::load(context)
            .now_or_never()
            .unwrap()
            .expect("Failed to load ViewCounter");
        view_counter.value.set(value);

        let dummy_argument = [2];
        let result = view_counter
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
