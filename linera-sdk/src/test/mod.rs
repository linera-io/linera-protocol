// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for writing integration tests for WebAssembly applications.
//!
//! Integration tests are usually written in the `tests` directory in the root of the crate's
//! directory (i.e., beside the `src` directory). Linera application integration tests should be
//! executed targeting the host architecture, instead of targeting `wasm32-unknown-unknown` like
//! done for unit tests.

#![cfg(any(with_testing, with_wasm_runtime))]

#[cfg(with_integration_testing)]
mod block;
#[cfg(with_integration_testing)]
mod chain;
mod mock_stubs;
#[cfg(with_integration_testing)]
mod validator;

#[cfg(with_integration_testing)]
pub use {
    linera_chain::{
        data_types::MessageAction, test::HttpServer, ChainError, ChainExecutionContext,
    },
    linera_core::worker::WorkerError,
    linera_execution::{ExecutionError, QueryOutcome, ResourceTracker, WasmExecutionError},
};

#[cfg(with_testing)]
pub use self::mock_stubs::*;
#[cfg(with_integration_testing)]
pub use self::{
    block::BlockBuilder,
    chain::{ActiveChain, TryGraphQLMutationError, TryGraphQLQueryError, TryQueryError},
    validator::TestValidator,
};
use crate::{Contract, ContractRuntime, Service, ServiceRuntime};

/// Queries the balance of an account owned by `account_owner` on a specific `chain`.
#[cfg(with_integration_testing)]
pub async fn query_account<Abi>(
    application_id: linera_base::identifiers::ApplicationId<Abi>,
    chain: &ActiveChain,
    account_owner: linera_base::identifiers::AccountOwner,
) -> Option<linera_base::data_types::Amount>
where
    Abi: linera_base::abi::ServiceAbi<
        Query = async_graphql::Request,
        QueryResponse = async_graphql::Response,
    >,
{
    use async_graphql::InputType as _;

    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        account_owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, query).await;
    let balance = response.pointer("/accounts/entry/value")?.as_str()?;

    Some(
        balance
            .parse()
            .expect("Account balance cannot be parsed as a number"),
    )
}

/// Creates a [`ContractRuntime`] to use in tests.
pub fn test_contract_runtime<Application: Contract>() -> ContractRuntime<Application> {
    ContractRuntime::new()
}

/// Creates a [`ServiceRuntime`] to use in tests.
pub fn test_service_runtime<Application: Service>() -> ServiceRuntime<Application> {
    ServiceRuntime::new()
}
