// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Stub functions for the API exported to unit tests.
//!
//! These can be used by mistake if running unit tests targeting the host architecture, and the
//! default compiler error of missing functions isn't very helpful. Instead, these allow
//! compilation to succeed and fails with a more helpful message *if* one of the functions is
//! called.

use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_views::memory::MemoryContext;
use serde::Serialize;

/// A helpful error message to explain why the mock API isn't available.
const ERROR_MESSAGE: &str =
    "The mock API is only available for unit tests running inside a WebAssembly virtual machine. \
    Please check that the unit tests are executed with `linera project test` or with \
    `cargo test --target wasm32-unknown-unknown`. \
    Also ensure that the unit tests (or the module containing them) has a \
    `#[cfg(target_arch = \"wasm32-unknown-unknown\")]` attribute so that they don't get compiled \
    in for the integration tests";

/// Sets the mocked chain ID.
pub fn mock_chain_id(_chain_id: impl Into<Option<ChainId>>) {
    unreachable!("{ERROR_MESSAGE}");
}

/// Sets the mocked application ID.
pub fn mock_application_id(_application_id: impl Into<Option<ApplicationId>>) {
    unreachable!("{ERROR_MESSAGE}");
}

/// Sets the mocked application parameters.
pub fn mock_application_parameters(_application_parameters: &impl Serialize) {
    unreachable!("{ERROR_MESSAGE}");
}

/// Sets the mocked chain balance.
pub fn mock_chain_balance(_chain_balance: impl Into<Option<Amount>>) {
    unreachable!("{ERROR_MESSAGE}");
}

/// Sets the mocked system timestamp.
pub fn mock_system_timestamp(_system_timestamp: impl Into<Option<Timestamp>>) {
    unreachable!("{ERROR_MESSAGE}");
}

/// Returns all messages logged so far.
pub fn log_messages() -> Vec<(log::Level, String)> {
    unreachable!("{ERROR_MESSAGE}");
}

/// Sets the mocked application state.
pub fn mock_application_state(_state: impl Into<Option<Vec<u8>>>) {
    unreachable!("{ERROR_MESSAGE}");
}

/// Initializes and returns a view context for using as the mocked key-value store.
pub fn mock_key_value_store() -> MemoryContext<()> {
    unreachable!("{ERROR_MESSAGE}");
}

/// Mocks the `try_query_application` system API.
pub fn mock_try_query_application<E>(
    _handler: impl FnMut(ApplicationId, Vec<u8>) -> Result<Vec<u8>, E> + 'static,
) where
    E: ToString + 'static,
{
    unreachable!("{ERROR_MESSAGE}");
}
