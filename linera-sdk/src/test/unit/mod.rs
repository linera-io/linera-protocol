// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types for writing unit tests for WebAssembly applications.
//!
//! Unit tests are usually written with the application's source code, and are placed inside the
//! `src` directory together with the main code. The tests are executed by a custom test runner
//! inside an isolated WebAssembly runtime.
//!
//! The system API isn't available to the tests by default. However, calls to them are intercepted
//! and can be controlled by the test to return mock values using the functions in this module.

// Import the contract system interface.
wit_bindgen_guest_rust::export!("mock_system_api.wit");

mod conversions_from_wit;
mod conversions_to_wit;

use self::mock_system_api as wit;
use futures::FutureExt;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_views::{
    batch::{Batch, WriteOperation},
    common::Context,
    memory::MemoryContext,
};
use serde::Serialize;

static mut MOCK_CHAIN_ID: Option<ChainId> = None;
static mut MOCK_APPLICATION_ID: Option<ApplicationId> = None;
static mut MOCK_APPLICATION_PARAMETERS: Option<Vec<u8>> = None;
static mut MOCK_SYSTEM_BALANCE: Option<Amount> = None;
static mut MOCK_SYSTEM_TIMESTAMP: Option<Timestamp> = None;
static mut MOCK_LOG_COLLECTOR: Vec<(log::Level, String)> = Vec::new();
static mut MOCK_KEY_VALUE_STORE: Option<MemoryContext<()>> = None;
static mut MOCK_TRY_QUERY_APPLICATION: Option<
    Box<dyn FnMut(ApplicationId, Vec<u8>) -> Result<Vec<u8>, String>>,
> = None;

/// Sets the mocked chain ID.
pub fn mock_chain_id(chain_id: impl Into<Option<ChainId>>) {
    unsafe { MOCK_CHAIN_ID = chain_id.into() };
}

/// Sets the mocked application ID.
pub fn mock_application_id(application_id: impl Into<Option<ApplicationId>>) {
    unsafe { MOCK_APPLICATION_ID = application_id.into() };
}

/// Sets the mocked application parameters.
pub fn mock_application_parameters(application_parameters: &impl Serialize) {
    let serialized_parameters = serde_json::to_vec(application_parameters)
        .expect("Failed to serialize mock application parameters");

    unsafe { MOCK_APPLICATION_PARAMETERS = Some(serialized_parameters) };
}

/// Sets the mocked system balance.
pub fn mock_system_balance(system_balance: impl Into<Option<Amount>>) {
    unsafe { MOCK_SYSTEM_BALANCE = system_balance.into() };
}

/// Sets the mocked system timestamp.
pub fn mock_system_timestamp(system_timestamp: impl Into<Option<Timestamp>>) {
    unsafe { MOCK_SYSTEM_TIMESTAMP = system_timestamp.into() };
}

/// Returns all messages logged so far.
pub fn log_messages() -> Vec<(log::Level, String)> {
    unsafe { MOCK_LOG_COLLECTOR.clone() }
}

/// Initializes and returns a view context for using as the mocked key-value store.
pub fn mock_key_value_store() -> MemoryContext<()> {
    let store = linera_views::memory::create_memory_context();
    unsafe { MOCK_KEY_VALUE_STORE = Some(store.clone()) };
    store
}

/// Mocks the `try_query_application` system API.
pub fn mock_try_query_application<E>(
    mut handler: impl FnMut(ApplicationId, Vec<u8>) -> Result<Vec<u8>, E> + 'static,
) where
    E: ToString + 'static,
{
    unsafe {
        MOCK_TRY_QUERY_APPLICATION = Some(Box::new(move |application, query| {
            handler(application, query).map_err(|error| error.to_string())
        }))
    }
}

/// Implementation of type that exports an interface for using the mock system API.
pub struct MockSystemApi;

impl wit::MockSystemApi for MockSystemApi {
    fn mocked_chain_id() -> wit::CryptoHash {
        unsafe { MOCK_CHAIN_ID }
            .expect(
                "Unexpected call to the `chain_id` system API. Please call `mock_chain_id` first",
            )
            .into()
    }

    fn mocked_application_id() -> wit::ApplicationId {
        unsafe { MOCK_APPLICATION_ID }
            .expect(
                "Unexpected call to the `application_id` system API. \
                Please call `mock_application_id` first",
            )
            .into()
    }

    fn mocked_application_parameters() -> Vec<u8> {
        unsafe { MOCK_APPLICATION_PARAMETERS.clone() }
            .expect(
                "Unexpected call to the `application_parameters` system API. \
                Please call `mock_application_parameters` first",
            )
            .into()
    }

    fn mocked_read_system_balance() -> wit::Amount {
        unsafe { MOCK_SYSTEM_BALANCE }
            .expect(
                "Unexpected call to the `read_system_balance` system API. \
                Please call `mock_system_balance` first",
            )
            .into()
    }

    fn mocked_read_system_timestamp() -> u64 {
        unsafe { MOCK_SYSTEM_TIMESTAMP }
            .expect(
                "Unexpected call to the `read_system_timestamp` system API. \
                Please call `mock_system_timestamp` first",
            )
            .us()
    }

    fn mocked_log(message: String, level: wit::LogLevel) {
        unsafe { MOCK_LOG_COLLECTOR.push((level.into(), message)) }
    }

    fn mocked_read_multi_values_bytes(keys: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
        unsafe { MOCK_KEY_VALUE_STORE.as_mut() }
            .expect(
                "Unexpected call to `read_multi_values_bytes` system API. \
                Please call `mock_key_value_store` first.",
            )
            .read_multi_values_bytes(keys)
            .now_or_never()
            .expect("Attempt to read from key-value store while it is being written to")
            .expect("Failed to read from memory store")
    }

    fn mocked_read_value_bytes(key: Vec<u8>) -> Option<Vec<u8>> {
        unsafe { MOCK_KEY_VALUE_STORE.as_mut() }
            .expect(
                "Unexpected call to `read_value_bytes` system API. \
                Please call `mock_key_value_store` first.",
            )
            .read_value_bytes(&key)
            .now_or_never()
            .expect("Attempt to read from key-value store while it is being written to")
            .expect("Failed to read from memory store")
    }

    fn mocked_find_keys(prefix: Vec<u8>) -> Vec<Vec<u8>> {
        unsafe { MOCK_KEY_VALUE_STORE.as_mut() }
            .expect(
                "Unexpected call to `find_keys` system API. \
                Please call `mock_key_value_store` first.",
            )
            .find_keys_by_prefix(&prefix)
            .now_or_never()
            .expect("Attempt to read from key-value store while it is being written to")
            .expect("Failed to read from memory store")
    }

    fn mocked_find_key_values(prefix: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
        unsafe { MOCK_KEY_VALUE_STORE.as_mut() }
            .expect(
                "Unexpected call to `find_key_values` system API. \
                Please call `mock_key_value_store` first.",
            )
            .find_key_values_by_prefix(&prefix)
            .now_or_never()
            .expect("Attempt to read from key-value store while it is being written to")
            .expect("Failed to read from memory store")
    }

    fn mocked_write_batch(operations: Vec<wit::WriteOperation>) {
        unsafe { MOCK_KEY_VALUE_STORE.as_mut() }
            .expect(
                "Unexpected call to `write_batch` system API. \
                Please call `mock_key_value_store` first.",
            )
            .write_batch(Batch {
                operations: operations.into_iter().map(WriteOperation::from).collect(),
            })
            .now_or_never()
            .expect("Attempt to write to key-value store while it is being used")
            .expect("Failed to write to memory store")
    }

    fn mocked_try_query_application(
        application: wit::ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        let handler = unsafe { MOCK_TRY_QUERY_APPLICATION.as_mut() }.expect(
            "Unexpected call to `try_query_application` system API. \
            Please call `mock_try_query_application` first",
        );

        handler(application.into(), query).into()
    }
}
