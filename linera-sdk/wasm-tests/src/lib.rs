// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests that should be executed inside a WebAssembly environment.
//!
//! Includes tests for the mocked system APIs.

#![cfg(test)]
#![cfg(target_arch = "wasm32")]

use async_trait::async_trait;
use linera_sdk::{
    abi::{ContractAbi, ServiceAbi, WithContractAbi, WithServiceAbi},
    base::{Amount, ApplicationId, BlockHeight, BytecodeId, ChainId, MessageId, Timestamp},
    test::{self, test_contract_runtime, test_service_runtime},
    util::BlockingWait,
    views::ViewStorageContext,
    Contract, ContractLogger, ContractRuntime, Service, ServiceLogger, ServiceRuntime,
    SimpleStateStorage,
};
use linera_views::{
    map_view::MapView,
    register_view::RegisterView,
    views::{HashableView, RootView, View},
};
use thiserror::Error;
use webassembly_test::webassembly_test;

/// Test if the chain ID getter API is mocked successfully.
#[webassembly_test]
fn mock_chain_id() {
    let chain_id = ChainId([0, 1, 2, 3].into());

    test::mock_chain_id(chain_id);

    assert_eq!(test_contract_runtime::<TestApp>().chain_id(), chain_id);
    assert_eq!(test_service_runtime::<TestApp>().chain_id(), chain_id);
}

/// Test if the application ID getter API is mocked successfully.
#[webassembly_test]
fn mock_application_id() {
    let application_id = ApplicationId {
        bytecode_id: BytecodeId::new(MessageId {
            chain_id: ChainId([0, 1, 2, 3].into()),
            height: BlockHeight::from(4),
            index: 5,
        }),
        creation: MessageId {
            chain_id: ChainId([6, 7, 8, 9].into()),
            height: BlockHeight::from(10),
            index: 11,
        },
    };

    test::mock_application_id(application_id);

    assert_eq!(
        test_contract_runtime::<TestApp>().application_id(),
        application_id.with_abi()
    );
    assert_eq!(
        test_service_runtime::<TestApp>().application_id(),
        application_id.with_abi()
    );
}

/// Test if the application parameters getter API is mocked successfully.
#[webassembly_test]
fn mock_application_parameters() {
    let parameters = vec![0, 1, 2, 3, 4, 5, 6];

    test::mock_application_parameters(&parameters);

    assert_eq!(
        test_contract_runtime::<TestApp>().application_parameters(),
        parameters
    );
    assert_eq!(
        test_service_runtime::<TestApp>().application_parameters(),
        parameters
    );
}

/// Test if the chain balance getter API is mocked successfully.
#[webassembly_test]
fn mock_chain_balance() {
    let balance = Amount::from_attos(0x00010203_04050607_08090a0b_0c0d0e0f);

    test::mock_chain_balance(balance);

    assert_eq!(test_contract_runtime::<TestApp>().chain_balance(), balance);
    assert_eq!(test_service_runtime::<TestApp>().chain_balance(), balance);
}

/// Test if the system timestamp getter API is mocked successfully.
#[webassembly_test]
fn mock_system_timestamp() {
    let timestamp = Timestamp::from(0x00010203_04050607);

    test::mock_system_timestamp(timestamp);

    assert_eq!(test_contract_runtime::<TestApp>().system_time(), timestamp);
    assert_eq!(test_service_runtime::<TestApp>().system_time(), timestamp);
}

/// Test if messages logged by a contract can be inspected.
#[webassembly_test]
fn mock_contract_log() {
    ContractLogger::install();

    log::trace!("Trace");
    log::debug!("Debug");
    log::info!("Info");
    log::warn!("Warn");
    log::error!("Error");

    let expected = vec![
        (log::Level::Trace, "Trace".to_owned()),
        (log::Level::Debug, "Debug".to_owned()),
        (log::Level::Info, "Info".to_owned()),
        (log::Level::Warn, "Warn".to_owned()),
        (log::Level::Error, "Error".to_owned()),
    ];

    assert_eq!(test::log_messages(), expected);
}

/// Test if messages logged by a service can be inspected.
#[webassembly_test]
fn mock_service_log() {
    ServiceLogger::install();

    log::trace!("Trace");
    log::debug!("Debug");
    log::info!("Info");
    log::warn!("Warn");
    log::error!("Error");

    let expected = vec![
        (log::Level::Trace, "Trace".to_owned()),
        (log::Level::Debug, "Debug".to_owned()),
        (log::Level::Info, "Info".to_owned()),
        (log::Level::Warn, "Warn".to_owned()),
        (log::Level::Error, "Error".to_owned()),
    ];

    assert_eq!(test::log_messages(), expected);
}

/// A dummy view to test the key value store.
#[derive(RootView)]
struct DummyView<C> {
    one: RegisterView<C, u8>,
    two: RegisterView<C, u16>,
    three: RegisterView<C, u32>,
    map: MapView<C, u8, i8>,
}

/// Test if views are loaded from a memory key-value store.
#[webassembly_test]
fn mock_load_view() {
    let store = test::mock_key_value_store();
    let mut initial_view = DummyView::load(store)
        .blocking_wait()
        .expect("Failed to initialize `DummyView` with the mock key value store");

    initial_view.one.set(1);
    initial_view.two.set(2);
    initial_view.three.set(3);
    initial_view
        .save()
        .blocking_wait()
        .expect("Failed to persist view state");

    let first_view = DummyView::load(ViewStorageContext::default())
        .blocking_wait()
        .expect("Failed to load `DummyView` using the view APIs");

    assert_eq!(initial_view.one.get(), first_view.one.get());
    assert_eq!(initial_view.two.get(), first_view.two.get());
    assert_eq!(initial_view.three.get(), first_view.three.get());

    let second_view = DummyView::load(ViewStorageContext::default())
        .blocking_wait()
        .expect("Failed to load `DummyView` using the view APIs");

    assert_eq!(initial_view.one.get(), second_view.one.get());
    assert_eq!(initial_view.two.get(), second_view.two.get());
    assert_eq!(initial_view.three.get(), second_view.three.get());
}

/// Test if key prefix search works in the mocked key-value store.
#[webassembly_test]
fn mock_find_keys() {
    let store = test::mock_key_value_store();
    let mut initial_view = DummyView::load(store)
        .blocking_wait()
        .expect("Failed to initialize `DummyView` with the mock key value store");

    let keys = [32, 36, 40, 44];

    for &key in &keys {
        initial_view
            .map
            .insert(&key, -(key as i8))
            .expect("Failed to insert value into dumy map view");
    }

    initial_view
        .save()
        .blocking_wait()
        .expect("Failed to persist view state");

    let test_view = DummyView::load(ViewStorageContext::default())
        .blocking_wait()
        .expect("Failed to load `DummyView` using the view APIs");

    let found_keys = test_view
        .map
        .indices()
        .blocking_wait()
        .expect("Failed to load keys of dummy map view");

    assert_eq!(found_keys, keys);
}

/// Test if key prefix search works in the mocked key-value store.
#[webassembly_test]
fn mock_find_key_value_pairs() {
    let store = test::mock_key_value_store();
    let mut initial_view = DummyView::load(store)
        .blocking_wait()
        .expect("Failed to initialize `DummyView` with the mock key value store");

    let keys = [32, 36, 40, 44];
    let mut expected_pairs = Vec::new();

    for &key in &keys {
        let value = -(key as i8);

        initial_view
            .map
            .insert(&key, value)
            .expect("Failed to insert value into dumy map view");

        expected_pairs.push((key, value));
    }

    initial_view
        .save()
        .blocking_wait()
        .expect("Failed to persist view state");

    let test_view = DummyView::load(ViewStorageContext::default())
        .blocking_wait()
        .expect("Failed to load `DummyView` using the view APIs");

    let mut pairs = Vec::new();

    test_view
        .map
        .for_each_index_value(|key, value| {
            pairs.push((key, value));
            Ok(())
        })
        .blocking_wait()
        .expect("Failed to load key value pairs of dummy map view");

    assert_eq!(pairs, expected_pairs);
}

/// Test the write operations of the key-value store.
#[webassembly_test]
fn mock_write_batch() {
    let store = test::mock_key_value_store();
    let mut initial_view = DummyView::load(store.clone())
        .blocking_wait()
        .expect("Failed to initialize `DummyView` with the mock key value store");

    let keys = [17, 23, 31, 37];
    let mut expected_pairs = Vec::new();

    for &key in &keys {
        let value = -(key as i8);

        initial_view
            .map
            .insert(&key, value)
            .expect("Failed to insert value into dumy map view");

        expected_pairs.push((key, value));
    }

    initial_view.one.set(1);
    initial_view.two.set(2);
    initial_view
        .two
        .hash()
        .blocking_wait()
        .expect("Failed to calculate the hash of a `RegisterView`");
    initial_view.three.set(3);
    initial_view
        .save()
        .blocking_wait()
        .expect("Failed to persist view state");

    let mut altered_view = DummyView::load(store.clone())
        .blocking_wait()
        .expect("Failed to initialize `DummyView` with the mock key value store");

    altered_view.one.set(100);
    altered_view.two.clear();
    altered_view.map.clear();

    altered_view
        .save()
        .blocking_wait()
        .expect("Failed to store key value pairs of dummy map view");

    let loaded_view = DummyView::load(store)
        .blocking_wait()
        .expect("Failed to reload `DummyView` from the mock key value store");

    let loaded_keys = loaded_view
        .map
        .indices()
        .blocking_wait()
        .expect("Failed to load keys of dummy map view");

    assert_eq!(loaded_view.one.get(), altered_view.one.get());
    assert_eq!(loaded_view.two.get(), altered_view.two.get());
    assert_eq!(loaded_view.three.get(), initial_view.three.get());
    assert!(loaded_keys.is_empty());
}

static mut INTERCEPTED_APPLICATION_ID: Option<ApplicationId> = None;
static mut INTERCEPTED_ARGUMENT: Option<Vec<u8>> = None;

/// Test mocking queries.
#[webassembly_test]
fn mock_query() {
    let response = vec![0xff, 0xfe, 0xfd];
    let expected_response = response.clone();

    test::mock_try_query_application(move |application_id, query| {
        assert_eq!(query, b"[17,23,31,37]");
        unsafe {
            INTERCEPTED_APPLICATION_ID = Some(application_id);
            INTERCEPTED_ARGUMENT = Some(query);
        }

        serde_json::to_vec(&response).expect("Failed to serialize query response")
    });

    let application_id = ApplicationId {
        bytecode_id: BytecodeId::new(MessageId {
            chain_id: ChainId([0, 1, 2, 3].into()),
            height: BlockHeight::from(4),
            index: 5,
        }),
        creation: MessageId {
            chain_id: ChainId([6, 7, 8, 9].into()),
            height: BlockHeight::from(10),
            index: 11,
        },
    };
    let query = vec![17, 23, 31, 37];
    let expected_query = serde_json::to_vec(&query).expect("Failed to serialize query");

    let response = test_service_runtime::<TestApp>()
        .query_application(application_id.with_abi::<TestApp>(), &query);

    assert_eq!(
        unsafe { INTERCEPTED_APPLICATION_ID.take() },
        Some(application_id)
    );
    assert_eq!(unsafe { INTERCEPTED_ARGUMENT.take() }, Some(expected_query));

    assert_eq!(response, expected_response);
}

/// An application ABI to use in the tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Abi;

impl ContractAbi for Abi {
    type Operation = Vec<u8>;
    type Response = Vec<u8>;
}

impl ServiceAbi for Abi {
    type Query = Vec<u8>;
    type QueryResponse = Vec<u8>;
}

pub struct TestApp {
    state: (),
}

impl WithContractAbi for TestApp {
    type Abi = Abi;
}

impl WithServiceAbi for TestApp {
    type Abi = Abi;
}

#[async_trait]
impl Contract for TestApp {
    type Error = TestAppError;
    type Storage = SimpleStateStorage<Self>;
    type State = ();
    type Message = Vec<u8>;
    type Parameters = Vec<u8>;
    type InstantiationArgument = Vec<u8>;

    async fn new(state: (), _runtime: ContractRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(TestApp { state })
    }

    fn state_mut(&mut self) -> &mut Self::State {
        &mut self.state
    }

    async fn instantiate(
        &mut self,
        _argument: Self::InstantiationArgument,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn execute_operation(
        &mut self,
        _operation: Self::Operation,
    ) -> Result<Self::Response, Self::Error> {
        Ok(vec![])
    }

    async fn execute_message(&mut self, _message: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Service for TestApp {
    type Error = TestAppError;
    type Storage = SimpleStateStorage<Self>;
    type State = ();
    type Parameters = Vec<u8>;

    async fn new(state: (), _runtime: ServiceRuntime<Self>) -> Result<Self, Self::Error> {
        Ok(TestApp { state })
    }

    async fn handle_query(&self, _query: Self::Query) -> Result<Self::QueryResponse, Self::Error> {
        Ok(vec![])
    }
}

#[derive(Debug, Error)]
pub enum TestAppError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Bcs(#[from] bcs::Error),
}
