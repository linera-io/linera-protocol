// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests that should be executed inside a WebAssembly environment.
//!
//! Includes tests for the mocked system APIs.

#![cfg(test)]
#![cfg(target_arch = "wasm32")]

use linera_sdk::{
    base::{Amount, ApplicationId, BlockHeight, BytecodeId, ChainId, MessageId, Timestamp},
    contract, service, test,
    util::BlockingWait,
    ContractLogger, ServiceLogger,
};
use linera_views::{
    map_view::MapView,
    register_view::RegisterView,
    views::{HashableView, RootView, View},
};
use webassembly_test::webassembly_test;

/// Test if the chain ID getter API is mocked successfully.
#[webassembly_test]
fn mock_chain_id() {
    let chain_id = ChainId([0, 1, 2, 3].into());

    test::mock_chain_id(chain_id);

    assert_eq!(contract::system_api::current_chain_id(), chain_id);
    assert_eq!(service::system_api::current_chain_id(), chain_id);
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
        contract::system_api::current_application_id(),
        application_id
    );
    assert_eq!(
        service::system_api::current_application_id(),
        application_id
    );
}

/// Test if the application parameters getter API is mocked successfully.
#[webassembly_test]
fn mock_application_parameters() {
    let parameters = vec![0, 1, 2, 3, 4, 5, 6];

    test::mock_application_parameters(&parameters);

    let serialized_parameters =
        serde_json::to_vec(&parameters).expect("Failed to serialize parameters");

    assert_eq!(
        contract::system_api::private::current_application_parameters(),
        serialized_parameters
    );
    assert_eq!(
        service::system_api::private::current_application_parameters(),
        serialized_parameters
    );
}

/// Test if the system balance getter API is mocked successfully.
#[webassembly_test]
fn mock_system_balance() {
    let balance = Amount::from_atto(0x00010203_04050607_08090a0b_0c0d0e0f);

    test::mock_system_balance(balance);

    assert_eq!(contract::system_api::current_system_balance(), balance);
    assert_eq!(service::system_api::current_system_balance(), balance);
}

/// Test if the system timestamp getter API is mocked successfully.
#[webassembly_test]
fn mock_system_timestamp() {
    let timestamp = Timestamp::from(0x00010203_04050607);

    test::mock_system_timestamp(timestamp);

    assert_eq!(contract::system_api::current_system_time(), timestamp);
    assert_eq!(service::system_api::current_system_time(), timestamp);
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

    let contract_view = contract::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    assert_eq!(initial_view.one.get(), contract_view.one.get());
    assert_eq!(initial_view.two.get(), contract_view.two.get());
    assert_eq!(initial_view.three.get(), contract_view.three.get());

    let service_view = service::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    assert_eq!(initial_view.one.get(), service_view.one.get());
    assert_eq!(initial_view.two.get(), service_view.two.get());
    assert_eq!(initial_view.three.get(), service_view.three.get());
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

    let contract_view = contract::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    let contract_keys = contract_view
        .map
        .indices()
        .blocking_wait()
        .expect("Failed to load keys of dummy map view");

    assert_eq!(contract_keys, keys);

    let service_view = service::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    let service_keys = service_view
        .map
        .indices()
        .blocking_wait()
        .expect("Failed to load keys of dummy map view");

    assert_eq!(service_keys, keys);
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

    let contract_view = contract::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    let mut contract_pairs = Vec::new();

    contract_view
        .map
        .for_each_index_value(|key, value| {
            contract_pairs.push((key, value));
            Ok(())
        })
        .blocking_wait()
        .expect("Failed to load key value pairs of dummy map view");

    assert_eq!(contract_pairs, expected_pairs);

    let service_view = service::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    let mut service_pairs = Vec::new();

    service_view
        .map
        .for_each_index_value(|key, value| {
            service_pairs.push((key, value));
            Ok(())
        })
        .blocking_wait()
        .expect("Failed to load key value pairs of dummy map view");

    assert_eq!(service_pairs, expected_pairs);
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

    let mut altered_view =
        contract::system_api::private::load_view::<DummyView<_>>().blocking_wait();

    altered_view.one.set(100);
    altered_view.two.clear();
    altered_view.map.clear();

    altered_view
        .save()
        .blocking_wait()
        .expect("Failed to store key value pairs of dummy map view");

    let loaded_view = DummyView::load(store)
        .blocking_wait()
        .expect("Failed to initialize `DummyView` with the mock key value store");

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
        unsafe {
            INTERCEPTED_APPLICATION_ID = Some(application_id);
            INTERCEPTED_ARGUMENT = Some(query);
        }

        response.clone()
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

    let response = service::system_api::private::query_application(application_id, &query);

    assert_eq!(
        unsafe { INTERCEPTED_APPLICATION_ID.take() },
        Some(application_id)
    );
    assert_eq!(unsafe { INTERCEPTED_ARGUMENT.take() }, Some(query));

    assert_eq!(response, expected_response);
}
