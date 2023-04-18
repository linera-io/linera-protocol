// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tests that should be executed inside a WebAssembly environment.
//!
//! Includes tests for the mocked system APIs.

#![cfg(test)]
#![cfg(target_arch = "wasm32")]

use futures::FutureExt;
use linera_sdk::{
    base::{ApplicationId, Balance, BlockHeight, BytecodeId, ChainId, EffectId, Timestamp},
    contract, service, test, ContractLogger, ServiceLogger,
};
use linera_views::{
    common::Context,
    register_view::RegisterView,
    views::{RootView, View},
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
        bytecode_id: BytecodeId(EffectId {
            chain_id: ChainId([0, 1, 2, 3].into()),
            height: BlockHeight::from(4),
            index: 5,
        }),
        creation: EffectId {
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

    test::mock_application_parameters(parameters.clone());

    assert_eq!(
        contract::system_api::current_application_parameters(),
        parameters
    );
    assert_eq!(
        service::system_api::current_application_parameters(),
        parameters
    );
}

/// Test if the system balance getter API is mocked successfully.
#[webassembly_test]
fn mock_system_balance() {
    let balance = Balance::from(0x00010203_04050607_08090a0b_0c0d0e0f);

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

/// Test loading a mocked application state without locking it.
#[webassembly_test]
fn mock_load_blob_state() {
    let state = vec![0, 1, 2, 3, 4, 5, 6];

    test::mock_application_state(
        bcs::to_bytes(&state).expect("Failed to serialize vector using BCS"),
    );

    assert_eq!(contract::system_api::load::<Vec<u8>>(), state);
    assert_eq!(service::system_api::load().now_or_never(), Some(state));
}

/// Test loading and locking a mocked application state.
#[webassembly_test]
fn mock_load_and_lock_blob_state() {
    let state = vec![0, 1, 2, 3, 4, 5, 6];

    test::mock_application_state(
        bcs::to_bytes(&state).expect("Failed to serialize vector using BCS"),
    );

    assert_eq!(
        contract::system_api::load_and_lock::<Vec<u8>>(),
        Some(state)
    );
}

/// A dummy view to test the key value store.
#[derive(RootView)]
struct DummyView<C> {
    one: RegisterView<C, u8>,
    two: RegisterView<C, u16>,
    three: RegisterView<C, u32>,
}

/// Test if views are loaded from a memory key-value store.
#[webassembly_test]
fn mock_load_view() {
    let store = test::mock_key_value_store();
    let mut initial_view = DummyView::load(store)
        .now_or_never()
        .expect("Memory key value store should always resolve immediately")
        .expect("Failed to initialize `DummyView` with the mock key value store");

    initial_view.one.set(1);
    initial_view.two.set(2);
    initial_view.three.set(3);
    initial_view
        .save()
        .now_or_never()
        .expect("Persisting a view to memory should be instantaneous")
        .expect("Failed to persist view state");

    let contract_view = contract::system_api::load_and_lock_view::<DummyView<_>>()
        .now_or_never()
        .expect("Memory key value store should always resolve immediately")
        .expect("Failed to lock view");

    assert_eq!(initial_view.one.get(), contract_view.one.get());
    assert_eq!(initial_view.two.get(), contract_view.two.get());
    assert_eq!(initial_view.three.get(), contract_view.three.get());

    let service_view = service::system_api::lock_and_load_view::<DummyView<_>>()
        .now_or_never()
        .expect("Memory key value store should always resolve immediately");

    assert_eq!(initial_view.one.get(), service_view.one.get());
    assert_eq!(initial_view.two.get(), service_view.two.get());
    assert_eq!(initial_view.three.get(), service_view.three.get());
}
