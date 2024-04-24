// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm specific client tests.
//!
//! These tests only run if a Wasm runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

// Tests for `RocksDb`, `DynamoDb`, `ScyllaDb` and `Service` are currently disabled
// because they are slow and their behavior appears to be correctly check by the
// test with memory.

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use std::collections::BTreeMap;

use assert_matches::assert_matches;
use async_graphql::Request;
use counter::CounterAbi;
use linera_base::{
    data_types::Amount,
    identifiers::{AccountOwner, ApplicationId, ChainDescription, ChainId, Destination, Owner},
};
use linera_chain::data_types::{CertificateValue, MessageAction, OutgoingMessage};
use linera_execution::{
    Bytecode, Message, MessageKind, Operation, ResourceControlPolicy, SystemMessage,
    UserApplicationDescription, WasmRuntime,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde_json::json;
use test_case::test_case;

#[cfg(feature = "dynamodb")]
use crate::client::client_tests::DynamoDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::client::client_tests::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::client::client_tests::ScyllaDbStorageBuilder;
use crate::client::client_tests::{
    MemoryStorageBuilder, ServiceStorageBuilder, StorageBuilder, TestBuilder,
};

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_service_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime).await).await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_rocks_db_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await).await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dynamo_db_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(DynamoDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_scylla_db_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(ScyllaDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_create_application<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let mut publisher = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::from_tokens(3))
        .await?;
    let mut creator = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::ONE)
        .await?;

    let cert = creator
        .subscribe_to_published_bytecodes(publisher.chain_id)
        .await
        .unwrap()
        .unwrap();
    publisher.receive_certificate(cert).await.unwrap();
    publisher.process_inbox().await.unwrap();

    let (contract_path, service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("counter")?;
    let (bytecode_id, cert) = publisher
        .publish_bytecode(
            Bytecode::load_from_file(contract_path).await?,
            Bytecode::load_from_file(service_path).await?,
        )
        .await
        .unwrap()
        .unwrap();
    let bytecode_id = bytecode_id.with_abi::<counter::CounterAbi, (), u64>();
    // Receive our own cert to broadcast the bytecode location.
    publisher.receive_certificate(cert).await.unwrap();
    publisher.process_inbox().await.unwrap();

    creator.synchronize_from_validators().await.unwrap();
    creator.process_inbox().await.unwrap();

    // No fuel was used so far, but some storage for messages and operations in three blocks.
    let balance_after_messaging = creator.local_balance().await?;
    assert!(balance_after_messaging < Amount::ONE);

    let initial_value = 10_u64;
    let (application_id, _) = creator
        .create_application(bytecode_id, &(), &initial_value, vec![])
        .await
        .unwrap()
        .unwrap();

    let increment = 5_u64;
    creator
        .execute_operation(Operation::user(application_id, &increment)?)
        .await
        .unwrap();

    let query = Request::new("{ value }");
    let response = creator
        .query_user_application(application_id, &query)
        .await
        .unwrap();

    let expected = async_graphql::Response::new(
        async_graphql::Value::from_json(json!({"value": 15})).unwrap(),
    );

    assert_eq!(expected, response);
    // Creating the application used fuel because of the `instantiate` call.
    let balance_after_init = creator.local_balance().await?;
    assert!(balance_after_init < balance_after_messaging);

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_run_application_with_dependency(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime))
        .await
}

#[ignore]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_service_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_run_application_with_dependency(
        ServiceStorageBuilder::with_wasm_runtime(wasm_runtime).await,
    )
    .await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_rocks_db_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_run_application_with_dependency(
        RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await,
    )
    .await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dynamo_db_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_run_application_with_dependency(DynamoDbStorageBuilder::with_wasm_runtime(
        wasm_runtime,
    ))
    .await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_scylla_db_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_run_application_with_dependency(ScyllaDbStorageBuilder::with_wasm_runtime(
        wasm_runtime,
    ))
    .await
}

async fn run_test_run_application_with_dependency<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    // Will publish the bytecodes.
    let mut publisher = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::from_tokens(3))
        .await?;
    // Will create the apps and use them to send a message.
    let mut creator = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::ONE)
        .await?;
    // Will receive the message.
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(2), Amount::ONE)
        .await?;
    let receiver_id = ChainId::root(2);

    let cert = creator
        .subscribe_to_published_bytecodes(publisher.chain_id)
        .await
        .unwrap()
        .unwrap();
    publisher.receive_certificate(cert).await.unwrap();
    publisher.process_inbox().await.unwrap();

    let (bytecode_id1, cert1) = {
        let (contract_path, service_path) =
            linera_execution::wasm_test::get_example_bytecode_paths("counter")?;
        publisher
            .publish_bytecode(
                Bytecode::load_from_file(contract_path).await?,
                Bytecode::load_from_file(service_path).await?,
            )
            .await
            .unwrap()
            .unwrap()
    };
    let bytecode_id1 = bytecode_id1.with_abi::<counter::CounterAbi, (), u64>();
    let (bytecode_id2, cert2) = {
        let (contract_path, service_path) =
            linera_execution::wasm_test::get_example_bytecode_paths("meta_counter")?;
        publisher
            .publish_bytecode(
                Bytecode::load_from_file(contract_path).await?,
                Bytecode::load_from_file(service_path).await?,
            )
            .await
            .unwrap()
            .unwrap()
    };
    let bytecode_id2 =
        bytecode_id2.with_abi::<meta_counter::MetaCounterAbi, ApplicationId<CounterAbi>, ()>();
    // Receive our own certs to broadcast the bytecode locations.
    publisher.receive_certificate(cert1).await.unwrap();
    publisher.receive_certificate(cert2).await.unwrap();
    publisher.process_inbox().await.unwrap();

    // Creator receives the bytecodes then creates the app.
    creator.synchronize_from_validators().await.unwrap();
    creator.process_inbox().await.unwrap();
    let initial_value = 10_u64;
    let (application_id1, _) = creator
        .create_application(bytecode_id1, &(), &initial_value, vec![])
        .await
        .unwrap()
        .unwrap();
    let (application_id2, _) = creator
        .create_application(
            bytecode_id2,
            &application_id1,
            &(),
            vec![application_id1.forget_abi()],
        )
        .await
        .unwrap()
        .unwrap();

    let mut operation = meta_counter::Operation::increment(receiver_id, 5);
    operation.fuel_grant = 1000000;
    let cert = creator
        .execute_operation(Operation::user(application_id2, &operation)?)
        .await
        .unwrap()
        .unwrap();

    receiver.receive_certificate(cert).await.unwrap();
    receiver.process_inbox().await.unwrap();

    let query = Request::new("{ value }");
    let response = receiver
        .query_user_application(application_id2, &query)
        .await
        .unwrap();

    let expected =
        async_graphql::Response::new(async_graphql::Value::from_json(json!({"value": 5})).unwrap());

    assert_eq!(expected, response);

    // Try again with a value that will make the (untracked) message fail.
    let operation = meta_counter::Operation::fail(receiver_id);
    let cert = creator
        .execute_operation(Operation::user(application_id2, &operation)?)
        .await
        .unwrap()
        .unwrap();

    receiver.receive_certificate(cert).await.unwrap();
    let mut certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let cert = certs.pop().unwrap();
    let incoming_messages = &cert.value().block().unwrap().incoming_messages;
    assert_eq!(incoming_messages.len(), 1);
    assert_eq!(incoming_messages[0].action, MessageAction::Reject);
    assert_eq!(incoming_messages[0].event.kind, MessageKind::Simple);
    let messages = cert.value().messages().unwrap();
    assert_eq!(messages.len(), 0);

    // Try again with a value that will make the (tracked) message fail.
    let mut operation = meta_counter::Operation::fail(receiver_id);
    operation.is_tracked = true;
    let cert = creator
        .execute_operation(Operation::user(application_id2, &operation)?)
        .await
        .unwrap()
        .unwrap();

    receiver.receive_certificate(cert).await.unwrap();
    let mut certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let cert = certs.pop().unwrap();
    let incoming_messages = &cert.value().block().unwrap().incoming_messages;
    assert_eq!(incoming_messages.len(), 1);
    assert_eq!(incoming_messages[0].action, MessageAction::Reject);
    assert_eq!(incoming_messages[0].event.kind, MessageKind::Tracked);
    let messages = cert.value().messages().unwrap();
    assert_eq!(messages.len(), 1);

    // The bounced message is marked as "bouncing" in the Wasm context and succeeds.
    creator.receive_certificate(cert).await.unwrap();
    let mut certs = creator.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let cert = certs.pop().unwrap();
    let incoming_messages = &cert.value().block().unwrap().incoming_messages;
    assert_eq!(incoming_messages.len(), 2);
    // First message is the grant refund for the successful message sent before.
    assert_eq!(incoming_messages[0].action, MessageAction::Accept);
    assert_eq!(incoming_messages[0].event.kind, MessageKind::Tracked);
    assert_matches!(
        incoming_messages[0].event.message,
        Message::System(SystemMessage::Credit { .. })
    );
    // Second message is the bounced message.
    assert_eq!(incoming_messages[1].action, MessageAction::Accept);
    assert_eq!(incoming_messages[1].event.kind, MessageKind::Bouncing);
    assert_matches!(incoming_messages[1].event.message, Message::User { .. });

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_service_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime).await).await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocks_db_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await).await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(DynamoDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_scylla_db_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(ScyllaDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_cross_chain_message<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::from_tokens(3))
        .await?;
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::ONE)
        .await?;

    let (bytecode_id, pub_cert) = {
        let bytecode_name = "fungible";
        let (contract_path, service_path) =
            linera_execution::wasm_test::get_example_bytecode_paths(bytecode_name)?;
        sender
            .publish_bytecode(
                Bytecode::load_from_file(contract_path).await?,
                Bytecode::load_from_file(service_path).await?,
            )
            .await
            .unwrap()
            .unwrap()
    };
    let bytecode_id = bytecode_id
        .with_abi::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>();

    // Receive our own cert to broadcast the bytecode location.
    sender.receive_certificate(pub_cert.clone()).await.unwrap();
    sender.process_inbox().await.unwrap();

    let sender_owner = AccountOwner::User(Owner::from(sender.key_pair().await?.public()));
    let receiver_owner = AccountOwner::User(Owner::from(receiver.key_pair().await?.public()));

    let accounts = BTreeMap::from_iter([(sender_owner, Amount::from_tokens(1_000_000))]);
    let state = fungible::InitialState { accounts };
    let params = fungible::Parameters::new("FUN");
    let (application_id, _cert) = sender
        .create_application(bytecode_id, &params, &state, vec![])
        .await
        .unwrap()
        .unwrap();

    // Make a transfer using the fungible app.
    let transfer = fungible::Operation::Transfer {
        owner: sender_owner,
        amount: 100.into(),
        target_account: fungible::Account {
            chain_id: receiver.chain_id(),
            owner: receiver_owner,
        },
    };
    let cert = sender
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .unwrap()
        .unwrap();

    let messages = cert.value().messages().unwrap();
    {
        let OutgoingMessage {
            destination,
            message,
            ..
        } = &messages[0];
        assert_matches!(
            message, Message::System(SystemMessage::RegisterApplications { applications })
            if applications.len() == 1 && matches!(
                applications[0], UserApplicationDescription{ bytecode_id: b_id, .. }
                if b_id == bytecode_id.forget_abi()
            ),
            "Unexpected message"
        );
        assert_eq!(*destination, Destination::Recipient(receiver.chain_id()));
    }
    receiver.synchronize_from_validators().await.unwrap();
    receiver.receive_certificate(cert).await.unwrap();
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let messages = match certs[0].value() {
        CertificateValue::ConfirmedBlock { executed_block, .. } => {
            &executed_block.block.incoming_messages
        }
        _ => panic!("Unexpected value"),
    };
    assert!(messages.iter().any(|msg| matches!(
        &msg.event.message,
        Message::System(SystemMessage::RegisterApplications { applications })
        if applications.iter().any(|app| app.bytecode_location.certificate_hash == pub_cert.hash())
    )));
    assert!(messages
        .iter()
        .any(|msg| matches!(&msg.event.message, Message::User { .. })));

    // Make another transfer.
    let transfer = fungible::Operation::Transfer {
        owner: sender_owner,
        amount: 200.into(),
        target_account: fungible::Account {
            chain_id: receiver.chain_id(),
            owner: receiver_owner,
        },
    };
    let cert = sender
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .unwrap()
        .unwrap();

    receiver.receive_certificate(cert).await.unwrap();
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let messages = match certs[0].value() {
        CertificateValue::ConfirmedBlock { executed_block, .. } => {
            &executed_block.block.incoming_messages
        }
        _ => panic!("Unexpected value"),
    };
    // The new block should _not_ contain another `RegisterApplications` message, because the
    // application is already registered.
    assert!(!messages.iter().any(|msg| matches!(
        &msg.event.message,
        Message::System(SystemMessage::RegisterApplications { applications })
        if applications.iter().any(|app| app.bytecode_location.certificate_hash == pub_cert.hash())
    )));
    assert!(messages
        .iter()
        .any(|msg| matches!(&msg.event.message, Message::User { .. })));

    // Try another transfer in the other direction except that the amount is too large.
    let transfer = fungible::Operation::Transfer {
        owner: receiver_owner,
        amount: 301.into(),
        target_account: fungible::Account {
            chain_id: sender.chain_id(),
            owner: sender_owner,
        },
    };
    assert!(receiver
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .is_err());
    receiver.clear_pending_block();

    // Try another transfer in the other direction with the correct amount.
    let transfer = fungible::Operation::Transfer {
        owner: receiver_owner,
        amount: 300.into(),
        target_account: fungible::Account {
            chain_id: sender.chain_id(),
            owner: sender_owner,
        },
    };
    receiver
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .unwrap();

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_user_pub_sub_channels(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_user_pub_sub_channels(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_service_user_pub_sub_channels(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_user_pub_sub_channels(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime).await)
        .await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocks_db_user_pub_sub_channels(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_user_pub_sub_channels(RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await)
        .await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_user_pub_sub_channels(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_user_pub_sub_channels(DynamoDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_scylla_db_user_pub_sub_channels(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_user_pub_sub_channels(ScyllaDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_user_pub_sub_channels<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
    ViewError: From<<B::Storage as Storage>::ContextError>,
{
    let mut builder = TestBuilder::new(storage_builder, 4, 1)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(0), Amount::ONE)
        .await?;
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(1), Amount::ONE)
        .await?;

    let (bytecode_id, pub_cert) = {
        let (contract_path, service_path) =
            linera_execution::wasm_test::get_example_bytecode_paths("social")?;
        receiver
            .publish_bytecode(
                Bytecode::load_from_file(contract_path).await?,
                Bytecode::load_from_file(service_path).await?,
            )
            .await
            .unwrap()
            .unwrap()
    };
    let bytecode_id = bytecode_id.with_abi::<social::SocialAbi, (), ()>();

    // Receive our own cert to broadcast the bytecode location.
    receiver
        .receive_certificate(pub_cert.clone())
        .await
        .unwrap();
    receiver.process_inbox().await.unwrap();

    let (application_id, _cert) = receiver
        .create_application(bytecode_id, &(), &(), vec![])
        .await
        .unwrap()
        .unwrap();

    // Request to subscribe to the sender.
    let request_subscribe = social::Operation::Subscribe {
        chain_id: sender.chain_id(),
    };
    let cert = receiver
        .execute_operation(Operation::user(application_id, &request_subscribe)?)
        .await
        .unwrap()
        .unwrap();

    // Subscribe the receiver. This also registers the application.
    sender.synchronize_from_validators().await.unwrap();
    sender.receive_certificate(cert).await.unwrap();
    let _certs = sender.process_inbox().await.unwrap();

    // Make a post.
    let text = "Please like and subscribe! No, wait, like isn't supported yet.".to_string();
    let post = social::Operation::Post { text: text.clone() };
    let cert = sender
        .execute_operation(Operation::user(application_id, &post)?)
        .await
        .unwrap()
        .unwrap();

    receiver.receive_certificate(cert.clone()).await.unwrap();
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);

    // There should be a message receiving the new post.
    let messages = match certs[0].value() {
        CertificateValue::ConfirmedBlock { executed_block, .. } => {
            &executed_block.block.incoming_messages
        }
        _ => panic!("Unexpected value"),
    };
    assert!(messages
        .iter()
        .any(|msg| matches!(&msg.event.message, Message::User { .. })));

    let query = async_graphql::Request::new("{ receivedPosts { keys { author, index } } }");
    let posts = receiver
        .query_user_application(application_id, &query)
        .await?;
    let expected = async_graphql::Response::new(
        async_graphql::Value::from_json(json!({
            "receivedPosts": {
                "keys": [
                    { "author": sender.chain_id, "index": 0 }
                ]
            }
        }))
        .unwrap(),
    );
    assert_eq!(posts, expected);

    // Request to unsubscribe from the sender.
    let request_unsubscribe = social::Operation::Unsubscribe {
        chain_id: sender.chain_id(),
    };
    let cert = receiver
        .execute_operation(Operation::user(application_id, &request_unsubscribe)?)
        .await
        .unwrap()
        .unwrap();

    // Unsubscribe the receiver.
    sender.synchronize_from_validators().await.unwrap();
    sender.receive_certificate(cert).await.unwrap();
    let _certs = sender.process_inbox().await.unwrap();

    // Make a post.
    let post = social::Operation::Post {
        text: "Nobody will read this!".to_string(),
    };
    let cert = sender
        .execute_operation(Operation::user(application_id, &post)?)
        .await
        .unwrap()
        .unwrap();

    // The post will not be received by the unsubscribed chain.
    receiver.receive_certificate(cert).await.unwrap();
    let certs = receiver.process_inbox().await.unwrap().0;
    assert!(certs.is_empty());

    // There is still only one post it can see.
    let query = async_graphql::Request::new("{ receivedPosts { keys { author, index } } }");
    let posts = receiver
        .query_user_application(application_id, &query)
        .await
        .unwrap();
    let expected = async_graphql::Response::new(
        async_graphql::Value::from_json(json!({
            "receivedPosts": {
                "keys": [ { "author": sender.chain_id, "index": 0 } ]
            }
        }))
        .unwrap(),
    );
    assert_eq!(posts, expected);

    Ok(())
}
