// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! WASM specific client tests.
//!
//! These tests only run if a WASM runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use crate::client::client_tests::{
    MakeMemoryStoreClient, MakeRocksdbStoreClient, StoreBuilder, TestBuilder, ROCKSDB_SEMAPHORE,
};
use fungible::{AccountOwner, SignedTransfer, SignedTransferPayload, Transfer};
use linera_base::data_types::*;
use linera_chain::data_types::OutgoingEffect;
use linera_execution::{
    system::Balance, ApplicationId, Bytecode, Destination, Effect, Operation, Query, Response,
    SystemEffect, UserApplicationDescription, WasmRuntime,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, iter};
use test_case::test_case;

#[cfg(feature = "aws")]
use crate::client::client_tests::MakeDynamoDbStoreClient;

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_create_application(wasm_runtime: WasmRuntime) -> Result<(), anyhow::Error> {
    run_test_create_application(MakeMemoryStoreClient::with_wasm_runtime(wasm_runtime)).await
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocksdb_create_application(wasm_runtime: WasmRuntime) -> Result<(), anyhow::Error> {
    let _lock = ROCKSDB_SEMAPHORE.acquire().await;
    run_test_create_application(MakeRocksdbStoreClient::with_wasm_runtime(wasm_runtime)).await
}

#[cfg(feature = "aws")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_create_application(wasm_runtime: WasmRuntime) -> Result<(), anyhow::Error> {
    run_test_create_application(MakeDynamoDbStoreClient::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_create_application<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut publisher = builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(3))
        .await?;
    let mut creator = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(0))
        .await?;

    let cert = creator
        .subscribe_to_published_bytecodes(publisher.chain_id)
        .await
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
        .unwrap();
    // Receive our own cert to broadcast the bytecode location.
    publisher.receive_certificate(cert).await.unwrap();
    publisher.process_inbox().await.unwrap();

    creator.synchronize_and_recompute_balance().await.unwrap();
    creator.process_inbox().await.unwrap();

    let initial_value = 10_u128;
    let initial_value_bytes = bcs::to_bytes(&initial_value)?;
    let (application_id, _) = creator
        .create_application(bytecode_id, vec![], initial_value_bytes, vec![])
        .await
        .unwrap();

    let increment = 5_u128;
    let user_operation = bcs::to_bytes(&increment)?;
    creator
        .execute_operation(
            ApplicationId::User(application_id),
            Operation::User(user_operation),
        )
        .await
        .unwrap();
    let response = creator
        .query_application(ApplicationId::User(application_id), &Query::User(vec![]))
        .await
        .unwrap();

    let expected = 15_u128;
    let expected_bytes = bcs::to_bytes(&expected)?;
    assert!(matches!(response, Response::User(bytes) if bytes == expected_bytes));
    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    run_test_run_application_with_dependency(MakeMemoryStoreClient::with_wasm_runtime(wasm_runtime))
        .await
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocksdb_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    let _lock = ROCKSDB_SEMAPHORE.acquire().await;
    run_test_run_application_with_dependency(MakeRocksdbStoreClient::with_wasm_runtime(
        wasm_runtime,
    ))
    .await
}

#[cfg(feature = "aws")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    run_test_run_application_with_dependency(MakeDynamoDbStoreClient::with_wasm_runtime(
        wasm_runtime,
    ))
    .await
}

async fn run_test_run_application_with_dependency<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    // Will publish the bytecodes.
    let mut publisher = builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(3))
        .await?;
    // Will create the apps and use them to send a message.
    let mut creator = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(0))
        .await?;
    // Will receive the message.
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(2), Balance::from(0))
        .await?;
    let receiver_id = ChainId::root(2);

    let cert = creator
        .subscribe_to_published_bytecodes(publisher.chain_id)
        .await
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
    };
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
    };
    // Receive our own certs to broadcast the bytecode locations.
    publisher.receive_certificate(cert1).await.unwrap();
    publisher.receive_certificate(cert2).await.unwrap();
    publisher.process_inbox().await.unwrap();

    // Creator receives the bytecodes then creates the app.
    creator.synchronize_and_recompute_balance().await.unwrap();
    creator.process_inbox().await.unwrap();
    let initial_value = 10_u128;
    let (application_id1, _) = creator
        .create_application(bytecode_id1, vec![], bcs::to_bytes(&initial_value)?, vec![])
        .await
        .unwrap();
    let (application_id2, _) = creator
        .create_application(
            bytecode_id2,
            bcs::to_bytes(&application_id1)?,
            vec![],
            vec![application_id1],
        )
        .await
        .unwrap();

    let increment = 5_u128;
    let user_operation = bcs::to_bytes(&(receiver_id, increment))?;
    let cert = creator
        .execute_operation(
            ApplicationId::User(application_id2),
            Operation::User(user_operation),
        )
        .await
        .unwrap();

    receiver.receive_certificate(cert).await.unwrap();
    receiver.process_inbox().await.unwrap();
    let response = receiver
        .query_application(ApplicationId::User(application_id2), &Query::User(vec![]))
        .await
        .unwrap();

    let expected = 5_u128;
    let expected_bytes = bcs::to_bytes(&expected)?;
    assert!(matches!(response, Response::User(bytes) if bytes == expected_bytes));
    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_cross_chain_message(wasm_runtime: WasmRuntime) -> Result<(), anyhow::Error> {
    run_test_cross_chain_message(MakeMemoryStoreClient::with_wasm_runtime(wasm_runtime)).await
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocksdb_cross_chain_message(wasm_runtime: WasmRuntime) -> Result<(), anyhow::Error> {
    let _lock = ROCKSDB_SEMAPHORE.acquire().await;
    run_test_cross_chain_message(MakeRocksdbStoreClient::with_wasm_runtime(wasm_runtime)).await
}

#[cfg(feature = "aws")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_cross_chain_message(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    run_test_cross_chain_message(MakeDynamoDbStoreClient::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_cross_chain_message<B>(store_builder: B) -> Result<(), anyhow::Error>
where
    B: StoreBuilder,
    ViewError: From<<B::Store as Store>::ContextError>,
{
    fn convert<S: ?Sized + Serialize, T: DeserializeOwned>(s: &S) -> bcs::Result<T> {
        bcs::from_bytes(&bcs::to_bytes(s)?)
    }

    let mut builder = TestBuilder::new(store_builder, 4, 1).await?;
    let mut sender = builder
        .add_initial_chain(ChainDescription::Root(0), Balance::from(3))
        .await?;
    let mut receiver = builder
        .add_initial_chain(ChainDescription::Root(1), Balance::from(0))
        .await?;

    let (contract_path, service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("fungible")?;
    let (bytecode_id, pub_cert) = sender
        .publish_bytecode(
            Bytecode::load_from_file(contract_path).await?,
            Bytecode::load_from_file(service_path).await?,
        )
        .await?;

    // Receive our own cert to broadcast the bytecode location.
    sender.receive_certificate(pub_cert.clone()).await.unwrap();
    sender.process_inbox().await.unwrap();

    let sender_kp = linera_base::crypto::KeyPair::generate();
    let receiver_kp = linera_base::crypto::KeyPair::generate();

    let accounts: BTreeMap<AccountOwner, u128> =
        iter::once((AccountOwner::Key(convert(&sender_kp.public())?), 1_000_000)).collect();
    let initial_value_bytes = bcs::to_bytes(&accounts)?;
    let (application_id, _cert) = sender
        .create_application(bytecode_id, vec![], initial_value_bytes, vec![])
        .await?;

    // Make a transfer using the fungible app.
    let mut payload = SignedTransferPayload {
        token_id: convert(&application_id)?,
        source_chain: convert(&sender.chain_id())?,
        nonce: Default::default(),
        transfer: Transfer {
            destination_account: AccountOwner::Key(convert(&receiver_kp.public())?),
            destination_chain: convert(&receiver.chain_id())?,
            amount: 100,
        },
    };
    let transfer = SignedTransfer {
        source: convert(&sender_kp.public())?,
        signature: convert(&linera_base::crypto::Signature::new(&payload, &sender_kp))?,
        payload: payload.clone(),
    };
    let cert = sender
        .execute_operation(
            ApplicationId::User(application_id),
            Operation::User(bcs::to_bytes(&transfer)?),
        )
        .await?;

    assert!(cert
        .value
        .effects()
        .iter()
        .any(|OutgoingEffect { application_id, destination, effect, .. }| {
            matches!(
                effect,
                Effect::System(SystemEffect::RegisterApplications { applications })
                if matches!(applications[0], UserApplicationDescription{ bytecode_id: b_id, .. } if b_id == bytecode_id)
            ) && *destination == Destination::Recipient(receiver.chain_id())
                && matches!(application_id, ApplicationId::System)
        }));
    receiver.synchronize_and_recompute_balance().await.unwrap();
    receiver.receive_certificate(cert).await.unwrap();
    let certs = receiver.process_inbox().await.unwrap();
    assert_eq!(certs.len(), 1);
    let messages = &certs[0].value.block().incoming_messages;
    assert!(messages.iter().any(|msg| matches!(
        &msg.event.effect,
        Effect::System(SystemEffect::RegisterApplications { applications })
        if applications.iter().any(|app| app.bytecode_location.certificate_hash == pub_cert.value.hash())
    )));
    assert!(messages
        .iter()
        .any(|msg| matches!(&msg.event.effect, Effect::User(_))
            && msg.application_id == ApplicationId::User(application_id)));

    // Make another transfer.
    payload.nonce = payload.nonce.next().unwrap();
    payload.transfer.amount = 200;
    let transfer = SignedTransfer {
        source: convert(&sender_kp.public())?,
        signature: convert(&linera_base::crypto::Signature::new(&payload, &sender_kp))?,
        payload,
    };
    let cert = sender
        .execute_operation(
            ApplicationId::User(application_id),
            Operation::User(bcs::to_bytes(&transfer)?),
        )
        .await?;

    receiver.receive_certificate(cert).await?;
    let certs = receiver.process_inbox().await?;
    assert_eq!(certs.len(), 1);
    let messages = &certs[0].value.block().incoming_messages;
    // The new block should _not_ contain another `RegisterApplications` effect, because the
    // application is already registered.
    assert!(!messages.iter().any(|msg| matches!(
        &msg.event.effect,
        Effect::System(SystemEffect::RegisterApplications { applications })
        if applications.iter().any(|app| app.bytecode_location.certificate_hash == pub_cert.value.hash())
    )));
    assert!(messages
        .iter()
        .any(|msg| matches!(&msg.event.effect, Effect::User(_))
            && msg.application_id == ApplicationId::User(application_id)));
    Ok(())
}
