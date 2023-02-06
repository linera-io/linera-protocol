// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! WASM specific client tests.
//!
//! These tests only run if a WASM runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use crate::client::client_tests::{
    MakeDynamoDbStoreClient, MakeMemoryStoreClient, MakeRocksdbStoreClient, StoreBuilder,
    TestBuilder, GUARD,
};
use linera_base::data_types::*;
use linera_execution::{system::Balance, ApplicationId, Bytecode, Operation, Query, Response};
use linera_storage::Store;
use linera_views::views::ViewError;
use test_log::test;

#[test(tokio::test)]
async fn test_memory_create_application() -> Result<(), anyhow::Error> {
    run_test_create_application(MakeMemoryStoreClient).await
}

#[test(tokio::test)]
async fn test_rocksdb_create_application() -> Result<(), anyhow::Error> {
    let _lock = GUARD.lock().await;
    run_test_create_application(MakeRocksdbStoreClient::default()).await
}

#[test(tokio::test)]
#[ignore]
async fn test_dynamo_db_create_application() -> Result<(), anyhow::Error> {
    run_test_create_application(MakeDynamoDbStoreClient::default()).await
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

    let (contract_path, service_path) = linera_execution::wasm_test::get_counter_bytecode_paths()?;
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
        .create_application(bytecode_id, initial_value_bytes, vec![])
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
