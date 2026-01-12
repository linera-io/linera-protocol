// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm specific worker tests.
//!
//! These tests only run if a Wasm runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

#![allow(clippy::large_futures)]
#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use std::collections::BTreeMap;

use assert_matches::assert_matches;
use linera_base::{
    crypto::AccountSecretKey,
    data_types::{
        Amount, ApplicationDescription, Blob, BlockHeight, Bytecode, OracleResponse, Timestamp,
    },
    identifiers::ModuleId,
    vm::VmRuntime,
};
use linera_chain::{
    data_types::OperationResult,
    test::{make_child_block, make_first_block, BlockTestExt},
};
use linera_execution::{system::SystemOperation, Operation, WasmRuntime};
use linera_storage::Storage;
use test_case::test_case;

use super::TestEnvironment;
#[cfg(feature = "dynamodb")]
use crate::test_utils::DynamoDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::test_utils::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::test_utils::ScyllaDbStorageBuilder;
use crate::{
    test_utils::{MemoryStorageBuilder, StorageBuilder},
    worker::WorkerError,
};

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let builder = MemoryStorageBuilder::with_wasm_runtime(Some(wasm_runtime));
    run_test_handle_certificates_to_create_application(builder).await
}

#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_rocks_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let builder = RocksDbStorageBuilder::with_wasm_runtime(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(builder).await
}

#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dynamo_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let builder = DynamoDbStorageBuilder::with_wasm_runtime(Some(wasm_runtime));
    run_test_handle_certificates_to_create_application(builder).await
}

#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_scylla_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let builder = ScyllaDbStorageBuilder::with_wasm_runtime(Some(wasm_runtime));
    run_test_handle_certificates_to_create_application(builder).await
}

async fn run_test_handle_certificates_to_create_application<B>(
    mut storage_builder: B,
) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let vm_runtime = VmRuntime::Wasm;
    let publisher_owner = AccountSecretKey::generate().public().into();
    let creator_owner = AccountSecretKey::generate().public().into();
    let mut env = TestEnvironment::new(&mut storage_builder, false, false).await?;
    let publisher_chain = env.add_root_chain(1, publisher_owner, Amount::ZERO).await;
    let creator_chain = env.add_root_chain(2, creator_owner, Amount::ZERO).await;

    // Load the bytecode files for a module.
    let (contract_path, service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("counter")?;
    let contract_bytecode = Bytecode::load_from_file(contract_path)?;
    let service_bytecode = Bytecode::load_from_file(service_path)?;

    let contract_blob = Blob::new_contract_bytecode(contract_bytecode.clone().compress());
    let service_blob = Blob::new_service_bytecode(service_bytecode.compress());

    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let contract_blob_hash = contract_blob_id.hash;
    let service_blob_hash = service_blob_id.hash;

    let module_id = ModuleId::new(contract_blob_hash, service_blob_hash, vm_runtime);

    // Publish the module.
    let publish_operation = SystemOperation::PublishModule { module_id };
    let publish_block = make_first_block(publisher_chain.id())
        .with_timestamp(1)
        .with_operation(publish_operation);
    env.executing_worker()
        .storage
        .write_blobs(&[contract_blob.clone(), service_blob.clone()])
        .await?;
    let publish_certificate = env
        .execute_proposal(
            publish_block.clone(),
            vec![contract_blob.clone(), service_blob.clone()],
        )
        .await?;

    assert!(publish_certificate
        .value()
        .matches_proposed_block(&publish_block));
    assert!(publish_certificate.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![OperationResult::default()]
    ));

    assert_matches!(
        env.worker()
            .fully_handle_certificate_with_notifications(publish_certificate.clone(), &())
            .await,
        Err(WorkerError::BlobsNotFound(_))
    );
    env.write_blobs(&[contract_blob.clone(), service_blob.clone()])
        .await?;
    let info = env
        .worker()
        .fully_handle_certificate_with_notifications(publish_certificate.clone(), &())
        .await
        .unwrap()
        .info;
    assert_eq!(publisher_chain.id(), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Timestamp::from(1), info.timestamp);
    assert_eq!(Some(publish_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Create an application.
    let initial_value = 10_u64;
    let initial_value_bytes = serde_json::to_vec(&initial_value)?;
    let parameters_bytes = serde_json::to_vec(&())?;
    let create_operation = SystemOperation::CreateApplication {
        module_id,
        parameters: parameters_bytes.clone(),
        instantiation_argument: initial_value_bytes.clone(),
        required_application_ids: vec![],
    };
    let application_description = ApplicationDescription {
        module_id,
        creator_chain_id: creator_chain.id(),
        block_height: BlockHeight::from(0),
        application_index: 0,
        required_application_ids: vec![],
        parameters: parameters_bytes,
    };
    let application_description_blob = Blob::new_application_description(&application_description);
    let application_id = From::from(&application_description);
    let create_block = make_first_block(creator_chain.id())
        .with_timestamp(2)
        .with_operation(create_operation);
    let create_certificate = env.execute_proposal(create_block.clone(), vec![]).await?;

    assert!(create_certificate
        .value()
        .matches_proposed_block(&create_block));
    assert!(create_certificate.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![
            OracleResponse::Blob(contract_blob_id),
            OracleResponse::Blob(service_blob_id),
        ]],
        vec![vec![]],
        vec![vec![application_description_blob.clone()]],
        vec![OperationResult::default()],
    ));

    env.write_blobs(std::slice::from_ref(&application_description_blob))
        .await?;
    let info = env
        .worker()
        .fully_handle_certificate_with_notifications(create_certificate.clone(), &())
        .await
        .unwrap()
        .info;
    assert_eq!(creator_chain.id(), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Timestamp::from(2), info.timestamp);
    assert_eq!(Some(create_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Execute an application operation
    let increment = 5_u64;
    let counter_operation = counter::CounterOperation::Increment { value: increment };
    let user_operation = bcs::to_bytes(&counter_operation)?;
    let run_block = make_child_block(&create_certificate.into_value())
        .with_timestamp(3)
        .with_operation(Operation::User {
            application_id,
            bytes: user_operation.clone(),
        });
    let run_certificate = env.execute_proposal(run_block.clone(), vec![]).await?;

    assert!(run_certificate.value().matches_proposed_block(&run_block));
    assert!(run_certificate.block().outcome_matches(
        vec![vec![]],
        BTreeMap::new(),
        BTreeMap::new(),
        vec![vec![]],
        vec![vec![]],
        vec![vec![]],
        vec![OperationResult(bcs::to_bytes(&15u64)?)],
    ));

    let info = env
        .worker()
        .fully_handle_certificate_with_notifications(run_certificate.clone(), &())
        .await
        .unwrap()
        .info;
    assert_eq!(creator_chain.id(), info.chain_id);
    assert_eq!(Amount::ZERO, info.chain_balance);
    assert_eq!(BlockHeight::from(2), info.next_block_height);
    assert_eq!(Some(run_certificate.hash()), info.block_hash);
    assert_eq!(Timestamp::from(3), info.timestamp);
    assert!(info.manager.pending.is_none());
    Ok(())
}
