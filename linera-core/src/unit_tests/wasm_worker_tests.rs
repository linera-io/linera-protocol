// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm specific worker tests.
//!
//! These tests only run if a Wasm runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

#![allow(clippy::large_futures)]
#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use std::collections::{BTreeMap, BTreeSet};

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
    data_types::{
        BlockExecutionOutcome, BundleExecutionPolicy, BundleFailurePolicy, IncomingBundle,
        MessageAction, MessageBundle, OperationResult, OutgoingMessageExt,
    },
    test::{make_child_block, make_first_block, BlockTestExt},
    types::ConfirmedBlock,
};
use linera_execution::{
    system::SystemOperation, test_utils::SystemExecutionState, ExecutionRuntimeContext,
    ExecutionStateActor, Operation, OperationContext, ResourceController, TransactionTracker,
    WasmContractModule, WasmRuntime,
};
use linera_storage::{DbStorage, Storage};
#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::DynamoDbDatabase;
#[cfg(feature = "rocksdb")]
use linera_views::rocks_db::RocksDbDatabase;
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::ScyllaDbDatabase;
use linera_views::{context::Context, memory::MemoryDatabase, views::View};
use test_case::test_case;

use super::TestEnvironment;
use crate::worker::WorkerError;

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_rocks_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let storage = DbStorage::<RocksDbDatabase, _>::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dynamo_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let storage = DbStorage::<DynamoDbDatabase, _>::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_scylla_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let storage = DbStorage::<ScyllaDbDatabase, _>::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

async fn run_test_handle_certificates_to_create_application<S>(
    storage: S,
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let vm_runtime = VmRuntime::Wasm;
    let publisher_owner = AccountSecretKey::generate().public().into();
    let creator_owner = AccountSecretKey::generate().public().into();
    let mut env = TestEnvironment::new(storage.clone(), false, false).await;
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
    let contract = WasmContractModule::new(contract_bytecode, wasm_runtime).await?;

    // Publish the module.
    let publish_operation = SystemOperation::PublishModule { module_id };
    let publish_block = make_first_block(publisher_chain.id())
        .with_timestamp(1)
        .with_operation(publish_operation);
    let publisher_system_state = SystemExecutionState {
        timestamp: Timestamp::from(1),
        used_blobs: BTreeSet::from([contract_blob_id, service_blob_id]),
        ..env.system_execution_state(&publisher_chain.id())
    };
    let publisher_state_hash = publisher_system_state.clone().into_hash().await;
    let publish_block_proposal = ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![Vec::new()],
            previous_message_blocks: BTreeMap::new(),
            previous_event_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            state_hash: publisher_state_hash,
            oracle_responses: vec![vec![]],
            operation_results: vec![OperationResult::default()],
        }
        .with(publish_block),
    );
    let publish_certificate = env.make_certificate(publish_block_proposal);

    assert_matches!(
        env.worker()
            .fully_handle_certificate_with_notifications(publish_certificate.clone(), &())
            .await,
        Err(WorkerError::BlobsNotFound(_))
    );
    storage
        .write_blobs(&[contract_blob.clone(), service_blob.clone()])
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

    let mut creator_system_state = SystemExecutionState {
        timestamp: Timestamp::from(1),
        ..env.system_execution_state(&creator_chain.id())
    };

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
    let application_description_blob_id = application_description_blob.id();
    let application_id = From::from(&application_description);
    let create_block = make_first_block(creator_chain.id())
        .with_timestamp(2)
        .with_operation(create_operation);
    creator_system_state.timestamp = Timestamp::from(2);
    let mut creator_state = creator_system_state.into_view().await;
    creator_state
        .simulate_instantiation(
            contract.into(),
            Timestamp::from(2),
            application_description.clone(),
            initial_value_bytes.clone(),
            contract_blob,
            service_blob,
        )
        .await?;
    let create_block_proposal = ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![vec![]],
            previous_message_blocks: BTreeMap::new(),
            previous_event_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            state_hash: creator_state.crypto_hash_mut().await?,
            oracle_responses: vec![vec![
                OracleResponse::Blob(contract_blob_id),
                OracleResponse::Blob(service_blob_id),
            ]],
            blobs: vec![vec![application_description_blob.clone()]],
            operation_results: vec![OperationResult::default()],
        }
        .with(create_block),
    );
    let create_certificate = env.make_certificate(create_block_proposal);

    storage
        .write_blobs(&[application_description_blob.clone()])
        .await?;
    creator_state
        .context()
        .extra()
        .add_blobs([application_description_blob])
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
    let user_operation = bcs::to_bytes(&increment)?;
    let run_block = make_child_block(&create_certificate.into_value())
        .with_timestamp(3)
        .with_operation(Operation::User {
            application_id,
            bytes: user_operation.clone(),
        });
    let operation_context = OperationContext {
        chain_id: creator_chain.id(),
        authenticated_signer: None,
        height: run_block.height,
        round: Some(0),
        timestamp: Timestamp::from(3),
    };
    let mut controller = ResourceController::default();
    let mut txn_tracker = TransactionTracker::new(
        Timestamp::from(3),
        0,
        0,
        0,
        Some(vec![OracleResponse::Blob(application_description_blob_id)]),
        &[],
    );
    ExecutionStateActor::new(&mut creator_state, &mut txn_tracker, &mut controller)
        .execute_operation(
            operation_context,
            Operation::User {
                application_id,
                bytes: user_operation,
            },
        )
        .await?;
    creator_state.system.timestamp.set(Timestamp::from(3));
    creator_state
        .system
        .used_blobs
        .insert(&application_description_blob_id)?;
    let run_block_proposal = ConfirmedBlock::new(
        BlockExecutionOutcome {
            messages: vec![Vec::new()],
            previous_message_blocks: BTreeMap::new(),
            previous_event_blocks: BTreeMap::new(),
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            state_hash: creator_state.crypto_hash_mut().await?,
            oracle_responses: vec![vec![]],
            operation_results: vec![OperationResult(bcs::to_bytes(&15u64)?)],
        }
        .with(run_block),
    );
    let run_certificate = env.make_certificate(run_block_proposal);

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

/// Tests that staging block execution with AutoRetry policy produces the same outcome
/// as re-staging the modified block with Abort policy.
///
/// This verifies that the checkpointing mechanism correctly restores state after failures,
/// and that the modified block (with rejected bundles) can be executed deterministically.
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_auto_retry_vs_abort_consistency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(Some(wasm_runtime)).await;
    run_test_auto_retry_vs_abort_consistency(storage).await
}

async fn run_test_auto_retry_vs_abort_consistency<S>(storage: S) -> anyhow::Result<()>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use linera_base::identifiers::ApplicationId;

    let vm_runtime = VmRuntime::Wasm;
    let sender_owner = AccountSecretKey::generate().public().into();
    let receiver_owner = AccountSecretKey::generate().public().into();
    let mut env = TestEnvironment::new(storage.clone(), false, false).await;
    let sender_chain = env.add_root_chain(1, sender_owner, Amount::ONE).await;
    let receiver_chain = env.add_root_chain(2, receiver_owner, Amount::ONE).await;

    // Load the bytecode for both counter and meta-counter.
    let (counter_contract_path, counter_service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("counter")?;
    let counter_contract_bytecode = Bytecode::load_from_file(counter_contract_path)?;
    let counter_service_bytecode = Bytecode::load_from_file(counter_service_path)?;
    let counter_contract_blob =
        Blob::new_contract_bytecode(counter_contract_bytecode.clone().compress());
    let counter_service_blob = Blob::new_service_bytecode(counter_service_bytecode.compress());
    let counter_module_id = ModuleId::new(
        counter_contract_blob.id().hash,
        counter_service_blob.id().hash,
        vm_runtime,
    );

    let (meta_contract_path, meta_service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("meta-counter")?;
    let meta_contract_bytecode = Bytecode::load_from_file(meta_contract_path)?;
    let meta_service_bytecode = Bytecode::load_from_file(meta_service_path)?;
    let meta_contract_blob = Blob::new_contract_bytecode(meta_contract_bytecode.clone().compress());
    let meta_service_blob = Blob::new_service_bytecode(meta_service_bytecode.compress());
    let meta_module_id = ModuleId::new(
        meta_contract_blob.id().hash,
        meta_service_blob.id().hash,
        vm_runtime,
    );

    // Write blobs to storage.
    let all_blobs = [
        counter_contract_blob.clone(),
        counter_service_blob.clone(),
        meta_contract_blob.clone(),
        meta_service_blob.clone(),
    ];
    env.worker()
        .storage_client()
        .write_blobs(&all_blobs)
        .await?;

    // Publish modules on sender chain.
    let publish_counter_op = SystemOperation::PublishModule {
        module_id: counter_module_id,
    };
    let publish_meta_op = SystemOperation::PublishModule {
        module_id: meta_module_id,
    };
    let publish_block = make_first_block(sender_chain.id())
        .with_timestamp(1)
        .with_operation(publish_counter_op)
        .with_operation(publish_meta_op);
    let (publish_executed, _, _) = env
        .worker()
        .stage_block_execution(publish_block, None, all_blobs.to_vec())
        .await?;
    let publish_cert = env.make_certificate(ConfirmedBlock::new(publish_executed));
    env.worker()
        .fully_handle_certificate_with_notifications(publish_cert.clone(), &())
        .await?;

    // Create counter application on sender.
    let counter_params = serde_json::to_vec(&())?;
    let counter_init = serde_json::to_vec(&10_u64)?;
    let create_counter_op = SystemOperation::CreateApplication {
        module_id: counter_module_id,
        parameters: counter_params.clone(),
        instantiation_argument: counter_init,
        required_application_ids: vec![],
    };
    let counter_app_desc = ApplicationDescription {
        module_id: counter_module_id,
        creator_chain_id: sender_chain.id(),
        block_height: BlockHeight::from(1),
        application_index: 0,
        required_application_ids: vec![],
        parameters: counter_params,
    };
    let counter_app_id: ApplicationId = From::from(&counter_app_desc);
    let create_counter_block = make_child_block(&publish_cert.into_value())
        .with_timestamp(2)
        .with_operation(create_counter_op);
    let (create_counter_executed, _, _) = env
        .worker()
        .stage_block_execution(create_counter_block, None, vec![])
        .await?;
    let create_counter_cert = env.make_certificate(ConfirmedBlock::new(create_counter_executed));
    env.worker()
        .fully_handle_certificate_with_notifications(create_counter_cert.clone(), &())
        .await?;

    // Create meta-counter application on sender (depends on counter).
    let meta_params = serde_json::to_vec(&counter_app_id)?;
    let meta_init = serde_json::to_vec(&())?;
    let create_meta_op = SystemOperation::CreateApplication {
        module_id: meta_module_id,
        parameters: meta_params.clone(),
        instantiation_argument: meta_init,
        required_application_ids: vec![counter_app_id],
    };
    let meta_app_desc = ApplicationDescription {
        module_id: meta_module_id,
        creator_chain_id: sender_chain.id(),
        block_height: BlockHeight::from(2),
        application_index: 0,
        required_application_ids: vec![counter_app_id],
        parameters: meta_params,
    };
    let meta_app_id: ApplicationId = From::from(&meta_app_desc);
    let create_meta_block = make_child_block(&create_counter_cert.into_value())
        .with_timestamp(3)
        .with_operation(create_meta_op);
    let (create_meta_executed, _, _) = env
        .worker()
        .stage_block_execution(create_meta_block, None, vec![])
        .await?;
    let create_meta_cert = env.make_certificate(ConfirmedBlock::new(create_meta_executed));
    env.worker()
        .fully_handle_certificate_with_notifications(create_meta_cert.clone(), &())
        .await?;

    // Send a message that will fail on the receiver (using meta-counter's fail operation).
    let fail_operation = meta_counter::Operation::fail(receiver_chain.id());
    let fail_op_bytes = bcs::to_bytes(&fail_operation)?;
    let send_fail_block = make_child_block(&create_meta_cert.into_value())
        .with_timestamp(4)
        .with_operation(Operation::User {
            application_id: meta_app_id,
            bytes: fail_op_bytes,
        });
    let (send_fail_executed, _, _) = env
        .worker()
        .stage_block_execution(send_fail_block, None, vec![])
        .await?;
    let send_fail_cert = env.make_certificate(ConfirmedBlock::new(send_fail_executed));
    env.worker()
        .fully_handle_certificate_with_notifications(send_fail_cert.clone(), &())
        .await?;

    // Get the outgoing messages from the certificate and convert to PostedMessages.
    let outgoing_messages = &send_fail_cert.block().body.messages[0];
    assert!(
        !outgoing_messages.is_empty(),
        "Should have outgoing messages"
    );
    let posted_messages: Vec<_> = outgoing_messages
        .iter()
        .enumerate()
        .map(|(i, msg)| msg.clone().into_posted(i as u32))
        .collect();

    // Build a proposed block for the receiver with the incoming message bundle.
    let incoming_bundle = IncomingBundle {
        origin: sender_chain.id(),
        bundle: MessageBundle {
            certificate_hash: send_fail_cert.hash(),
            height: send_fail_cert.block().header.height,
            timestamp: send_fail_cert.block().header.timestamp,
            transaction_index: 0,
            messages: posted_messages,
        },
        action: MessageAction::Accept,
    };

    let proposed_block = make_first_block(receiver_chain.id())
        .with_timestamp(5)
        .with_incoming_bundle(incoming_bundle);

    // Stage execution with AutoRetry policy.
    // This should handle the failing message by rejecting the bundle.
    let (modified_block, auto_retry_executed, _, _) = env
        .worker()
        .stage_block_execution_with_policy(
            proposed_block.clone(),
            None,
            vec![],
            BundleExecutionPolicy {
                on_failure: BundleFailurePolicy::AutoRetry { max_failures: 3 },
                time_budget: None,
            },
        )
        .await?;

    // Verify the bundle was rejected (not just accepted).
    let modified_bundles: Vec<_> = modified_block
        .transactions
        .iter()
        .filter_map(|t| t.incoming_bundle())
        .collect();
    assert_eq!(modified_bundles.len(), 1);
    assert_eq!(
        modified_bundles[0].action,
        MessageAction::Reject,
        "The failing bundle should be rejected"
    );

    // Now stage the modified block with Abort policy.
    // Since the bundle is already marked as Reject, this should succeed
    // and produce the same outcome.
    let (_, abort_executed, _, _) = env
        .worker()
        .stage_block_execution_with_policy(
            modified_block.clone(),
            None,
            vec![],
            BundleExecutionPolicy::committed(),
        )
        .await?;

    // The executed blocks should be identical.
    assert_eq!(
        auto_retry_executed.header.state_hash, abort_executed.header.state_hash,
        "State hashes should match between AutoRetry and Abort execution"
    );
    assert_eq!(
        auto_retry_executed.body.messages, abort_executed.body.messages,
        "Outgoing messages should match"
    );
    assert_eq!(
        auto_retry_executed.body.events, abort_executed.body.events,
        "Events should match"
    );

    Ok(())
}
