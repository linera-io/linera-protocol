// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm specific client tests.
//!
//! These tests only run if a Wasm runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

// Tests for `RocksDb`, `DynamoDb`, `ScyllaDb` and `Service` are currently disabled
// because they are slow and their behavior appears to be correctly check by the
// test with memory.

#![allow(clippy::large_futures)]
#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use std::collections::BTreeMap;

use assert_matches::assert_matches;
use async_graphql::Request;
use counter::CounterAbi;
use crowd_funding::{CrowdFundingAbi, InstantiationArgument, Operation as CrowdFundingOperation};
use fungible::{FungibleOperation, InitialState, Parameters};
use hex_game::{HexAbi, Operation as HexOperation, Timeouts};
use linera_base::{
    crypto::{CryptoHash, InMemorySigner},
    data_types::{
        Amount, BlanketMessagePolicy, BlobContent, BlockHeight, Bytecode, ChainDescription, Event,
        MessagePolicy, OracleResponse, Round, TimeDelta, Timestamp,
    },
    identifiers::{ApplicationId, BlobId, BlobType, DataBlobHash, ModuleId, StreamId, StreamName},
    ownership::{ChainOwnership, TimeoutConfig},
    vm::VmRuntime,
};
use linera_chain::{data_types::MessageAction, ChainError, ChainExecutionContext};
use linera_execution::{
    wasm_test, ExecutionError, Message, MessageKind, Operation, QueryOutcome,
    ResourceControlPolicy, SystemMessage, SystemOperation, WasmRuntime,
};
use linera_storage::Storage as _;
use serde_json::json;
use test_case::test_case;

#[cfg(feature = "dynamodb")]
use crate::client::client_tests::DynamoDbStorageBuilder;
#[cfg(feature = "rocksdb")]
use crate::client::client_tests::RocksDbStorageBuilder;
#[cfg(feature = "scylladb")]
use crate::client::client_tests::ScyllaDbStorageBuilder;
#[cfg(feature = "storage-service")]
use crate::client::client_tests::ServiceStorageBuilder;
use crate::{
    client::{
        client_tests::{MemoryStorageBuilder, StorageBuilder, TestBuilder},
        ChainClient, ChainClientError, ClientOutcome,
    },
    local_node::LocalNodeError,
    test_utils::{ClientOutcomeResultExt as _, FaultType},
    worker::WorkerError,
    Environment,
};

trait ChainClientExt {
    /// Reads the bytecode of a Wasm example and publishes it. Returns the new module ID.
    async fn publish_wasm_example(&self, name: &str) -> anyhow::Result<ModuleId>;
}

impl<Env: Environment> ChainClientExt for ChainClient<Env> {
    async fn publish_wasm_example(&self, name: &str) -> anyhow::Result<ModuleId> {
        let (contract_path, service_path) = wasm_test::get_example_bytecode_paths(name)?;
        let contract_bytecode = Bytecode::load_from_file(contract_path)?;
        let service_bytecode = Bytecode::load_from_file(service_path)?;
        let (module_id, _cert) = self
            .publish_module(contract_bytecode, service_bytecode, VmRuntime::Wasm)
            .await
            .unwrap_ok_committed();
        Ok(module_id)
    }
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "storage-service")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_service_create_application(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_create_application(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime)).await
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
{
    let keys = InMemorySigner::new(None);
    let vm_runtime = VmRuntime::Wasm;
    let (contract_path, service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("counter")?;
    let contract_bytecode = Bytecode::load_from_file(contract_path)?;
    let service_bytecode = Bytecode::load_from_file(service_path)?;
    let contract_compressed_len = contract_bytecode.compress().compressed_bytes.len();
    let service_compressed_len = service_bytecode.compress().compressed_bytes.len();

    let mut policy = ResourceControlPolicy::all_categories();
    policy.maximum_bytecode_size = contract_bytecode
        .bytes
        .len()
        .max(service_bytecode.bytes.len()) as u64;
    policy.maximum_blob_size = contract_compressed_len.max(service_compressed_len) as u64;
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(policy.clone());
    let publisher = builder.add_root_chain(0, Amount::from_tokens(3)).await?;
    let creator = builder.add_root_chain(1, Amount::ONE).await?;

    let (module_id, _cert) = publisher
        .publish_module(contract_bytecode, service_bytecode, vm_runtime)
        .await
        .unwrap_ok_committed();
    let module_id = module_id.with_abi::<counter::CounterAbi, (), u64>();

    creator.synchronize_from_validators().await.unwrap();
    creator.process_inbox().await.unwrap();

    // No fuel was used so far.
    let balance_after_messaging = creator.local_balance().await?;
    assert_eq!(balance_after_messaging, Amount::ONE);

    let initial_value = 10_u64;
    let (application_id, _) = creator
        .create_application(module_id, &(), &initial_value, vec![])
        .await
        .unwrap_ok_committed();

    let increment = 5_u64;
    creator
        .execute_operation(Operation::user(application_id, &increment)?)
        .await
        .unwrap();

    let query = Request::new("{ value }");
    let outcome = creator
        .query_user_application(application_id, &query)
        .await
        .unwrap();

    let expected = QueryOutcome {
        response: async_graphql::Response::new(
            async_graphql::Value::from_json(json!({"value": 15})).unwrap(),
        ),
        operations: vec![],
    };

    assert_eq!(outcome, expected);
    // Creating the application used fuel because of the `instantiate` call.
    let balance_after_init = creator.local_balance().await?;
    assert!(balance_after_init < balance_after_messaging);

    let large_bytecode = Bytecode::new(vec![0; policy.maximum_bytecode_size as usize + 1]);
    let small_bytecode = Bytecode::new(vec![]);
    // Publishing bytecode that exceeds the limit fails.
    let result = publisher
        .publish_module(large_bytecode.clone(), small_bytecode.clone(), vm_runtime)
        .await;
    assert_matches!(
        result,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(chain_error))
        )) if matches!(&*chain_error, ChainError::ExecutionError(
            error, ChainExecutionContext::Block
        ) if matches!(**error, ExecutionError::BytecodeTooLarge))
    );
    let result = publisher
        .publish_module(small_bytecode, large_bytecode, vm_runtime)
        .await;
    assert_matches!(
        result,
        Err(ChainClientError::LocalNodeError(
            LocalNodeError::WorkerError(WorkerError::ChainError(chain_error))
        )) if matches!(&*chain_error, ChainError::ExecutionError(
            error, ChainExecutionContext::Block
        ) if matches!(**error, ExecutionError::BytecodeTooLarge))
    );

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
#[cfg(feature = "storage-service")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_service_run_application_with_dependency(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_run_application_with_dependency(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime))
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
{
    let keys = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    // Will publish the module.
    let publisher = builder.add_root_chain(0, Amount::from_tokens(3)).await?;
    // Will create the apps and use them to send a message.
    let creator = builder.add_root_chain(1, Amount::ONE).await?;
    // Will receive the message.
    let receiver = builder.add_root_chain(2, Amount::ONE).await?;
    let receiver_id = receiver.chain_id();

    // Handling the message causes an oracle request to the counter service, so no fast blocks
    // are allowed.
    let receiver_key = receiver.identity().await.unwrap();

    receiver
        .change_ownership(ChainOwnership::multiple(
            [(receiver_key, 100)],
            100,
            TimeoutConfig::default(),
        ))
        .await
        .unwrap();

    let creator_key = creator.identity().await.unwrap();
    creator
        .change_ownership(ChainOwnership::multiple(
            [(creator_key, 100)],
            100,
            TimeoutConfig::default(),
        ))
        .await
        .unwrap();

    let module_id1 = publisher.publish_wasm_example("counter").await?;
    let module_id1 = module_id1.with_abi::<counter::CounterAbi, (), u64>();
    let module_id2 = publisher.publish_wasm_example("meta-counter").await?;
    let module_id2 =
        module_id2.with_abi::<meta_counter::MetaCounterAbi, ApplicationId<CounterAbi>, ()>();

    // Creator receives the bytecode files then creates the app.
    creator.synchronize_from_validators().await.unwrap();
    let initial_value = 10_u64;
    let (application_id1, _) = creator
        .create_application(module_id1, &(), &initial_value, vec![])
        .await
        .unwrap_ok_committed();
    let (application_id2, certificate) = creator
        .create_application(
            module_id2,
            &application_id1,
            &(),
            vec![application_id1.forget_abi()],
        )
        .await
        .unwrap_ok_committed();
    assert_eq!(
        certificate.block().body.events,
        vec![vec![Event {
            stream_id: StreamId {
                application_id: application_id2.forget_abi().into(),
                stream_name: StreamName(b"announcements".to_vec()),
            },
            index: 0,
            value: bcs::to_bytes(&"instantiated".to_string()).unwrap(),
        }]]
    );

    let mut operation = meta_counter::Operation::increment(receiver_id, 5, true);
    operation.fuel_grant = 1000000;
    let cert = creator
        .execute_operation(Operation::user(application_id2, &operation)?)
        .await
        .unwrap_ok_committed();
    let block = cert.block();
    let responses = &block.body.oracle_responses;
    let [_, responses] = &responses[..] else {
        panic!("Unexpected oracle responses: {:?}", responses);
    };
    let [OracleResponse::Service(json)] = &responses[..] else {
        assert_eq!(&responses[..], &[]);
        panic!("Unexpected oracle responses: {:?}", responses);
    };
    let response_json = serde_json::from_slice::<serde_json::Value>(json).unwrap();
    assert_eq!(response_json["data"], json!({"value": 10}));

    receiver.synchronize_from_validators().await.unwrap();
    receiver.process_inbox().await.unwrap();

    let query = Request::new("{ value }");
    let outcome = receiver
        .query_user_application(application_id2, &query)
        .await
        .unwrap();

    let expected = QueryOutcome {
        response: async_graphql::Response::new(
            async_graphql::Value::from_json(json!({"value": 5})).unwrap(),
        ),
        operations: vec![],
    };

    assert_eq!(outcome, expected);

    // Try again with a value that will make the (untracked) message fail.
    let operation = meta_counter::Operation::fail(receiver_id);
    creator
        .execute_operation(Operation::user(application_id2, &operation)?)
        .await
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await.unwrap();
    let mut certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let cert = certs.pop().unwrap();
    let incoming_bundles = cert.block().body.incoming_bundles().collect::<Vec<_>>();
    assert_eq!(incoming_bundles.len(), 1);
    assert_eq!(incoming_bundles[0].action, MessageAction::Reject);
    assert_eq!(
        incoming_bundles[0].bundle.messages[0].kind,
        MessageKind::Simple
    );
    let messages = cert.block().messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].len(), 0);

    // Try again with a value that will make the (tracked) message fail.
    let mut operation = meta_counter::Operation::fail(receiver_id);
    operation.is_tracked = true;
    creator
        .execute_operation(Operation::user(application_id2, &operation)?)
        .await
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await.unwrap();
    let mut certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let cert = certs.pop().unwrap();
    let incoming_bundles = cert.block().body.incoming_bundles().collect::<Vec<_>>();
    assert_eq!(incoming_bundles.len(), 1);
    assert_eq!(incoming_bundles[0].action, MessageAction::Reject);
    assert_eq!(
        incoming_bundles[0].bundle.messages[0].kind,
        MessageKind::Tracked
    );
    let messages = cert.block().messages();
    assert_eq!(messages.len(), 1);

    // The bounced message is marked as "bouncing" in the Wasm context and succeeds.
    creator.synchronize_from_validators().await.unwrap();
    let mut certs = creator.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let cert = certs.pop().unwrap();
    let incoming_bundles = cert.block().body.incoming_bundles().collect::<Vec<_>>();
    assert_eq!(incoming_bundles.len(), 2);
    // First message is the grant refund for the successful message sent before.
    assert_eq!(incoming_bundles[0].action, MessageAction::Accept);
    assert_eq!(
        incoming_bundles[0].bundle.messages[0].kind,
        MessageKind::Tracked
    );
    assert_matches!(
        incoming_bundles[0].bundle.messages[0].message,
        Message::System(SystemMessage::Credit { .. })
    );
    // Second message is the bounced message.
    assert_eq!(incoming_bundles[1].action, MessageAction::Accept);
    assert_eq!(
        incoming_bundles[1].bundle.messages[0].kind,
        MessageKind::Bouncing
    );
    assert_matches!(
        incoming_bundles[1].bundle.messages[0].message,
        Message::User { .. }
    );

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "storage-service")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_service_cross_chain_message(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_cross_chain_message(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime)).await
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
{
    let keys = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let _admin = builder.add_root_chain(0, Amount::ONE).await?;
    let sender = builder.add_root_chain(1, Amount::from_tokens(3)).await?;
    let receiver = builder.add_root_chain(2, Amount::ONE).await?;
    let receiver2 = builder.add_root_chain(3, Amount::ONE).await?;

    let module_id = sender.publish_wasm_example("fungible").await?;
    let module_id = module_id.with_abi::<fungible::FungibleTokenAbi, Parameters, InitialState>();

    let sender_owner = sender.preferred_owner.unwrap();
    let receiver_owner = receiver.preferred_owner.unwrap();
    let receiver2_owner = receiver2.preferred_owner.unwrap();

    let accounts = BTreeMap::from_iter([(sender_owner, Amount::from_tokens(1_000_000))]);
    let state = InitialState { accounts };
    let params = Parameters::new("FUN");
    let (application_id, _cert) = sender
        .create_application(module_id, &params, &state, vec![])
        .await
        .unwrap_ok_committed();

    // Make a transfer using the fungible app.
    let transfer = FungibleOperation::Transfer {
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
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await.unwrap();
    {
        // The receiver did not execute the sender chain.
        let chain = receiver
            .storage_client()
            .load_chain(sender.chain_id())
            .await?;
        assert_eq!(chain.tip_state.get().next_block_height.0, 0);
        assert_eq!(
            chain
                .preprocessed_blocks
                .get(&cert.inner().height())
                .await?,
            Some(cert.hash())
        );
    }
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let bundles = certs[0].block().body.incoming_bundles();
    assert!(bundles
        .flat_map(|msg| &msg.bundle.messages)
        .any(|msg| matches!(msg.message, Message::User { .. })));

    // Make another transfer.
    let transfer = FungibleOperation::Transfer {
        owner: sender_owner,
        amount: 200.into(),
        target_account: fungible::Account {
            chain_id: receiver.chain_id(),
            owner: receiver_owner,
        },
    };
    sender
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await.unwrap();
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);
    let bundles = certs[0].block().body.incoming_bundles();
    assert!(bundles
        .flat_map(|msg| &msg.bundle.messages)
        .any(|msg| matches!(msg.message, Message::User { .. })));

    // Try another transfer except that the amount is too large.
    let transfer = FungibleOperation::Transfer {
        owner: receiver_owner,
        amount: 301.into(),
        target_account: fungible::Account {
            chain_id: receiver2.chain_id(),
            owner: receiver2_owner,
        },
    };
    assert!(receiver
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .is_err());
    receiver.clear_pending_proposal();

    // Try another transfer with the correct amount.
    let transfer = FungibleOperation::Transfer {
        owner: receiver_owner,
        amount: 300.into(),
        target_account: fungible::Account {
            chain_id: receiver2.chain_id(),
            owner: receiver2_owner,
        },
    };
    receiver
        .execute_operation(Operation::user(application_id, &transfer)?)
        .await
        .unwrap_ok_committed();

    receiver2.synchronize_from_validators().await.unwrap();

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_event_streams(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_event_streams(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "storage-service")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_service_event_streams(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_event_streams(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocks_db_event_streams(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_event_streams(RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await).await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_event_streams(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_event_streams(DynamoDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_scylla_db_event_streams(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_event_streams(ScyllaDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_event_streams<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let keys = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 0, keys)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    builder.set_fault_type([3], FaultType::Offline);

    let sender = builder.add_root_chain(0, Amount::ONE).await?;
    let sender2 = builder.add_root_chain(1, Amount::ONE).await?;
    // Make sure that sender's chain ID is less than sender2's - important for the final
    // query check
    let (sender, sender2) = if sender.chain_id() < sender2.chain_id() {
        (sender, sender2)
    } else {
        (sender2, sender)
    };
    let mut receiver = builder.add_root_chain(2, Amount::ONE).await?;

    let module_id = receiver.publish_wasm_example("social").await?;
    let module_id = module_id.with_abi::<social::SocialAbi, (), ()>();

    let (application_id, _cert) = receiver
        .create_application(module_id, &(), &(), vec![])
        .await
        .unwrap_ok_committed();

    // Request to subscribe to the senders.
    let request_subscribe = social::Operation::Subscribe {
        chain_id: sender.chain_id(),
    };
    let request_subscribe2 = social::Operation::Subscribe {
        chain_id: sender2.chain_id(),
    };
    receiver
        .execute_operations(
            vec![
                Operation::user(application_id, &request_subscribe)?,
                Operation::user(application_id, &request_subscribe2)?,
            ],
            vec![],
        )
        .await
        .unwrap_ok_committed();

    // Make a post.
    let text = "Please like and comment!".to_string();
    let post = social::Operation::Post {
        text: text.clone(),
        image_url: None,
    };
    sender
        .execute_operation(Operation::user(application_id, &post)?)
        .await
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await.unwrap();

    builder.set_fault_type([3], FaultType::Honest);
    builder.set_fault_type([2], FaultType::Offline);

    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);

    // There should be an UpdateStreams operation due to the new post.
    let operations = certs[0].block().body.operations().collect::<Vec<_>>();
    let [Operation::System(operation)] = &*operations else {
        panic!("Expected one operation, got {:?}", operations);
    };
    let stream_id = StreamId {
        application_id: application_id.forget_abi().into(),
        stream_name: b"posts".into(),
    };
    assert_eq!(
        **operation,
        SystemOperation::UpdateStreams(vec![(sender.chain_id(), stream_id, 1)])
    );

    let query = async_graphql::Request::new("{ receivedPosts { keys { author, index } } }");
    let outcome = receiver
        .query_user_application(application_id, &query)
        .await?;
    let expected = QueryOutcome {
        response: async_graphql::Response::new(
            async_graphql::Value::from_json(json!({
                "receivedPosts": {
                    "keys": [
                        { "author": sender.chain_id, "index": 0 }
                    ]
                }
            }))
            .unwrap(),
        ),
        operations: vec![],
    };
    assert_eq!(outcome, expected);

    // Make two more posts.
    let text = "Follow sender2!".to_string();
    let post = social::Operation::Post {
        text: text.clone(),
        image_url: None,
    };
    sender
        .execute_operation(Operation::user(application_id, &post)?)
        .await
        .unwrap_ok_committed();

    let text = "Thanks for the shoutout!".to_string();
    let post = social::Operation::Post {
        text: text.clone(),
        image_url: None,
    };
    sender2
        .execute_operation(Operation::user(application_id, &post)?)
        .await
        .unwrap_ok_committed();

    receiver.synchronize_from_validators().await.unwrap();

    receiver.options_mut().message_policy = MessagePolicy::new(
        BlanketMessagePolicy::Accept,
        Some([sender.chain_id()].into_iter().collect()),
        None,
        None,
    );

    // Receiver should only process the event from sender now.
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);

    // There should be an UpdateStreams operation due to the new post.
    let operations = certs[0].block().body.operations().collect::<Vec<_>>();
    let [Operation::System(operation)] = &*operations else {
        panic!("Expected one operation, got {:?}", operations);
    };
    let stream_id = StreamId {
        application_id: application_id.forget_abi().into(),
        stream_name: b"posts".into(),
    };
    assert_eq!(
        **operation,
        SystemOperation::UpdateStreams(vec![(sender.chain_id(), stream_id, 2)])
    );

    // Let's receive from everyone again.
    receiver.options_mut().message_policy =
        MessagePolicy::new(BlanketMessagePolicy::Accept, None, None, None);

    // Receiver should now process the event from sender2 as well.
    let certs = receiver.process_inbox().await.unwrap().0;
    assert_eq!(certs.len(), 1);

    // There should be an UpdateStreams operation due to the new post.
    let operations = certs[0].block().body.operations().collect::<Vec<_>>();
    let [Operation::System(operation)] = &*operations else {
        panic!("Expected one operation, got {:?}", operations);
    };
    let stream_id = StreamId {
        application_id: application_id.forget_abi().into(),
        stream_name: b"posts".into(),
    };
    assert_eq!(
        **operation,
        SystemOperation::UpdateStreams(vec![(sender2.chain_id(), stream_id, 1)])
    );

    // Request to unsubscribe from the sender.
    let request_unsubscribe = social::Operation::Unsubscribe {
        chain_id: sender.chain_id(),
    };
    receiver
        .execute_operation(Operation::user(application_id, &request_unsubscribe)?)
        .await
        .unwrap_ok_committed();

    // Unsubscribe the receiver.
    sender.synchronize_from_validators().await.unwrap();
    let _certs = sender.process_inbox().await.unwrap();

    // Make a post.
    let post = social::Operation::Post {
        text: "Nobody will read this!".to_string(),
        image_url: None,
    };
    sender
        .execute_operation(Operation::user(application_id, &post)?)
        .await
        .unwrap_ok_committed();

    // The post will not be received by the unsubscribed chain.
    receiver.synchronize_from_validators().await.unwrap();
    let certs = receiver.process_inbox().await.unwrap().0;
    assert!(certs.is_empty());

    // There is still only one post it can see.
    let query = async_graphql::Request::new("{ receivedPosts { keys { author, index } } }");
    let outcome = receiver
        .query_user_application(application_id, &query)
        .await
        .unwrap();
    let expected = QueryOutcome {
        response: async_graphql::Response::new(
            async_graphql::Value::from_json(json!({
                "receivedPosts": {
                    "keys": [ { "author": sender.chain_id(), "index": 1 },
                              { "author": sender.chain_id(), "index": 0 },
                              { "author": sender2.chain_id(), "index": 0 } ]
                }
            }))
            .unwrap(),
        ),
        operations: vec![],
    };
    assert_eq!(outcome, expected);

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_memory_message_policy_accept_apps(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_message_policy_accept_apps(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "storage-service")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_service_message_policy_accept_apps(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_message_policy_accept_apps(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime))
        .await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_rocks_db_message_policy_accept_apps(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_message_policy_accept_apps(
        RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await,
    )
    .await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_message_policy_accept_apps(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_message_policy_accept_apps(DynamoDbStorageBuilder::with_wasm_runtime(wasm_runtime))
        .await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime; "wasmtime"))]
#[test_log::test(tokio::test)]
async fn test_scylla_db_message_policy_accept_apps(
    wasm_runtime: WasmRuntime,
) -> anyhow::Result<()> {
    run_test_message_policy_accept_apps(ScyllaDbStorageBuilder::with_wasm_runtime(wasm_runtime))
        .await
}

async fn run_test_message_policy_accept_apps<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let keys = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let pledger_chain = builder.add_root_chain(1, Amount::from_tokens(10)).await?;
    let mut campaign_chain = builder.add_root_chain(2, Amount::from_tokens(10)).await?;

    let pledger_owner = pledger_chain.preferred_owner().unwrap();
    let campaign_owner = campaign_chain.preferred_owner().unwrap();

    // Create a fungible token application.
    let fungible_module = pledger_chain.publish_wasm_example("fungible").await?;
    let fungible_module =
        fungible_module.with_abi::<fungible::FungibleTokenAbi, Parameters, InitialState>();
    let accounts = BTreeMap::from_iter([(pledger_owner, Amount::from_tokens(1_000))]);
    let state = InitialState { accounts };
    let params = Parameters::new("FUN");
    let (fungible_id, _cert) = pledger_chain
        .create_application(fungible_module, &params, &state, vec![])
        .await
        .unwrap_ok_committed();

    // Create a crowd-funding application that uses the fungible token.
    let crowd_funding_module = pledger_chain.publish_wasm_example("crowd-funding").await?;
    let crowd_funding_module =
        crowd_funding_module.with_abi::<CrowdFundingAbi, ApplicationId, InstantiationArgument>();
    let deadline = Timestamp::from(u64::MAX);
    let target = Amount::from_tokens(10);
    let instantiation_arg = InstantiationArgument {
        owner: campaign_owner,
        deadline,
        target,
    };
    let (crowd_funding_id, _cert) = campaign_chain
        .create_application(
            crowd_funding_module,
            &fungible_id.forget_abi(),
            &instantiation_arg,
            vec![],
        )
        .await
        .unwrap_ok_committed();

    // Make a pledge from pledger_chain to campaign_chain.
    // This creates a cross-chain bundle with messages from both fungible and crowd-funding apps.
    let pledge_amount = Amount::from_tokens(5);
    pledger_chain
        .execute_operation(Operation::user(
            crowd_funding_id,
            &CrowdFundingOperation::Pledge {
                owner: pledger_owner,
                amount: pledge_amount,
            },
        )?)
        .await
        .unwrap_ok_committed();

    campaign_chain.synchronize_from_validators().await?;

    // Test 1: Accept bundles with at least one message from fungible app.
    campaign_chain.options_mut().message_policy = MessagePolicy::new(
        BlanketMessagePolicy::Accept,
        None,
        Some([fungible_id.forget_abi().into()].into_iter().collect()),
        None,
    );
    let certs = campaign_chain.process_inbox().await?.0;
    assert_eq!(certs.len(), 1, "Should accept bundle with fungible message");

    // Reset for next test by making another pledge.
    pledger_chain
        .execute_operation(Operation::user(
            crowd_funding_id,
            &CrowdFundingOperation::Pledge {
                owner: pledger_owner,
                amount: pledge_amount,
            },
        )?)
        .await
        .unwrap_ok_committed();
    campaign_chain.synchronize_from_validators().await?;

    // Test 2: Accept bundles with at least one message from crowd-funding app.
    campaign_chain.options_mut().message_policy = MessagePolicy::new(
        BlanketMessagePolicy::Accept,
        None,
        Some([crowd_funding_id.forget_abi().into()].into_iter().collect()),
        None,
    );
    let certs = campaign_chain.process_inbox().await?.0;
    assert_eq!(
        certs.len(),
        1,
        "Should accept bundle with crowd-funding message"
    );

    // Reset for next test.
    pledger_chain
        .execute_operation(Operation::user(
            crowd_funding_id,
            &CrowdFundingOperation::Pledge {
                owner: pledger_owner,
                amount: pledge_amount,
            },
        )?)
        .await
        .unwrap_ok_committed();
    campaign_chain.synchronize_from_validators().await?;

    // Test 3: Reject bundles without any message from a non-existent app.
    // Use a different application description hash to create a fake app ID.
    let fake_app_id = ApplicationId::new(CryptoHash::test_hash("fake app"));
    campaign_chain.options_mut().message_policy = MessagePolicy::new(
        BlanketMessagePolicy::Accept,
        None,
        Some([fake_app_id.into()].into_iter().collect()),
        None,
    );
    let certs = campaign_chain.process_inbox().await?.0;
    assert_eq!(
        certs.len(),
        0,
        "Should reject bundle without message from fake app"
    );

    // Test 4: Reject bundles that contain messages from apps not in the allowlist.
    // The bundle has messages from both fungible and crowd-funding, but we only allow fungible.
    campaign_chain.options_mut().message_policy = MessagePolicy::new(
        BlanketMessagePolicy::Accept,
        None,
        None,
        Some([fungible_id.forget_abi().into()].into_iter().collect()),
    );
    let certs = campaign_chain.process_inbox().await?.0;
    assert_eq!(
        certs.len(),
        0,
        "Should reject bundle with message from non-allowed crowd-funding app"
    );

    // Test 5: Accept bundles when all app messages are in the allowlist.
    campaign_chain.options_mut().message_policy = MessagePolicy::new(
        BlanketMessagePolicy::Accept,
        None,
        None,
        Some(
            [
                fungible_id.forget_abi().into(),
                crowd_funding_id.forget_abi().into(),
            ]
            .into_iter()
            .collect(),
        ),
    );
    let certs = campaign_chain.process_inbox().await?.0;
    assert_eq!(
        certs.len(),
        1,
        "Should accept bundle when all app messages are allowed"
    );

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_fuel_limit(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    let storage_builder = MemoryStorageBuilder::with_wasm_runtime(wasm_runtime);
    // Set a fuel limit that is enough to instantiate the application and do one increment
    // operation, but not ten. We also verify blob fees for the bytecode.
    let policy = ResourceControlPolicy {
        maximum_wasm_fuel_per_block: 30_000,
        blob_read: Amount::from_tokens(10), // Should not be charged.
        blob_published: Amount::from_attos(100),
        blob_byte_read: Amount::from_tokens(10), // Should not be charged.
        blob_byte_published: Amount::from_attos(1),
        ..ResourceControlPolicy::default()
    };
    let keys = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(policy.clone());
    let publisher = builder.add_root_chain(0, Amount::from_tokens(3)).await?;

    let mut expected_balance = publisher.local_balance().await?;

    let module_id = publisher.publish_wasm_example("counter").await?;
    let module_id = module_id.with_abi::<counter::CounterAbi, (), u64>();
    let mut blobs = publisher
        .storage_client()
        .read_blobs(&[
            BlobId::new(module_id.contract_blob_hash, BlobType::ContractBytecode),
            BlobId::new(module_id.service_blob_hash, BlobType::ServiceBytecode),
        ])
        .await?
        .into_iter()
        .flatten();
    expected_balance = expected_balance
        - policy.blob_published * 2
        - policy.blob_byte_published
            * (blobs.next().unwrap().bytes().len() as u128
                + blobs.next().unwrap().bytes().len() as u128);
    assert_eq!(publisher.local_balance().await?, expected_balance);

    let initial_value = 10_u64;
    let (application_id, _) = publisher
        .create_application(module_id, &(), &initial_value, vec![])
        .await
        .unwrap_ok_committed();

    let increment = 5_u64;
    publisher
        .execute_operation(Operation::user(application_id, &increment)?)
        .await
        .unwrap_ok_committed();

    assert!(publisher
        .execute_operations(
            vec![Operation::user(application_id, &increment)?; 10],
            vec![]
        )
        .await
        .is_err());

    Ok(())
}

/// Tests that if a client synchronizes a shared chain from the validators and learns about
/// a proposal from another owner that it can't successfully execute locally anymore (e.g. due
/// to validation time-based oracles), it is still able to successfully propose new blocks.
/// Specifially, it doesn't try to propose in the same round as the failed conflicting proposal.
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_skipping_proposal(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_skipping_proposal(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_skipping_proposal<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let mut keys = InMemorySigner::new(None);
    let owner_a = keys.generate_new().into();
    let owner_b = keys.generate_new().into();
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, keys).await?;

    // We use the Hex game example: it uses the validation time-based assert_before oracle.
    let creator = builder.add_root_chain(0, Amount::from_tokens(3)).await?;
    let module_id = creator.publish_wasm_example("hex-game").await?;
    let module_id = module_id.with_abi::<HexAbi, (), Timeouts>();
    let timeouts = Timeouts::default();
    let (app_id, _) = creator
        .create_application(module_id, &(), &timeouts, vec![])
        .await
        .unwrap_ok_committed();

    // Start a game with players A and B.
    let start_op = HexOperation::Start {
        players: [owner_a, owner_b],
        board_size: 11,
        fee_budget: Amount::ONE,
        timeouts: None, // Use the default timeouts.
    };
    let cert = creator
        .execute_operation(Operation::user(app_id, &start_op)?)
        .await
        .unwrap_ok_committed();

    // Get the game chain ID from the certificate.
    let blobs = cert.inner().block().created_blobs();
    let chain_blob = blobs
        .values()
        .find(|blob| blob.content().blob_type() == BlobType::ChainDescription)
        .unwrap();
    let chain_id = bcs::from_bytes::<ChainDescription>(chain_blob.bytes())?.into();

    // Create clients for the new game chain. We need to register the chain with the owner in each
    // wallet so the client can participate in consensus (fetch manager values).
    builder.chain_owners.insert(chain_id, owner_a);
    let mut client_a = builder
        .make_client(chain_id, None, BlockHeight::ZERO)
        .await?;
    client_a.set_preferred_owner(owner_a);
    builder.chain_owners.insert(chain_id, owner_b);
    let mut client_b = builder
        .make_client(chain_id, None, BlockHeight::ZERO)
        .await?;
    client_b.set_preferred_owner(owner_b);

    // Client A makes the first move, starting the game at time 0.
    client_a.synchronize_from_validators().await?;
    let move_op = HexOperation::MakeMove { x: 5, y: 5 };
    client_a
        .execute_operation(Operation::user(app_id, &move_op)?)
        .await?;

    // Client B tries to make a move but fails: the validators go down after signing to validate.
    client_b.synchronize_from_validators().await?;
    builder.set_fault_type([0, 1, 2, 3], FaultType::DontProcessValidated);
    let move_op = HexOperation::MakeMove { x: 4, y: 4 };
    let result = client_b
        .execute_operation(Operation::user(app_id, &move_op)?)
        .await;
    assert_matches!(result, Err(ChainClientError::CommunicationError(_)));

    // Advance the clock so much that player B times out.
    clock.add(timeouts.start_time * 2);

    // Set the validators back to Honest.
    builder.set_fault_type([0, 1, 2, 3], FaultType::Honest);

    // Now player A claims victory since B has timed out. This works because it sees the existing
    // block proposal and makes a new proposal in the next round instead.
    let claim_victory_operation = HexOperation::ClaimVictory;
    client_a.synchronize_from_validators().await?;
    client_a
        .execute_operation(Operation::user(app_id, &claim_victory_operation)?)
        .await?;
    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_publish_read_data_blob(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_publish_read_data_blob(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "storage-service")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_service_publish_read_data_blob(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_publish_read_data_blob(ServiceStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_rocks_db_publish_read_data_blob(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_publish_read_data_blob(RocksDbStorageBuilder::with_wasm_runtime(wasm_runtime).await)
        .await
}

#[ignore]
#[cfg(feature = "dynamodb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dynamo_db_publish_read_data_blob(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_publish_read_data_blob(DynamoDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

#[ignore]
#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_scylla_db_publish_read_data_blob(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_publish_read_data_blob(ScyllaDbStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_publish_read_data_blob<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    use publish_read_data_blob::PublishReadDataBlobAbi;

    let keys = InMemorySigner::new(None);
    let mut builder = TestBuilder::new(storage_builder, 4, 1, keys)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());
    let client = builder.add_root_chain(0, Amount::from_tokens(3)).await?;

    let module_id = client
        .publish_wasm_example("publish-read-data-blob")
        .await?;
    let module_id = module_id.with_abi::<PublishReadDataBlobAbi, (), ()>();

    let (application_id, _) = client
        .create_application(module_id, &(), &(), vec![])
        .await
        .unwrap_ok_committed();

    // Method 1: Publishing and reading in different blocks.
    let test_data = b"This is test data for method 1.".to_vec();

    // publishing the data.
    let publish_op = publish_read_data_blob::Operation::CreateDataBlob(test_data.clone());
    let certificate = client
        .execute_operation(Operation::user(application_id, &publish_op)?)
        .await
        .unwrap_ok_committed();

    // getting the hash
    let content = BlobContent::new_data(test_data.clone());
    let hash = DataBlobHash(CryptoHash::new(&content));

    // reading and checking
    let read_op = publish_read_data_blob::Operation::ReadDataBlob(hash, test_data);
    client
        .execute_operation(Operation::user(application_id, &read_op)?)
        .await
        .unwrap_ok_committed();
    // None of the following blocks should have oracle responses: all read blobs were created
    // on the same chain, so no oracle is needed.
    assert_eq!(certificate.block().body.oracle_responses[0].len(), 0);

    // Method 2: Publishing and reading in the same transaction
    let test_data = b"This is test data for method 2.".to_vec();
    let combined_op = publish_read_data_blob::Operation::CreateAndReadDataBlob(test_data);
    let certificate = client
        .execute_operation(Operation::user(application_id, &combined_op)?)
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.block().body.oracle_responses[0].len(), 0);

    // Method 3: Publishing and reading in the same block but different transactions
    let test_data = b"This is test data for method 3.".to_vec();
    let publish_op = publish_read_data_blob::Operation::CreateDataBlob(test_data.clone());
    let content = BlobContent::new_data(test_data.clone());
    let hash = DataBlobHash(CryptoHash::new(&content));
    let read_op = publish_read_data_blob::Operation::ReadDataBlob(hash, test_data);
    let op1 = Operation::user(application_id, &publish_op)?;
    let op2 = Operation::user(application_id, &read_op)?;
    let certificate = client
        .execute_operations(vec![op1, op2], vec![])
        .await
        .unwrap_ok_committed();
    assert_eq!(certificate.block().body.oracle_responses[0].len(), 0);
    assert_eq!(certificate.block().body.oracle_responses[1].len(), 0);

    Ok(())
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_time_expiry_rounds(wasm_runtime: WasmRuntime) -> anyhow::Result<()> {
    run_test_time_expiry_rounds(MemoryStorageBuilder::with_wasm_runtime(wasm_runtime)).await
}

async fn run_test_time_expiry_rounds<B>(storage_builder: B) -> anyhow::Result<()>
where
    B: StorageBuilder,
{
    let keys = InMemorySigner::new(None);
    let clock = storage_builder.clock().clone();
    let mut builder = TestBuilder::new(storage_builder, 4, 0, keys)
        .await?
        .with_policy(ResourceControlPolicy::all_categories());

    let creator = builder.add_root_chain(0, Amount::from_tokens(3)).await?;

    // Publish and create the time-expiry application.
    let module_id = creator.publish_wasm_example("time-expiry").await?;
    let module_id = module_id.with_abi::<time_expiry::TimeExpiryAbi, (), ()>();
    let (app_id, _) = creator
        .create_application(module_id, &(), &(), vec![])
        .await
        .unwrap_ok_committed();

    // Set two validators offline so we can't reach a quorum.
    builder.set_fault_type([2, 3], FaultType::Offline);

    // Try to commit ExpireAfter(5 seconds) - should fail (no quorum).
    let op1 = time_expiry::TimeExpiryOperation::ExpireAfter(TimeDelta::from_secs(5));
    let result = creator
        .execute_operation(Operation::user(app_id, &op1)?)
        .await;
    assert_matches!(result, Err(ChainClientError::CommunicationError(_)));

    // The proposal should be in round MultiLeader(0).
    let chain_info = creator.chain_info_with_manager_values().await?;
    assert_eq!(
        chain_info.manager.requested_proposed.unwrap().content.round,
        Round::MultiLeader(0)
    );

    // Clear the pending proposal and try again with ExpireAfter(6 seconds).
    creator.clear_pending_proposal();
    let op2 = time_expiry::TimeExpiryOperation::ExpireAfter(TimeDelta::from_secs(6));
    let result = creator
        .execute_operation(Operation::user(app_id, &op2)?)
        .await;
    assert_matches!(result, Err(ChainClientError::CommunicationError(_)));

    // The proposal should now be in round MultiLeader(1).
    let chain_info = creator.chain_info_with_manager_values().await?;
    assert_eq!(
        chain_info.manager.requested_proposed.unwrap().content.round,
        Round::MultiLeader(1)
    );

    // Clear the pending proposal and try once more with ExpireAfter(7 seconds).
    creator.clear_pending_proposal();
    let op3 = time_expiry::TimeExpiryOperation::ExpireAfter(TimeDelta::from_secs(7));
    let result = creator
        .execute_operation(Operation::user(app_id, &op3)?)
        .await;
    assert_matches!(result, Err(ChainClientError::CommunicationError(_)));

    // The proposal should now be in round SingleLeader(0).
    let chain_info = creator.chain_info_with_manager_values().await?;
    assert_eq!(
        chain_info.manager.requested_proposed.unwrap().content.round,
        Round::SingleLeader(0)
    );

    // Make all validators honest again.
    builder.set_fault_type([0, 1, 2, 3], FaultType::Honest);

    // Advance the clock by 10 seconds.
    clock.add(TimeDelta::from_secs(10));

    // Clear pending and try to commit ExpireAfter(10 minutes) - should timeout.
    creator.clear_pending_proposal();
    let op4 = time_expiry::TimeExpiryOperation::ExpireAfter(TimeDelta::from_secs(600));
    let result = creator
        .execute_operation(Operation::user(app_id, &op4)?)
        .await?;
    // The operation should return WaitForTimeout because the block expired.
    assert_matches!(result, ClientOutcome::WaitForTimeout(_));

    // Retry to process the pending block a few times. It is not guaranteed in each attempt
    // that the client successfully updates enough validators before `communicate_with_quorum`
    // cancels the tasks, but eventually it will succeed and all validators will agree that the
    // round has timed out.
    let certificate = loop {
        clock.add(TimeDelta::from_secs(20));
        match creator.process_pending_block().await? {
            ClientOutcome::Committed(Some(cert)) => break cert,
            ClientOutcome::WaitForTimeout(_)
                if clock.current_time()
                    < Timestamp::from(0).saturating_add(TimeDelta::from_secs(200)) =>
            {
                continue
            }
            outcome => panic!("Failed to commit the block: {outcome:?}"),
        }
    };

    // Verify the certificate contains the "10 minutes" operation.
    let operations: Vec<_> = certificate.block().body.operations().collect();
    assert_eq!(operations.len(), 1);
    let operation = &operations[0];
    if let Operation::User {
        application_id,
        bytes,
    } = operation
    {
        assert_eq!(application_id, &app_id.forget_abi());
        let decoded_op: time_expiry::TimeExpiryOperation = bcs::from_bytes(bytes)?;
        assert_matches!(
            decoded_op,
            time_expiry::TimeExpiryOperation::ExpireAfter(delta) if delta == TimeDelta::from_secs(600)
        );
    } else {
        panic!("Expected a user operation");
    }

    Ok(())
}
