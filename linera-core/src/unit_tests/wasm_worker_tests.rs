// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Wasm specific worker tests.
//!
//! These tests only run if a Wasm runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use crate::worker::worker_tests::make_state;

use super::{init_worker_with_chains, make_certificate, make_state_hash};
use linera_base::{
    crypto::KeyPair,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{BytecodeId, ChainDescription, ChainId, Destination, MessageId},
};
use linera_chain::{
    data_types::{
        ChannelFullName, Event, ExecutedBlock, HashedValue, IncomingMessage, MessageAction, Origin,
        OutgoingMessage,
    },
    test::{make_child_block, make_first_block, BlockTestExt},
};
use linera_execution::{
    committee::Epoch,
    policy::ResourceControlPolicy,
    system::{SystemChannel, SystemMessage, SystemOperation},
    Bytecode, BytecodeLocation, ChainOwnership, ChannelSubscription, ExecutionRuntimeConfig,
    ExecutionStateView, GenericApplicationId, Message, Operation, OperationContext,
    ResourceTracker, SystemExecutionState, UserApplicationDescription, UserApplicationId,
    WasmContractModule, WasmRuntime,
};
use linera_storage::{MemoryStorage, Storage};
use linera_views::views::{CryptoHashView, ViewError};
use std::sync::Arc;
use test_case::test_case;

#[cfg(feature = "rocksdb")]
use linera_storage::RocksDbStorage;

#[cfg(feature = "aws")]
use linera_storage::DynamoDbStorage;

#[cfg(feature = "scylladb")]
use linera_storage::ScyllaDbStorage;

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_memory_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    let storage = MemoryStorage::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

#[cfg(feature = "rocksdb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_rocks_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    let storage = RocksDbStorage::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

#[cfg(feature = "aws")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_dynamo_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    let storage = DynamoDbStorage::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

#[cfg(feature = "scylladb")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer ; "wasmer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime ; "wasmtime"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_scylla_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error> {
    let storage = ScyllaDbStorage::make_test_storage(Some(wasm_runtime)).await;
    run_test_handle_certificates_to_create_application(storage, wasm_runtime).await
}

async fn run_test_handle_certificates_to_create_application<S>(
    storage: S,
    wasm_runtime: WasmRuntime,
) -> Result<(), anyhow::Error>
where
    S: Storage + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let admin_id = ChainDescription::Root(0);
    let publisher_key_pair = KeyPair::generate();
    let publisher_chain = ChainDescription::Root(1);
    let creator_key_pair = KeyPair::generate();
    let creator_chain = ChainDescription::Root(2);
    let (committee, mut worker) = init_worker_with_chains(
        storage,
        vec![
            (publisher_chain, publisher_key_pair.public(), Amount::ZERO),
            (creator_chain, creator_key_pair.public(), Amount::ZERO),
        ],
    )
    .await;

    // Load some bytecode.
    let (contract_path, service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths("counter")?;
    let contract_bytecode = Bytecode::load_from_file(contract_path).await?;
    let service_bytecode = Bytecode::load_from_file(service_path).await?;
    let contract =
        Arc::new(WasmContractModule::new(contract_bytecode.clone(), wasm_runtime).await?);

    // Publish some bytecode.
    let publish_operation = SystemOperation::PublishBytecode {
        contract: contract_bytecode,
        service: service_bytecode,
    };
    let publish_message = SystemMessage::BytecodePublished { operation_index: 0 };
    let publish_block = make_first_block(publisher_chain.into())
        .with_timestamp(1)
        .with_operation(publish_operation);
    let publish_block_height = publish_block.height;
    let mut publisher_system_state = SystemExecutionState {
        committees: [(Epoch::ZERO, committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(publisher_key_pair.public()),
        timestamp: Timestamp::from(1),
        ..make_state(Epoch::ZERO, publisher_chain, admin_id)
    };
    let publisher_state_hash = make_state_hash(publisher_system_state.clone()).await;
    let publish_block_proposal = HashedValue::new_confirmed(ExecutedBlock {
        block: publish_block,
        messages: vec![OutgoingMessage {
            destination: Destination::Recipient(publisher_chain.into()),
            authenticated_signer: None,
            is_protected: true,
            message: Message::System(publish_message.clone()),
        }],
        message_counts: vec![1],
        state_hash: publisher_state_hash,
    });
    let publish_certificate = make_certificate(&committee, &worker, publish_block_proposal);

    let info = worker
        .fully_handle_certificate(publish_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(publisher_chain), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Timestamp::from(1), info.timestamp);
    assert_eq!(Some(publish_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Produce one more block to broadcast the bytecode ID.
    let broadcast_message = IncomingMessage {
        origin: Origin::chain(publisher_chain.into()),
        event: Event {
            certificate_hash: publish_certificate.hash(),
            height: publish_block_height,
            index: 0,
            authenticated_signer: None,
            is_protected: true,
            timestamp: Timestamp::from(1),
            message: Message::System(publish_message),
        },
        action: MessageAction::Accept,
    };
    let broadcast_block = make_child_block(&publish_certificate.value)
        .with_timestamp(1)
        .with_incoming_message(broadcast_message);
    let bytecode_id = BytecodeId::new(MessageId {
        chain_id: publisher_chain.into(),
        height: publish_block_height,
        index: 0,
    });
    let bytecode_location = BytecodeLocation {
        certificate_hash: publish_certificate.hash(),
        operation_index: 0,
    };
    let broadcast_message = SystemMessage::BytecodeLocations {
        locations: vec![(bytecode_id, bytecode_location)],
    };
    let broadcast_channel = Destination::Subscribers(SystemChannel::PublishedBytecodes.name());
    let broadcast_block_height = broadcast_block.height;
    publisher_system_state
        .registry
        .published_bytecodes
        .insert(bytecode_id, bytecode_location);
    let publisher_state_hash = make_state_hash(publisher_system_state.clone()).await;
    let broadcast_block_proposal = HashedValue::new_confirmed(ExecutedBlock {
        block: broadcast_block,
        messages: vec![OutgoingMessage {
            destination: broadcast_channel,
            authenticated_signer: None,
            is_protected: false,
            message: Message::System(broadcast_message.clone()),
        }],
        message_counts: vec![1],
        state_hash: publisher_state_hash,
    });
    let broadcast_certificate = make_certificate(&committee, &worker, broadcast_block_proposal);

    let info = worker
        .fully_handle_certificate(broadcast_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(publisher_chain), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(2), info.next_block_height);
    assert_eq!(Timestamp::from(1), info.timestamp);
    assert_eq!(Some(broadcast_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Subscribe to get the bytecode ID.
    let subscribe_operation = SystemOperation::Subscribe {
        chain_id: publisher_chain.into(),
        channel: SystemChannel::PublishedBytecodes,
    };
    let publisher_channel = ChannelSubscription {
        chain_id: publisher_chain.into(),
        name: SystemChannel::PublishedBytecodes.name(),
    };
    let subscribe_message = SystemMessage::Subscribe {
        id: creator_chain.into(),
        subscription: publisher_channel.clone(),
    };
    let subscribe_block = make_first_block(creator_chain.into())
        .with_timestamp(2)
        .with_operation(subscribe_operation);
    let subscribe_block_height = subscribe_block.height;
    let mut creator_system_state = SystemExecutionState {
        subscriptions: [publisher_channel].into_iter().collect(),
        committees: [(Epoch::ZERO, committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(creator_key_pair.public()),
        timestamp: Timestamp::from(2),
        ..make_state(Epoch::ZERO, creator_chain, admin_id)
    };
    let creator_state = ExecutionStateView::from_system_state(
        creator_system_state.clone(),
        ExecutionRuntimeConfig::default(),
    )
    .await;
    let subscribe_block_proposal = HashedValue::new_confirmed(ExecutedBlock {
        block: subscribe_block,
        messages: vec![OutgoingMessage {
            destination: Destination::Recipient(publisher_chain.into()),
            authenticated_signer: None,
            is_protected: true,
            message: Message::System(subscribe_message.clone()),
        }],
        message_counts: vec![1],
        state_hash: creator_state.crypto_hash().await?,
    });
    let subscribe_certificate = make_certificate(&committee, &worker, subscribe_block_proposal);

    let info = worker
        .fully_handle_certificate(subscribe_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(creator_chain), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Timestamp::from(2), info.timestamp);
    assert_eq!(Some(subscribe_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Accept subscription
    let accept_message = IncomingMessage {
        origin: Origin::chain(creator_chain.into()),
        event: Event {
            certificate_hash: subscribe_certificate.hash(),
            height: subscribe_block_height,
            index: 0,
            authenticated_signer: None,
            is_protected: true,
            timestamp: Timestamp::from(2),
            message: subscribe_message.into(),
        },
        action: MessageAction::Accept,
    };
    let accept_block = make_child_block(&broadcast_certificate.value)
        .with_timestamp(3)
        .with_incoming_message(accept_message);
    publisher_system_state.timestamp = Timestamp::from(3);
    let publisher_state_hash = make_state_hash(publisher_system_state).await;
    let accept_block_proposal = HashedValue::new_confirmed(ExecutedBlock {
        block: accept_block,
        messages: vec![OutgoingMessage {
            destination: Destination::Recipient(creator_chain.into()),
            authenticated_signer: None,
            is_protected: true,
            message: Message::System(SystemMessage::Notify {
                id: creator_chain.into(),
            }),
        }],
        message_counts: vec![1],
        state_hash: publisher_state_hash,
    });
    let accept_certificate = make_certificate(&committee, &worker, accept_block_proposal);

    let info = worker
        .fully_handle_certificate(accept_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(publisher_chain), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(3), info.next_block_height);
    assert_eq!(Timestamp::from(3), info.timestamp);
    assert_eq!(Some(accept_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Create an application.
    let initial_value = 10_u64;
    let initial_value_bytes = serde_json::to_vec(&initial_value)?;
    let parameters_bytes = serde_json::to_vec(&())?;
    let create_operation = SystemOperation::CreateApplication {
        bytecode_id,
        parameters: parameters_bytes.clone(),
        initialization_argument: initial_value_bytes.clone(),
        required_application_ids: vec![],
    };
    let application_id = UserApplicationId {
        bytecode_id,
        creation: MessageId {
            chain_id: creator_chain.into(),
            height: BlockHeight::from(1),
            index: 0,
        },
    };
    let application_description = UserApplicationDescription {
        bytecode_id,
        bytecode_location,
        creation: application_id.creation,
        required_application_ids: vec![],
        parameters: parameters_bytes,
    };
    let publish_admin_channel = ChannelFullName {
        application_id: GenericApplicationId::System,
        name: SystemChannel::PublishedBytecodes.name(),
    };
    let create_block = make_child_block(&subscribe_certificate.value)
        .with_timestamp(4)
        .with_operation(create_operation)
        .with_incoming_message(IncomingMessage {
            origin: Origin::channel(publisher_chain.into(), publish_admin_channel),
            event: Event {
                certificate_hash: broadcast_certificate.hash(),
                height: broadcast_block_height,
                index: 0,
                authenticated_signer: None,
                is_protected: false,
                timestamp: Timestamp::from(1),
                message: Message::System(broadcast_message),
            },
            action: MessageAction::Accept,
        });
    creator_system_state
        .registry
        .published_bytecodes
        .insert(bytecode_id, bytecode_location);
    creator_system_state
        .registry
        .known_applications
        .insert(application_id, application_description.clone());
    creator_system_state.timestamp = Timestamp::from(4);
    let mut creator_state = ExecutionStateView::from_system_state(
        creator_system_state,
        ExecutionRuntimeConfig::default(),
    )
    .await;
    creator_state
        .simulate_initialization(
            contract,
            application_description,
            initial_value_bytes.clone(),
        )
        .await?;
    let create_block_proposal = HashedValue::new_confirmed(ExecutedBlock {
        block: create_block,
        messages: vec![OutgoingMessage {
            destination: Destination::Recipient(creator_chain.into()),
            authenticated_signer: None,
            is_protected: true,
            message: Message::System(SystemMessage::ApplicationCreated),
        }],
        message_counts: vec![0, 1],
        state_hash: creator_state.crypto_hash().await?,
    });
    let create_certificate = make_certificate(&committee, &worker, create_block_proposal);

    let info = worker
        .fully_handle_certificate(create_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(2), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(2), info.next_block_height);
    assert_eq!(Timestamp::from(4), info.timestamp);
    assert_eq!(Some(create_certificate.hash()), info.block_hash);
    assert!(info.manager.pending.is_none());

    // Execute an application operation
    let increment = 5_u64;
    let user_operation = bcs::to_bytes(&increment)?;
    let run_block = make_child_block(&create_certificate.value)
        .with_timestamp(5)
        .with_operation(Operation::User {
            application_id,
            bytes: user_operation.clone(),
        });
    let operation_context = OperationContext {
        chain_id: creator_chain.into(),
        authenticated_signer: None,
        height: run_block.height,
        index: 0,
        next_message_index: 0,
    };
    let mut tracker = ResourceTracker::default();
    let policy = ResourceControlPolicy::default();
    creator_state
        .execute_operation(
            operation_context,
            Operation::User {
                application_id,
                bytes: user_operation,
            },
            &policy,
            &mut tracker,
        )
        .await?;
    creator_state.system.timestamp.set(Timestamp::from(5));
    let run_block_proposal = HashedValue::new_confirmed(ExecutedBlock {
        block: run_block,
        messages: vec![],
        message_counts: vec![0],
        state_hash: creator_state.crypto_hash().await?,
    });
    let run_certificate = make_certificate(&committee, &worker, run_block_proposal);

    let info = worker
        .fully_handle_certificate(run_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(2), info.chain_id);
    assert_eq!(Amount::ZERO, info.system_balance);
    assert_eq!(BlockHeight::from(3), info.next_block_height);
    assert_eq!(Some(run_certificate.hash()), info.block_hash);
    assert_eq!(Timestamp::from(5), info.timestamp);
    assert!(info.manager.pending.is_none());
    Ok(())
}
