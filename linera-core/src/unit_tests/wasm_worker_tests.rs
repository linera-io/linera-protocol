// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! WASM specific worker tests.
//!
//! These tests only run if a WASM runtime has been configured by enabling either the `wasmer` or
//! the `wasmtime` feature flags.

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use super::{init_worker_with_chains, make_block, make_certificate, make_state_hash};
use linera_base::{
    crypto::KeyPair,
    data_types::{BlockHeight, ChainDescription, ChainId, EffectId, Epoch, Timestamp},
};
use linera_chain::data_types::{Event, HashedValue, Message, Origin, OutgoingEffect};
use linera_execution::{
    system::{Balance, SystemChannel, SystemEffect, SystemOperation},
    ApplicationId, ApplicationRegistry, Bytecode, BytecodeId, BytecodeLocation, ChainOwnership,
    ChannelId, Destination, Effect, ExecutionStateView, Operation, OperationContext,
    SystemExecutionState, UserApplicationDescription, UserApplicationId, WasmApplication,
    WasmRuntime,
};
use linera_storage::{MemoryStoreClient, RocksdbStoreClient, Store};
use linera_views::views::{CryptoHashView, ViewError};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use test_case::test_case;

#[cfg(feature = "aws")]
use {linera_storage::DynamoDbStoreClient, linera_views::test_utils::LocalStackTestContext};

#[derive(Copy, Clone, Debug)]
enum StorageKind {
    Simple,
    View,
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, StorageKind::Simple ; "wasmer_simple"))]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, StorageKind::View ; "wasmer_view"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, StorageKind::Simple ; "wasmtime_simple"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, StorageKind::View ; "wasmtime_view"))]
#[test_log::test(tokio::test)]
async fn test_memory_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
    storage_kind: StorageKind,
) -> Result<(), anyhow::Error> {
    let client = MemoryStoreClient::new(Some(wasm_runtime));
    run_test_handle_certificates_to_create_application(client, storage_kind).await
}

#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, StorageKind::Simple ; "wasmer_simple"))]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, StorageKind::View ; "wasmer_view"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, StorageKind::Simple ; "wasmtime_simple"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, StorageKind::View ; "wasmtime_view"))]
#[test_log::test(tokio::test)]
async fn test_rocksdb_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
    storage_kind: StorageKind,
) -> Result<(), anyhow::Error> {
    let dir = tempfile::TempDir::new().unwrap();
    let client = RocksdbStoreClient::new(dir.path().to_path_buf(), Some(wasm_runtime));
    run_test_handle_certificates_to_create_application(client, storage_kind).await
}

#[cfg(feature = "aws")]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, StorageKind::Simple ; "wasmer_simple"))]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, StorageKind::View ; "wasmer_view"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, StorageKind::Simple ; "wasmtime_simple"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, StorageKind::View ; "wasmtime_view"))]
#[test_log::test(tokio::test)]
async fn test_dynamo_db_handle_certificates_to_create_application(
    wasm_runtime: WasmRuntime,
    storage_kind: StorageKind,
) -> Result<(), anyhow::Error> {
    let table = "linera".parse().expect("Invalid table name");
    let localstack = LocalStackTestContext::new().await?;
    let (client, _) =
        DynamoDbStoreClient::from_config(localstack.dynamo_db_config(), table, Some(wasm_runtime))
            .await?;
    run_test_handle_certificates_to_create_application(client, storage_kind).await
}

async fn run_test_handle_certificates_to_create_application<S>(
    client: S,
    storage_kind: StorageKind,
) -> Result<(), anyhow::Error>
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    let admin_id = ChainDescription::Root(0);
    let publisher_key_pair = KeyPair::generate();
    let publisher_chain = ChainDescription::Root(1);
    let creator_key_pair = KeyPair::generate();
    let creator_chain = ChainDescription::Root(2);
    let (committee, mut worker) = init_worker_with_chains(
        client,
        vec![
            (
                publisher_chain,
                publisher_key_pair.public(),
                Balance::from(0),
            ),
            (creator_chain, creator_key_pair.public(), Balance::from(0)),
        ],
    )
    .await;

    // Load some bytecode.
    let name_counter = match storage_kind {
        StorageKind::Simple => "counter",
        StorageKind::View => "counter2",
    };
    let (contract_path, service_path) =
        linera_execution::wasm_test::get_example_bytecode_paths(name_counter)?;
    let contract_bytecode = Bytecode::load_from_file(contract_path).await?;
    let service_bytecode = Bytecode::load_from_file(service_path).await?;
    let application = Arc::new(WasmApplication::new(
        contract_bytecode.clone(),
        service_bytecode.clone(),
        WasmRuntime::default(),
    )?);

    // Publish some bytecode.
    let publish_operation = SystemOperation::PublishBytecode {
        contract: contract_bytecode,
        service: service_bytecode,
    };
    let publish_effect = SystemEffect::BytecodePublished { operation_index: 0 };
    let publish_block = make_block(
        Epoch::from(0),
        publisher_chain.into(),
        vec![publish_operation],
        vec![],
        None,
        None,
        Timestamp::from(1),
    );
    let publish_block_height = publish_block.height;
    let mut publisher_system_state = SystemExecutionState {
        epoch: Some(Epoch::from(0)),
        description: Some(publisher_chain),
        admin_id: Some(admin_id.into()),
        subscriptions: BTreeSet::new(),
        committees: [(Epoch::from(0), committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(publisher_key_pair.public().into()),
        balance: Balance::from(0),
        balances: BTreeMap::new(),
        timestamp: Timestamp::from(1),
        registry: ApplicationRegistry::default(),
    };
    let publisher_state_hash = make_state_hash(publisher_system_state.clone(), 10_000_000).await;
    let publish_block_proposal = HashedValue::new_confirmed(
        publish_block,
        vec![OutgoingEffect {
            application_id: ApplicationId::System,
            destination: Destination::Recipient(publisher_chain.into()),
            authenticated_signer: None,
            effect: Effect::System(publish_effect.clone()),
        }],
        publisher_state_hash,
    );
    let publish_certificate = make_certificate(&committee, &worker, publish_block_proposal);

    let info = worker
        .fully_handle_certificate(publish_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(publisher_chain), info.chain_id);
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Timestamp::from(1), info.timestamp);
    assert_eq!(Some(publish_certificate.value.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Produce one more block to broadcast the bytecode ID.
    let broadcast_message = Message {
        application_id: ApplicationId::System,
        origin: Origin::chain(publisher_chain.into()),
        event: Event {
            certificate_hash: publish_certificate.value.hash(),
            height: publish_block_height,
            index: 0,
            authenticated_signer: None,
            timestamp: Timestamp::from(1),
            effect: Effect::System(publish_effect),
        },
    };
    let broadcast_block = make_block(
        Epoch::from(0),
        publisher_chain.into(),
        Vec::<SystemOperation>::new(),
        vec![broadcast_message],
        Some(&publish_certificate),
        None,
        Timestamp::from(1),
    );
    let bytecode_id = BytecodeId(EffectId {
        chain_id: publisher_chain.into(),
        height: publish_block_height,
        index: 0,
    });
    let bytecode_location = BytecodeLocation {
        certificate_hash: publish_certificate.value.hash(),
        operation_index: 0,
    };
    let broadcast_effect = SystemEffect::BytecodeLocations {
        locations: vec![(bytecode_id, bytecode_location)],
    };
    let broadcast_channel = Destination::Subscribers(SystemChannel::PublishedBytecodes.name());
    let broadcast_block_height = broadcast_block.height;
    publisher_system_state
        .registry
        .published_bytecodes
        .insert(bytecode_id, bytecode_location);
    let publisher_state_hash = make_state_hash(publisher_system_state.clone(), 20_000_000).await;
    let broadcast_block_proposal = HashedValue::new_confirmed(
        broadcast_block,
        vec![OutgoingEffect {
            application_id: ApplicationId::System,
            destination: broadcast_channel,
            authenticated_signer: None,
            effect: Effect::System(broadcast_effect.clone()),
        }],
        publisher_state_hash,
    );
    let broadcast_certificate = make_certificate(&committee, &worker, broadcast_block_proposal);

    let info = worker
        .fully_handle_certificate(broadcast_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(publisher_chain), info.chain_id);
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(2), info.next_block_height);
    assert_eq!(Timestamp::from(1), info.timestamp);
    assert_eq!(Some(broadcast_certificate.value.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Subscribe to get the bytecode ID.
    let subscribe_operation = SystemOperation::Subscribe {
        chain_id: publisher_chain.into(),
        channel: SystemChannel::PublishedBytecodes,
    };
    let publisher_channel = ChannelId {
        chain_id: publisher_chain.into(),
        name: SystemChannel::PublishedBytecodes.name(),
    };
    let subscribe_effect = SystemEffect::Subscribe {
        id: creator_chain.into(),
        channel_id: publisher_channel.clone(),
    };
    let subscribe_block = make_block(
        Epoch::from(0),
        creator_chain.into(),
        vec![subscribe_operation],
        vec![],
        None,
        None,
        Timestamp::from(2),
    );
    let subscribe_block_height = subscribe_block.height;
    let mut creator_system_state = SystemExecutionState {
        epoch: Some(Epoch::from(0)),
        description: Some(creator_chain),
        admin_id: Some(admin_id.into()),
        subscriptions: [publisher_channel].into_iter().collect(),
        committees: [(Epoch::from(0), committee.clone())].into_iter().collect(),
        ownership: ChainOwnership::single(creator_key_pair.public().into()),
        balance: Balance::from(0),
        balances: BTreeMap::new(),
        timestamp: Timestamp::from(2),
        registry: ApplicationRegistry::default(),
    };
    let creator_state = ExecutionStateView::from_system_state(creator_system_state.clone())
        .await
        .with_fuel(10_000_000);
    let subscribe_block_proposal = HashedValue::new_confirmed(
        subscribe_block,
        vec![OutgoingEffect {
            application_id: ApplicationId::System,
            destination: Destination::Recipient(publisher_chain.into()),
            authenticated_signer: None,
            effect: Effect::System(subscribe_effect.clone()),
        }],
        creator_state.crypto_hash().await?,
    );
    let subscribe_certificate = make_certificate(&committee, &worker, subscribe_block_proposal);

    let info = worker
        .fully_handle_certificate(subscribe_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(creator_chain), info.chain_id);
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(1), info.next_block_height);
    assert_eq!(Timestamp::from(2), info.timestamp);
    assert_eq!(Some(subscribe_certificate.value.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Accept subscription
    let accept_message = Message {
        application_id: ApplicationId::System,
        origin: Origin::chain(creator_chain.into()),
        event: Event {
            certificate_hash: subscribe_certificate.value.hash(),
            height: subscribe_block_height,
            index: 0,
            authenticated_signer: None,
            timestamp: Timestamp::from(2),
            effect: subscribe_effect.into(),
        },
    };
    let accept_block = make_block(
        Epoch::from(0),
        publisher_chain.into(),
        Vec::<SystemOperation>::new(),
        vec![accept_message],
        Some(&broadcast_certificate),
        None,
        Timestamp::from(3),
    );
    publisher_system_state.timestamp = Timestamp::from(3);
    let publisher_state_hash = make_state_hash(publisher_system_state, 30_000_000).await;
    let accept_block_proposal = HashedValue::new_confirmed(
        accept_block,
        vec![OutgoingEffect {
            application_id: ApplicationId::System,
            destination: Destination::Recipient(creator_chain.into()),
            authenticated_signer: None,
            effect: Effect::System(SystemEffect::Notify {
                id: creator_chain.into(),
            }),
        }],
        publisher_state_hash,
    );
    let accept_certificate = make_certificate(&committee, &worker, accept_block_proposal);

    let info = worker
        .fully_handle_certificate(accept_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::from(publisher_chain), info.chain_id);
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(3), info.next_block_height);
    assert_eq!(Timestamp::from(3), info.timestamp);
    assert_eq!(Some(accept_certificate.value.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Create an application.
    let initial_value = 10_u128;
    let initial_value_bytes = bcs::to_bytes(&initial_value)?;
    let create_operation = SystemOperation::CreateApplication {
        bytecode_id,
        parameters: vec![],
        initialization_argument: initial_value_bytes.clone(),
        required_application_ids: vec![],
    };
    let application_id = UserApplicationId {
        bytecode_id,
        creation: EffectId {
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
        parameters: vec![],
    };
    let create_block = make_block(
        Epoch::from(0),
        creator_chain.into(),
        vec![create_operation],
        vec![Message {
            application_id: ApplicationId::System,
            origin: Origin::channel(
                publisher_chain.into(),
                SystemChannel::PublishedBytecodes.name(),
            ),
            event: Event {
                certificate_hash: broadcast_certificate.value.hash(),
                height: broadcast_block_height,
                index: 0,
                authenticated_signer: None,
                timestamp: Timestamp::from(1),
                effect: Effect::System(broadcast_effect),
            },
        }],
        Some(&subscribe_certificate),
        None,
        Timestamp::from(4),
    );
    creator_system_state
        .registry
        .published_bytecodes
        .insert(bytecode_id, bytecode_location);
    creator_system_state
        .registry
        .known_applications
        .insert(application_id, application_description.clone());
    creator_system_state.timestamp = Timestamp::from(4);
    let mut creator_state = ExecutionStateView::from_system_state(creator_system_state)
        .await
        .with_fuel(20_000_000);
    creator_state
        .simulate_initialization(
            application,
            application_description,
            initial_value_bytes.clone(),
        )
        .await?;
    let create_block_proposal =
        HashedValue::new_confirmed(create_block, vec![], creator_state.crypto_hash().await?);
    let create_certificate = make_certificate(&committee, &worker, create_block_proposal);

    let info = worker
        .fully_handle_certificate(create_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(2), info.chain_id);
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(2), info.next_block_height);
    assert_eq!(Timestamp::from(4), info.timestamp);
    assert_eq!(Some(create_certificate.value.hash()), info.block_hash);
    assert!(info.manager.pending().is_none());

    // Execute an application operation
    let increment = 5_u128;
    let user_operation = bcs::to_bytes(&increment)?;
    let run_block = make_block(
        Epoch::from(0),
        creator_chain.into(),
        vec![(application_id, user_operation.clone())],
        vec![],
        Some(&create_certificate),
        None,
        Timestamp::from(5),
    );
    let operation_context = OperationContext {
        chain_id: creator_chain.into(),
        authenticated_signer: None,
        height: run_block.height,
        index: 0,
    };
    creator_state.add_fuel(10_000_000);
    creator_state
        .execute_operation(
            ApplicationId::User(application_id),
            &operation_context,
            &Operation::User(user_operation),
        )
        .await?;
    creator_state.system.timestamp.set(Timestamp::from(5));
    let run_block_proposal =
        HashedValue::new_confirmed(run_block, vec![], creator_state.crypto_hash().await?);
    let run_certificate = make_certificate(&committee, &worker, run_block_proposal);

    let info = worker
        .fully_handle_certificate(run_certificate.clone(), vec![])
        .await
        .unwrap()
        .info;
    assert_eq!(ChainId::root(2), info.chain_id);
    assert_eq!(Balance::from(0), info.system_balance);
    assert_eq!(BlockHeight::from(3), info.next_block_height);
    assert_eq!(Some(run_certificate.value.hash()), info.block_hash);
    assert_eq!(Timestamp::from(5), info.timestamp);
    assert!(info.manager.pending().is_none());
    Ok(())
}
