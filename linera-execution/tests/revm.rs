// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(with_revm)]

use std::sync::Arc;

use alloy_sol_types::{sol, SolCall, SolValue};
use linera_base::{
    data_types::{Amount, Blob, BlockHeight, Timestamp},
    vm::EvmQuery,
};
use linera_execution::{
    evm::revm::{EvmContractModule, EvmServiceModule},
    test_utils::{
        create_dummy_user_application_description, dummy_chain_description,
        solidity::{load_solidity_example, read_evm_u64_entry},
        SystemExecutionState,
    },
    ExecutionRuntimeConfig, ExecutionRuntimeContext, Operation, OperationContext, Query,
    QueryContext, QueryResponse, ResourceControlPolicy, ResourceController, ResourceTracker,
    TransactionTracker,
};
use linera_views::{context::Context as _, views::View};

#[tokio::test]
async fn test_fuel_for_counter_revm_application() -> anyhow::Result<()> {
    let module = load_solidity_example("tests/fixtures/evm_example_counter.sol")?;

    sol! {
        struct ConstructorArgs {
            uint64 initial_value;
        }
        function increment(uint64 input);
        function get_value();
    }

    let initial_value = 10000;
    let mut value = initial_value;
    let args = ConstructorArgs { initial_value };
    let constructor_argument = args.abi_encode();
    let constructor_argument = serde_json::to_string(&constructor_argument)?.into_bytes();
    let instantiation_argument = Vec::<u8>::new();
    let instantiation_argument = serde_json::to_string(&instantiation_argument)?.into_bytes();
    let state = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..Default::default()
    };
    let (mut app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
    app_desc.parameters = constructor_argument;
    let chain_id = app_desc.creator_chain_id;
    let mut view = state
        .into_view_with(chain_id, ExecutionRuntimeConfig::default())
        .await;
    let app_id = From::from(&app_desc);
    let app_desc_blob_id = Blob::new_application_description(&app_desc).id();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let contract = EvmContractModule::Revm {
        module: module.clone(),
    };
    view.context()
        .extra()
        .user_contracts()
        .insert(app_id, contract.clone().into());

    let service = EvmServiceModule::Revm { module };
    view.context()
        .extra()
        .user_services()
        .insert(app_id, service.into());

    view.simulate_instantiation(
        contract.into(),
        Timestamp::from(2),
        app_desc,
        instantiation_argument,
        contract_blob,
        service_blob,
    )
    .await?;

    let operation_context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_signer: None,
        timestamp: Default::default(),
    };

    let query_context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };

    let increments = [2_u64, 9_u64, 7_u64, 1000_u64];
    let policy = ResourceControlPolicy {
        evm_fuel_unit: Amount::from_attos(1),
        ..ResourceControlPolicy::default()
    };
    let amount = Amount::from_tokens(1);
    *view.system.balance.get_mut() = amount;
    let mut controller =
        ResourceController::new(Arc::new(policy), ResourceTracker::default(), None);
    for increment in &increments {
        let mut txn_tracker = TransactionTracker::new_replaying_blobs([
            app_desc_blob_id,
            contract_blob_id,
            service_blob_id,
        ]);
        value += increment;
        let operation = incrementCall { input: *increment };
        let bytes = operation.abi_encode();
        let operation = Operation::User {
            application_id: app_id,
            bytes,
        };
        view.execute_operation(
            operation_context,
            operation,
            &mut txn_tracker,
            &mut controller,
        )
        .await?;

        let query = get_valueCall {};
        let query = query.abi_encode();
        let query = EvmQuery::Query(query);
        let bytes = serde_json::to_vec(&query)?;

        let query = Query::User {
            application_id: app_id,
            bytes,
        };

        let result = view.query_application(query_context, query, None).await?;

        let QueryResponse::User(result) = result.response else {
            anyhow::bail!("Wrong QueryResponse result");
        };
        let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
        let result = read_evm_u64_entry(result);
        assert_eq!(result, value);
    }
    Ok(())
}

#[tokio::test]
async fn test_terminate_execute_operation_by_lack_of_fuel() -> anyhow::Result<()> {
    let module = load_solidity_example("tests/fixtures/evm_example_counter.sol")?;

    sol! {
        struct ConstructorArgs {
            uint64 initial_value;
        }
        function increment(uint64 input);
        function get_value();
    }

    let initial_value = 10000;
    let args = ConstructorArgs { initial_value };
    let constructor_argument = args.abi_encode();
    let constructor_argument = serde_json::to_string(&constructor_argument)?.into_bytes();
    let instantiation_argument = Vec::<u8>::new();
    let instantiation_argument = serde_json::to_string(&instantiation_argument)?.into_bytes();
    let state = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..Default::default()
    };
    let (mut app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
    app_desc.parameters = constructor_argument;
    let chain_id = app_desc.creator_chain_id;
    let mut view = state
        .into_view_with(chain_id, ExecutionRuntimeConfig::default())
        .await;
    let app_id = From::from(&app_desc);
    let app_desc_blob_id = Blob::new_application_description(&app_desc).id();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let contract = EvmContractModule::Revm {
        module: module.clone(),
    };
    view.context()
        .extra()
        .user_contracts()
        .insert(app_id, contract.clone().into());

    let service = EvmServiceModule::Revm { module };
    view.context()
        .extra()
        .user_services()
        .insert(app_id, service.into());

    view.simulate_instantiation(
        contract.into(),
        Timestamp::from(2),
        app_desc,
        instantiation_argument,
        contract_blob,
        service_blob,
    )
    .await?;

    let operation_context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_signer: None,
        timestamp: Default::default(),
    };

    let policy = ResourceControlPolicy {
        evm_fuel_unit: Amount::from_attos(1),
        maximum_evm_fuel_per_block: 20000,
        ..ResourceControlPolicy::default()
    };
    let amount = Amount::from_tokens(1);
    *view.system.balance.get_mut() = amount;
    let mut controller =
        ResourceController::new(Arc::new(policy), ResourceTracker::default(), None);

    // Trying the increment, should fail
    let mut txn_tracker = TransactionTracker::new_replaying_blobs([
        app_desc_blob_id,
        contract_blob_id,
        service_blob_id,
    ]);
    let input = 2;
    let operation = incrementCall { input };
    let bytes = operation.abi_encode();
    let operation = Operation::User {
        application_id: app_id,
        bytes,
    };
    let result = view
        .execute_operation(
            operation_context,
            operation,
            &mut txn_tracker,
            &mut controller,
        )
        .await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_terminate_query_by_lack_of_fuel() -> anyhow::Result<()> {
    let module = load_solidity_example("tests/fixtures/evm_too_long_service.sol")?;

    sol! {
        struct ConstructorArgs {
            uint64 initial_value;
        }
        function too_long_run();
    }

    let args = ConstructorArgs { initial_value: 0 };
    let constructor_argument = args.abi_encode();
    let constructor_argument = serde_json::to_string(&constructor_argument)?.into_bytes();
    let instantiation_argument = Vec::<u8>::new();
    let instantiation_argument = serde_json::to_string(&instantiation_argument)?.into_bytes();
    let state = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..Default::default()
    };
    let (mut app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
    app_desc.parameters = constructor_argument;
    let chain_id = app_desc.creator_chain_id;
    let mut view = state
        .into_view_with(chain_id, ExecutionRuntimeConfig::default())
        .await;
    let app_id = From::from(&app_desc);

    let contract = EvmContractModule::Revm {
        module: module.clone(),
    };
    view.context()
        .extra()
        .user_contracts()
        .insert(app_id, contract.clone().into());

    let service = EvmServiceModule::Revm { module };
    view.context()
        .extra()
        .user_services()
        .insert(app_id, service.into());

    view.simulate_instantiation(
        contract.into(),
        Timestamp::from(2),
        app_desc,
        instantiation_argument,
        contract_blob,
        service_blob,
    )
    .await?;

    let query_context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };

    let amount = Amount::from_tokens(1);
    *view.system.balance.get_mut() = amount;

    // Trying to read the value, should fail
    let query = too_long_runCall {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);
    let bytes = serde_json::to_vec(&query)?;

    let query = Query::User {
        application_id: app_id,
        bytes,
    };

    let result = view.query_application(query_context, query, None).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_basic_evm_features() -> anyhow::Result<()> {
    let module = load_solidity_example("tests/fixtures/evm_basic_check.sol")?;

    sol! {
        function failing_function();
        function test_precompile_sha256();
        function check_contract_address(address evm_address);
    }

    let constructor_argument = Vec::<u8>::new();
    let constructor_argument = serde_json::to_string(&constructor_argument)?.into_bytes();
    let instantiation_argument = Vec::<u8>::new();
    let instantiation_argument = serde_json::to_string(&instantiation_argument)?.into_bytes();
    let state = SystemExecutionState {
        description: Some(dummy_chain_description(0)),
        ..Default::default()
    };
    let (mut app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
    app_desc.parameters = constructor_argument;
    let chain_id = app_desc.creator_chain_id;
    let mut view = state
        .into_view_with(chain_id, ExecutionRuntimeConfig::default())
        .await;
    let app_id = From::from(&app_desc);
    let app_desc_blob_id = Blob::new_application_description(&app_desc).id();
    let contract_blob_id = contract_blob.id();
    let service_blob_id = service_blob.id();

    let contract = EvmContractModule::Revm {
        module: module.clone(),
    };
    view.context()
        .extra()
        .user_contracts()
        .insert(app_id, contract.clone().into());

    let service = EvmServiceModule::Revm { module };
    view.context()
        .extra()
        .user_services()
        .insert(app_id, service.into());

    view.simulate_instantiation(
        contract.into(),
        Timestamp::from(2),
        app_desc,
        instantiation_argument,
        contract_blob,
        service_blob,
    )
    .await?;

    let operation_context = OperationContext {
        chain_id,
        height: BlockHeight(0),
        round: Some(0),
        authenticated_signer: None,
        timestamp: Default::default(),
    };

    let policy = ResourceControlPolicy {
        evm_fuel_unit: Amount::from_attos(1),
        maximum_evm_fuel_per_block: 20000,
        ..ResourceControlPolicy::default()
    };
    let mut controller =
        ResourceController::new(Arc::new(policy), ResourceTracker::default(), None);
    let mut txn_tracker = TransactionTracker::new_replaying_blobs([
        app_desc_blob_id,
        contract_blob_id,
        service_blob_id,
    ]);
    let query_context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };

    // Trying a failing function, should be an error
    let operation = failing_functionCall {};
    let bytes = operation.abi_encode();
    let operation = Operation::User {
        application_id: app_id,
        bytes,
    };
    let result = view
        .execute_operation(
            operation_context,
            operation,
            &mut txn_tracker,
            &mut controller,
        )
        .await;
    assert!(result.is_err());

    // Trying a call to an ethereum precompile function
    let query = test_precompile_sha256Call {};
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);
    let bytes = serde_json::to_vec(&query)?;

    let query = Query::User {
        application_id: app_id,
        bytes,
    };

    let result = view.query_application(query_context, query, None).await?;

    let QueryResponse::User(result) = result.response else {
        anyhow::bail!("Wrong QueryResponse result");
    };
    let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(read_evm_u64_entry(result), 0);

    // Testing that the created contract has the right address
    let evm_address = app_id.evm_address();
    let query = check_contract_addressCall { evm_address };
    let query = query.abi_encode();
    let query = EvmQuery::Query(query);
    let bytes = serde_json::to_vec(&query)?;

    let query = Query::User {
        application_id: app_id,
        bytes,
    };

    let result = view.query_application(query_context, query, None).await?;

    let QueryResponse::User(result) = result.response else {
        anyhow::bail!("Wrong QueryResponse result");
    };
    let result: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(read_evm_u64_entry(result), 49);

    Ok(())
}
