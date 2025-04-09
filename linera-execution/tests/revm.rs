// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(with_revm)]

use std::sync::Arc;

use alloy_sol_types::{sol, SolCall, SolValue};
use linera_base::{
    data_types::{Amount, Blob, BlockHeight, Timestamp},
    identifiers::ChainDescription,
    vm::EvmQuery,
};
use linera_execution::{
    revm::{EvmContractModule, EvmServiceModule},
    test_utils::{
        create_dummy_user_application_description,
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
    let instantiation_argument = args.abi_encode();
    let instantiation_argument = serde_json::to_string(&instantiation_argument)?.into_bytes();

    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..Default::default()
    };
    let (app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
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
        index: Some(0),
        authenticated_signer: None,
        authenticated_caller_id: None,
    };

    let query_context = QueryContext {
        chain_id,
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };

    let increments = [2_u64, 9_u64, 7_u64, 1000_u64];
    let policy = ResourceControlPolicy {
        fuel_unit: Amount::from_attos(1),
        ..ResourceControlPolicy::default()
    };
    let amount = Amount::from_tokens(1);
    *view.system.balance.get_mut() = amount;
    let mut controller = ResourceController {
        policy: Arc::new(policy),
        tracker: ResourceTracker::default(),
        account: None,
    };
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
