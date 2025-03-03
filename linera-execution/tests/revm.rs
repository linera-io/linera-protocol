// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(with_revm)]

use std::sync::Arc;

use alloy_sol_types::{sol, SolCall, SolValue};
use linera_base::{
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    revm::{EvmContractModule, EvmServiceModule},
    test_utils::{
        create_dummy_user_application_description, solidity::get_example_counter,
        SystemExecutionState,
    },
    ExecutionRuntimeConfig, ExecutionRuntimeContext, Operation, OperationContext, Query,
    QueryContext, QueryResponse, ResourceControlPolicy, ResourceController, ResourceTracker,
    TransactionTracker,
};
use linera_views::{context::Context as _, views::View};
use revm_primitives::U256;

#[tokio::test]
async fn test_fuel_for_counter_revm_application() -> anyhow::Result<()> {
    let module = get_example_counter()?;

    sol! {
        struct ConstructorArgs {
            uint256 initial_value;
        }
        function increment(uint256 input);
        function get_value();
    }

    let initial_value = U256::from(10000);
    let mut value = initial_value;
    let args = ConstructorArgs { initial_value };
    let instantiation_argument: Vec<u8> = args.abi_encode();

    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..Default::default()
    };
    let mut view = state
        .into_view_with(ChainId::root(0), ExecutionRuntimeConfig::default())
        .await;
    let (app_desc, contract_blob, service_blob) = create_dummy_user_application_description(1);
    let app_id = view
        .system
        .registry
        .register_application(app_desc.clone())
        .await?;

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
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        round: Some(0),
        index: Some(0),
        authenticated_signer: None,
        authenticated_caller_id: None,
    };

    let query_context = QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
        local_time: Timestamp::from(0),
    };

    let increments = [
        U256::from(2),
        U256::from(9),
        U256::from(7),
        U256::from(1000),
    ];
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
        let mut txn_tracker = TransactionTracker::new(0, Some(Vec::new()));
        value += increment;
        let operation = incrementCall { input: *increment };
        let bytes = operation.abi_encode();
        let operation = Operation::User {
            application_id: app_id,
            bytes,
        };
        view.execute_operation(
            operation_context,
            Timestamp::from(0),
            operation,
            &mut txn_tracker,
            &mut controller,
        )
        .await?;

        let query = get_valueCall {};
        let bytes = query.abi_encode();
        let query = Query::User {
            application_id: app_id,
            bytes,
        };

        let result = view.query_application(query_context, query, None).await?;
        let QueryResponse::User(result) = result.response else {
            anyhow::bail!("Wrong QueryResponse result");
        };
        let result = U256::from_be_slice(&result);
        assert_eq!(result, value);
    }
    Ok(())
}
