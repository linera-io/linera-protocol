// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(with_wasm_runtime)]

use counter::CounterAbi;
use linera_base::{
    data_types::{Amount, BlockHeight},
    identifiers::{Account, ChainDescription, ChainId},
};
use linera_execution::{
    test_utils::{create_dummy_user_application_description, SystemExecutionState},
    ExecutionOutcome, ExecutionRuntimeConfig, ExecutionRuntimeContext, Operation, OperationContext,
    Query, QueryContext, RawExecutionOutcome, ResourceControlPolicy, ResourceController,
    ResourceTracker, Response, WasmContractModule, WasmRuntime, WasmServiceModule,
};
use linera_views::views::View;
use serde_json::json;
use std::sync::Arc;
use test_case::test_case;

/// Test if the "counter" example application in `linera-sdk` compiled to a Wasm module can be
/// called correctly and consume the expected amount of fuel.
///
/// To update the bytecode files, run `linera-execution/update_wasm_fixtures.sh`.
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, 40_754, ExecutionRuntimeConfig::Synchronous; "wasmer"))]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::WasmerWithSanitizer, 41_182, ExecutionRuntimeConfig::Synchronous; "wasmer_with_sanitizer"))]
#[cfg_attr(with_wasmtime, test_case(WasmRuntime::Wasmtime, 41_182, ExecutionRuntimeConfig::Synchronous; "wasmtime"))]
#[cfg_attr(with_wasmtime, test_case(WasmRuntime::WasmtimeWithSanitizer, 41_182, ExecutionRuntimeConfig::Synchronous; "wasmtime_with_sanitizer"))]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_fuel_for_counter_wasm_application(
    wasm_runtime: WasmRuntime,
    expected_fuel: u64,
    execution_runtime_config: ExecutionRuntimeConfig,
) -> anyhow::Result<()> {
    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..Default::default()
    };
    let mut view = state.into_view_with_runtime(execution_runtime_config).await;
    let app_desc = create_dummy_user_application_description(1);
    let app_id = view
        .system
        .registry
        .register_application(app_desc.clone())
        .await?;

    let contract =
        WasmContractModule::from_file("tests/fixtures/counter_contract.wasm", wasm_runtime).await?;
    view.context()
        .extra
        .user_contracts()
        .insert(app_id, Arc::new(contract));

    let service =
        WasmServiceModule::from_file("tests/fixtures/counter_service.wasm", wasm_runtime).await?;
    view.context()
        .extra
        .user_services()
        .insert(app_id, Arc::new(service));

    let app_id = app_id.with_abi::<CounterAbi>();

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
        next_message_index: 0,
    };
    let increments = [2_u64, 9, 7, 1000];
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
        let account = Account {
            chain_id: ChainId::root(0),
            owner: None,
        };
        let outcomes = view
            .execute_operation(
                context,
                Operation::user(app_id, increment).unwrap(),
                &mut controller,
            )
            .await?;
        assert_eq!(
            outcomes,
            vec![ExecutionOutcome::User(
                app_id.forget_abi(),
                RawExecutionOutcome::default().with_refund_grant_to(Some(account))
            )]
        );
    }
    assert_eq!(controller.tracker.fuel, expected_fuel);
    assert_eq!(
        controller.with_state(&mut view).await?.balance().unwrap(),
        Amount::ONE
            .try_sub(Amount::from_attos(expected_fuel as u128))
            .unwrap()
    );

    let context = QueryContext {
        chain_id: ChainId::root(0),
        next_block_height: BlockHeight(0),
    };
    let expected_value = async_graphql::Response::new(
        async_graphql::Value::from_json(json!({"value" : increments.into_iter().sum::<u64>()}))
            .unwrap(),
    );
    let request = async_graphql::Request::new("query { value }");
    let Response::User(serialized_value) = view
        .query_application(context, Query::user(app_id, &request).unwrap())
        .await?
    else {
        panic!("unexpected response")
    };
    assert_eq!(
        serde_json::from_slice::<async_graphql::Response>(&serialized_value).unwrap(),
        expected_value
    );
    Ok(())
}
