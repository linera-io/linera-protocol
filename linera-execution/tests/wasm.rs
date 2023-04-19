// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

mod utils;

use self::utils::create_dummy_user_application_description;
use linera_base::{
    data_types::BlockHeight,
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    ExecutionResult, ExecutionRuntimeContext, ExecutionStateView, Operation, OperationContext,
    Query, QueryContext, RawExecutionResult, Response, SystemExecutionState,
    TestExecutionRuntimeContext, WasmApplication, WasmRuntime,
};
use linera_views::{memory::MemoryContext, views::View};
use std::sync::Arc;
use test_case::test_case;

/// Test if the "counter" example application in `linera-sdk` compiled to a WASM module can be
/// called correctly and consume the expected amount of fuel.
///
/// To update the bytecode files, run `linera-execution/update_wasm_fixtures.sh`.
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::Wasmer, 9_965_746; "wasmer"))]
#[cfg_attr(feature = "wasmer", test_case(WasmRuntime::WasmerWithSanitizer, 9_965_385; "wasmer_with_sanitizer"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::Wasmtime, 9_965_385 ; "wasmtime"))]
#[cfg_attr(feature = "wasmtime", test_case(WasmRuntime::WasmtimeWithSanitizer, 9_965_385 ; "wasmtime_with_sanitizer"))]
#[test_log::test(tokio::test)]
async fn test_fuel_for_counter_wasm_application(
    wasm_runtime: WasmRuntime,
    expected_gas: u64,
) -> anyhow::Result<()> {
    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..Default::default()
    };
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await
            .with_fuel(10_000_000);
    let app_desc = create_dummy_user_application_description();
    let app_id = view
        .system
        .registry
        .register_application(app_desc.clone())
        .await?;

    let application = WasmApplication::from_files(
        "tests/fixtures/counter_contract.wasm",
        "tests/fixtures/counter_service.wasm",
        wasm_runtime,
    )
    .await?;
    view.context()
        .extra
        .user_applications()
        .insert(app_id, Arc::new(application));

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
        authenticated_signer: None,
    };
    let increments = [2_u128, 9, 7, 1000];
    for increment in &increments {
        let operation = bcs::to_bytes(increment).expect("Serialization of u128 failed");
        let result = view
            .execute_operation(
                &context,
                &Operation::User {
                    application_id: app_id,
                    bytes: operation,
                },
            )
            .await?;
        assert_eq!(
            result,
            vec![ExecutionResult::User(app_id, RawExecutionResult::default())]
        );
    }

    assert_eq!(*view.available_fuel.get(), expected_gas);

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    let expected_value: u128 = increments.into_iter().sum();
    let expected_serialized_value =
        bcs::to_bytes(&expected_value).expect("Serialization of u128 failed");
    assert_eq!(
        view.query_application(
            &context,
            &Query::User {
                application_id: app_id,
                bytes: vec![]
            }
        )
        .await?,
        Response::User(expected_serialized_value)
    );
    Ok(())
}
