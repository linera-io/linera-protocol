// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

use linera_base::{
    error::Error,
    messages::{ApplicationId, BlockHeight, ChainDescription, ChainId},
};
use linera_execution::{
    ExecutionResult, ExecutionRuntimeContext, ExecutionStateView, Operation, OperationContext,
    Query, QueryContext, RawExecutionResult, Response, SystemExecutionState,
    TestExecutionRuntimeContext, WasmApplication,
};
use linera_views::{
    memory::MemoryContext,
    views::{Context, View},
};
use std::sync::Arc;

#[tokio::test]
async fn test_wasm_application() {
    let mut state = SystemExecutionState::default();
    state.description = Some(ChainDescription::Root(0));
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let app_id = ApplicationId(1);
    view.context().extra().user_applications().insert(
        app_id,
        Arc::new(WasmApplication::new(
            "../target/wasm32-unknown-unknown/debug/examples/counter.wasm",
        )),
    );

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };
    let operation = bcs::to_bytes(&2_u128).expect("Serialization of u128 failed");
    let result = view
        .execute_operation(
            app_id,
            &context,
            &Operation::User(vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
        )
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![ExecutionResult::User(app_id, RawExecutionResult::default())]
    );

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    let expected_value = bcs::to_bytes(&2_u128).expect("Serialization of u128 failed");
    assert_eq!(
        view.query_application(app_id, &context, &Query::User(vec![]))
            .await
            .unwrap(),
        Response::User(vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    );
}
