// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(any(feature = "wasmer", feature = "wasmtime"))]

mod utils;

use self::utils::create_dummy_user_application_description;
use linera_base::data_types::{BlockHeight, ChainDescription, ChainId};
use linera_execution::{
    ApplicationDescription, ApplicationRegistryView, ExecutionResult, ExecutionRuntimeContext,
    ExecutionStateView, OperationContext, Query, QueryContext, RawExecutionResult, Response,
    SystemExecutionState, TestExecutionRuntimeContext, WasmApplication,
};
use linera_views::{memory::MemoryContext, views::View};
use std::sync::Arc;

/// Test if the "counter" example application in `linera-sdk` compiled to a WASM module can be
/// called correctly.
#[tokio::test]
async fn test_counter_wasm_application() -> anyhow::Result<()> {
    let state = SystemExecutionState {
        description: Some(ChainDescription::Root(0)),
        ..Default::default()
    };
    let mut view =
        ExecutionStateView::<MemoryContext<TestExecutionRuntimeContext>>::from_system_state(state)
            .await;
    let mut applications = ApplicationRegistryView::load(view.context().clone()).await?;
    let app_desc = create_dummy_user_application_description();
    let app_id = applications.register_existing_application(app_desc.clone());
    view.context().extra.user_applications().insert(
        app_id,
        Arc::new(
            WasmApplication::from_files(
                "../target/wasm32-unknown-unknown/debug/examples/counter_contract.wasm",
                "../target/wasm32-unknown-unknown/debug/examples/counter_service.wasm",
            )
            .await?,
        ),
    );

    let context = OperationContext {
        chain_id: ChainId::root(0),
        height: BlockHeight(0),
        index: 0,
    };
    let increments = [2_u128, 9, 7, 1000];
    for increment in &increments {
        let operation = bcs::to_bytes(increment).expect("Serialization of u128 failed");
        let result = view
            .execute_operation(
                &ApplicationDescription::User(app_desc.clone()),
                &context,
                &operation.into(),
                &mut applications,
            )
            .await?;
        assert_eq!(
            result,
            vec![ExecutionResult::User(app_id, RawExecutionResult::default())]
        );
    }

    let context = QueryContext {
        chain_id: ChainId::root(0),
    };
    let expected_value: u128 = increments.into_iter().sum();
    let expected_serialized_value =
        bcs::to_bytes(&expected_value).expect("Serialization of u128 failed");
    assert_eq!(
        view.query_application(
            &ApplicationDescription::User(app_desc),
            &context,
            &Query::User(vec![]),
            &mut applications
        )
        .await?,
        Response::User(expected_serialized_value)
    );

    Ok(())
}
