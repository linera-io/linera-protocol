// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Create and Call application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::test::{ActiveChain, TestValidator};

/// Test creating and calling a counter application dynamically.
///
/// This test publishes the counter-no-graphql bytecode, then uses the create-and-call
/// application to dynamically create an instance of the counter and increment it.
/// The test verifies that the counter is correctly initialized and incremented.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_and_call() {
    let (validator, create_call_module_id) =
        TestValidator::with_current_module::<create_and_call::CreateAndCallAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    // Step 1: Get the bytecode for "counter-no-graphql" by compiling it from its directory
    let counter_no_graphql_path = std::path::Path::new("../counter-no-graphql");
    let counter_no_graphql_path = std::fs::canonicalize(counter_no_graphql_path)
        .expect("Failed to get absolute path to counter-no-graphql");

    // Build and find the counter-no-graphql bytecode files
    ActiveChain::build_bytecode_files_in(&counter_no_graphql_path).await;
    let (counter_contract, counter_service) =
        ActiveChain::find_bytecode_files_in(&counter_no_graphql_path).await;

    // Extract the raw bytes from the bytecode
    let counter_contract_bytes = counter_contract.bytes.to_vec();
    let counter_service_bytes = counter_service.bytes.to_vec();

    // Step 2: Create the "create-and-call" application
    let application_id = chain
        .create_application(create_call_module_id, (), (), vec![])
        .await;

    // Step 3: Call the CreateAndCall operation with the counter bytecode,
    // initialization value of 43, and increment of 5
    let initialization_value = 43;
    let increment_value = 5;
    let create_and_call_operation = create_and_call::CreateAndCallOperation::CreateAndCall(
        counter_contract_bytes,
        counter_service_bytes,
        initialization_value,
        increment_value,
    );

    chain
        .add_block(|block| {
            block.with_operation(application_id, create_and_call_operation);
        })
        .await;

    // Step 4: Query the create-and-call application to get the result (should be 48 = 43 + 5)
    let query_request = create_and_call::CreateAndCallRequest::Query;
    let outcome = chain.query(application_id, query_request).await;

    let expected_value = 48; // 43 + 5
    assert_eq!(outcome.response, expected_value);
}
