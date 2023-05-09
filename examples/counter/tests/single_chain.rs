// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::test::TestValidator;

/// Test setting a counter and testing its coherency across microchains.
///
/// Creates the application on a `chain`, initializing it with a 42 then add 15 and obtain 57.
/// which is then checked.
#[tokio::test]
async fn single_chain_test() {
    let (validator, bytecode_id) = TestValidator::with_current_bytecode().await;
    let mut chain = validator.new_chain().await;

    let initial_state: u64 = 42;
    let initial_state_u8 = bcs::to_bytes(&initial_state).unwrap();
    let application_id = chain
        .create_application(bytecode_id, vec![], initial_state_u8, vec![])
        .await;

    let increment: u64 = 15;
    chain
        .add_block(|block| {
            block.with_operation(application_id, increment);
        })
        .await;

    let final_value = initial_state + increment;
    let query_string = "query { value }";

    let value: serde_json::Value = chain.query(application_id, query_string).await;
    let state_value = value
        .as_object()
        .expect("Failed to obtain the first object")
        .get("data")
        .expect("Failed to obtain \"data\"")
        .as_object()
        .expect("Failed to obtain the second object")
        .get("value")
        .expect("Failed to obtain \"value\"")
        .as_u64()
        .expect("Failed to get the u64");
    assert_eq!(state_value, final_value);
}
