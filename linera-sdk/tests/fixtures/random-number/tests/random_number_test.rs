// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Random Number application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::test::TestValidator;
use random_number::{Query, QueryResponse, RandomNumberAbi};

/// Test that the contract generates distinct random seeds and that
/// StdRng instances seeded from them produce distinct values.
#[tokio::test]
async fn test_contract_random_numbers() {
    let (validator, module_id) =
        TestValidator::with_current_module::<RandomNumberAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    // Execute operation: contract calls random_number() twice, asserts distinctness,
    // seeds two StdRng instances, asserts their outputs are distinct, and stores seeds.
    chain
        .add_block(|block| {
            block.with_operation(application_id, ());
        })
        .await;

    // Query the stored seeds from the service.
    let response = chain.query(application_id, Query::GetSeeds).await.response;
    match response {
        QueryResponse::Seeds { seed1, seed2 } => {
            assert_ne!(seed1, seed2, "Contract seeds must be distinct");
            assert_ne!(seed1, 0, "First seed should be non-trivial");
            assert_ne!(seed2, 0, "Second seed should be non-trivial");
        }
        other => panic!("Unexpected response: {other:?}"),
    }
}

/// Test that the service also generates distinct random numbers.
#[tokio::test]
async fn test_service_random_numbers() {
    let (validator, module_id) =
        TestValidator::with_current_module::<RandomNumberAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    // Query the service to generate random numbers directly.
    let response = chain
        .query(application_id, Query::ServiceRandom)
        .await
        .response;
    match response {
        QueryResponse::ServiceSeeds { seed1, seed2 } => {
            assert_ne!(seed1, seed2, "Service seeds must be distinct");
        }
        other => panic!("Unexpected response: {other:?}"),
    }
}
