// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Random Source application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::test::TestValidator;
use random_source::{Query, QueryResponse, RandomSourceAbi};

/// Runs the operation that derives a deterministic seed from the runtime
/// context (chain ID, application ID, block height, transaction index) and
/// stores two RNG samples; then queries them back.
///
/// The operation also asserts inside the contract that its `transaction_index`
/// equals `0`, demonstrating that issue #2411 has been addressed.
#[tokio::test]
async fn test_random_source_seed_from_transaction_index() {
    let (validator, module_id) =
        TestValidator::with_current_module::<RandomSourceAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    chain
        .add_block(|block| {
            block.with_operation(application_id, ());
        })
        .await;

    let response = chain
        .query(application_id, Query::GetSamples)
        .await
        .response;
    let QueryResponse::Samples {
        seed,
        sample1,
        sample2,
    } = response;
    assert_ne!(seed, 0, "Seed derived from the runtime must be non-trivial");
    assert_ne!(
        sample1, sample2,
        "Two consecutive RNG samples must be distinct",
    );
}
