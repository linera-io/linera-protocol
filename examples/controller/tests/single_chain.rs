// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration testing for the controller application.
#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::{
    abis::controller::ControllerAbi,
    test::{QueryOutcome, TestValidator},
};

/// Tests basic application instantiation and query
#[tokio::test(flavor = "multi_thread")]
async fn single_chain_test() {
    let (validator, module_id) =
        TestValidator::with_current_module::<ControllerAbi, (), ()>().await;
    let mut chain = validator.new_chain().await;

    let application_id = chain.create_application(module_id, (), (), vec![]).await;

    // Query the localWorker field - should be null for a freshly instantiated app
    let QueryOutcome { response, .. } = chain
        .graphql_query(application_id, "query { localWorker { owner } }")
        .await;

    assert!(response["localWorker"].is_null());
}
