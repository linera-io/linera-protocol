// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::test::TestValidator;
use track_instantiation::{Query, TrackInstantiationAbi};

/// Test transferring tokens across microchains.
///
/// Creates the application on a `sender_chain`, initializing it with a single account with some
/// tokens for that chain's owner. Transfers some of those tokens to a new `receiver_chain`, and
/// checks that the balances on each microchain are correct.
#[tokio::test]
async fn test_instantiation_messages() {
    let (validator, module_id) =
        TestValidator::with_current_module::<TrackInstantiationAbi, (), ()>().await;
    let mut sender_chain = validator.new_chain().await;

    let application_id = sender_chain
        .create_application(module_id, (), (), vec![])
        .await;

    sender_chain.handle_received_messages().await;

    let query = Query::GetCount;
    assert_eq!(sender_chain.query(application_id, query).await.response, 1);
}
