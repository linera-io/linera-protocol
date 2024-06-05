// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Hex application.

#![cfg(not(target_arch = "wasm32"))]

use hex_game::{HexAbi, InstantiationArgument, Operation};
use linera_sdk::{
    base::{KeyPair, Owner, TimeDelta},
    test::TestValidator,
};

#[tokio::test]
async fn hex_game() {
    let key_pair1 = KeyPair::generate();
    let key_pair2 = KeyPair::generate();
    let owner1 = Owner::from(key_pair1.public());
    let owner2 = Owner::from(key_pair2.public());
    let arg = InstantiationArgument {
        players: [owner1, owner2],
        board_size: 2u16,
        start_time: TimeDelta::from_secs(60),
        increment: TimeDelta::from_secs(30),
        block_delay: TimeDelta::from_secs(5),
    };
    let (_, app_id, mut chain) =
        TestValidator::with_current_application::<HexAbi, _, _>((), arg).await;

    chain
        .add_block(|block| {
            block.with_owner_change(
                Vec::new(),
                vec![(key_pair1.public(), 1), (key_pair2.public(), 1)],
                100,
                Default::default(),
            );
        })
        .await;

    chain.set_key_pair(key_pair1.copy());
    chain
        .add_block(|block| {
            block.with_operation(app_id, Operation::MakeMove { x: 0, y: 0 });
        })
        .await;

    chain.set_key_pair(key_pair2.copy());
    chain
        .add_block(|block| {
            block.with_operation(app_id, Operation::MakeMove { x: 0, y: 1 });
        })
        .await;

    chain.set_key_pair(key_pair1.copy());
    chain
        .add_block(|block| {
            block.with_operation(app_id, Operation::MakeMove { x: 1, y: 1 });
        })
        .await;

    let response = chain.graphql_query(app_id, "query { winner }").await;
    assert!(response["winner"].is_null());

    chain.set_key_pair(key_pair2.copy());
    chain
        .add_block(|block| {
            block.with_operation(app_id, Operation::MakeMove { x: 1, y: 0 });
        })
        .await;

    let response = chain.graphql_query(app_id, "query { winner }").await;
    assert_eq!(Some("TWO"), response["winner"].as_str());
}
