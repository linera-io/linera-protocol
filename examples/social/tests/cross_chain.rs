// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the social network application.

#![cfg(not(target_arch = "wasm32"))]

use linera_sdk::test::TestValidator;
use social::Operation;

/// Test posting messages across microchains.
///
/// Creates the application on chain2 indirectly from the subscription, then
/// send a message to chain2 and see it received on chain1.
#[tokio::test]
async fn test_cross_chain_posting() {
    let (validator, bytecode_id) = TestValidator::with_current_bytecode().await;
    let mut chain1 = validator.new_chain().await;

    // Initialization is trivial for the social app
    let application_id = chain1
        .create_application::<social::SocialAbi>(bytecode_id, (), (), vec![])
        .await;

    let chain2 = validator.new_chain().await;

    // Subscribe chain1 to chain2
    chain1
        .add_block(|block| {
            block.with_operation(application_id, Operation::RequestSubscribe(chain2.id()));
        })
        .await;

    // Make chain2 handle that fact.
    chain2.handle_received_effects().await;

    // Post on chain2
    chain2
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Post("Linera is the new Mastodon".to_string()),
            );
        })
        .await;

    // Now make chain1 handle that fact.
    chain1.handle_received_effects().await;

    // Querying the own posts
    let query = "query { ownPosts(start: 0, end: 1) { timestamp, text } }";
    let response = chain2.graphql_query(application_id, query).await;
    let value = response["ownPosts"][0]["text"].clone();
    assert_eq!(value, "Linera is the new Mastodon".to_string());

    // Now handling the received messages
    let query = "query { receivedPostsKeys { timestamp, author, index } }";
    let response = chain1.graphql_query(application_id, query).await;
    let author = response["receivedPostsKeys"][0]["author"].clone();
    assert_eq!(author, chain2.id().to_string());
}
