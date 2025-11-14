// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for basic matching engine operations

#![cfg(not(target_arch = "wasm32"))]

use async_graphql::InputType;
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount},
    test::{ActiveChain, TestValidator},
};
use matching_engine::{MatchingEngineAbi, Operation, Order, OrderNature, Parameters, Price};

/// Helper to create test parameters with two fungible tokens
async fn setup_matching_engine(
    price_decimals: u16,
) -> (
    TestValidator,
    linera_sdk::linera_base_types::ApplicationId<matching_engine::MatchingEngineAbi>,
    linera_sdk::linera_base_types::ApplicationId<fungible::FungibleTokenAbi>,
    linera_sdk::linera_base_types::ApplicationId<fungible::FungibleTokenAbi>,
    ActiveChain,
    ActiveChain,
    ActiveChain,
) {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut user_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(user_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;

    // Create fungible tokens
    let fungible_module_id_a = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;
    let fungible_module_id_b = user_chain_b
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state_a =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(100));
    let params_a = fungible::Parameters::new("TokenA");
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id_a,
            params_a,
            initial_state_a.build(),
            vec![],
        )
        .await;

    let initial_state_b =
        fungible::InitialStateBuilder::default().with_account(owner_b, Amount::from_tokens(100));
    let params_b = fungible::Parameters::new("TokenB");
    let token_id_b = user_chain_b
        .create_application(
            fungible_module_id_b,
            params_b,
            initial_state_b.build(),
            vec![],
        )
        .await;

    // Create matching engine
    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
        price_decimals,
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    (
        validator,
        matching_id,
        token_id_a,
        token_id_b,
        user_chain_a,
        user_chain_b,
        matching_chain,
    )
}

#[tokio::test]
async fn test_insert_bid_order() {
    let (
        _validator,
        matching_id,
        _token_id_a,
        _token_id_b,
        user_chain_a,
        _user_chain_b,
        matching_chain,
    ) = setup_matching_engine(2).await;

    let owner_a = AccountOwner::from(user_chain_a.public_key());

    // Insert a bid order
    let order = Order::Insert {
        owner: owner_a,
        quantity: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };

    let operation = Operation::ExecuteOrder { order };
    let certificate = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    // Process the order on the matching engine chain
    matching_chain
        .add_block(|block| {
            block.with_messages_from(&certificate);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // The order should be created successfully (no panics)
}

#[tokio::test]
async fn test_insert_ask_order() {
    let (
        _validator,
        matching_id,
        _token_id_a,
        _token_id_b,
        _user_chain_a,
        user_chain_b,
        matching_chain,
    ) = setup_matching_engine(2).await;

    let owner_b = AccountOwner::from(user_chain_b.public_key());

    // Insert an ask order
    let order = Order::Insert {
        owner: owner_b,
        quantity: Amount::from_tokens(5),
        nature: OrderNature::Ask,
        price: Price { price: 200 },
    };

    let operation = Operation::ExecuteOrder { order };
    let certificate = user_chain_b
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&certificate);
        })
        .await;

    user_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // The order should be created successfully
}

#[tokio::test]
async fn test_cancel_order() {
    let (
        _validator,
        matching_id,
        _token_id_a,
        _token_id_b,
        user_chain_a,
        _user_chain_b,
        matching_chain,
    ) = setup_matching_engine(2).await;

    let owner_a = AccountOwner::from(user_chain_a.public_key());

    // Insert an order
    let order = Order::Insert {
        owner: owner_a,
        quantity: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let operation = Operation::ExecuteOrder { order };
    let cert1 = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert1);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Query to get the order ID
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response = matching_chain.graphql_query(matching_id, query).await;
    let orders = response.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .expect("Should have orders array");
    assert_eq!(orders.len(), 1);
    let order_id = orders[0].as_u64().unwrap();

    // Cancel the order
    let cancel_order = Order::Cancel {
        owner: owner_a,
        order_id,
    };
    let operation = Operation::ExecuteOrder {
        order: cancel_order,
    };
    let cert2 = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert2);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // The order should be cancelled successfully
}

#[tokio::test]
async fn test_modify_order() {
    let (
        _validator,
        matching_id,
        _token_id_a,
        _token_id_b,
        user_chain_a,
        _user_chain_b,
        matching_chain,
    ) = setup_matching_engine(2).await;

    let owner_a = AccountOwner::from(user_chain_a.public_key());

    // Insert an order
    let order = Order::Insert {
        owner: owner_a,
        quantity: Amount::from_tokens(20),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let operation = Operation::ExecuteOrder { order };
    let cert1 = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert1);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Get the order ID
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response = matching_chain.graphql_query(matching_id, query).await;
    let orders = response.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();
    let order_id = orders[0].as_u64().unwrap();

    // Modify the order (reduce quantity)
    let modify_order = Order::Modify {
        owner: owner_a,
        order_id,
        reduce_quantity: Amount::from_tokens(5),
    };
    let operation = Operation::ExecuteOrder {
        order: modify_order,
    };
    let cert2 = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert2);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // The order should be modified successfully
}

#[tokio::test]
async fn test_multiple_orders_from_same_user() {
    let (
        _validator,
        matching_id,
        _token_id_a,
        _token_id_b,
        user_chain_a,
        _user_chain_b,
        matching_chain,
    ) = setup_matching_engine(2).await;

    let owner_a = AccountOwner::from(user_chain_a.public_key());

    // Insert multiple orders
    for price in [100, 150, 200] {
        let order = Order::Insert {
            owner: owner_a,
            quantity: Amount::from_tokens(5),
            nature: OrderNature::Bid,
            price: Price { price },
        };
        let operation = Operation::ExecuteOrder { order };
        let cert = user_chain_a
            .add_block(|block| {
                block.with_operation(matching_id, operation);
            })
            .await;

        matching_chain
            .add_block(|block| {
                block.with_messages_from(&cert);
            })
            .await;

        user_chain_a.handle_received_messages().await;
    }

    // Query orders
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response = matching_chain.graphql_query(matching_id, query).await;
    let orders = response.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();

    // Should have 3 orders
    assert_eq!(orders.len(), 3);
}
