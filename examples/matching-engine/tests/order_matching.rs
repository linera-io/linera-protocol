// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for order matching logic

#![cfg(not(target_arch = "wasm32"))]

use async_graphql::InputType;
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount},
    test::TestValidator,
};
use matching_engine::{MatchingEngineAbi, Operation, Order, OrderNature, Parameters, Price};

#[tokio::test]
async fn test_bid_ask_matching_exact_price() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut user_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(user_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;

    // Setup tokens
    let fungible_module_id_a = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;
    let fungible_module_id_b = user_chain_b
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state_a =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(10000));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id_a,
            fungible::Parameters::new("A"),
            initial_state_a.build(),
            vec![],
        )
        .await;

    let initial_state_b =
        fungible::InitialStateBuilder::default().with_account(owner_b, Amount::from_tokens(10000));
    let token_id_b = user_chain_b
        .create_application(
            fungible_module_id_b,
            fungible::Parameters::new("B"),
            initial_state_b.build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // User A places a bid at price 100 for 10 tokens
    let bid_order = Order::Insert {
        owner: owner_a,
        amount: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let cert_bid = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: bid_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_bid);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;

    // User B places an ask at price 100 for 10 tokens - should match!
    let ask_order = Order::Insert {
        owner: owner_b,
        amount: Amount::from_tokens(10),
        nature: OrderNature::Ask,
        price: Price { price: 100 },
    };
    let cert_ask = user_chain_b
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: ask_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_ask);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Both orders should be fully matched
    // User A should have received 10 tokens B
    // User B should have received 10 tokens A * price
}

#[tokio::test]
async fn test_bid_ask_matching_better_price() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut user_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(user_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id_a = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;
    let fungible_module_id_b = user_chain_b
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state_a =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(10000));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id_a,
            fungible::Parameters::new("A"),
            initial_state_a.build(),
            vec![],
        )
        .await;

    let initial_state_b =
        fungible::InitialStateBuilder::default().with_account(owner_b, Amount::from_tokens(10000));
    let token_id_b = user_chain_b
        .create_application(
            fungible_module_id_b,
            fungible::Parameters::new("B"),
            initial_state_b.build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // User A places a bid at price 120 (willing to pay more)
    let bid_order = Order::Insert {
        owner: owner_a,
        amount: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 120 },
    };
    let cert_bid = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: bid_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_bid);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // User B places an ask at price 100 (lower than bid) - should match!
    let ask_order = Order::Insert {
        owner: owner_b,
        amount: Amount::from_tokens(10),
        nature: OrderNature::Ask,
        price: Price { price: 100 },
    };
    let cert_ask = user_chain_b
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: ask_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_ask);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Orders should match - liquidity provider gets the better price
}

#[tokio::test]
async fn test_partial_fill() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut user_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(user_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id_a = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;
    let fungible_module_id_b = user_chain_b
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state_a =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(10000));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id_a,
            fungible::Parameters::new("A"),
            initial_state_a.build(),
            vec![],
        )
        .await;

    let initial_state_b =
        fungible::InitialStateBuilder::default().with_account(owner_b, Amount::from_tokens(10000));
    let token_id_b = user_chain_b
        .create_application(
            fungible_module_id_b,
            fungible::Parameters::new("B"),
            initial_state_b.build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // User A places a bid for 20 tokens
    let bid_order = Order::Insert {
        owner: owner_a,
        amount: Amount::from_tokens(20),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let cert_bid = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: bid_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_bid);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // User B places an ask for only 5 tokens - partial fill
    let ask_order = Order::Insert {
        owner: owner_b,
        amount: Amount::from_tokens(5),
        nature: OrderNature::Ask,
        price: Price { price: 100 },
    };
    let cert_ask = user_chain_b
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: ask_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_ask);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Bid should be partially filled (15 tokens remaining)
    // Ask should be fully filled
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response = matching_chain.graphql_query(matching_id, query).await;
    let orders = response.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();

    // User A should still have an open order
    assert_eq!(orders.len(), 1);
}

#[tokio::test]
async fn test_no_match_when_prices_dont_cross() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut user_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(user_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id_a = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;
    let fungible_module_id_b = user_chain_b
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state_a =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(10000));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id_a,
            fungible::Parameters::new("A"),
            initial_state_a.build(),
            vec![],
        )
        .await;

    let initial_state_b =
        fungible::InitialStateBuilder::default().with_account(owner_b, Amount::from_tokens(10000));
    let token_id_b = user_chain_b
        .create_application(
            fungible_module_id_b,
            fungible::Parameters::new("B"),
            initial_state_b.build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // User A places a bid at price 100
    let bid_order = Order::Insert {
        owner: owner_a,
        amount: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let cert_bid = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: bid_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_bid);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // User B places an ask at price 150 (higher than bid) - should NOT match
    let ask_order = Order::Insert {
        owner: owner_b,
        amount: Amount::from_tokens(10),
        nature: OrderNature::Ask,
        price: Price { price: 150 },
    };
    let cert_ask = user_chain_b
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order: ask_order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert_ask);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;

    // Both orders should remain open
    let query_a = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response_a = matching_chain.graphql_query(matching_id, query_a).await;
    let orders_a = response_a.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();
    assert_eq!(orders_a.len(), 1);

    let query_b = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_b.to_value()
    );
    let response_b = matching_chain.graphql_query(matching_id, query_b).await;
    let orders_b = response_b.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();
    assert_eq!(orders_b.len(), 1);
}
