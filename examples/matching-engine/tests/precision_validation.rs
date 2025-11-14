// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for price precision validation

#![cfg(not(target_arch = "wasm32"))]

use async_graphql::InputType;
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount},
    test::TestValidator,
};
use matching_engine::{MatchingEngineAbi, Operation, Order, OrderNature, Parameters, Price};

#[tokio::test]
async fn test_valid_precision_with_price_decimals_2() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(100));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("A"),
            initial_state.build(),
            vec![],
        )
        .await;

    let token_id_b = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("B"),
            fungible::InitialStateBuilder::default()
                .with_account(owner_a, Amount::from_tokens(100))
                .build(),
            vec![],
        )
        .await;

    // price_decimals = 2 means quantities can have at most 18 - 2 = 16 decimal places
    // This means quantities must be divisible by 10^2 = 100 (in attos)
    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
        price_decimals: 2,
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // Valid: quantity with precision that respects the limit
    // Amount is internally stored with 18 decimal places (attos)
    // With price_decimals=2, we need the amount to be divisible by 100
    let valid_quantity = Amount::from_attos(1_000_000_000_000_000_000); // 1.0 token = divisible by 100

    let order = Order::Insert {
        owner: owner_a,
        quantity: valid_quantity,
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };

    let operation = Operation::ExecuteOrder { order };
    let certificate = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&certificate);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Should succeed - valid precision
}

#[tokio::test]
#[should_panic]
async fn test_invalid_precision_with_price_decimals_2() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(100));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("A"),
            initial_state.build(),
            vec![],
        )
        .await;

    let token_id_b = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("B"),
            fungible::InitialStateBuilder::default()
                .with_account(owner_a, Amount::from_tokens(100))
                .build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
        price_decimals: 2,
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // Invalid: quantity with too much precision
    // Not divisible by 100
    let invalid_quantity = Amount::from_attos(1_000_000_000_000_000_001); // 1.000000000000000001 - NOT divisible by 100

    let order = Order::Insert {
        owner: owner_a,
        quantity: invalid_quantity,
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };

    let operation = Operation::ExecuteOrder { order };
    let _certificate = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    // Should panic due to precision check failure
}

#[tokio::test]
async fn test_valid_precision_with_price_decimals_0() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(100));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("A"),
            initial_state.build(),
            vec![],
        )
        .await;

    let token_id_b = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("B"),
            fungible::InitialStateBuilder::default()
                .with_account(owner_a, Amount::from_tokens(100))
                .build(),
            vec![],
        )
        .await;

    // price_decimals = 0 means quantities can use all 18 decimal places
    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
        price_decimals: 0,
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // With price_decimals=0, any precision is valid
    let quantity = Amount::from_attos(1); // 1 atto - smallest possible unit

    let order = Order::Insert {
        owner: owner_a,
        quantity,
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };

    let operation = Operation::ExecuteOrder { order };
    let certificate = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&certificate);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Should succeed - with price_decimals=0, all precisions are valid
}

#[tokio::test]
async fn test_modify_order_valid_precision() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(100));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("A"),
            initial_state.build(),
            vec![],
        )
        .await;

    let token_id_b = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("B"),
            fungible::InitialStateBuilder::default()
                .with_account(owner_a, Amount::from_tokens(100))
                .build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
        price_decimals: 2,
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // Insert order
    let order = Order::Insert {
        owner: owner_a,
        quantity: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let cert1 = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert1);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Get order ID
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response = matching_chain.graphql_query(matching_id, query).await;
    let orders = response.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();
    let order_id = orders[0].as_u64().unwrap();

    // Modify with valid precision (divisible by 100 attos)
    let modify_order = Order::Modify {
        owner: owner_a,
        order_id,
        reduce_quantity: Amount::from_tokens(2), // Valid precision
    };
    let cert2 = user_chain_a
        .add_block(|block| {
            block.with_operation(
                matching_id,
                Operation::ExecuteOrder {
                    order: modify_order,
                },
            );
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert2);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Should succeed
}

#[tokio::test]
#[should_panic]
async fn test_modify_order_invalid_precision() {
    let (validator, module_id) =
        TestValidator::with_current_module::<MatchingEngineAbi, Parameters, ()>().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut matching_chain = validator.new_chain().await;

    let fungible_module_id = user_chain_a
        .publish_bytecode_files_in::<fungible::FungibleTokenAbi, fungible::Parameters, fungible::InitialState>("../fungible")
        .await;

    let initial_state =
        fungible::InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(100));
    let token_id_a = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("A"),
            initial_state.build(),
            vec![],
        )
        .await;

    let token_id_b = user_chain_a
        .create_application(
            fungible_module_id,
            fungible::Parameters::new("B"),
            fungible::InitialStateBuilder::default()
                .with_account(owner_a, Amount::from_tokens(100))
                .build(),
            vec![],
        )
        .await;

    let matching_parameter = Parameters {
        tokens: [token_id_a, token_id_b],
        price_decimals: 2,
    };
    let matching_id = matching_chain
        .create_application(
            module_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;

    // Insert order
    let order = Order::Insert {
        owner: owner_a,
        quantity: Amount::from_tokens(10),
        nature: OrderNature::Bid,
        price: Price { price: 100 },
    };
    let cert1 = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, Operation::ExecuteOrder { order });
        })
        .await;

    matching_chain
        .add_block(|block| {
            block.with_messages_from(&cert1);
        })
        .await;

    user_chain_a.handle_received_messages().await;

    // Get order ID
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        owner_a.to_value()
    );
    let response = matching_chain.graphql_query(matching_id, query).await;
    let orders = response.response["accountInfo"]["entry"]["value"]["orders"]
        .as_array()
        .unwrap();
    let order_id = orders[0].as_u64().unwrap();

    // Modify with invalid precision (not divisible by 100 attos)
    let invalid_reduce = Amount::from_attos(2_000_000_000_000_000_001); // Not divisible by 100
    let modify_order = Order::Modify {
        owner: owner_a,
        order_id,
        reduce_quantity: invalid_reduce,
    };
    let _cert2 = user_chain_a
        .add_block(|block| {
            block.with_operation(
                matching_id,
                Operation::ExecuteOrder {
                    order: modify_order,
                },
            );
        })
        .await;

    // Should panic
}
