// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Matching Engine application

#![cfg(not(target_arch = "wasm32"))]

use async_graphql::InputType;
use fungible::{FungibleTokenAbi, InitialStateBuilder};
use linera_sdk::{
    base::{AccountOwner, Amount, ApplicationId, ApplicationPermissions},
    test::{ActiveChain, TestValidator},
};
use matching_engine::{
    MatchingEngineAbi, Operation, Order, OrderId, OrderNature, Parameters, Price,
};

pub async fn get_orders(
    application_id: ApplicationId<MatchingEngineAbi>,
    chain: &ActiveChain,
    account_owner: AccountOwner,
) -> Option<Vec<OrderId>> {
    let query = format!(
        "query {{ accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }} }}",
        account_owner.to_value()
    );
    let value = chain.graphql_query(application_id, query).await;
    let orders = &value["accountInfo"]["entry"]["value"]["orders"];
    let values = orders
        .as_array()?
        .iter()
        .map(|order| order.as_u64().unwrap())
        .collect();
    Some(values)
}

/// Test creating a matching engine, pushing some orders, canceling some and
/// seeing how the transactions went.
///
/// The operation is done in exactly the same way with the same amounts
/// and quantities as the corresponding end to end test.
///
/// We have 3 chains:
/// * The chain A of User_a for tokens A
/// * The chain B of User_b for tokens B
/// * The admin chain of the matching engine.
///
/// The following operations are done:
/// * We create users and assign them their initial positions:
///   * user_a with 10 tokens A.
///   * user_b with 9 tokens B.
/// * Then we create the following orders:
///   * User_a: Offer to buy token B in exchange of token A for a price of 1 (or 2) with
///     a quantity of 3 token B.
///     User_a thus commits 3 * 1 + 3 * 2 = 9 token A to the matching engine chain and is
///     left with 1 token A on chain A
///   * User_b: Offer to sell token B in exchange of token A for a pice of 2 (or 4) with
///     a quantity of 4 token B
///     User_b thus commits 4 + 4 = 8 token B on the matching engine chain and is left
///     with 1 token B.
/// * The price that is matching is 2 where a transaction can actually occur
///   * Only 3 token B can be exhanged against 6 tokens A.
///   * So, the order from user_b is only partially filled.
/// * Then the orders are cancelled and the user get back their tokens.
///   After the exchange we have
///   * User_a: It has 9 - 6 = 3 token A and the newly acquired 3 token B.
///   * User_b: It has 8 - 3 = 5 token B and the newly acquired 6 token A
#[tokio::test]
async fn single_transaction() {
    let (validator, bytecode_id) = TestValidator::with_current_bytecode().await;

    let mut user_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(user_chain_a.public_key());
    let mut user_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(user_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;
    let admin_account = AccountOwner::from(matching_chain.public_key());

    let fungible_bytecode_id_a = user_chain_a.publish_bytecodes_in("../fungible").await;
    let fungible_bytecode_id_b = user_chain_b.publish_bytecodes_in("../fungible").await;

    let initial_state_a =
        InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(10));
    let params_a = fungible::Parameters::new("A");
    let token_id_a = user_chain_a
        .create_application::<FungibleTokenAbi>(
            fungible_bytecode_id_a,
            params_a,
            initial_state_a.build(),
            vec![],
        )
        .await;
    let initial_state_b =
        InitialStateBuilder::default().with_account(owner_b, Amount::from_tokens(9));
    let params_b = fungible::Parameters::new("B");
    let token_id_b = user_chain_b
        .create_application::<FungibleTokenAbi>(
            fungible_bytecode_id_b,
            params_b,
            initial_state_b.build(),
            vec![],
        )
        .await;

    user_chain_a.register_application(token_id_b).await;
    user_chain_b.register_application(token_id_a).await;

    // Check the initial starting amounts for chain a and chain b
    for (owner, amount) in [
        (admin_account, None),
        (owner_a, Some(Amount::from_tokens(10))),
        (owner_b, None),
    ] {
        let value = FungibleTokenAbi::query_account(token_id_a, &user_chain_a, owner).await;
        assert_eq!(value, amount);
    }
    for (owner, amount) in [
        (admin_account, None),
        (owner_a, None),
        (owner_b, Some(Amount::from_tokens(9))),
    ] {
        let value = FungibleTokenAbi::query_account(token_id_b, &user_chain_b, owner).await;
        assert_eq!(value, amount);
    }

    // Creating the matching engine chain
    let tokens = [token_id_a, token_id_b];
    let matching_parameter = Parameters { tokens };
    let matching_id = matching_chain
        .create_application::<MatchingEngineAbi>(
            bytecode_id,
            matching_parameter,
            (),
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;
    // Doing the registrations
    user_chain_a.register_application(matching_id).await;
    user_chain_b.register_application(matching_id).await;

    // Creating the bid orders
    let mut orders_bids = Vec::new();
    for price in [1, 2] {
        let price = Price { price };
        let order = Order::Insert {
            owner: owner_a,
            amount: Amount::from_tokens(3),
            nature: OrderNature::Bid,
            price,
        };
        let operation = Operation::ExecuteOrder { order };
        let order_messages = user_chain_a
            .add_block(|block| {
                block.with_operation(matching_id, operation);
            })
            .await;
        assert_eq!(order_messages.len(), 3);
        orders_bids.extend(order_messages);
    }

    matching_chain
        .add_block(|block| {
            block.with_incoming_messages(orders_bids);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Checking the values for token_a
    for (owner, amount) in [
        (admin_account, None),
        (owner_a, Some(Amount::ONE)),
        (owner_b, None),
    ] {
        let value = FungibleTokenAbi::query_account(token_id_a, &user_chain_a, owner).await;
        assert_eq!(value, amount);
    }
    for owner in [admin_account, owner_a, owner_b] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, &user_chain_b, owner).await,
            None
        );
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, &matching_chain, owner).await,
            None
        );
    }

    let mut orders_asks = Vec::new();
    for price in [4, 2] {
        let price = Price { price };
        let order = Order::Insert {
            owner: owner_b,
            amount: Amount::from_tokens(4),
            nature: OrderNature::Ask,
            price,
        };
        let operation = Operation::ExecuteOrder { order };
        let order_messages = user_chain_b
            .add_block(|block| {
                block.with_operation(matching_id, operation);
            })
            .await;

        assert_eq!(order_messages.len(), 3);
        orders_asks.extend(order_messages);
    }

    matching_chain
        .add_block(|block| {
            block.with_incoming_messages(orders_asks);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Now querying the matching engine for the remaining orders
    let order_ids_a = get_orders(matching_id, &matching_chain, owner_a)
        .await
        .expect("order_ids_a");
    assert_eq!(order_ids_a.len(), 1);
    let order_ids_b = get_orders(matching_id, &matching_chain, owner_b)
        .await
        .expect("order_ids_b");
    assert_eq!(order_ids_b.len(), 2);

    // Checking the balances on chain A
    for (owner, amount) in [(owner_a, Some(Amount::from_tokens(1))), (owner_b, None)] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, &user_chain_a, owner).await,
            amount
        );
    }
    // Checking the balances on chain B
    for (owner, amount) in [(owner_a, None), (owner_b, Some(Amount::from_tokens(1)))] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_b, &user_chain_b, owner).await,
            amount
        );
    }

    // Cancel A's order.
    let order = Order::Cancel {
        owner: owner_a,
        order_id: order_ids_a[0],
    };
    let operation = Operation::ExecuteOrder { order };
    let order_messages = user_chain_a
        .add_block(|block| {
            block.with_operation(matching_id, operation);
        })
        .await;
    assert_eq!(order_messages.len(), 2);
    matching_chain
        .add_block(|block| {
            block.with_incoming_messages(order_messages);
        })
        .await;
    user_chain_a.handle_received_messages().await;

    let permissions = ApplicationPermissions::new_single(matching_id.forget_abi());
    matching_chain
        .add_block(|block| {
            block.with_change_application_permissions(permissions);
        })
        .await;
    matching_chain
        .add_block(|block| {
            block.with_operation(matching_id, Operation::CloseChain);
        })
        .await;

    user_chain_a.handle_received_messages().await;
    user_chain_b.handle_received_messages().await;

    // Check owner balances
    for (owner, user_chain, amount) in [
        (owner_a, &matching_chain, None),
        (owner_b, &matching_chain, None),
        (owner_a, &user_chain_a, Some(Amount::from_tokens(4))),
        (owner_b, &user_chain_b, Some(Amount::from_tokens(6))),
    ] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, user_chain, owner).await,
            amount
        );
    }
    for (owner, user_chain, amount) in [
        (owner_a, &matching_chain, None),
        (owner_b, &matching_chain, None),
        (owner_a, &user_chain_a, Some(Amount::from_tokens(3))),
        (owner_b, &user_chain_b, Some(Amount::from_tokens(6))),
    ] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_b, user_chain, owner).await,
            amount
        );
    }
}
