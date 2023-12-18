// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use async_graphql::InputType;
use fungible::{AccountOwner, FungibleTokenAbi, InitialStateBuilder};
use linera_sdk::{
    base::{Amount, ApplicationId},
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
    let value = chain
        .graphql_query(application_id, query)
        .await
        .as_object()?
        .clone();
    let value = value.get("accountInfo")?.as_object()?.clone();
    let value = value.get("entry")?.as_object()?.clone();
    let value = value.get("value")?.as_object()?.clone();
    let value = value.get("orders")?.as_array()?.clone();
    let mut values = Vec::new();
    for sing_value in value {
        let value = sing_value.as_u64().unwrap();
        values.push(value);
    }
    Some(values)
}

/// Test creating a matching engine, pushing some orders, canceling some and
/// seeing how the transactions went.
///
/// The operation is done in exactly the same way with the same amounts
/// and quantities as the corresponding end to end test.
#[tokio::test]
async fn single_transaction() {
    let (validator, bytecode_id) = TestValidator::with_current_bytecode().await;

    let mut fungible_chain_a = validator.new_chain().await;
    let owner_a = AccountOwner::from(fungible_chain_a.public_key());
    let mut fungible_chain_b = validator.new_chain().await;
    let owner_b = AccountOwner::from(fungible_chain_b.public_key());
    let mut matching_chain = validator.new_chain().await;
    let admin_account = AccountOwner::from(matching_chain.public_key());

    let fungible_bytecode_id_a = fungible_chain_a.publish_bytecodes_in("../fungible").await;
    let fungible_bytecode_id_b = fungible_chain_b.publish_bytecodes_in("../fungible").await;

    let initial_state_a =
        InitialStateBuilder::default().with_account(owner_a, Amount::from_tokens(10));
    let params_a = fungible::Parameters::new("A");
    let token_id_a = fungible_chain_a
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
    let token_id_b = fungible_chain_b
        .create_application::<FungibleTokenAbi>(
            fungible_bytecode_id_b,
            params_b,
            initial_state_b.build(),
            vec![],
        )
        .await;

    fungible_chain_a.register_application(token_id_b).await;
    fungible_chain_b.register_application(token_id_a).await;

    for (owner, amount) in [
        (admin_account, None),
        (owner_a, Some(Amount::from_tokens(10))),
        (owner_b, None),
    ] {
        let value = FungibleTokenAbi::query_account(token_id_a, &fungible_chain_a, owner).await;
        assert_eq!(value, amount);
    }
    for (owner, amount) in [
        (admin_account, None),
        (owner_a, None),
        (owner_b, Some(Amount::from_tokens(9))),
    ] {
        let value = FungibleTokenAbi::query_account(token_id_b, &fungible_chain_b, owner).await;
        assert_eq!(value, amount);
    }

    let matching_initial = ();
    let tokens = [token_id_a, token_id_b];
    let matching_parameter = Parameters { tokens };
    let matching_id = matching_chain
        .create_application::<MatchingEngineAbi>(
            bytecode_id,
            matching_parameter,
            matching_initial,
            vec![token_id_a.forget_abi(), token_id_b.forget_abi()],
        )
        .await;
    // Doing the registrations
    matching_chain.register_application(token_id_a).await;
    matching_chain.register_application(token_id_b).await;
    fungible_chain_a.register_application(matching_id).await;
    fungible_chain_b.register_application(matching_id).await;

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
        let order_messages = fungible_chain_a
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

    fungible_chain_a.handle_received_messages().await;
    fungible_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Checking the values for token_a
    for (owner, amount) in [
        (admin_account, None),
        (owner_a, Some(Amount::from_tokens(1))),
        (owner_b, None),
    ] {
        let value = FungibleTokenAbi::query_account(token_id_a, &fungible_chain_a, owner).await;
        assert_eq!(value, amount);
    }
    for (owner, amount) in [(admin_account, None), (owner_a, None), (owner_b, None)] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, &fungible_chain_b, owner).await,
            amount
        );
    }
    for (owner, amount) in [
        (admin_account, None),
        (owner_a, Some(Amount::ZERO)),
        (owner_b, None),
    ] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, &matching_chain, owner).await,
            amount
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
        let order_messages = fungible_chain_b
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

    fungible_chain_a.handle_received_messages().await;
    fungible_chain_b.handle_received_messages().await;
    matching_chain.handle_received_messages().await;

    // Now querying the matching
    let order_ids_a = get_orders(matching_id, &matching_chain, owner_a)
        .await
        .expect("order_ids_a");
    assert_eq!(order_ids_a.len(), 1);
    let order_ids_b = get_orders(matching_id, &matching_chain, owner_b)
        .await
        .expect("order_ids_b");
    assert_eq!(order_ids_b.len(), 2);

    // Cancelling the remaining orders
    let mut orders_cancels = Vec::new();
    for (owner, order_ids) in [(owner_a, order_ids_a), (owner_b, order_ids_b)] {
        for order_id in order_ids {
            let order = Order::Cancel { owner, order_id };
            let operation = Operation::ExecuteOrder { order };
            let order_messages = fungible_chain_a
                .add_block(|block| {
                    block.with_operation(matching_id, operation);
                })
                .await;
            assert_eq!(order_messages.len(), 2);
            orders_cancels.extend(order_messages);
        }
    }

    matching_chain
        .add_block(|block| {
            block.with_incoming_messages(orders_cancels);
        })
        .await;

    fungible_chain_a.handle_received_messages().await;
    fungible_chain_b.handle_received_messages().await;

    // Check balances
    for (owner, amount) in [
        (owner_a, Amount::from_tokens(3)),
        (owner_b, Amount::from_tokens(6)),
    ] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_a, &matching_chain, owner).await,
            Some(amount)
        );
    }
    for (owner, amount) in [
        (owner_a, Amount::from_tokens(3)),
        (owner_b, Amount::from_tokens(5)),
    ] {
        assert_eq!(
            FungibleTokenAbi::query_account(token_id_b, &matching_chain, owner).await,
            Some(amount)
        );
    }
}
