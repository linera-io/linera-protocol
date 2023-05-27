// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use fungible::{Account, AccountOwner, FungibleTokenAbi, InitialStateBuilder, Operation};
use linera_sdk::{base::Amount, test::TestValidator};

/// Test transferring tokens across microchains.
///
/// Creates the application on a `sender_chain`, initializing it with a single account with some
/// tokens for that chain's owner. Transfers some of those tokens to a new `receiver_chain`, and
/// checks that the balances on each microchain are correct.
#[tokio::test]
async fn test_cross_chain_transfer() {
    let initial_amount: Amount = Amount::from_tokens(20);
    let transfer_amount: Amount = Amount::from_tokens(15);

    let (validator, bytecode_id) = TestValidator::with_current_bytecode().await;
    let mut sender_chain = validator.new_chain().await;
    let sender_account = AccountOwner::from(sender_chain.public_key());

    let initial_state = InitialStateBuilder::default().with_account(sender_account, initial_amount);
    let application_id = sender_chain
        .create_application::<fungible::FungibleTokenAbi>(
            bytecode_id,
            (),
            initial_state.build(),
            vec![],
        )
        .await;

    let receiver_chain = validator.new_chain().await;
    let receiver_account = AccountOwner::from(receiver_chain.public_key());

    sender_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Transfer {
                    owner: sender_account,
                    amount: transfer_amount,
                    target_account: Account {
                        chain_id: receiver_chain.id(),
                        owner: receiver_account,
                    },
                },
            );
        })
        .await;

    assert_eq!(
        FungibleTokenAbi::query_account(application_id, &sender_chain, sender_account).await,
        Some(initial_amount.saturating_sub(transfer_amount)),
    );

    receiver_chain.handle_received_effects().await;

    assert_eq!(
        FungibleTokenAbi::query_account(application_id, &receiver_chain, receiver_account).await,
        Some(transfer_amount),
    );
}
