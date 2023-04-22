// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use fungible::{Account, AccountOwner, InitialStateBuilder, Operation};
use linera_sdk::{base::Amount, test::TestValidator};

/// Test transfering tokens across microchains.
///
/// Create the application on a `sender_chain`, initializing it with a single account with some
/// tokens for that chain's owner. Transfer some of those tokens to a new `receiver_chain`, and
/// check that the balances on each microchain is correct.
#[tokio::test]
async fn cross_chain_transfer() {
    let initial_amount = Amount::from(20);
    let transfer_amount = Amount::from(15);

    let (validator, bytecode_id) = TestValidator::with_current_bytecode().await;
    let mut sender_chain = validator.new_chain().await;
    let sender_account = AccountOwner::from(sender_chain.public_key());

    let initial_state = InitialStateBuilder::default().with_account(sender_account, initial_amount);
    let application_id = sender_chain
        .create_application(bytecode_id, vec![], initial_state.build(), vec![])
        .await;

    let receiver_chain = validator.new_chain().await;
    let receiver_account = AccountOwner::from(receiver_chain.public_key());

    sender_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                Operation::Transfer {
                    owner: sender_account,
                    amount: transfer_amount.into(),
                    target_account: Account {
                        chain_id: receiver_chain.id(),
                        owner: receiver_account,
                    },
                },
            );
        })
        .await;

    assert_eq!(
        sender_chain
            .query::<Amount>(application_id, sender_account)
            .await,
        initial_amount.saturating_sub(transfer_amount),
    );

    receiver_chain.handle_received_effects().await;

    assert_eq!(
        receiver_chain
            .query::<Amount>(application_id, receiver_account)
            .await,
        transfer_amount
    );
}
