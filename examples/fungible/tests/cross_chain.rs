// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use fungible::{FungibleTokenAbi, InitialState, InitialStateBuilder, Parameters};
use linera_sdk::{
    abis::fungible::FungibleOperation,
    linera_base_types::{Account, AccountOwner, Amount},
    test::{MessageAction, TestValidator},
};

/// Test transferring tokens across microchains.
///
/// Creates the application on a `sender_chain`, initializing it with a single account with some
/// tokens for that chain's owner. Transfers some of those tokens to a new `receiver_chain`, and
/// checks that the balances on each microchain are correct.
#[tokio::test]
async fn test_cross_chain_transfer() {
    let initial_amount = Amount::from_tokens(20);
    let transfer_amount = Amount::from_tokens(15);

    let (validator, module_id) =
        TestValidator::with_current_module::<fungible::FungibleTokenAbi, Parameters, InitialState>(
        )
        .await;
    let mut sender_chain = validator.new_chain().await;
    let sender_account = AccountOwner::from(sender_chain.public_key());

    let initial_state = InitialStateBuilder::default().with_account(sender_account, initial_amount);
    let params = Parameters::new("FUN");
    let application_id = sender_chain
        .create_application(module_id, params, initial_state.build(), vec![])
        .await;

    let receiver_chain = validator.new_chain().await;
    let receiver_account = AccountOwner::from(receiver_chain.public_key());

    sender_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Transfer {
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
        fungible::query_account(application_id, &sender_chain, sender_account).await,
        Some(initial_amount.saturating_sub(transfer_amount)),
    );

    receiver_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(application_id, &receiver_chain, receiver_account).await,
        Some(transfer_amount),
    );
}

/// Test bouncing some tokens back to the sender.
///
/// Creates the application on a `sender_chain`, initializing it with a single account with some
/// tokens for that chain's owner. Attempts to transfer some tokens to a new `receiver_chain`,
/// but makes the `receiver_chain` reject the transfer message, causing the tokens to be
/// returned back to the sender.
#[tokio::test]
async fn test_bouncing_tokens() {
    let initial_amount = Amount::from_tokens(19);
    let transfer_amount = Amount::from_tokens(7);

    let (validator, module_id) =
        TestValidator::with_current_module::<FungibleTokenAbi, Parameters, InitialState>().await;
    let mut sender_chain = validator.new_chain().await;
    let sender_account = AccountOwner::from(sender_chain.public_key());

    let initial_state = InitialStateBuilder::default().with_account(sender_account, initial_amount);
    let params = Parameters::new("RET");
    let application_id = sender_chain
        .create_application(module_id, params, initial_state.build(), vec![])
        .await;

    let receiver_chain = validator.new_chain().await;
    let receiver_account = AccountOwner::from(receiver_chain.public_key());

    let certificate = sender_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Transfer {
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
        fungible::query_account(application_id, &sender_chain, sender_account).await,
        Some(initial_amount.saturating_sub(transfer_amount)),
    );

    assert_eq!(certificate.outgoing_message_count(), 1);

    receiver_chain
        .add_block(move |block| {
            block.with_messages_from_by_action(&certificate, MessageAction::Reject);
        })
        .await;

    assert_eq!(
        fungible::query_account(application_id, &receiver_chain, receiver_account).await,
        None,
    );

    sender_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(application_id, &sender_chain, sender_account).await,
        Some(initial_amount),
    );
}
