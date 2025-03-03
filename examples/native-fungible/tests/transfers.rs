// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

//! Integration tests for Native Fungible Token transfers.

use fungible::{self, FungibleTokenAbi};
use linera_sdk::{
    linera_base_types::{Account, Amount, ChainId, CryptoHash, Owner},
    test::{Recipient, TestValidator},
};

/// Tests if tokens from the shared chain balance can be sent to a different chain.
#[test_log::test(tokio::test)]
async fn chain_balance_transfers() {
    let parameters = fungible::Parameters {
        ticker_symbol: "NAT".to_owned(),
    };
    let initial_state = fungible::InitialStateBuilder::default().build();
    let (validator, _application_id, recipient_chain) = TestValidator::with_current_application::<
        FungibleTokenAbi,
        _,
        _,
    >(parameters, initial_state)
    .await;

    let transfer_amount = Amount::ONE;
    let funding_chain = validator.get_chain(&ChainId::root(0));
    let recipient = Recipient::chain(recipient_chain.id());

    let transfer_certificate = funding_chain
        .add_block(|block| {
            block.with_native_token_transfer(None, recipient, transfer_amount);
        })
        .await;

    recipient_chain
        .add_block(|block| {
            block.with_messages_from(&transfer_certificate);
        })
        .await;

    assert_eq!(recipient_chain.chain_balance().await, transfer_amount);
}

/// Tests if an individual account can receive tokens.
#[test_log::test(tokio::test)]
async fn transfer_to_owner() {
    let parameters = fungible::Parameters {
        ticker_symbol: "NAT".to_owned(),
    };
    let initial_state = fungible::InitialStateBuilder::default().build();
    let (validator, _application_id, recipient_chain) = TestValidator::with_current_application::<
        FungibleTokenAbi,
        _,
        _,
    >(parameters, initial_state)
    .await;

    let transfer_amount = Amount::from_tokens(2);
    let funding_chain = validator.get_chain(&ChainId::root(0));
    let owner = Owner(CryptoHash::test_hash("owner"));
    let account = Account::owner(recipient_chain.id(), owner);
    let recipient = Recipient::Account(account);

    let transfer_certificate = funding_chain
        .add_block(|block| {
            block.with_native_token_transfer(None, recipient, transfer_amount);
        })
        .await;

    recipient_chain
        .add_block(|block| {
            block.with_messages_from(&transfer_certificate);
        })
        .await;

    assert_eq!(
        recipient_chain.owner_balance(&owner.into()).await,
        Some(transfer_amount)
    );
}
