// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg(not(target_arch = "wasm32"))]

//! Integration tests for Native Fungible Token transfers.

use std::collections::{BTreeMap, HashMap};

use fungible::{self, FungibleTokenAbi};
use linera_sdk::{
    linera_base_types::{Account, AccountOwner, Amount, ChainId, CryptoHash, Owner},
    test::{ActiveChain, Recipient, TestValidator},
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
    assert_balances(&recipient_chain, []).await;
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

    assert_balances(&recipient_chain, [(owner.into(), transfer_amount)]).await;
}

/// Tests if multiple accounts can receive tokens.
#[test_log::test(tokio::test)]
async fn transfer_to_multiple_owners() {
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

    let number_of_owners = 10;
    let transfer_amounts = (1..=number_of_owners).map(Amount::from_tokens);
    let funding_chain = validator.get_chain(&ChainId::root(0));

    let account_owners = (1..=number_of_owners)
        .map(|index| Owner(CryptoHash::test_hash(format!("owner{index}"))))
        .map(AccountOwner::from)
        .collect::<Vec<_>>();

    let recipients = account_owners
        .iter()
        .copied()
        .map(|account_owner| Account::owner(recipient_chain.id(), account_owner))
        .map(Recipient::Account);

    let transfer_certificate = funding_chain
        .add_block(|block| {
            for (recipient, transfer_amount) in recipients.zip(transfer_amounts.clone()) {
                block.with_native_token_transfer(None, recipient, transfer_amount);
            }
        })
        .await;

    recipient_chain
        .add_block(|block| {
            block.with_messages_from(&transfer_certificate);
        })
        .await;

    assert_balances(
        &recipient_chain,
        account_owners.into_iter().zip(transfer_amounts),
    )
    .await;
}

/// Asserts that all the accounts in the [`ActiveChain`] have the `expected_balances`.
async fn assert_balances(
    chain: &ActiveChain,
    expected_balances: impl IntoIterator<Item = (AccountOwner, Amount)>,
) {
    let expected_balances = expected_balances.into_iter().collect::<BTreeMap<_, _>>();
    let accounts = expected_balances.keys().copied().collect::<Vec<_>>();

    assert_eq!(chain.accounts().await, accounts);

    let missing_accounts = ["missing1", "missing2"]
        .into_iter()
        .map(CryptoHash::test_hash)
        .map(Owner)
        .map(AccountOwner::from)
        .collect::<Vec<_>>();

    let accounts_to_query = accounts
        .into_iter()
        .chain(missing_accounts.iter().copied())
        .collect::<Vec<_>>();

    let expected_query_response = expected_balances
        .iter()
        .map(|(&account, &balance)| (account, Some(balance)))
        .chain(missing_accounts.into_iter().map(|account| (account, None)))
        .collect::<HashMap<_, _>>();

    for account in &accounts_to_query {
        assert_eq!(
            &chain.owner_balance(account).await,
            expected_query_response
                .get(account)
                .expect("Missing balance amount for a test account")
        );
    }

    assert_eq!(
        chain.owner_balances(accounts_to_query).await,
        expected_query_response
    );
    assert_eq!(
        chain.all_owner_balances().await,
        HashMap::from_iter(expected_balances)
    );
}
