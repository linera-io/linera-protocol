// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Wrapped Fungible Token application's Mint and Burn operations.

#![cfg(not(target_arch = "wasm32"))]

use fungible::{Account, FungibleOperation, InitialState, InitialStateBuilder};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount},
    test::TestValidator,
};
use wrapped_fungible::{WrappedFungibleTokenAbi, WrappedParameters};

/// Helper to query an account balance via GraphQL.
async fn query_account(
    application_id: linera_sdk::linera_base_types::ApplicationId<WrappedFungibleTokenAbi>,
    chain: &linera_sdk::test::ActiveChain,
    account_owner: AccountOwner,
) -> Option<Amount> {
    use async_graphql::InputType;
    use linera_sdk::test::QueryOutcome;

    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        account_owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(application_id, query).await;
    let balance = response.pointer("/accounts/entry/value")?.as_str()?;

    Some(
        balance
            .parse()
            .expect("Account balance cannot be parsed as a number"),
    )
}

fn test_params(minter: AccountOwner) -> WrappedParameters {
    WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        minter,
        evm_token_address: [0xA0; 20],
        evm_source_chain_id: 8453,
    }
}

#[tokio::test]
async fn test_mint_from_authorized_minter() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut minter_chain = validator.new_chain().await;
    let minter_account = AccountOwner::from(minter_chain.public_key());

    let params = test_params(minter_account);
    let initial_state = InitialStateBuilder::default().build();
    let application_id = minter_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    let mint_amount = Amount::from_tokens(1000);

    minter_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Mint {
                    target_account: Account {
                        chain_id: minter_chain.id(),
                        owner: minter_account,
                    },
                    amount: mint_amount,
                },
            );
        })
        .await;

    assert_eq!(
        query_account(application_id, &minter_chain, minter_account).await,
        Some(mint_amount),
    );
}

#[tokio::test]
async fn test_mint_from_unauthorized_signer() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    // Minter is a different account
    let other_minter = AccountOwner::Address20([0xBB; 20]);
    let params = test_params(other_minter);
    let initial_state = InitialStateBuilder::default().build();
    let application_id = chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Chain owner tries to mint, but they're not the minter — should fail
    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Mint {
                    target_account: Account {
                        chain_id: chain.id(),
                        owner: chain_owner,
                    },
                    amount: Amount::from_tokens(100),
                },
            );
        })
        .await;
    assert!(result.is_err(), "mint from unauthorized signer should fail");
}

#[tokio::test]
async fn test_burn_from_authorized_minter() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut minter_chain = validator.new_chain().await;
    let minter_account = AccountOwner::from(minter_chain.public_key());

    let params = test_params(minter_account);
    let initial_state = InitialStateBuilder::default()
        .with_account(minter_account, Amount::from_tokens(500))
        .build();
    let application_id = minter_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    let burn_amount = Amount::from_tokens(200);

    minter_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Burn {
                    owner: minter_account,
                    amount: burn_amount,
                },
            );
        })
        .await;

    assert_eq!(
        query_account(application_id, &minter_chain, minter_account).await,
        Some(Amount::from_tokens(300)),
    );
}

#[tokio::test]
async fn test_burn_from_unauthorized_signer() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    let other_minter = AccountOwner::Address20([0xBB; 20]);
    let params = test_params(other_minter);
    let initial_state = InitialStateBuilder::default()
        .with_account(chain_owner, Amount::from_tokens(500))
        .build();
    let application_id = chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Burn {
                    owner: chain_owner,
                    amount: Amount::from_tokens(100),
                },
            );
        })
        .await;
    assert!(result.is_err(), "burn from unauthorized signer should fail");
}

#[tokio::test]
async fn test_burn_insufficient_balance() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut minter_chain = validator.new_chain().await;
    let minter_account = AccountOwner::from(minter_chain.public_key());

    let params = test_params(minter_account);
    let initial_state = InitialStateBuilder::default()
        .with_account(minter_account, Amount::from_tokens(100))
        .build();
    let application_id = minter_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Try to burn more than balance — should fail
    let result = minter_chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Burn {
                    owner: minter_account,
                    amount: Amount::from_tokens(200),
                },
            );
        })
        .await;
    assert!(
        result.is_err(),
        "burn with insufficient balance should fail"
    );
}

#[tokio::test]
async fn test_wrapped_fungible_standard_transfer() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut chain = validator.new_chain().await;
    let owner = AccountOwner::from(chain.public_key());
    let recipient = AccountOwner::Address20([0xCC; 20]);

    let params = test_params(owner);
    let initial_state = InitialStateBuilder::default()
        .with_account(owner, Amount::from_tokens(1000))
        .build();
    let application_id = chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                FungibleOperation::Transfer {
                    owner,
                    amount: Amount::from_tokens(300),
                    target_account: Account {
                        chain_id: chain.id(),
                        owner: recipient,
                    },
                },
            );
        })
        .await;

    assert_eq!(
        query_account(application_id, &chain, owner).await,
        Some(Amount::from_tokens(700)),
    );
    assert_eq!(
        query_account(application_id, &chain, recipient).await,
        Some(Amount::from_tokens(300)),
    );
}
