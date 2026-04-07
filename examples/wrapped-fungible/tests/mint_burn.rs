// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Wrapped Fungible Token application's Mint and Burn operations.

#![cfg(not(target_arch = "wasm32"))]

use fungible::{InitialState, InitialStateBuilder};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount, ApplicationId, ChainId, CryptoHash, TestString},
    test::TestValidator,
};
use wrapped_fungible::{
    Account, WrappedFungibleOperation, WrappedFungibleTokenAbi, WrappedParameters,
};

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

fn dummy_bridge_app_id() -> ApplicationId {
    ApplicationId::new(CryptoHash::new(&TestString::new("dummy_bridge")))
}

fn test_params(
    minter: AccountOwner,
    mint_chain_id: ChainId,
    bridge_app_id: ApplicationId,
) -> WrappedParameters {
    WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        minter: Some(minter),
        mint_chain_id: Some(mint_chain_id),
        evm_token_address: [0xA0; 20],
        evm_source_chain_id: 8453,
        bridge_app_id: Some(bridge_app_id),
    }
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
    let params = test_params(other_minter, chain.id(), dummy_bridge_app_id());
    let initial_state = InitialStateBuilder::default().build();
    let application_id = chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Chain owner tries to mint, but they're not the minter — should fail
    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                WrappedFungibleOperation::Mint {
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

    let params = test_params(owner, chain.id(), dummy_bridge_app_id());
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
                WrappedFungibleOperation::Transfer {
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

#[tokio::test]
async fn test_credit_to_address20_on_non_bridge_chain_does_not_burn() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut minter_chain = validator.new_chain().await;
    let other_chain = validator.new_chain().await;
    let minter_account = AccountOwner::from(minter_chain.public_key());
    let evm_address = AccountOwner::Address20([0xAA; 20]);

    // Bridge chain is minter_chain; other_chain is NOT the bridge chain.
    let mint_amount = Amount::from_tokens(500);
    let params = test_params(minter_account, minter_chain.id(), dummy_bridge_app_id());
    let initial_state = InitialStateBuilder::default()
        .with_account(minter_account, mint_amount)
        .build();
    let application_id = minter_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Transfer cross-chain to an Address20 on other_chain (NOT the bridge chain).
    let (transfer_cert, _) = minter_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                WrappedFungibleOperation::Transfer {
                    owner: minter_account,
                    amount: mint_amount,
                    target_account: Account {
                        chain_id: other_chain.id(),
                        owner: evm_address,
                    },
                },
            );
        })
        .await;

    // Process the Credit message on other_chain.
    other_chain
        .add_block(|block| {
            block.with_messages_from(&transfer_cert);
        })
        .await;

    // Credit to Address20 on a non-bridge chain should be credited normally.
    let balance = query_account(application_id, &other_chain, evm_address).await;
    assert_eq!(
        balance,
        Some(mint_amount),
        "Credit to Address20 on non-bridge chain should credit normally, not burn"
    );
}

#[tokio::test]
async fn test_credit_to_address20_on_bridge_chain_auto_burns() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut sender_chain = validator.new_chain().await;
    let bridge_chain = validator.new_chain().await;
    let sender_account = AccountOwner::from(sender_chain.public_key());
    let evm_address = AccountOwner::Address20([0xAA; 20]);

    // Bridge chain is bridge_chain (the mint chain).
    let minter = AccountOwner::from(bridge_chain.public_key());
    let params = test_params(minter, bridge_chain.id(), dummy_bridge_app_id());
    let initial_state = InitialStateBuilder::default()
        .with_account(sender_account, Amount::from_tokens(500))
        .build();
    let application_id = sender_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Transfer cross-chain to an Address20 on bridge_chain.
    let (transfer_cert, _) = sender_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                WrappedFungibleOperation::Transfer {
                    owner: sender_account,
                    amount: Amount::from_tokens(500),
                    target_account: Account {
                        chain_id: bridge_chain.id(),
                        owner: evm_address,
                    },
                },
            );
        })
        .await;

    // Process the Credit message on bridge_chain.
    bridge_chain
        .add_block(|block| {
            block.with_messages_from(&transfer_cert);
        })
        .await;

    // Verify sender was debited (tokens removed from circulation).
    let sender_balance = query_account(application_id, &sender_chain, sender_account).await;
    assert_eq!(
        sender_balance, None,
        "Sender's tokens should have been debited on the source chain"
    );

    // Credit to Address20 on the bridge chain should auto-burn: balance must be 0.
    let balance = query_account(application_id, &bridge_chain, evm_address).await;
    assert_eq!(
        balance, None,
        "Credit to Address20 on the bridge chain should auto-burn, not credit the account"
    );
}

#[tokio::test]
async fn test_credit_to_non_address20_on_bridge_chain_credits_normally() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut sender_chain = validator.new_chain().await;
    let bridge_chain = validator.new_chain().await;
    let sender_account = AccountOwner::from(sender_chain.public_key());
    let recipient = AccountOwner::from(bridge_chain.public_key());

    let minter = AccountOwner::from(bridge_chain.public_key());
    let params = test_params(minter, bridge_chain.id(), dummy_bridge_app_id());
    let initial_state = InitialStateBuilder::default()
        .with_account(sender_account, Amount::from_tokens(500))
        .build();
    let application_id = sender_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Transfer cross-chain to a non-Address20 on the bridge chain.
    let (transfer_cert, _) = sender_chain
        .add_block(|block| {
            block.with_operation(
                application_id,
                WrappedFungibleOperation::Transfer {
                    owner: sender_account,
                    amount: Amount::from_tokens(500),
                    target_account: Account {
                        chain_id: bridge_chain.id(),
                        owner: recipient,
                    },
                },
            );
        })
        .await;

    // Process the Credit message on bridge_chain.
    bridge_chain
        .add_block(|block| {
            block.with_messages_from(&transfer_cert);
        })
        .await;

    // Non-Address20 on the bridge chain should be credited normally.
    let balance = query_account(application_id, &bridge_chain, recipient).await;
    assert_eq!(
        balance,
        Some(Amount::from_tokens(500)),
        "Credit to non-Address20 on bridge chain should credit normally, not burn"
    );
}

#[tokio::test]
async fn test_mint_on_wrong_chain() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut minter_chain = validator.new_chain().await;
    let other_chain = validator.new_chain().await;
    let minter_account = AccountOwner::from(minter_chain.public_key());

    // Designate other_chain as the mint chain, but deploy on minter_chain
    let params = test_params(minter_account, other_chain.id(), dummy_bridge_app_id());
    let initial_state = InitialStateBuilder::default().build();
    let application_id = minter_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Minter is authorized but on the wrong chain — should fail
    let result = minter_chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                WrappedFungibleOperation::Mint {
                    target_account: Account {
                        chain_id: minter_chain.id(),
                        owner: minter_account,
                    },
                    amount: Amount::from_tokens(100),
                },
            );
        })
        .await;
    assert!(result.is_err(), "mint on wrong chain should fail");
}

#[tokio::test]
async fn test_direct_mint_without_bridge_is_rejected() {
    let (validator, module_id) = TestValidator::with_current_module::<
        WrappedFungibleTokenAbi,
        WrappedParameters,
        InitialState,
    >()
    .await;
    let mut minter_chain = validator.new_chain().await;
    let minter_account = AccountOwner::from(minter_chain.public_key());

    let params = test_params(minter_account, minter_chain.id(), dummy_bridge_app_id());
    let initial_state = InitialStateBuilder::default().build();
    let application_id = minter_chain
        .create_application(module_id, params, initial_state, vec![])
        .await;

    // Minter directly calls Mint (not via FungibleBridge) — should be rejected
    let result = minter_chain
        .try_add_block(|block| {
            block.with_operation(
                application_id,
                WrappedFungibleOperation::Mint {
                    target_account: Account {
                        chain_id: minter_chain.id(),
                        owner: minter_account,
                    },
                    amount: Amount::from_tokens(1000),
                },
            );
        })
        .await;
    assert!(
        result.is_err(),
        "direct mint without going through FungibleBridge should be rejected"
    );
}
