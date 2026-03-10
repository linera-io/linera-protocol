// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Crowd-Funding with Native Fungible Token.
//!
//! These tests exercise the `transfer_auth_depth` / `claim_auth_depth` code path:
//! crowd-funding calls native-fungible, which in turn calls `transfer_auth_depth(1, ...)`
//! to authenticate the transfer using the caller (crowd-funding) rather than itself
//! (native-fungible).

#![cfg(not(target_arch = "wasm32"))]

use crowd_funding::{CrowdFundingAbi, InstantiationArgument, Operation};
use fungible::{FungibleTokenAbi, InitialState, Parameters};
use linera_sdk::{
    linera_base_types::{
        Account, AccountOwner, Amount, ApplicationId, Timestamp,
    },
    test::TestValidator,
};

/// Test creating a campaign backed by native-fungible tokens and collecting pledges.
///
/// This exercises the critical `transfer_auth_depth` code path: when crowd-funding
/// collects pledges, it transfers from its own app-owned account via native-fungible.
/// Without `transfer_auth_depth`, the system transfer would fail because
/// native-fungible's own app ID would be used for authentication instead of
/// crowd-funding's app ID.
#[tokio::test(flavor = "multi_thread")]
async fn collect_pledges_native_fungible() {
    let initial_amount = Amount::from_tokens(5);
    let target_amount = Amount::from_tokens(6);
    let pledge_amount = Amount::from_tokens(3);

    let (validator, crowd_funding_module_id) = TestValidator::with_current_module::<
        CrowdFundingAbi,
        ApplicationId<FungibleTokenAbi>,
        InstantiationArgument,
    >()
    .await;

    // Create the campaign chain.
    let mut campaign_chain = validator.new_chain().await;
    let campaign_account = AccountOwner::from(campaign_chain.public_key());

    // Publish native-fungible bytecode.
    let native_fungible_publisher = validator.new_chain().await;
    let native_fungible_module_id = native_fungible_publisher
        .publish_bytecode_files_in::<FungibleTokenAbi, Parameters, InitialState>(
            "../native-fungible",
        )
        .await;

    // Create the native-fungible application on a dedicated chain.
    let mut native_fungible_chain = validator.new_chain().await;
    let native_fungible_params = Parameters::new("NAT");
    let native_fungible_initial_state = fungible::InitialStateBuilder::default().build();
    let native_fungible_id = native_fungible_chain
        .create_application(
            native_fungible_module_id,
            native_fungible_params,
            native_fungible_initial_state,
            vec![],
        )
        .await;

    // Create the crowd-funding campaign with native-fungible as the token.
    let campaign_state = InstantiationArgument {
        owner: campaign_account,
        deadline: Timestamp::from(u64::MAX),
        target: target_amount,
    };
    let campaign_id = campaign_chain
        .create_application(
            crowd_funding_module_id,
            native_fungible_id,
            campaign_state,
            vec![native_fungible_id.forget_abi()],
        )
        .await;

    // Create backer chains and fund them with native tokens in their owner accounts.
    let num_backers = 3;
    let mut backers = Vec::new();
    let admin_chain = validator.get_chain(&validator.admin_chain_id());

    for _ in 0..num_backers {
        let backer_chain = validator.new_chain().await;
        let backer_account = AccountOwner::from(backer_chain.public_key());

        // Transfer native tokens from the admin chain balance to the backer's
        // owner account on the backer chain.
        let recipient = Account::new(backer_chain.id(), backer_account);
        let (transfer_cert, _) = admin_chain
            .add_block(|block| {
                block.with_native_token_transfer(
                    AccountOwner::CHAIN,
                    recipient,
                    initial_amount,
                );
            })
            .await;

        // Receive the transfer on the backer chain.
        backer_chain
            .add_block(|block| {
                block.with_messages_from(&transfer_cert);
            })
            .await;

        backers.push((backer_chain, backer_account));
    }

    // Each backer pledges to the campaign. This calls crowd-funding on the backer chain,
    // which calls native-fungible's Transfer operation via call_application.
    // Native-fungible then calls transfer_auth_depth(owner, target, amount, 1).
    let mut pledge_certificates = Vec::new();
    for (backer_chain, backer_account) in &backers {
        let (pledge_cert, _) = backer_chain
            .add_block(|block| {
                block.with_operation(
                    campaign_id,
                    Operation::Pledge {
                        owner: *backer_account,
                        amount: pledge_amount,
                    },
                );
            })
            .await;

        pledge_certificates.push(pledge_cert);
    }

    // Receive all pledge messages on the campaign chain.
    campaign_chain
        .add_block(|block| {
            for cert in &pledge_certificates {
                block.with_messages_from(cert);
            }
        })
        .await;

    // The crowd-funding app's account on the campaign chain should now hold
    // the pledged native tokens (3 backers * 3 tokens = 9 tokens).
    let crowd_funding_owner: AccountOwner = campaign_id.forget_abi().into();
    let crowd_funding_balance = campaign_chain.owner_balance(&crowd_funding_owner).await;
    assert_eq!(
        crowd_funding_balance,
        Some(pledge_amount.saturating_mul(num_backers as u128)),
    );

    // Collect pledges. This is the critical test: crowd-funding calls native-fungible
    // to transfer from crowd-funding's app-owned account to the campaign owner.
    // native-fungible uses transfer_auth_depth(1, ...) so the system sees
    // crowd-funding (the caller) as the authenticated app, not native-fungible.
    campaign_chain
        .add_block(|block| {
            block.with_operation(campaign_id, Operation::Collect);
        })
        .await;

    // The campaign owner should now have received the collected pledges.
    let campaign_owner_balance = campaign_chain.owner_balance(&campaign_account).await;
    assert_eq!(
        campaign_owner_balance,
        Some(pledge_amount.saturating_mul(num_backers as u128)),
    );

    // Verify backer balances: each should have (initial - pledge) in their owner account.
    for (backer_chain, backer_account) in &backers {
        let remaining = backer_chain.owner_balance(backer_account).await;
        assert_eq!(remaining, Some(initial_amount.saturating_sub(pledge_amount)));
    }
}
