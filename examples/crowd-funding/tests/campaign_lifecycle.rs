// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the Fungible Token application.

#![cfg(not(target_arch = "wasm32"))]

use std::iter;

use crowd_funding::{CrowdFundingAbi, InstantiationArgument, Operation};
use fungible::FungibleTokenAbi;
use linera_sdk::{
    linera_base_types::{
        AccountOwner, AccountSecretKey, Amount, ApplicationId, Ed25519SecretKey,
        Secp256k1SecretKey, Timestamp,
    },
    test::TestValidator,
};

/// Test creating a campaign and collecting pledges.
///
/// Creates a campaign on a `campaign_chain` and sets up the fungible token to use with three
/// backer chains. Pledges part of each backer's balance to the campaign and then completes it,
/// collecting the pledges. The final balance of each backer and the campaign owner is checked.
#[tokio::test(flavor = "multi_thread")]
async fn collect_pledges() {
    let initial_amount = Amount::from_tokens(100);
    let target_amount = Amount::from_tokens(220);
    let pledge_amount = Amount::from_tokens(75);

    let (validator, bytecode_id) = TestValidator::with_current_bytecode::<
        CrowdFundingAbi,
        ApplicationId<FungibleTokenAbi>,
        InstantiationArgument,
    >()
    .await;

    let fungible_chain_owner: AccountSecretKey = Ed25519SecretKey::generate().into();
    let fungible_publisher_chain = validator.new_chain_with_keypair(fungible_chain_owner).await;
    let campaign_chain_owner: AccountSecretKey = Secp256k1SecretKey::generate().into();
    let mut campaign_chain = validator.new_chain_with_keypair(campaign_chain_owner).await;
    let campaign_account = AccountOwner::from(campaign_chain.public_key());

    let fungible_bytecode_id = fungible_publisher_chain
        .publish_bytecodes_in("../fungible")
        .await;

    let (token_id, backers) = fungible::create_with_accounts(
        &validator,
        fungible_bytecode_id,
        iter::repeat(initial_amount).take(3),
    )
    .await;

    let campaign_state = InstantiationArgument {
        owner: campaign_account,
        deadline: Timestamp::from(u64::MAX),
        target: target_amount,
    };
    let campaign_id = campaign_chain
        .create_application(
            bytecode_id,
            token_id,
            campaign_state,
            vec![token_id.forget_abi()],
        )
        .await;

    let mut pledges_and_transfers = Vec::new();

    for (backer_chain, backer_account, _balance) in &backers {
        backer_chain.register_application(campaign_id).await;

        let pledge_certificate = backer_chain
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

        assert_eq!(pledge_certificate.outgoing_message_count(), 3);
        pledges_and_transfers.push(pledge_certificate);
    }

    campaign_chain
        .add_block(|block| {
            for certificate in &pledges_and_transfers {
                block.with_messages_from(certificate);
            }
        })
        .await;

    assert_eq!(
        fungible::query_account(token_id, &campaign_chain, campaign_account).await,
        None
    );

    campaign_chain
        .add_block(|block| {
            block.with_operation(campaign_id, Operation::Collect);
        })
        .await;

    assert_eq!(
        fungible::query_account(token_id, &campaign_chain, campaign_account).await,
        Some(pledge_amount.saturating_mul(backers.len() as u128)),
    );

    for (backer_chain, backer_account, initial_amount) in backers {
        assert_eq!(
            fungible::query_account(token_id, &backer_chain, backer_account).await,
            Some(initial_amount.saturating_sub(pledge_amount)),
        );
        assert_eq!(
            fungible::query_account(token_id, &campaign_chain, backer_account).await,
            None,
        );
    }
}

/// Test creating a campaign and cancelling it.
///
/// Creates a campaign on a `campaign_chain` and sets up the fungible token to use with three
/// backer chains. Pledges part of each backer's balance to the campaign and then completes it,
/// collecting the pledges. The final balance of each backer and the campaign owner is checked.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_successful_campaign() {
    let initial_amount = Amount::from_tokens(100);
    let target_amount = Amount::from_tokens(220);
    let pledge_amount = Amount::from_tokens(75);

    let (validator, bytecode_id) = TestValidator::with_current_bytecode::<
        CrowdFundingAbi,
        ApplicationId<FungibleTokenAbi>,
        InstantiationArgument,
    >()
    .await;

    let fungible_publisher_chain = validator.new_chain().await;
    let mut campaign_chain = validator.new_chain().await;
    let campaign_account = AccountOwner::from(campaign_chain.public_key());

    let fungible_bytecode_id = fungible_publisher_chain
        .publish_bytecodes_in("../fungible")
        .await;

    let (token_id, backers) = fungible::create_with_accounts(
        &validator,
        fungible_bytecode_id,
        iter::repeat(initial_amount).take(3),
    )
    .await;

    let campaign_state = InstantiationArgument {
        owner: campaign_account,
        deadline: Timestamp::from(10),
        target: target_amount,
    };
    let campaign_id = campaign_chain
        .create_application(
            bytecode_id,
            token_id,
            campaign_state,
            vec![token_id.forget_abi()],
        )
        .await;

    let mut pledges_and_transfers = Vec::new();

    for (backer_chain, backer_account, _balance) in &backers {
        backer_chain.register_application(campaign_id).await;

        let pledge_certificate = backer_chain
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

        assert_eq!(pledge_certificate.outgoing_message_count(), 3);
        pledges_and_transfers.push(pledge_certificate);
    }

    campaign_chain
        .add_block(|block| {
            for certificate in &pledges_and_transfers {
                block.with_messages_from(certificate);
            }
        })
        .await;

    assert_eq!(
        fungible::query_account(token_id, &campaign_chain, campaign_account).await,
        None
    );

    campaign_chain
        .add_block(|block| {
            block
                .with_timestamp(Timestamp::from(20))
                .with_operation(campaign_id, Operation::Cancel);
        })
        .await;

    assert_eq!(
        fungible::query_account(token_id, &campaign_chain, campaign_account).await,
        None,
    );

    for (backer_chain, backer_account, initial_amount) in backers {
        assert_eq!(
            fungible::query_account(token_id, &backer_chain, backer_account).await,
            Some(initial_amount.saturating_sub(pledge_amount)),
        );
        assert_eq!(
            fungible::query_account(token_id, &campaign_chain, backer_account).await,
            Some(pledge_amount),
        );
    }
}
