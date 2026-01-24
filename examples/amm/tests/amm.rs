// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the AMM application.

#![cfg(not(target_arch = "wasm32"))]

use amm::{AmmAbi, Operation, Parameters};
use fungible::{InitialStateBuilder, Parameters as FungibleParameters};
use linera_sdk::{
    abis::fungible::FungibleOperation,
    linera_base_types::{Account, AccountOwner, Amount, ApplicationId},
    test::{ActiveChain, TestValidator},
};

struct Setup {
    _validator: TestValidator,
    amm_chain: ActiveChain,
    liquidity_chain: ActiveChain,
    swapper_chain: ActiveChain,
    liquidity_owner: AccountOwner,
    swapper_owner: AccountOwner,
    token0_id: ApplicationId<fungible::FungibleTokenAbi>,
    token1_id: ApplicationId<fungible::FungibleTokenAbi>,
    amm_id: ApplicationId<AmmAbi>,
    amm_pool_owner: AccountOwner,
}

impl Setup {
    async fn new() -> Self {
        let (validator, amm_module_id) =
            TestValidator::with_current_module::<AmmAbi, Parameters, ()>().await;

        let mut amm_chain = validator.new_chain().await;
        let amm_chain_owner = AccountOwner::from(amm_chain.public_key());
        let liquidity_chain = validator.new_chain().await;
        let liquidity_owner = AccountOwner::from(liquidity_chain.public_key());
        let swapper_chain = validator.new_chain().await;
        let swapper_owner = AccountOwner::from(swapper_chain.public_key());

        let fungible_module_id = amm_chain
            .publish_bytecode_files_in::<
                fungible::FungibleTokenAbi,
                fungible::Parameters,
                fungible::InitialState,
            >("../fungible")
            .await;

        let token0_id = amm_chain
            .create_application(
                fungible_module_id,
                FungibleParameters::new("TK0"),
                InitialStateBuilder::default()
                    .with_account(amm_chain_owner, Amount::from_tokens(100))
                    .build(),
                vec![],
            )
            .await;
        let token1_id = amm_chain
            .create_application(
                fungible_module_id,
                FungibleParameters::new("TK1"),
                InitialStateBuilder::default()
                    .with_account(amm_chain_owner, Amount::from_tokens(100))
                    .build(),
                vec![],
            )
            .await;

        let amm_id = amm_chain
            .create_application(
                amm_module_id,
                Parameters {
                    tokens: [token0_id, token1_id],
                },
                (),
                vec![token0_id.forget_abi(), token1_id.forget_abi()],
            )
            .await;
        let amm_pool_owner: AccountOwner = amm_id.into();

        amm_chain
            .add_block(|block| {
                block.with_operation(
                    token0_id,
                    FungibleOperation::Transfer {
                        owner: amm_chain_owner,
                        amount: Amount::from_tokens(50),
                        target_account: Account {
                            chain_id: liquidity_chain.id(),
                            owner: liquidity_owner,
                        },
                    },
                );
                block.with_operation(
                    token0_id,
                    FungibleOperation::Transfer {
                        owner: amm_chain_owner,
                        amount: Amount::from_tokens(30),
                        target_account: Account {
                            chain_id: swapper_chain.id(),
                            owner: swapper_owner,
                        },
                    },
                );
                block.with_operation(
                    token1_id,
                    FungibleOperation::Transfer {
                        owner: amm_chain_owner,
                        amount: Amount::from_tokens(50),
                        target_account: Account {
                            chain_id: liquidity_chain.id(),
                            owner: liquidity_owner,
                        },
                    },
                );
                block.with_operation(
                    token1_id,
                    FungibleOperation::Transfer {
                        owner: amm_chain_owner,
                        amount: Amount::from_tokens(30),
                        target_account: Account {
                            chain_id: swapper_chain.id(),
                            owner: swapper_owner,
                        },
                    },
                );
            })
            .await;

        liquidity_chain.handle_received_messages().await;
        swapper_chain.handle_received_messages().await;

        assert_eq!(
            fungible::query_account(token0_id, &liquidity_chain, liquidity_owner).await,
            Some(Amount::from_tokens(50))
        );
        assert_eq!(
            fungible::query_account(token1_id, &liquidity_chain, liquidity_owner).await,
            Some(Amount::from_tokens(50))
        );
        assert_eq!(
            fungible::query_account(token0_id, &swapper_chain, swapper_owner).await,
            Some(Amount::from_tokens(30))
        );
        assert_eq!(
            fungible::query_account(token1_id, &swapper_chain, swapper_owner).await,
            Some(Amount::from_tokens(30))
        );

        Self {
            _validator: validator,
            amm_chain,
            liquidity_chain,
            swapper_chain,
            liquidity_owner,
            swapper_owner,
            token0_id,
            token1_id,
            amm_id,
            amm_pool_owner,
        }
    }
}

#[tokio::test]
async fn add_liquidity_swap_and_remove_all() {
    let setup = Setup::new().await;

    let (add_liquidity_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::AddLiquidity {
                    owner: setup.liquidity_owner,
                    max_token0_amount: Amount::from_tokens(40),
                    max_token1_amount: Amount::from_tokens(40),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&add_liquidity_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.swapper_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(
            setup.token0_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(10))
    );
    assert_eq!(
        fungible::query_account(
            setup.token1_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(10))
    );
    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(40))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(40))
    );

    let (swap_certificate, _) = setup
        .swapper_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::Swap {
                    owner: setup.swapper_owner,
                    input_token_idx: 0,
                    input_amount: Amount::from_tokens(10),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&swap_certificate);
        })
        .await;

    setup.swapper_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.swapper_chain, setup.swapper_owner).await,
        Some(Amount::from_tokens(20))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.swapper_chain, setup.swapper_owner).await,
        Some(Amount::from_tokens(38))
    );
    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(50))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(32))
    );

    let (remove_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::RemoveAllAddedLiquidity {
                    owner: setup.liquidity_owner,
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&remove_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(
            setup.token0_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(60))
    );
    assert_eq!(
        fungible::query_account(
            setup.token1_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(42))
    );
    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.amm_chain, setup.amm_pool_owner).await,
        None
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.amm_chain, setup.amm_pool_owner).await,
        None
    );
}

#[tokio::test]
async fn add_liquidity_with_ratio_refund() {
    let setup = Setup::new().await;

    let (first_add_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::AddLiquidity {
                    owner: setup.liquidity_owner,
                    max_token0_amount: Amount::from_tokens(20),
                    max_token1_amount: Amount::from_tokens(20),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&first_add_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    let (second_add_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::AddLiquidity {
                    owner: setup.liquidity_owner,
                    max_token0_amount: Amount::from_tokens(30),
                    max_token1_amount: Amount::from_tokens(10),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&second_add_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(
            setup.token0_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(20))
    );
    assert_eq!(
        fungible::query_account(
            setup.token1_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(20))
    );
    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(30))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(30))
    );
}

#[tokio::test]
async fn swap_from_token1() {
    let setup = Setup::new().await;

    let (add_liquidity_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::AddLiquidity {
                    owner: setup.liquidity_owner,
                    max_token0_amount: Amount::from_tokens(40),
                    max_token1_amount: Amount::from_tokens(40),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&add_liquidity_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    let (swap_certificate, _) = setup
        .swapper_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::Swap {
                    owner: setup.swapper_owner,
                    input_token_idx: 1,
                    input_amount: Amount::from_tokens(10),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&swap_certificate);
        })
        .await;

    setup.swapper_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.swapper_chain, setup.swapper_owner).await,
        Some(Amount::from_tokens(38))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.swapper_chain, setup.swapper_owner).await,
        Some(Amount::from_tokens(20))
    );
    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(32))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(50))
    );
}

#[tokio::test]
async fn remove_liquidity_partial() {
    let setup = Setup::new().await;

    let (add_liquidity_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::AddLiquidity {
                    owner: setup.liquidity_owner,
                    max_token0_amount: Amount::from_tokens(40),
                    max_token1_amount: Amount::from_tokens(40),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&add_liquidity_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    let (remove_certificate, _) = setup
        .liquidity_chain
        .add_block(|block| {
            block.with_operation(
                setup.amm_id,
                Operation::RemoveLiquidity {
                    owner: setup.liquidity_owner,
                    token_to_remove_idx: 0,
                    token_to_remove_amount: Amount::from_tokens(10),
                },
            );
        })
        .await;

    setup
        .amm_chain
        .add_block(|block| {
            block.with_messages_from(&remove_certificate);
        })
        .await;

    setup.liquidity_chain.handle_received_messages().await;
    setup.amm_chain.handle_received_messages().await;

    assert_eq!(
        fungible::query_account(
            setup.token0_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(20))
    );
    assert_eq!(
        fungible::query_account(
            setup.token1_id,
            &setup.liquidity_chain,
            setup.liquidity_owner
        )
        .await,
        Some(Amount::from_tokens(20))
    );
    assert_eq!(
        fungible::query_account(setup.token0_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(30))
    );
    assert_eq!(
        fungible::query_account(setup.token1_id, &setup.amm_chain, setup.amm_pool_owner).await,
        Some(Amount::from_tokens(30))
    );
}
