// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration test for the EVM bridge app's `Burn` operation. A user submits
//! `BridgeOperation::Burn` on their own chain; the bridge moves the tokens into
//! the signer's *own* escrow account on the bridge chain, burns them, and emits
//! a `BurnEvent` that the relayer forwards to EVM. The bridge never holds the
//! escrow under its own application id.

#![cfg(not(target_arch = "wasm32"))]

use evm_bridge::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters, EvmBridgeAbi};
use linera_sdk::{
    linera_base_types::{AccountOwner, ApplicationId, U128},
    test::{ActiveChain, TestValidator},
};
use wrapped_fungible::{
    InitialState, InitialStateBuilder, WrappedFungibleOperation, WrappedFungibleTokenAbi,
    WrappedParameters,
};

/// Queries an account balance on the wrapped-fungible app.
async fn query_balance(
    app_id: ApplicationId<WrappedFungibleTokenAbi>,
    chain: &ActiveChain,
    owner: AccountOwner,
) -> Option<U128> {
    use async_graphql::InputType;
    use linera_sdk::test::QueryOutcome;

    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(app_id, query).await;
    let balance = response["accounts"]["entry"]["value"].as_str()?;
    Some(U128(
        balance.parse().expect("balance cannot be parsed as u128"),
    ))
}

#[tokio::test]
async fn test_burn_via_bridge_debits_owner_and_burns_escrow() {
    let (validator, bridge_module_id) = TestValidator::with_current_module::<
        EvmBridgeAbi,
        BridgeParameters,
        BridgeInstantiationArgument,
    >()
    .await;
    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    let token_address = [0xA0; 20];
    let source_chain_id = 8453u64;
    let initial_supply = U128(1_000_000);

    // 1. Deploy wrapped-fungible first (the bridge takes its app id as a
    //    creation parameter), seeding the user with a balance.
    let fungible_module_id = chain
        .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
            "../../../examples/wrapped-fungible",
        )
        .await;
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        decimals: 6,
        mint_chain_id: chain.id(),
        evm_token_address: token_address,
        evm_source_chain_id: source_chain_id,
    };
    let fungible_app_id = chain
        .create_application(
            fungible_module_id,
            wrapped_params,
            InitialStateBuilder::default()
                .with_account(chain_owner, initial_supply)
                .build(),
            vec![],
        )
        .await;

    // 2. Deploy the bridge (the bridge chain is this chain), pointing it at the
    //    wrapped-fungible app.
    let bridge_params = BridgeParameters {
        source_chain_id,
        token_address,
        bridge_chain_id: chain.id(),
        fungible_app_id: fungible_app_id.forget_abi(),
    };
    let bridge_app_id = chain
        .create_application(
            bridge_module_id,
            bridge_params,
            BridgeInstantiationArgument::default(),
            vec![],
        )
        .await;

    // 3. Register the bridge with the wrapped-fungible app so it may mint/burn.
    chain
        .add_block(|block| {
            block.with_operation(
                fungible_app_id,
                WrappedFungibleOperation::RegisterAuthorizedCaller {
                    app_id: bridge_app_id.forget_abi(),
                },
            );
        })
        .await;

    let transfer_amount = U128(600_000);

    // 4. User burns part of the balance toward an EVM address.
    let evm_target = [0xE7; 20];
    let (burn_cert, _) = chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::Burn {
                    amount: transfer_amount,
                    evm_target,
                },
            );
        })
        .await;

    // Between the Burn operation and the burn message, the bridge must NOT hold
    // the escrow under its own application id: the user is the escrow holder.
    let bridge_as_owner = AccountOwner::from(bridge_app_id.forget_abi());
    assert_eq!(
        query_balance(fungible_app_id, &chain, bridge_as_owner).await,
        None,
        "bridge must never escrow tokens under its own application id",
    );

    // 5. Process the BridgeMessage::Burn that the operation sent to the bridge
    //    chain (here, the same chain).
    let (burn_msg_cert, _) = chain
        .add_block(|block| {
            block.with_messages_from(&burn_cert);
        })
        .await;

    let expected_amount = U128(initial_supply.0 - transfer_amount.0);

    // The signer was debited by the burned amount.
    assert_eq!(
        query_balance(fungible_app_id, &chain, chain_owner).await,
        Some(expected_amount),
        "signer must be debited by the burn amount"
    );

    // The bridge still holds nothing under its own application id after the burn.
    assert_eq!(
        query_balance(fungible_app_id, &chain, bridge_as_owner).await,
        None,
        "bridge must never escrow tokens under its own application id"
    );

    // The bridge emitted a BurnEvent in the burn-processing block.
    let event_count: usize = burn_msg_cert
        .inner()
        .block()
        .body
        .events
        .iter()
        .map(|tx_events| tx_events.len())
        .sum();
    assert!(
        event_count >= 1,
        "bridge must emit a BurnEvent for the relayer"
    );
}
