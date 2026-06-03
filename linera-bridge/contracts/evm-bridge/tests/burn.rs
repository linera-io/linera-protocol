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
    linera_base_types::{AccountOwner, ApplicationId, GenericApplicationId, StreamName, U128},
    test::{ActiveChain, MessageAction, TestValidator},
};
use wrapped_fungible::{
    Account, BurnEvent, InitialState, InitialStateBuilder, WrappedFungibleOperation,
    WrappedFungibleTokenAbi, WrappedParameters,
};

/// Happy path of the bridge-driven burn, end to end across two chains.
///
/// A user on their own chain submits `Burn`, which debits them locally and sends a
/// (`Credit` + `BridgeMessage::Burn`) bundle to the bridge chain. The bridge chain accepts
/// it: the funding `Credit` lands in the user's *own* escrow account, then the burn consumes
/// that escrow and emits the `BurnEvent` the relayer forwards to EVM. Asserts the user is
/// debited on their chain, the escrow is funded-then-burned to nothing, the bridge never
/// holds tokens under its own application id, and a `BurnEvent` is emitted.
#[tokio::test]
async fn test_burn_via_bridge_debits_owner_and_burns_escrow() {
    let initial_supply = U128(1_000_000);
    let BridgeDeployment {
        validator,
        bridge_chain,
        chain_owner: bridge_owner,
        fungible_app_id,
        bridge_app_id,
    } = deploy(initial_supply, true).await;

    // Give a user a wrapped balance on their own chain (distinct from the bridge chain),
    // by transferring from the bridge chain's owner. The user drives the burn from there.
    let user_chain = validator.new_chain().await;
    let user = AccountOwner::from(user_chain.public_key());
    let user_balance = U128(600_000);

    bridge_chain
        .add_block(|block| {
            block.with_operation(
                fungible_app_id,
                WrappedFungibleOperation::Transfer {
                    owner: bridge_owner,
                    amount: user_balance,
                    target_account: Account {
                        chain_id: user_chain.id(),
                        owner: user,
                    },
                },
            );
        })
        .await;
    user_chain.handle_received_messages().await;
    assert_eq!(
        query_balance(fungible_app_id, &user_chain, user).await,
        Some(user_balance),
        "user must hold the transferred wrapped balance on their own chain",
    );

    // The user burns part of their balance toward an EVM address. This debits the user on
    // their chain and sends the (Credit + BridgeMessage::Burn) bundle to the bridge chain.
    let burn_amount = U128(250_000);
    let evm_target = [0xE7; 20];
    let (burn_cert, _) = user_chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::Burn {
                    amount: burn_amount,
                    evm_target,
                },
            );
        })
        .await;

    // The user is debited on their own chain while the burn is in flight.
    assert_eq!(
        query_balance(fungible_app_id, &user_chain, user).await,
        Some(U128(user_balance.0 - burn_amount.0)),
        "user must be debited by the burn amount on their own chain",
    );

    // The escrow is the user's own account on the bridge chain — the bridge must never hold
    // the escrow under its own application id.
    let bridge_as_owner = AccountOwner::from(bridge_app_id.forget_abi());
    assert_eq!(
        query_balance(fungible_app_id, &bridge_chain, bridge_as_owner).await,
        None,
        "bridge must never escrow tokens under its own application id",
    );

    // The bridge chain accepts the bundle: the funding Credit lands in the user's escrow
    // account, then BridgeMessage::Burn burns that escrow and emits the BurnEvent.
    let (burn_msg_cert, _) = bridge_chain
        .add_block(|block| {
            block.with_messages_from(&burn_cert);
        })
        .await;

    // The escrow on the bridge chain was funded and then fully burned — net zero.
    assert_eq!(
        query_balance(fungible_app_id, &bridge_chain, user).await,
        None,
        "the user's escrow on the bridge chain must be burned, not left behind",
    );
    assert_eq!(
        query_balance(fungible_app_id, &bridge_chain, bridge_as_owner).await,
        None,
        "bridge must never escrow tokens under its own application id",
    );

    // The bridge emitted exactly one BurnEvent, on its "burns" stream, carrying the user's
    // EVM target and burn amount — this is what the relayer forwards to EVM.
    let events: Vec<_> = burn_msg_cert
        .inner()
        .block()
        .body
        .events
        .iter()
        .flatten()
        .collect();
    assert_eq!(
        events.len(),
        1,
        "the burn-processing block must emit exactly one event",
    );
    let event = events[0];
    assert_eq!(
        event.stream_id.application_id,
        GenericApplicationId::User(bridge_app_id.forget_abi()),
        "the event must be emitted by the bridge application — the relayer keys burns on it",
    );
    assert_eq!(
        event.stream_id.stream_name,
        StreamName::from("burns"),
        "the event must be on the bridge's \"burns\" stream",
    );
    let burn_event: BurnEvent = linera_sdk::bcs::from_bytes(&event.value)
        .expect("the event value must decode as a BurnEvent");
    assert_eq!(
        burn_event,
        BurnEvent {
            target: evm_target,
            amount: burn_amount,
        },
        "the BurnEvent must carry the user's EVM target and burn amount",
    );
}

/// Refund-on-reject: the load-bearing safety property of the bridge-driven burn.
///
/// A user on their own chain submits `Burn`, which in one bundle escrows their wrapped
/// tokens onto the bridge chain (a tracked `Credit`) and sends a tracked
/// `BridgeMessage::Burn`. If the bridge chain rejects that bundle, the funding `Credit`
/// must bounce and refund the user on *their* chain — never leaving the escrow stranded on
/// the bridge chain. This test forces the rejection and asserts the user is made whole.
#[tokio::test]
async fn test_rejected_burn_refunds_user_and_strands_no_escrow() {
    let initial_supply = U128(1_000_000);
    let BridgeDeployment {
        validator,
        bridge_chain,
        chain_owner: bridge_owner,
        fungible_app_id,
        bridge_app_id,
    } = deploy(initial_supply, true).await;

    // Give a user on their own chain a wrapped balance, by transferring from the bridge
    // chain's owner. The user — not the bridge chain owner — will drive the burn.
    let user_chain = validator.new_chain().await;
    let user = AccountOwner::from(user_chain.public_key());
    let user_balance = U128(400_000);

    bridge_chain
        .add_block(|block| {
            block.with_operation(
                fungible_app_id,
                WrappedFungibleOperation::Transfer {
                    owner: bridge_owner,
                    amount: user_balance,
                    target_account: Account {
                        chain_id: user_chain.id(),
                        owner: user,
                    },
                },
            );
        })
        .await;
    user_chain.handle_received_messages().await;
    assert_eq!(
        query_balance(fungible_app_id, &user_chain, user).await,
        Some(user_balance),
        "user must hold the transferred wrapped balance on their own chain",
    );

    // The user burns part of their balance toward an EVM address. This debits the user on
    // their chain and sends the (Credit + BridgeMessage::Burn) bundle to the bridge chain.
    let burn_amount = U128(250_000);
    let evm_target = [0xE7; 20];
    let (burn_cert, _) = user_chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::Burn {
                    amount: burn_amount,
                    evm_target,
                },
            );
        })
        .await;
    assert_eq!(
        query_balance(fungible_app_id, &user_chain, user).await,
        Some(U128(user_balance.0 - burn_amount.0)),
        "user is debited on their chain while the burn is in flight",
    );

    // The bridge chain rejects the burn bundle (e.g. the burn handler failed). Both tracked
    // messages bounce back to the user's chain.
    let (reject_cert, _) = bridge_chain
        .add_block(|block| {
            block.with_messages_from_by_action(&burn_cert, MessageAction::Reject);
        })
        .await;

    // No burn was performed: no escrow held on the bridge chain, and no BurnEvent emitted.
    assert_eq!(
        query_balance(fungible_app_id, &bridge_chain, user).await,
        None,
        "a rejected burn must leave no escrow stranded on the bridge chain",
    );
    let reject_event_count: usize = reject_cert
        .inner()
        .block()
        .body
        .events
        .iter()
        .map(|tx_events| tx_events.len())
        .sum();
    assert_eq!(
        reject_event_count, 0,
        "a rejected burn must not emit a BurnEvent",
    );

    // The funding Credit bounces back and the user is made whole on their own chain.
    user_chain.handle_received_messages().await;
    assert_eq!(
        query_balance(fungible_app_id, &user_chain, user).await,
        Some(user_balance),
        "a rejected burn must refund the user in full on their own chain",
    );
}

/// A wrapped-fungible + evm-bridge deployment, both apps on one chain that is both the
/// bridge chain and the wrapped-fungible mint chain. Each test adds its own separate user
/// chain on top of this.
struct BridgeDeployment {
    validator: TestValidator,
    bridge_chain: ActiveChain,
    chain_owner: AccountOwner,
    fungible_app_id: ApplicationId<WrappedFungibleTokenAbi>,
    bridge_app_id: ApplicationId<EvmBridgeAbi>,
}

/// Deploys wrapped-fungible (seeded with `initial_supply` for the chain owner) and the
/// evm-bridge that points at it, on a single chain that is both the bridge chain and the
/// wrapped-fungible mint chain. When `register_caller` is set, the bridge is registered as
/// the wrapped-fungible authorized caller (required for `Mint`/`Burn` to succeed).
async fn deploy(initial_supply: U128, register_caller: bool) -> BridgeDeployment {
    let (validator, bridge_module_id) = TestValidator::with_current_module::<
        EvmBridgeAbi,
        BridgeParameters,
        BridgeInstantiationArgument,
    >()
    .await;
    let mut bridge_chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(bridge_chain.public_key());

    let token_address = [0xA0; 20];
    let source_chain_id = 8453u64;

    let fungible_module_id = bridge_chain
        .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
            "../../../examples/wrapped-fungible",
        )
        .await;
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        decimals: 6,
        mint_chain_id: bridge_chain.id(),
        evm_token_address: token_address,
        evm_source_chain_id: source_chain_id,
    };
    let fungible_app_id = bridge_chain
        .create_application(
            fungible_module_id,
            wrapped_params,
            InitialStateBuilder::default()
                .with_account(chain_owner, initial_supply)
                .build(),
            vec![],
        )
        .await;

    let bridge_params = BridgeParameters {
        source_chain_id,
        token_address,
        bridge_chain_id: bridge_chain.id(),
        fungible_app_id: fungible_app_id.forget_abi(),
    };
    let bridge_app_id = bridge_chain
        .create_application(
            bridge_module_id,
            bridge_params,
            BridgeInstantiationArgument::default(),
            vec![],
        )
        .await;

    if register_caller {
        bridge_chain
            .add_block(|block| {
                block.with_operation(
                    fungible_app_id,
                    WrappedFungibleOperation::RegisterAuthorizedCaller {
                        app_id: bridge_app_id.forget_abi(),
                    },
                );
            })
            .await;
    }

    BridgeDeployment {
        validator,
        bridge_chain,
        chain_owner,
        fungible_app_id,
        bridge_app_id,
    }
}

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
