// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the EVM bridge app's RefundBurn operation.
//!
//! Mirrors the ProcessDeposit fixture: builds a one-tx EVM block whose
//! receipt contains a single BurnBlocked log, hands it to the bridge as
//! an MPT proof, and asserts that wrapped tokens are minted back to the
//! original burner on the source Linera chain.

#![cfg(not(target_arch = "wasm32"))]

use alloy_primitives::{Address, B256};
use evm_bridge::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters, EvmBridgeAbi};
use fungible::{InitialState, InitialStateBuilder};
use linera_bridge::proof::{
    burn_blocked_event_signature, deposit_event_signature,
    testing::{
        build_burn_blocked_event_data, build_deposit_event_data, build_receipt_trie,
        build_test_header, build_test_receipt,
    },
    ReceiptLog,
};
use linera_sdk::{
    bcs,
    linera_base_types::{AccountOwner, ApplicationId, U128},
    test::{ActiveChain, QueryOutcome, TestValidator},
};
use wrapped_fungible::{WrappedFungibleTokenAbi, WrappedParameters};

/// Queries an account balance on the wrapped-fungible app. The wrapped-fungible
/// state stores raw `U128` (no decimal scaling), so we return that directly
/// rather than re-parsing as `Amount` (which would multiply by 10^18).
async fn query_balance(
    app_id: ApplicationId<WrappedFungibleTokenAbi>,
    chain: &ActiveChain,
    owner: AccountOwner,
) -> Option<U128> {
    use async_graphql::InputType;

    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(app_id, query).await;
    let balance = response["accounts"]["entry"]["value"].as_str()?;
    Some(U128(balance.parse().expect("balance must be a u128")))
}

/// Fixture for refund tests: a registered bridge + fungible app pair on a
/// fresh chain, plus the test-only EVM identifiers and source-account fields
/// the refund flow encodes into the BurnBlocked event.
struct RefundFixture {
    chain: ActiveChain,
    bridge_app_id: ApplicationId<EvmBridgeAbi>,
    fungible_app_id: ApplicationId<WrappedFungibleTokenAbi>,
    bridge_contract: Address,
    source_chain_b256: B256,
    source_owner: AccountOwner,
    source_owner_bcs: Vec<u8>,
}

impl RefundFixture {
    async fn setup() -> Self {
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

        let bridge_params = BridgeParameters {
            source_chain_id,
            token_address,
        };
        let bridge_app_id = chain
            .create_application(
                bridge_module_id,
                bridge_params,
                BridgeInstantiationArgument::default(),
                vec![],
            )
            .await;

        let fungible_module_id = chain
            .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
                "../../../examples/wrapped-fungible",
            )
            .await;
        let wrapped_params = WrappedParameters {
            ticker_symbol: "wUSDC".to_string(),
            decimals: 6,
            minter: Some(chain_owner),
            mint_chain_id: Some(chain.id()),
            evm_token_address: token_address,
            evm_source_chain_id: source_chain_id,
            bridge_app_id: Some(bridge_app_id.forget_abi()),
        };
        let fungible_app_id = chain
            .create_application(
                fungible_module_id,
                wrapped_params,
                InitialStateBuilder::default().build(),
                vec![],
            )
            .await;

        chain
            .add_block(|block| {
                block.with_operation(
                    bridge_app_id,
                    BridgeOperation::RegisterFungibleApp {
                        app_id: fungible_app_id.forget_abi(),
                    },
                );
            })
            .await;

        let bridge_contract_bytes = [0xBB; 20];
        chain
            .add_block(|block| {
                block.with_operation(
                    bridge_app_id,
                    BridgeOperation::RegisterFungibleBridge {
                        address: bridge_contract_bytes,
                    },
                );
            })
            .await;

        // The BurnBlocked event credits the original burner, identified by their
        // (source chain id, source owner) tuple. Mirror this chain's id and the
        // chain-owner's account as that tuple so we can assert mint succeeded.
        let chain_id_bytes: [u8; 32] = chain.id().0.into();
        let source_chain_b256 = B256::from(chain_id_bytes);
        let source_owner = chain_owner;
        let source_owner_bcs = bcs::to_bytes(&source_owner).expect("bcs encode source owner");

        RefundFixture {
            chain,
            bridge_app_id,
            fungible_app_id,
            bridge_contract: Address::from(bridge_contract_bytes),
            source_chain_b256,
            source_owner,
            source_owner_bcs,
        }
    }

    /// Builds a valid receipt + MPT proof for a BurnBlocked log carrying the
    /// given amount. Returns `(block_header, receipt, proof_nodes, tx_index, log_index)`.
    fn build_valid_refund(&self, amount: u128) -> (Vec<u8>, Vec<u8>, Vec<Vec<u8>>, u64, u64) {
        let log = self.build_burn_blocked_log(amount);
        self.build_refund_with_logs(&[log])
    }

    /// Builds a single valid BurnBlocked log with the fixture's source chain/owner.
    fn build_burn_blocked_log(&self, amount: u128) -> ReceiptLog {
        let event_data =
            build_burn_blocked_event_data(self.source_chain_b256, &self.source_owner_bcs, amount);
        ReceiptLog {
            address: self.bridge_contract,
            topics: vec![
                burn_blocked_event_signature(),
                uint_topic(1, 8),                                  // height
                uint_topic(0, 4),                                  // event_index
                blocked_by_topic(Address::from([0xCC; 20])),       // blocked_by
            ],
            data: event_data,
        }
    }

    /// Builds a receipt + MPT proof from the given logs.
    fn build_refund_with_logs(
        &self,
        logs: &[ReceiptLog],
    ) -> (Vec<u8>, Vec<u8>, Vec<Vec<u8>>, u64, u64) {
        let receipt = build_test_receipt(logs);
        let tx_index = 1u64;
        let (receipts_root, proof_bytes) =
            build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);
        let proof_nodes: Vec<Vec<u8>> = proof_bytes.into_iter().map(|b| b.to_vec()).collect();
        let block_header = build_test_header(receipts_root, 12345);
        (block_header, receipt, proof_nodes, tx_index, 0)
    }
}

/// Builds an indexed-uint topic by left-padding a big-endian integer to 32 bytes.
fn uint_topic(value: u128, byte_len: usize) -> B256 {
    let mut topic = [0u8; 32];
    let be = value.to_be_bytes();
    topic[32 - byte_len..32].copy_from_slice(&be[16 - byte_len..16]);
    B256::from(topic)
}

/// Builds a `blocked_by` topic (left-padded address in a 32-byte word).
fn blocked_by_topic(addr: Address) -> B256 {
    let mut topic = [0u8; 32];
    topic[12..32].copy_from_slice(addr.as_slice());
    B256::from(topic)
}

#[tokio::test]
async fn refund_burn_success_mints_to_source() {
    let f = RefundFixture::setup().await;
    let (block_header, receipt, proof_nodes, tx_index, log_index) = f.build_valid_refund(7_000_000);

    f.chain
        .add_block(|block| {
            block.with_operation(
                f.bridge_app_id,
                BridgeOperation::RefundBurn {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert_eq!(
        query_balance(f.fungible_app_id, &f.chain, f.source_owner).await,
        Some(U128(7_000_000u128)),
    );
}

#[tokio::test]
async fn refund_burn_replay_rejected() {
    let f = RefundFixture::setup().await;
    let (block_header, receipt, proof_nodes, tx_index, log_index) = f.build_valid_refund(7_000_000);

    f.chain
        .add_block(|block| {
            block.with_operation(
                f.bridge_app_id,
                BridgeOperation::RefundBurn {
                    block_header_rlp: block_header.clone(),
                    receipt_rlp: receipt.clone(),
                    proof_nodes: proof_nodes.clone(),
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    let result = f
        .chain
        .try_add_block(|block| {
            block.with_operation(
                f.bridge_app_id,
                BridgeOperation::RefundBurn {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(result.is_err(), "replay refund should be rejected");
}

#[tokio::test]
async fn refund_burn_wrong_bridge_address() {
    let f = RefundFixture::setup().await;

    // Same valid event data, but emit from the wrong contract address.
    let event_data =
        build_burn_blocked_event_data(f.source_chain_b256, &f.source_owner_bcs, 1_000_000);
    let wrong_emitter = Address::from([0xCC; 20]);
    let log = ReceiptLog {
        address: wrong_emitter,
        topics: vec![
            burn_blocked_event_signature(),
            uint_topic(1, 8),
            uint_topic(0, 4),
            blocked_by_topic(Address::from([0xCC; 20])),
        ],
        data: event_data,
    };
    let (block_header, receipt, proof_nodes, tx_index, log_index) = f.build_refund_with_logs(&[log]);

    let result = f
        .chain
        .try_add_block(|block| {
            block.with_operation(
                f.bridge_app_id,
                BridgeOperation::RefundBurn {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(
        result.is_err(),
        "log from wrong emitter must be rejected by parse_burn_blocked_event"
    );
}

#[tokio::test]
async fn refund_burn_wrong_topic() {
    let f = RefundFixture::setup().await;

    // The proof points at a DepositInitiated log, not a BurnBlocked one — parse must reject.
    let deposit_data = build_deposit_event_data(
        8453,
        f.source_chain_b256,
        B256::ZERO,
        B256::from([0x33; 32]),
        Address::from([0xA0; 20]),
        1_000_000,
        0,
    );
    let depositor = Address::from([0xDD; 20]);
    let mut depositor_topic_bytes = [0u8; 32];
    depositor_topic_bytes[12..32].copy_from_slice(depositor.as_slice());
    let log = ReceiptLog {
        address: f.bridge_contract,
        topics: vec![deposit_event_signature(), B256::from(depositor_topic_bytes)],
        data: deposit_data,
    };
    let (block_header, receipt, proof_nodes, tx_index, log_index) = f.build_refund_with_logs(&[log]);

    let result = f
        .chain
        .try_add_block(|block| {
            block.with_operation(
                f.bridge_app_id,
                BridgeOperation::RefundBurn {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(
        result.is_err(),
        "DepositInitiated topic must be rejected by parse_burn_blocked_event"
    );
}
