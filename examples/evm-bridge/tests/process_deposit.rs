// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the EVM bridge app's ProcessDeposit operation.

#![cfg(not(target_arch = "wasm32"))]

use alloy_primitives::{Address, B256, U256};
use evm_bridge::{BridgeOperation, BridgeParameters, EvmBridgeAbi};
use fungible::{InitialState, InitialStateBuilder};
use linera_bridge::proof::{
    deposit_event_signature,
    testing::{
        build_deposit_event_data, build_receipt_trie, build_test_header, build_test_receipt,
    },
    ReceiptLog,
};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount, ApplicationId},
    test::{ActiveChain, TestValidator},
};
use serde::Deserialize;
use wrapped_fungible::{WrappedFungibleTokenAbi, WrappedParameters};

/// Helper to query an account balance on the wrapped-fungible app.
async fn query_balance(
    app_id: ApplicationId<WrappedFungibleTokenAbi>,
    chain: &ActiveChain,
    owner: AccountOwner,
) -> Option<Amount> {
    use async_graphql::InputType;
    use linera_sdk::test::QueryOutcome;

    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(app_id, query).await;
    let balance = response["accounts"]["entry"]["value"].as_str()?;
    Some(
        balance
            .parse()
            .expect("balance cannot be parsed as a number"),
    )
}

/// Common setup for bridge integration tests.
struct TestBridge {
    chain: ActiveChain,
    chain_owner: AccountOwner,
    bridge_app_id: ApplicationId<EvmBridgeAbi>,
    fungible_app_id: ApplicationId<WrappedFungibleTokenAbi>,
    source_chain_id: u64,
    bridge_contract: Address,
    token: Address,
    target_chain_b256: B256,
    target_owner_b256: B256,
}

impl TestBridge {
    async fn setup() -> Self {
        let (validator, bridge_module_id) =
            TestValidator::with_current_module::<EvmBridgeAbi, BridgeParameters, ()>().await;
        let mut chain = validator.new_chain().await;
        let chain_owner = AccountOwner::from(chain.public_key());

        let token_address = [0xA0; 20];
        let source_chain_id = 8453u64;

        // 1. Deploy bridge first
        let bridge_params = BridgeParameters {
            source_chain_id,
            bridge_contract_address: [0xBB; 20],
            token_address,
            rpc_endpoint: String::new(),
        };
        let bridge_app_id = chain
            .create_application(bridge_module_id, bridge_params, (), vec![])
            .await;

        // 2. Deploy wrapped-fungible with the bridge's app ID
        let fungible_module_id = chain
            .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
                "../wrapped-fungible",
            )
            .await;
        let wrapped_params = WrappedParameters {
            ticker_symbol: "wUSDC".to_string(),
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

        // 3. Register the fungible app in the bridge
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

        let chain_id_bytes: [u8; 32] = chain.id().0.into();
        let target_chain_b256 = B256::from(chain_id_bytes);

        let owner_hash = match chain_owner {
            AccountOwner::Address32(hash) => <[u8; 32]>::from(hash),
            _ => panic!("expected Address32"),
        };
        let target_owner_b256 = B256::from(owner_hash);

        let bridge_contract = Address::from([0xBB; 20]);
        let token = Address::from(token_address);

        TestBridge {
            chain,
            chain_owner,
            bridge_app_id,
            fungible_app_id,
            source_chain_id,
            bridge_contract,
            token,
            target_chain_b256,
            target_owner_b256,
        }
    }

    /// Builds a valid deposit receipt and MPT proof.
    ///
    /// Returns `(block_header, receipt, proof_nodes, tx_index, log_index)`.
    fn build_valid_deposit(&self) -> (Vec<u8>, Vec<u8>, Vec<Vec<u8>>, u64, u64) {
        self.build_deposit_with_logs(&[self.build_valid_log(0)])
    }

    /// Builds a valid `ReceiptLog` for a deposit event with the given nonce.
    fn build_valid_log(&self, nonce: u64) -> ReceiptLog {
        let event_data = build_deposit_event_data(
            self.source_chain_id,
            self.target_chain_b256,
            B256::ZERO, // target_application_id placeholder
            self.target_owner_b256,
            self.token,
            1_000_000,
            nonce,
        );
        let depositor = Address::from([0xDD; 20]);
        let mut depositor_topic = [0u8; 32];
        depositor_topic[12..32].copy_from_slice(depositor.as_slice());

        ReceiptLog {
            address: self.bridge_contract,
            topics: vec![deposit_event_signature(), B256::from(depositor_topic)],
            data: event_data,
        }
    }

    /// Builds a deposit receipt and proof from the given logs.
    ///
    /// Returns `(block_header, receipt, proof_nodes, tx_index, log_index)`.
    /// `log_index` defaults to 0.
    fn build_deposit_with_logs(
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

// -- integration tests --

#[tokio::test]
async fn test_process_deposit() {
    let tb = TestBridge::setup().await;
    let (block_header, receipt, proof_nodes, tx_index, log_index) = tb.build_valid_deposit();

    // Submit ProcessDeposit
    tb.chain
        .add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header.clone(),
                    receipt_rlp: receipt.clone(),
                    proof_nodes: proof_nodes.clone(),
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    // Verify tokens were minted
    assert_eq!(
        query_balance(tb.fungible_app_id, &tb.chain, tb.chain_owner).await,
        Some(Amount::from_attos(1_000_000u128)),
    );

    // Second deposit with same proof should fail (replay)
    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt.clone(),
                    proof_nodes: proof_nodes.clone(),
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(result.is_err(), "replay deposit should be rejected");

    // Use a different (wrong) receipts root in the block header
    let wrong_header = build_test_header(B256::from([0xFF; 32]), 12345);

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp: wrong_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(result.is_err(), "invalid proof should be rejected");
}

#[tokio::test]
async fn test_source_chain_id_mismatch() {
    let tb = TestBridge::setup().await;

    // Build event data with wrong source_chain_id (1 instead of 8453)
    let event_data = build_deposit_event_data(
        1, // wrong source chain ID
        tb.target_chain_b256,
        B256::ZERO,
        tb.target_owner_b256,
        tb.token,
        1_000_000,
        0,
    );
    let depositor = Address::from([0xDD; 20]);
    let mut depositor_topic = [0u8; 32];
    depositor_topic[12..32].copy_from_slice(depositor.as_slice());

    let log = ReceiptLog {
        address: tb.bridge_contract,
        topics: vec![deposit_event_signature(), B256::from(depositor_topic)],
        data: event_data,
    };
    let (block_header, receipt, proof_nodes, tx_index, log_index) =
        tb.build_deposit_with_logs(&[log]);

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
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
        "source chain ID mismatch should be rejected"
    );
}

#[tokio::test]
async fn test_token_address_mismatch() {
    let tb = TestBridge::setup().await;

    // Build event data with wrong token address
    let wrong_token = Address::from([0xCC; 20]);
    let event_data = build_deposit_event_data(
        tb.source_chain_id,
        tb.target_chain_b256,
        B256::ZERO,
        tb.target_owner_b256,
        wrong_token, // wrong token
        1_000_000,
        0,
    );
    let depositor = Address::from([0xDD; 20]);
    let mut depositor_topic = [0u8; 32];
    depositor_topic[12..32].copy_from_slice(depositor.as_slice());

    let log = ReceiptLog {
        address: tb.bridge_contract,
        topics: vec![deposit_event_signature(), B256::from(depositor_topic)],
        data: event_data,
    };
    let (block_header, receipt, proof_nodes, tx_index, log_index) =
        tb.build_deposit_with_logs(&[log]);

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(result.is_err(), "token address mismatch should be rejected");
}

#[tokio::test]
async fn test_wrong_emitter_address() {
    let tb = TestBridge::setup().await;

    // Build valid event data but emit from the wrong contract address
    let event_data = build_deposit_event_data(
        tb.source_chain_id,
        tb.target_chain_b256,
        B256::ZERO,
        tb.target_owner_b256,
        tb.token,
        1_000_000,
        0,
    );
    let depositor = Address::from([0xDD; 20]);
    let mut depositor_topic = [0u8; 32];
    depositor_topic[12..32].copy_from_slice(depositor.as_slice());

    let wrong_emitter = Address::from([0xCC; 20]);
    let log = ReceiptLog {
        address: wrong_emitter, // wrong emitter
        topics: vec![deposit_event_signature(), B256::from(depositor_topic)],
        data: event_data,
    };
    let (block_header, receipt, proof_nodes, tx_index, log_index) =
        tb.build_deposit_with_logs(&[log]);

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(result.is_err(), "wrong emitter address should be rejected");
}

#[tokio::test]
async fn test_wrong_event_signature() {
    let tb = TestBridge::setup().await;

    // Build valid event data but with a garbage event signature
    let event_data = build_deposit_event_data(
        tb.source_chain_id,
        tb.target_chain_b256,
        B256::ZERO,
        tb.target_owner_b256,
        tb.token,
        1_000_000,
        0,
    );
    let depositor = Address::from([0xDD; 20]);
    let mut depositor_topic = [0u8; 32];
    depositor_topic[12..32].copy_from_slice(depositor.as_slice());

    let log = ReceiptLog {
        address: tb.bridge_contract,
        topics: vec![
            B256::from([0xFF; 32]), // garbage event signature
            B256::from(depositor_topic),
        ],
        data: event_data,
    };
    let (block_header_rlp, receipt_rlp, proof_nodes, tx_index, log_index) =
        tb.build_deposit_with_logs(&[log]);

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp,
                    receipt_rlp,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(result.is_err(), "wrong event signature should be rejected");
}

#[tokio::test]
async fn test_log_index_out_of_range() {
    let tb = TestBridge::setup().await;

    // Build receipt with 1 log, but submit with log_index: 1 (out of range)
    let (block_header_rlp, receipt_rlp, proof_nodes, tx_index, _) = tb.build_valid_deposit();

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp,
                    receipt_rlp,
                    proof_nodes,
                    tx_index,
                    log_index: 1, // out of range
                },
            );
        })
        .await;

    assert!(result.is_err(), "log_index out of range should be rejected");
}

#[tokio::test]
async fn test_deposit_amount_exceeds_u128() {
    let tb = TestBridge::setup().await;

    // Manually construct 224-byte event data with U256::MAX in the amount slot
    let mut event_data = Vec::with_capacity(224);
    event_data.extend_from_slice(&U256::from(tb.source_chain_id).to_be_bytes::<32>());
    event_data.extend_from_slice(tb.target_chain_b256.as_slice());
    event_data.extend_from_slice(B256::ZERO.as_slice()); // target_application_id
    event_data.extend_from_slice(tb.target_owner_b256.as_slice());
    event_data.extend_from_slice(&[0u8; 12]); // address padding
    event_data.extend_from_slice(tb.token.as_slice());
    event_data.extend_from_slice(&U256::MAX.to_be_bytes::<32>()); // amount overflows u128
    event_data.extend_from_slice(&U256::from(0u64).to_be_bytes::<32>()); // nonce

    let depositor = Address::from([0xDD; 20]);
    let mut depositor_topic = [0u8; 32];
    depositor_topic[12..32].copy_from_slice(depositor.as_slice());

    let log = ReceiptLog {
        address: tb.bridge_contract,
        topics: vec![deposit_event_signature(), B256::from(depositor_topic)],
        data: event_data,
    };
    let (block_header_rlp, receipt_rlp, proof_nodes, tx_index, log_index) =
        tb.build_deposit_with_logs(&[log]);

    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp,
                    receipt_rlp,
                    proof_nodes,
                    tx_index,
                    log_index,
                },
            );
        })
        .await;

    assert!(
        result.is_err(),
        "deposit amount exceeding u128 should be rejected"
    );
}

#[tokio::test]
async fn test_replay_different_log_index_succeeds() {
    let tb = TestBridge::setup().await;

    // Build receipt with two valid deposit logs (different nonces)
    let log0 = tb.build_valid_log(0);
    let log1 = tb.build_valid_log(1);
    let (block_header_rlp, receipt_rlp, proof_nodes, tx_index, _) =
        tb.build_deposit_with_logs(&[log0, log1]);

    // Process log_index: 0
    tb.chain
        .add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header_rlp.clone(),
                    receipt_rlp: receipt_rlp.clone(),
                    proof_nodes: proof_nodes.clone(),
                    tx_index,
                    log_index: 0,
                },
            );
        })
        .await;

    assert_eq!(
        query_balance(tb.fungible_app_id, &tb.chain, tb.chain_owner).await,
        Some(Amount::from_attos(1_000_000u128)),
    );

    // Process log_index: 1 — should also succeed (different DepositKey)
    tb.chain
        .add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                &BridgeOperation::ProcessDeposit {
                    block_header_rlp,
                    receipt_rlp,
                    proof_nodes,
                    tx_index,
                    log_index: 1,
                },
            );
        })
        .await;

    // Both deposits should have been minted
    assert_eq!(
        query_balance(tb.fungible_app_id, &tb.chain, tb.chain_owner).await,
        Some(Amount::from_attos(2_000_000u128)),
    );
}

// -- finality verification tests --

/// When `rpc_endpoint` is set but the RPC endpoint is unreachable,
/// instantiation should fail because the chain ID check cannot succeed.
#[tokio::test]
async fn test_instantiation_fails_with_unreachable_endpoint() {
    let (validator, bridge_module_id) =
        TestValidator::with_current_module::<EvmBridgeAbi, BridgeParameters, ()>().await;
    let mut chain = validator.new_chain().await;

    let token_address = [0xA0; 20];
    let source_chain_id = 8453u64;

    // Non-empty endpoint that is unreachable → instantiation should fail
    let bridge_params = BridgeParameters {
        source_chain_id,
        bridge_contract_address: [0xBB; 20],
        token_address,
        rpc_endpoint: "http://localhost:8545".to_string(),
    };
    let result = chain
        .try_create_application(bridge_module_id, bridge_params, (), vec![])
        .await;

    assert!(
        result.is_err(),
        "instantiation should fail with unreachable endpoint"
    );
}

// -- Anvil-based finality verification tests --
// These require `anvil` (from Foundry) to be installed.

/// Minimal block response for extracting hash from an Anvil RPC call.
#[derive(Deserialize)]
struct EthBlock {
    hash: B256,
}

/// Queries Anvil for the latest block hash (outside the contract, for test setup).
async fn get_anvil_block_hash(endpoint: &str) -> B256 {
    use linera_ethereum::client::JsonRpcClient;
    let rpc = linera_ethereum::provider::EthereumClientSimplified::new(endpoint.to_string());
    let block: EthBlock = rpc
        .request("eth_getBlockByNumber", ("latest", false))
        .await
        .expect("failed to query Anvil for latest block");
    block.hash
}

/// Sets up a bridge instance with Anvil as the EVM endpoint and the TestValidator
/// configured to allow HTTP requests to Anvil's host.
async fn setup_bridge_with_anvil(
    anvil_endpoint: &str,
) -> (
    ActiveChain,
    AccountOwner,
    ApplicationId<EvmBridgeAbi>,
    ApplicationId<WrappedFungibleTokenAbi>,
) {
    let (mut validator, bridge_module_id) =
        TestValidator::with_current_module::<EvmBridgeAbi, BridgeParameters, ()>().await;

    // Allow the contract to make HTTP requests to the Anvil host.
    validator
        .change_resource_control_policy(|policy| {
            policy
                .http_request_allow_list
                .insert("localhost".to_owned());
        })
        .await;

    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    let token_address = [0xA0; 20];
    // Anvil's default chain ID is 31337.
    let source_chain_id = 31337u64;

    // 1. Deploy bridge first
    let bridge_params = BridgeParameters {
        source_chain_id,
        bridge_contract_address: [0xBB; 20],
        token_address,
        rpc_endpoint: anvil_endpoint.to_string(),
    };
    let bridge_app_id = chain
        .create_application(bridge_module_id, bridge_params, (), vec![])
        .await;

    // 2. Deploy wrapped-fungible with bridge's app ID
    let fungible_module_id = chain
        .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
            "../wrapped-fungible",
        )
        .await;
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
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

    // 3. Register fungible app in bridge
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

    (chain, chain_owner, bridge_app_id, fungible_app_id)
}

/// VerifyBlockHash with a real finalized Anvil block hash should succeed.
#[tokio::test]
#[ignore] // requires `anvil` from Foundry
async fn test_verify_block_hash_anvil() {
    let anvil = linera_ethereum::test_utils::get_anvil()
        .await
        .expect("failed to start anvil");

    let block_hash = get_anvil_block_hash(&anvil.endpoint).await;

    let (chain, _chain_owner, bridge_app_id, _fungible_app_id) =
        setup_bridge_with_anvil(&anvil.endpoint).await;

    // VerifyBlockHash should succeed — Anvil treats all blocks as finalized.
    chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                &BridgeOperation::VerifyBlockHash {
                    block_hash: block_hash.0,
                },
            );
        })
        .await;
}

/// VerifyBlockHash with a non-existent block hash should fail.
#[tokio::test]
#[ignore] // requires `anvil` from Foundry
async fn test_verify_block_hash_not_found() {
    let anvil = linera_ethereum::test_utils::get_anvil()
        .await
        .expect("failed to start anvil");

    let (chain, _chain_owner, bridge_app_id, _fungible_app_id) =
        setup_bridge_with_anvil(&anvil.endpoint).await;

    // VerifyBlockHash with a fake hash — Anvil will return null.
    let fake_hash = [0xDE; 32];
    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                bridge_app_id,
                &BridgeOperation::VerifyBlockHash {
                    block_hash: fake_hash,
                },
            );
        })
        .await;

    assert!(
        result.is_err(),
        "VerifyBlockHash with non-existent hash should fail"
    );
}

#[tokio::test]
async fn test_register_fungible_app_cannot_be_called_twice() {
    let tb = TestBridge::setup().await;

    // The bridge already has a registered fungible app from setup.
    // Attempting to register again should fail.
    let dummy_app_id = ApplicationId::new(linera_sdk::linera_base_types::CryptoHash::new(
        &linera_sdk::linera_base_types::TestString::new("other_app"),
    ));
    let result = tb
        .chain
        .try_add_block(|block| {
            block.with_operation(
                tb.bridge_app_id,
                BridgeOperation::RegisterFungibleApp {
                    app_id: dummy_app_id,
                },
            );
        })
        .await;
    assert!(
        result.is_err(),
        "registering fungible app a second time should be rejected"
    );
}
