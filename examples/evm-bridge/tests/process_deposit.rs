// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the EVM bridge app's ProcessDeposit operation.

#![cfg(not(target_arch = "wasm32"))]

use alloy_primitives::{Address, B256};
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
    linera_base_types::{AccountOwner, Amount},
    test::TestValidator,
};
use wrapped_fungible::{WrappedFungibleTokenAbi, WrappedParameters};

/// Helper to query an account balance on the wrapped-fungible app.
async fn query_balance(
    app_id: linera_sdk::linera_base_types::ApplicationId<WrappedFungibleTokenAbi>,
    chain: &linera_sdk::test::ActiveChain,
    owner: AccountOwner,
) -> Option<Amount> {
    use async_graphql::InputType;
    use linera_sdk::test::QueryOutcome;

    let query = format!(
        "query {{ accounts {{ entry(key: {}) {{ value }} }} }}",
        owner.to_value()
    );
    let QueryOutcome { response, .. } = chain.graphql_query(app_id, query).await;
    let balance = response.pointer("/accounts/entry/value")?.as_str()?;
    Some(
        balance
            .parse()
            .expect("balance cannot be parsed as a number"),
    )
}

// -- integration tests --

#[tokio::test]
async fn test_process_deposit_mints_tokens() {
    let (validator, bridge_module_id) =
        TestValidator::with_current_module::<EvmBridgeAbi, BridgeParameters, ()>().await;
    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    // Deploy wrapped-fungible app with chain owner as minter
    let fungible_module_id = chain
        .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
            "../wrapped-fungible",
        )
        .await;

    let token_address = [0xA0; 20];
    let source_chain_id = 8453u64;

    let wrapped_params = WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        minter: chain_owner,
        evm_token_address: token_address,
        evm_source_chain_id: source_chain_id,
    };
    let fungible_app_id = chain
        .create_application(
            fungible_module_id,
            wrapped_params,
            InitialStateBuilder::default().build(),
            vec![],
        )
        .await;

    // Deploy bridge app
    let bridge_params = BridgeParameters {
        source_chain_id,
        bridge_contract_address: [0xBB; 20],
        fungible_app_id: fungible_app_id.forget_abi(),
        token_address,
    };
    let bridge_app_id = chain
        .create_application(bridge_module_id, bridge_params, (), vec![])
        .await;

    // Build a synthetic deposit event and proof
    let deposit_amount = 1_000_000u64;

    // ChainId(CryptoHash) → [u8; 32] via CryptoHash's From impl
    let chain_id_bytes: [u8; 32] = chain.id().0.into();
    let target_chain_b256 = B256::from(chain_id_bytes);

    // The application id as bytes32 — use the forget_abi hash
    let app_id_bytes: [u8; 32] = [0; 32]; // placeholder, bridge doesn't check this yet

    // The target account owner as bytes32 (Address32 = CryptoHash)
    let owner_hash = match chain_owner {
        AccountOwner::Address32(hash) => <[u8; 32]>::from(hash),
        _ => panic!("expected Address32"),
    };
    let target_owner_b256 = B256::from(owner_hash);

    let bridge_contract = Address::from([0xBB; 20]);
    let token = Address::from(token_address);

    let event_data = build_deposit_event_data(
        source_chain_id,
        target_chain_b256,
        B256::from(app_id_bytes),
        target_owner_b256,
        token,
        deposit_amount,
    );

    let receipt = build_test_receipt(&[ReceiptLog {
        address: bridge_contract,
        topics: vec![deposit_event_signature()],
        data: event_data,
    }]);

    let tx_index = 1u64;
    let (receipts_root, proof_bytes) = build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);
    let proof_nodes: Vec<Vec<u8>> = proof_bytes.into_iter().map(|b| b.to_vec()).collect();
    let block_header = build_test_header(receipts_root);

    // Submit ProcessDeposit
    chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index: 0,
                },
            );
        })
        .await;

    // Verify tokens were minted
    assert_eq!(
        query_balance(fungible_app_id, &chain, chain_owner).await,
        Some(Amount::from_attos(deposit_amount as u128)),
    );
}

#[tokio::test]
async fn test_replay_protection() {
    let (validator, bridge_module_id) =
        TestValidator::with_current_module::<EvmBridgeAbi, BridgeParameters, ()>().await;
    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    let fungible_module_id = chain
        .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
            "../wrapped-fungible",
        )
        .await;

    let token_address = [0xA0; 20];
    let source_chain_id = 8453u64;

    let wrapped_params = WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        minter: chain_owner,
        evm_token_address: token_address,
        evm_source_chain_id: source_chain_id,
    };
    let fungible_app_id = chain
        .create_application(
            fungible_module_id,
            wrapped_params,
            InitialStateBuilder::default().build(),
            vec![],
        )
        .await;

    let bridge_params = BridgeParameters {
        source_chain_id,
        bridge_contract_address: [0xBB; 20],
        fungible_app_id: fungible_app_id.forget_abi(),
        token_address,
    };
    let bridge_app_id = chain
        .create_application(bridge_module_id, bridge_params, (), vec![])
        .await;

    // Build deposit
    let chain_id_bytes: [u8; 32] = chain.id().0.into();
    let owner_hash = match chain_owner {
        AccountOwner::Address32(hash) => <[u8; 32]>::from(hash),
        _ => panic!("expected Address32"),
    };

    let event_data = build_deposit_event_data(
        source_chain_id,
        B256::from(chain_id_bytes),
        B256::ZERO,
        B256::from(owner_hash),
        Address::from(token_address),
        500_000,
    );

    let receipt = build_test_receipt(&[ReceiptLog {
        address: Address::from([0xBB; 20]),
        topics: vec![deposit_event_signature()],
        data: event_data,
    }]);

    let tx_index = 1u64;
    let (receipts_root, proof_bytes) = build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);
    let proof_nodes: Vec<Vec<u8>> = proof_bytes.into_iter().map(|b| b.to_vec()).collect();
    let block_header = build_test_header(receipts_root);

    // First deposit should succeed
    chain
        .add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header.clone(),
                    receipt_rlp: receipt.clone(),
                    proof_nodes: proof_nodes.clone(),
                    tx_index,
                    log_index: 0,
                },
            );
        })
        .await;

    // Second deposit with same proof should fail (replay)
    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::ProcessDeposit {
                    block_header_rlp: block_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index: 0,
                },
            );
        })
        .await;

    assert!(result.is_err(), "replay deposit should be rejected");
}

#[tokio::test]
async fn test_invalid_proof_rejected() {
    let (validator, bridge_module_id) =
        TestValidator::with_current_module::<EvmBridgeAbi, BridgeParameters, ()>().await;
    let mut chain = validator.new_chain().await;
    let chain_owner = AccountOwner::from(chain.public_key());

    let fungible_module_id = chain
        .publish_bytecode_files_in::<WrappedFungibleTokenAbi, WrappedParameters, InitialState>(
            "../wrapped-fungible",
        )
        .await;

    let token_address = [0xA0; 20];
    let wrapped_params = WrappedParameters {
        ticker_symbol: "wUSDC".to_string(),
        minter: chain_owner,
        evm_token_address: token_address,
        evm_source_chain_id: 8453,
    };
    let fungible_app_id = chain
        .create_application(
            fungible_module_id,
            wrapped_params,
            InitialStateBuilder::default().build(),
            vec![],
        )
        .await;

    let bridge_params = BridgeParameters {
        source_chain_id: 8453,
        bridge_contract_address: [0xBB; 20],
        fungible_app_id: fungible_app_id.forget_abi(),
        token_address,
    };
    let bridge_app_id = chain
        .create_application(bridge_module_id, bridge_params, (), vec![])
        .await;

    // Build receipt and proof, but use a wrong receipts root in the header
    let chain_id_bytes: [u8; 32] = chain.id().0.into();
    let owner_hash = match chain_owner {
        AccountOwner::Address32(hash) => <[u8; 32]>::from(hash),
        _ => panic!("expected Address32"),
    };

    let event_data = build_deposit_event_data(
        8453,
        B256::from(chain_id_bytes),
        B256::ZERO,
        B256::from(owner_hash),
        Address::from(token_address),
        100,
    );

    let receipt = build_test_receipt(&[ReceiptLog {
        address: Address::from([0xBB; 20]),
        topics: vec![deposit_event_signature()],
        data: event_data,
    }]);

    let tx_index = 1u64;
    let (_, proof_bytes) = build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);
    let proof_nodes: Vec<Vec<u8>> = proof_bytes.into_iter().map(|b| b.to_vec()).collect();

    // Use a different (wrong) receipts root in the block header
    let wrong_header = build_test_header(B256::from([0xFF; 32]));

    let result = chain
        .try_add_block(|block| {
            block.with_operation(
                bridge_app_id,
                BridgeOperation::ProcessDeposit {
                    block_header_rlp: wrong_header,
                    receipt_rlp: receipt,
                    proof_nodes,
                    tx_index,
                    log_index: 0,
                },
            );
        })
        .await;

    assert!(result.is_err(), "invalid proof should be rejected");
}
