// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the EVM bridge app's ProcessDeposit operation.

#![cfg(not(target_arch = "wasm32"))]

use alloy_primitives::{keccak256, Address, Bloom, Bytes, FixedBytes, B256, U256};
use alloy_rlp::Encodable;
use alloy_trie::{proof::ProofRetainer, HashBuilder, Nibbles};
use evm_bridge::{BridgeOperation, BridgeParameters, EvmBridgeAbi};
use fungible::{InitialState, InitialStateBuilder};
use linera_sdk::{
    linera_base_types::{AccountOwner, Amount},
    test::TestValidator,
};
use wrapped_fungible::{WrappedFungibleTokenAbi, WrappedParameters};

// -- proof building helpers (mirrors linera-bridge/src/proof.rs test helpers) --

fn build_test_header(receipts_root: B256) -> Vec<u8> {
    let mut payload = Vec::new();
    B256::ZERO.encode(&mut payload);
    B256::ZERO.encode(&mut payload);
    Address::ZERO.encode(&mut payload);
    B256::ZERO.encode(&mut payload);
    B256::ZERO.encode(&mut payload);
    receipts_root.encode(&mut payload);
    Bloom::ZERO.encode(&mut payload);
    0u64.encode(&mut payload);
    12345u64.encode(&mut payload);
    30_000_000u64.encode(&mut payload);
    21_000u64.encode(&mut payload);
    1_700_000_000u64.encode(&mut payload);
    Bytes::new().encode(&mut payload);
    B256::ZERO.encode(&mut payload);
    FixedBytes::<8>::ZERO.encode(&mut payload);

    let mut out = Vec::new();
    alloy_rlp::Header {
        list: true,
        payload_length: payload.len(),
    }
    .encode(&mut out);
    out.extend_from_slice(&payload);
    out
}

fn build_test_receipt(logs: &[TestLog]) -> Vec<u8> {
    let mut payload = Vec::new();
    1u8.encode(&mut payload);
    21_000u64.encode(&mut payload);
    Bloom::ZERO.encode(&mut payload);

    let mut logs_payload = Vec::new();
    for log in logs {
        encode_log(log, &mut logs_payload);
    }
    alloy_rlp::Header {
        list: true,
        payload_length: logs_payload.len(),
    }
    .encode(&mut payload);
    payload.extend_from_slice(&logs_payload);

    let mut out = Vec::new();
    alloy_rlp::Header {
        list: true,
        payload_length: payload.len(),
    }
    .encode(&mut out);
    out.extend_from_slice(&payload);
    out
}

struct TestLog {
    address: Address,
    topics: Vec<B256>,
    data: Vec<u8>,
}

fn encode_log(log: &TestLog, out: &mut Vec<u8>) {
    let mut payload = Vec::new();
    log.address.encode(&mut payload);

    let mut topics_payload = Vec::new();
    for topic in &log.topics {
        topic.encode(&mut topics_payload);
    }
    alloy_rlp::Header {
        list: true,
        payload_length: topics_payload.len(),
    }
    .encode(&mut payload);
    payload.extend_from_slice(&topics_payload);

    Bytes::copy_from_slice(&log.data).encode(&mut payload);

    alloy_rlp::Header {
        list: true,
        payload_length: payload.len(),
    }
    .encode(out);
    out.extend_from_slice(&payload);
}

fn receipt_trie_key(tx_index: u64) -> Nibbles {
    let mut key_bytes = Vec::new();
    tx_index.encode(&mut key_bytes);
    Nibbles::unpack(&key_bytes)
}

fn build_receipt_trie(receipts: &[(u64, Vec<u8>)], target_tx_index: u64) -> (B256, Vec<Vec<u8>>) {
    let mut entries: Vec<(Nibbles, Vec<u8>)> = receipts
        .iter()
        .map(|(idx, rlp)| {
            let mut key_bytes = Vec::new();
            idx.encode(&mut key_bytes);
            (Nibbles::unpack(&key_bytes), rlp.clone())
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let target_key = receipt_trie_key(target_tx_index);
    let retainer = ProofRetainer::new(vec![target_key]);
    let mut builder = HashBuilder::default().with_proof_retainer(retainer);

    for (key, value) in &entries {
        builder.add_leaf(*key, value);
    }

    let root = builder.root();
    let proof_nodes = builder.take_proof_nodes();
    let proof: Vec<Vec<u8>> = proof_nodes
        .matching_nodes_sorted(&target_key)
        .into_iter()
        .map(|(_, bytes)| bytes.to_vec())
        .collect();

    (root, proof)
}

fn deposit_event_signature() -> B256 {
    keccak256(b"DepositInitiated(uint256,bytes32,bytes32,bytes32,address,uint256)")
}

fn build_deposit_event_data(
    source_chain_id: u64,
    target_chain_id: B256,
    target_application_id: B256,
    target_account_owner: B256,
    token: Address,
    amount: u64,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(192);
    data.extend_from_slice(&U256::from(source_chain_id).to_be_bytes::<32>());
    data.extend_from_slice(target_chain_id.as_slice());
    data.extend_from_slice(target_application_id.as_slice());
    data.extend_from_slice(target_account_owner.as_slice());
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(token.as_slice());
    data.extend_from_slice(&U256::from(amount).to_be_bytes::<32>());
    data
}

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

    let bridge_contract = Address::from(bridge_params_address());
    let token = Address::from(token_address);

    let event_data = build_deposit_event_data(
        source_chain_id,
        target_chain_b256,
        B256::from(app_id_bytes),
        target_owner_b256,
        token,
        deposit_amount,
    );

    let receipt = build_test_receipt(&[TestLog {
        address: bridge_contract,
        topics: vec![deposit_event_signature()],
        data: event_data,
    }]);

    let tx_index = 1u64;
    let (receipts_root, proof_nodes) = build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);
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

fn bridge_params_address() -> [u8; 20] {
    [0xBB; 20]
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

    let receipt = build_test_receipt(&[TestLog {
        address: Address::from([0xBB; 20]),
        topics: vec![deposit_event_signature()],
        data: event_data,
    }]);

    let tx_index = 1u64;
    let (receipts_root, proof_nodes) = build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);
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

    let receipt = build_test_receipt(&[TestLog {
        address: Address::from([0xBB; 20]),
        topics: vec![deposit_event_signature()],
        data: event_data,
    }]);

    let tx_index = 1u64;
    let (_, proof_nodes) = build_receipt_trie(&[(tx_index, receipt.clone())], tx_index);

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
