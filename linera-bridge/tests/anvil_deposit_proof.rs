// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Integration test: generate a deposit proof against a live Anvil node
//! and verify it using the on-chain proof module.
//!
//! Prerequisites: `anvil` (foundry) and `solc` must be installed.
//! Run: `cargo test -p linera-bridge -- --ignored test_deposit_proof`

#![cfg(not(target_arch = "wasm32"))]

use std::{
    fs::File,
    io::Write,
    process::{Command, Stdio},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    node_bindings::Anvil,
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use alloy_primitives::{Bytes, B256, U256};
use alloy_sol_types::{SolCall, SolValue};
use linera_bridge::{
    proof::{
        decode_block_header, decode_receipt_logs, parse_deposit_event, verify_receipt_inclusion,
    },
    proof_gen::{DepositProofClient, HttpDepositProofClient},
    BRIDGE_TYPES_SOURCE, FUNGIBLE_BRIDGE_SOURCE, FUNGIBLE_TYPES_SOURCE,
};

const MOCK_ERC20_SOL: &str = r#"
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract MockERC20 {
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    uint256 public totalSupply;

    constructor(uint256 initialSupply) {
        balanceOf[msg.sender] = initialSupply;
        totalSupply = initialSupply;
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        require(balanceOf[msg.sender] >= amount, "insufficient balance");
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        return true;
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        require(balanceOf[from] >= amount, "insufficient balance");
        require(allowance[from][msg.sender] >= amount, "insufficient allowance");
        allowance[from][msg.sender] -= amount;
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
        return true;
    }
}
"#;

// ABI bindings for contract interactions
alloy_sol_types::sol! {
    function approve(address spender, uint256 amount) external returns (bool);

    function deposit(
        bytes32 target_chain_id,
        bytes32 target_application_id,
        bytes32 target_account_owner,
        uint256 amount
    ) external;
}

/// Compiles a Solidity contract via `solc`, returning deployment bytecode.
fn compile_contract(source_code: &str, file_name: &str, contract_name: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    // Write all shared Solidity files so imports resolve
    for (name, content) in [
        ("BridgeTypes.sol", BRIDGE_TYPES_SOURCE),
        ("FungibleTypes.sol", FUNGIBLE_TYPES_SOURCE),
        ("FungibleBridge.sol", FUNGIBLE_BRIDGE_SOURCE),
        ("LightClient.sol", linera_bridge::light_client::SOURCE),
        ("Microchain.sol", linera_bridge::microchain::SOURCE),
    ] {
        let mut f = File::create(path.join(name)).unwrap();
        writeln!(f, "{}", content).unwrap();
    }

    // Write the contract under test
    let mut test_file = File::create(path.join(file_name)).unwrap();
    writeln!(test_file, "{}", source_code).unwrap();

    // Solc standard JSON config
    let config = serde_json::json!({
        "language": "Solidity",
        "sources": {
            file_name: { "urls": [format!("./{file_name}")] }
        },
        "settings": {
            "viaIR": true,
            "outputSelection": { "*": { "*": ["evm.bytecode"] } }
        }
    });
    let config_path = path.join("config.json");
    std::fs::write(&config_path, config.to_string()).unwrap();

    let config_file = File::open(&config_path).unwrap();
    let output_file = File::create(path.join("result.json")).unwrap();

    let status = Command::new("solc")
        .current_dir(path)
        .arg("--standard-json")
        .stdin(Stdio::from(config_file))
        .stdout(Stdio::from(output_file))
        .status()
        .expect("solc must be installed");
    assert!(status.success(), "solc compilation failed");

    let result: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path.join("result.json")).unwrap()).unwrap();

    if let Some(errors) = result.get("errors") {
        for error in errors.as_array().unwrap() {
            if error["severity"].as_str() == Some("error") {
                panic!(
                    "solc error: {}",
                    error["formattedMessage"].as_str().unwrap_or("unknown")
                );
            }
        }
    }

    let hex_str = result["contracts"][file_name][contract_name]["evm"]["bytecode"]["object"]
        .as_str()
        .expect("bytecode not found in solc output");
    hex::decode(hex_str).unwrap()
}

#[tokio::test]
#[ignore] // Requires `anvil` and `solc`
async fn test_deposit_proof_generation() {
    // 1. Spawn Anvil
    // Use Shanghai hardfork to avoid header field mismatches with older Anvil versions
    // (Anvil v0.2.0 doesn't return all Cancun header fields via JSON-RPC).
    let anvil = Anvil::new()
        .arg("--hardfork")
        .arg("shanghai")
        .try_spawn()
        .expect("anvil must be installed");
    let endpoint = anvil.endpoint();

    // Default Anvil account 0
    let pk: PrivateKeySigner = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        .parse()
        .unwrap();
    let deployer = pk.address();
    let wallet = EthereumWallet::from(pk);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(endpoint.parse().unwrap());

    // 2. Compile and deploy MockERC20
    let erc20_bytecode = compile_contract(MOCK_ERC20_SOL, "MockERC20.sol", "MockERC20");
    let initial_supply = U256::from(1_000_000_000u64);
    let mut erc20_deploy = erc20_bytecode;
    erc20_deploy.extend_from_slice(&(initial_supply,).abi_encode_params());

    let tx = TransactionRequest::default().with_deploy_code(Bytes::from(erc20_deploy));
    let receipt = provider
        .send_transaction(tx)
        .await
        .expect("send erc20 deploy")
        .get_receipt()
        .await
        .expect("erc20 deploy receipt");
    let token_address = receipt.contract_address.expect("erc20 address");

    // 3. Compile and deploy FungibleBridge
    let target_chain_id = B256::from([0xAA; 32]);
    let target_application_id = B256::from([0xBB; 32]);

    let bridge_bytecode = compile_contract(
        FUNGIBLE_BRIDGE_SOURCE,
        "FungibleBridge.sol",
        "FungibleBridge",
    );
    let bridge_constructor = (
        deployer,                                // light_client (unused by deposit)
        <[u8; 32]>::from(target_chain_id),       // chainId
        0u64,                                    // nextExpectedHeight
        <[u8; 32]>::from(target_application_id), // applicationId
        token_address,                           // token
    )
        .abi_encode_params();

    let mut bridge_deploy = bridge_bytecode;
    bridge_deploy.extend_from_slice(&bridge_constructor);

    let tx = TransactionRequest::default().with_deploy_code(Bytes::from(bridge_deploy));
    let receipt = provider
        .send_transaction(tx)
        .await
        .expect("send bridge deploy")
        .get_receipt()
        .await
        .expect("bridge deploy receipt");
    let bridge_address = receipt.contract_address.expect("bridge address");

    // 4. Approve bridge to spend tokens, then deposit
    let deposit_amount = U256::from(1_000_000u64);
    let target_owner = B256::from([0xCC; 32]);

    let approve_data = approveCall {
        spender: bridge_address,
        amount: deposit_amount,
    }
    .abi_encode();
    let tx = TransactionRequest::default()
        .to(token_address)
        .input(approve_data.into());
    provider
        .send_transaction(tx)
        .await
        .expect("send approve")
        .get_receipt()
        .await
        .expect("approve receipt");

    let deposit_data = depositCall {
        target_chain_id,
        target_application_id,
        target_account_owner: target_owner,
        amount: deposit_amount,
    }
    .abi_encode();
    let tx = TransactionRequest::default()
        .to(bridge_address)
        .input(deposit_data.into());
    let deposit_receipt = provider
        .send_transaction(tx)
        .await
        .expect("send deposit")
        .get_receipt()
        .await
        .expect("deposit receipt");
    let deposit_tx_hash = deposit_receipt.transaction_hash;

    // 5. Generate deposit proof using HttpDepositProofClient
    let client = HttpDepositProofClient::new(&endpoint).expect("create proof client");
    let proof = client
        .generate_deposit_proof(deposit_tx_hash)
        .await
        .expect("generate deposit proof");

    // 6. Verify the proof using on-chain verification functions

    // Block header decoding
    let (block_hash, receipts_root) =
        decode_block_header(&proof.block_header_rlp).expect("decode block header");
    assert_ne!(block_hash, B256::ZERO, "block hash should be non-zero");

    // Receipt inclusion proof
    let proof_bytes: Vec<Bytes> = proof
        .proof_nodes
        .iter()
        .map(|n| Bytes::copy_from_slice(n))
        .collect();
    verify_receipt_inclusion(
        receipts_root,
        proof.tx_index,
        &proof.receipt_rlp,
        &proof_bytes,
    )
    .expect("receipt inclusion proof should verify");

    // Decode and parse the deposit event
    let logs = decode_receipt_logs(&proof.receipt_rlp).expect("decode receipt logs");
    let deposit_log = &logs[proof.log_index as usize];
    let deposit = parse_deposit_event(deposit_log).expect("parse deposit event");

    // Anvil's default chain ID is 31337
    assert_eq!(deposit.source_chain_id, U256::from(31337u64));
    assert_eq!(deposit.target_chain_id, target_chain_id);
    assert_eq!(deposit.target_application_id, target_application_id);
    assert_eq!(deposit.target_account_owner, target_owner);
    assert_eq!(deposit.token, token_address);
    assert_eq!(deposit.amount, deposit_amount);

    // Verify the bridge contract address in the log matches
    assert_eq!(
        deposit_log.address, bridge_address,
        "event should come from bridge contract"
    );
}
