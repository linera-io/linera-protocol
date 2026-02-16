// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use alloy_sol_types::{sol, SolCall};
use linera_base::{
    crypto::{CryptoHash, TestString},
    data_types::{BlockHeight, Epoch, Round, Timestamp},
    identifiers::ChainId,
};
use linera_chain::{
    block::{Block, BlockBody, BlockHeader, ConfirmedBlock},
    types::ConfirmedBlockCertificate,
};
use revm::{
    database::{CacheDB, EmptyDB},
    primitives::{Address, Bytes, TxKind, U256},
    Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
};
use revm_context::result::{ExecutionResult, Output};

const BRIDGE_TYPES_SOL: &str = include_str!("../src/BridgeTypes.sol");
const GAS_LIMIT: u64 = 500_000_000;

sol! {
    function deserialize(bytes calldata data)
        external
        pure
        returns (bytes32 chainId, uint32 epoch, uint64 height);
}

#[test]
fn test_deserialize_confirmed_block_certificate() {
    let expected_chain_id = CryptoHash::new(&TestString::new("test_chain"));
    let expected_epoch = Epoch::ZERO;
    let expected_height = BlockHeight(1);
    let block = create_test_block(expected_chain_id, expected_epoch, expected_height);
    let confirmed = ConfirmedBlock::new(block);
    let certificate = ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![]);
    let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

    let test_source =
        std::fs::read_to_string("tests/solidity/BridgeTest.sol").expect("BridgeTest.sol not found");
    let bytecode = compile_bridge_test(&test_source);

    let deployer = Address::ZERO;
    let mut db = CacheDB::<EmptyDB>::default();
    let contract = deploy_contract(&mut db, deployer, bytecode);

    let calldata = deserializeCall {
        data: bcs_bytes.into(),
    }
    .abi_encode();
    let output = call_contract(&mut db, deployer, contract, calldata);

    let decoded =
        deserializeCall::abi_decode_returns(&output).expect("failed to ABI-decode return values");
    assert_eq!(
        decoded.chainId.0,
        *expected_chain_id.as_bytes(),
        "chain_id mismatch"
    );
    assert_eq!(decoded.epoch, expected_epoch.0, "epoch mismatch");
    assert_eq!(decoded.height, expected_height.0, "block height mismatch");
}

/// Deploys a compiled contract and returns its address.
fn deploy_contract(db: &mut CacheDB<EmptyDB>, deployer: Address, bytecode: Vec<u8>) -> Address {
    let result = Context::mainnet()
        .with_db(db)
        .modify_cfg_chained(|cfg| {
            // BridgeTypes.sol exceeds the default EIP-170 contract size limit.
            cfg.limit_contract_code_size = Some(usize::MAX);
        })
        .modify_tx_chained(|tx| {
            tx.caller = deployer;
            tx.kind = TxKind::Create;
            tx.data = Bytes::from(bytecode);
            tx.gas_limit = GAS_LIMIT;
            tx.value = U256::ZERO;
        })
        .build_mainnet()
        .replay_commit()
        .expect("deployment transaction failed");

    match result {
        ExecutionResult::Success { output, .. } => match output {
            Output::Create(_, Some(addr)) => addr,
            other => panic!("expected Create output with address, got: {:?}", other),
        },
        ExecutionResult::Revert { output, .. } => {
            panic!("deployment reverted: {}", hex::encode(&output));
        }
        ExecutionResult::Halt { reason, .. } => {
            panic!("deployment halted: {:?}", reason);
        }
    }
}

/// Calls a deployed contract and returns the raw output bytes.
fn call_contract(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    contract: Address,
    calldata: Vec<u8>,
) -> Bytes {
    let result = Context::mainnet()
        .with_db(db)
        .modify_tx_chained(|tx| {
            tx.caller = deployer;
            tx.nonce = 1;
            tx.kind = TxKind::Call(contract);
            tx.data = Bytes::from(calldata);
            tx.gas_limit = GAS_LIMIT;
            tx.value = U256::ZERO;
        })
        .build_mainnet()
        .replay()
        .expect("call transaction failed");

    match result.result {
        ExecutionResult::Success { output, .. } => match output {
            Output::Call(bytes) => bytes,
            other => panic!("expected Call output, got: {:?}", other),
        },
        ExecutionResult::Revert { output, .. } => {
            panic!("call reverted: {}", hex::encode(&output));
        }
        ExecutionResult::Halt { reason, .. } => {
            panic!("call halted: {:?}", reason);
        }
    }
}

fn create_test_block(chain_id: CryptoHash, epoch: Epoch, height: BlockHeight) -> Block {
    Block {
        header: BlockHeader {
            chain_id: ChainId(chain_id),
            epoch,
            height,
            timestamp: Timestamp::from(0),
            state_hash: CryptoHash::new(&TestString::new("state")),
            previous_block_hash: None,
            authenticated_signer: None,
            transactions_hash: CryptoHash::new(&TestString::new("tx")),
            messages_hash: CryptoHash::new(&TestString::new("msg")),
            previous_message_blocks_hash: CryptoHash::new(&TestString::new("prev_msg")),
            previous_event_blocks_hash: CryptoHash::new(&TestString::new("prev_evt")),
            oracle_responses_hash: CryptoHash::new(&TestString::new("oracle")),
            events_hash: CryptoHash::new(&TestString::new("events")),
            blobs_hash: CryptoHash::new(&TestString::new("blobs")),
            operation_results_hash: CryptoHash::new(&TestString::new("op_results")),
        },
        body: BlockBody {
            transactions: vec![],
            messages: vec![],
            previous_message_blocks: Default::default(),
            previous_event_blocks: Default::default(),
            oracle_responses: vec![],
            events: vec![],
            blobs: vec![],
            operation_results: vec![],
        },
    }
}

fn compile_bridge_test(source_code: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    // Write BridgeTypes.sol
    let types_path = path.join("BridgeTypes.sol");
    let mut types_file = File::create(&types_path).unwrap();
    writeln!(types_file, "{}", BRIDGE_TYPES_SOL).unwrap();

    // Write the test contract
    let file_name = "BridgeTest.sol";
    let test_path = path.join(file_name);
    let mut test_file = File::create(&test_path).unwrap();
    writeln!(test_file, "{}", source_code).unwrap();

    // Write solc config
    write_compilation_json(path, file_name);

    // Compile
    let config_file = File::open(path.join("config.json")).unwrap();
    let output_file = File::create(path.join("result.json")).unwrap();

    let status = Command::new("solc")
        .current_dir(path)
        .arg("--standard-json")
        .stdin(Stdio::from(config_file))
        .stdout(Stdio::from(output_file))
        .status()
        .expect("solc must be installed");
    assert!(status.success(), "solc compilation failed");

    let contents = std::fs::read_to_string(path.join("result.json")).unwrap();
    let json_data: serde_json::Value = serde_json::from_str(&contents).unwrap();

    // Check for compilation errors
    if let Some(errors) = json_data.get("errors") {
        for error in errors.as_array().unwrap() {
            let severity = error["severity"].as_str().unwrap_or("");
            if severity == "error" {
                panic!(
                    "solc compilation error: {}",
                    error["formattedMessage"].as_str().unwrap_or("unknown")
                );
            }
        }
    }

    let bytecode_hex = json_data["contracts"][file_name]["BridgeTest"]["evm"]["bytecode"]["object"]
        .as_str()
        .expect("failed to extract bytecode from solc output");
    hex::decode(bytecode_hex).unwrap()
}

fn write_compilation_json(path: &Path, file_name: &str) {
    let config_path = path.join("config.json");
    let mut source = File::create(config_path).unwrap();
    writeln!(
        source,
        r#"
{{
  "language": "Solidity",
  "sources": {{
    "{file_name}": {{
      "urls": ["./{file_name}"]
    }}
  }},
  "settings": {{
    "viaIR": true,
    "outputSelection": {{
      "*": {{
        "*": ["evm.bytecode"]
      }}
    }}
  }}
}}
"#
    )
    .unwrap();
}
