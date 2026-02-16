// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use alloy_primitives::keccak256;
use alloy_sol_types::{sol, SolValue};
use linera_base::{
    crypto::{CryptoHash, TestString, ValidatorPublicKey, ValidatorSecretKey},
    data_types::{BlockHeight, Epoch, Round, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_chain::{
    block::{Block, BlockBody, BlockHeader, ConfirmedBlock},
    data_types::{Transaction, Vote},
    types::ConfirmedBlockCertificate,
};
use linera_execution::Operation;
use revm::{
    database::{CacheDB, EmptyDB},
    primitives::{Address, Bytes, TxKind, U256},
    Context, ExecuteCommitEvm, MainBuilder, MainContext,
};
use revm_context::result::{ExecutionResult, Output};

pub const BRIDGE_TYPES_SOL: &str = include_str!("../../src/BridgeTypes.sol");
pub const LIGHT_CLIENT_SOL: &str = include_str!("../../src/LightClient.sol");
pub const MICROCHAIN_SOL: &str = include_str!("../../src/Microchain.sol");
pub const GAS_LIMIT: u64 = 500_000_000;

sol! {
    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        address[] calldata validators,
        uint64[] calldata weights
    ) external;
    function verifyBlock(bytes calldata data) external view;

    function addBlock(bytes calldata data) external;
    function blockCount() external view returns (uint64);
    function latestHeight() external view returns (uint64);
}

/// Derives the Ethereum address from a secp256k1 validator public key.
pub fn validator_evm_address(public: &ValidatorPublicKey) -> Address {
    let uncompressed = public.0.to_encoded_point(false);
    let hash = keccak256(&uncompressed.as_bytes()[1..]); // skip 0x04 prefix
    Address::from_slice(&hash[12..])
}

/// Creates a certificate with a real signature from the given key pair.
pub fn create_signed_certificate(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
) -> ConfirmedBlockCertificate {
    let chain_id = CryptoHash::new(&TestString::new("test_chain"));
    let transactions = vec![Transaction::ExecuteOperation(Operation::User {
        application_id: ApplicationId::new(CryptoHash::new(&TestString::new("test_app"))),
        bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
    })];
    let block = create_test_block(chain_id, Epoch::ZERO, BlockHeight(1), transactions);
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
}

/// Creates a certificate for a specific chain and height.
pub fn create_signed_certificate_for_chain(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
    chain_id: CryptoHash,
    height: BlockHeight,
) -> ConfirmedBlockCertificate {
    let transactions = vec![Transaction::ExecuteOperation(Operation::User {
        application_id: ApplicationId::new(CryptoHash::new(&TestString::new("test_app"))),
        bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
    })];
    let block = create_test_block(chain_id, Epoch::ZERO, height, transactions);
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
}

pub fn deploy_microchain(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    light_client: Address,
    chain_id: CryptoHash,
) -> Address {
    let test_source = std::fs::read_to_string("tests/solidity/MicrochainTest.sol")
        .expect("MicrochainTest.sol not found");
    let bytecode = compile_contract(&test_source, "MicrochainTest.sol", "MicrochainTest");
    let constructor_args =
        (light_client, <[u8; 32]>::from(*chain_id.as_bytes())).abi_encode_params();
    let mut deploy_data = bytecode;
    deploy_data.extend_from_slice(&constructor_args);
    deploy_contract(db, deployer, deploy_data)
}

pub fn deploy_light_client(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    validators: &[Address],
    weights: &[u64],
) -> Address {
    let bytecode = compile_contract(LIGHT_CLIENT_SOL, "LightClient.sol", "LightClient");
    let constructor_args = (validators.to_vec(), weights.to_vec()).abi_encode_params();
    let mut deploy_data = bytecode;
    deploy_data.extend_from_slice(&constructor_args);
    deploy_contract(db, deployer, deploy_data)
}

/// Deploys a compiled contract and returns its address.
pub fn deploy_contract(db: &mut CacheDB<EmptyDB>, deployer: Address, bytecode: Vec<u8>) -> Address {
    // Look up account nonce from the DB so multiple deployments work
    let nonce = db
        .cache
        .accounts
        .get(&deployer)
        .map_or(0, |info| info.info.nonce);
    let result = Context::mainnet()
        .with_db(db)
        .modify_cfg_chained(|cfg| {
            // BridgeTypes.sol exceeds the default EIP-170 contract size limit.
            cfg.limit_contract_code_size = Some(usize::MAX);
        })
        .modify_tx_chained(|tx| {
            tx.caller = deployer;
            tx.nonce = nonce;
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
pub fn call_contract(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    contract: Address,
    calldata: Vec<u8>,
) -> Bytes {
    match try_call_contract(db, deployer, contract, calldata) {
        Ok(bytes) => bytes,
        Err(msg) => panic!("{}", msg),
    }
}

/// Calls a deployed contract, returning Ok(output) on success or Err(message) on revert/halt.
pub fn try_call_contract(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    contract: Address,
    calldata: Vec<u8>,
) -> Result<Bytes, String> {
    let nonce = db
        .cache
        .accounts
        .get(&deployer)
        .map_or(0, |info| info.info.nonce);
    let result = Context::mainnet()
        .with_db(db)
        .modify_tx_chained(|tx| {
            tx.caller = deployer;
            tx.nonce = nonce;
            tx.kind = TxKind::Call(contract);
            tx.data = Bytes::from(calldata);
            tx.gas_limit = GAS_LIMIT;
            tx.value = U256::ZERO;
        })
        .build_mainnet()
        .replay_commit()
        .expect("call transaction failed");

    match result {
        ExecutionResult::Success { output, .. } => match output {
            Output::Call(bytes) => Ok(bytes),
            other => Err(format!("expected Call output, got: {:?}", other)),
        },
        ExecutionResult::Revert { output, .. } => {
            Err(format!("call reverted: {}", hex::encode(&output)))
        }
        ExecutionResult::Halt { reason, .. } => Err(format!("call halted: {:?}", reason)),
    }
}

pub fn create_test_block(
    chain_id: CryptoHash,
    epoch: Epoch,
    height: BlockHeight,
    transactions: Vec<Transaction>,
) -> Block {
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
            transactions,
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

pub fn compile_contract(source_code: &str, file_name: &str, contract_name: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    // Write shared source files so imports resolve
    for (name, content) in [
        ("BridgeTypes.sol", BRIDGE_TYPES_SOL),
        ("LightClient.sol", LIGHT_CLIENT_SOL),
        ("Microchain.sol", MICROCHAIN_SOL),
    ] {
        let mut f = File::create(path.join(name)).unwrap();
        writeln!(f, "{}", content).unwrap();
    }

    // Write the contract under test
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

    let bytecode_hex = json_data["contracts"][file_name][contract_name]["evm"]["bytecode"]
        ["object"]
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
