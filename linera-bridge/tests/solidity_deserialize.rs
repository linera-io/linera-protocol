// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use alloy_primitives::keccak256;
use alloy_sol_types::{sol, SolCall, SolValue};
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
    Context, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
};
use revm_context::result::{ExecutionResult, Output};

const BRIDGE_TYPES_SOL: &str = include_str!("../src/BridgeTypes.sol");
const LIGHT_CLIENT_SOL: &str = include_str!("../src/LightClient.sol");
const GAS_LIMIT: u64 = 500_000_000;

sol! {
    function deserialize(bytes calldata data)
        external
        pure
        returns (
            bytes32 chainId,
            uint32 epoch,
            uint64 height,
            uint64 txCount,
            bytes32 firstTxAppId,
            bytes memory firstTxBytes
        );

    function addCommittee(bytes calldata data) external;
    function addBlock(bytes calldata data) external;
}

#[test]
fn test_deserialize_confirmed_block_certificate() {
    let expected_chain_id = CryptoHash::new(&TestString::new("test_chain"));
    let expected_epoch = Epoch::ZERO;
    let expected_height = BlockHeight(1);
    let expected_app_id = ApplicationId::new(CryptoHash::new(&TestString::new("test_app")));
    let expected_user_bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];

    let transactions = vec![
        Transaction::ExecuteOperation(Operation::User {
            application_id: expected_app_id,
            bytes: expected_user_bytes.clone(),
        }),
        Transaction::ExecuteOperation(Operation::User {
            application_id: expected_app_id,
            bytes: vec![0x01, 0x02, 0x03],
        }),
    ];

    let block = create_test_block(
        expected_chain_id,
        expected_epoch,
        expected_height,
        transactions,
    );
    let confirmed = ConfirmedBlock::new(block);
    let certificate = ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![]);
    let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

    let test_source =
        std::fs::read_to_string("tests/solidity/BridgeTest.sol").expect("BridgeTest.sol not found");
    let bytecode = compile_contract(&test_source, "BridgeTest.sol", "BridgeTest");

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
    assert_eq!(decoded.txCount, 2, "transaction count mismatch");
    assert_eq!(
        decoded.firstTxAppId.0,
        *expected_app_id.application_description_hash.as_bytes(),
        "first transaction app_id mismatch"
    );
    assert_eq!(
        decoded.firstTxBytes.as_ref(),
        expected_user_bytes.as_slice(),
        "first transaction bytes mismatch"
    );
}

#[test]
fn test_light_client_add_committee() {
    let secret = ValidatorSecretKey::generate();
    let public = secret.public();
    let address = validator_evm_address(&public);

    let certificate = create_signed_certificate(&secret, &public);
    let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

    let deployer = Address::ZERO;
    let mut db = CacheDB::<EmptyDB>::default();
    let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

    let calldata = addCommitteeCall {
        data: bcs_bytes.into(),
    }
    .abi_encode();
    call_contract(&mut db, deployer, contract, calldata);
}

#[test]
fn test_light_client_add_block() {
    let secret = ValidatorSecretKey::generate();
    let public = secret.public();
    let address = validator_evm_address(&public);

    let certificate = create_signed_certificate(&secret, &public);
    let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

    let deployer = Address::ZERO;
    let mut db = CacheDB::<EmptyDB>::default();
    let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

    let calldata = addBlockCall {
        data: bcs_bytes.into(),
    }
    .abi_encode();
    call_contract(&mut db, deployer, contract, calldata);
}

#[test]
fn test_light_client_rejects_invalid_signature() {
    let secret = ValidatorSecretKey::generate();
    let public = secret.public();
    let address = validator_evm_address(&public);

    // Sign with a different key than the one in the committee
    let wrong_secret = ValidatorSecretKey::generate();
    let wrong_public = wrong_secret.public();
    let certificate = create_signed_certificate(&wrong_secret, &wrong_public);
    let bcs_bytes = bcs::to_bytes(&certificate).expect("BCS serialization failed");

    let deployer = Address::ZERO;
    let mut db = CacheDB::<EmptyDB>::default();
    let contract = deploy_light_client(&mut db, deployer, &[address], &[1]);

    let calldata = addBlockCall {
        data: bcs_bytes.into(),
    }
    .abi_encode();
    assert!(
        try_call_contract(&mut db, deployer, contract, calldata).is_err(),
        "should reject certificate signed by unknown validator"
    );
}

/// Derives the Ethereum address from a secp256k1 validator public key.
fn validator_evm_address(public: &ValidatorPublicKey) -> Address {
    let uncompressed = public.0.to_encoded_point(false);
    let hash = keccak256(&uncompressed.as_bytes()[1..]); // skip 0x04 prefix
    Address::from_slice(&hash[12..])
}

/// Creates a certificate with a real signature from the given key pair.
fn create_signed_certificate(
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

fn deploy_light_client(
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
    match try_call_contract(db, deployer, contract, calldata) {
        Ok(bytes) => bytes,
        Err(msg) => panic!("{}", msg),
    }
}

/// Calls a deployed contract, returning Ok(output) on success or Err(message) on revert/halt.
fn try_call_contract(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    contract: Address,
    calldata: Vec<u8>,
) -> Result<Bytes, String> {
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
            Output::Call(bytes) => Ok(bytes),
            other => Err(format!("expected Call output, got: {:?}", other)),
        },
        ExecutionResult::Revert { output, .. } => {
            Err(format!("call reverted: {}", hex::encode(&output)))
        }
        ExecutionResult::Halt { reason, .. } => Err(format!("call halted: {:?}", reason)),
    }
}

fn create_test_block(
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

fn compile_contract(source_code: &str, file_name: &str, contract_name: &str) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();

    // Write BridgeTypes.sol
    let types_path = path.join("BridgeTypes.sol");
    let mut types_file = File::create(&types_path).unwrap();
    writeln!(types_file, "{}", BRIDGE_TYPES_SOL).unwrap();

    // Write the contract
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
