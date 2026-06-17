// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use alloy_sol_types::{SolCall, SolValue};
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, TestString, ValidatorPublicKey, ValidatorSecretKey},
<<<<<<< HEAD
    data_types::{Amount, BlobContent, BlockHeight, Epoch, Round, Timestamp, U128},
    identifiers::{ApplicationId, ChainId},
=======
    data_types::{Amount, BlobContent, BlockHeight, Epoch, Event, Round, Timestamp, U128},
    identifiers::{ApplicationId, ChainId, StreamId},
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
};
use linera_chain::{
    block::{Block, BlockBody, BlockHeader, ConfirmedBlock},
    data_types::{IncomingBundle, MessageAction, MessageBundle, PostedMessage, Transaction, Vote},
    types::ConfirmedBlockCertificate,
};
use linera_execution::{
<<<<<<< HEAD
    committee::ValidatorState, system::AdminOperation, Message, MessageKind, Operation,
    ResourceControlPolicy, SystemOperation,
=======
    committee::ValidatorState,
    system::{EpochEventData, EPOCH_STREAM_NAME},
    test_utils::solidity::compile_solidity_contract_with_options,
    Message, MessageKind, Operation, ResourceControlPolicy,
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
};
use revm::{
    database::{CacheDB, EmptyDB},
    primitives::{Address, Bytes, Log, TxKind, U256},
    Context, ExecuteCommitEvm, MainBuilder, MainContext,
};
use revm_context::result::{ExecutionResult, Output};

<<<<<<< HEAD
use crate::evm;
pub use crate::evm::client::{validator_evm_address, validator_uncompressed_key};
=======
pub use crate::evm::client::validator_evm_address;
use crate::{
    block_proof::{BlockProof, ProvenEvents},
    contracts::ILightClient::addCommitteeCall,
    evm,
};
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))

pub const GAS_LIMIT: u64 = 500_000_000;

/// The admin chain ID used in tests.
pub fn test_admin_chain_id() -> CryptoHash {
    CryptoHash::new(&TestString::new("admin_chain"))
}

/// Creates a single-validator committee blob and returns `(committee_bytes, blob_hash)`.
pub fn create_committee_blob(public: &ValidatorPublicKey) -> (Vec<u8>, CryptoHash) {
    let committee = linera_execution::Committee::new(
        BTreeMap::from([(
            *public,
            ValidatorState {
                network_address: "127.0.0.1:8080".to_string(),
                votes: 1,
                account_public_key: AccountPublicKey::Secp256k1(*public),
            },
        )]),
        ResourceControlPolicy::default(),
    );
    let bytes = bcs::to_bytes(&committee).expect("committee serialization failed");
    let blob_content = BlobContent::new_committee(bytes.clone());
    let blob_hash = CryptoHash::new(&blob_content);
    (bytes, blob_hash)
}

/// Creates the system epoch event Linera emits on `CreateCommittee`: indexed by the new epoch,
/// carrying the committee blob hash in its `EpochEventData` payload.
pub fn epoch_event(new_epoch: Epoch, blob_hash: CryptoHash) -> Event {
    Event {
        stream_id: StreamId::system(EPOCH_STREAM_NAME),
        index: new_epoch.0,
        value: bcs::to_bytes(&EpochEventData {
            blob_hash,
            timestamp: Timestamp::from(0),
        })
        .expect("epoch event serialization failed"),
    }
}

/// Builds an event identical to [`epoch_event`] in index, stream-name bytes, and payload, but
/// emitted on a *user* application stream (`GenericApplicationId::User`) instead of the system
/// stream. Used to verify the LightClient upgrades committees only from the system stream.
pub fn forged_user_epoch_event(
    application_id: CryptoHash,
    new_epoch: Epoch,
    blob_hash: CryptoHash,
) -> Event {
    use linera_base::identifiers::{GenericApplicationId, StreamName};
    Event {
        stream_id: StreamId {
            application_id: GenericApplicationId::User(ApplicationId::new(application_id)),
            stream_name: StreamName(EPOCH_STREAM_NAME.to_vec()),
        },
        index: new_epoch.0,
        value: bcs::to_bytes(&EpochEventData {
            blob_hash,
            timestamp: Timestamp::from(0),
        })
        .expect("epoch event serialization failed"),
    }
}

/// Signs a block with a single validator and returns the `ConfirmedBlockCertificate`.
pub fn sign_certificate(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
    block: Block,
) -> ConfirmedBlockCertificate {
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
}

/// Signs a block and returns the BCS-serialized `BlockProof`.
pub fn sign_and_serialize(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
    block: Block,
) -> Vec<u8> {
<<<<<<< HEAD
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    let certificate =
        ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)]);
    bcs::to_bytes(&certificate).expect("BCS serialization failed")
=======
    bcs::to_bytes(&BlockProof::from_certificate(&sign_certificate(
        secret, public, block,
    )))
    .expect("BCS serialization failed")
}

/// Builds the register-then-`addCommittee` inputs for a block whose sole event (transaction 0,
/// position 0) is `event`, signed by the given validator on `chain_id` at `block_epoch`/`height`:
/// the [`ProvenEvents`] witness `addCommittee` consumes, and the BCS block proof to `registerBlock`
/// first.
pub fn committee_call_args_for_event(
    signer_secret: &ValidatorSecretKey,
    signer_public: &ValidatorPublicKey,
    event: Event,
    block_epoch: Epoch,
    height: BlockHeight,
    chain_id: CryptoHash,
) -> (ProvenEvents, Bytes) {
    let block = build_block(chain_id, block_epoch, height, vec![], vec![vec![event]]);
    let cert = sign_certificate(signer_secret, signer_public, block);
    let block_proof = Bytes::from(
        bcs::to_bytes(&BlockProof::from_certificate(&cert)).expect("BCS serialization failed"),
    );
    (ProvenEvents::new(&cert, 0, &[0]), block_proof)
}

/// Assembles an `addCommitteeCall` from the proven-events witness and the committee blob.
pub fn build_add_committee_call(proven: ProvenEvents, committee_blob: Vec<u8>) -> addCommitteeCall {
    addCommitteeCall {
        blockHash: proven.block_hash,
        eventBcs: proven.event_bcs,
        txIndex: proven.tx_index,
        numTxs: proven.num_txs,
        numEventsInTx: proven.num_events_in_tx,
        positions: proven.positions,
        siblings: proven.siblings,
        committeeBlob: committee_blob.into(),
    }
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
}

/// Creates a certificate with custom transactions for a specific chain and height.
pub fn create_certificate_with_transactions(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
    chain_id: CryptoHash,
    height: BlockHeight,
    transactions: Vec<Transaction>,
) -> ConfirmedBlockCertificate {
    let block = create_test_block(chain_id, Epoch::ZERO, height, transactions);
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
}

/// Creates a certificate for a specific chain and height with a default user operation.
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
    create_certificate_with_transactions(secret, public, chain_id, height, transactions)
}

/// Creates a certificate with default chain, height, and transaction.
pub fn create_signed_certificate(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
) -> ConfirmedBlockCertificate {
    let chain_id = CryptoHash::new(&TestString::new("test_chain"));
    create_signed_certificate_for_chain(secret, public, chain_id, BlockHeight(1))
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
    let constructor_args = (
        light_client,
        *chain_id.as_bytes(),
        test_pause_guardian(),
        test_proposer(),
        test_canceller(),
        test_timelock_delay(),
    )
        .abi_encode_params();
    let mut deploy_data = bytecode;
    deploy_data.extend_from_slice(&constructor_args);
    deploy_contract(db, deployer, deploy_data)
}

/// Deploys the V1 burn-event decoder (no constructor args) and returns its
/// address.
pub fn deploy_burn_event_decoder_v1(db: &mut CacheDB<EmptyDB>, deployer: Address) -> Address {
    let bytecode = compile_contract(
        evm::FUNGIBLE_BURN_EVENT_DECODER_V1_SOURCE,
        "FungibleBurnEventDecoderV1.sol",
        "FungibleBurnEventDecoderV1",
    );
    deploy_contract(db, deployer, bytecode)
}

pub fn deploy_fungible_bridge(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    light_client: Address,
    chain_id: CryptoHash,
    token: Address,
    application_id: CryptoHash,
    bridge_application_id: CryptoHash,
) -> Address {
    let decoder = deploy_burn_event_decoder_v1(db, deployer);
    let bytecode = compile_contract(
        evm::FUNGIBLE_BRIDGE_SOURCE,
        "FungibleBridge.sol",
        "FungibleBridge",
    );
    let constructor_args = (
        light_client,
        *chain_id.as_bytes(),
        token,
        *application_id.as_bytes(),
        *bridge_application_id.as_bytes(),
        decoder,
        test_pause_guardian(),
        test_proposer(),
        test_canceller(),
        test_timelock_delay(),
    )
        .abi_encode_params();
    let mut deploy_data = bytecode;
    deploy_data.extend_from_slice(&constructor_args);
    deploy_contract(db, deployer, deploy_data)
}

const LINERA_TOKEN_SOL: &str = include_str!("solidity/LineraToken.sol");

alloy_sol_types::sol! {
    #[allow(missing_docs)]
    struct LineraTokenConstructorArgs {
        string name;
        string symbol;
        uint8 decimals_;
        uint256 initialSupply;
    }
}

pub fn deploy_linera_token(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    initial_supply: alloy_primitives::U256,
) -> Address {
    let bytecode = compile_contract(LINERA_TOKEN_SOL, "LineraToken.sol", "LineraToken");
    let args = LineraTokenConstructorArgs {
        name: "TestToken".to_string(),
        symbol: "TT".to_string(),
        decimals_: 18,
        initialSupply: initial_supply,
    };
    let mut deploy_data = bytecode;
    deploy_data.extend_from_slice(&args.abi_encode_params());
    deploy_contract(db, deployer, deploy_data)
}

/// Creates a Transaction::ReceiveMessages containing a fungible Message as a user message.
pub fn fungible_message_transaction(
    origin: ChainId,
    application_id: CryptoHash,
    message: &wrapped_fungible::Message,
) -> Transaction {
    Transaction::ReceiveMessages(IncomingBundle {
        origin,
        bundle: MessageBundle {
            height: BlockHeight(0),
            timestamp: Timestamp::from(0),
            certificate_hash: CryptoHash::new(&TestString::new("cert")),
            transaction_index: 0,
            messages: vec![PostedMessage {
                authenticated_signer: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Simple,
                index: 0,
                message: Message::User {
                    application_id: ApplicationId::new(application_id),
                    bytes: bcs::to_bytes(message).unwrap(),
                },
            }],
        },
        action: MessageAction::Accept,
    })
}

/// Creates a BurnEvent as a linera_base Event on the "burns" stream for a given application.
pub fn burn_event(
    application_id: CryptoHash,
    target: [u8; 20],
    amount: U128,
    index: u32,
) -> linera_base::data_types::Event {
    use linera_base::identifiers::{GenericApplicationId, StreamId, StreamName};
    linera_base::data_types::Event {
        stream_id: StreamId {
            application_id: GenericApplicationId::User(ApplicationId::new(application_id)),
            stream_name: StreamName(b"burns".to_vec()),
        },
        index,
        value: bcs::to_bytes(&wrapped_fungible::BurnEvent { target, amount }).unwrap(),
    }
}

/// Creates a certificate containing events (no transactions).
pub fn create_certificate_with_events(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
    chain_id: CryptoHash,
    height: BlockHeight,
    events: Vec<Vec<linera_base::data_types::Event>>,
) -> ConfirmedBlockCertificate {
    let block = Block {
        header: BlockHeader {
            chain_id: ChainId(chain_id),
            epoch: Epoch::ZERO,
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
            events,
            blobs: vec![],
            operation_results: vec![],
        },
    };
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
}

/// Default governance addresses used when deploying a LightClient/FungibleBridge
/// in tests that do not exercise governance. Non-zero so the constructors'
/// zero-address guards pass; tests that exercise governance act as these
/// addresses (e.g. `expireEpochsBelow` must be called by `test_proposer()`).
pub fn test_pause_guardian() -> Address {
    Address::from([0xDA; 20])
}

pub fn test_proposer() -> Address {
    Address::from([0xBE; 20])
}

pub fn test_canceller() -> Address {
    Address::from([0xCA; 20])
}

/// Default bridge timelock delay (1 day) — the minimum the Microchain
/// constructor accepts.
pub fn test_timelock_delay() -> U256 {
    U256::from(86_400u64)
}

pub fn deploy_light_client(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    validators: &[Address],
    weights: &[u64],
    admin_chain_id: CryptoHash,
    epoch: u32,
) -> Address {
    let bytecode = compile_contract(evm::light_client::SOURCE, "LightClient.sol", "LightClient");
    let chain_id_bytes = *admin_chain_id.as_bytes();
    let constructor_args = (
        validators.to_vec(),
        weights.to_vec(),
        chain_id_bytes,
        epoch,
        test_pause_guardian(),
        test_proposer(),
    )
        .abi_encode_params();
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
            other => panic!("expected Create output with address, got: {other:?}"),
        },
        ExecutionResult::Revert { output, .. } => {
            panic!("deployment reverted: {}", hex::encode(&output));
        }
        ExecutionResult::Halt { reason, .. } => {
            panic!("deployment halted: {reason:?}");
        }
    }
}

/// Calls a deployed contract, returning the decoded return value, emitted logs, and gas used.
pub fn call_contract<C: SolCall>(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    contract: Address,
    call: C,
) -> (C::Return, Vec<Log>, u64) {
    match try_call_contract(db, deployer, contract, call) {
        Ok(ret) => ret,
        Err(msg) => panic!("{}", msg),
    }
}

/// Calls a deployed contract, returning the decoded return value, logs, and gas on success
/// or an error message on revert/halt/decode failure.
pub fn try_call_contract<C: SolCall>(
    db: &mut CacheDB<EmptyDB>,
    deployer: Address,
    contract: Address,
    call: C,
) -> Result<(C::Return, Vec<Log>, u64), String> {
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
            tx.data = Bytes::from(call.abi_encode());
            tx.gas_limit = GAS_LIMIT;
            tx.value = U256::ZERO;
        })
        .build_mainnet()
        .replay_commit()
        .expect("call transaction failed");

    let gas_used = result.gas_used();
    match result {
        ExecutionResult::Success { output, logs, .. } => match output {
            Output::Call(bytes) => {
                let ret = C::abi_decode_returns(&bytes)
                    .map_err(|e| format!("failed to decode return value: {e}"))?;
                Ok((ret, logs, gas_used))
            }
            other => Err(format!("expected Call output, got: {other:?}")),
        },
        ExecutionResult::Revert { output, .. } => {
            Err(format!("call reverted: {}", hex::encode(&output)))
        }
        ExecutionResult::Halt { reason, .. } => Err(format!("call halted: {reason:?}")),
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

    // Write hand-written shared contracts so imports resolve.
    for (name, content) in [
        ("BridgeTypes.sol", evm::BRIDGE_TYPES_SOURCE),
        (
            "WrappedFungibleTypesV1.sol",
            evm::WRAPPED_FUNGIBLE_TYPES_V1_SOURCE,
        ),
        ("LightClient.sol", evm::light_client::SOURCE),
        ("ILightClient.sol", evm::ILIGHTCLIENT_SOURCE),
        ("Microchain.sol", evm::microchain::SOURCE),
        ("IBurnEventDecoder.sol", evm::IBURN_EVENT_DECODER_SOURCE),
        (
            "FungibleBurnEventDecoderV1.sol",
            evm::FUNGIBLE_BURN_EVENT_DECODER_V1_SOURCE,
        ),
        ("FungibleBridge.sol", evm::FUNGIBLE_BRIDGE_SOURCE),
    ] {
        let mut f = File::create(path.join(name)).unwrap();
        writeln!(f, "{content}").unwrap();
    }

    // Write the contract under test.
    let test_path = path.join(file_name);
    let mut test_file = File::create(&test_path).unwrap();
    writeln!(test_file, "{source_code}").unwrap();

    // Resolve the OpenZeppelin submodule path. Tests run from the crate root.
    let oz_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/solidity/lib/openzeppelin-contracts");

    let oz_files: &[&str] = &[
        "contracts/token/ERC20/ERC20.sol",
        "contracts/token/ERC20/IERC20.sol",
        "contracts/token/ERC20/extensions/IERC20Metadata.sol",
        "contracts/utils/Context.sol",
        "contracts/interfaces/draft-IERC6093.sol",
    ];

    write_compilation_json(path, file_name, &oz_root, oz_files);

    let config_file = File::open(path.join("config.json")).unwrap();
    let output_file = File::create(path.join("result.json")).unwrap();

    let status = Command::new("solc")
        .current_dir(path)
        .arg("--standard-json")
        .arg("--allow-paths")
        .arg(oz_root.to_str().unwrap())
        .stdin(Stdio::from(config_file))
        .stdout(Stdio::from(output_file))
        .status()
        .expect("solc must be installed");
    assert!(status.success(), "solc compilation failed");

    let contents = std::fs::read_to_string(path.join("result.json")).unwrap();
    let json_data: serde_json::Value = serde_json::from_str(&contents).unwrap();

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

fn write_compilation_json(path: &Path, file_name: &str, oz_root: &Path, oz_files: &[&str]) {
    let mut sources = serde_json::Map::new();
    sources.insert(
        file_name.to_string(),
        serde_json::json!({ "urls": [format!("./{file_name}")] }),
    );
    for rel in oz_files {
        let import_key = format!("@openzeppelin/{rel}");
        let abs = oz_root.join(rel);
        sources.insert(
            import_key,
            serde_json::json!({ "urls": [abs.to_str().unwrap()] }),
        );
    }

    let config = serde_json::json!({
        "language": "Solidity",
        "sources": sources,
        "settings": {
            "viaIR": true,
            "optimizer": { "enabled": true, "runs": 1 },
            "remappings": ["@openzeppelin/contracts/=@openzeppelin/contracts/"],
            "outputSelection": { "*": { "*": ["evm.bytecode"] } }
        }
    });

    std::fs::write(path.join("config.json"), config.to_string()).unwrap();
}
