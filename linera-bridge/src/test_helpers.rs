// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::collections::BTreeMap;

use alloy_primitives::B256;
use alloy_sol_types::{SolCall, SolValue};
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, TestString, ValidatorPublicKey, ValidatorSecretKey},
    data_types::{Amount, BlobContent, BlockHeight, Epoch, Event, Round, Timestamp, U128},
    identifiers::{ApplicationId, ChainId, StreamId},
};
use linera_chain::{
    block::{Block, ConfirmedBlock},
    data_types::{
        BlockExecutionOutcome, IncomingBundle, MessageAction, MessageBundle, PostedMessage,
        ProposedBlock, Transaction, Vote,
    },
    types::ConfirmedBlockCertificate,
};
use linera_execution::{
    committee::ValidatorState,
    system::{EpochEventData, EPOCH_STREAM_NAME},
    test_utils::solidity::compile_solidity_contract_with_options,
    Message, MessageKind, Operation, ResourceControlPolicy,
};
use revm::{
    database::{CacheDB, EmptyDB},
    primitives::{Address, Bytes, Log, TxKind, U256},
    Context, ExecuteCommitEvm, MainBuilder, MainContext,
};
use revm_context::result::{ExecutionResult, Output};

pub use crate::evm::client::{validator_evm_address, validator_uncompressed_key};
use crate::{block_proof::BlockProof, contracts::ILightClient::addCommitteeCall, evm};

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
    )
    .expect("committee creation failed");
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
    bcs::to_bytes(&BlockProof::from_certificate(&sign_certificate(
        secret, public, block,
    )))
    .expect("BCS serialization failed")
}

/// Builds the three `addBlock` arguments from a certificate: the lean block proof, the per-event
/// BCS encodings (flattened across transactions), and the number of events in each transaction.
pub fn add_block_args(certificate: &ConfirmedBlockCertificate) -> (Bytes, Vec<Bytes>, Vec<u32>) {
    let proof = Bytes::from(
        bcs::to_bytes(&BlockProof::from_certificate(certificate))
            .expect("BCS serialization failed"),
    );
    let events = &certificate.block().body.events;
    let event_bcs = events
        .iter()
        .flatten()
        .map(|event| Bytes::from(bcs::to_bytes(event).expect("BCS serialization failed")))
        .collect();
    let events_per_tx = events
        .iter()
        .map(|tx_events| u32::try_from(tx_events.len()).expect("event count exceeds u32"))
        .collect();
    (proof, event_bcs, events_per_tx)
}

/// The block-proof and inclusion-proof arguments for an `addCommittee` call: the lean block proof,
/// the single committee event's BCS encoding, and the inclusion proof binding it to the block.
pub struct CommitteeCallArgs {
    pub block_proof: Bytes,
    pub event_bcs: Vec<Bytes>,
    pub tx_index: u32,
    pub num_txs: u32,
    pub num_events_in_tx: u32,
    pub positions: Vec<u32>,
    pub inner_siblings: Vec<B256>,
    pub outer_siblings: Vec<B256>,
}

/// Builds the `addCommittee` proof arguments for a block whose sole event (transaction 0, position
/// 0) is `event`, signed by the given validator on `chain_id` at `block_epoch`/`height`. The
/// inclusion proof binds `event` to the block's `events_hash`.
pub fn committee_call_args_for_event(
    signer_secret: &ValidatorSecretKey,
    signer_public: &ValidatorPublicKey,
    event: Event,
    block_epoch: Epoch,
    height: BlockHeight,
    chain_id: CryptoHash,
) -> CommitteeCallArgs {
    let events = vec![vec![event]];
    let block = build_block(chain_id, block_epoch, height, vec![], events.clone());
    let cert = sign_certificate(signer_secret, signer_public, block);
    let proof = crate::block_proof::EventInclusionProof::new(&events, 0, &[0]);
    let to_b256 = |h: &CryptoHash| B256::from(*h.as_bytes());
    CommitteeCallArgs {
        block_proof: Bytes::from(
            bcs::to_bytes(&BlockProof::from_certificate(&cert)).expect("BCS serialization failed"),
        ),
        event_bcs: vec![Bytes::from(
            bcs::to_bytes(&events[0][0]).expect("BCS serialization failed"),
        )],
        tx_index: proof.tx_index,
        num_txs: proof.num_txs,
        num_events_in_tx: proof.num_events_in_tx,
        positions: vec![0],
        inner_siblings: proof.inner_siblings.iter().map(to_b256).collect(),
        outer_siblings: proof.outer_siblings.iter().map(to_b256).collect(),
    }
}

/// Like `committee_call_args_for_event`, but the event is the system epoch event for
/// `new_epoch`/`blob_hash` — the real `CreateCommittee` event.
pub fn committee_block_args(
    signer_secret: &ValidatorSecretKey,
    signer_public: &ValidatorPublicKey,
    new_epoch: Epoch,
    blob_hash: CryptoHash,
    block_epoch: Epoch,
    height: BlockHeight,
    chain_id: CryptoHash,
) -> CommitteeCallArgs {
    committee_call_args_for_event(
        signer_secret,
        signer_public,
        epoch_event(new_epoch, blob_hash),
        block_epoch,
        height,
        chain_id,
    )
}

/// Assembles an `addCommitteeCall` from the inclusion-proof args, the committee blob, and the
/// caller's uncompressed validator keys.
pub fn build_add_committee_call(
    args: CommitteeCallArgs,
    committee_blob: Vec<u8>,
    validators: Vec<Bytes>,
) -> addCommitteeCall {
    addCommitteeCall {
        blockProof: args.block_proof,
        eventBcs: args.event_bcs,
        txIndex: args.tx_index,
        numTxs: args.num_txs,
        numEventsInTx: args.num_events_in_tx,
        positions: args.positions,
        innerSiblings: args.inner_siblings,
        outerSiblings: args.outer_siblings,
        committeeBlob: committee_blob.into(),
        validators,
    }
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

/// Creates a certificate for a block carrying `events` (one inner vector per transaction), signed
/// by the given validator. Used to exercise event-inclusion proofs.
pub fn create_signed_certificate_with_events(
    secret: &ValidatorSecretKey,
    public: &ValidatorPublicKey,
    chain_id: CryptoHash,
    height: BlockHeight,
    events: Vec<Vec<Event>>,
) -> ConfirmedBlockCertificate {
    let block = build_block(chain_id, Epoch::ZERO, height, vec![], events);
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
    let constructor_args = (light_client, *chain_id.as_bytes()).abi_encode_params();
    let mut deploy_data = bytecode;
    deploy_data.extend_from_slice(&constructor_args);
    deploy_contract(db, deployer, deploy_data)
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
                authenticated_owner: None,
                grant: Amount::ZERO,
                refund_grant_to: None,
                kind: MessageKind::Simple,
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
pub fn burn_event(application_id: CryptoHash, target: [u8; 20], amount: U128, index: u32) -> Event {
    use linera_base::identifiers::{GenericApplicationId, StreamId, StreamName};
    Event {
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
    events: Vec<Vec<Event>>,
) -> ConfirmedBlockCertificate {
    let block = build_block(chain_id, Epoch::ZERO, height, vec![], events);
    let confirmed = ConfirmedBlock::new(block);
    let vote = Vote::new(confirmed.clone(), Round::Fast, secret);
    ConfirmedBlockCertificate::new(confirmed, Round::Fast, vec![(*public, vote.signature)])
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
    let constructor_args =
        (validators.to_vec(), weights.to_vec(), chain_id_bytes, epoch).abi_encode_params();
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
    call: &C,
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
    call: &C,
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

/// Builds a test block whose header is consistent with its body (the header is computed from
/// the body via `Block::new`), so it round-trips through the light client's verification.
fn build_block(
    chain_id: CryptoHash,
    epoch: Epoch,
    height: BlockHeight,
    transactions: Vec<Transaction>,
    events: Vec<Vec<Event>>,
) -> Block {
    let proposed = ProposedBlock {
        chain_id: ChainId(chain_id),
        epoch,
        transactions,
        height,
        timestamp: Timestamp::from(0),
        authenticated_owner: None,
        previous_block_hash: None,
    };
    let outcome = BlockExecutionOutcome {
        state_hash: CryptoHash::new(&TestString::new("state")),
        messages: vec![],
        previous_message_blocks: Default::default(),
        previous_event_blocks: Default::default(),
        oracle_responses: vec![],
        events,
        blobs: vec![],
        operation_results: vec![],
    };
    Block::new(proposed, outcome)
}

pub fn create_test_block(
    chain_id: CryptoHash,
    epoch: Epoch,
    height: BlockHeight,
    transactions: Vec<Transaction>,
) -> Block {
    build_block(chain_id, epoch, height, transactions, vec![])
}

pub fn compile_contract(source_code: &str, file_name: &str, contract_name: &str) -> Vec<u8> {
    // `runs = 1` optimizes for smaller deployed bytecode at the cost of per-call
    // gas. Bridge contracts are large; tests compile faster with this setting.
    compile_solidity_contract_with_options(
        source_code,
        file_name,
        contract_name,
        &[
            ("BridgeTypes.sol", evm::BRIDGE_TYPES_SOURCE),
            (
                "WrappedFungibleTypes.sol",
                evm::WRAPPED_FUNGIBLE_TYPES_SOURCE,
            ),
            ("LightClient.sol", evm::light_client::SOURCE),
            ("Microchain.sol", evm::microchain::SOURCE),
            ("FungibleBridge.sol", evm::FUNGIBLE_BRIDGE_SOURCE),
        ],
        Some(1),
    )
    .expect("solc compilation failed")
}
