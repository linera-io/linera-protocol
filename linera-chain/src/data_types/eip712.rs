// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EIP-712 structured typed data hashing for block proposals.
//!
//! This module implements the EIP-712 signing hash computation so that EVM wallet
//! users can see human-readable block proposal details when signing.

use std::sync::LazyLock;

use alloy_primitives::{hex, keccak256};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ApplicationPermissions, Round, TimeDelta},
    identifiers::{AccountOwner, ApplicationId, BlobType, ChainId, GenericApplicationId, ModuleId},
};
use linera_execution::{
    system::{AdminOperation, OpenChainConfig},
    Operation, SystemOperation,
};

use super::{MessageAction, ProposalContent, Transaction};

// -- EIP-712 type strings --
//
// Per the EIP-712 spec, the type string for a struct includes all referenced types
// sorted alphabetically and concatenated after the primary type.

const EIP712_DOMAIN_TYPE: &str = "EIP712Domain(string name,string version)";

// The primary proposal type — fields listed in order, then all referenced
// struct types sorted alphabetically and concatenated.
const LINERA_BLOCK_PROPOSAL_TYPE: &str = "\
LineraBlockProposal(\
bytes32 chainId,\
uint64 epoch,\
uint64 height,\
uint64 timestamp,\
string round,\
bytes32 contentHash,\
TransferOp[] transfers,\
ClaimOp[] claims,\
ReceiveMsg[] incomingMessages,\
UserOp[] userOperations,\
OpenChainOp[] openChainOps,\
CloseChainOp[] closeChainOps,\
ChangeOwnershipOp[] changeOwnershipOps,\
ChangeAppPermissionsOp[] changeAppPermissionsOps,\
PublishModuleOp[] publishModuleOps,\
PublishDataBlobOp[] publishDataBlobOps,\
VerifyBlobOp[] verifyBlobOps,\
CreateApplicationOp[] createApplicationOps,\
AdminOp[] adminOps,\
ProcessNewEpochOp[] processNewEpochOps,\
ProcessRemovedEpochOp[] processRemovedEpochOps,\
UpdateStreamsOp[] updateStreamsOps,\
bytes32[] otherTransactionHashes\
)\
AdminOp(string operation,uint64 epoch,bytes32 blobHash)\
ChangeAppPermissionsOp(\
bool hasExecuteFilter,string[] executeOperations,\
string[] mandatoryApplications,string[] closeChainApps,\
string[] changePermissionsApps,\
bool hasOracleFilter,string[] oracleApps,\
bool hasHttpFilter,string[] httpApps)\
ChangeOwnershipOp(\
string[] superOwners,WeightedOwner[] owners,\
uint32 multiLeaderRounds,bool openMultiLeaderRounds,\
uint64 baseTimeoutMicros,uint64 timeoutIncrementMicros,\
uint64 fallbackDurationMicros,uint64 fastRoundDurationMicros)\
ClaimOp(string owner,string targetChain,string recipient,uint128 amount)\
CloseChainOp()\
CreateApplicationOp(\
bytes32 contractBlobHash,bytes32 serviceBlobHash,string vmRuntime,\
bytes32 parametersHash,bytes32 instantiationArgumentHash,\
string[] requiredApplicationIds)\
OpenChainOp(\
uint128 balance,\
string[] superOwners,WeightedOwner[] owners,\
uint32 multiLeaderRounds,bool openMultiLeaderRounds,\
uint64 baseTimeoutMicros,uint64 timeoutIncrementMicros,\
uint64 fallbackDurationMicros,uint64 fastRoundDurationMicros,\
bytes32 permissionsHash)\
ProcessNewEpochOp(uint64 epoch)\
ProcessRemovedEpochOp(uint64 epoch)\
PublishDataBlobOp(bytes32 blobHash)\
PublishModuleOp(bytes32 contractBlobHash,bytes32 serviceBlobHash,string vmRuntime)\
ReceiveMsg(string origin,string action,uint32 messageCount)\
StreamUpdate(string chainId,string streamId,uint32 index)\
TransferOp(string owner,string recipient,uint128 amount)\
UpdateStreamsOp(StreamUpdate[] updates)\
UserOp(string applicationId,bytes32 dataHash)\
VerifyBlobOp(string blobType,bytes32 hash)\
WeightedOwner(string owner,uint64 weight)";

// Individual type strings (for type hash computation).
const TRANSFER_OP_TYPE: &str = "TransferOp(string owner,string recipient,uint128 amount)";
const CLAIM_OP_TYPE: &str =
    "ClaimOp(string owner,string targetChain,string recipient,uint128 amount)";
const RECEIVE_MSG_TYPE: &str = "ReceiveMsg(string origin,string action,uint32 messageCount)";
const USER_OP_TYPE: &str = "UserOp(string applicationId,bytes32 dataHash)";
const OPEN_CHAIN_OP_TYPE: &str = "\
OpenChainOp(\
uint128 balance,\
string[] superOwners,WeightedOwner[] owners,\
uint32 multiLeaderRounds,bool openMultiLeaderRounds,\
uint64 baseTimeoutMicros,uint64 timeoutIncrementMicros,\
uint64 fallbackDurationMicros,uint64 fastRoundDurationMicros,\
bytes32 permissionsHash)\
WeightedOwner(string owner,uint64 weight)";
const CLOSE_CHAIN_OP_TYPE: &str = "CloseChainOp()";
const CHANGE_OWNERSHIP_OP_TYPE: &str = "\
ChangeOwnershipOp(\
string[] superOwners,WeightedOwner[] owners,\
uint32 multiLeaderRounds,bool openMultiLeaderRounds,\
uint64 baseTimeoutMicros,uint64 timeoutIncrementMicros,\
uint64 fallbackDurationMicros,uint64 fastRoundDurationMicros)\
WeightedOwner(string owner,uint64 weight)";
const WEIGHTED_OWNER_TYPE: &str = "WeightedOwner(string owner,uint64 weight)";
const CHANGE_APP_PERMISSIONS_OP_TYPE: &str = "\
ChangeAppPermissionsOp(\
bool hasExecuteFilter,string[] executeOperations,\
string[] mandatoryApplications,string[] closeChainApps,\
string[] changePermissionsApps,\
bool hasOracleFilter,string[] oracleApps,\
bool hasHttpFilter,string[] httpApps)";
const PUBLISH_MODULE_OP_TYPE: &str =
    "PublishModuleOp(bytes32 contractBlobHash,bytes32 serviceBlobHash,string vmRuntime)";
const PUBLISH_DATA_BLOB_OP_TYPE: &str = "PublishDataBlobOp(bytes32 blobHash)";
const VERIFY_BLOB_OP_TYPE: &str = "VerifyBlobOp(string blobType,bytes32 hash)";
const CREATE_APPLICATION_OP_TYPE: &str = "\
CreateApplicationOp(\
bytes32 contractBlobHash,bytes32 serviceBlobHash,string vmRuntime,\
bytes32 parametersHash,bytes32 instantiationArgumentHash,\
string[] requiredApplicationIds)";
const ADMIN_OP_TYPE: &str = "AdminOp(string operation,uint64 epoch,bytes32 blobHash)";
const PROCESS_NEW_EPOCH_OP_TYPE: &str = "ProcessNewEpochOp(uint64 epoch)";
const PROCESS_REMOVED_EPOCH_OP_TYPE: &str = "ProcessRemovedEpochOp(uint64 epoch)";
const UPDATE_STREAMS_OP_TYPE: &str = "UpdateStreamsOp(StreamUpdate[] updates)\
     StreamUpdate(string chainId,string streamId,uint32 index)";
const STREAM_UPDATE_TYPE: &str = "StreamUpdate(string chainId,string streamId,uint32 index)";

// -- Precomputed type hashes --

static DOMAIN_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(EIP712_DOMAIN_TYPE).0);
static PROPOSAL_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(LINERA_BLOCK_PROPOSAL_TYPE).0);
static TRANSFER_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(TRANSFER_OP_TYPE).0);
static CLAIM_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(CLAIM_OP_TYPE).0);
static RECEIVE_MSG_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(RECEIVE_MSG_TYPE).0);
static USER_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(USER_OP_TYPE).0);
static OPEN_CHAIN_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(OPEN_CHAIN_OP_TYPE).0);
static CLOSE_CHAIN_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(CLOSE_CHAIN_OP_TYPE).0);
static CHANGE_OWNERSHIP_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(CHANGE_OWNERSHIP_OP_TYPE).0);
static CHANGE_APP_PERMISSIONS_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(CHANGE_APP_PERMISSIONS_OP_TYPE).0);
static PUBLISH_MODULE_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(PUBLISH_MODULE_OP_TYPE).0);
static PUBLISH_DATA_BLOB_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(PUBLISH_DATA_BLOB_OP_TYPE).0);
static VERIFY_BLOB_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(VERIFY_BLOB_OP_TYPE).0);
static CREATE_APPLICATION_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(CREATE_APPLICATION_OP_TYPE).0);
static ADMIN_OP_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(ADMIN_OP_TYPE).0);
static PROCESS_NEW_EPOCH_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(PROCESS_NEW_EPOCH_OP_TYPE).0);
static PROCESS_REMOVED_EPOCH_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(PROCESS_REMOVED_EPOCH_OP_TYPE).0);
static UPDATE_STREAMS_OP_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(UPDATE_STREAMS_OP_TYPE).0);
static STREAM_UPDATE_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(STREAM_UPDATE_TYPE).0);
static WEIGHTED_OWNER_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(WEIGHTED_OWNER_TYPE).0);

// -- Domain separator (constant: name="Linera", version="1") --

static DOMAIN_SEPARATOR: LazyLock<[u8; 32]> = LazyLock::new(|| {
    let mut buf = Vec::with_capacity(96);
    buf.extend_from_slice(&*DOMAIN_TYPE_HASH);
    buf.extend_from_slice(&keccak256("Linera").0); // Domain name.
    buf.extend_from_slice(&keccak256("1").0); // Version of the EIP-712 schema, not the protocol version.
    keccak256(&buf).0
});

// -- ABI encoding helpers --

fn encode_u64(value: u64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[24..32].copy_from_slice(&value.to_be_bytes());
    buf
}

fn encode_u32(value: u32) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[28..32].copy_from_slice(&value.to_be_bytes());
    buf
}

fn encode_u128(value: u128) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[16..32].copy_from_slice(&value.to_be_bytes());
    buf
}

fn encode_string(s: &str) -> [u8; 32] {
    keccak256(s).0
}

fn encode_bytes32(value: [u8; 32]) -> [u8; 32] {
    value
}

fn encode_bool(value: bool) -> [u8; 32] {
    let mut buf = [0u8; 32];
    if value {
        buf[31] = 1;
    }
    buf
}

/// Encodes a dynamic array of 32-byte elements: `keccak256(concat(elem...))`.
fn encode_hash_array(hashes: &[[u8; 32]]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(hashes.len() * 32);
    for h in hashes {
        buf.extend_from_slice(h);
    }
    keccak256(&buf).0
}

// -- String formatting --

fn format_account_owner(owner: &AccountOwner) -> String {
    owner.to_string()
}

fn format_chain_id(chain_id: &ChainId) -> String {
    chain_id.to_string()
}

fn format_account(chain_id: &ChainId, owner: &AccountOwner) -> String {
    format!(
        "{}:{}",
        format_account_owner(owner),
        format_chain_id(chain_id)
    )
}

fn format_round(round: &Round) -> String {
    match round {
        Round::Fast => "Fast".to_string(),
        Round::MultiLeader(n) => format!("MultiLeader({})", n),
        Round::SingleLeader(n) => format!("SingleLeader({})", n),
        Round::Validator(n) => format!("Validator({})", n),
    }
}

fn format_application_id(app_id: &ApplicationId) -> String {
    format!("{}", app_id.application_description_hash)
}

fn format_bytes32(bytes: [u8; 32]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn format_vm_runtime(module_id: &ModuleId) -> &'static str {
    match module_id.vm_runtime {
        linera_base::vm::VmRuntime::Wasm => "Wasm",
        linera_base::vm::VmRuntime::Evm => "Evm",
    }
}

fn format_blob_type(blob_type: &BlobType) -> &'static str {
    match blob_type {
        BlobType::Data => "Data",
        BlobType::ContractBytecode => "ContractBytecode",
        BlobType::ServiceBytecode => "ServiceBytecode",
        BlobType::EvmBytecode => "EvmBytecode",
        BlobType::ApplicationDescription => "ApplicationDescription",
        BlobType::Committee => "Committee",
        BlobType::ChainDescription => "ChainDescription",
    }
}

fn format_message_action(action: &MessageAction) -> &'static str {
    match action {
        MessageAction::Accept => "Accept",
        MessageAction::Reject => "Reject",
    }
}

fn format_generic_app_id(id: &GenericApplicationId) -> String {
    match id {
        GenericApplicationId::System => "system".to_string(),
        GenericApplicationId::User(app_id) => format_application_id(app_id),
    }
}

fn timedelta_micros(td: &TimeDelta) -> u64 {
    td.as_micros()
}

// -- Ownership encoding helpers (shared by OpenChainOp and ChangeOwnershipOp) --

/// Hashes a single `WeightedOwner(string owner, uint64 weight)` struct.
fn hash_weighted_owner(owner: &str, weight: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(3 * 32);
    buf.extend_from_slice(&*WEIGHTED_OWNER_TYPE_HASH);
    buf.extend_from_slice(&encode_string(owner));
    buf.extend_from_slice(&encode_u64(weight));
    keccak256(&buf).0
}

/// Appends the common ownership fields to an ABI buffer for struct hashing.
fn encode_ownership_fields(
    super_owners: &[String],
    owners: &[(String, u64)],
    multi_leader_rounds: u32,
    open_multi_leader_rounds: bool,
    timeout: &linera_base::ownership::TimeoutConfig,
    buf: &mut Vec<u8>,
) {
    let so_hashes: Vec<[u8; 32]> = super_owners.iter().map(|s| encode_string(s)).collect();
    buf.extend_from_slice(&encode_hash_array(&so_hashes));

    let owner_hashes: Vec<[u8; 32]> = owners
        .iter()
        .map(|(o, w)| hash_weighted_owner(o, *w))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&owner_hashes));

    buf.extend_from_slice(&encode_u32(multi_leader_rounds));
    buf.extend_from_slice(&encode_bool(open_multi_leader_rounds));
    buf.extend_from_slice(&encode_u64(timedelta_micros(&timeout.base_timeout)));
    buf.extend_from_slice(&encode_u64(timedelta_micros(&timeout.timeout_increment)));
    buf.extend_from_slice(&encode_u64(timedelta_micros(&timeout.fallback_duration)));
    buf.extend_from_slice(&encode_u64(
        timeout
            .fast_round_duration
            .map(|d| timedelta_micros(&d))
            .unwrap_or(0),
    ));
}

/// Extracts ownership vectors from `ChangeOwnership` fields.
fn extract_ownership_vecs(
    super_owners: &[AccountOwner],
    owners: &[(AccountOwner, u64)],
) -> (Vec<String>, Vec<(String, u64)>) {
    let so: Vec<String> = super_owners.iter().map(format_account_owner).collect();
    let o: Vec<(String, u64)> = owners
        .iter()
        .map(|(o, w)| (format_account_owner(o), *w))
        .collect();
    (so, o)
}

/// Extracts ownership vectors from a `ChainOwnership` struct.
fn extract_chain_ownership_vecs(
    ownership: &linera_base::ownership::ChainOwnership,
) -> (Vec<String>, Vec<(String, u64)>) {
    let so: Vec<String> = ownership
        .super_owners
        .iter()
        .map(format_account_owner)
        .collect();
    let o: Vec<(String, u64)> = ownership
        .owners
        .iter()
        .map(|(o, w)| (format_account_owner(o), *w))
        .collect();
    (so, o)
}

// -- ApplicationPermissions encoding --

fn hash_app_permissions(perms: &ApplicationPermissions) -> [u8; 32] {
    let mut buf = Vec::with_capacity(10 * 32);
    buf.extend_from_slice(&*CHANGE_APP_PERMISSIONS_OP_TYPE_HASH);
    encode_app_permissions_fields(perms, &mut buf);
    keccak256(&buf).0
}

fn encode_app_permissions_fields(perms: &ApplicationPermissions, buf: &mut Vec<u8>) {
    let has_execute = perms.execute_operations.is_some();
    buf.extend_from_slice(&encode_bool(has_execute));
    let exec_hashes: Vec<[u8; 32]> = perms
        .execute_operations
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&exec_hashes));

    let mandatory_hashes: Vec<[u8; 32]> = perms
        .mandatory_applications
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&mandatory_hashes));

    let close_hashes: Vec<[u8; 32]> = perms
        .close_chain
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&close_hashes));

    let change_hashes: Vec<[u8; 32]> = perms
        .change_application_permissions
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&change_hashes));

    let has_oracle = perms.call_service_as_oracle.is_some();
    buf.extend_from_slice(&encode_bool(has_oracle));
    let oracle_hashes: Vec<[u8; 32]> = perms
        .call_service_as_oracle
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&oracle_hashes));

    let has_http = perms.make_http_requests.is_some();
    buf.extend_from_slice(&encode_bool(has_http));
    let http_hashes: Vec<[u8; 32]> = perms
        .make_http_requests
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&http_hashes));
}

// -- Struct hashing functions --

fn hash_transfer_op(
    owner: &AccountOwner,
    recipient_chain: &ChainId,
    recipient_owner: &AccountOwner,
    amount: Amount,
) -> [u8; 32] {
    let mut buf = Vec::with_capacity(5 * 32);
    buf.extend_from_slice(&*TRANSFER_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_account_owner(owner)));
    buf.extend_from_slice(&encode_string(&format_account(
        recipient_chain,
        recipient_owner,
    )));
    buf.extend_from_slice(&encode_u128(amount.to_attos()));
    keccak256(&buf).0
}

fn hash_claim_op(
    owner: &AccountOwner,
    target_chain: &ChainId,
    recipient_chain: &ChainId,
    recipient_owner: &AccountOwner,
    amount: Amount,
) -> [u8; 32] {
    let mut buf = Vec::with_capacity(6 * 32);
    buf.extend_from_slice(&*CLAIM_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_account_owner(owner)));
    buf.extend_from_slice(&encode_string(&format_chain_id(target_chain)));
    buf.extend_from_slice(&encode_string(&format_account(
        recipient_chain,
        recipient_owner,
    )));
    buf.extend_from_slice(&encode_u128(amount.to_attos()));
    keccak256(&buf).0
}

fn hash_receive_msg(origin: &ChainId, action: &MessageAction, message_count: u32) -> [u8; 32] {
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&*RECEIVE_MSG_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_chain_id(origin)));
    buf.extend_from_slice(&encode_string(format_message_action(action)));
    buf.extend_from_slice(&encode_u32(message_count));
    keccak256(&buf).0
}

fn hash_user_op(application_id: &ApplicationId, data: &[u8]) -> [u8; 32] {
    let data_hash = keccak256(data).0;
    let mut buf = Vec::with_capacity(3 * 32);
    buf.extend_from_slice(&*USER_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_application_id(application_id)));
    buf.extend_from_slice(&encode_bytes32(data_hash));
    keccak256(&buf).0
}

fn hash_open_chain_op(config: &OpenChainConfig) -> [u8; 32] {
    let (so, o) = extract_chain_ownership_vecs(&config.ownership);
    let perms_hash = hash_app_permissions(&config.application_permissions);

    let mut buf = Vec::with_capacity(11 * 32);
    buf.extend_from_slice(&*OPEN_CHAIN_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_u128(config.balance.to_attos()));
    encode_ownership_fields(
        &so,
        &o,
        config.ownership.multi_leader_rounds,
        config.ownership.open_multi_leader_rounds,
        &config.ownership.timeout_config,
        &mut buf,
    );
    buf.extend_from_slice(&encode_bytes32(perms_hash));
    keccak256(&buf).0
}

fn hash_close_chain_op() -> [u8; 32] {
    let buf: Vec<u8> = CLOSE_CHAIN_OP_TYPE_HASH.to_vec();
    keccak256(&buf).0
}

fn hash_change_ownership_op(
    super_owners: &[AccountOwner],
    owners: &[(AccountOwner, u64)],
    multi_leader_rounds: u32,
    open_multi_leader_rounds: bool,
    timeout_config: &linera_base::ownership::TimeoutConfig,
) -> [u8; 32] {
    let (so, o) = extract_ownership_vecs(super_owners, owners);
    let mut buf = Vec::with_capacity(10 * 32);
    buf.extend_from_slice(&*CHANGE_OWNERSHIP_OP_TYPE_HASH);
    encode_ownership_fields(
        &so,
        &o,
        multi_leader_rounds,
        open_multi_leader_rounds,
        timeout_config,
        &mut buf,
    );
    keccak256(&buf).0
}

fn hash_change_app_permissions_op(perms: &ApplicationPermissions) -> [u8; 32] {
    hash_app_permissions(perms)
}

fn hash_publish_module_op(module_id: &ModuleId) -> [u8; 32] {
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&*PUBLISH_MODULE_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_bytes32(module_id.contract_blob_hash.into()));
    buf.extend_from_slice(&encode_bytes32(module_id.service_blob_hash.into()));
    buf.extend_from_slice(&encode_string(format_vm_runtime(module_id)));
    keccak256(&buf).0
}

fn hash_publish_data_blob_op(blob_hash: &CryptoHash) -> [u8; 32] {
    let mut buf = Vec::with_capacity(2 * 32);
    buf.extend_from_slice(&*PUBLISH_DATA_BLOB_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_bytes32((*blob_hash).into()));
    keccak256(&buf).0
}

fn hash_verify_blob_op(blob_type: &BlobType, hash: &CryptoHash) -> [u8; 32] {
    let mut buf = Vec::with_capacity(3 * 32);
    buf.extend_from_slice(&*VERIFY_BLOB_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(format_blob_type(blob_type)));
    buf.extend_from_slice(&encode_bytes32((*hash).into()));
    keccak256(&buf).0
}

fn hash_create_application_op(
    module_id: &ModuleId,
    parameters: &[u8],
    instantiation_argument: &[u8],
    required_application_ids: &[ApplicationId],
) -> [u8; 32] {
    let mut buf = Vec::with_capacity(7 * 32);
    buf.extend_from_slice(&*CREATE_APPLICATION_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_bytes32(module_id.contract_blob_hash.into()));
    buf.extend_from_slice(&encode_bytes32(module_id.service_blob_hash.into()));
    buf.extend_from_slice(&encode_string(format_vm_runtime(module_id)));
    buf.extend_from_slice(&encode_bytes32(keccak256(parameters).0));
    buf.extend_from_slice(&encode_bytes32(keccak256(instantiation_argument).0));
    let req_hashes: Vec<[u8; 32]> = required_application_ids
        .iter()
        .map(|id| encode_string(&format_application_id(id)))
        .collect();
    buf.extend_from_slice(&encode_hash_array(&req_hashes));
    keccak256(&buf).0
}

fn hash_admin_op(admin_op: &AdminOperation) -> [u8; 32] {
    let (operation, epoch, blob_hash) = match admin_op {
        AdminOperation::PublishCommitteeBlob { blob_hash } => {
            ("PublishCommitteeBlob", 0u64, (*blob_hash).into())
        }
        AdminOperation::CreateCommittee { epoch, blob_hash } => {
            ("CreateCommittee", epoch.0 as u64, (*blob_hash).into())
        }
        AdminOperation::RemoveCommittee { epoch } => ("RemoveCommittee", epoch.0 as u64, [0u8; 32]),
    };
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&*ADMIN_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_string(operation));
    buf.extend_from_slice(&encode_u64(epoch));
    buf.extend_from_slice(&encode_bytes32(blob_hash));
    keccak256(&buf).0
}

fn hash_process_new_epoch_op(epoch: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(2 * 32);
    buf.extend_from_slice(&*PROCESS_NEW_EPOCH_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_u64(epoch));
    keccak256(&buf).0
}

fn hash_process_removed_epoch_op(epoch: u64) -> [u8; 32] {
    let mut buf = Vec::with_capacity(2 * 32);
    buf.extend_from_slice(&*PROCESS_REMOVED_EPOCH_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_u64(epoch));
    keccak256(&buf).0
}

fn hash_stream_update(chain_id: &ChainId, stream_id: &str, index: u32) -> [u8; 32] {
    let mut buf = Vec::with_capacity(4 * 32);
    buf.extend_from_slice(&*STREAM_UPDATE_TYPE_HASH);
    buf.extend_from_slice(&encode_string(&format_chain_id(chain_id)));
    buf.extend_from_slice(&encode_string(stream_id));
    buf.extend_from_slice(&encode_u32(index));
    keccak256(&buf).0
}

fn hash_update_streams_op(
    updates: &[(ChainId, linera_base::identifiers::StreamId, u32)],
) -> [u8; 32] {
    let update_hashes: Vec<[u8; 32]> = updates
        .iter()
        .map(|(chain_id, stream_id, index)| {
            let sid = format_stream_id(stream_id);
            hash_stream_update(chain_id, &sid, *index)
        })
        .collect();
    let mut buf = Vec::with_capacity(2 * 32);
    buf.extend_from_slice(&*UPDATE_STREAMS_OP_TYPE_HASH);
    buf.extend_from_slice(&encode_hash_array(&update_hashes));
    keccak256(&buf).0
}

fn format_stream_id(stream_id: &linera_base::identifiers::StreamId) -> String {
    let app = format_generic_app_id(&stream_id.application_id);
    let name = hex::encode(&stream_id.stream_name.0);
    format!("{}:{}", app, name)
}

// -- Categorized transactions --

struct CategorizedTransactions {
    transfers: Vec<[u8; 32]>,
    claims: Vec<[u8; 32]>,
    incoming_messages: Vec<[u8; 32]>,
    user_operations: Vec<[u8; 32]>,
    open_chain_ops: Vec<[u8; 32]>,
    close_chain_ops: Vec<[u8; 32]>,
    change_ownership_ops: Vec<[u8; 32]>,
    change_app_permissions_ops: Vec<[u8; 32]>,
    publish_module_ops: Vec<[u8; 32]>,
    publish_data_blob_ops: Vec<[u8; 32]>,
    verify_blob_ops: Vec<[u8; 32]>,
    create_application_ops: Vec<[u8; 32]>,
    admin_ops: Vec<[u8; 32]>,
    process_new_epoch_ops: Vec<[u8; 32]>,
    process_removed_epoch_ops: Vec<[u8; 32]>,
    update_streams_ops: Vec<[u8; 32]>,
    other_hashes: Vec<[u8; 32]>,
}

impl Default for CategorizedTransactions {
    fn default() -> Self {
        Self {
            transfers: Vec::new(),
            claims: Vec::new(),
            incoming_messages: Vec::new(),
            user_operations: Vec::new(),
            open_chain_ops: Vec::new(),
            close_chain_ops: Vec::new(),
            change_ownership_ops: Vec::new(),
            change_app_permissions_ops: Vec::new(),
            publish_module_ops: Vec::new(),
            publish_data_blob_ops: Vec::new(),
            verify_blob_ops: Vec::new(),
            create_application_ops: Vec::new(),
            admin_ops: Vec::new(),
            process_new_epoch_ops: Vec::new(),
            process_removed_epoch_ops: Vec::new(),
            update_streams_ops: Vec::new(),
            other_hashes: Vec::new(),
        }
    }
}

fn categorize_transactions(transactions: &[Transaction]) -> CategorizedTransactions {
    let mut result = CategorizedTransactions::default();

    for tx in transactions {
        match tx {
            Transaction::ExecuteOperation(Operation::System(sys_op)) => match sys_op.as_ref() {
                SystemOperation::Transfer {
                    owner,
                    recipient,
                    amount,
                } => {
                    result.transfers.push(hash_transfer_op(
                        owner,
                        &recipient.chain_id,
                        &recipient.owner,
                        *amount,
                    ));
                }
                SystemOperation::Claim {
                    owner,
                    target_id,
                    recipient,
                    amount,
                } => {
                    result.claims.push(hash_claim_op(
                        owner,
                        target_id,
                        &recipient.chain_id,
                        &recipient.owner,
                        *amount,
                    ));
                }
                SystemOperation::OpenChain(config) => {
                    result.open_chain_ops.push(hash_open_chain_op(config));
                }
                SystemOperation::CloseChain => {
                    result.close_chain_ops.push(hash_close_chain_op());
                }
                SystemOperation::ChangeOwnership {
                    super_owners,
                    owners,
                    multi_leader_rounds,
                    open_multi_leader_rounds,
                    timeout_config,
                } => {
                    result.change_ownership_ops.push(hash_change_ownership_op(
                        super_owners,
                        owners,
                        *multi_leader_rounds,
                        *open_multi_leader_rounds,
                        timeout_config,
                    ));
                }
                SystemOperation::ChangeApplicationPermissions(perms) => {
                    result
                        .change_app_permissions_ops
                        .push(hash_change_app_permissions_op(perms));
                }
                SystemOperation::PublishModule { module_id } => {
                    result
                        .publish_module_ops
                        .push(hash_publish_module_op(module_id));
                }
                SystemOperation::PublishDataBlob { blob_hash } => {
                    result
                        .publish_data_blob_ops
                        .push(hash_publish_data_blob_op(blob_hash));
                }
                SystemOperation::VerifyBlob { blob_id } => {
                    result
                        .verify_blob_ops
                        .push(hash_verify_blob_op(&blob_id.blob_type, &blob_id.hash));
                }
                SystemOperation::CreateApplication {
                    module_id,
                    parameters,
                    instantiation_argument,
                    required_application_ids,
                } => {
                    result
                        .create_application_ops
                        .push(hash_create_application_op(
                            module_id,
                            parameters,
                            instantiation_argument,
                            required_application_ids,
                        ));
                }
                SystemOperation::Admin(admin_op) => {
                    result.admin_ops.push(hash_admin_op(admin_op));
                }
                SystemOperation::ProcessNewEpoch(epoch) => {
                    result
                        .process_new_epoch_ops
                        .push(hash_process_new_epoch_op(epoch.0 as u64));
                }
                SystemOperation::ProcessRemovedEpoch(epoch) => {
                    result
                        .process_removed_epoch_ops
                        .push(hash_process_removed_epoch_op(epoch.0 as u64));
                }
                SystemOperation::UpdateStreams(updates) => {
                    result
                        .update_streams_ops
                        .push(hash_update_streams_op(updates));
                }
            },
            Transaction::ExecuteOperation(Operation::User {
                application_id,
                bytes,
            }) => {
                result
                    .user_operations
                    .push(hash_user_op(application_id, bytes));
            }
            Transaction::ReceiveMessages(bundle) => {
                result.incoming_messages.push(hash_receive_msg(
                    &bundle.origin,
                    &bundle.action,
                    bundle.bundle.messages.len() as u32,
                ));
            }
        }
    }

    result
}

/// Computes the EIP-712 signing hash for a `ProposalContent`.
///
/// Returns `keccak256("\x19\x01" || domainSeparator || hashStruct(proposal))`.
pub fn eip712_signing_hash(content: &ProposalContent) -> [u8; 32] {
    let block = &content.block;
    let cat = categorize_transactions(&block.transactions);

    let content_hash: [u8; 32] = CryptoHash::new(content).into();

    let mut buf = Vec::with_capacity(24 * 32);
    buf.extend_from_slice(&*PROPOSAL_TYPE_HASH);
    buf.extend_from_slice(&encode_bytes32(block.chain_id.0.into()));
    buf.extend_from_slice(&encode_u64(block.epoch.0 as u64));
    buf.extend_from_slice(&encode_u64(block.height.0));
    buf.extend_from_slice(&encode_u64(block.timestamp.micros()));
    buf.extend_from_slice(&encode_string(&format_round(&content.round)));
    buf.extend_from_slice(&encode_bytes32(content_hash));
    buf.extend_from_slice(&encode_hash_array(&cat.transfers));
    buf.extend_from_slice(&encode_hash_array(&cat.claims));
    buf.extend_from_slice(&encode_hash_array(&cat.incoming_messages));
    buf.extend_from_slice(&encode_hash_array(&cat.user_operations));
    buf.extend_from_slice(&encode_hash_array(&cat.open_chain_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.close_chain_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.change_ownership_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.change_app_permissions_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.publish_module_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.publish_data_blob_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.verify_blob_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.create_application_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.admin_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.process_new_epoch_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.process_removed_epoch_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.update_streams_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.other_hashes));
    let struct_hash = keccak256(&buf).0;

    let mut final_buf = Vec::with_capacity(2 + 32 + 32);
    final_buf.extend_from_slice(b"\x19\x01");
    final_buf.extend_from_slice(&*DOMAIN_SEPARATOR);
    final_buf.extend_from_slice(&struct_hash);
    keccak256(&final_buf).0
}

// -- JSON typed data builder --

/// Categorized transaction values for JSON serialization.
struct CategorizedValues {
    transfers: Vec<serde_json::Value>,
    claims: Vec<serde_json::Value>,
    incoming_messages: Vec<serde_json::Value>,
    user_operations: Vec<serde_json::Value>,
    open_chain_ops: Vec<serde_json::Value>,
    close_chain_ops: Vec<serde_json::Value>,
    change_ownership_ops: Vec<serde_json::Value>,
    change_app_permissions_ops: Vec<serde_json::Value>,
    publish_module_ops: Vec<serde_json::Value>,
    publish_data_blob_ops: Vec<serde_json::Value>,
    verify_blob_ops: Vec<serde_json::Value>,
    create_application_ops: Vec<serde_json::Value>,
    admin_ops: Vec<serde_json::Value>,
    process_new_epoch_ops: Vec<serde_json::Value>,
    process_removed_epoch_ops: Vec<serde_json::Value>,
    update_streams_ops: Vec<serde_json::Value>,
    other_hashes: Vec<serde_json::Value>,
}

impl Default for CategorizedValues {
    fn default() -> Self {
        Self {
            transfers: Vec::new(),
            claims: Vec::new(),
            incoming_messages: Vec::new(),
            user_operations: Vec::new(),
            open_chain_ops: Vec::new(),
            close_chain_ops: Vec::new(),
            change_ownership_ops: Vec::new(),
            change_app_permissions_ops: Vec::new(),
            publish_module_ops: Vec::new(),
            publish_data_blob_ops: Vec::new(),
            verify_blob_ops: Vec::new(),
            create_application_ops: Vec::new(),
            admin_ops: Vec::new(),
            process_new_epoch_ops: Vec::new(),
            process_removed_epoch_ops: Vec::new(),
            update_streams_ops: Vec::new(),
            other_hashes: Vec::new(),
        }
    }
}

fn ownership_json(
    super_owners: &[String],
    owners: &[(String, u64)],
    multi_leader_rounds: u32,
    open_multi_leader_rounds: bool,
    timeout: &linera_base::ownership::TimeoutConfig,
) -> serde_json::Value {
    let owners_json: Vec<serde_json::Value> = owners
        .iter()
        .map(|(o, w)| serde_json::json!({"owner": o, "weight": w}))
        .collect();
    serde_json::json!({
        "superOwners": super_owners,
        "owners": owners_json,
        "multiLeaderRounds": multi_leader_rounds,
        "openMultiLeaderRounds": open_multi_leader_rounds,
        "baseTimeoutMicros": timedelta_micros(&timeout.base_timeout),
        "timeoutIncrementMicros": timedelta_micros(&timeout.timeout_increment),
        "fallbackDurationMicros": timedelta_micros(&timeout.fallback_duration),
        "fastRoundDurationMicros": timeout.fast_round_duration
            .map(|d| timedelta_micros(&d))
            .unwrap_or(0),
    })
}

fn app_permissions_json(perms: &ApplicationPermissions) -> serde_json::Value {
    let exec_ops: Vec<String> = perms
        .execute_operations
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(format_application_id)
        .collect();
    let mandatory: Vec<String> = perms
        .mandatory_applications
        .iter()
        .map(format_application_id)
        .collect();
    let close: Vec<String> = perms
        .close_chain
        .iter()
        .map(format_application_id)
        .collect();
    let change: Vec<String> = perms
        .change_application_permissions
        .iter()
        .map(format_application_id)
        .collect();
    let oracle: Vec<String> = perms
        .call_service_as_oracle
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(format_application_id)
        .collect();
    let http: Vec<String> = perms
        .make_http_requests
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(format_application_id)
        .collect();

    serde_json::json!({
        "hasExecuteFilter": perms.execute_operations.is_some(),
        "executeOperations": exec_ops,
        "mandatoryApplications": mandatory,
        "closeChainApps": close,
        "changePermissionsApps": change,
        "hasOracleFilter": perms.call_service_as_oracle.is_some(),
        "oracleApps": oracle,
        "hasHttpFilter": perms.make_http_requests.is_some(),
        "httpApps": http,
    })
}

fn categorize_transactions_values(transactions: &[Transaction]) -> CategorizedValues {
    let mut result = CategorizedValues::default();

    for tx in transactions {
        match tx {
            Transaction::ExecuteOperation(Operation::System(sys_op)) => match sys_op.as_ref() {
                SystemOperation::Transfer {
                    owner,
                    recipient,
                    amount,
                } => {
                    result.transfers.push(serde_json::json!({
                        "owner": format_account_owner(owner),
                        "recipient": format_account(&recipient.chain_id, &recipient.owner),
                        "amount": amount.to_attos().to_string(),
                    }));
                }
                SystemOperation::Claim {
                    owner,
                    target_id,
                    recipient,
                    amount,
                } => {
                    result.claims.push(serde_json::json!({
                        "owner": format_account_owner(owner),
                        "targetChain": format_chain_id(target_id),
                        "recipient": format_account(&recipient.chain_id, &recipient.owner),
                        "amount": amount.to_attos().to_string(),
                    }));
                }
                SystemOperation::OpenChain(config) => {
                    let (so, o) = extract_chain_ownership_vecs(&config.ownership);
                    let perms_hash = hash_app_permissions(&config.application_permissions);
                    let mut val = serde_json::json!({
                        "balance": config.balance.to_attos().to_string(),
                        "permissionsHash": format_bytes32(perms_hash),
                    });
                    let ownership = ownership_json(
                        &so,
                        &o,
                        config.ownership.multi_leader_rounds,
                        config.ownership.open_multi_leader_rounds,
                        &config.ownership.timeout_config,
                    );
                    // Merge ownership fields into val.
                    if let (Some(v), Some(ow)) = (val.as_object_mut(), ownership.as_object()) {
                        for (k, v2) in ow {
                            v.insert(k.clone(), v2.clone());
                        }
                    }
                    result.open_chain_ops.push(val);
                }
                SystemOperation::CloseChain => {
                    result.close_chain_ops.push(serde_json::json!({}));
                }
                SystemOperation::ChangeOwnership {
                    super_owners,
                    owners,
                    multi_leader_rounds,
                    open_multi_leader_rounds,
                    timeout_config,
                } => {
                    let (so, o) = extract_ownership_vecs(super_owners, owners);
                    result.change_ownership_ops.push(ownership_json(
                        &so,
                        &o,
                        *multi_leader_rounds,
                        *open_multi_leader_rounds,
                        timeout_config,
                    ));
                }
                SystemOperation::ChangeApplicationPermissions(perms) => {
                    result
                        .change_app_permissions_ops
                        .push(app_permissions_json(perms));
                }
                SystemOperation::PublishModule { module_id } => {
                    result.publish_module_ops.push(serde_json::json!({
                        "contractBlobHash": format_bytes32(module_id.contract_blob_hash.into()),
                        "serviceBlobHash": format_bytes32(module_id.service_blob_hash.into()),
                        "vmRuntime": format_vm_runtime(module_id),
                    }));
                }
                SystemOperation::PublishDataBlob { blob_hash } => {
                    result.publish_data_blob_ops.push(serde_json::json!({
                        "blobHash": format_bytes32((*blob_hash).into()),
                    }));
                }
                SystemOperation::VerifyBlob { blob_id } => {
                    result.verify_blob_ops.push(serde_json::json!({
                        "blobType": format_blob_type(&blob_id.blob_type),
                        "hash": format_bytes32(blob_id.hash.into()),
                    }));
                }
                SystemOperation::CreateApplication {
                    module_id,
                    parameters,
                    instantiation_argument,
                    required_application_ids,
                } => {
                    let req_ids: Vec<String> = required_application_ids
                        .iter()
                        .map(format_application_id)
                        .collect();
                    result.create_application_ops.push(serde_json::json!({
                        "contractBlobHash": format_bytes32(module_id.contract_blob_hash.into()),
                        "serviceBlobHash": format_bytes32(module_id.service_blob_hash.into()),
                        "vmRuntime": format_vm_runtime(module_id),
                        "parametersHash": format_bytes32(keccak256(parameters).0),
                        "instantiationArgumentHash": format_bytes32(keccak256(instantiation_argument).0),
                        "requiredApplicationIds": req_ids,
                    }));
                }
                SystemOperation::Admin(admin_op) => {
                    let (operation, epoch, blob_hash) = match admin_op {
                        AdminOperation::PublishCommitteeBlob { blob_hash } => (
                            "PublishCommitteeBlob",
                            0u64,
                            format_bytes32((*blob_hash).into()),
                        ),
                        AdminOperation::CreateCommittee { epoch, blob_hash } => (
                            "CreateCommittee",
                            epoch.0 as u64,
                            format_bytes32((*blob_hash).into()),
                        ),
                        AdminOperation::RemoveCommittee { epoch } => {
                            ("RemoveCommittee", epoch.0 as u64, format_bytes32([0u8; 32]))
                        }
                    };
                    result.admin_ops.push(serde_json::json!({
                        "operation": operation,
                        "epoch": epoch,
                        "blobHash": blob_hash,
                    }));
                }
                SystemOperation::ProcessNewEpoch(epoch) => {
                    result.process_new_epoch_ops.push(serde_json::json!({
                        "epoch": epoch.0 as u64,
                    }));
                }
                SystemOperation::ProcessRemovedEpoch(epoch) => {
                    result.process_removed_epoch_ops.push(serde_json::json!({
                        "epoch": epoch.0 as u64,
                    }));
                }
                SystemOperation::UpdateStreams(updates) => {
                    let update_vals: Vec<serde_json::Value> = updates
                        .iter()
                        .map(|(chain_id, stream_id, index)| {
                            serde_json::json!({
                                "chainId": format_chain_id(chain_id),
                                "streamId": format_stream_id(stream_id),
                                "index": *index,
                            })
                        })
                        .collect();
                    result.update_streams_ops.push(serde_json::json!({
                        "updates": update_vals,
                    }));
                }
            },
            Transaction::ExecuteOperation(Operation::User {
                application_id,
                bytes,
            }) => {
                let data_hash = keccak256(bytes).0;
                result.user_operations.push(serde_json::json!({
                    "applicationId": format_application_id(application_id),
                    "dataHash": format_bytes32(data_hash),
                }));
            }
            Transaction::ReceiveMessages(bundle) => {
                result.incoming_messages.push(serde_json::json!({
                    "origin": format_chain_id(&bundle.origin),
                    "action": format_message_action(&bundle.action),
                    "messageCount": bundle.bundle.messages.len() as u32,
                }));
            }
        }
    }

    result
}

/// Produces the full EIP-712 typed data JSON for a `ProposalContent`.
///
/// The JSON can be sent to MetaMask via `eth_signTypedData_v4`, or parsed by the
/// generic `compute_eip712_hash` in `linera-base` to produce the signing hash.
pub fn eip712_typed_data_json(content: &ProposalContent) -> String {
    let block = &content.block;
    let cat = categorize_transactions_values(&block.transactions);
    let content_hash: [u8; 32] = CryptoHash::new(content).into();

    let typed_data = serde_json::json!({
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"}
            ],
            "LineraBlockProposal": [
                {"name": "chainId", "type": "bytes32"},
                {"name": "epoch", "type": "uint64"},
                {"name": "height", "type": "uint64"},
                {"name": "timestamp", "type": "uint64"},
                {"name": "round", "type": "string"},
                {"name": "contentHash", "type": "bytes32"},
                {"name": "transfers", "type": "TransferOp[]"},
                {"name": "claims", "type": "ClaimOp[]"},
                {"name": "incomingMessages", "type": "ReceiveMsg[]"},
                {"name": "userOperations", "type": "UserOp[]"},
                {"name": "openChainOps", "type": "OpenChainOp[]"},
                {"name": "closeChainOps", "type": "CloseChainOp[]"},
                {"name": "changeOwnershipOps", "type": "ChangeOwnershipOp[]"},
                {"name": "changeAppPermissionsOps", "type": "ChangeAppPermissionsOp[]"},
                {"name": "publishModuleOps", "type": "PublishModuleOp[]"},
                {"name": "publishDataBlobOps", "type": "PublishDataBlobOp[]"},
                {"name": "verifyBlobOps", "type": "VerifyBlobOp[]"},
                {"name": "createApplicationOps", "type": "CreateApplicationOp[]"},
                {"name": "adminOps", "type": "AdminOp[]"},
                {"name": "processNewEpochOps", "type": "ProcessNewEpochOp[]"},
                {"name": "processRemovedEpochOps", "type": "ProcessRemovedEpochOp[]"},
                {"name": "updateStreamsOps", "type": "UpdateStreamsOp[]"},
                {"name": "otherTransactionHashes", "type": "bytes32[]"}
            ],
            "TransferOp": [
                {"name": "owner", "type": "string"},
                {"name": "recipient", "type": "string"},
                {"name": "amount", "type": "uint128"}
            ],
            "ClaimOp": [
                {"name": "owner", "type": "string"},
                {"name": "targetChain", "type": "string"},
                {"name": "recipient", "type": "string"},
                {"name": "amount", "type": "uint128"}
            ],
            "ReceiveMsg": [
                {"name": "origin", "type": "string"},
                {"name": "action", "type": "string"},
                {"name": "messageCount", "type": "uint32"}
            ],
            "UserOp": [
                {"name": "applicationId", "type": "string"},
                {"name": "dataHash", "type": "bytes32"}
            ],
            "OpenChainOp": [
                {"name": "balance", "type": "uint128"},
                {"name": "superOwners", "type": "string[]"},
                {"name": "owners", "type": "WeightedOwner[]"},
                {"name": "multiLeaderRounds", "type": "uint32"},
                {"name": "openMultiLeaderRounds", "type": "bool"},
                {"name": "baseTimeoutMicros", "type": "uint64"},
                {"name": "timeoutIncrementMicros", "type": "uint64"},
                {"name": "fallbackDurationMicros", "type": "uint64"},
                {"name": "fastRoundDurationMicros", "type": "uint64"},
                {"name": "permissionsHash", "type": "bytes32"}
            ],
            "CloseChainOp": [],
            "ChangeOwnershipOp": [
                {"name": "superOwners", "type": "string[]"},
                {"name": "owners", "type": "WeightedOwner[]"},
                {"name": "multiLeaderRounds", "type": "uint32"},
                {"name": "openMultiLeaderRounds", "type": "bool"},
                {"name": "baseTimeoutMicros", "type": "uint64"},
                {"name": "timeoutIncrementMicros", "type": "uint64"},
                {"name": "fallbackDurationMicros", "type": "uint64"},
                {"name": "fastRoundDurationMicros", "type": "uint64"}
            ],
            "WeightedOwner": [
                {"name": "owner", "type": "string"},
                {"name": "weight", "type": "uint64"}
            ],
            "ChangeAppPermissionsOp": [
                {"name": "hasExecuteFilter", "type": "bool"},
                {"name": "executeOperations", "type": "string[]"},
                {"name": "mandatoryApplications", "type": "string[]"},
                {"name": "closeChainApps", "type": "string[]"},
                {"name": "changePermissionsApps", "type": "string[]"},
                {"name": "hasOracleFilter", "type": "bool"},
                {"name": "oracleApps", "type": "string[]"},
                {"name": "hasHttpFilter", "type": "bool"},
                {"name": "httpApps", "type": "string[]"}
            ],
            "PublishModuleOp": [
                {"name": "contractBlobHash", "type": "bytes32"},
                {"name": "serviceBlobHash", "type": "bytes32"},
                {"name": "vmRuntime", "type": "string"}
            ],
            "PublishDataBlobOp": [
                {"name": "blobHash", "type": "bytes32"}
            ],
            "VerifyBlobOp": [
                {"name": "blobType", "type": "string"},
                {"name": "hash", "type": "bytes32"}
            ],
            "CreateApplicationOp": [
                {"name": "contractBlobHash", "type": "bytes32"},
                {"name": "serviceBlobHash", "type": "bytes32"},
                {"name": "vmRuntime", "type": "string"},
                {"name": "parametersHash", "type": "bytes32"},
                {"name": "instantiationArgumentHash", "type": "bytes32"},
                {"name": "requiredApplicationIds", "type": "string[]"}
            ],
            "AdminOp": [
                {"name": "operation", "type": "string"},
                {"name": "epoch", "type": "uint64"},
                {"name": "blobHash", "type": "bytes32"}
            ],
            "ProcessNewEpochOp": [
                {"name": "epoch", "type": "uint64"}
            ],
            "ProcessRemovedEpochOp": [
                {"name": "epoch", "type": "uint64"}
            ],
            "UpdateStreamsOp": [
                {"name": "updates", "type": "StreamUpdate[]"}
            ],
            "StreamUpdate": [
                {"name": "chainId", "type": "string"},
                {"name": "streamId", "type": "string"},
                {"name": "index", "type": "uint32"}
            ]
        },
        "primaryType": "LineraBlockProposal",
        "domain": {
            "name": "Linera",
            "version": "1"
        },
        "message": {
            "chainId": format_bytes32(block.chain_id.0.into()),
            "epoch": block.epoch.0 as u64,
            "height": block.height.0,
            "timestamp": block.timestamp.micros(),
            "round": format_round(&content.round),
            "contentHash": format_bytes32(content_hash),
            "transfers": cat.transfers,
            "claims": cat.claims,
            "incomingMessages": cat.incoming_messages,
            "userOperations": cat.user_operations,
            "openChainOps": cat.open_chain_ops,
            "closeChainOps": cat.close_chain_ops,
            "changeOwnershipOps": cat.change_ownership_ops,
            "changeAppPermissionsOps": cat.change_app_permissions_ops,
            "publishModuleOps": cat.publish_module_ops,
            "publishDataBlobOps": cat.publish_data_blob_ops,
            "verifyBlobOps": cat.verify_blob_ops,
            "createApplicationOps": cat.create_application_ops,
            "adminOps": cat.admin_ops,
            "processNewEpochOps": cat.process_new_epoch_ops,
            "processRemovedEpochOps": cat.process_removed_epoch_ops,
            "updateStreamsOps": cat.update_streams_ops,
            "otherTransactionHashes": cat.other_hashes,
        }
    });

    typed_data.to_string()
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::{BlockHeight, Epoch, Round},
        identifiers::{Account, AccountOwner, ChainId},
    };

    use super::*;
    use crate::data_types::{ProposalContent, ProposedBlock};

    fn test_chain_id() -> ChainId {
        ChainId(CryptoHash::new(&TestString::new("ChainId")))
    }

    fn empty_proposal() -> ProposalContent {
        ProposalContent {
            block: ProposedBlock {
                chain_id: test_chain_id(),
                epoch: Epoch(11),
                transactions: vec![],
                height: BlockHeight(11),
                timestamp: 190000000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::SingleLeader(11),
            outcome: None,
        }
    }

    #[test]
    fn test_domain_separator_deterministic() {
        let sep1 = *DOMAIN_SEPARATOR;
        let sep2 = *DOMAIN_SEPARATOR;
        assert_eq!(sep1, sep2);
        assert_ne!(sep1, [0u8; 32]);
    }

    #[test]
    fn test_type_hash_strings_sorted() {
        let type_str = LINERA_BLOCK_PROPOSAL_TYPE;
        let after_primary = type_str.find(')').unwrap() + 1;
        let referenced = &type_str[after_primary..];
        let type_names: Vec<&str> = referenced
            .split(')')
            .filter(|s| !s.is_empty())
            .map(|s| s.split('(').next().unwrap())
            .collect();
        let mut sorted = type_names.clone();
        sorted.sort();
        assert_eq!(type_names, sorted);
    }

    #[test]
    fn test_empty_proposal_hash() {
        let proposal = empty_proposal();
        let hash = eip712_signing_hash(&proposal);
        assert_ne!(hash, [0u8; 32]);
    }

    #[test]
    fn test_full_proposal_deterministic() {
        let proposal = empty_proposal();
        let hash1 = eip712_signing_hash(&proposal);
        let hash2 = eip712_signing_hash(&proposal);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_proposals_different_hashes() {
        let proposal1 = empty_proposal();
        let mut proposal2 = empty_proposal();
        proposal2.block.height = BlockHeight(12);
        let hash1 = eip712_signing_hash(&proposal1);
        let hash2 = eip712_signing_hash(&proposal2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_transfer_op_encoding() {
        let owner = AccountOwner::Address20([0xAB; 20]);
        let recipient_chain = test_chain_id();
        let recipient_owner = AccountOwner::Address20([0xCD; 20]);
        let amount = Amount::from_tokens(100);
        let hash1 = hash_transfer_op(&owner, &recipient_chain, &recipient_owner, amount);
        let hash2 = hash_transfer_op(&owner, &recipient_chain, &recipient_owner, amount);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, [0u8; 32]);

        let hash3 = hash_transfer_op(
            &owner,
            &recipient_chain,
            &recipient_owner,
            Amount::from_tokens(200),
        );
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_proposal_with_transfer() {
        use linera_execution::Operation;

        let chain_id = test_chain_id();
        let owner = AccountOwner::Address20([0xAB; 20]);
        let recipient = Account {
            chain_id,
            owner: AccountOwner::Address20([0xCD; 20]),
        };

        let transfer =
            Transaction::ExecuteOperation(Operation::System(Box::new(SystemOperation::Transfer {
                owner,
                recipient,
                amount: Amount::from_tokens(50),
            })));

        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id,
                epoch: Epoch(1),
                transactions: vec![transfer],
                height: BlockHeight(0),
                timestamp: 1000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::Fast,
            outcome: None,
        };

        let hash = eip712_signing_hash(&proposal);
        let empty_hash = eip712_signing_hash(&empty_proposal());
        assert_ne!(hash, empty_hash);
    }

    #[test]
    fn test_json_hash_matches_direct_hash_empty() {
        let proposal = empty_proposal();
        let direct_hash = eip712_signing_hash(&proposal);
        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);
        assert_eq!(direct_hash, json_hash);
    }

    #[test]
    fn test_json_hash_matches_direct_hash_with_transfer() {
        use linera_execution::Operation;

        let chain_id = test_chain_id();
        let owner = AccountOwner::Address20([0xAB; 20]);
        let recipient = Account {
            chain_id,
            owner: AccountOwner::Address20([0xCD; 20]),
        };

        let transfer =
            Transaction::ExecuteOperation(Operation::System(Box::new(SystemOperation::Transfer {
                owner,
                recipient,
                amount: Amount::from_tokens(50),
            })));

        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id,
                epoch: Epoch(1),
                transactions: vec![transfer],
                height: BlockHeight(0),
                timestamp: 1000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::Fast,
            outcome: None,
        };

        let direct_hash = eip712_signing_hash(&proposal);
        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);
        assert_eq!(direct_hash, json_hash);
    }

    #[test]
    fn test_json_hash_matches_close_chain() {
        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id: test_chain_id(),
                epoch: Epoch(1),
                transactions: vec![Transaction::ExecuteOperation(Operation::System(Box::new(
                    SystemOperation::CloseChain,
                )))],
                height: BlockHeight(0),
                timestamp: 1000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::Fast,
            outcome: None,
        };

        let direct_hash = eip712_signing_hash(&proposal);
        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);
        assert_eq!(direct_hash, json_hash);
    }

    #[test]
    fn test_json_hash_matches_publish_data_blob() {
        let blob_hash = CryptoHash::new(&TestString::new("blob"));
        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id: test_chain_id(),
                epoch: Epoch(1),
                transactions: vec![Transaction::ExecuteOperation(Operation::System(Box::new(
                    SystemOperation::PublishDataBlob { blob_hash },
                )))],
                height: BlockHeight(0),
                timestamp: 1000u64.into(),
                authenticated_signer: None,
                previous_block_hash: None,
            },
            round: Round::Fast,
            outcome: None,
        };

        let direct_hash = eip712_signing_hash(&proposal);
        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);
        assert_eq!(direct_hash, json_hash);
    }
}
