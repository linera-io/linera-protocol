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
    data_types::{ApplicationPermissions, Round, TimeDelta},
    identifiers::{AccountOwner, ApplicationId, BlobType, ChainId, GenericApplicationId, ModuleId},
};
use linera_eip712_derive::Eip712Struct;
use linera_execution::{system::AdminOperation, Operation, SystemOperation};

use super::{MessageAction, ProposalContent, Transaction};

// -- Eip712Struct trait --

pub(crate) trait Eip712Struct {
    const PRIMARY_DEF: &'static str;
    fn eip712_type_hash() -> &'static [u8; 32];
    fn hash_struct(&self) -> [u8; 32];
    fn to_eip712_json(&self) -> serde_json::Value;
    fn eip712_json_type_def() -> serde_json::Value;
}

// -- Derive structs --

#[derive(Eip712Struct)]
#[eip712(name = "TransferOp")]
struct TransferOpEip712 {
    #[eip712(soltype = "string")]
    owner: String,
    #[eip712(soltype = "string")]
    recipient: String,
    #[eip712(soltype = "uint128")]
    amount: u128,
}

#[derive(Eip712Struct)]
#[eip712(name = "ClaimOp")]
struct ClaimOpEip712 {
    #[eip712(soltype = "string")]
    owner: String,
    #[eip712(soltype = "string")]
    target_chain: String,
    #[eip712(soltype = "string")]
    recipient: String,
    #[eip712(soltype = "uint128")]
    amount: u128,
}

#[derive(Eip712Struct)]
#[eip712(name = "ReceiveMsg")]
struct ReceiveMsgEip712 {
    #[eip712(soltype = "string")]
    origin: String,
    #[eip712(soltype = "string")]
    action: String,
    #[eip712(soltype = "uint32")]
    message_count: u32,
}

#[derive(Eip712Struct)]
#[eip712(name = "UserOp")]
struct UserOpEip712 {
    #[eip712(soltype = "string")]
    application_id: String,
    #[eip712(soltype = "bytes32")]
    data_hash: [u8; 32],
}

#[derive(Eip712Struct)]
#[eip712(name = "WeightedOwner")]
struct WeightedOwnerEip712 {
    #[eip712(soltype = "string")]
    owner: String,
    #[eip712(soltype = "uint64")]
    weight: u64,
}

#[derive(Eip712Struct)]
#[eip712(name = "CloseChainOp")]
struct CloseChainOpEip712 {}

#[derive(Eip712Struct)]
#[eip712(name = "OpenChainOp", references(WeightedOwnerEip712))]
struct OpenChainOpEip712 {
    #[eip712(soltype = "uint128")]
    balance: u128,
    #[eip712(soltype = "string[]")]
    super_owners: Vec<String>,
    #[eip712(soltype = "WeightedOwner[]")]
    owners: Vec<WeightedOwnerEip712>,
    #[eip712(soltype = "uint32")]
    multi_leader_rounds: u32,
    #[eip712(soltype = "bool")]
    open_multi_leader_rounds: bool,
    #[eip712(soltype = "uint64")]
    base_timeout_micros: u64,
    #[eip712(soltype = "uint64")]
    timeout_increment_micros: u64,
    #[eip712(soltype = "uint64")]
    fallback_duration_micros: u64,
    #[eip712(soltype = "uint64")]
    fast_round_duration_micros: u64,
    #[eip712(soltype = "bytes32")]
    permissions_hash: [u8; 32],
}

#[derive(Eip712Struct)]
#[eip712(name = "ChangeOwnershipOp", references(WeightedOwnerEip712))]
struct ChangeOwnershipOpEip712 {
    #[eip712(soltype = "string[]")]
    super_owners: Vec<String>,
    #[eip712(soltype = "WeightedOwner[]")]
    owners: Vec<WeightedOwnerEip712>,
    #[eip712(soltype = "uint32")]
    multi_leader_rounds: u32,
    #[eip712(soltype = "bool")]
    open_multi_leader_rounds: bool,
    #[eip712(soltype = "uint64")]
    base_timeout_micros: u64,
    #[eip712(soltype = "uint64")]
    timeout_increment_micros: u64,
    #[eip712(soltype = "uint64")]
    fallback_duration_micros: u64,
    #[eip712(soltype = "uint64")]
    fast_round_duration_micros: u64,
}

#[derive(Eip712Struct)]
#[eip712(name = "ChangeAppPermissionsOp")]
struct ChangeAppPermissionsOpEip712 {
    #[eip712(soltype = "bool")]
    has_execute_filter: bool,
    #[eip712(soltype = "string[]")]
    execute_operations: Vec<String>,
    #[eip712(soltype = "string[]")]
    mandatory_applications: Vec<String>,
    #[eip712(soltype = "string[]")]
    manage_chain_apps: Vec<String>,
    #[eip712(soltype = "bool")]
    has_oracle_filter: bool,
    #[eip712(soltype = "string[]")]
    oracle_apps: Vec<String>,
    #[eip712(soltype = "bool")]
    has_http_filter: bool,
    #[eip712(soltype = "string[]")]
    http_apps: Vec<String>,
}

#[derive(Eip712Struct)]
#[eip712(name = "PublishModuleOp")]
struct PublishModuleOpEip712 {
    #[eip712(soltype = "bytes32")]
    contract_blob_hash: [u8; 32],
    #[eip712(soltype = "bytes32")]
    service_blob_hash: [u8; 32],
    #[eip712(soltype = "string")]
    vm_runtime: String,
}

#[derive(Eip712Struct)]
#[eip712(name = "PublishDataBlobOp")]
struct PublishDataBlobOpEip712 {
    #[eip712(soltype = "bytes32")]
    blob_hash: [u8; 32],
}

#[derive(Eip712Struct)]
#[eip712(name = "VerifyBlobOp")]
struct VerifyBlobOpEip712 {
    #[eip712(soltype = "string")]
    blob_type: String,
    #[eip712(soltype = "bytes32")]
    hash: [u8; 32],
}

#[derive(Eip712Struct)]
#[eip712(name = "CreateApplicationOp")]
struct CreateApplicationOpEip712 {
    #[eip712(soltype = "bytes32")]
    contract_blob_hash: [u8; 32],
    #[eip712(soltype = "bytes32")]
    service_blob_hash: [u8; 32],
    #[eip712(soltype = "string")]
    vm_runtime: String,
    #[eip712(soltype = "bytes32")]
    parameters_hash: [u8; 32],
    #[eip712(soltype = "bytes32")]
    instantiation_argument_hash: [u8; 32],
    #[eip712(soltype = "string[]")]
    required_application_ids: Vec<String>,
}

#[derive(Eip712Struct)]
#[eip712(name = "AdminOp")]
struct AdminOpEip712 {
    #[eip712(soltype = "string")]
    operation: String,
    #[eip712(soltype = "uint64")]
    epoch: u64,
    #[eip712(soltype = "bytes32")]
    blob_hash: [u8; 32],
}

#[derive(Eip712Struct)]
#[eip712(name = "ProcessNewEpochOp")]
struct ProcessNewEpochOpEip712 {
    #[eip712(soltype = "uint64")]
    epoch: u64,
}

#[derive(Eip712Struct)]
#[eip712(name = "ProcessRemovedEpochOp")]
struct ProcessRemovedEpochOpEip712 {
    #[eip712(soltype = "uint64")]
    epoch: u64,
}

#[derive(Eip712Struct)]
#[eip712(name = "StreamUpdate")]
struct StreamUpdateEip712 {
    #[eip712(soltype = "string")]
    chain_id: String,
    #[eip712(soltype = "string")]
    stream_id: String,
    #[eip712(soltype = "uint32")]
    index: u32,
}

#[derive(Eip712Struct)]
#[eip712(name = "UpdateStreamsOp", references(StreamUpdateEip712))]
struct UpdateStreamsOpEip712 {
    #[eip712(soltype = "StreamUpdate[]")]
    updates: Vec<StreamUpdateEip712>,
}

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
string[] mandatoryApplications,string[] manageChainApps,\
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

// -- Precomputed type hashes --

static DOMAIN_TYPE_HASH: LazyLock<[u8; 32]> = LazyLock::new(|| keccak256(EIP712_DOMAIN_TYPE).0);
static PROPOSAL_TYPE_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| keccak256(LINERA_BLOCK_PROPOSAL_TYPE).0);

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

// -- Ownership extraction helpers --

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

fn format_stream_id(stream_id: &linera_base::identifiers::StreamId) -> String {
    let app = format_generic_app_id(&stream_id.application_id);
    let name = hex::encode(&stream_id.stream_name.0);
    format!("{}:{}", app, name)
}

// -- Categorized transactions --

#[derive(Default)]
struct CategorizedTransactions {
    transfers: Vec<TransferOpEip712>,
    claims: Vec<ClaimOpEip712>,
    incoming_messages: Vec<ReceiveMsgEip712>,
    user_operations: Vec<UserOpEip712>,
    open_chain_ops: Vec<OpenChainOpEip712>,
    close_chain_ops: Vec<CloseChainOpEip712>,
    change_ownership_ops: Vec<ChangeOwnershipOpEip712>,
    change_app_permissions_ops: Vec<ChangeAppPermissionsOpEip712>,
    publish_module_ops: Vec<PublishModuleOpEip712>,
    publish_data_blob_ops: Vec<PublishDataBlobOpEip712>,
    verify_blob_ops: Vec<VerifyBlobOpEip712>,
    create_application_ops: Vec<CreateApplicationOpEip712>,
    admin_ops: Vec<AdminOpEip712>,
    process_new_epoch_ops: Vec<ProcessNewEpochOpEip712>,
    process_removed_epoch_ops: Vec<ProcessRemovedEpochOpEip712>,
    update_streams_ops: Vec<UpdateStreamsOpEip712>,
    other_hashes: Vec<[u8; 32]>,
}

fn make_ownership_eip712(
    ownership: &linera_base::ownership::ChainOwnership,
) -> (
    Vec<String>,
    Vec<WeightedOwnerEip712>,
    u32,
    bool,
    u64,
    u64,
    u64,
    u64,
) {
    let (so, o) = extract_chain_ownership_vecs(ownership);
    let owners = o
        .iter()
        .map(|(owner, weight)| WeightedOwnerEip712 {
            owner: owner.clone(),
            weight: *weight,
        })
        .collect();
    (
        so,
        owners,
        ownership.multi_leader_rounds,
        ownership.open_multi_leader_rounds,
        timedelta_micros(&ownership.timeout_config.base_timeout),
        timedelta_micros(&ownership.timeout_config.timeout_increment),
        timedelta_micros(&ownership.timeout_config.fallback_duration),
        ownership
            .timeout_config
            .fast_round_duration
            .map(|d| timedelta_micros(&d))
            .unwrap_or(0),
    )
}

fn make_app_permissions_eip712(perms: &ApplicationPermissions) -> ChangeAppPermissionsOpEip712 {
    ChangeAppPermissionsOpEip712 {
        has_execute_filter: perms.execute_operations.is_some(),
        execute_operations: perms
            .execute_operations
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|id| format_application_id(id))
            .collect(),
        mandatory_applications: perms
            .mandatory_applications
            .iter()
            .map(format_application_id)
            .collect(),
        manage_chain_apps: perms
            .manage_chain
            .iter()
            .map(format_application_id)
            .collect(),
        has_oracle_filter: perms.call_service_as_oracle.is_some(),
        oracle_apps: perms
            .call_service_as_oracle
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|id| format_application_id(id))
            .collect(),
        has_http_filter: perms.make_http_requests.is_some(),
        http_apps: perms
            .make_http_requests
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|id| format_application_id(id))
            .collect(),
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
                    result.transfers.push(TransferOpEip712 {
                        owner: format_account_owner(owner),
                        recipient: format_account(&recipient.chain_id, &recipient.owner),
                        amount: amount.to_attos(),
                    });
                }
                SystemOperation::Claim {
                    owner,
                    target_id,
                    recipient,
                    amount,
                } => {
                    result.claims.push(ClaimOpEip712 {
                        owner: format_account_owner(owner),
                        target_chain: format_chain_id(target_id),
                        recipient: format_account(&recipient.chain_id, &recipient.owner),
                        amount: amount.to_attos(),
                    });
                }
                SystemOperation::OpenChain(config) => {
                    let (so, owners, mlr, omlr, bt, ti, fd, frd) =
                        make_ownership_eip712(&config.ownership);
                    let perms_hash =
                        make_app_permissions_eip712(&config.application_permissions).hash_struct();
                    result.open_chain_ops.push(OpenChainOpEip712 {
                        balance: config.balance.to_attos(),
                        super_owners: so,
                        owners,
                        multi_leader_rounds: mlr,
                        open_multi_leader_rounds: omlr,
                        base_timeout_micros: bt,
                        timeout_increment_micros: ti,
                        fallback_duration_micros: fd,
                        fast_round_duration_micros: frd,
                        permissions_hash: perms_hash,
                    });
                }
                SystemOperation::CloseChain => {
                    result.close_chain_ops.push(CloseChainOpEip712 {});
                }
                SystemOperation::ChangeOwnership {
                    super_owners,
                    owners,
                    first_leader: _,
                    multi_leader_rounds,
                    open_multi_leader_rounds,
                    timeout_config,
                } => {
                    let (so, o) = extract_ownership_vecs(super_owners, owners);
                    let weighted_owners = o
                        .iter()
                        .map(|(owner, weight)| WeightedOwnerEip712 {
                            owner: owner.clone(),
                            weight: *weight,
                        })
                        .collect();
                    result.change_ownership_ops.push(ChangeOwnershipOpEip712 {
                        super_owners: so,
                        owners: weighted_owners,
                        multi_leader_rounds: *multi_leader_rounds,
                        open_multi_leader_rounds: *open_multi_leader_rounds,
                        base_timeout_micros: timedelta_micros(&timeout_config.base_timeout),
                        timeout_increment_micros: timedelta_micros(
                            &timeout_config.timeout_increment,
                        ),
                        fallback_duration_micros: timedelta_micros(
                            &timeout_config.fallback_duration,
                        ),
                        fast_round_duration_micros: timeout_config
                            .fast_round_duration
                            .map(|d| timedelta_micros(&d))
                            .unwrap_or(0),
                    });
                }
                SystemOperation::ChangeApplicationPermissions(perms) => {
                    result
                        .change_app_permissions_ops
                        .push(make_app_permissions_eip712(perms));
                }
                SystemOperation::PublishModule { module_id } => {
                    result.publish_module_ops.push(PublishModuleOpEip712 {
                        contract_blob_hash: module_id.contract_blob_hash.into(),
                        service_blob_hash: module_id.service_blob_hash.into(),
                        vm_runtime: format_vm_runtime(module_id).to_string(),
                    });
                }
                SystemOperation::PublishDataBlob { blob_hash } => {
                    result.publish_data_blob_ops.push(PublishDataBlobOpEip712 {
                        blob_hash: (*blob_hash).into(),
                    });
                }
                SystemOperation::VerifyBlob { blob_id } => {
                    result.verify_blob_ops.push(VerifyBlobOpEip712 {
                        blob_type: format_blob_type(&blob_id.blob_type).to_string(),
                        hash: blob_id.hash.into(),
                    });
                }
                SystemOperation::CreateApplication {
                    module_id,
                    parameters,
                    instantiation_argument,
                    required_application_ids,
                } => {
                    result
                        .create_application_ops
                        .push(CreateApplicationOpEip712 {
                            contract_blob_hash: module_id.contract_blob_hash.into(),
                            service_blob_hash: module_id.service_blob_hash.into(),
                            vm_runtime: format_vm_runtime(module_id).to_string(),
                            parameters_hash: keccak256(parameters).0,
                            instantiation_argument_hash: keccak256(instantiation_argument).0,
                            required_application_ids: required_application_ids
                                .iter()
                                .map(format_application_id)
                                .collect(),
                        });
                }
                SystemOperation::Admin(admin_op) => {
                    let (operation, epoch, blob_hash) = match admin_op {
                        AdminOperation::PublishCommitteeBlob { blob_hash } => {
                            ("PublishCommitteeBlob", 0u64, (*blob_hash).into())
                        }
                        AdminOperation::CreateCommittee { epoch, blob_hash } => {
                            ("CreateCommittee", epoch.0 as u64, (*blob_hash).into())
                        }
                        AdminOperation::RemoveCommittee { epoch } => {
                            ("RemoveCommittee", epoch.0 as u64, [0u8; 32])
                        }
                    };
                    result.admin_ops.push(AdminOpEip712 {
                        operation: operation.to_string(),
                        epoch,
                        blob_hash,
                    });
                }
                SystemOperation::ProcessNewEpoch(epoch) => {
                    result.process_new_epoch_ops.push(ProcessNewEpochOpEip712 {
                        epoch: epoch.0 as u64,
                    });
                }
                SystemOperation::ProcessRemovedEpoch(epoch) => {
                    result
                        .process_removed_epoch_ops
                        .push(ProcessRemovedEpochOpEip712 {
                            epoch: epoch.0 as u64,
                        });
                }
                SystemOperation::UpdateStreams(updates) => {
                    result.update_streams_ops.push(UpdateStreamsOpEip712 {
                        updates: updates
                            .iter()
                            .map(|(chain_id, stream_id, index)| StreamUpdateEip712 {
                                chain_id: format_chain_id(chain_id),
                                stream_id: format_stream_id(stream_id),
                                index: *index,
                            })
                            .collect(),
                    });
                }
            },
            Transaction::ExecuteOperation(Operation::User {
                application_id,
                bytes,
            }) => {
                result.user_operations.push(UserOpEip712 {
                    application_id: format_application_id(application_id),
                    data_hash: keccak256(bytes).0,
                });
            }
            Transaction::ReceiveMessages(bundle) => {
                result.incoming_messages.push(ReceiveMsgEip712 {
                    origin: format_chain_id(&bundle.origin),
                    action: format_message_action(&bundle.action).to_string(),
                    message_count: bundle.bundle.messages.len() as u32,
                });
            }
        }
    }

    result
}

fn hash_vec<T: Eip712Struct>(items: &[T]) -> [u8; 32] {
    let hashes: Vec<[u8; 32]> = items.iter().map(|v| v.hash_struct()).collect();
    encode_hash_array(&hashes)
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
    buf.extend_from_slice(&hash_vec(&cat.transfers));
    buf.extend_from_slice(&hash_vec(&cat.claims));
    buf.extend_from_slice(&hash_vec(&cat.incoming_messages));
    buf.extend_from_slice(&hash_vec(&cat.user_operations));
    buf.extend_from_slice(&hash_vec(&cat.open_chain_ops));
    buf.extend_from_slice(&hash_vec(&cat.close_chain_ops));
    buf.extend_from_slice(&hash_vec(&cat.change_ownership_ops));
    buf.extend_from_slice(&hash_vec(&cat.change_app_permissions_ops));
    buf.extend_from_slice(&hash_vec(&cat.publish_module_ops));
    buf.extend_from_slice(&hash_vec(&cat.publish_data_blob_ops));
    buf.extend_from_slice(&hash_vec(&cat.verify_blob_ops));
    buf.extend_from_slice(&hash_vec(&cat.create_application_ops));
    buf.extend_from_slice(&hash_vec(&cat.admin_ops));
    buf.extend_from_slice(&hash_vec(&cat.process_new_epoch_ops));
    buf.extend_from_slice(&hash_vec(&cat.process_removed_epoch_ops));
    buf.extend_from_slice(&hash_vec(&cat.update_streams_ops));
    buf.extend_from_slice(&encode_hash_array(&cat.other_hashes));
    let struct_hash = keccak256(&buf).0;

    let mut final_buf = Vec::with_capacity(2 + 32 + 32);
    final_buf.extend_from_slice(b"\x19\x01");
    final_buf.extend_from_slice(&*DOMAIN_SEPARATOR);
    final_buf.extend_from_slice(&struct_hash);
    keccak256(&final_buf).0
}

// -- JSON typed data builder --

fn json_vec<T: Eip712Struct>(items: &[T]) -> Vec<serde_json::Value> {
    items.iter().map(|v| v.to_eip712_json()).collect()
}

/// Produces the full EIP-712 typed data JSON for a `ProposalContent`.
///
/// The JSON can be sent to MetaMask via `eth_signTypedData_v4`, or parsed by the
/// generic `compute_eip712_hash` in `linera-base` to produce the signing hash.
pub fn eip712_typed_data_json(content: &ProposalContent) -> String {
    let block = &content.block;
    let cat = categorize_transactions(&block.transactions);
    let content_hash: [u8; 32] = CryptoHash::new(content).into();

    let other_hashes_json: Vec<serde_json::Value> = cat
        .other_hashes
        .iter()
        .map(|h| serde_json::Value::String(format!("0x{}", hex::encode(h))))
        .collect();

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
            "TransferOp": TransferOpEip712::eip712_json_type_def(),
            "ClaimOp": ClaimOpEip712::eip712_json_type_def(),
            "ReceiveMsg": ReceiveMsgEip712::eip712_json_type_def(),
            "UserOp": UserOpEip712::eip712_json_type_def(),
            "OpenChainOp": OpenChainOpEip712::eip712_json_type_def(),
            "CloseChainOp": CloseChainOpEip712::eip712_json_type_def(),
            "ChangeOwnershipOp": ChangeOwnershipOpEip712::eip712_json_type_def(),
            "WeightedOwner": WeightedOwnerEip712::eip712_json_type_def(),
            "ChangeAppPermissionsOp": ChangeAppPermissionsOpEip712::eip712_json_type_def(),
            "PublishModuleOp": PublishModuleOpEip712::eip712_json_type_def(),
            "PublishDataBlobOp": PublishDataBlobOpEip712::eip712_json_type_def(),
            "VerifyBlobOp": VerifyBlobOpEip712::eip712_json_type_def(),
            "CreateApplicationOp": CreateApplicationOpEip712::eip712_json_type_def(),
            "AdminOp": AdminOpEip712::eip712_json_type_def(),
            "ProcessNewEpochOp": ProcessNewEpochOpEip712::eip712_json_type_def(),
            "ProcessRemovedEpochOp": ProcessRemovedEpochOpEip712::eip712_json_type_def(),
            "UpdateStreamsOp": UpdateStreamsOpEip712::eip712_json_type_def(),
            "StreamUpdate": StreamUpdateEip712::eip712_json_type_def(),
        },
        "primaryType": "LineraBlockProposal",
        "domain": {
            "name": "Linera",
            "version": "1"
        },
        "message": {
            "chainId": format_bytes32(block.chain_id.0.into()),
            "epoch": (block.epoch.0 as u64).to_string(),
            "height": block.height.0.to_string(),
            "timestamp": block.timestamp.micros().to_string(),
            "round": format_round(&content.round),
            "contentHash": format_bytes32(content_hash),
            "transfers": json_vec(&cat.transfers),
            "claims": json_vec(&cat.claims),
            "incomingMessages": json_vec(&cat.incoming_messages),
            "userOperations": json_vec(&cat.user_operations),
            "openChainOps": json_vec(&cat.open_chain_ops),
            "closeChainOps": json_vec(&cat.close_chain_ops),
            "changeOwnershipOps": json_vec(&cat.change_ownership_ops),
            "changeAppPermissionsOps": json_vec(&cat.change_app_permissions_ops),
            "publishModuleOps": json_vec(&cat.publish_module_ops),
            "publishDataBlobOps": json_vec(&cat.publish_data_blob_ops),
            "verifyBlobOps": json_vec(&cat.verify_blob_ops),
            "createApplicationOps": json_vec(&cat.create_application_ops),
            "adminOps": json_vec(&cat.admin_ops),
            "processNewEpochOps": json_vec(&cat.process_new_epoch_ops),
            "processRemovedEpochOps": json_vec(&cat.process_removed_epoch_ops),
            "updateStreamsOps": json_vec(&cat.update_streams_ops),
            "otherTransactionHashes": other_hashes_json,
        }
    });

    typed_data.to_string()
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{CryptoHash, TestString},
        data_types::{Amount, BlockHeight, Epoch, Round},
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
                authenticated_owner: None,
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
        let t1 = TransferOpEip712 {
            owner: format_account_owner(&owner),
            recipient: format_account(&recipient_chain, &recipient_owner),
            amount: amount.to_attos(),
        };
        let t2 = TransferOpEip712 {
            owner: format_account_owner(&owner),
            recipient: format_account(&recipient_chain, &recipient_owner),
            amount: amount.to_attos(),
        };
        let hash1 = t1.hash_struct();
        let hash2 = t2.hash_struct();
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, [0u8; 32]);

        let t3 = TransferOpEip712 {
            owner: format_account_owner(&owner),
            recipient: format_account(&recipient_chain, &recipient_owner),
            amount: Amount::from_tokens(200).to_attos(),
        };
        assert_ne!(hash1, t3.hash_struct());
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
                authenticated_owner: None,
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
                authenticated_owner: None,
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
                authenticated_owner: None,
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
                authenticated_owner: None,
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

    // -- Derive macro correctness tests --

    #[test]
    fn derived_primary_def_is_valid() {
        assert_eq!(
            TransferOpEip712::PRIMARY_DEF,
            "TransferOp(string owner,string recipient,uint128 amount)"
        );
        assert_eq!(
            ClaimOpEip712::PRIMARY_DEF,
            "ClaimOp(string owner,string targetChain,string recipient,uint128 amount)"
        );
        assert_eq!(
            ReceiveMsgEip712::PRIMARY_DEF,
            "ReceiveMsg(string origin,string action,uint32 messageCount)"
        );
        assert_eq!(
            UserOpEip712::PRIMARY_DEF,
            "UserOp(string applicationId,bytes32 dataHash)"
        );
        assert_eq!(
            WeightedOwnerEip712::PRIMARY_DEF,
            "WeightedOwner(string owner,uint64 weight)"
        );
        assert_eq!(CloseChainOpEip712::PRIMARY_DEF, "CloseChainOp()");
        assert_eq!(
            PublishModuleOpEip712::PRIMARY_DEF,
            "PublishModuleOp(bytes32 contractBlobHash,bytes32 serviceBlobHash,string vmRuntime)"
        );
        assert_eq!(
            PublishDataBlobOpEip712::PRIMARY_DEF,
            "PublishDataBlobOp(bytes32 blobHash)"
        );
        assert_eq!(
            VerifyBlobOpEip712::PRIMARY_DEF,
            "VerifyBlobOp(string blobType,bytes32 hash)"
        );
        assert_eq!(
            AdminOpEip712::PRIMARY_DEF,
            "AdminOp(string operation,uint64 epoch,bytes32 blobHash)"
        );
        assert_eq!(
            ProcessNewEpochOpEip712::PRIMARY_DEF,
            "ProcessNewEpochOp(uint64 epoch)"
        );
        assert_eq!(
            ProcessRemovedEpochOpEip712::PRIMARY_DEF,
            "ProcessRemovedEpochOp(uint64 epoch)"
        );
        assert_eq!(
            StreamUpdateEip712::PRIMARY_DEF,
            "StreamUpdate(string chainId,string streamId,uint32 index)"
        );
    }

    #[test]
    fn derived_type_hash_simple_types() {
        // For types without references, type_hash = keccak256(PRIMARY_DEF).
        assert_eq!(
            *TransferOpEip712::eip712_type_hash(),
            keccak256(TransferOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *ClaimOpEip712::eip712_type_hash(),
            keccak256(ClaimOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *ReceiveMsgEip712::eip712_type_hash(),
            keccak256(ReceiveMsgEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *UserOpEip712::eip712_type_hash(),
            keccak256(UserOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *WeightedOwnerEip712::eip712_type_hash(),
            keccak256(WeightedOwnerEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *CloseChainOpEip712::eip712_type_hash(),
            keccak256(CloseChainOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *ChangeAppPermissionsOpEip712::eip712_type_hash(),
            keccak256(ChangeAppPermissionsOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *PublishModuleOpEip712::eip712_type_hash(),
            keccak256(PublishModuleOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *PublishDataBlobOpEip712::eip712_type_hash(),
            keccak256(PublishDataBlobOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *VerifyBlobOpEip712::eip712_type_hash(),
            keccak256(VerifyBlobOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *AdminOpEip712::eip712_type_hash(),
            keccak256(AdminOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *ProcessNewEpochOpEip712::eip712_type_hash(),
            keccak256(ProcessNewEpochOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *ProcessRemovedEpochOpEip712::eip712_type_hash(),
            keccak256(ProcessRemovedEpochOpEip712::PRIMARY_DEF).0
        );
        assert_eq!(
            *StreamUpdateEip712::eip712_type_hash(),
            keccak256(StreamUpdateEip712::PRIMARY_DEF).0
        );
    }

    #[test]
    fn derived_type_hash_with_references() {
        // Types with references: type hash = keccak256(PRIMARY_DEF + sorted referenced PRIMARY_DEFs).
        let mut open_chain_full = OpenChainOpEip712::PRIMARY_DEF.to_string();
        open_chain_full.push_str(WeightedOwnerEip712::PRIMARY_DEF);
        assert_eq!(
            *OpenChainOpEip712::eip712_type_hash(),
            keccak256(open_chain_full.as_bytes()).0
        );

        let mut change_own_full = ChangeOwnershipOpEip712::PRIMARY_DEF.to_string();
        change_own_full.push_str(WeightedOwnerEip712::PRIMARY_DEF);
        assert_eq!(
            *ChangeOwnershipOpEip712::eip712_type_hash(),
            keccak256(change_own_full.as_bytes()).0
        );

        let mut update_streams_full = UpdateStreamsOpEip712::PRIMARY_DEF.to_string();
        update_streams_full.push_str(StreamUpdateEip712::PRIMARY_DEF);
        assert_eq!(
            *UpdateStreamsOpEip712::eip712_type_hash(),
            keccak256(update_streams_full.as_bytes()).0
        );
    }

    #[test]
    fn derived_hash_struct_deterministic() {
        let t = TransferOpEip712 {
            owner: "owner1".to_string(),
            recipient: "recipient1".to_string(),
            amount: 1000,
        };
        let h1 = t.hash_struct();
        let h2 = t.hash_struct();
        assert_eq!(h1, h2);
        assert_ne!(h1, [0u8; 32]);

        let t2 = TransferOpEip712 {
            owner: "owner2".to_string(),
            recipient: "recipient1".to_string(),
            amount: 1000,
        };
        assert_ne!(h1, t2.hash_struct());
    }

    #[test]
    fn derived_close_chain_hash_deterministic() {
        let h1 = CloseChainOpEip712 {}.hash_struct();
        let h2 = CloseChainOpEip712 {}.hash_struct();
        assert_eq!(h1, h2);
        assert_ne!(h1, [0u8; 32]);
    }

    #[test]
    fn derived_struct_with_references_hash() {
        let update = StreamUpdateEip712 {
            chain_id: "chain1".to_string(),
            stream_id: "stream1".to_string(),
            index: 5,
        };
        let op = UpdateStreamsOpEip712 {
            updates: vec![update],
        };
        let h = op.hash_struct();
        assert_ne!(h, [0u8; 32]);

        let op_empty = UpdateStreamsOpEip712 { updates: vec![] };
        assert_ne!(h, op_empty.hash_struct());
    }

    #[test]
    fn derived_json_type_def_correct() {
        let def = TransferOpEip712::eip712_json_type_def();
        let arr = def.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["name"], "owner");
        assert_eq!(arr[0]["type"], "string");
        assert_eq!(arr[1]["name"], "recipient");
        assert_eq!(arr[1]["type"], "string");
        assert_eq!(arr[2]["name"], "amount");
        assert_eq!(arr[2]["type"], "uint128");
    }

    #[test]
    fn derived_to_eip712_json_correct() {
        let t = TransferOpEip712 {
            owner: "alice".to_string(),
            recipient: "bob".to_string(),
            amount: 500,
        };
        let json = t.to_eip712_json();
        assert_eq!(json["owner"], "alice");
        assert_eq!(json["recipient"], "bob");
        assert_eq!(json["amount"], "500");
    }

    /// Every derive struct's PRIMARY_DEF must appear in LINERA_BLOCK_PROPOSAL_TYPE,
    /// and every referenced type in the proposal type string must have a matching struct.
    #[test]
    fn derive_structs_consistent_with_proposal_type() {
        let proposal_type = LINERA_BLOCK_PROPOSAL_TYPE;

        // All derive struct PRIMARY_DEFs must appear verbatim in the proposal type string.
        // (LineraBlockProposal itself is excluded since it's the primary, not a reference.)
        let all_primary_defs = [
            TransferOpEip712::PRIMARY_DEF,
            ClaimOpEip712::PRIMARY_DEF,
            ReceiveMsgEip712::PRIMARY_DEF,
            UserOpEip712::PRIMARY_DEF,
            WeightedOwnerEip712::PRIMARY_DEF,
            CloseChainOpEip712::PRIMARY_DEF,
            OpenChainOpEip712::PRIMARY_DEF,
            ChangeOwnershipOpEip712::PRIMARY_DEF,
            ChangeAppPermissionsOpEip712::PRIMARY_DEF,
            PublishModuleOpEip712::PRIMARY_DEF,
            PublishDataBlobOpEip712::PRIMARY_DEF,
            VerifyBlobOpEip712::PRIMARY_DEF,
            CreateApplicationOpEip712::PRIMARY_DEF,
            AdminOpEip712::PRIMARY_DEF,
            ProcessNewEpochOpEip712::PRIMARY_DEF,
            ProcessRemovedEpochOpEip712::PRIMARY_DEF,
            StreamUpdateEip712::PRIMARY_DEF,
            UpdateStreamsOpEip712::PRIMARY_DEF,
        ];

        for def in &all_primary_defs {
            assert!(
                proposal_type.contains(def),
                "PRIMARY_DEF not found in LINERA_BLOCK_PROPOSAL_TYPE: {def}"
            );
        }

        // Every referenced type after the primary definition's closing ')' must match
        // a derive struct's PRIMARY_DEF.
        let after_primary = proposal_type.find(')').unwrap() + 1;
        let referenced = &proposal_type[after_primary..];
        // Each referenced type is "TypeName(...)" — extract them and verify.
        for ref_def in referenced.split(')').filter(|s| !s.is_empty()) {
            let full_def = format!("{})", ref_def);
            assert!(
                all_primary_defs.contains(&full_def.as_str()),
                "Referenced type in proposal not backed by derive struct: {full_def}"
            );
        }
    }

    #[test]
    fn empty_array_hash_encoding() {
        // Empty arrays should produce keccak256("") which is a specific known hash.
        let op = UpdateStreamsOpEip712 { updates: vec![] };
        let h1 = op.hash_struct();
        assert_ne!(h1, [0u8; 32]);

        // Adding an element must change the hash.
        let op2 = UpdateStreamsOpEip712 {
            updates: vec![StreamUpdateEip712 {
                chain_id: "c".to_string(),
                stream_id: "s".to_string(),
                index: 0,
            }],
        };
        assert_ne!(h1, op2.hash_struct());
    }

    #[test]
    fn empty_string_field_hashing() {
        let t1 = TransferOpEip712 {
            owner: String::new(),
            recipient: String::new(),
            amount: 0,
        };
        let h = t1.hash_struct();
        assert_ne!(h, [0u8; 32]);

        // Empty string should differ from non-empty.
        let t2 = TransferOpEip712 {
            owner: "a".to_string(),
            recipient: String::new(),
            amount: 0,
        };
        assert_ne!(h, t2.hash_struct());
    }

    #[test]
    fn zero_and_max_value_hashing() {
        // Zero amount.
        let t_zero = TransferOpEip712 {
            owner: "o".to_string(),
            recipient: "r".to_string(),
            amount: 0,
        };
        assert_ne!(t_zero.hash_struct(), [0u8; 32]);

        // u128::MAX amount.
        let t_max = TransferOpEip712 {
            owner: "o".to_string(),
            recipient: "r".to_string(),
            amount: u128::MAX,
        };
        assert_ne!(t_max.hash_struct(), [0u8; 32]);
        assert_ne!(t_zero.hash_struct(), t_max.hash_struct());
    }

    #[test]
    fn u64_max_json_hash_matches_binary() {
        // u64::MAX in a proposal field must hash identically via binary and JSON paths.
        // This is the scenario that triggered the MetaMask overflow bug.
        let proposal = ProposalContent {
            block: ProposedBlock {
                chain_id: test_chain_id(),
                epoch: Epoch(11),
                transactions: vec![],
                height: BlockHeight(u64::MAX),
                timestamp: 190000000u64.into(),
                authenticated_owner: None,
                previous_block_hash: None,
            },
            round: Round::SingleLeader(11),
            outcome: None,
        };
        let direct_hash = eip712_signing_hash(&proposal);
        let json = eip712_typed_data_json(&proposal);
        let json_hash = linera_base::crypto::eip712::compute_eip712_hash(&json);
        assert_eq!(direct_hash, json_hash);
    }

    #[test]
    fn field_order_matters() {
        // Two structs with the same field values but different field assignments
        // must hash differently (verifies field ordering in encodeData).
        let t1 = ClaimOpEip712 {
            owner: "alice".to_string(),
            target_chain: "chain_a".to_string(),
            recipient: "bob".to_string(),
            amount: 100,
        };
        let t2 = ClaimOpEip712 {
            owner: "bob".to_string(),
            target_chain: "chain_a".to_string(),
            recipient: "alice".to_string(),
            amount: 100,
        };
        assert_ne!(t1.hash_struct(), t2.hash_struct());
    }
}
