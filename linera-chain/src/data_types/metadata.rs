// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GraphQL-compatible structured metadata representations for operations and messages.

use async_graphql::SimpleObject;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ApplicationPermissions},
    hex,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_execution::{system::AdminOperation, Message, SystemMessage, SystemOperation};
use serde::{Deserialize, Serialize};

/// Timeout configuration metadata for GraphQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct TimeoutConfigMetadata {
    /// The duration of the fast round in milliseconds.
    pub fast_round_ms: Option<String>,
    /// The duration of the first single-leader and all multi-leader rounds in milliseconds.
    pub base_timeout_ms: String,
    /// The duration by which the timeout increases after each single-leader round in milliseconds.
    pub timeout_increment_ms: String,
    /// The age of an incoming tracked or protected message after which validators start
    /// transitioning to fallback mode, in milliseconds.
    pub fallback_duration_ms: String,
}

impl From<&TimeoutConfig> for TimeoutConfigMetadata {
    fn from(config: &TimeoutConfig) -> Self {
        TimeoutConfigMetadata {
            fast_round_ms: config
                .fast_round_duration
                .map(|d| (d.as_micros() / 1000).to_string()),
            base_timeout_ms: (config.base_timeout.as_micros() / 1000).to_string(),
            timeout_increment_ms: (config.timeout_increment.as_micros() / 1000).to_string(),
            fallback_duration_ms: (config.fallback_duration.as_micros() / 1000).to_string(),
        }
    }
}

/// Chain ownership metadata for GraphQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct ChainOwnershipMetadata {
    /// JSON serialized `ChainOwnership` for full representation.
    pub ownership_json: String,
}

impl From<&ChainOwnership> for ChainOwnershipMetadata {
    fn from(ownership: &ChainOwnership) -> Self {
        ChainOwnershipMetadata {
            // Fallback to Debug format should never be needed, as ChainOwnership implements Serialize.
            // But we include it as a safety measure for GraphQL responses to always succeed.
            ownership_json: serde_json::to_string(ownership)
                .unwrap_or_else(|_| format!("{:?}", ownership)),
        }
    }
}

/// Application permissions metadata for GraphQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct ApplicationPermissionsMetadata {
    /// JSON serialized `ApplicationPermissions`.
    pub permissions_json: String,
}

impl From<&ApplicationPermissions> for ApplicationPermissionsMetadata {
    fn from(permissions: &ApplicationPermissions) -> Self {
        ApplicationPermissionsMetadata {
            // Fallback to Debug format should never be needed, as ApplicationPermissions implements Serialize.
            // But we include it as a safety measure for GraphQL responses to always succeed.
            permissions_json: serde_json::to_string(permissions)
                .unwrap_or_else(|_| format!("{:?}", permissions)),
        }
    }
}

/// Structured representation of a system operation for GraphQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct SystemOperationMetadata {
    /// The type of system operation
    pub system_operation_type: String,
    /// Transfer operation details
    pub transfer: Option<TransferOperationMetadata>,
    /// Claim operation details
    pub claim: Option<ClaimOperationMetadata>,
    /// Open chain operation details
    pub open_chain: Option<OpenChainOperationMetadata>,
    /// Change ownership operation details
    pub change_ownership: Option<ChangeOwnershipOperationMetadata>,
    /// Change application permissions operation details
    pub change_application_permissions: Option<ChangeApplicationPermissionsMetadata>,
    /// Admin operation details
    pub admin: Option<AdminOperationMetadata>,
    /// Create application operation details
    pub create_application: Option<CreateApplicationOperationMetadata>,
    /// Publish data blob operation details
    pub publish_data_blob: Option<PublishDataBlobMetadata>,
    /// Verify blob operation details
    pub verify_blob: Option<VerifyBlobMetadata>,
    /// Publish module operation details
    pub publish_module: Option<PublishModuleMetadata>,
    /// Epoch operation details (`ProcessNewEpoch`, `ProcessRemovedEpoch`)
    pub epoch: Option<i32>,
    /// `UpdateStreams` operation details
    pub update_streams: Option<Vec<UpdateStreamMetadata>>,
}

impl SystemOperationMetadata {
    /// Creates a new metadata with the given operation type and all fields set to `None`.
    fn new(system_operation_type: &str) -> Self {
        SystemOperationMetadata {
            system_operation_type: system_operation_type.to_string(),
            transfer: None,
            claim: None,
            open_chain: None,
            change_ownership: None,
            change_application_permissions: None,
            admin: None,
            create_application: None,
            publish_data_blob: None,
            verify_blob: None,
            publish_module: None,
            epoch: None,
            update_streams: None,
        }
    }
}

/// Transfer operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct TransferOperationMetadata {
    pub owner: AccountOwner,
    pub recipient: Account,
    pub amount: Amount,
}

/// Claim operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct ClaimOperationMetadata {
    pub owner: AccountOwner,
    pub target_id: ChainId,
    pub recipient: Account,
    pub amount: Amount,
}

/// Open chain operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct OpenChainOperationMetadata {
    pub balance: Amount,
    pub ownership: ChainOwnershipMetadata,
    pub application_permissions: ApplicationPermissionsMetadata,
}

/// Change ownership operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct ChangeOwnershipOperationMetadata {
    pub super_owners: Vec<AccountOwner>,
    pub owners: Vec<OwnerWithWeight>,
    pub first_leader: Option<AccountOwner>,
    pub multi_leader_rounds: i32,
    pub open_multi_leader_rounds: bool,
    pub timeout_config: TimeoutConfigMetadata,
}

/// Owner with weight metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct OwnerWithWeight {
    pub owner: AccountOwner,
    pub weight: String, // Using String to represent u64 safely in GraphQL
}

/// Change application permissions operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct ChangeApplicationPermissionsMetadata {
    pub permissions: ApplicationPermissionsMetadata,
}

/// Admin operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct AdminOperationMetadata {
    pub admin_operation_type: String,
    pub epoch: Option<i32>,
    pub blob_hash: Option<CryptoHash>,
}

/// Create application operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct CreateApplicationOperationMetadata {
    pub module_id: String,
    pub parameters_hex: String,
    pub instantiation_argument_hex: String,
    pub required_application_ids: Vec<ApplicationId>,
}

/// Publish data blob operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct PublishDataBlobMetadata {
    pub blob_hash: CryptoHash,
}

/// Verify blob operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct VerifyBlobMetadata {
    pub blob_id: String,
}

/// Publish module operation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct PublishModuleMetadata {
    pub module_id: String,
}

/// Update stream metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct UpdateStreamMetadata {
    pub chain_id: ChainId,
    pub stream_id: String,
    pub next_index: i32,
}

/// Structured representation of a system message for GraphQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct SystemMessageMetadata {
    /// The type of system message
    pub system_message_type: String,
    /// Credit message details
    pub credit: Option<CreditMessageMetadata>,
    /// Withdraw message details
    pub withdraw: Option<WithdrawMessageMetadata>,
}

/// Credit message metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct CreditMessageMetadata {
    pub target: AccountOwner,
    pub amount: Amount,
    pub source: AccountOwner,
}

/// Withdraw message metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct WithdrawMessageMetadata {
    pub owner: AccountOwner,
    pub amount: Amount,
    pub recipient: Account,
}

/// Structured representation of a message for GraphQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, SimpleObject)]
pub struct MessageMetadata {
    /// The type of message: "System" or "User"
    pub message_type: String,
    /// For user messages, the application ID
    pub application_id: Option<ApplicationId>,
    /// For user messages, the serialized bytes (as a hex string for GraphQL)
    pub user_bytes_hex: Option<String>,
    /// For system messages, structured representation
    pub system_message: Option<SystemMessageMetadata>,
}

impl From<&SystemOperation> for SystemOperationMetadata {
    fn from(sys_op: &SystemOperation) -> Self {
        match sys_op {
            SystemOperation::Transfer {
                owner,
                recipient,
                amount,
            } => SystemOperationMetadata {
                transfer: Some(TransferOperationMetadata {
                    owner: *owner,
                    recipient: *recipient,
                    amount: *amount,
                }),
                ..SystemOperationMetadata::new("Transfer")
            },
            SystemOperation::Claim {
                owner,
                target_id,
                recipient,
                amount,
            } => SystemOperationMetadata {
                claim: Some(ClaimOperationMetadata {
                    owner: *owner,
                    target_id: *target_id,
                    recipient: *recipient,
                    amount: *amount,
                }),
                ..SystemOperationMetadata::new("Claim")
            },
            SystemOperation::OpenChain(config) => SystemOperationMetadata {
                open_chain: Some(OpenChainOperationMetadata {
                    balance: config.balance,
                    ownership: ChainOwnershipMetadata::from(&config.ownership),
                    application_permissions: ApplicationPermissionsMetadata::from(
                        &config.application_permissions,
                    ),
                }),
                ..SystemOperationMetadata::new("OpenChain")
            },
            SystemOperation::CloseChain => SystemOperationMetadata::new("CloseChain"),
            SystemOperation::ChangeOwnership {
                super_owners,
                owners,
                first_leader,
                multi_leader_rounds,
                open_multi_leader_rounds,
                timeout_config,
            } => SystemOperationMetadata {
                change_ownership: Some(ChangeOwnershipOperationMetadata {
                    super_owners: super_owners.clone(),
                    owners: owners
                        .iter()
                        .map(|(owner, weight)| OwnerWithWeight {
                            owner: *owner,
                            weight: weight.to_string(),
                        })
                        .collect(),
                    first_leader: *first_leader,
                    multi_leader_rounds: *multi_leader_rounds as i32,
                    open_multi_leader_rounds: *open_multi_leader_rounds,
                    timeout_config: TimeoutConfigMetadata::from(timeout_config),
                }),
                ..SystemOperationMetadata::new("ChangeOwnership")
            },
            SystemOperation::ChangeApplicationPermissions(permissions) => SystemOperationMetadata {
                change_application_permissions: Some(ChangeApplicationPermissionsMetadata {
                    permissions: ApplicationPermissionsMetadata::from(permissions),
                }),
                ..SystemOperationMetadata::new("ChangeApplicationPermissions")
            },
            SystemOperation::Admin(admin_op) => SystemOperationMetadata {
                admin: Some(AdminOperationMetadata::from(admin_op)),
                ..SystemOperationMetadata::new("Admin")
            },
            SystemOperation::CreateApplication {
                module_id,
                parameters,
                instantiation_argument,
                required_application_ids,
            } => SystemOperationMetadata {
                create_application: Some(CreateApplicationOperationMetadata {
                    module_id: serde_json::to_string(module_id)
                        .unwrap_or_else(|_| format!("{:?}", module_id)),
                    parameters_hex: hex::encode(parameters),
                    instantiation_argument_hex: hex::encode(instantiation_argument),
                    required_application_ids: required_application_ids.clone(),
                }),
                ..SystemOperationMetadata::new("CreateApplication")
            },
            SystemOperation::PublishDataBlob { blob_hash } => SystemOperationMetadata {
                publish_data_blob: Some(PublishDataBlobMetadata {
                    blob_hash: *blob_hash,
                }),
                ..SystemOperationMetadata::new("PublishDataBlob")
            },
            SystemOperation::VerifyBlob { blob_id } => SystemOperationMetadata {
                verify_blob: Some(VerifyBlobMetadata {
                    blob_id: blob_id.to_string(),
                }),
                ..SystemOperationMetadata::new("VerifyBlob")
            },
            SystemOperation::PublishModule { module_id } => SystemOperationMetadata {
                publish_module: Some(PublishModuleMetadata {
                    module_id: serde_json::to_string(module_id)
                        .unwrap_or_else(|_| format!("{:?}", module_id)),
                }),
                ..SystemOperationMetadata::new("PublishModule")
            },
            SystemOperation::ProcessNewEpoch(epoch) => SystemOperationMetadata {
                epoch: Some(epoch.0 as i32),
                ..SystemOperationMetadata::new("ProcessNewEpoch")
            },
            SystemOperation::ProcessRemovedEpoch(epoch) => SystemOperationMetadata {
                epoch: Some(epoch.0 as i32),
                ..SystemOperationMetadata::new("ProcessRemovedEpoch")
            },
            SystemOperation::UpdateStreams(streams) => SystemOperationMetadata {
                update_streams: Some(
                    streams
                        .iter()
                        .map(|(chain_id, stream_id, next_index)| UpdateStreamMetadata {
                            chain_id: *chain_id,
                            stream_id: stream_id.to_string(),
                            next_index: *next_index as i32,
                        })
                        .collect(),
                ),
                ..SystemOperationMetadata::new("UpdateStreams")
            },
        }
    }
}

impl From<&AdminOperation> for AdminOperationMetadata {
    fn from(admin_op: &AdminOperation) -> Self {
        match admin_op {
            AdminOperation::PublishCommitteeBlob { blob_hash } => AdminOperationMetadata {
                admin_operation_type: "PublishCommitteeBlob".to_string(),
                epoch: None,
                blob_hash: Some(*blob_hash),
            },
            AdminOperation::CreateCommittee { epoch, blob_hash } => AdminOperationMetadata {
                admin_operation_type: "CreateCommittee".to_string(),
                epoch: Some(epoch.0 as i32),
                blob_hash: Some(*blob_hash),
            },
            AdminOperation::RemoveCommittee { epoch } => AdminOperationMetadata {
                admin_operation_type: "RemoveCommittee".to_string(),
                epoch: Some(epoch.0 as i32),
                blob_hash: None,
            },
        }
    }
}

impl From<&Message> for MessageMetadata {
    fn from(message: &Message) -> Self {
        match message {
            Message::System(sys_msg) => MessageMetadata {
                message_type: "System".to_string(),
                application_id: None,
                user_bytes_hex: None,
                system_message: Some(SystemMessageMetadata::from(sys_msg)),
            },
            Message::User {
                application_id,
                bytes,
            } => MessageMetadata {
                message_type: "User".to_string(),
                application_id: Some(*application_id),
                user_bytes_hex: Some(hex::encode(bytes)),
                system_message: None,
            },
        }
    }
}

impl From<&SystemMessage> for SystemMessageMetadata {
    fn from(sys_msg: &SystemMessage) -> Self {
        match sys_msg {
            SystemMessage::Credit {
                target,
                amount,
                source,
            } => SystemMessageMetadata {
                system_message_type: "Credit".to_string(),
                credit: Some(CreditMessageMetadata {
                    target: *target,
                    amount: *amount,
                    source: *source,
                }),
                withdraw: None,
            },
            SystemMessage::Withdraw {
                owner,
                amount,
                recipient,
            } => SystemMessageMetadata {
                system_message_type: "Withdraw".to_string(),
                credit: None,
                withdraw: Some(WithdrawMessageMetadata {
                    owner: *owner,
                    amount: *amount,
                    recipient: *recipient,
                }),
            },
        }
    }
}
