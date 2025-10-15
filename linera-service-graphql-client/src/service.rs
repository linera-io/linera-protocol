// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Blob, BlockHeight, ChainDescription, OracleResponse, Round, Timestamp},
    identifiers::{Account, AccountOwner, BlobId, ChainId, GenericApplicationId, StreamName},
};
use thiserror::Error;

pub type JSONObject = serde_json::Value;

#[cfg(target_arch = "wasm32")]
mod types {
    use linera_base::data_types::Round;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{BlockHeight, ChainId, CryptoHash};

    pub type ChainManager = Value;
    pub type ChainOwnership = Value;
    pub type Epoch = Value;
    pub type MessageBundle = Value;
    pub type MessageKind = Value;
    pub type Message = Value;
    pub type MessageAction = Value;
    pub type Operation = Value;
    pub type Origin = Value;
    pub type ApplicationDescription = Value;
    pub type OperationResult = Value;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    pub struct Notification {
        pub chain_id: ChainId,
        pub reason: Reason,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    #[expect(clippy::enum_variant_names)]
    pub enum Reason {
        NewBlock {
            height: BlockHeight,
            hash: CryptoHash,
        },
        NewIncomingBundle {
            origin: Origin,
            height: BlockHeight,
        },
        NewRound {
            height: BlockHeight,
            round: Round,
        },
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod types {
    pub use linera_base::{
        data_types::{ApplicationDescription, Epoch},
        ownership::ChainOwnership,
    };
    pub use linera_chain::{
        data_types::{IncomingBundle, MessageAction, MessageBundle, OperationResult, Transaction},
        manager::ChainManager,
    };
    pub use linera_core::worker::{Notification, Reason};
    pub use linera_execution::{Message, MessageKind, Operation, SystemOperation};
}

pub use types::*;
pub type ApplicationId = String;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Chain;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Chains;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone, PartialEq"
)]
pub struct Applications;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone, PartialEq"
)]
pub struct Blocks;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone, PartialEq"
)]
pub struct Block;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone, PartialEq"
)]
pub struct Notifications;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/service_schema.graphql",
    query_path = "gql/service_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Transfer;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("Unexpected certificate type: {0}")]
    UnexpectedCertificateType(String),
}

#[cfg(not(target_arch = "wasm32"))]
mod from {
    use linera_base::{
        data_types::{ApplicationPermissions, Event, TimeDelta},
        identifiers::{Account, ApplicationId as RealApplicationId, ModuleId, StreamId},
        ownership::{ChainOwnership, TimeoutConfig},
    };
    use linera_chain::{
        block::{Block, BlockBody, BlockHeader},
        types::ConfirmedBlock,
    };
    use linera_execution::{
        system::{AdminOperation, OpenChainConfig},
        OutgoingMessage,
    };

    use super::*;

    /// Convert GraphQL system operation metadata to a SystemOperation object.
    fn convert_system_operation(
        system_op: block::BlockBlockBlockBodyTransactionMetadataOperationSystemOperation,
    ) -> Result<SystemOperation, ConversionError> {
        match system_op.system_operation_type.as_str() {
            "Transfer" => {
                let transfer = system_op.transfer.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing transfer metadata for Transfer operation".to_string(),
                    )
                })?;
                Ok(SystemOperation::Transfer {
                    owner: transfer.owner,
                    recipient: Account {
                        chain_id: transfer.recipient.chain_id,
                        owner: transfer.recipient.owner,
                    },
                    amount: transfer.amount,
                })
            }
            "Claim" => {
                let claim = system_op.claim.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing claim metadata for Claim operation".to_string(),
                    )
                })?;
                Ok(SystemOperation::Claim {
                    owner: claim.owner,
                    target_id: claim.target_id,
                    recipient: Account {
                        chain_id: claim.recipient.chain_id,
                        owner: claim.recipient.owner,
                    },
                    amount: claim.amount,
                })
            }
            "OpenChain" => {
                let open_chain = system_op.open_chain.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing open_chain metadata for OpenChain operation".to_string(),
                    )
                })?;

                let ownership: ChainOwnership =
                    serde_json::from_str(&open_chain.ownership.ownership_json)
                        .map_err(ConversionError::Serde)?;

                let application_permissions: ApplicationPermissions =
                    serde_json::from_str(&open_chain.application_permissions.permissions_json)
                        .map_err(ConversionError::Serde)?;

                Ok(SystemOperation::OpenChain(OpenChainConfig {
                    ownership,
                    balance: open_chain.balance,
                    application_permissions,
                }))
            }
            "CloseChain" => Ok(SystemOperation::CloseChain),
            "ChangeOwnership" => {
                let change_ownership = system_op.change_ownership.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing change_ownership metadata for ChangeOwnership operation"
                            .to_string(),
                    )
                })?;

                let timeout_config = TimeoutConfig {
                    fast_round_duration: change_ownership
                        .timeout_config
                        .fast_round_ms
                        .as_ref()
                        .map(|s| {
                            s.parse::<u64>().map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid fast_round_ms value".to_string(),
                                )
                            })
                        })
                        .transpose()?
                        .map(|ms| TimeDelta::from_micros(ms * 1000)),
                    base_timeout: TimeDelta::from_micros(
                        change_ownership
                            .timeout_config
                            .base_timeout_ms
                            .parse::<u64>()
                            .map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid base_timeout_ms value".to_string(),
                                )
                            })?
                            * 1000,
                    ),
                    timeout_increment: TimeDelta::from_micros(
                        change_ownership
                            .timeout_config
                            .timeout_increment_ms
                            .parse::<u64>()
                            .map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid timeout_increment_ms value".to_string(),
                                )
                            })?
                            * 1000,
                    ),
                    fallback_duration: TimeDelta::from_micros(
                        change_ownership
                            .timeout_config
                            .fallback_duration_ms
                            .parse::<u64>()
                            .map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid fallback_duration_ms value".to_string(),
                                )
                            })?
                            * 1000,
                    ),
                };

                Ok(SystemOperation::ChangeOwnership {
                    super_owners: change_ownership.super_owners,
                    owners: change_ownership
                        .owners
                        .into_iter()
                        .map(|ow| {
                            let weight = ow.weight.parse::<u64>().map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid owner weight value".to_string(),
                                )
                            })?;
                            Ok((ow.owner, weight))
                        })
                        .collect::<Result<Vec<_>, ConversionError>>()?,
                    multi_leader_rounds: change_ownership.multi_leader_rounds as u32,
                    open_multi_leader_rounds: change_ownership.open_multi_leader_rounds,
                    timeout_config,
                })
            }
            "ChangeApplicationPermissions" => {
                let change_permissions =
                    system_op.change_application_permissions.ok_or_else(|| {
                        ConversionError::UnexpectedCertificateType(
                            "Missing change_application_permissions metadata".to_string(),
                        )
                    })?;

                let permissions: ApplicationPermissions =
                    serde_json::from_str(&change_permissions.permissions.permissions_json)
                        .map_err(ConversionError::Serde)?;

                Ok(SystemOperation::ChangeApplicationPermissions(permissions))
            }
            "PublishModule" => {
                let publish_module = system_op.publish_module.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing publish_module metadata for PublishModule operation".to_string(),
                    )
                })?;

                let module_id: ModuleId = publish_module.module_id.parse().map_err(|_| {
                    ConversionError::UnexpectedCertificateType(
                        "Invalid module_id format".to_string(),
                    )
                })?;

                Ok(SystemOperation::PublishModule { module_id })
            }
            "PublishDataBlob" => {
                let publish_data_blob = system_op.publish_data_blob.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing publish_data_blob metadata".to_string(),
                    )
                })?;
                Ok(SystemOperation::PublishDataBlob {
                    blob_hash: publish_data_blob.blob_hash,
                })
            }
            "VerifyBlob" => {
                let verify_blob = system_op.verify_blob.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing verify_blob metadata for VerifyBlob operation".to_string(),
                    )
                })?;
                let blob_id: BlobId = verify_blob.blob_id.parse().map_err(|_| {
                    ConversionError::UnexpectedCertificateType("Invalid blob_id format".to_string())
                })?;
                Ok(SystemOperation::VerifyBlob { blob_id })
            }
            "CreateApplication" => {
                let create_application = system_op.create_application.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing create_application metadata".to_string(),
                    )
                })?;

                let module_id: ModuleId = create_application.module_id.parse().map_err(|_| {
                    ConversionError::UnexpectedCertificateType(
                        "Invalid module_id format".to_string(),
                    )
                })?;

                let parameters = hex::decode(create_application.parameters_hex).map_err(|_| {
                    ConversionError::UnexpectedCertificateType(
                        "Invalid hex in parameters_hex".to_string(),
                    )
                })?;

                let instantiation_argument =
                    hex::decode(create_application.instantiation_argument_hex).map_err(|_| {
                        ConversionError::UnexpectedCertificateType(
                            "Invalid hex in instantiation_argument_hex".to_string(),
                        )
                    })?;

                let required_application_ids = create_application
                    .required_application_ids
                    .into_iter()
                    .map(|id| {
                        id.parse::<RealApplicationId>().map_err(|_| {
                            ConversionError::UnexpectedCertificateType(
                                "Invalid required_application_id format".to_string(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(SystemOperation::CreateApplication {
                    module_id,
                    parameters,
                    instantiation_argument,
                    required_application_ids,
                })
            }
            "Admin" => {
                let admin = system_op.admin.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing admin metadata for Admin operation".to_string(),
                    )
                })?;

                let admin_op = match admin.admin_operation_type.as_str() {
                    "PublishCommitteeBlob" => {
                        let blob_hash = admin.blob_hash.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing blob_hash for PublishCommitteeBlob".to_string(),
                            )
                        })?;
                        AdminOperation::PublishCommitteeBlob { blob_hash }
                    }
                    "CreateCommittee" => {
                        let epoch_val = admin.epoch.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing epoch for CreateCommittee".to_string(),
                            )
                        })?;
                        let epoch = Epoch(epoch_val as u32);
                        let blob_hash = admin.blob_hash.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing blob_hash for CreateCommittee".to_string(),
                            )
                        })?;
                        AdminOperation::CreateCommittee { epoch, blob_hash }
                    }
                    "RemoveCommittee" => {
                        let epoch_val = admin.epoch.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing epoch for RemoveCommittee".to_string(),
                            )
                        })?;
                        let epoch = Epoch(epoch_val as u32);
                        AdminOperation::RemoveCommittee { epoch }
                    }
                    _ => {
                        return Err(ConversionError::UnexpectedCertificateType(format!(
                            "Unknown admin operation type: {}",
                            admin.admin_operation_type
                        )));
                    }
                };

                Ok(SystemOperation::Admin(admin_op))
            }
            "ProcessNewEpoch" => {
                let epoch_val = system_op.epoch.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing epoch for ProcessNewEpoch operation".to_string(),
                    )
                })?;
                Ok(SystemOperation::ProcessNewEpoch(Epoch(epoch_val as u32)))
            }
            "ProcessRemovedEpoch" => {
                let epoch_val = system_op.epoch.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing epoch for ProcessRemovedEpoch operation".to_string(),
                    )
                })?;
                Ok(SystemOperation::ProcessRemovedEpoch(Epoch(
                    epoch_val as u32,
                )))
            }
            "UpdateStreams" => {
                let update_streams = system_op.update_streams.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing update_streams metadata for UpdateStreams operation".to_string(),
                    )
                })?;

                let streams = update_streams
                    .into_iter()
                    .map(|stream| {
                        let stream_id_parsed: StreamId =
                            stream.stream_id.parse().map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid stream_id format".to_string(),
                                )
                            })?;
                        Ok((stream.chain_id, stream_id_parsed, stream.next_index as u32))
                    })
                    .collect::<Result<Vec<_>, ConversionError>>()?;

                Ok(SystemOperation::UpdateStreams(streams))
            }
            _ => Err(ConversionError::UnexpectedCertificateType(format!(
                "Unknown system operation type: {}",
                system_op.system_operation_type
            ))),
        }
    }

    /// Convert GraphQL transaction metadata to a Transaction object.
    fn convert_transaction_metadata(
        metadata: block::BlockBlockBlockBodyTransactionMetadata,
    ) -> Result<Transaction, ConversionError> {
        match metadata.transaction_type.as_str() {
            "ReceiveMessages" => {
                let incoming_bundle = metadata.incoming_bundle.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing incoming_bundle for ReceiveMessages transaction".to_string(),
                    )
                })?;

                let bundle = IncomingBundle {
                    origin: incoming_bundle.origin,
                    bundle: MessageBundle {
                        height: incoming_bundle.bundle.height,
                        timestamp: incoming_bundle.bundle.timestamp,
                        certificate_hash: incoming_bundle.bundle.certificate_hash,
                        transaction_index: incoming_bundle.bundle.transaction_index as u32,
                        messages: incoming_bundle
                            .bundle
                            .messages
                            .into_iter()
                            .map(|msg| linera_chain::data_types::PostedMessage {
                                authenticated_signer: msg.authenticated_signer,
                                grant: msg.grant,
                                refund_grant_to: msg.refund_grant_to,
                                kind: msg.kind,
                                index: msg.index as u32,
                                message: msg.message,
                            })
                            .collect(),
                    },
                    action: incoming_bundle.action,
                };

                Ok(Transaction::ReceiveMessages(bundle))
            }
            "ExecuteOperation" => {
                let graphql_operation = metadata.operation.ok_or_else(|| {
                    ConversionError::UnexpectedCertificateType(
                        "Missing operation for ExecuteOperation transaction".to_string(),
                    )
                })?;

                let operation = match graphql_operation.operation_type.as_str() {
                    "System" => {
                        let system_op = graphql_operation.system_operation.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing system_operation for System operation".to_string(),
                            )
                        })?;

                        let sys_op = convert_system_operation(system_op)?;
                        Operation::System(Box::new(sys_op))
                    }
                    "User" => {
                        let application_id = graphql_operation.application_id.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing application_id for User operation".to_string(),
                            )
                        })?;

                        let bytes_hex = graphql_operation.user_bytes_hex.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing user_bytes_hex for User operation".to_string(),
                            )
                        })?;

                        // Convert hex string to bytes
                        let bytes = hex::decode(bytes_hex).map_err(|_| {
                            ConversionError::UnexpectedCertificateType(
                                "Invalid hex in user_bytes_hex".to_string(),
                            )
                        })?;

                        Operation::User {
                            application_id: application_id.parse().map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Invalid application_id format".to_string(),
                                )
                            })?,
                            bytes,
                        }
                    }
                    _ => {
                        return Err(ConversionError::UnexpectedCertificateType(format!(
                            "Unknown operation type: {}",
                            graphql_operation.operation_type
                        )));
                    }
                };

                Ok(Transaction::ExecuteOperation(operation))
            }
            _ => Err(ConversionError::UnexpectedCertificateType(format!(
                "Unknown transaction type: {}",
                metadata.transaction_type
            ))),
        }
    }

    impl From<block::BlockBlockBlockBodyMessages> for OutgoingMessage {
        fn from(val: block::BlockBlockBlockBodyMessages) -> Self {
            let block::BlockBlockBlockBodyMessages {
                destination,
                authenticated_signer,
                grant,
                refund_grant_to,
                kind,
                message,
            } = val;
            OutgoingMessage {
                destination,
                authenticated_signer,
                grant,
                refund_grant_to,
                kind,
                message,
            }
        }
    }

    impl TryFrom<block::BlockBlockBlock> for Block {
        type Error = ConversionError;

        fn try_from(val: block::BlockBlockBlock) -> Result<Self, Self::Error> {
            let block::BlockBlockBlock { header, body } = val;
            let block::BlockBlockBlockHeader {
                chain_id,
                epoch,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
                state_hash,
                transactions_hash,
                messages_hash,
                previous_message_blocks_hash,
                previous_event_blocks_hash,
                oracle_responses_hash,
                events_hash,
                blobs_hash,
                operation_results_hash,
            } = header;
            let block::BlockBlockBlockBody {
                messages,
                previous_message_blocks,
                previous_event_blocks,
                oracle_responses,
                events,
                blobs,
                operation_results,
                transaction_metadata,
            } = body;

            let block_header = BlockHeader {
                chain_id,
                epoch,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
                state_hash,
                transactions_hash,
                messages_hash,
                previous_message_blocks_hash,
                previous_event_blocks_hash,
                oracle_responses_hash,
                events_hash,
                blobs_hash,
                operation_results_hash,
            };

            // Convert GraphQL transaction metadata to Transaction objects
            let transactions = transaction_metadata
                .into_iter()
                .map(convert_transaction_metadata)
                .collect::<Result<Vec<_>, _>>()?;

            let block_body = BlockBody {
                transactions,
                messages: messages
                    .into_iter()
                    .map(|messages| messages.into_iter().map(Into::into).collect())
                    .collect::<Vec<Vec<_>>>(),
                previous_message_blocks: serde_json::from_value(previous_message_blocks)
                    .map_err(ConversionError::Serde)?,
                previous_event_blocks: serde_json::from_value(previous_event_blocks)
                    .map_err(ConversionError::Serde)?,
                oracle_responses: oracle_responses.into_iter().collect(),
                events: events
                    .into_iter()
                    .map(|events| events.into_iter().map(Into::into).collect())
                    .collect(),
                blobs: blobs
                    .into_iter()
                    .map(|blobs| blobs.into_iter().collect())
                    .collect(),
                operation_results,
            };

            Ok(Block {
                header: block_header,
                body: block_body,
            })
        }
    }

    impl From<block::BlockBlockBlockBodyEvents> for Event {
        fn from(event: block::BlockBlockBlockBodyEvents) -> Self {
            Event {
                stream_id: event.stream_id.into(),
                index: event.index as u32,
                value: event.value.into_iter().map(|byte| byte as u8).collect(),
            }
        }
    }

    impl From<block::BlockBlockBlockBodyEventsStreamId> for StreamId {
        fn from(stream_id: block::BlockBlockBlockBodyEventsStreamId) -> Self {
            StreamId {
                application_id: stream_id.application_id,
                stream_name: stream_id.stream_name,
            }
        }
    }

    impl TryFrom<block::BlockBlock> for ConfirmedBlock {
        type Error = ConversionError;

        fn try_from(val: block::BlockBlock) -> Result<Self, Self::Error> {
            match (val.status.as_str(), val.block) {
                ("confirmed", block) => Ok(ConfirmedBlock::new(block.try_into()?)),
                _ => Err(ConversionError::UnexpectedCertificateType(val.status)),
            }
        }
    }
}
