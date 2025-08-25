// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Blob, BlockHeight, ChainDescription, OracleResponse, Round, Timestamp},
    identifiers::{AccountOwner, BlobId, ChainId, GenericApplicationId, StreamName},
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
        data_types::Event,
        identifiers::{Account, StreamId},
    };
    use linera_chain::{
        block::{Block, BlockBody, BlockHeader},
        types::ConfirmedBlock,
    };
    use linera_execution::OutgoingMessage;

    use super::*;

    /// Convert GraphQL transaction metadata to a Transaction object
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
                                refund_grant_to: msg.refund_grant_to.map(|rgt| Account {
                                    chain_id: rgt.chain_id,
                                    owner: rgt.owner,
                                }),
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
                        let bytes_hex = graphql_operation.system_bytes_hex.ok_or_else(|| {
                            ConversionError::UnexpectedCertificateType(
                                "Missing system_bytes_hex for System operation".to_string(),
                            )
                        })?;

                        // Convert hex string to bytes
                        let bytes = hex::decode(bytes_hex).map_err(|_| {
                            ConversionError::UnexpectedCertificateType(
                                "Invalid hex in system_bytes_hex".to_string(),
                            )
                        })?;

                        // Deserialize the system operation from BCS bytes
                        let system_operation: SystemOperation =
                            linera_base::bcs::from_bytes(&bytes).map_err(|_| {
                                ConversionError::UnexpectedCertificateType(
                                    "Failed to deserialize system operation from BCS bytes"
                                        .to_string(),
                                )
                            })?;

                        Operation::System(Box::new(system_operation))
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
                refund_grant_to: refund_grant_to.map(|rgt| Account {
                    chain_id: rgt.chain_id,
                    owner: rgt.owner,
                }),
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
