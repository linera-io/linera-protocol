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
        data_types::{MessageAction, MessageBundle, OperationResult},
        manager::ChainManager,
    };
    pub use linera_core::worker::{Notification, Reason};
    pub use linera_execution::{Message, MessageKind, Operation};
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
    use linera_base::{data_types::Event, identifiers::StreamId};
    use linera_chain::{
        block::{Block, BlockBody, BlockHeader},
        types::ConfirmedBlock,
    };
    use linera_execution::OutgoingMessage;

    use super::*;

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
        type Error = serde_json::Error;

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

            // Create an empty transactions vector since GraphQL doesn't provide detailed transaction data
            let transactions = Vec::new();

            let block_body = BlockBody {
                transactions,
                messages: messages
                    .into_iter()
                    .map(|messages| messages.into_iter().map(Into::into).collect())
                    .collect::<Vec<Vec<_>>>(),
                previous_message_blocks: serde_json::from_value(previous_message_blocks)?,
                previous_event_blocks: serde_json::from_value(previous_event_blocks)?,
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
