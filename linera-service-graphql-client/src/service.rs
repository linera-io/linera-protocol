// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, OracleResponse, Round, Timestamp},
    identifiers::{
        Account, BlobId, ChainDescription, ChainId, ChannelName, Destination, GenericApplicationId,
        Owner, StreamName,
    },
};

pub type JSONObject = serde_json::Value;

#[cfg(target_arch = "wasm32")]
mod types {
    use linera_base::data_types::Round;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{BlockHeight, ChainId, CryptoHash};

    pub type ChainManager = Value;
    pub type ChainOwnership = Value;
    pub type ChannelFullName = Value;
    pub type Epoch = Value;
    pub type MessageBundle = Value;
    pub type MessageKind = Value;
    pub type Message = Value;
    pub type MessageAction = Value;
    pub type Operation = Value;
    pub type Origin = Value;
    pub type Target = Value;
    pub type UserApplicationDescription = Value;

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
    pub use linera_base::{data_types::UserApplicationDescription, ownership::ChainOwnership};
    pub use linera_chain::{
        data_types::{ChannelFullName, MessageAction, MessageBundle, Origin, Target},
        manager::ChainManager,
    };
    pub use linera_core::worker::{Notification, Reason};
    pub use linera_execution::{committee::Epoch, Message, MessageKind, Operation};
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

#[cfg(not(target_arch = "wasm32"))]
mod from {
    use linera_base::{hashed::Hashed, identifiers::StreamId};
    use linera_chain::{
        block::{Block, BlockBody, BlockHeader},
        data_types::{
            EventRecord, ExecutedBlock, IncomingBundle, MessageBundle, OutgoingMessage,
            PostedMessage,
        },
        types::ConfirmedBlock,
    };

    use super::*;

    impl From<block::BlockBlockValueBlockBodyIncomingBundles> for IncomingBundle {
        fn from(val: block::BlockBlockValueBlockBodyIncomingBundles) -> Self {
            let block::BlockBlockValueBlockBodyIncomingBundles {
                origin,
                bundle,
                action,
            } = val;
            IncomingBundle {
                origin,
                bundle: bundle.into(),
                action,
            }
        }
    }

    impl From<block::BlockBlockValueBlockBodyIncomingBundlesBundle> for MessageBundle {
        fn from(val: block::BlockBlockValueBlockBodyIncomingBundlesBundle) -> Self {
            let block::BlockBlockValueBlockBodyIncomingBundlesBundle {
                height,
                timestamp,
                certificate_hash,
                transaction_index,
                messages,
            } = val;
            let messages = messages.into_iter().map(PostedMessage::from).collect();
            MessageBundle {
                height,
                timestamp,
                certificate_hash,
                transaction_index: transaction_index as u32,
                messages,
            }
        }
    }

    impl From<block::BlockBlockValueBlockBodyIncomingBundlesBundleMessages> for PostedMessage {
        fn from(val: block::BlockBlockValueBlockBodyIncomingBundlesBundleMessages) -> Self {
            let block::BlockBlockValueBlockBodyIncomingBundlesBundleMessages {
                authenticated_signer,
                grant,
                refund_grant_to,
                kind,
                index,
                message,
            } = val;
            PostedMessage {
                authenticated_signer,
                grant,
                refund_grant_to,
                kind,
                index: index as u32,
                message,
            }
        }
    }

    impl From<block::BlockBlockValueBlockBodyMessages> for OutgoingMessage {
        fn from(val: block::BlockBlockValueBlockBodyMessages) -> Self {
            let block::BlockBlockValueBlockBodyMessages {
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

    impl From<block::BlockBlockValueBlock> for ExecutedBlock {
        fn from(val: block::BlockBlockValueBlock) -> Self {
            let block::BlockBlockValueBlock { header, body } = val;
            let block::BlockBlockValueBlockHeader {
                chain_id,
                epoch,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
                state_hash,
                bundles_hash,
                messages_hash,
                operations_hash,
                oracle_responses_hash,
                events_hash,
            } = header;
            let block::BlockBlockValueBlockBody {
                incoming_bundles,
                messages,
                operations,
                oracle_responses,
                events,
            } = body;

            let block_header = BlockHeader {
                chain_id,
                epoch,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
                state_hash,
                bundles_hash,
                messages_hash,
                operations_hash,
                oracle_responses_hash,
                events_hash,
            };
            let block_body = BlockBody {
                incoming_bundles: incoming_bundles
                    .into_iter()
                    .map(IncomingBundle::from)
                    .collect(),
                messages: messages
                    .into_iter()
                    .map(|messages| messages.into_iter().map(Into::into).collect())
                    .collect::<Vec<Vec<_>>>(),
                operations,
                oracle_responses: oracle_responses.into_iter().map(Into::into).collect(),
                events: events
                    .into_iter()
                    .map(|events| events.into_iter().map(Into::into).collect())
                    .collect(),
            };

            Block {
                header: block_header,
                body: block_body,
            }
            .into()
        }
    }

    impl From<block::BlockBlockValueBlockBodyEvents> for EventRecord {
        fn from(event: block::BlockBlockValueBlockBodyEvents) -> Self {
            EventRecord {
                stream_id: event.stream_id.into(),
                key: event.key.into_iter().map(|byte| byte as u8).collect(),
                value: event.value.into_iter().map(|byte| byte as u8).collect(),
            }
        }
    }

    impl From<block::BlockBlockValueBlockBodyEventsStreamId> for StreamId {
        fn from(stream_id: block::BlockBlockValueBlockBodyEventsStreamId) -> Self {
            StreamId {
                application_id: stream_id.application_id,
                stream_name: stream_id.stream_name,
            }
        }
    }

    impl TryFrom<block::BlockBlock> for Hashed<ConfirmedBlock> {
        type Error = String;
        fn try_from(val: block::BlockBlock) -> Result<Self, Self::Error> {
            match (val.value.status.as_str(), val.value.block) {
                ("confirmed", block) => Ok(Hashed::new(ConfirmedBlock::new(block.into()))),
                _ => Err(val.value.status),
            }
        }
    }
}
