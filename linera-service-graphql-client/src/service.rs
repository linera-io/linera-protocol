// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, OracleResponse, Timestamp},
    identifiers::{
        Account, ChainDescription, ChainId, ChannelName, Destination, GenericApplicationId, Owner,
        StreamName,
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
    #[allow(clippy::enum_variant_names)]
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
    pub use linera_base::ownership::ChainOwnership;
    pub use linera_chain::{
        data_types::{ChannelFullName, MessageAction, MessageBundle, Origin, Target},
        manager::ChainManager,
    };
    pub use linera_core::worker::{Notification, Reason};
    pub use linera_execution::{
        committee::Epoch, Message, MessageKind, Operation, UserApplicationDescription,
    };
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
    use linera_base::identifiers::StreamId;
    use linera_chain::data_types::{
        BlockExecutionOutcome, EventRecord, ExecutedBlock, HashedCertificateValue, IncomingBundle,
        OutgoingMessage,
    };

    use super::*;

    impl From<block::BlockBlockValueExecutedBlockBlockIncomingBundles> for IncomingBundle {
        fn from(val: block::BlockBlockValueExecutedBlockBlockIncomingBundles) -> Self {
            let block::BlockBlockValueExecutedBlockBlockIncomingBundles {
                origin,
                bundle,
                action,
            } = val;
            IncomingBundle {
                origin,
                bundle,
                action,
            }
        }
    }

    impl From<block::BlockBlockValueExecutedBlockBlock> for linera_chain::data_types::Block {
        fn from(val: block::BlockBlockValueExecutedBlockBlock) -> Self {
            let block::BlockBlockValueExecutedBlockBlock {
                chain_id,
                epoch,
                incoming_bundles,
                operations,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
            } = val;
            let incoming_bundles = incoming_bundles
                .into_iter()
                .map(IncomingBundle::from)
                .collect();
            linera_chain::data_types::Block {
                chain_id,
                epoch,
                incoming_bundles,
                operations,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
            }
        }
    }

    impl From<block::BlockBlockValueExecutedBlockOutcomeMessages> for OutgoingMessage {
        fn from(val: block::BlockBlockValueExecutedBlockOutcomeMessages) -> Self {
            let block::BlockBlockValueExecutedBlockOutcomeMessages {
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

    impl From<block::BlockBlockValueExecutedBlock> for ExecutedBlock {
        fn from(val: block::BlockBlockValueExecutedBlock) -> Self {
            let block::BlockBlockValueExecutedBlock {
                block,
                outcome:
                    block::BlockBlockValueExecutedBlockOutcome {
                        messages,
                        state_hash,
                        oracle_responses,
                        events,
                    },
            } = val;
            let messages = messages
                .into_iter()
                .map(|messages| messages.into_iter().map(OutgoingMessage::from).collect())
                .collect::<Vec<Vec<_>>>();
            ExecutedBlock {
                block: block.into(),
                outcome: BlockExecutionOutcome {
                    messages,
                    state_hash,
                    oracle_responses: oracle_responses.into_iter().map(Into::into).collect(),
                    events: events
                        .into_iter()
                        .map(|events| events.into_iter().map(Into::into).collect())
                        .collect(),
                },
            }
        }
    }

    impl From<block::BlockBlockValueExecutedBlockOutcomeEvents> for EventRecord {
        fn from(event: block::BlockBlockValueExecutedBlockOutcomeEvents) -> Self {
            EventRecord {
                stream_id: event.stream_id.into(),
                key: event.key.into_iter().map(|byte| byte as u8).collect(),
                value: event.value.into_iter().map(|byte| byte as u8).collect(),
            }
        }
    }

    impl From<block::BlockBlockValueExecutedBlockOutcomeEventsStreamId> for StreamId {
        fn from(stream_id: block::BlockBlockValueExecutedBlockOutcomeEventsStreamId) -> Self {
            StreamId {
                application_id: stream_id.application_id,
                stream_name: stream_id.stream_name,
            }
        }
    }

    impl TryFrom<block::BlockBlock> for HashedCertificateValue {
        type Error = String;
        fn try_from(val: block::BlockBlock) -> Result<Self, Self::Error> {
            match (val.value.status.as_str(), val.value.executed_block) {
                ("validated", Some(executed_block)) => {
                    Ok(HashedCertificateValue::new_validated(executed_block.into()))
                }
                ("confirmed", Some(executed_block)) => {
                    Ok(HashedCertificateValue::new_confirmed(executed_block.into()))
                }
                _ => Err(val.value.status),
            }
        }
    }
}
