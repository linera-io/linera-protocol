// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{Account, ChainDescription, ChainId, ChannelName, Destination, Owner},
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
    pub type Event = Value;
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
        NewIncomingMessage {
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
        data_types::{ChannelFullName, Event, MessageAction, Origin, Target},
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
    use linera_chain::data_types::{
        BlockExecutionOutcome, ExecutedBlock, HashedCertificateValue, IncomingMessage,
        OutgoingMessage,
    };

    use super::*;

    impl From<block::BlockBlockValueExecutedBlockBlockIncomingMessages> for IncomingMessage {
        fn from(val: block::BlockBlockValueExecutedBlockBlockIncomingMessages) -> Self {
            let block::BlockBlockValueExecutedBlockBlockIncomingMessages {
                origin,
                event,
                action,
            } = val;
            IncomingMessage {
                origin,
                event,
                action,
            }
        }
    }

    impl From<block::BlockBlockValueExecutedBlockBlock> for linera_chain::data_types::Block {
        fn from(val: block::BlockBlockValueExecutedBlockBlock) -> Self {
            let block::BlockBlockValueExecutedBlockBlock {
                chain_id,
                epoch,
                incoming_messages,
                operations,
                height,
                timestamp,
                authenticated_signer,
                previous_block_hash,
            } = val;
            let incoming_messages = incoming_messages
                .into_iter()
                .map(IncomingMessage::from)
                .collect();
            linera_chain::data_types::Block {
                chain_id,
                epoch,
                incoming_messages,
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
                        message_counts,
                        state_hash,
                    },
            } = val;
            let messages = messages
                .into_iter()
                .map(OutgoingMessage::from)
                .collect::<Vec<_>>();
            ExecutedBlock {
                block: block.into(),
                outcome: BlockExecutionOutcome {
                    messages,
                    message_counts: message_counts.into_iter().map(|c| c as u32).collect(),
                    state_hash,
                },
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
