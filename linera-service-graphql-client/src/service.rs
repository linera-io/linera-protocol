// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainId, Destination, Owner},
};

#[cfg(target_arch = "wasm32")]
mod types {
    use super::{BlockHeight, ChainId, CryptoHash};
    use linera_base::data_types::RoundNumber;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    pub type Epoch = Value;
    pub type Message = Value;
    pub type Operation = Value;
    pub type Event = Value;
    pub type Origin = Value;
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
            round: RoundNumber,
        },
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod types {
    pub use linera_chain::data_types::{Event, Origin};
    pub use linera_core::worker::{Notification, Reason};
    pub use linera_execution::{committee::Epoch, Message, Operation, UserApplicationDescription};
}

pub use types::*;
pub type ApplicationId = String;

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
    use super::*;
    use linera_chain::data_types::{ExecutedBlock, HashedValue, IncomingMessage, OutgoingMessage};

    impl From<block::BlockBlockValueExecutedBlockBlockIncomingMessages> for IncomingMessage {
        fn from(val: block::BlockBlockValueExecutedBlockBlockIncomingMessages) -> Self {
            let block::BlockBlockValueExecutedBlockBlockIncomingMessages { origin, event } = val;
            IncomingMessage { origin, event }
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

    impl From<block::BlockBlockValueExecutedBlockMessages> for OutgoingMessage {
        fn from(val: block::BlockBlockValueExecutedBlockMessages) -> Self {
            let block::BlockBlockValueExecutedBlockMessages {
                destination,
                authenticated_signer,
                is_skippable,
                message,
            } = val;
            OutgoingMessage {
                destination,
                authenticated_signer,
                is_skippable,
                message,
            }
        }
    }

    impl From<block::BlockBlockValueExecutedBlock> for ExecutedBlock {
        fn from(val: block::BlockBlockValueExecutedBlock) -> Self {
            let block::BlockBlockValueExecutedBlock {
                block,
                messages,
                state_hash,
            } = val;
            let messages: Vec<OutgoingMessage> =
                messages.into_iter().map(OutgoingMessage::from).collect();
            ExecutedBlock {
                block: block.into(),
                messages,
                state_hash,
            }
        }
    }

    impl TryFrom<block::BlockBlock> for HashedValue {
        type Error = String;
        fn try_from(val: block::BlockBlock) -> Result<Self, Self::Error> {
            match (val.value.status.as_str(), val.value.executed_block) {
                ("validated", Some(executed_block)) => {
                    Ok(HashedValue::new_validated(executed_block.into()))
                }
                ("confirmed", Some(executed_block)) => {
                    Ok(HashedValue::new_confirmed(executed_block.into()))
                }
                _ => Err(val.value.status),
            }
        }
    }
}
