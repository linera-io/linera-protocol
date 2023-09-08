// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the structures for GraphQL queries.

use crate::common::IndexerError;
use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{ChainId, Destination, Owner},
};
use linera_chain::data_types::{
    Event, ExecutedBlock, HashedValue, IncomingMessage, Origin, OutgoingMessage,
};
use linera_core::worker::Notification;
use linera_execution::{committee::Epoch, Message, Operation};

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "../linera-explorer/graphql/schema.graphql",
    query_path = "../linera-explorer/graphql/notifications.graphql",
    response_derives = "Debug"
)]
pub struct Notifications;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "../linera-explorer/graphql/schema.graphql",
    query_path = "../linera-explorer/graphql/block.graphql",
    response_derives = "Debug"
)]
pub struct Block;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "../linera-explorer/graphql/schema.graphql",
    query_path = "../linera-explorer/graphql/chains.graphql",
    response_derives = "Debug"
)]
pub struct Chains;

impl From<block::BlockBlockValueExecutedBlockBlockIncomingMessages> for IncomingMessage {
    fn from(val: block::BlockBlockValueExecutedBlockBlockIncomingMessages) -> Self {
        IncomingMessage {
            origin: val.origin,
            event: val.event,
        }
    }
}

impl From<block::BlockBlockValueExecutedBlockBlock> for linera_chain::data_types::Block {
    fn from(val: block::BlockBlockValueExecutedBlockBlock) -> Self {
        let incoming_messages = val
            .incoming_messages
            .into_iter()
            .map(IncomingMessage::from)
            .collect();
        linera_chain::data_types::Block {
            chain_id: val.chain_id,
            epoch: val.epoch,
            incoming_messages,
            operations: val.operations,
            height: val.height,
            timestamp: val.timestamp,
            authenticated_signer: val.authenticated_signer,
            previous_block_hash: val.previous_block_hash,
        }
    }
}

impl From<block::BlockBlockValueExecutedBlockMessages> for OutgoingMessage {
    fn from(val: block::BlockBlockValueExecutedBlockMessages) -> Self {
        OutgoingMessage {
            destination: val.destination,
            authenticated_signer: val.authenticated_signer,
            message: val.message,
        }
    }
}

impl From<block::BlockBlockValueExecutedBlock> for ExecutedBlock {
    fn from(val: block::BlockBlockValueExecutedBlock) -> Self {
        let messages: Vec<OutgoingMessage> = val
            .messages
            .into_iter()
            .map(OutgoingMessage::from)
            .collect();
        ExecutedBlock {
            block: val.block.into(),
            messages,
            state_hash: val.state_hash,
        }
    }
}

impl TryFrom<block::BlockBlock> for HashedValue {
    type Error = IndexerError;
    fn try_from(val: block::BlockBlock) -> Result<Self, Self::Error> {
        match (val.value.status.as_str(), val.value.executed_block) {
            ("validated", Some(executed_block)) => {
                Ok(HashedValue::new_validated(executed_block.into()))
            }
            // this constructor needs the "test" feature from linera-service
            ("confirmed", Some(executed_block)) => {
                Ok(HashedValue::new_confirmed(executed_block.into()))
            }
            _ => Err(IndexerError::UnknownCertificateStatus(val.value.status)),
        }
    }
}
