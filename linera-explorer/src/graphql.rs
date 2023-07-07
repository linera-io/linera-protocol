// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, Timestamp},
    identifiers::{ChainId, Destination, Owner},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

type Epoch = Value;
type Message = Value;
type Operation = Value;
type Event = Value;
type Origin = Value;
type UserApplicationDescription = Value;
type ApplicationId = String;

#[derive(Serialize, Deserialize, Debug)]
pub struct Notification {
    pub chain_id: ChainId,
    pub reason: Reason,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Reason {
    NewBlock {
        height: BlockHeight,
        hash: CryptoHash,
    },
    NewIncomingMessage {
        origin: Origin,
        height: BlockHeight,
    },
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/blocks.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Blocks;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/block.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Block;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/chains.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct Chains;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/applications.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Applications;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/schema.graphql",
    query_path = "graphql/notifications.graphql",
    response_derives = "Debug, Serialize"
)]
pub struct Notifications;
