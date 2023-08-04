// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use graphql_client::GraphQLQuery;
use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::{BlockHeight, RoundNumber, Timestamp},
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperationKey {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

impl BcsHashable for OperationKey {}

#[derive(Serialize, Deserialize, Debug)]
pub struct Notification {
    pub chain_id: ChainId,
    pub reason: Reason,
}

#[derive(Serialize, Deserialize, Debug)]
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

pub async fn introspection(url: &str) -> Result<Value> {
    let client = reqwest::Client::new();
    let graphql_query =
        "query { \
           __schema { \
             queryType { name } \
             mutationType { name } \
             subscriptionType { name } \
             types { ...FullType } \
             directives { name description locations args { ...InputValue } } } } \
         fragment FullType on __Type { \
           kind name description \
           fields(includeDeprecated:true) { \
             name description \
             args { ...InputValue } \
             type{ ...TypeRef } \
             isDeprecated deprecationReason } \
           inputFields { ...InputValue } \
           interfaces { ...TypeRef } \
           enumValues(includeDeprecated:true) { name description isDeprecated deprecationReason } \
           possibleTypes { ...TypeRef } } \
         fragment InputValue on __InputValue { \
           name description \
           type { ...TypeRef } \
           defaultValue } \
         fragment TypeRef on __Type { \
           kind name \
           ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name ofType {kind name} } } } } } } }";
    let res = client
        .post(url)
        .body(format!("{{\"query\":\"{}\"}}", graphql_query))
        .send()
        .await?
        .text()
        .await?;
    Ok(serde_json::from_str::<Value>(&res)?)
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/operations_schema.graphql",
    query_path = "graphql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Operations;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/operations_schema.graphql",
    query_path = "graphql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct OperationsCount;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/operations_schema.graphql",
    query_path = "graphql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct LastOperation;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "graphql/operations_schema.graphql",
    query_path = "graphql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct GetOperation;
