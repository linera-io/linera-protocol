// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
pub type Operation = serde_json::Value;

#[cfg(not(target_arch = "wasm32"))]
pub use linera_execution::Operation;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct OperationKey {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: usize,
}

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Operations;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct OperationsCount;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct LastOperation;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct GetOperation;
