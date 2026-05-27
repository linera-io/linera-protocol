// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GraphQL queries against the indexer's operations API, and supporting types.

// The `GraphQLQuery` derive macro generates public items (per-query modules,
// `Variables` and `ResponseData` structs, etc.) that cannot carry doc comments,
// so this module of generated bindings is exempted from the crate's `missing_docs`
// policy. `expect` (rather than `allow`) flags this if the generated code ever stops
// producing undocumented items. The hand-written types and queries are still documented below.
#![expect(missing_docs)]

use graphql_client::GraphQLQuery;
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
use serde::{Deserialize, Serialize};

#[cfg(target_arch = "wasm32")]
pub type Operation = serde_json::Value;

#[cfg(not(target_arch = "wasm32"))]
pub use linera_execution::Operation;

/// The key identifying an operation: the chain it ran on, the height of the block
/// that contains it, and its index within that block.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct OperationKey {
    /// The chain on which the operation was executed.
    pub chain_id: ChainId,
    /// The height of the block containing the operation.
    pub height: BlockHeight,
    /// The index of the operation within the block.
    pub index: usize,
}

/// GraphQL query returning a range of operations starting from a given key.
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Operations;

/// GraphQL query returning the number of operations on a chain.
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct OperationsCount;

/// GraphQL query returning the last operation on a chain.
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct LastOperation;

/// GraphQL query returning a single operation identified by its key.
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/operations_schema.graphql",
    query_path = "gql/operations_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct GetOperation;
