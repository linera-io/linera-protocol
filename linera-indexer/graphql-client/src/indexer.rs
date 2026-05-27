// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! GraphQL queries against the indexer's main API.

// The `GraphQLQuery` derive macro generates public items (per-query modules,
// `Variables` and `ResponseData` structs, etc.) that cannot carry doc comments,
// so this module of generated bindings is exempt from the crate's `missing_docs`
// policy. The hand-written query structs are still documented below.
#![allow(missing_docs)]

use graphql_client::GraphQLQuery;
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

/// GraphQL query returning the list of plugins registered with the indexer.
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/indexer_schema.graphql",
    query_path = "gql/indexer_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Plugins;

/// GraphQL query returning the indexer's current state (chain, block and height).
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/indexer_schema.graphql",
    query_path = "gql/indexer_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct State;
