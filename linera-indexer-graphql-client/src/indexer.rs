// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use graphql_client::GraphQLQuery;
use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/indexer_schema.graphql",
    query_path = "gql/indexer_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct Plugins;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "gql/indexer_schema.graphql",
    query_path = "gql/indexer_requests.graphql",
    response_derives = "Debug, Serialize, Clone"
)]
pub struct State;
