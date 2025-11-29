// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, ServiceAbi},
};
use serde::{Deserialize, Serialize};

pub struct GraphQlQueriesAbi;

#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum GraphQlQueriesOperation {
    /// Set the register
    SetRegister { value: u64 },
    /// Insert in MapView
    InsertMapString { key: String, value: u8 },
    /// Insert in CollectionView
    InsertCollString { key: String, value: u8 },
    /// Insertion in the CollectionView / MapView
    InsertCollMap {
        key1: String,
        key2: String,
        value: u64,
    },
}

impl ContractAbi for GraphQlQueriesAbi {
    type Operation = GraphQlQueriesOperation;
    type Response = ();
}

impl ServiceAbi for GraphQlQueriesAbi {
    type Query = Request;
    type QueryResponse = Response;
}
