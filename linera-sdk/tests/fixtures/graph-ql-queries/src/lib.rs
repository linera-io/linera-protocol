// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, ServiceAbi},
};
use serde::{Deserialize, Serialize};

pub struct ComplexDataAbi;

#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum ComplexDataOperation {
    /// Field4 operation
    InsertField4 { key1: String, key2: String, value: u64 },
}

impl ContractAbi for ComplexDataAbi {
    type Operation = ComplexDataOperation;
    type Response = ();
}

impl ServiceAbi for ComplexDataAbi {
    type Query = Request;
    type QueryResponse = Response;
}
