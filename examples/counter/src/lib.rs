// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, ServiceAbi},
};
use serde::{Deserialize, Serialize};

pub struct CounterAbi;

#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub struct CounterOperation {
    /// The value to increment the counter by
    pub increment: u64,
}

impl ContractAbi for CounterAbi {
    type Operation = CounterOperation;
    type Response = u64;
}

impl ServiceAbi for CounterAbi {
    type Query = Request;
    type QueryResponse = Response;
}
