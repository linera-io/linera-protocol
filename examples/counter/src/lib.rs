// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CounterAbi;

#[derive(Debug, Deserialize, Serialize)]
pub enum CounterOperation {
    /// Increment the counter by the given value
    Increment { value: u64 },
}

impl ContractAbi for CounterAbi {
    type Operation = CounterOperation;
    type Response = u64;
}

impl ServiceAbi for CounterAbi {
    type Query = Request;
    type QueryResponse = Response;
}
