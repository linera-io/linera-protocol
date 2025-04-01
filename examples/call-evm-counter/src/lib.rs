// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CallCounterAbi;

impl ContractAbi for CallCounterAbi {
    type Operation = CallCounterOperation;
    type Response = u64;
}

impl ServiceAbi for CallCounterAbi {
    type Query = CallCounterRequest;
    type QueryResponse = u64;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CallCounterRequest {
    Query,
    Increment(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CallCounterOperation {
    Increment(u64),
}
