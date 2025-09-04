// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Create and Call Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CreateAndCallAbi;

impl ContractAbi for CreateAndCallAbi {
    type Operation = CreateAndCallOperation;
    type Response = u64;
}

impl ServiceAbi for CreateAndCallAbi {
    type Query = CreateAndCallRequest;
    type QueryResponse = u64;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CreateAndCallRequest {
    Query,
    CreateAndCall(Vec<u8>, Vec<u8>, u64, u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CreateAndCallOperation {
    CreateAndCall(Vec<u8>, Vec<u8>, u64, u64),
}
