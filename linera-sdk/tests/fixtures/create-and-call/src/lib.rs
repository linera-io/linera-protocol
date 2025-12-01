// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Create and Call Example Application that does not use GraphQL */

use std::fmt::Debug;

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

#[derive(Serialize, Deserialize)]
pub enum CreateAndCallRequest {
    Query,
    CreateAndCall(Vec<u8>, Vec<u8>, u64, u64),
}

#[derive(Serialize, Deserialize)]
pub enum CreateAndCallOperation {
    CreateAndCall(Vec<u8>, Vec<u8>, u64, u64),
}

impl Debug for CreateAndCallRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CreateAndCallRequest::Query => write!(f, "CreateAndCallRequest::Query"),
            CreateAndCallRequest::CreateAndCall(code, calldata, initial_value, increment) => {
                write!(
                    f,
                    "CreateAndCallRequest::CreateAndCall(code: <{} bytes>, calldata: <{} bytes>, initial value: {}, increment: {})",
                    code.len(),
                    calldata.len(),
                    initial_value,
                    increment
                )
            }
        }
    }
}

impl Debug for CreateAndCallOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CreateAndCallOperation::CreateAndCall(code, calldata, initial_value, increment) => {
                write!(
                    f,
                    "CreateAndCall(code: <{} bytes>, calldata: <{} bytes>, initial_value: {}, increment: {})",
                    code.len(),
                    calldata.len(),
                    initial_value,
                    increment
                )
            }
        }
    }
}
