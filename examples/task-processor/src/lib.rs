// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI of the Task Processor Example Application.
//!
//! This application demonstrates the off-chain task processor functionality.
//! It requests tasks to be executed by an external "echo" operator and stores
//! the results in its state.

use async_graphql::{Request, Response};
use linera_sdk::linera_base_types::{ChainId, ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct TaskProcessorAbi;

/// Operations that can be executed on the contract.
#[derive(Debug, Deserialize, Serialize)]
pub enum TaskProcessorOperation {
    /// Request a task to be processed by the given operator with the given input.
    RequestTask { operator: String, input: String },
    RequestTaskOn {
        chain_id: ChainId,
        operator: String,
        input: String,
    },
    /// Store the result of a completed task.
    StoreResult { result: String },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    RequestTask { operator: String, input: String },
}

impl ContractAbi for TaskProcessorAbi {
    type Operation = TaskProcessorOperation;
    type Response = ();
}

impl ServiceAbi for TaskProcessorAbi {
    type Query = Request;
    type QueryResponse = Response;
}
