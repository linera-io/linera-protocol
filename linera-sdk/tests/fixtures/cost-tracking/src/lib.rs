// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Cost Tracking Application */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CostTrackingAbi;

/// Operations that can be executed by the contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Run all cost tracking operations.
    RunAll,
}

/// Queries that can be made to the service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    /// Get all log entries.
    GetLogs,
    /// Get the number of log entries.
    GetLogCount,
}

/// A log entry with a label and the remaining fuel at that point.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub label: String,
    pub fuel: u64,
}

/// Messages that can be sent by the contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// A dummy message for benchmarking.
    Ping,
}

impl ContractAbi for CostTrackingAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for CostTrackingAbi {
    type Query = Query;
    type QueryResponse = QueryResponse;
}

/// Response to service queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResponse {
    Logs(Vec<LogEntry>),
    LogCount(u64),
}
