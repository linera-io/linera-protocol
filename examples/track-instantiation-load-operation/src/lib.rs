// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Track Instantiation Load Operation Application */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct TrackInstantiationLoadOperationAbi;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageContent {
    IncrementInstantiation,
    IncrementLoad,
    IncrementExecuteOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    GetStats,
    ExecuteOperation,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    pub instantiation_count: u64,
    pub load_count: u64,
    pub execute_operation_count: u64,
}

impl ContractAbi for TrackInstantiationLoadOperationAbi {
    type Operation = ();
    type Response = ();
}

impl ServiceAbi for TrackInstantiationLoadOperationAbi {
    type Query = Query;
    type QueryResponse = Stats;
}
