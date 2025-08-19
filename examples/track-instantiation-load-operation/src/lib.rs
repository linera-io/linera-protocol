// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Track Instantiation Load Operation Application */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct TrackInstantiationLoadOperationAbi;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    GetCount,
}

impl ContractAbi for TrackInstantiationLoadOperationAbi {
    type Operation = ();
    type Response = ();
}

impl ServiceAbi for TrackInstantiationLoadOperationAbi {
    type Query = Query;
    type QueryResponse = u64;
}
