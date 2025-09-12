// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};

// ANCHOR: contract_abi
pub struct CounterAbi;

impl ContractAbi for CounterAbi {
    type Operation = u64;
    type Response = u64;
}
// ANCHOR_END: contract_abi

// ANCHOR: service_abi
impl ServiceAbi for CounterAbi {
    type Query = Request;
    type QueryResponse = Response;
}
// ANCHOR_END: service_abi
