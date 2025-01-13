// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use async_graphql::{Request, Response};
use linera_sdk::base::{ContractAbi, ServiceAbi};

pub struct CounterAbi;

impl ContractAbi for CounterAbi {
    type Operation = u64;
    type Response = u64;
}

impl ServiceAbi for CounterAbi {
    type Query = Request;
    type QueryResponse = Response;
}
