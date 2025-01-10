// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the LLM Example Application */

use async_graphql::{Request, Response};
use linera_sdk::base::{ContractAbi, ServiceAbi};

pub struct LlmAbi;

impl ContractAbi for LlmAbi {
    type Operation = ();
    type Response = ();
}

impl ServiceAbi for LlmAbi {
    type Query = Request;
    type QueryResponse = Response;
}
