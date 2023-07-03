// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Request, Response};
use linera_sdk::base::{ContractAbi, ServiceAbi};

pub struct CounterAbi;

impl ContractAbi for CounterAbi {
    type InitializationArgument = u64;
    type Parameters = ();
    type Operation = u64;
    type ApplicationCall = u64;
    type Message = ();
    type SessionCall = ();
    type Response = u64;
    type SessionState = ();
}

impl ServiceAbi for CounterAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = ();
}
