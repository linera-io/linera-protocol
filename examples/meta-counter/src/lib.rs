// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::{Request, Response};
use linera_sdk::base::{ApplicationId, ChainId, ContractAbi, ServiceAbi};

pub struct MetaCounterAbi;

impl ContractAbi for MetaCounterAbi {
    type InitializationArgument = ();
    type Parameters = ApplicationId<counter::CounterAbi>;
    type Operation = (ChainId, u64);
    type ApplicationCall = ();
    type Message = u64;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for MetaCounterAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = ApplicationId<counter::CounterAbi>;
}
