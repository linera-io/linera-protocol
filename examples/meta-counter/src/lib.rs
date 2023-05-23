// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::base::{ApplicationId, ChainId, ContractAbi, ServiceAbi};

pub struct MetaCounterAbi;

impl ContractAbi for MetaCounterAbi {
    type InitializationArgument = ();
    type Parameters = ApplicationId;
    type Operation = (ChainId, u64);
    type ApplicationCall = ();
    type Effect = u64;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for MetaCounterAbi {
    type Query = Vec<u8>;
    type QueryResponse = Vec<u8>;
    type Parameters = ApplicationId;
}
