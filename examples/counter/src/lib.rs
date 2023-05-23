// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::base::ContractAbi;

pub struct CounterAbi;

impl ContractAbi for CounterAbi {
    type InitializationArgument = u64;
    type Parameters = ();
    type Operation = u64;
    type ApplicationCall = u64;
    type Effect = ();
    type SessionCall = ();
    type Response = u64;
    type SessionState = ();
}
