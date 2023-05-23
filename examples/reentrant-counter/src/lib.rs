// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::base::ContractAbi;

pub struct ReentrantCounterAbi;

impl ContractAbi for ReentrantCounterAbi {
    type InitializationArgument = u128;
    type Parameters = ();
    type Operation = u128;
    type ApplicationCall = u128;
    type Effect = ();
    type SessionCall = ();
    type Response = u128;
    type SessionState = ();
}
