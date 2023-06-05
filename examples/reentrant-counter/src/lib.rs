// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::base::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

// TODO(#768): Remove the derive macros.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct ReentrantCounterAbi;

impl ContractAbi for ReentrantCounterAbi {
    type InitializationArgument = u64;
    type Parameters = ();
    type Operation = u64;
    type ApplicationCall = u64;
    type Message = ();
    type SessionCall = ();
    type Response = u64;
    type SessionState = ();
}

impl ServiceAbi for ReentrantCounterAbi {
    type Query = ();
    type QueryResponse = u64;
    type Parameters = ();
}
