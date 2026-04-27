// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Panicker Example Application */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct PanickerAbi;

impl ContractAbi for PanickerAbi {
    type Operation = PanickerOperation;
    type Response = ();
}

impl ServiceAbi for PanickerAbi {
    type Query = PanickerRequest;
    type QueryResponse = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PanickerOperation;

#[derive(Debug, Serialize, Deserialize)]
pub struct PanickerRequest;
