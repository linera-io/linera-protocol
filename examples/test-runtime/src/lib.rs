// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct TestRuntimeAbi;

impl ContractAbi for TestRuntimeAbi {
    type Operation = TestRuntimeOperation;
    type Response = ();
}

impl ServiceAbi for TestRuntimeAbi {
    type Query = TestRuntimeRequest;
    type QueryResponse = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TestRuntimeRequest {
    Test,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TestRuntimeOperation {
    Test,
}
