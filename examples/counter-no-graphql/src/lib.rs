// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};

pub struct CounterNoGraphQlAbi;

impl ContractAbi for CounterNoGraphQlAbi {
    type Operation = u64;
    type Response = u64;
}

impl ServiceAbi for CounterNoGraphQlAbi {
    type Query = ();
    type QueryResponse = u64;
}
