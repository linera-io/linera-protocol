// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Random Source Test Application.

This fixture demonstrates how a smart contract can derive a deterministic
pseudo-random number from the runtime's `transaction_index`, combined with the
chain ID, application ID and block height. It also asserts that the operation
runs as the first transaction in the block, which exercises the fix for
<https://github.com/linera-io/linera-protocol/issues/2411>. */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct RandomSourceAbi;

impl ContractAbi for RandomSourceAbi {
    type Operation = ();
    type Response = ();
}

impl ServiceAbi for RandomSourceAbi {
    type Query = Query;
    type QueryResponse = QueryResponse;
}

/// Query exposed by the random source service.
#[derive(Debug, Deserialize, Serialize)]
pub enum Query {
    /// Returns the seed and the random samples that the contract derived from it.
    GetSamples,
}

/// Response from the random source service.
#[derive(Debug, Deserialize, Serialize)]
pub enum QueryResponse {
    /// The seed and the two distinct random `u64` values it produced.
    Samples {
        seed: u64,
        sample1: u64,
        sample2: u64,
    },
}
