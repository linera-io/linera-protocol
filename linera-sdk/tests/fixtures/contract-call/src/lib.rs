// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::linera_base_types::{
    Account, AccountOwner, Amount, ContractAbi, ModuleId, ServiceAbi,
};
use serde::{Deserialize, Serialize};

pub struct ContractTransferAbi;

impl ContractAbi for ContractTransferAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for ContractTransferAbi {
    type Query = Query;
    type QueryResponse = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    DirectTransfer {
        source: AccountOwner,
        destination: Account,
        amount: Amount,
    },
    IndirectTransfer {
        source: AccountOwner,
        destination: Account,
        amount: Amount,
    },
    TestNoneAuthenticatedSignerCaller,
    TestSomeAuthenticatedSignerCaller,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Query {
    DirectTransfer {
        source: AccountOwner,
        destination: Account,
        amount: Amount,
    },
    IndirectTransfer {
        source: AccountOwner,
        destination: Account,
        amount: Amount,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Parameters {
    pub module_id: ModuleId,
}
