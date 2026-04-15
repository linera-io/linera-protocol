// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Flash Loan Example Application */

use linera_sdk::linera_base_types::{Amount, ApplicationId, ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct FlashLoanAbi;

impl ContractAbi for FlashLoanAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for FlashLoanAbi {
    type Query = ();
    type QueryResponse = ();
}

/// Operations supported by the flash-loan application.
#[derive(Debug, Deserialize, Serialize)]
pub enum Operation {
    /// Borrow tokens from the flash-loan pool.
    GetCash { amount: Amount },
    /// Repay borrowed tokens to the flash-loan pool.
    RepayLoan { amount: Amount },
}

/// Parameters for instantiating the flash-loan application.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FlashLoanParameters {
    /// The fungible token application to use.
    pub fungible_app_id: ApplicationId<fungible::FungibleTokenAbi>,
    /// Interest in millionths (e.g. 10_000 = 1%).
    pub interest_millionths: u64,
}

/// The initial state: how many tokens the pool starts with.
#[derive(Debug, Deserialize, Serialize)]
pub struct FlashLoanInitialState {
    pub pool_balance: Amount,
}
