// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Time Expiry Test Application */

use linera_sdk::{
    formats::StableEnum,
    linera_base_types::{ContractAbi, ServiceAbi, TimeDelta},
};

pub struct TimeExpiryAbi;

#[derive(Debug, StableEnum)]
pub enum TimeExpiryOperation {
    /// Expire the operation after the given time delta from block timestamp.
    ExpireAfter(TimeDelta),
}

impl ContractAbi for TimeExpiryAbi {
    type Operation = TimeExpiryOperation;
    type Response = ();
}

impl ServiceAbi for TimeExpiryAbi {
    type Query = ();
    type QueryResponse = ();
}
