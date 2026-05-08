// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI and types for the EVM→Linera bridge application.
//!
//! This bridge verifies MPT inclusion proofs for `DepositInitiated` events
//! on EVM and mints wrapped tokens on Linera via the wrapped-fungible app.

use async_graphql::{Request, Response};
pub use linera_bridge::{
    abi::{BridgeOperation, BridgeParameters},
    proof::DepositKey,
};
use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};

pub struct EvmBridgeAbi;

impl ContractAbi for EvmBridgeAbi {
    type Operation = BridgeOperation;
    type Response = ();
}

impl ServiceAbi for EvmBridgeAbi {
    type Query = Request;
    type QueryResponse = Response;
}
