// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI of the Counter Example Application

use async_graphql::{Request, Response};
use linera_sdk::abi::{ContractAbi, ServiceAbi};

/// The marker type that connects the types used to interface with the application.
pub struct Abi;

impl ContractAbi for Abi {
    type Operation = ();
    type Response = ();
}

impl ServiceAbi for Abi {
    type Query = Request;
    type QueryResponse = Response;
}
