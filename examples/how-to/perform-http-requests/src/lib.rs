// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! ABI of the Counter Example Application

use async_graphql::{Request, Response};
use linera_sdk::{
    abi::{ContractAbi, ServiceAbi},
    graphql::GraphQLMutationRoot,
};
use serde::{Deserialize, Serialize};

/// The marker type that connects the types used to interface with the application.
pub struct Abi;

impl ContractAbi for Abi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for Abi {
    type Query = Request;
    type QueryResponse = Response;
}

/// Operations that the contract can handle.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Handles the HTTP response of a request made outside the contract.
    HandleHttpResponse(Vec<u8>),
    /// Performs an HTTP request inside the contract.
    PerformHttpRequest,
    /// Requests the service to perform the HTTP request as an oracle.
    UseServiceAsOracle,
}
