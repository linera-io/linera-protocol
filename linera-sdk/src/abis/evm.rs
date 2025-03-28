// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An ABI for applications that implement an EVM runtime.
use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    vm::EvmQuery,
};

/// An ABI for applications that implement an EVM runtime.
pub struct EvmAbi;

impl ContractAbi for EvmAbi {
    type Operation = Vec<u8>;
    type Response = Vec<u8>;

    fn deserialize_operation(operation: Vec<u8>) -> Self::Operation {
        operation
    }

    fn serialize_response(response: Self::Response) -> Vec<u8> {
        response
    }
}

impl ServiceAbi for EvmAbi {
    type Query = EvmQuery;
    type QueryResponse = Vec<u8>;
}
