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

    fn deserialize_operation(operation: Vec<u8>) -> Result<Self::Operation, String> {
        Ok(operation)
    }

    fn serialize_operation(operation: &Self::Operation) -> Result<Vec<u8>, String> {
        Ok(operation.to_vec())
    }

    fn deserialize_response(response: Vec<u8>) -> Result<Self::Response, String> {
        Ok(response)
    }

    fn serialize_response(response: Self::Response) -> Result<Vec<u8>, String> {
        Ok(response)
    }
}

impl ServiceAbi for EvmAbi {
    type Query = EvmQuery;
    type QueryResponse = Vec<u8>;
}
