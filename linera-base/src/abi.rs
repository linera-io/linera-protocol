// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the notion of Application Binary Interface (ABI) for Linera
//! applications across Wasm and native architectures.

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

// ANCHOR: abi
/// A trait that includes all the types exported by a Linera application (both contract
/// and service).
pub trait Abi: ContractAbi + ServiceAbi {}
// ANCHOR_END: abi

// T::Parameters is duplicated for simplicity but it must match.
impl<T> Abi for T where T: ContractAbi + ServiceAbi {}

// ANCHOR: contract_abi
/// A trait that includes all the types exported by a Linera application contract.
pub trait ContractAbi {
    /// The type of operation executed by the application.
    ///
    /// Operations are transactions directly added to a block by the creator (and signer)
    /// of the block. Users typically use operations to start interacting with an
    /// application on their own chain.
    type Operation: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The response type of an application call.
    type Response: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;
}
// ANCHOR_END: contract_abi

// ANCHOR: service_abi
/// A trait that includes all the types exported by a Linera application service.
pub trait ServiceAbi {
    /// The type of a query receivable by the application's service.
    type Query: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The response type of the application's service.
    type QueryResponse: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;
}
// ANCHOR_END: service_abi

/// Marker trait to help importing contract types.
pub trait WithContractAbi {
    /// The contract types to import.
    type Abi: ContractAbi;
}

impl<A> ContractAbi for A
where
    A: WithContractAbi,
{
    type Operation = <<A as WithContractAbi>::Abi as ContractAbi>::Operation;
    type Response = <<A as WithContractAbi>::Abi as ContractAbi>::Response;
}

/// Marker trait to help importing service types.
pub trait WithServiceAbi {
    /// The service types to import.
    type Abi: ServiceAbi;
}

impl<A> ServiceAbi for A
where
    A: WithServiceAbi,
{
    type Query = <<A as WithServiceAbi>::Abi as ServiceAbi>::Query;
    type QueryResponse = <<A as WithServiceAbi>::Abi as ServiceAbi>::QueryResponse;
}
