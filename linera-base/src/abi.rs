// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the notion of Application Binary Interface (ABI) for Linera
//! applications across Wasm and native architectures.

use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

// ANCHOR: abi
/// A trait that includes all the types exported by a Linera application (both contract
/// and service).
pub trait Abi: ContractAbi + ServiceAbi {}
// ANCHOR_END: abi

// T::Parameters is duplicated for simplicity but it must match.
impl<T> Abi for T where T: ContractAbi + ServiceAbi<Parameters = <T as ContractAbi>::Parameters> {}

// ANCHOR: contract_abi
/// A trait that includes all the types exported by a Linera application contract.
pub trait ContractAbi {
    /// Immutable parameters specific to this application (e.g. the name of a token).
    type Parameters: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// Initialization argument passed to a new application on the chain that created it
    /// (e.g. an initial amount of tokens minted).
    ///
    /// To share configuration data on every chain, use [`ContractAbi::Parameters`]
    /// instead.
    type InitializationArgument: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type of operation executed by the application.
    ///
    /// Operations are transactions directly added to a block by the creator (and signer)
    /// of the block. Users typically use operations to start interacting with an
    /// application on their own chain.
    type Operation: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type of message executed by the application.
    ///
    /// Messages are executed when a message created by the same application is received
    /// from another chain and accepted in a block.
    type Message: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The argument type when this application is called from another application on the same chain.
    type ApplicationCall: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The argument type when a session of this application is called from another
    /// application on the same chain.
    ///
    /// Sessions are temporary objects that may be spawned by an application call. Once
    /// created, they must be consumed before the current transaction ends.
    type SessionCall: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type for the state of a session.
    type SessionState: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The response type of an application call.
    type Response: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;
}
// ANCHOR_END: contract_abi

// ANCHOR: service_abi
/// A trait that includes all the types exported by a Linera application service.
pub trait ServiceAbi {
    /// Immutable parameters specific to this application (e.g. the name of a token).
    type Parameters: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

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
    type InitializationArgument =
        <<A as WithContractAbi>::Abi as ContractAbi>::InitializationArgument;
    type Parameters = <<A as WithContractAbi>::Abi as ContractAbi>::Parameters;
    type Operation = <<A as WithContractAbi>::Abi as ContractAbi>::Operation;
    type ApplicationCall = <<A as WithContractAbi>::Abi as ContractAbi>::ApplicationCall;
    type Message = <<A as WithContractAbi>::Abi as ContractAbi>::Message;
    type SessionCall = <<A as WithContractAbi>::Abi as ContractAbi>::SessionCall;
    type Response = <<A as WithContractAbi>::Abi as ContractAbi>::Response;
    type SessionState = <<A as WithContractAbi>::Abi as ContractAbi>::SessionState;
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
    type Parameters = <<A as WithServiceAbi>::Abi as ServiceAbi>::Parameters;
}
