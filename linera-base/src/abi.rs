// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the notion of Application Binary Interface (ABI) for Linera
//! applications across Wasm and native architectures.

use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// A trait that includes all the types exported by a Linera application.
pub trait ContractAbi {
    /// Initialization argument passed to a new application on the chain that created it
    /// (e.g. an initial amount of tokens minted).
    ///
    /// To share configuration data on every chain, use parameters instead.
    type InitializationArgument: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// Immutable parameters specific to this application (e.g. the name of a token).
    type Parameters: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type of operations executed by the application.
    ///
    /// Operations are transactions directly added to a block by the creator (and signer)
    /// of the block. Users typically use operations to start interacting with an
    /// application on their own chain.
    type Operation: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type of effects executed by the application.
    ///
    /// Effects are executed when a message created by the same application is received
    /// from another chain and accepted in a block.
    type Effect: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type of arguments used to call this application from another application on the chain.
    type ApplicationCall: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type of arguments used to call a session of this application from another application on the chain.
    ///
    /// Sessions are temporary objects that may be spawned by an application call. Once
    /// created, they must be consumed before the current transaction ends.
    type SessionCall: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The type for the state of a session.
    type SessionState: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;

    /// The response type of an application call.
    type Response: Serialize + DeserializeOwned + Send + Sync + Debug + 'static;
}

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
    type Effect = <<A as WithContractAbi>::Abi as ContractAbi>::Effect;
    type SessionCall = <<A as WithContractAbi>::Abi as ContractAbi>::SessionCall;
    type Response = <<A as WithContractAbi>::Abi as ContractAbi>::Response;
    type SessionState = <<A as WithContractAbi>::Abi as ContractAbi>::SessionState;
}
