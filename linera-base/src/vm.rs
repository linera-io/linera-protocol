// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The virtual machines being supported.

use std::str::FromStr;

use allocative::Allocative;
use alloy_primitives::U256;
use async_graphql::scalar;
use derive_more::Display;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::data_types::Amount;

#[derive(
    Clone,
    Copy,
    Default,
    Display,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    WitType,
    WitStore,
    WitLoad,
    Debug,
    Allocative,
)]
#[cfg_attr(with_testing, derive(test_strategy::Arbitrary))]
/// The virtual machine runtime
pub enum VmRuntime {
    /// The Wasm virtual machine
    #[default]
    Wasm,
    /// The Evm virtual machine
    Evm,
}

impl FromStr for VmRuntime {
    type Err = InvalidVmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            "wasm" => Ok(VmRuntime::Wasm),
            "evm" => Ok(VmRuntime::Evm),
            unknown => Err(InvalidVmRuntime(unknown.to_owned())),
        }
    }
}

scalar!(VmRuntime);

/// Error caused by invalid VM runtimes
#[derive(Clone, Debug, Error)]
#[error("{0:?} is not a valid virtual machine runtime")]
pub struct InvalidVmRuntime(String);

/// The possible types of queries for an EVM contract
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum EvmQuery {
    /// Account state
    AccountInfo,
    /// Storage query
    Storage(U256),
    /// A read-only query.
    Query(Vec<u8>),
    /// A request to schedule an operation that can mutate the application state.
    Operation(Vec<u8>),
    /// A request to schedule operations that can mutate the application state.
    Operations(Vec<Vec<u8>>),
}

/// An EVM operation containing a value and argument data.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct EvmOperation {
    /// The amount being transferred.
    pub value: alloy_primitives::U256,
    /// The encoded argument data.
    pub argument: Vec<u8>,
}

impl EvmOperation {
    /// Creates an EVM operation with a specified amount and function input.
    pub fn new(amount: Amount, argument: Vec<u8>) -> Self {
        Self {
            value: amount.into(),
            argument,
        }
    }

    /// Converts the input to a `Vec<u8>` if possible.
    pub fn to_bytes(&self) -> Result<Vec<u8>, bcs::Error> {
        bcs::to_bytes(&self)
    }

    /// Creates an `EvmQuery` from the input.
    pub fn to_evm_query(&self) -> Result<EvmQuery, bcs::Error> {
        Ok(EvmQuery::Operation(self.to_bytes()?))
    }
}

/// The instantiation argument to EVM smart contracts.
/// `value` is the amount being transferred.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct EvmInstantiation {
    /// The initial value put in the instantiation of the contract.
    pub value: alloy_primitives::U256,
    /// The input to the `fn instantiate` of the EVM smart contract.
    pub argument: Vec<u8>,
}
