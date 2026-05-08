// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The virtual machines being supported.

use std::{fmt, str::FromStr};

use allocative::Allocative;
use async_graphql::scalar;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::data_types::NativeApplicationKind;

#[derive(
    Clone,
    Copy,
    Default,
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
    /// A runtime-native application implemented directly in `linera-execution`. The
    /// associated bytecode hashes in the `ModuleId` are sentinel values and are not
    /// expected to resolve to any blob.
    Native(NativeApplicationKind),
}

impl fmt::Display for VmRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VmRuntime::Wasm => f.write_str("Wasm"),
            VmRuntime::Evm => f.write_str("Evm"),
            VmRuntime::Native(NativeApplicationKind::Fungible) => {
                f.write_str("Native(Fungible)")
            }
        }
    }
}

impl FromStr for VmRuntime {
    type Err = InvalidVmRuntime;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        match string {
            "wasm" => Ok(VmRuntime::Wasm),
            "evm" => Ok(VmRuntime::Evm),
            "native-fungible" => Ok(VmRuntime::Native(NativeApplicationKind::Fungible)),
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
    /// A read-only query.
    Query(Vec<u8>),
    /// A request to schedule an operation that can mutate the application state.
    Mutation(Vec<u8>),
}
