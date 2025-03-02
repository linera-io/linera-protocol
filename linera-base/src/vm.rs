// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The virtual machines being supported.

use std::str::FromStr;

use async_graphql::scalar;
use derive_more::Display;
use linera_witty::{WitLoad, WitStore, WitType};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
