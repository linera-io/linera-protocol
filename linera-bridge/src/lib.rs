// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Solidity source for the BridgeTypes library (generated).
pub const BRIDGE_TYPES_SOURCE: &str = include_str!("solidity/BridgeTypes.sol");

/// Solidity source for the FungibleTypes library (generated).
pub const FUNGIBLE_TYPES_SOURCE: &str = include_str!("solidity/FungibleTypes.sol");

/// Solidity source for the FungibleBridge contract.
pub const FUNGIBLE_BRIDGE_SOURCE: &str = include_str!("solidity/FungibleBridge.sol");

pub mod light_client;
pub mod microchain;

#[cfg(test)]
mod gas;
#[cfg(test)]
pub(crate) mod test_helpers;
