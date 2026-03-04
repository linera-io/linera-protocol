// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Solidity source for the BridgeTypes library (generated).
pub const BRIDGE_TYPES_SOURCE: &str = include_str!("solidity/BridgeTypes.sol");

/// Solidity source for the WrappedFungibleTypes library (generated).
pub const WRAPPED_FUNGIBLE_TYPES_SOURCE: &str = include_str!("solidity/WrappedFungibleTypes.sol");

/// Solidity source for the FungibleBridge contract.
pub const FUNGIBLE_BRIDGE_SOURCE: &str = include_str!("solidity/FungibleBridge.sol");

pub mod evm_client;
pub mod light_client;
pub mod microchain;

#[cfg(test)]
mod fungible_bridge;
#[cfg(test)]
mod gas;
#[cfg(test)]
pub(crate) mod test_helpers;
