// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// -- On-chain (Wasm-compatible, always available) --

pub mod proof;

// -- Off-chain only (requires `not(chain)` / default features) --

#[cfg(not(feature = "chain"))]
/// Solidity source for the BridgeTypes library (generated).
pub const BRIDGE_TYPES_SOURCE: &str = include_str!("solidity/BridgeTypes.sol");

#[cfg(not(feature = "chain"))]
/// Solidity source for the FungibleTypes library (generated).
pub const FUNGIBLE_TYPES_SOURCE: &str = include_str!("solidity/FungibleTypes.sol");

#[cfg(not(feature = "chain"))]
/// Solidity source for the FungibleBridge contract.
pub const FUNGIBLE_BRIDGE_SOURCE: &str = include_str!("solidity/FungibleBridge.sol");

#[cfg(not(feature = "chain"))]
pub mod evm_client;
#[cfg(not(feature = "chain"))]
pub mod light_client;
#[cfg(not(feature = "chain"))]
pub mod microchain;

#[cfg(all(test, not(feature = "chain")))]
mod fungible_bridge;
#[cfg(all(test, not(feature = "chain")))]
mod gas;
#[cfg(all(test, not(feature = "chain")))]
pub(crate) mod test_helpers;
