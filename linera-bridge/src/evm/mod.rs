// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM contract ABIs, relay clients, and Solidity sources for the Linera→EVM bridge.

/// EVM relay client for pushing committee updates to the LightClient contract.
pub mod client;

/// LightClient Solidity ABI (committee/epoch management on EVM).
pub mod light_client;

/// Microchain Solidity ABI (block relay on EVM).
pub mod microchain;

/// Solidity source for the BridgeTypes library (generated).
pub const BRIDGE_TYPES_SOURCE: &str = include_str!("../solidity/BridgeTypes.sol");

/// Solidity source for the FungibleTypes library (generated).
pub const FUNGIBLE_TYPES_SOURCE: &str = include_str!("../solidity/FungibleTypes.sol");

/// Solidity source for the FungibleBridge contract.
pub const FUNGIBLE_BRIDGE_SOURCE: &str = include_str!("../solidity/FungibleBridge.sol");
