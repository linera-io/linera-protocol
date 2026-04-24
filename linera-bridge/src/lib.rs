// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

// -- On-chain (Wasm-compatible, always available) --

/// EVM receipt proof verification and deposit event parsing.
pub mod proof;

// -- Off-chain only (requires `not(chain)` / default features) --

/// EVM contract ABIs, relay clients, and Solidity sources.
#[cfg(feature = "offchain")]
pub mod evm;

/// Bridge monitoring: tracks in-flight EVM↔Linera bridging requests.
#[cfg(feature = "relay")]
pub mod monitor;

/// Relay server: HTTP proof endpoint + Linera chain inbox processing + EVM block forwarding.
#[cfg(feature = "relay")]
pub mod relay;

// -- Test-only modules --

/// Tests for the FungibleBridge EVM contract.
#[cfg(all(test, feature = "offchain"))]
mod fungible_bridge;

/// Gas usage measurements for LightClient and Microchain operations.
#[cfg(all(test, feature = "offchain"))]
mod gas;

/// Shared test helpers for EVM contract deployment and interaction.
#[cfg(all(test, feature = "offchain"))]
pub(crate) mod test_helpers;
