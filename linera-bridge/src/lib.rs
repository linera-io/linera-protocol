// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// -- Off-chain only (requires `not(chain)` / default features) --

/// EVM contract ABIs, relay clients, and Solidity sources.
#[cfg(not(feature = "chain"))]
pub mod evm;

// -- Test-only modules --

/// Tests for the FungibleBridge EVM contract.
#[cfg(all(test, not(feature = "chain")))]
mod fungible_bridge;

/// Gas usage measurements for LightClient and Microchain operations.
#[cfg(all(test, not(feature = "chain")))]
mod gas;

/// Shared test helpers for EVM contract deployment and interaction.
#[cfg(all(test, not(feature = "chain")))]
pub(crate) mod test_helpers;
