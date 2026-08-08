// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! EVM contract ABIs, relay clients, and Solidity sources for the Linera→EVM bridge.

/// EVM relay client for pushing committee updates to the LightClient contract.
pub mod client;

/// LightClient Solidity ABI (committee/epoch management on EVM).
pub mod light_client;

/// Solidity source for the BridgeTypes library (generated).
pub const BRIDGE_TYPES_SOURCE: &str = include_str!("../solidity/BridgeTypes.sol");

/// Solidity source for the WrappedFungibleTypesV1 library (generated). Versioned
/// in lockstep with `FungibleBurnEventDecoderV1`.
pub const WRAPPED_FUNGIBLE_TYPES_V1_SOURCE: &str =
    include_str!("../solidity/WrappedFungibleTypesV1.sol");

/// Solidity source for the FungibleBridge contract.
pub const FUNGIBLE_BRIDGE_SOURCE: &str = include_str!("../solidity/FungibleBridge.sol");

/// Solidity source for the Microchain abstract contract.
pub const MICROCHAIN_SOURCE: &str = include_str!("../solidity/Microchain.sol");

/// Solidity source for the LightClient contract.
pub const LIGHTCLIENT_SOURCE: &str = include_str!("../solidity/LightClient.sol");

/// Solidity source for the ILightClient consumer interface.
pub const ILIGHTCLIENT_SOURCE: &str = include_str!("../solidity/ILightClient.sol");

/// Solidity source for the IBurnEventDecoder interface.
pub const IBURN_EVENT_DECODER_SOURCE: &str = include_str!("../solidity/IBurnEventDecoder.sol");

/// Solidity source for the FungibleBurnEventDecoderV1 contract.
pub const FUNGIBLE_BURN_EVENT_DECODER_V1_SOURCE: &str =
    include_str!("../solidity/FungibleBurnEventDecoderV1.sol");
