// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

/// @notice The narrow surface a consumer bridge (`Microchain`/`FungibleBridge`)
///         depends on, so the consumer never sees the light client's committee /
///         certificate-verification internals and the `LightClient` can be
///         swapped behind a timelock without migrating TVL.
/// @dev    On this network the light client verifies full Linera certificates
///         (committee signatures over the block) — there are no merkle event
///         inclusion proofs. The consumer only needs to (a) verify a block and
///         (b) learn which Linera network the client tracks.
interface ILightClient {
    /// Verifies a serialized confirmed-block certificate and returns the decoded
    /// block together with its signed hash. Reverts if the certificate is not
    /// backed by a quorum of the block's epoch committee.
    function verifyBlock(bytes calldata data) external view returns (BridgeTypes.Block memory, bytes32);

    /// The Linera admin chain whose committees this client tracks. Two
    /// `LightClient`s are "the same Linera network" iff their `adminChainId`
    /// matches; used as the light-client-update sanity check.
    function adminChainId() external view returns (bytes32);
}
