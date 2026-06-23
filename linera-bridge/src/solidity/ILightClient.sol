// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

/// @notice The narrow surface a consumer bridge (`Microchain`/`FungibleBridge`)
///         depends on, so the consumer never sees Linera block structure and the
///         `LightClient` can be swapped behind a timelock without migrating TVL.
interface ILightClient {
    /// Metadata for a block whose validator quorum was verified via
    /// `registerBlock`. A zero `eventsHash` means "unregistered".
    function registeredBlocks(bytes32 blockHash)
        external
        view
        returns (bytes32 eventsHash, uint64 height, bytes32 chainId, uint32 epoch);

    /// Proves that `eventBcs` sit at `positions` within transaction `txIndex` of
    /// the events a block commits to via `eventsHash`. Reverts unless they fold
    /// to `eventsHash` with the supplied siblings.
    function assertEventsCommitted(
        bytes32 eventsHash,
        bytes[] calldata eventBcs,
        uint32 txIndex,
        uint32 numTxs,
        uint32 numEventsInTx,
        uint32[] calldata positions,
        bytes32[] calldata siblings
    ) external pure;

    /// The Linera admin chain whose committees this client tracks. Two
    /// `LightClient`s are "the same Linera network" iff their `adminChainId`
    /// matches; used as the `setLightClient` sanity check.
    function adminChainId() external view returns (bytes32);
}
