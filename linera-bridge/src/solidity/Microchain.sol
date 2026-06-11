// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "LightClient.sol";

abstract contract Microchain {
    LightClient public immutable lightClient;
    bytes32 public immutable chainId;

    constructor(address _lightClient, bytes32 _chainId) {
        lightClient = LightClient(_lightClient);
        chainId = _chainId;
    }

    /// Verifies a block from its header proof and the events it commits to (`eventBcs` are the
    /// per-event BCS encodings, `eventsPerTx` how many belong to each transaction), then dispatches
    /// to the subclass. Subclasses MUST be idempotent under repeated calls for the same block: this
    /// contract does not gate on `signedHash`. The off-chain relayer relies on that idempotency to
    /// safely re-submit after partial settlement.
    function addBlock(bytes calldata blockProof, bytes[] calldata eventBcs, uint32[] calldata eventsPerTx) external {
        (BridgeTypes.BlockHeader memory header,) = lightClient.verifyBlockFromEvents(blockProof, eventBcs, eventsPerTx);
        require(header.chain_id.value.value == chainId, "chain id mismatch");
        _onBlock(header, eventBcs);
    }

    /// Called after a block has been verified and accepted. Subcontracts implement this to extract
    /// and store application-specific data from the header and the block's events (each entry of
    /// `eventBcs` is one event's BCS encoding).
    function _onBlock(BridgeTypes.BlockHeader memory header, bytes[] calldata eventBcs) internal virtual;
}
