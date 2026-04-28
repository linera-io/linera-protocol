// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "LightClient.sol";

abstract contract Microchain {
    LightClient public immutable lightClient;
    bytes32 public immutable chainId;
    mapping(bytes32 => bool) public verifiedBlocks;

    constructor(address _lightClient, bytes32 _chainId) {
        lightClient = LightClient(_lightClient);
        chainId = _chainId;
    }

    /// Verifies a certificate and accepts the block if it matches this chain.
    ///
    /// Note: this contract does NOT check `previous_block_hash` or enforce
    /// sequential block heights. This is safe because `ConfirmedBlockCertificate`
    /// implies BFT-finalized canonicality — a quorum of validators signed this
    /// specific block at this height, so no conflicting block can exist.
    /// Blocks can be submitted in any order; the `verifiedBlocks` mapping
    /// prevents duplicate processing.
    function addBlock(bytes calldata data) external {
        (BridgeTypes.Block memory blockValue, bytes32 signedHash) = lightClient.verifyBlock(data);

        require(!verifiedBlocks[signedHash], "block already verified");
        require(blockValue.header.chain_id.value.value == chainId, "chain id mismatch");

        verifiedBlocks[signedHash] = true;
        _onBlock(blockValue);
    }

    /// Called after a block has been verified and accepted. Subcontracts implement
    /// this to extract and store application-specific data from the block.
    function _onBlock(BridgeTypes.Block memory blockValue) internal virtual;
}
