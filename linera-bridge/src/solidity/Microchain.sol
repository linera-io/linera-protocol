// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "LightClient.sol";

abstract contract Microchain {
    LightClient public immutable lightClient;
    bytes32 public immutable chainId;
    uint64 public nextExpectedHeight;
    mapping(bytes32 => bool) public verifiedBlocks;

    constructor(address _lightClient, bytes32 _chainId, uint64 _nextExpectedHeight) {
        lightClient = LightClient(_lightClient);
        chainId = _chainId;
        nextExpectedHeight = _nextExpectedHeight;
    }

    /// Verifies a certificate and accepts the block if it matches this chain and
    /// the next expected height.
    ///
    /// Note: this contract does NOT check `previous_block_hash` to link blocks
    /// into a hash chain. This is safe because `ConfirmedBlockCertificate`
    /// implies BFT-finalized canonicality — a quorum of validators signed this
    /// specific block at this height, so no conflicting block can exist.
    /// If this assumption ever changes at the protocol layer, a
    /// `previous_block_hash` check should be added here.
    function addBlock(bytes calldata data) external {
        (BridgeTypes.Block memory blockValue, bytes32 signedHash) = lightClient.verifyBlock(data);

        require(!verifiedBlocks[signedHash], "block already verified");
        require(blockValue.header.chain_id.value.value == chainId, "chain id mismatch");
        require(blockValue.header.height.value == nextExpectedHeight, "block height must be sequential");

        nextExpectedHeight = blockValue.header.height.value + 1;
        verifiedBlocks[signedHash] = true;
        _onBlock(blockValue);
    }

    /// Called after a block has been verified and accepted. Subcontracts implement
    /// this to extract and store application-specific data from the block.
    function _onBlock(BridgeTypes.Block memory blockValue) internal virtual;
}
