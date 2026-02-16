// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "LightClient.sol";

abstract contract Microchain {
    LightClient public immutable lightClient;
    bytes32 public immutable chainId;
    uint64 public latestHeight;

    constructor(address _lightClient, bytes32 _chainId) {
        lightClient = LightClient(_lightClient);
        chainId = _chainId;
    }

    function addBlock(bytes calldata data) external {
        BridgeTypes.Block memory blockValue = lightClient.verifyBlock(data);

        require(blockValue.header.chain_id.value.value == chainId, "chain id mismatch");
        require(blockValue.header.height.value == latestHeight + 1, "block height must be sequential");

        latestHeight = blockValue.header.height.value;
        _onBlock(blockValue);
    }

    /// Called after a block has been verified and accepted. Subcontracts implement
    /// this to extract and store application-specific data from the block.
    function _onBlock(BridgeTypes.Block memory blockValue) internal virtual;
}
