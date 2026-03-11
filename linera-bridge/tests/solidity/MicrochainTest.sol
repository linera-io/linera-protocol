// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "Microchain.sol";

contract MicrochainTest is Microchain {
    uint64 public blockCount;

    constructor(address _lightClient, bytes32 _chainId, uint64 _latestHeight)
        Microchain(_lightClient, _chainId, _latestHeight)
    {}

    function _onBlock(BridgeTypes.Block memory) internal override {
        blockCount++;
    }
}
