// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "Microchain.sol";

contract MicrochainTest is Microchain {
    uint64 public blockCount;

    constructor(
        address _lightClient,
        bytes32 _chainId,
        address _pauseGuardian,
        address _proposer,
        address _canceller,
        uint256 _timelockDelay
    ) Microchain(_lightClient, _chainId, _pauseGuardian, _proposer, _canceller, _timelockDelay) {}

    function _onBlock(BridgeTypes.Block memory) internal override {
        blockCount++;
    }
}
