// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "LightClient.sol";

abstract contract Microchain {
    LightClient public immutable lightClient;
    bytes32 public immutable chainId;

    constructor(address _lightClient, bytes32 _chainId) {
        lightClient = LightClient(_lightClient);
        chainId = _chainId;
    }
}
