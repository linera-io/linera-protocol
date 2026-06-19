// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {LightClient} from "../LightClient.sol";
import {ILightClient} from "../ILightClient.sol";

contract ILightClientConformanceTest is Test {
    bytes32 constant ADMIN_CHAIN = bytes32(uint256(0xAD));

    function _deploy() internal returns (LightClient) {
        address[] memory validators = new address[](1);
        validators[0] = vm.addr(1);
        uint64[] memory weights = new uint64[](1);
        weights[0] = 1;
        return new LightClient(validators, weights, ADMIN_CHAIN, 0, makeAddr("guardian"), makeAddr("proposer"));
    }

    /// LightClient is usable through the narrow ILightClient surface the consumer
    /// depends on: adminChainId() and the registeredBlocks() getter.
    function test_lightClient_satisfies_interface() public {
        ILightClient lc = ILightClient(address(_deploy()));

        assertEq(lc.adminChainId(), ADMIN_CHAIN, "adminChainId via interface");

        (bytes32 eventsHash,,,) = lc.registeredBlocks(bytes32(uint256(0xB10C)));
        assertEq(eventsHash, bytes32(0), "unregistered block has zero eventsHash via interface");
    }
}
