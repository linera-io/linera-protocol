// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {DeployFungibleBridge} from "../script/DeployFungibleBridge.s.sol";
import {DeployLightClient} from "../script/DeployLightClient.s.sol";
import {DeployLineraToken} from "../script/DeployLineraToken.s.sol";
import {FungibleBridge} from "../FungibleBridge.sol";
import {LightClient} from "../LightClient.sol";
import {LineraToken} from "../LineraToken.sol";

contract DeployFungibleBridgeTest is Test {
    function setUp() public {
        vm.setEnv("LIGHT_CLIENT_ARGS_JSON_FILE", "test/fixtures/light-client-args.json");
        vm.setEnv("TOKEN_NAME", "TestToken");
        vm.setEnv("TOKEN_SYMBOL", "TT");
        vm.setEnv("TOKEN_SUPPLY", "1000000000000000000000");
    }

    function test_run_wires_constructor_args() public {
        LightClient lc = new DeployLightClient().run();
        LineraToken token = new DeployLineraToken().run();

        bytes32 chainId = bytes32(uint256(0xdeadbeef));
        bytes32 appId = bytes32(uint256(0xfeedface));

        vm.setEnv("LIGHT_CLIENT", vm.toString(address(lc)));
        vm.setEnv("BRIDGE_CHAIN_ID", vm.toString(chainId));
        vm.setEnv("TOKEN_ADDRESS", vm.toString(address(token)));
        vm.setEnv("FUNGIBLE_APP_ID", vm.toString(appId));

        FungibleBridge bridge = new DeployFungibleBridge().run();

        assertEq(address(bridge.lightClient()), address(lc));
        assertEq(address(bridge.token()), address(token));
    }
}
