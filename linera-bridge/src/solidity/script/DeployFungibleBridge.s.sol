// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Script} from "forge-std/Script.sol";
import {FungibleBridge} from "../FungibleBridge.sol";
import {FungibleBurnEventDecoderV1} from "../FungibleBurnEventDecoderV1.sol";

contract DeployFungibleBridge is Script {
    function run() external returns (FungibleBridge bridge) {
        address lightClient = vm.envAddress("LIGHT_CLIENT");
        bytes32 chainId = vm.envBytes32("BRIDGE_CHAIN_ID");
        address token = vm.envAddress("TOKEN_ADDRESS");
        bytes32 fungibleAppId = vm.envBytes32("FUNGIBLE_APP_ID");
        bytes32 bridgeAppId = vm.envBytes32("BRIDGE_APP_ID");
        address pauseGuardian = vm.envAddress("PAUSE_GUARDIAN");
        address proposer = vm.envAddress("PROPOSER");
        address canceller = vm.envAddress("CANCELLER");
        uint256 timelockDelay = vm.envUint("TIMELOCK_DELAY");

        // Deploy the initial decoder for the current BurnEvent schema alongside
        // the bridge. Subsequent schema changes go through setDecoder with a
        // separately-deployed decoder.
        vm.startBroadcast();
        FungibleBurnEventDecoderV1 decoder = new FungibleBurnEventDecoderV1();
        bridge = new FungibleBridge(
            lightClient,
            chainId,
            token,
            fungibleAppId,
            bridgeAppId,
            address(decoder),
            pauseGuardian,
            proposer,
            canceller,
            timelockDelay
        );
        vm.stopBroadcast();

        require(address(bridge.lightClient()) == lightClient, "post-deploy lightClient mismatch");
        require(address(bridge.decoder()) == address(decoder), "post-deploy decoder mismatch");
    }
}
