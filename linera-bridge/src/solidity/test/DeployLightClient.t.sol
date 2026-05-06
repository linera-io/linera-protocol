// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {DeployLightClient} from "../script/DeployLightClient.s.sol";
import {LightClient} from "../LightClient.sol";

contract DeployLightClientTest is Test {
    function setUp() public {
        vm.setEnv("LIGHT_CLIENT_ARGS_JSON_FILE", "test/fixtures/light-client-args.json");
    }

    function test_run_deploys_with_admin_chain_id_from_json() public {
        DeployLightClient script = new DeployLightClient();
        LightClient lc = script.run();
        assertEq(
            lc.adminChainId(),
            0xc175bb0579c7a182de91397710c39a3b27b241a163e2347fb458f297d1f583de
        );
    }
}
