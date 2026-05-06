// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {DeployMockERC20} from "../script/DeployMockERC20.s.sol";
import {MockERC20} from "../MockERC20.sol";

contract DeployMockERC20Test is Test {
    function test_run_uses_env_var_metadata() public {
        vm.setEnv("TOKEN_NAME", "TestToken");
        vm.setEnv("TOKEN_SYMBOL", "TT");
        vm.setEnv("TOKEN_SUPPLY", "1000000000000000000000");

        MockERC20 token = new DeployMockERC20().run();

        assertEq(token.name(), "TestToken");
        assertEq(token.symbol(), "TT");
        assertEq(token.totalSupply(), 1_000_000_000_000_000_000_000);
    }
}
