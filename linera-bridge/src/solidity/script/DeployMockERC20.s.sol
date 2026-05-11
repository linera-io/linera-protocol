// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Script} from "forge-std/Script.sol";
import {MockERC20} from "../MockERC20.sol";

contract DeployMockERC20 is Script {
    function run() external returns (MockERC20 token) {
        string memory name = vm.envOr("TOKEN_NAME", string("TestToken"));
        string memory symbol = vm.envOr("TOKEN_SYMBOL", string("TT"));
        uint256 supply = vm.envOr("TOKEN_SUPPLY", uint256(1_000_000_000_000_000_000_000));

        vm.broadcast();
        token = new MockERC20(name, symbol, supply);
    }
}
