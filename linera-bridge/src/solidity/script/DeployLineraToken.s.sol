// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Script} from "forge-std/Script.sol";
import {LineraToken} from "../LineraToken.sol";

contract DeployLineraToken is Script {
    function run() external returns (LineraToken token) {
        string memory name = vm.envOr("TOKEN_NAME", string("LineraToken"));
        string memory symbol = vm.envOr("TOKEN_SYMBOL", string("LIN"));
        uint256 supply = vm.envOr("TOKEN_SUPPLY", uint256(1_000_000_000_000_000_000_000));
        uint8 decimals_ = uint8(vm.envOr("TOKEN_DECIMALS", uint256(18)));

        vm.broadcast();
        token = new LineraToken(name, symbol, decimals_, supply);
    }
}
