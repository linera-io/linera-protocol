// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {LineraToken} from "../LineraToken.sol";

contract LineraTokenTest is Test {
    function test_constructor_mints_supply_to_deployer() public {
        LineraToken t = new LineraToken("Foo", "FOO", 18, 100);
        assertEq(t.name(), "Foo");
        assertEq(t.symbol(), "FOO");
        assertEq(t.decimals(), 18);
        assertEq(t.totalSupply(), 100);
        assertEq(t.balanceOf(address(this)), 100);
    }
}
