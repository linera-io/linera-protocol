// SPDX-License-Identifier: MIT
// ERC20 instantiation

pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";

contract MyTokenInner is ERC20 {
    constructor(address sender, uint256 initial_supply) ERC20("MyTokenInner", "MTK") {
        _mint(sender, initial_supply);
    }
}

contract MyToken {
    MyTokenInner public child;
    uint256 total_supply;

    constructor(uint256 initial_supply) {
        total_supply = initial_supply;
        child = new MyTokenInner(msg.sender, initial_supply);
    }

    function totalSupply() external view returns (uint256) {
        return total_supply;
    }
}
