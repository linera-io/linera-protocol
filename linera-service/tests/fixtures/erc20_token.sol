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

    function balanceOf(address account) external view returns (uint256) {
        return child.balanceOf(account);
    }

    function transfer(address recipient, uint256 amount) external returns (bool) {
        return child.transfer(recipient, amount);
    }

    function allowance(address owner, address spender) external view returns (uint256) {
        return child.allowance(owner, spender);
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        return child.approve(spender, amount);
    }

    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool) {
        return child.transferFrom(sender, recipient, amount);
    }
}
