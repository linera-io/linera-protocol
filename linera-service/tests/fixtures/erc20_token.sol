// SPDX-License-Identifier: MIT
// ERC20 instantiation

pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";

contract MyToken is ERC20 {
    constructor(uint256 initial_supply) ERC20("MyToken", "MTK") {
        _mint(msg.sender, initial_supply);
    }
}
