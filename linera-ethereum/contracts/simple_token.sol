/// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;
contract SimpleToken {
    mapping(address => uint256) public balances;
    uint256 public totalSupply;

    event Initial(address, uint256 amount);
    event Transfer(address indexed from, address indexed to, uint256 amount);

    constructor(uint256 initialSupply) {
        totalSupply = initialSupply;
        balances[msg.sender] = initialSupply;
        emit Initial(msg.sender, initialSupply);
    }

    function transfer(address to, uint256 value) public returns (bool) {
        require(value > 0, "Value must be greater than 0");
        require(balances[msg.sender] >= value, "Insufficient balance");
        balances[msg.sender] -= value;
        balances[to] += value;
        emit Transfer(msg.sender, to, value);
        return true;
    }

    function balanceOf(address account) public view returns (uint256) {
        return balances[account];
    }
}
