/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;
contract SimpleToken {
    mapping(address => uint256) public balances;
    uint256 public totalSupply;
    event Transfer(address indexed from, address indexed to, uint256 value);
    constructor(uint256 initialSupply) {
        totalSupply = initialSupply;
        balances[msg.sender] = initialSupply;
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
