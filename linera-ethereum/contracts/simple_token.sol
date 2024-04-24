/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;
contract SimpleToken {
    mapping(address => uint256) public balances;
    uint256 public totalSupply;
    event Types(address indexed from, address to, uint256 val0, uint64 val1, int64 val2, uint32 val3, int32 val4, uint16 val5, int16 val6, uint8 val7, int8 val8, bool val9);
    event Initial(address, uint256 amount);
    event Transfer(address indexed from, address indexed to, uint256 amount);
    constructor(uint256 initialSupply) {
        totalSupply = initialSupply;
        balances[msg.sender] = initialSupply;
        emit Initial(msg.sender, initialSupply);
        uint256 val0;
        val0 = 239675476885367459284564394732743434463843674346373355625;
        uint64 val1;
        val1 = 4611686018427387904;
        int64 val2;
        val2 = -1152921504606846976;
        uint32 val3;
        val3 = 1073726139;
        int32 val4;
        val4 = -1072173379;
        uint16 val5;
        val5 = 16261;
        int16 val6;
        val6 = -16249;
        uint8 val7;
        val7 = 135;
        int8 val8;
        val8 = -120;
        bool val9;
        val9 = true;
        emit Types(msg.sender, msg.sender, val0, val1, val2, val3, val4, val5, val6, val7, val8, val9);
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
