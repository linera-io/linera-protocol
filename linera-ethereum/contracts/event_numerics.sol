/// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;
contract EventNumerics {
    uint256 total_value;

    event Types(address indexed from, address to, uint256 val0, uint64 val1, int64 val2, uint32 val3, int32 val4, uint16 val5, int16 val6, uint8 val7, int8 val8, bool val9);

    constructor(uint256 initialSupply) {
        total_value = initialSupply;
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
}
