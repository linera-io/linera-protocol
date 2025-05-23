// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract ExampleLongService {
    constructor(uint64 start_value) {
    }

    // An operation that costs more than 20.000.000 gas fuel.
    function too_long_run() external view returns (uint256) {
       uint256 v = 0;
       for (uint256 i=0; i<500; i++) {
           for (uint256 j=0; j<500; j++) {
               v = v + i + j;
           }
       }
       return v;
    }
}
