// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract ExampleEvent {
    uint64 value;
    event Start(uint64 value);
    event Increment(uint64 input, uint64 value);

    constructor(uint64 start_value) {
        value = start_value;
        emit Start(start_value);
    }

    function increment(uint64 input) external {
        value = value + input;
        emit Increment(input, value);
    }
}
