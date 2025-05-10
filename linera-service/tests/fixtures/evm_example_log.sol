// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract ExampleEvent {
    uint64 value;
    event Constructor(uint64 value);
    event Increment(address from, uint64 input, uint64 value);

    constructor(uint64 start_value) {
        emit Instantiation(start_value);
    }

    function increment(uint64 input) external {
        value = value + input;
        emit Increment(msg.sender, input, value);
    }
}
