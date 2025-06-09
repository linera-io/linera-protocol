// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract ExampleEmptyInstantiate {
    uint64 value;

    constructor() {
        value = 37;
    }

    function instantiate(bytes memory input) external {
        require(input.length == 0);
        value = 42;
    }

    function get_value() external view returns (uint64) {
        return value;
    }
}
