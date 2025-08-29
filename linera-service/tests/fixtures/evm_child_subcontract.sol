// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0


// Simple subcontract
contract Counter {
    uint256 public count;

    constructor(uint256 _initialValue) {
        count = _initialValue;
    }

    function increment() public {
        count++;
    }

    function get_value() external returns (uint256) {
        return count;
    }
}

// Main contract that creates subcontracts
contract CounterFactory {
    Counter[] public counters;

    function createCounter(uint256 initialValue) public returns (address) {
        Counter newCounter = new Counter(initialValue);
        counters.push(newCounter);
        return address(newCounter);
    }

    function get_address(uint256 index) external returns (address) {
        return address(counters[index]);
    }
}
