// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// Simple subcontract
contract Counter {
    uint256 public count;

    constructor(uint256 _initialValue) payable {
        count = _initialValue;
    }

    function increment() public {
        count++;
    }

    function get_value() external returns (uint256) {
        return count;
    }

    function get_balance(address account) external returns (uint256) {
        uint256 balance = account.balance;
        return balance;
    }
}

// Main contract that creates subcontracts
contract CounterFactory {
    Counter[] public counters;

    constructor() payable {
    }

    function createCounter(uint256 initialValue) public returns (address) {
        Counter newCounter = new Counter{value: 1000000000000000000}(initialValue);
        counters.push(newCounter);
        return address(newCounter);
    }

    function get_address(uint256 index) external returns (address) {
        return address(counters[index]);
    }

    function get_balance(address account) external returns (uint256) {
        uint256 balance = account.balance;
        return balance;
    }

    function remote_increment(uint256 index) external {
        Counter counter = counters[index];
        counter.increment();
    }

    function remote_value(uint256 index) external returns (uint256) {
        Counter counter = counters[index];
        return counter.get_value();
    }
}
