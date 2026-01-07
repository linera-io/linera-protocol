// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.20;

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

    function call_back(address caller, uint256 value) public {
        CounterFactory cf = CounterFactory(caller);
        uint256 value2 = cf.get_value();
        require(value == value2);
    }
}

// Main contract that creates subcontracts
contract CounterFactory {
    Counter[] public counters;
    uint256 reentrant_value;

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

    function reentrant_test(uint256 index, uint256 value) public {
        reentrant_value = value;
        Counter counter = counters[index];
        counter.call_back(address(this), value);
    }

    function get_value() public returns (uint256) {
        return reentrant_value;
    }

    function test_code_length(uint256 index) public {
        Counter counter = counters[index];
        require(address(counter).code.length > 0);
    }

    function create_two_counters(uint256 initialValue) public {
        Counter counter1 = new Counter(initialValue);
        Counter counter2 = new Counter(initialValue);
        uint256 value1 = counter1.get_value();
        uint256 value2 = counter2.get_value();
        require(value1 == value2, "value1 should be equal to value2");
        require(value1 == initialValue);
    }
}
