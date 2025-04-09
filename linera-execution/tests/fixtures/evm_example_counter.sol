// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract ExampleCounter {
  uint64 value;
  constructor(uint64 start_value) {
    value = start_value;
  }

  function increment(uint64 input) external returns (uint64) {
    value = value + input;
    return value;
  }

  function get_value() external view returns (uint64) {
    return value;
  }

}
