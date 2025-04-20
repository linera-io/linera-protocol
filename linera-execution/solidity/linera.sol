// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// Precompile keys:
// 0: try_call_application
// 1: try_query_application

library Linera {
  function try_call_application(bytes32 universal_address, bytes memory operation) internal returns (bytes memory) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(uint8(0));
    bytes memory input2 = abi.encodePacked(input1, universal_address, operation);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }

  function try_query_application(bytes32 universal_address, bytes memory argument) internal returns (bytes memory) {
    address precompile = address(0x0b);
    bytes1 input1 = bytes1(uint8(1));
    bytes memory input2 = abi.encodePacked(input1, universal_address, argument);
    (bool success, bytes memory output) = precompile.call(input2);
    require(success);
    return output;
  }

}
