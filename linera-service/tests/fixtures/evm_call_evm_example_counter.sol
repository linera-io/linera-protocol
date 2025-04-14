// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

interface IExternalContract {
  function increment(uint64 value) external returns (uint64);
  function get_value() external returns (uint64);
}


contract ExampleCallEvmCounter {
  address evm_address;
  constructor(address _evm_address) {
    evm_address = _evm_address;
  }
  function nest_increment(uint64 input) external returns (uint64) {
    IExternalContract externalContract = IExternalContract(evm_address);
    return externalContract.increment(input);
  }
  function nest_get_value() external returns (uint64) {
    IExternalContract externalContract = IExternalContract(evm_address);
    return externalContract.get_value();
  }
}
