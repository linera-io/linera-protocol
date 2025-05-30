// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract BasicCheck {

    function failing_function() external view returns (uint64) {
        require(false);
        return 0;
    }

    function test_precompile_sha256() external view returns (uint64) {
        bytes memory v;
        bytes32 ret_zero;
        bytes32 ret_val = sha256(v);
        require(ret_zero != ret_val);
        return 0;
    }

    function check_contract_address(address address2) external view returns (uint64) {
      address address1 = address(this);
      require(address1 == address2);
      return 49;
    }
}
