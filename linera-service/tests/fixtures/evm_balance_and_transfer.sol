// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract InnerContractCheck {
    constructor() payable {
    }

    function send_cash(address recipient, uint256 amount) external returns (uint64) {
        (bool success, ) = recipient.call{value: amount}("");
        require(success, "Native token transfer failed");
        return 1;
    }

    function get_balance(address account) external returns (uint256) {
        uint256 balance = account.balance;
        return balance;
    }

    function null_operation() payable external returns (uint256) {
        uint56 value = 0;
        return value;
    }
}
