// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

contract InnerContractCheck {
    function check_msg_sender(address address2) external returns (uint64) {
        address address1 = msg.sender;
        require(address1 == address2);
        return 49;
    }
}
