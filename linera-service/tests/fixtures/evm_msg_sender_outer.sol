// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

interface CheckMsgSender {
    function check_msg_sender(address address2) external returns (uint64);
}

contract OuterContractCheck {
    function remote_check(address remote_address) external returns (uint64) {
        CheckMsgSender externalContract = CheckMsgSender(remote_address);
        address local_address = address(this);
        return externalContract.check_msg_sender(local_address);
    }
}
