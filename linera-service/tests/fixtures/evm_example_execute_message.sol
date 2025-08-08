// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "./Linera.sol";

contract ExampleExecuteMessage {
    uint64 value;
    Linera.opt_ChainId last_message_origin_chain_id;

    constructor(uint64 test_value) {
       require(test_value == 42);
    }

    function instantiate(bytes memory input) external {
        uint64 read_value = abi.decode(input, (uint64));
        value = read_value;
    }

    function execute_message(bytes memory input) external {
        uint64 increment = abi.decode(input, (uint64));
        value = value + increment;
        last_message_origin_chain_id = Linera.message_origin_chain_id();
    }

    function move_value_to_chain(bytes32 chain_id, uint64 moved_value) external {
        require(value >= moved_value);
        value = value - moved_value;
        bytes memory message = abi.encode(moved_value);
        Linera.send_message(chain_id, message);
    }

    function get_value() external view returns (uint64) {
        return value;
    }
}
