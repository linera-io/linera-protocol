// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "./Linera.sol";

contract ExampleProcessStreams {
    uint64 total_value;
    uint8 public constant stream_key = 49;
    mapping(bytes32 => uint64) chain_values;

    function subscribe(bytes32 chain_id1, bytes32 application_id) external {
        bytes memory stream_name = abi.encodePacked(stream_key);
        Linera.subscribe_to_events(chain_id1, application_id, stream_name);
    }

    function increment_value(uint64 increment) external {
        total_value = total_value + increment;
        bytes memory stream_name = abi.encodePacked(stream_key);
        bytes memory value = abi.encode(total_value);
        Linera.linera_emit(stream_name, value);
    }

    function process_streams(Linera.StreamUpdate[] memory streams) external {
        bytes memory stream_name = abi.encodePacked(stream_key);
        uint256 n_entries = streams.length;
        for (uint256 i=0; i<n_entries; i++) {
            Linera.StreamUpdate memory update = streams[i];
            bytes32 chain_id = update.chain_id.value;
            for (uint32 index=update.previous_index; index<update.next_index; index++) {
                bytes memory result = Linera.read_event(chain_id, stream_name, index);
                uint64 value = abi.decode(result, (uint64));
                chain_values[chain_id] = value;
            }
        }
    }

    function get_value(bytes32 input) external view returns (uint64) {
        return chain_values[input];
    }
}
