// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// Precompile keys:
// 0: try_call_application
// 1: try_query_application
// 2: send_message
// 3: message_id
// 4: message_is_bouncing

library Linera {
    function bcs_deserialize_offset_bool(uint256 pos, bytes memory input) internal pure returns (uint256, bool) {
        uint8 val = uint8(input[pos]);
        bool result = false;
        if (val == 1) {
            result = true;
        } else {
            require(val == 0);
        }
        return (pos + 1, result);
    }

    function bcs_deserialize_offset_bytes32(uint256 pos, bytes memory src) internal pure returns (uint256, bytes32) {
        bytes32 dest;
        assembly {
            dest := mload(add(add(src, 0x20), pos))
        }
        uint256 new_pos = pos + 32;
        return (new_pos, dest);
    }

    struct MessageId {
        bytes32 chain_id;
        uint64 block_height;
        uint32 index;
    }

    function bcs_deserialize_offset_MessageId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageId memory)
    {
        uint256 new_pos = pos;
        bytes32 chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_bytes32(new_pos, input);
        uint64 block_height;
        (new_pos, block_height) = bcs_deserialize_offset_uint64(new_pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, MessageId(chain_id, block_height, index));
    }

    function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input) internal pure returns (uint256, uint8) {
        require(pos < input.length, "Position out of bound");
        uint8 value = uint8(input[pos]);
        return (pos + 1, value);
    }

    function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input) internal pure returns (uint256, uint64) {
        require(pos + 7 < input.length, "Position out of bound");
        uint64 value = uint8(input[pos + 7]);
        for (uint256 i = 0; i < 7; i++) {
            value = value << 8;
            value += uint8(input[pos + 6 - i]);
        }
        return (pos + 8, value);
    }

    struct OptionMessageId {
        bool has_value;
        MessageId value;
    }

    function bcs_deserialize_offset_OptionMessageId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionMessageId memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        MessageId memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_MessageId(new_pos, input);
        }
        return (new_pos, OptionMessageId(true, value));
    }

    function bcs_deserialize_OptionMessageId(bytes memory input) public pure returns (OptionMessageId memory) {
        uint256 new_pos;
        OptionMessageId memory value;
        (new_pos, value) = bcs_deserialize_offset_OptionMessageId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_deserialize_offset_uint32(uint256 pos, bytes memory input) internal pure returns (uint256, uint32) {
        require(pos + 3 < input.length, "Position out of bound");
        uint32 value = uint8(input[pos + 3]);
        for (uint256 i = 0; i < 3; i++) {
            value = value << 8;
            value += uint8(input[pos + 2 - i]);
        }
        return (pos + 4, value);
    }

    enum MessageIsBouncing {
        NONE,
        IS_BOUNCING,
        NOT_BOUNCING
    }

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

    function send_message(bytes32 chain_id, bytes memory message) internal {
        address precompile = address(0x0b);
        bytes1 input1 = bytes1(uint8(2));
        bytes memory input2 = abi.encodePacked(input1, chain_id, message);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function message_id() internal returns (OptionMessageId memory) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(1);
        input1[0] = bytes1(uint8(3));
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        OptionMessageId memory output2 = bcs_deserialize_OptionMessageId(output1);
        return output2;
    }

    function message_is_bouncing() internal returns (MessageIsBouncing) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(1);
        input1[0] = bytes1(uint8(4));
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        MessageIsBouncing output2 = abi.decode(output1, (MessageIsBouncing));
        return output2;
    }
}
