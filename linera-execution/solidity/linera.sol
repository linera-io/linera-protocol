// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

// This library provides Linera functionalities to EVM contracts
// It should not be modified.

// Precompile keys:
// (0,0): chain_id
// (0,1): application_creator_chain_id
// (0,2): chain_ownership
// (0,3): read data blob
// (0,4): assert data blob exists
// (1,0): try_call_application
// (1,1): validation round
// (1,2): send_message
// (1,3): message_id
// (1,4): message_is_bouncing
// (2,0): try_query_application
library Linera {
    function bcs_deserialize_offset_bytes32(uint256 pos, bytes memory src) internal pure returns (uint256, bytes32) {
        // First offset of 0x20 is because the first 32 bytes contains the size of src.
        bytes32 dest;
        assembly {
            dest := mload(add(add(src, 0x20), pos))
        }
        uint256 new_pos = pos + 32;
        return (new_pos, dest);
    }

    function bcs_deserialize_offset_address(uint256 pos, bytes memory src) internal pure returns (uint256, address) {
        address dest;
        assembly {
            dest := mload(add(add(src, 0x20), pos))
        }
        uint256 new_pos = pos + 20;
        return (new_pos, dest);
    }

    function bcs_deserialize_bytes32(bytes memory input) public pure returns (bytes32) {
        uint256 new_pos;
        bytes32 value;
        (new_pos, value) = bcs_deserialize_offset_bytes32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input) internal pure returns (uint256, uint64) {
        uint64 value = uint8(input[pos + 7]);
        for (uint256 i = 0; i < 7; i++) {
            value = value << 8;
            value += uint8(input[pos + 6 - i]);
        }
        return (pos + 8, value);
    }

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

    function bcs_deserialize_offset_uint32(uint256 pos, bytes memory input) internal pure returns (uint256, uint32) {
        uint32 value = uint8(input[pos + 3]);
        for (uint256 i = 0; i < 3; i++) {
            value = value << 8;
            value += uint8(input[pos + 2 - i]);
        }
        return (pos + 4, value);
    }

    function bcs_deserialize_uint32(bytes memory input) public pure returns (uint32) {
        uint256 new_pos;
        uint32 value;
        (new_pos, value) = bcs_deserialize_offset_uint32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_deserialize_offset_len(uint256 pos, bytes memory input) internal pure returns (uint256, uint256) {
        uint256 idx = 0;
        while (true) {
            if (uint8(input[pos + idx]) < 128) {
                uint256 result = 0;
                uint256 power = 1;
                for (uint256 u = 0; u < idx; u++) {
                    uint8 val = uint8(input[pos + u]) - 128;
                    result += power * uint256(val);
                    power *= 128;
                }
                result += power * uint8(input[pos + idx]);
                uint256 new_pos = pos + idx + 1;
                return (new_pos, result);
            }
            idx += 1;
        }
    }

    function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input) internal pure returns (uint256, uint8) {
        uint8 value = uint8(input[pos]);
        return (pos + 1, value);
    }

    struct TimeDelta {
        uint64 value;
    }

    function bcs_deserialize_offset_TimeDelta(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, TimeDelta memory)
    {
        uint256 new_pos = pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(new_pos, input);
        return (new_pos, TimeDelta(value));
    }

    struct opt_TimeDelta {
        bool has_value;
        TimeDelta value;
    }

    function bcs_deserialize_offset_opt_TimeDelta(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_TimeDelta memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        TimeDelta memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_TimeDelta(new_pos, input);
        }
        return (new_pos, opt_TimeDelta(true, value));
    }

    struct TimeoutConfig {
        opt_TimeDelta fast_round_duration;
        TimeDelta base_timeout;
        TimeDelta timeout_increment;
        TimeDelta fallback_duration;
    }

    function bcs_deserialize_offset_TimeoutConfig(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, TimeoutConfig memory)
    {
        uint256 new_pos = pos;
        opt_TimeDelta memory fast_round_duration;
        (new_pos, fast_round_duration) = bcs_deserialize_offset_opt_TimeDelta(new_pos, input);
        TimeDelta memory base_timeout;
        (new_pos, base_timeout) = bcs_deserialize_offset_TimeDelta(new_pos, input);
        TimeDelta memory timeout_increment;
        (new_pos, timeout_increment) = bcs_deserialize_offset_TimeDelta(new_pos, input);
        TimeDelta memory fallback_duration;
        (new_pos, fallback_duration) = bcs_deserialize_offset_TimeDelta(new_pos, input);
        return (new_pos, TimeoutConfig(fast_round_duration, base_timeout, timeout_increment, fallback_duration));
    }

    struct AccountOwner {
        uint8 choice;
        // choice=0 corresponds to Reserved
        uint8 reserved;
        // choice=1 corresponds to Address32
        bytes32 address32;
        // choice=2 corresponds to Address20
        address address20;
    }

    function bcs_deserialize_offset_AccountOwner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AccountOwner memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        uint8 reserved;
        if (choice == 0) {
            (new_pos, reserved) = bcs_deserialize_offset_uint8(new_pos, input);
        }
        bytes32 address32;
        if (choice == 1) {
            (new_pos, address32) = bcs_deserialize_offset_bytes32(new_pos, input);
        }
        address address20;
        if (choice == 2) {
            (new_pos, address20) = bcs_deserialize_offset_address(new_pos, input);
        }
        return (new_pos, AccountOwner(choice, reserved, address32, address20));
    }

    function bcs_deserialize_offset_seq_AccountOwner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AccountOwner[] memory)
    {
        uint256 new_pos;
        uint256 len;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        AccountOwner[] memory result;
        result = new AccountOwner[](len);
        AccountOwner memory value;
        for (uint256 i = 0; i < len; i++) {
            (new_pos, value) = bcs_deserialize_offset_AccountOwner(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    struct key_values_AccountOwner_uint64 {
        AccountOwner key;
        uint64 value;
    }

    function bcs_deserialize_offset_key_values_AccountOwner_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_AccountOwner_uint64 memory)
    {
        uint256 new_pos = pos;
        AccountOwner memory key;
        (new_pos, key) = bcs_deserialize_offset_AccountOwner(new_pos, input);
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(new_pos, input);
        return (new_pos, key_values_AccountOwner_uint64(key, value));
    }

    function bcs_deserialize_offset_seq_key_values_AccountOwner_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_AccountOwner_uint64[] memory)
    {
        uint256 new_pos;
        uint256 len;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        key_values_AccountOwner_uint64[] memory result;
        result = new key_values_AccountOwner_uint64[](len);
        key_values_AccountOwner_uint64 memory value;
        for (uint256 i = 0; i < len; i++) {
            (new_pos, value) = bcs_deserialize_offset_key_values_AccountOwner_uint64(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    struct ChainOwnership {
        AccountOwner[] super_owners;
        key_values_AccountOwner_uint64[] owners;
        uint32 multi_leader_rounds;
        bool open_multi_leader_rounds;
        TimeoutConfig timeout_config;
    }

    function bcs_deserialize_offset_ChainOwnership(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ChainOwnership memory)
    {
        uint256 new_pos = pos;
        AccountOwner[] memory super_owners;
        (new_pos, super_owners) = bcs_deserialize_offset_seq_AccountOwner(new_pos, input);
        key_values_AccountOwner_uint64[] memory owners;
        (new_pos, owners) = bcs_deserialize_offset_seq_key_values_AccountOwner_uint64(new_pos, input);
        uint32 multi_leader_rounds;
        (new_pos, multi_leader_rounds) = bcs_deserialize_offset_uint32(new_pos, input);
        bool open_multi_leader_rounds;
        (new_pos, open_multi_leader_rounds) = bcs_deserialize_offset_bool(new_pos, input);
        TimeoutConfig memory timeout_config;
        (new_pos, timeout_config) = bcs_deserialize_offset_TimeoutConfig(new_pos, input);
        return (
            new_pos, ChainOwnership(super_owners, owners, multi_leader_rounds, open_multi_leader_rounds, timeout_config)
        );
    }

    function bcs_deserialize_ChainOwnership(bytes memory input) public pure returns (ChainOwnership memory) {
        uint256 new_pos;
        ChainOwnership memory value;
        (new_pos, value) = bcs_deserialize_offset_ChainOwnership(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function inner_chain_id(uint8 val) internal returns (bytes32) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(0));
        input1[1] = bytes1(val);
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        return bcs_deserialize_bytes32(output1);
    }

    function chain_id() internal returns (bytes32) {
        return inner_chain_id(uint8(0));
    }

    function application_creator_chain_id() internal returns (bytes32) {
        return inner_chain_id(uint8(1));
    }

    function chain_ownership() internal returns (ChainOwnership memory) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(0));
        input1[1] = bytes1(uint8(2));
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        return bcs_deserialize_ChainOwnership(output1);
    }

    function read_data_blob(bytes32 hash) internal returns (bytes memory) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(0));
        input1[1] = bytes1(uint8(3));
        bytes memory input2 = abi.encodePacked(input1, hash);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        return output1;
    }

    function assert_data_blob_exists(bytes32 hash) internal {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(0));
        input1[1] = bytes1(uint8(4));
        bytes memory input2 = abi.encodePacked(input1, hash);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        assert(output1.length == 0);
    }

    struct OptionU32 {
        bool has_value;
        uint32 value;
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
        bytes32 ret_chain_id;
        (new_pos, ret_chain_id) = bcs_deserialize_offset_bytes32(new_pos, input);
        uint64 block_height;
        (new_pos, block_height) = bcs_deserialize_offset_uint64(new_pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, MessageId(ret_chain_id, block_height, index));
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

    enum MessageIsBouncing {
        NONE,
        IS_BOUNCING,
        NOT_BOUNCING
    }

    function try_call_application(bytes32 universal_address, bytes memory operation) internal returns (bytes memory) {
        address precompile = address(0x0b);
        bytes memory input2 = abi.encodePacked(uint8(1), uint8(0), universal_address, operation);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }

    function validation_round() internal returns (OptionU32 memory) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(1));
        input1[1] = bytes1(uint8(1));
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        uint32 val = bcs_deserialize_uint32(output1);
        bool has_value = true;
        uint32 value = 0;
        if (val == 0) {
            has_value = false;
        } else {
            value = val - 1;
        }
        return OptionU32(has_value, value);
    }

    function send_message(bytes32 input_chain_id, bytes memory message) internal {
        address precompile = address(0x0b);
        bytes memory input2 = abi.encodePacked(uint8(1), uint8(2), input_chain_id, message);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function message_id() internal returns (OptionMessageId memory) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(1));
        input1[1] = bytes1(uint8(3));
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        OptionMessageId memory output2 = bcs_deserialize_OptionMessageId(output1);
        return output2;
    }

    function message_is_bouncing() internal returns (MessageIsBouncing) {
        address precompile = address(0x0b);
        bytes memory input1 = new bytes(2);
        input1[0] = bytes1(uint8(1));
        input1[1] = bytes1(uint8(4));
        (bool success, bytes memory output1) = precompile.call(input1);
        require(success);
        MessageIsBouncing output2 = abi.decode(output1, (MessageIsBouncing));
        return output2;
    }

    function try_query_application(bytes32 universal_address, bytes memory argument) internal returns (bytes memory) {
        address precompile = address(0x0b);
        bytes memory input2 = abi.encodePacked(uint8(2), uint8(0), universal_address, argument);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }
}
