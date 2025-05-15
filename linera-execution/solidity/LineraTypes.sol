/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;

library LineraTypes {

    function bcs_serialize_len(uint256 x)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result;
        bytes1 entry;
        while (true) {
            if (x < 128) {
                entry = bytes1(uint8(x));
                result = abi.encodePacked(result, entry);
                return result;
            } else {
                uint256 xb = x >> 7;
                uint256 remainder = x - (xb << 7);
                require(remainder < 128);
                entry = bytes1(uint8(remainder) + 128);
                result = abi.encodePacked(result, entry);
                x = xb;
            }
        }
        require(false, "This line is unreachable");
        return result;
    }

    function bcs_deserialize_offset_len(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint256)
    {
        uint256 idx = 0;
        while (true) {
            if (uint8(input[pos + idx]) < 128) {
                uint256 result = 0;
                uint256 power = 1;
                for (uint256 u=0; u<idx; u++) {
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
        require(false, "This line is unreachable");
        return (0,0);
    }

    struct TimeoutConfig {
        opt_TimeDelta fast_round_duration;
        TimeDelta base_timeout;
        TimeDelta timeout_increment;
        TimeDelta fallback_duration;
    }

    function bcs_serialize_TimeoutConfig(TimeoutConfig memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_opt_TimeDelta(input.fast_round_duration);
        result = abi.encodePacked(result, bcs_serialize_TimeDelta(input.base_timeout));
        result = abi.encodePacked(result, bcs_serialize_TimeDelta(input.timeout_increment));
        result = abi.encodePacked(result, bcs_serialize_TimeDelta(input.fallback_duration));
        return result;
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

    function bcs_deserialize_TimeoutConfig(bytes memory input)
        internal
        pure
        returns (TimeoutConfig memory)
    {
        uint256 new_pos;
        TimeoutConfig memory value;
        (new_pos, value) = bcs_deserialize_offset_TimeoutConfig(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum OptionBool { None, True, False }

    function bcs_serialize_OptionBool(OptionBool input)
        internal
        pure
        returns (bytes memory)
    {
        uint8 value0;
        uint8 value1;
        if (input == OptionBool.None) {
            value0 = 0;
            return abi.encodePacked(value0);
        }
        value0 = 1;
        if (input == OptionBool.False) {
            value1 = 0;
            return abi.encodePacked(value0, value1);
        }
        value1 = 1;
        return abi.encodePacked(value0, value1);
    }

    function bcs_deserialize_offset_OptionBool(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionBool)
    {
        uint8 choice = uint8(input[pos]);
        if (choice == 0) {
           return (pos + 1, OptionBool.None);
        } else {
            require(choice == 1);
            uint8 value = uint8(input[pos + 1]);
            if (value == 0) {
                return (pos + 2, OptionBool.False);
            } else {
                require(value == 1);
                return (pos + 2, OptionBool.True);
            }
        }
    }

    function bcs_deserialize_OptionBool(bytes memory input)
        internal
        pure
        returns (OptionBool)
    {
        uint256 new_pos;
        OptionBool value;
        (new_pos, value) = bcs_deserialize_offset_OptionBool(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct TimeDelta {
        uint64 value;
    }

    function bcs_serialize_TimeDelta(TimeDelta memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_uint64(input.value);
        return result;
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

    function bcs_deserialize_TimeDelta(bytes memory input)
        internal
        pure
        returns (TimeDelta memory)
    {
        uint256 new_pos;
        TimeDelta memory value;
        (new_pos, value) = bcs_deserialize_offset_TimeDelta(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct key_values_AccountOwner_uint64 {
        AccountOwner key;
        uint64 value;
    }

    function bcs_serialize_key_values_AccountOwner_uint64(key_values_AccountOwner_uint64 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.key);
        result = abi.encodePacked(result, bcs_serialize_uint64(input.value));
        return result;
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

    function bcs_deserialize_key_values_AccountOwner_uint64(bytes memory input)
        internal
        pure
        returns (key_values_AccountOwner_uint64 memory)
    {
        uint256 new_pos;
        key_values_AccountOwner_uint64 memory value;
        (new_pos, value) = bcs_deserialize_offset_key_values_AccountOwner_uint64(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint32(uint32 input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = new bytes(4);
        uint32 value = input;
        result[0] = bytes1(uint8(value));
        for (uint i=1; i<4; i++) {
            value = value >> 8;
            result[i] = bytes1(uint8(value));
        }
        return result;
    }

    function bcs_deserialize_offset_uint32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint32)
    {
        uint32 value = uint8(input[pos + 3]);
        for (uint256 i=0; i<3; i++) {
            value = value << 8;
            value += uint8(input[pos + 2 - i]);
        }
        return (pos + 4, value);
    }

    function bcs_serialize_bool(bool input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_bool(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, bool)
    {
        uint8 val = uint8(input[pos]);
        bool result = false;
        if (val == 1) {
            result = true;
        } else {
            require(val == 0);
        }
        return (pos + 1, result);
    }

    struct ChainOwnership {
        AccountOwner[] super_owners;
        key_values_AccountOwner_uint64[] owners;
        uint32 multi_leader_rounds;
        bool open_multi_leader_rounds;
        TimeoutConfig timeout_config;
    }

    function bcs_serialize_ChainOwnership(ChainOwnership memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_seq_AccountOwner(input.super_owners);
        result = abi.encodePacked(result, bcs_serialize_seq_key_values_AccountOwner_uint64(input.owners));
        result = abi.encodePacked(result, bcs_serialize_uint32(input.multi_leader_rounds));
        result = abi.encodePacked(result, bcs_serialize_bool(input.open_multi_leader_rounds));
        result = abi.encodePacked(result, bcs_serialize_TimeoutConfig(input.timeout_config));
        return result;
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
        return (new_pos, ChainOwnership(super_owners, owners, multi_leader_rounds, open_multi_leader_rounds, timeout_config));
    }

    function bcs_deserialize_ChainOwnership(bytes memory input)
        internal
        pure
        returns (ChainOwnership memory)
    {
        uint256 new_pos;
        ChainOwnership memory value;
        (new_pos, value) = bcs_deserialize_offset_ChainOwnership(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlockHeight {
        uint64 value;
    }

    function bcs_serialize_BlockHeight(BlockHeight memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_uint64(input.value);
        return result;
    }

    function bcs_deserialize_offset_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlockHeight memory)
    {
        uint256 new_pos = pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(new_pos, input);
        return (new_pos, BlockHeight(value));
    }

    function bcs_deserialize_BlockHeight(bytes memory input)
        internal
        pure
        returns (BlockHeight memory)
    {
        uint256 new_pos;
        BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct CryptoHash {
        bytes32 value;
    }

    function bcs_serialize_CryptoHash(CryptoHash memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_bytes32(input.value);
        return result;
    }

    function bcs_deserialize_offset_CryptoHash(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, CryptoHash memory)
    {
        uint256 new_pos = pos;
        bytes32 value;
        (new_pos, value) = bcs_deserialize_offset_bytes32(new_pos, input);
        return (new_pos, CryptoHash(value));
    }

    function bcs_deserialize_CryptoHash(bytes memory input)
        internal
        pure
        returns (CryptoHash memory)
    {
        uint256 new_pos;
        CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_CryptoHash(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct MessageIsBouncing {
        OptionBool value;
    }

    function bcs_serialize_MessageIsBouncing(MessageIsBouncing memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_OptionBool(input.value);
        return result;
    }

    function bcs_deserialize_offset_MessageIsBouncing(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageIsBouncing memory)
    {
        uint256 new_pos = pos;
        OptionBool value;
        (new_pos, value) = bcs_deserialize_offset_OptionBool(new_pos, input);
        return (new_pos, MessageIsBouncing(value));
    }

    function bcs_deserialize_MessageIsBouncing(bytes memory input)
        internal
        pure
        returns (MessageIsBouncing memory)
    {
        uint256 new_pos;
        MessageIsBouncing memory value;
        (new_pos, value) = bcs_deserialize_offset_MessageIsBouncing(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bytes20(bytes20 input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_bytes20(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, bytes20)
    {
        bytes20 dest;
        assembly {
            dest := mload(add(add(input, 0x20), pos))
        }
        uint256 new_pos = pos + 20;
        return (new_pos, dest);
    }

    struct opt_MessageId {
        bool has_value;
        MessageId value;
    }

    function bcs_serialize_opt_MessageId(opt_MessageId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bool has_value = input.has_value;
        bytes memory block1 = bcs_serialize_bool(has_value);
        if (has_value) {
            bytes memory block2 = bcs_serialize_MessageId(input.value);
            return abi.encodePacked(block1, block2);
        } else {
            return block1;
        }
    }

    function bcs_deserialize_offset_opt_MessageId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_MessageId memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        MessageId memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_MessageId(new_pos, input);
        }
        return (new_pos, opt_MessageId(true, value));
    }

    function bcs_deserialize_opt_MessageId(bytes memory input)
        internal
        pure
        returns (opt_MessageId memory)
    {
        uint256 new_pos;
        opt_MessageId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_MessageId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint64(uint64 input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = new bytes(8);
        uint64 value = input;
        result[0] = bytes1(uint8(value));
        for (uint i=1; i<8; i++) {
            value = value >> 8;
            result[i] = bytes1(uint8(value));
        }
        return result;
    }

    function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint64)
    {
        uint64 value = uint8(input[pos + 7]);
        for (uint256 i=0; i<7; i++) {
            value = value << 8;
            value += uint8(input[pos + 6 - i]);
        }
        return (pos + 8, value);
    }

    function bcs_serialize_seq_AccountOwner(AccountOwner[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_AccountOwner(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_AccountOwner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AccountOwner[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        AccountOwner[] memory result;
        result = new AccountOwner[](len);
        AccountOwner memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_AccountOwner(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_AccountOwner(bytes memory input)
        internal
        pure
        returns (AccountOwner[] memory)
    {
        uint256 new_pos;
        AccountOwner[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_AccountOwner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct opt_TimeDelta {
        bool has_value;
        TimeDelta value;
    }

    function bcs_serialize_opt_TimeDelta(opt_TimeDelta memory input)
        internal
        pure
        returns (bytes memory)
    {
        bool has_value = input.has_value;
        bytes memory block1 = bcs_serialize_bool(has_value);
        if (has_value) {
            bytes memory block2 = bcs_serialize_TimeDelta(input.value);
            return abi.encodePacked(block1, block2);
        } else {
            return block1;
        }
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

    function bcs_deserialize_opt_TimeDelta(bytes memory input)
        internal
        pure
        returns (opt_TimeDelta memory)
    {
        uint256 new_pos;
        opt_TimeDelta memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_TimeDelta(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bytes32(bytes32 input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_bytes32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, bytes32)
    {
        bytes32 dest;
        assembly {
            dest := mload(add(add(input, 0x20), pos))
        }
        uint256 new_pos = pos + 32;
        return (new_pos, dest);
    }

    function bcs_serialize_seq_key_values_AccountOwner_uint64(key_values_AccountOwner_uint64[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_key_values_AccountOwner_uint64(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_key_values_AccountOwner_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_AccountOwner_uint64[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        key_values_AccountOwner_uint64[] memory result;
        result = new key_values_AccountOwner_uint64[](len);
        key_values_AccountOwner_uint64 memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_key_values_AccountOwner_uint64(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_key_values_AccountOwner_uint64(bytes memory input)
        internal
        pure
        returns (key_values_AccountOwner_uint64[] memory)
    {
        uint256 new_pos;
        key_values_AccountOwner_uint64[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_key_values_AccountOwner_uint64(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }
    struct AccountOwner {
        uint8 choice;
        // choice=0 corresponds to Reserved
        uint8 reserved;
        // choice=1 corresponds to Address32
        CryptoHash address32;
        // choice=2 corresponds to Address20
        bytes20 address20;
    }

    function bcs_serialize_AccountOwner(AccountOwner memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = abi.encodePacked(input.choice);
        if (input.choice == 0) {
            return abi.encodePacked(result, bcs_serialize_uint8(input.reserved));
        }
        if (input.choice == 1) {
            return abi.encodePacked(result, bcs_serialize_CryptoHash(input.address32));
        }
        if (input.choice == 2) {
            return abi.encodePacked(result, bcs_serialize_bytes20(input.address20));
        }
        return result;
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
        CryptoHash memory address32;
        if (choice == 1) {
            (new_pos, address32) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        }
        bytes20 address20;
        if (choice == 2) {
            (new_pos, address20) = bcs_deserialize_offset_bytes20(new_pos, input);
        }
        return (new_pos, AccountOwner(choice, reserved, address32, address20));
    }

    function bcs_deserialize_AccountOwner(bytes memory input)
        internal
        pure
        returns (AccountOwner memory)
    {
        uint256 new_pos;
        AccountOwner memory value;
        (new_pos, value) = bcs_deserialize_offset_AccountOwner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct opt_uint32 {
        bool has_value;
        uint32 value;
    }

    function bcs_serialize_opt_uint32(opt_uint32 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bool has_value = input.has_value;
        bytes memory block1 = bcs_serialize_bool(has_value);
        if (has_value) {
            bytes memory block2 = bcs_serialize_uint32(input.value);
            return abi.encodePacked(block1, block2);
        } else {
            return block1;
        }
    }

    function bcs_deserialize_offset_opt_uint32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_uint32 memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        uint32 value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_uint32(new_pos, input);
        }
        return (new_pos, opt_uint32(true, value));
    }

    function bcs_deserialize_opt_uint32(bytes memory input)
        internal
        pure
        returns (opt_uint32 memory)
    {
        uint256 new_pos;
        opt_uint32 memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_uint32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OptionU32 {
        opt_uint32 value;
    }

    function bcs_serialize_OptionU32(OptionU32 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_opt_uint32(input.value);
        return result;
    }

    function bcs_deserialize_offset_OptionU32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionU32 memory)
    {
        uint256 new_pos = pos;
        opt_uint32 memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_uint32(new_pos, input);
        return (new_pos, OptionU32(value));
    }

    function bcs_deserialize_OptionU32(bytes memory input)
        internal
        pure
        returns (OptionU32 memory)
    {
        uint256 new_pos;
        OptionU32 memory value;
        (new_pos, value) = bcs_deserialize_offset_OptionU32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint8(uint8 input)
        internal
        pure
        returns (bytes memory)
    {
      return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint8)
    {
        uint8 value = uint8(input[pos]);
        return (pos + 1, value);
    }

    struct ChainId {
        CryptoHash value;
    }

    function bcs_serialize_ChainId(ChainId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_CryptoHash(input.value);
        return result;
    }

    function bcs_deserialize_offset_ChainId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ChainId memory)
    {
        uint256 new_pos = pos;
        CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        return (new_pos, ChainId(value));
    }

    function bcs_deserialize_ChainId(bytes memory input)
        internal
        pure
        returns (ChainId memory)
    {
        uint256 new_pos;
        ChainId memory value;
        (new_pos, value) = bcs_deserialize_offset_ChainId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct MessageId {
        ChainId chain_id;
        BlockHeight height;
        uint32 index;
    }

    function bcs_serialize_MessageId(MessageId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_BlockHeight(input.height));
        result = abi.encodePacked(result, bcs_serialize_uint32(input.index));
        return result;
    }

    function bcs_deserialize_offset_MessageId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageId memory)
    {
        uint256 new_pos = pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(new_pos, input);
        BlockHeight memory height;
        (new_pos, height) = bcs_deserialize_offset_BlockHeight(new_pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, MessageId(chain_id, height, index));
    }

    function bcs_deserialize_MessageId(bytes memory input)
        internal
        pure
        returns (MessageId memory)
    {
        uint256 new_pos;
        MessageId memory value;
        (new_pos, value) = bcs_deserialize_offset_MessageId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OptionMessageId {
        opt_MessageId value;
    }

    function bcs_serialize_OptionMessageId(OptionMessageId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_opt_MessageId(input.value);
        return result;
    }

    function bcs_deserialize_offset_OptionMessageId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionMessageId memory)
    {
        uint256 new_pos = pos;
        opt_MessageId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_MessageId(new_pos, input);
        return (new_pos, OptionMessageId(value));
    }

    function bcs_deserialize_OptionMessageId(bytes memory input)
        internal
        pure
        returns (OptionMessageId memory)
    {
        uint256 new_pos;
        OptionMessageId memory value;
        (new_pos, value) = bcs_deserialize_offset_OptionMessageId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

} // end of library LineraTypes

