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
                return abi.encodePacked(result, entry);
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
                return (pos + idx + 1, result);
            }
            idx += 1;
        }
        require(false, "This line is unreachable");
        return (0,0);
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

    function AccountOwner_case_reserved(uint8 reserved)
        internal
        pure
        returns (AccountOwner memory)
    {
        CryptoHash memory address32;
        bytes20 address20;
        return AccountOwner(uint8(0), reserved, address32, address20);
    }

    function AccountOwner_case_address32(CryptoHash memory address32)
        internal
        pure
        returns (AccountOwner memory)
    {
        uint8 reserved;
        bytes20 address20;
        return AccountOwner(uint8(1), reserved, address32, address20);
    }

    function AccountOwner_case_address20(bytes20 address20)
        internal
        pure
        returns (AccountOwner memory)
    {
        uint8 reserved;
        CryptoHash memory address32;
        return AccountOwner(uint8(2), reserved, address32, address20);
    }

    function bcs_serialize_AccountOwner(AccountOwner memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_uint8(input.reserved));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_CryptoHash(input.address32));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_bytes20(input.address20));
        }
        return abi.encodePacked(input.choice);
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
        require(choice < 3);
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

    struct AccountOwnerBalanceInner {
        AccountOwner account_owner;
        Amount balance_;
    }

    function bcs_serialize_AccountOwnerBalanceInner(AccountOwnerBalanceInner memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.account_owner);
        return abi.encodePacked(result, bcs_serialize_Amount(input.balance_));
    }

    function bcs_deserialize_offset_AccountOwnerBalanceInner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AccountOwnerBalanceInner memory)
    {
        uint256 new_pos;
        AccountOwner memory account_owner;
        (new_pos, account_owner) = bcs_deserialize_offset_AccountOwner(pos, input);
        Amount memory balance_;
        (new_pos, balance_) = bcs_deserialize_offset_Amount(new_pos, input);
        return (new_pos, AccountOwnerBalanceInner(account_owner, balance_));
    }

    function bcs_deserialize_AccountOwnerBalanceInner(bytes memory input)
        internal
        pure
        returns (AccountOwnerBalanceInner memory)
    {
        uint256 new_pos;
        AccountOwnerBalanceInner memory value;
        (new_pos, value) = bcs_deserialize_offset_AccountOwnerBalanceInner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Amount {
        bytes32 value;
    }

    function bcs_serialize_Amount(Amount memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_bytes32(input.value);
    }

    function bcs_deserialize_offset_Amount(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Amount memory)
    {
        uint256 new_pos;
        bytes32 value;
        (new_pos, value) = bcs_deserialize_offset_bytes32(pos, input);
        return (new_pos, Amount(value));
    }

    function bcs_deserialize_Amount(bytes memory input)
        internal
        pure
        returns (Amount memory)
    {
        uint256 new_pos;
        Amount memory value;
        (new_pos, value) = bcs_deserialize_offset_Amount(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ApplicationId {
        CryptoHash application_description_hash;
    }

    function bcs_serialize_ApplicationId(ApplicationId memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_CryptoHash(input.application_description_hash);
    }

    function bcs_deserialize_offset_ApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ApplicationId memory)
    {
        uint256 new_pos;
        CryptoHash memory application_description_hash;
        (new_pos, application_description_hash) = bcs_deserialize_offset_CryptoHash(pos, input);
        return (new_pos, ApplicationId(application_description_hash));
    }

    function bcs_deserialize_ApplicationId(bytes memory input)
        internal
        pure
        returns (ApplicationId memory)
    {
        uint256 new_pos;
        ApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_ApplicationId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BaseRuntimePrecompile {
        uint8 choice;
        // choice=0 corresponds to ChainId
        // choice=1 corresponds to BlockHeight
        // choice=2 corresponds to ApplicationCreatorChainId
        // choice=3 corresponds to ReadSystemTimestamp
        // choice=4 corresponds to ReadChainBalance
        // choice=5 corresponds to ReadOwnerBalance
        AccountOwner read_owner_balance;
        // choice=6 corresponds to ReadOwnerBalances
        // choice=7 corresponds to ReadBalanceOwners
        // choice=8 corresponds to ChainOwnership
        // choice=9 corresponds to ReadDataBlob
        DataBlobHash read_data_blob;
        // choice=10 corresponds to AssertDataBlobExists
        DataBlobHash assert_data_blob_exists;
    }

    function BaseRuntimePrecompile_case_chain_id()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(0), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_block_height()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(1), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_application_creator_chain_id()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(2), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_read_system_timestamp()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(3), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_read_chain_balance()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(4), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_read_owner_balance(AccountOwner memory read_owner_balance)
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(5), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_read_owner_balances()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(6), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_read_balance_owners()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(7), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_chain_ownership()
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(8), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_read_data_blob(DataBlobHash memory read_data_blob)
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory assert_data_blob_exists;
        return BaseRuntimePrecompile(uint8(9), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function BaseRuntimePrecompile_case_assert_data_blob_exists(DataBlobHash memory assert_data_blob_exists)
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        AccountOwner memory read_owner_balance;
        DataBlobHash memory read_data_blob;
        return BaseRuntimePrecompile(uint8(10), read_owner_balance, read_data_blob, assert_data_blob_exists);
    }

    function bcs_serialize_BaseRuntimePrecompile(BaseRuntimePrecompile memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 5) {
            return abi.encodePacked(input.choice, bcs_serialize_AccountOwner(input.read_owner_balance));
        }
        if (input.choice == 9) {
            return abi.encodePacked(input.choice, bcs_serialize_DataBlobHash(input.read_data_blob));
        }
        if (input.choice == 10) {
            return abi.encodePacked(input.choice, bcs_serialize_DataBlobHash(input.assert_data_blob_exists));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_BaseRuntimePrecompile(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BaseRuntimePrecompile memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        AccountOwner memory read_owner_balance;
        if (choice == 5) {
            (new_pos, read_owner_balance) = bcs_deserialize_offset_AccountOwner(new_pos, input);
        }
        DataBlobHash memory read_data_blob;
        if (choice == 9) {
            (new_pos, read_data_blob) = bcs_deserialize_offset_DataBlobHash(new_pos, input);
        }
        DataBlobHash memory assert_data_blob_exists;
        if (choice == 10) {
            (new_pos, assert_data_blob_exists) = bcs_deserialize_offset_DataBlobHash(new_pos, input);
        }
        require(choice < 11);
        return (new_pos, BaseRuntimePrecompile(choice, read_owner_balance, read_data_blob, assert_data_blob_exists));
    }

    function bcs_deserialize_BaseRuntimePrecompile(bytes memory input)
        internal
        pure
        returns (BaseRuntimePrecompile memory)
    {
        uint256 new_pos;
        BaseRuntimePrecompile memory value;
        (new_pos, value) = bcs_deserialize_offset_BaseRuntimePrecompile(0, input);
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
        return bcs_serialize_uint64(input.value);
    }

    function bcs_deserialize_offset_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlockHeight memory)
    {
        uint256 new_pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(pos, input);
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

    struct ChainId {
        CryptoHash value;
    }

    function bcs_serialize_ChainId(ChainId memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_CryptoHash(input.value);
    }

    function bcs_deserialize_offset_ChainId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ChainId memory)
    {
        uint256 new_pos;
        CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_CryptoHash(pos, input);
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
        return abi.encodePacked(result, bcs_serialize_TimeoutConfig(input.timeout_config));
    }

    function bcs_deserialize_offset_ChainOwnership(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ChainOwnership memory)
    {
        uint256 new_pos;
        AccountOwner[] memory super_owners;
        (new_pos, super_owners) = bcs_deserialize_offset_seq_AccountOwner(pos, input);
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

    struct ContractRuntimePrecompile {
        uint8 choice;
        // choice=0 corresponds to AuthenticatedSigner
        // choice=1 corresponds to MessageOriginChainId
        // choice=2 corresponds to MessageIsBouncing
        // choice=3 corresponds to AuthenticatedCallerId
        // choice=4 corresponds to SendMessage
        ContractRuntimePrecompile_SendMessage send_message;
        // choice=5 corresponds to TryCallApplication
        ContractRuntimePrecompile_TryCallApplication try_call_application;
        // choice=6 corresponds to Emit
        ContractRuntimePrecompile_Emit emit_;
        // choice=7 corresponds to ReadEvent
        ContractRuntimePrecompile_ReadEvent read_event;
        // choice=8 corresponds to SubscribeToEvents
        ContractRuntimePrecompile_SubscribeToEvents subscribe_to_events;
        // choice=9 corresponds to UnsubscribeFromEvents
        ContractRuntimePrecompile_UnsubscribeFromEvents unsubscribe_from_events;
        // choice=10 corresponds to QueryService
        ContractRuntimePrecompile_QueryService query_service;
        // choice=11 corresponds to ValidationRound
    }

    function ContractRuntimePrecompile_case_authenticated_signer()
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(0), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_message_origin_chain_id()
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(1), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_message_is_bouncing()
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(2), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_authenticated_caller_id()
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(3), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_send_message(ContractRuntimePrecompile_SendMessage memory send_message)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(4), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_try_call_application(ContractRuntimePrecompile_TryCallApplication memory try_call_application)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(5), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_emit(ContractRuntimePrecompile_Emit memory emit_)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(6), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_read_event(ContractRuntimePrecompile_ReadEvent memory read_event)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(7), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_subscribe_to_events(ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(8), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_unsubscribe_from_events(ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(9), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_query_service(ContractRuntimePrecompile_QueryService memory query_service)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        return ContractRuntimePrecompile(uint8(10), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function ContractRuntimePrecompile_case_validation_round()
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        ContractRuntimePrecompile_SendMessage memory send_message;
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        ContractRuntimePrecompile_Emit memory emit_;
        ContractRuntimePrecompile_ReadEvent memory read_event;
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        ContractRuntimePrecompile_QueryService memory query_service;
        return ContractRuntimePrecompile(uint8(11), send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service);
    }

    function bcs_serialize_ContractRuntimePrecompile(ContractRuntimePrecompile memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 4) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_SendMessage(input.send_message));
        }
        if (input.choice == 5) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_TryCallApplication(input.try_call_application));
        }
        if (input.choice == 6) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_Emit(input.emit_));
        }
        if (input.choice == 7) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_ReadEvent(input.read_event));
        }
        if (input.choice == 8) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_SubscribeToEvents(input.subscribe_to_events));
        }
        if (input.choice == 9) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_UnsubscribeFromEvents(input.unsubscribe_from_events));
        }
        if (input.choice == 10) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile_QueryService(input.query_service));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        ContractRuntimePrecompile_SendMessage memory send_message;
        if (choice == 4) {
            (new_pos, send_message) = bcs_deserialize_offset_ContractRuntimePrecompile_SendMessage(new_pos, input);
        }
        ContractRuntimePrecompile_TryCallApplication memory try_call_application;
        if (choice == 5) {
            (new_pos, try_call_application) = bcs_deserialize_offset_ContractRuntimePrecompile_TryCallApplication(new_pos, input);
        }
        ContractRuntimePrecompile_Emit memory emit_;
        if (choice == 6) {
            (new_pos, emit_) = bcs_deserialize_offset_ContractRuntimePrecompile_Emit(new_pos, input);
        }
        ContractRuntimePrecompile_ReadEvent memory read_event;
        if (choice == 7) {
            (new_pos, read_event) = bcs_deserialize_offset_ContractRuntimePrecompile_ReadEvent(new_pos, input);
        }
        ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events;
        if (choice == 8) {
            (new_pos, subscribe_to_events) = bcs_deserialize_offset_ContractRuntimePrecompile_SubscribeToEvents(new_pos, input);
        }
        ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events;
        if (choice == 9) {
            (new_pos, unsubscribe_from_events) = bcs_deserialize_offset_ContractRuntimePrecompile_UnsubscribeFromEvents(new_pos, input);
        }
        ContractRuntimePrecompile_QueryService memory query_service;
        if (choice == 10) {
            (new_pos, query_service) = bcs_deserialize_offset_ContractRuntimePrecompile_QueryService(new_pos, input);
        }
        require(choice < 12);
        return (new_pos, ContractRuntimePrecompile(choice, send_message, try_call_application, emit_, read_event, subscribe_to_events, unsubscribe_from_events, query_service));
    }

    function bcs_deserialize_ContractRuntimePrecompile(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_Emit {
        StreamName stream_name;
        bytes value;
    }

    function bcs_serialize_ContractRuntimePrecompile_Emit(ContractRuntimePrecompile_Emit memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_StreamName(input.stream_name);
        return abi.encodePacked(result, bcs_serialize_bytes(input.value));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_Emit(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_Emit memory)
    {
        uint256 new_pos;
        StreamName memory stream_name;
        (new_pos, stream_name) = bcs_deserialize_offset_StreamName(pos, input);
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_Emit(stream_name, value));
    }

    function bcs_deserialize_ContractRuntimePrecompile_Emit(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_Emit memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_Emit memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_Emit(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_QueryService {
        ApplicationId application_id;
        bytes query;
    }

    function bcs_serialize_ContractRuntimePrecompile_QueryService(ContractRuntimePrecompile_QueryService memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ApplicationId(input.application_id);
        return abi.encodePacked(result, bcs_serialize_bytes(input.query));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_QueryService(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_QueryService memory)
    {
        uint256 new_pos;
        ApplicationId memory application_id;
        (new_pos, application_id) = bcs_deserialize_offset_ApplicationId(pos, input);
        bytes memory query;
        (new_pos, query) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_QueryService(application_id, query));
    }

    function bcs_deserialize_ContractRuntimePrecompile_QueryService(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_QueryService memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_QueryService memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_QueryService(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_ReadEvent {
        ChainId chain_id;
        StreamName stream_name;
        uint32 index;
    }

    function bcs_serialize_ContractRuntimePrecompile_ReadEvent(ContractRuntimePrecompile_ReadEvent memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_StreamName(input.stream_name));
        return abi.encodePacked(result, bcs_serialize_uint32(input.index));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_ReadEvent(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_ReadEvent memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        StreamName memory stream_name;
        (new_pos, stream_name) = bcs_deserialize_offset_StreamName(new_pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_ReadEvent(chain_id, stream_name, index));
    }

    function bcs_deserialize_ContractRuntimePrecompile_ReadEvent(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_ReadEvent memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_ReadEvent memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_ReadEvent(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_SendMessage {
        ChainId destination;
        bytes message;
    }

    function bcs_serialize_ContractRuntimePrecompile_SendMessage(ContractRuntimePrecompile_SendMessage memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.destination);
        return abi.encodePacked(result, bcs_serialize_bytes(input.message));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_SendMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_SendMessage memory)
    {
        uint256 new_pos;
        ChainId memory destination;
        (new_pos, destination) = bcs_deserialize_offset_ChainId(pos, input);
        bytes memory message;
        (new_pos, message) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_SendMessage(destination, message));
    }

    function bcs_deserialize_ContractRuntimePrecompile_SendMessage(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_SendMessage memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_SendMessage memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_SendMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_SubscribeToEvents {
        ChainId chain_id;
        ApplicationId application_id;
        StreamName stream_name;
    }

    function bcs_serialize_ContractRuntimePrecompile_SubscribeToEvents(ContractRuntimePrecompile_SubscribeToEvents memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_ApplicationId(input.application_id));
        return abi.encodePacked(result, bcs_serialize_StreamName(input.stream_name));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_SubscribeToEvents(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_SubscribeToEvents memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        ApplicationId memory application_id;
        (new_pos, application_id) = bcs_deserialize_offset_ApplicationId(new_pos, input);
        StreamName memory stream_name;
        (new_pos, stream_name) = bcs_deserialize_offset_StreamName(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_SubscribeToEvents(chain_id, application_id, stream_name));
    }

    function bcs_deserialize_ContractRuntimePrecompile_SubscribeToEvents(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_SubscribeToEvents memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_SubscribeToEvents memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_SubscribeToEvents(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_TryCallApplication {
        ApplicationId target;
        bytes argument;
    }

    function bcs_serialize_ContractRuntimePrecompile_TryCallApplication(ContractRuntimePrecompile_TryCallApplication memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ApplicationId(input.target);
        return abi.encodePacked(result, bcs_serialize_bytes(input.argument));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_TryCallApplication(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_TryCallApplication memory)
    {
        uint256 new_pos;
        ApplicationId memory target;
        (new_pos, target) = bcs_deserialize_offset_ApplicationId(pos, input);
        bytes memory argument;
        (new_pos, argument) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_TryCallApplication(target, argument));
    }

    function bcs_deserialize_ContractRuntimePrecompile_TryCallApplication(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_TryCallApplication memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_TryCallApplication memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_TryCallApplication(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ContractRuntimePrecompile_UnsubscribeFromEvents {
        ChainId chain_id;
        ApplicationId application_id;
        StreamName stream_name;
    }

    function bcs_serialize_ContractRuntimePrecompile_UnsubscribeFromEvents(ContractRuntimePrecompile_UnsubscribeFromEvents memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_ApplicationId(input.application_id));
        return abi.encodePacked(result, bcs_serialize_StreamName(input.stream_name));
    }

    function bcs_deserialize_offset_ContractRuntimePrecompile_UnsubscribeFromEvents(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ContractRuntimePrecompile_UnsubscribeFromEvents memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        ApplicationId memory application_id;
        (new_pos, application_id) = bcs_deserialize_offset_ApplicationId(new_pos, input);
        StreamName memory stream_name;
        (new_pos, stream_name) = bcs_deserialize_offset_StreamName(new_pos, input);
        return (new_pos, ContractRuntimePrecompile_UnsubscribeFromEvents(chain_id, application_id, stream_name));
    }

    function bcs_deserialize_ContractRuntimePrecompile_UnsubscribeFromEvents(bytes memory input)
        internal
        pure
        returns (ContractRuntimePrecompile_UnsubscribeFromEvents memory)
    {
        uint256 new_pos;
        ContractRuntimePrecompile_UnsubscribeFromEvents memory value;
        (new_pos, value) = bcs_deserialize_offset_ContractRuntimePrecompile_UnsubscribeFromEvents(0, input);
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
        return bcs_serialize_bytes32(input.value);
    }

    function bcs_deserialize_offset_CryptoHash(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, CryptoHash memory)
    {
        uint256 new_pos;
        bytes32 value;
        (new_pos, value) = bcs_deserialize_offset_bytes32(pos, input);
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

    struct DataBlobHash {
        CryptoHash value;
    }

    function bcs_serialize_DataBlobHash(DataBlobHash memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_CryptoHash(input.value);
    }

    function bcs_deserialize_offset_DataBlobHash(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, DataBlobHash memory)
    {
        uint256 new_pos;
        CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_CryptoHash(pos, input);
        return (new_pos, DataBlobHash(value));
    }

    function bcs_deserialize_DataBlobHash(bytes memory input)
        internal
        pure
        returns (DataBlobHash memory)
    {
        uint256 new_pos;
        DataBlobHash memory value;
        (new_pos, value) = bcs_deserialize_offset_DataBlobHash(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct GenericApplicationId {
        uint8 choice;
        // choice=0 corresponds to System
        // choice=1 corresponds to User
        ApplicationId user;
    }

    function GenericApplicationId_case_system()
        internal
        pure
        returns (GenericApplicationId memory)
    {
        ApplicationId memory user;
        return GenericApplicationId(uint8(0), user);
    }

    function GenericApplicationId_case_user(ApplicationId memory user)
        internal
        pure
        returns (GenericApplicationId memory)
    {
        return GenericApplicationId(uint8(1), user);
    }

    function bcs_serialize_GenericApplicationId(GenericApplicationId memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_ApplicationId(input.user));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_GenericApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, GenericApplicationId memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        ApplicationId memory user;
        if (choice == 1) {
            (new_pos, user) = bcs_deserialize_offset_ApplicationId(new_pos, input);
        }
        require(choice < 2);
        return (new_pos, GenericApplicationId(choice, user));
    }

    function bcs_deserialize_GenericApplicationId(bytes memory input)
        internal
        pure
        returns (GenericApplicationId memory)
    {
        uint256 new_pos;
        GenericApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_GenericApplicationId(0, input);
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
        return bcs_serialize_OptionBool(input.value);
    }

    function bcs_deserialize_offset_MessageIsBouncing(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageIsBouncing memory)
    {
        uint256 new_pos;
        OptionBool value;
        (new_pos, value) = bcs_deserialize_offset_OptionBool(pos, input);
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

    struct OptionAccountOwner {
        opt_AccountOwner value;
    }

    function bcs_serialize_OptionAccountOwner(OptionAccountOwner memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_opt_AccountOwner(input.value);
    }

    function bcs_deserialize_offset_OptionAccountOwner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionAccountOwner memory)
    {
        uint256 new_pos;
        opt_AccountOwner memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_AccountOwner(pos, input);
        return (new_pos, OptionAccountOwner(value));
    }

    function bcs_deserialize_OptionAccountOwner(bytes memory input)
        internal
        pure
        returns (OptionAccountOwner memory)
    {
        uint256 new_pos;
        OptionAccountOwner memory value;
        (new_pos, value) = bcs_deserialize_offset_OptionAccountOwner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OptionApplicationId {
        opt_ApplicationId value;
    }

    function bcs_serialize_OptionApplicationId(OptionApplicationId memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_opt_ApplicationId(input.value);
    }

    function bcs_deserialize_offset_OptionApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionApplicationId memory)
    {
        uint256 new_pos;
        opt_ApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_ApplicationId(pos, input);
        return (new_pos, OptionApplicationId(value));
    }

    function bcs_deserialize_OptionApplicationId(bytes memory input)
        internal
        pure
        returns (OptionApplicationId memory)
    {
        uint256 new_pos;
        OptionApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_OptionApplicationId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum OptionBool { None, True, False }

    function bcs_serialize_OptionBool(OptionBool input)
        internal
        pure
        returns (bytes memory)
    {
        if (input == OptionBool.None) {
            return abi.encodePacked(uint8(0));
        }
        if (input == OptionBool.False) {
            return abi.encodePacked(uint8(1), uint8(0));
        }
        return abi.encodePacked(uint8(1), uint8(1));
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

    struct OptionChainId {
        opt_ChainId value;
    }

    function bcs_serialize_OptionChainId(OptionChainId memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_opt_ChainId(input.value);
    }

    function bcs_deserialize_offset_OptionChainId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionChainId memory)
    {
        uint256 new_pos;
        opt_ChainId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_ChainId(pos, input);
        return (new_pos, OptionChainId(value));
    }

    function bcs_deserialize_OptionChainId(bytes memory input)
        internal
        pure
        returns (OptionChainId memory)
    {
        uint256 new_pos;
        OptionChainId memory value;
        (new_pos, value) = bcs_deserialize_offset_OptionChainId(0, input);
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
        return bcs_serialize_opt_uint32(input.value);
    }

    function bcs_deserialize_offset_OptionU32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OptionU32 memory)
    {
        uint256 new_pos;
        opt_uint32 memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_uint32(pos, input);
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

    struct ResponseReadBalanceOwners {
        AccountOwner[] value;
    }

    function bcs_serialize_ResponseReadBalanceOwners(ResponseReadBalanceOwners memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_seq_AccountOwner(input.value);
    }

    function bcs_deserialize_offset_ResponseReadBalanceOwners(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ResponseReadBalanceOwners memory)
    {
        uint256 new_pos;
        AccountOwner[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_AccountOwner(pos, input);
        return (new_pos, ResponseReadBalanceOwners(value));
    }

    function bcs_deserialize_ResponseReadBalanceOwners(bytes memory input)
        internal
        pure
        returns (ResponseReadBalanceOwners memory)
    {
        uint256 new_pos;
        ResponseReadBalanceOwners memory value;
        (new_pos, value) = bcs_deserialize_offset_ResponseReadBalanceOwners(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ResponseReadOwnerBalances {
        AccountOwnerBalanceInner[] value;
    }

    function bcs_serialize_ResponseReadOwnerBalances(ResponseReadOwnerBalances memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_seq_AccountOwnerBalanceInner(input.value);
    }

    function bcs_deserialize_offset_ResponseReadOwnerBalances(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ResponseReadOwnerBalances memory)
    {
        uint256 new_pos;
        AccountOwnerBalanceInner[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_AccountOwnerBalanceInner(pos, input);
        return (new_pos, ResponseReadOwnerBalances(value));
    }

    function bcs_deserialize_ResponseReadOwnerBalances(bytes memory input)
        internal
        pure
        returns (ResponseReadOwnerBalances memory)
    {
        uint256 new_pos;
        ResponseReadOwnerBalances memory value;
        (new_pos, value) = bcs_deserialize_offset_ResponseReadOwnerBalances(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct RuntimePrecompile {
        uint8 choice;
        // choice=0 corresponds to Base
        BaseRuntimePrecompile base;
        // choice=1 corresponds to Contract
        ContractRuntimePrecompile contract_;
        // choice=2 corresponds to Service
        ServiceRuntimePrecompile service;
    }

    function RuntimePrecompile_case_base(BaseRuntimePrecompile memory base)
        internal
        pure
        returns (RuntimePrecompile memory)
    {
        ContractRuntimePrecompile memory contract_;
        ServiceRuntimePrecompile memory service;
        return RuntimePrecompile(uint8(0), base, contract_, service);
    }

    function RuntimePrecompile_case_contract(ContractRuntimePrecompile memory contract_)
        internal
        pure
        returns (RuntimePrecompile memory)
    {
        BaseRuntimePrecompile memory base;
        ServiceRuntimePrecompile memory service;
        return RuntimePrecompile(uint8(1), base, contract_, service);
    }

    function RuntimePrecompile_case_service(ServiceRuntimePrecompile memory service)
        internal
        pure
        returns (RuntimePrecompile memory)
    {
        BaseRuntimePrecompile memory base;
        ContractRuntimePrecompile memory contract_;
        return RuntimePrecompile(uint8(2), base, contract_, service);
    }

    function bcs_serialize_RuntimePrecompile(RuntimePrecompile memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_BaseRuntimePrecompile(input.base));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_ContractRuntimePrecompile(input.contract_));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_ServiceRuntimePrecompile(input.service));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_RuntimePrecompile(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, RuntimePrecompile memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        BaseRuntimePrecompile memory base;
        if (choice == 0) {
            (new_pos, base) = bcs_deserialize_offset_BaseRuntimePrecompile(new_pos, input);
        }
        ContractRuntimePrecompile memory contract_;
        if (choice == 1) {
            (new_pos, contract_) = bcs_deserialize_offset_ContractRuntimePrecompile(new_pos, input);
        }
        ServiceRuntimePrecompile memory service;
        if (choice == 2) {
            (new_pos, service) = bcs_deserialize_offset_ServiceRuntimePrecompile(new_pos, input);
        }
        require(choice < 3);
        return (new_pos, RuntimePrecompile(choice, base, contract_, service));
    }

    function bcs_deserialize_RuntimePrecompile(bytes memory input)
        internal
        pure
        returns (RuntimePrecompile memory)
    {
        uint256 new_pos;
        RuntimePrecompile memory value;
        (new_pos, value) = bcs_deserialize_offset_RuntimePrecompile(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ServiceRuntimePrecompile {
        uint8 choice;
        // choice=0 corresponds to TryQueryApplication
        ServiceRuntimePrecompile_TryQueryApplication try_query_application;
    }

    function ServiceRuntimePrecompile_case_try_query_application(ServiceRuntimePrecompile_TryQueryApplication memory try_query_application)
        internal
        pure
        returns (ServiceRuntimePrecompile memory)
    {
        return ServiceRuntimePrecompile(uint8(0), try_query_application);
    }

    function bcs_serialize_ServiceRuntimePrecompile(ServiceRuntimePrecompile memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_ServiceRuntimePrecompile_TryQueryApplication(input.try_query_application));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_ServiceRuntimePrecompile(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ServiceRuntimePrecompile memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        ServiceRuntimePrecompile_TryQueryApplication memory try_query_application;
        if (choice == 0) {
            (new_pos, try_query_application) = bcs_deserialize_offset_ServiceRuntimePrecompile_TryQueryApplication(new_pos, input);
        }
        require(choice < 1);
        return (new_pos, ServiceRuntimePrecompile(choice, try_query_application));
    }

    function bcs_deserialize_ServiceRuntimePrecompile(bytes memory input)
        internal
        pure
        returns (ServiceRuntimePrecompile memory)
    {
        uint256 new_pos;
        ServiceRuntimePrecompile memory value;
        (new_pos, value) = bcs_deserialize_offset_ServiceRuntimePrecompile(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ServiceRuntimePrecompile_TryQueryApplication {
        ApplicationId target;
        bytes argument;
    }

    function bcs_serialize_ServiceRuntimePrecompile_TryQueryApplication(ServiceRuntimePrecompile_TryQueryApplication memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ApplicationId(input.target);
        return abi.encodePacked(result, bcs_serialize_bytes(input.argument));
    }

    function bcs_deserialize_offset_ServiceRuntimePrecompile_TryQueryApplication(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ServiceRuntimePrecompile_TryQueryApplication memory)
    {
        uint256 new_pos;
        ApplicationId memory target;
        (new_pos, target) = bcs_deserialize_offset_ApplicationId(pos, input);
        bytes memory argument;
        (new_pos, argument) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, ServiceRuntimePrecompile_TryQueryApplication(target, argument));
    }

    function bcs_deserialize_ServiceRuntimePrecompile_TryQueryApplication(bytes memory input)
        internal
        pure
        returns (ServiceRuntimePrecompile_TryQueryApplication memory)
    {
        uint256 new_pos;
        ServiceRuntimePrecompile_TryQueryApplication memory value;
        (new_pos, value) = bcs_deserialize_offset_ServiceRuntimePrecompile_TryQueryApplication(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct StreamId {
        GenericApplicationId application_id;
        StreamName stream_name;
    }

    function bcs_serialize_StreamId(StreamId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_GenericApplicationId(input.application_id);
        return abi.encodePacked(result, bcs_serialize_StreamName(input.stream_name));
    }

    function bcs_deserialize_offset_StreamId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, StreamId memory)
    {
        uint256 new_pos;
        GenericApplicationId memory application_id;
        (new_pos, application_id) = bcs_deserialize_offset_GenericApplicationId(pos, input);
        StreamName memory stream_name;
        (new_pos, stream_name) = bcs_deserialize_offset_StreamName(new_pos, input);
        return (new_pos, StreamId(application_id, stream_name));
    }

    function bcs_deserialize_StreamId(bytes memory input)
        internal
        pure
        returns (StreamId memory)
    {
        uint256 new_pos;
        StreamId memory value;
        (new_pos, value) = bcs_deserialize_offset_StreamId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct StreamName {
        bytes value;
    }

    function bcs_serialize_StreamName(StreamName memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_bytes(input.value);
    }

    function bcs_deserialize_offset_StreamName(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, StreamName memory)
    {
        uint256 new_pos;
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(pos, input);
        return (new_pos, StreamName(value));
    }

    function bcs_deserialize_StreamName(bytes memory input)
        internal
        pure
        returns (StreamName memory)
    {
        uint256 new_pos;
        StreamName memory value;
        (new_pos, value) = bcs_deserialize_offset_StreamName(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct StreamUpdate {
        ChainId chain_id;
        StreamId stream_id;
        uint32 previous_index;
        uint32 next_index;
    }

    function bcs_serialize_StreamUpdate(StreamUpdate memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_StreamId(input.stream_id));
        result = abi.encodePacked(result, bcs_serialize_uint32(input.previous_index));
        return abi.encodePacked(result, bcs_serialize_uint32(input.next_index));
    }

    function bcs_deserialize_offset_StreamUpdate(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, StreamUpdate memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        StreamId memory stream_id;
        (new_pos, stream_id) = bcs_deserialize_offset_StreamId(new_pos, input);
        uint32 previous_index;
        (new_pos, previous_index) = bcs_deserialize_offset_uint32(new_pos, input);
        uint32 next_index;
        (new_pos, next_index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, StreamUpdate(chain_id, stream_id, previous_index, next_index));
    }

    function bcs_deserialize_StreamUpdate(bytes memory input)
        internal
        pure
        returns (StreamUpdate memory)
    {
        uint256 new_pos;
        StreamUpdate memory value;
        (new_pos, value) = bcs_deserialize_offset_StreamUpdate(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct StreamUpdates {
        StreamUpdate[] entries;
    }

    function bcs_serialize_StreamUpdates(StreamUpdates memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_seq_StreamUpdate(input.entries);
    }

    function bcs_deserialize_offset_StreamUpdates(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, StreamUpdates memory)
    {
        uint256 new_pos;
        StreamUpdate[] memory entries;
        (new_pos, entries) = bcs_deserialize_offset_seq_StreamUpdate(pos, input);
        return (new_pos, StreamUpdates(entries));
    }

    function bcs_deserialize_StreamUpdates(bytes memory input)
        internal
        pure
        returns (StreamUpdates memory)
    {
        uint256 new_pos;
        StreamUpdates memory value;
        (new_pos, value) = bcs_deserialize_offset_StreamUpdates(0, input);
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
        return bcs_serialize_uint64(input.value);
    }

    function bcs_deserialize_offset_TimeDelta(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, TimeDelta memory)
    {
        uint256 new_pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(pos, input);
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
        return abi.encodePacked(result, bcs_serialize_TimeDelta(input.fallback_duration));
    }

    function bcs_deserialize_offset_TimeoutConfig(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, TimeoutConfig memory)
    {
        uint256 new_pos;
        opt_TimeDelta memory fast_round_duration;
        (new_pos, fast_round_duration) = bcs_deserialize_offset_opt_TimeDelta(pos, input);
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

    struct Timestamp {
        uint64 value;
    }

    function bcs_serialize_Timestamp(Timestamp memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_uint64(input.value);
    }

    function bcs_deserialize_offset_Timestamp(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Timestamp memory)
    {
        uint256 new_pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(pos, input);
        return (new_pos, Timestamp(value));
    }

    function bcs_deserialize_Timestamp(bytes memory input)
        internal
        pure
        returns (Timestamp memory)
    {
        uint256 new_pos;
        Timestamp memory value;
        (new_pos, value) = bcs_deserialize_offset_Timestamp(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
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

    function bcs_deserialize_bool(bytes memory input)
        internal
        pure
        returns (bool)
    {
        uint256 new_pos;
        bool value;
        (new_pos, value) = bcs_deserialize_offset_bool(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bytes(bytes memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        return abi.encodePacked(result, input);
    }

    function bcs_deserialize_offset_bytes(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, bytes memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        bytes memory result = new bytes(len);
        for (uint256 u=0; u<len; u++) {
            result[u] = input[new_pos + u];
        }
        return (new_pos + len, result);
    }

    function bcs_deserialize_bytes(bytes memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 new_pos;
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(0, input);
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
        return (pos + 20, dest);
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
        return (pos + 32, dest);
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
        return abi.encodePacked(result, bcs_serialize_uint64(input.value));
    }

    function bcs_deserialize_offset_key_values_AccountOwner_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_AccountOwner_uint64 memory)
    {
        uint256 new_pos;
        AccountOwner memory key;
        (new_pos, key) = bcs_deserialize_offset_AccountOwner(pos, input);
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

    struct opt_AccountOwner {
        bool has_value;
        AccountOwner value;
    }

    function bcs_serialize_opt_AccountOwner(opt_AccountOwner memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_AccountOwner(input.value));
        } else {
            return abi.encodePacked(uint8(0));
        }
    }

    function bcs_deserialize_offset_opt_AccountOwner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_AccountOwner memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        AccountOwner memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_AccountOwner(new_pos, input);
        }
        return (new_pos, opt_AccountOwner(has_value, value));
    }

    function bcs_deserialize_opt_AccountOwner(bytes memory input)
        internal
        pure
        returns (opt_AccountOwner memory)
    {
        uint256 new_pos;
        opt_AccountOwner memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_AccountOwner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct opt_ApplicationId {
        bool has_value;
        ApplicationId value;
    }

    function bcs_serialize_opt_ApplicationId(opt_ApplicationId memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_ApplicationId(input.value));
        } else {
            return abi.encodePacked(uint8(0));
        }
    }

    function bcs_deserialize_offset_opt_ApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_ApplicationId memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        ApplicationId memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_ApplicationId(new_pos, input);
        }
        return (new_pos, opt_ApplicationId(has_value, value));
    }

    function bcs_deserialize_opt_ApplicationId(bytes memory input)
        internal
        pure
        returns (opt_ApplicationId memory)
    {
        uint256 new_pos;
        opt_ApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_ApplicationId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct opt_ChainId {
        bool has_value;
        ChainId value;
    }

    function bcs_serialize_opt_ChainId(opt_ChainId memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_ChainId(input.value));
        } else {
            return abi.encodePacked(uint8(0));
        }
    }

    function bcs_deserialize_offset_opt_ChainId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_ChainId memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        ChainId memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_ChainId(new_pos, input);
        }
        return (new_pos, opt_ChainId(has_value, value));
    }

    function bcs_deserialize_opt_ChainId(bytes memory input)
        internal
        pure
        returns (opt_ChainId memory)
    {
        uint256 new_pos;
        opt_ChainId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_ChainId(0, input);
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
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_TimeDelta(input.value));
        } else {
            return abi.encodePacked(uint8(0));
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
        return (new_pos, opt_TimeDelta(has_value, value));
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

    struct opt_uint32 {
        bool has_value;
        uint32 value;
    }

    function bcs_serialize_opt_uint32(opt_uint32 memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_uint32(input.value));
        } else {
            return abi.encodePacked(uint8(0));
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
        return (new_pos, opt_uint32(has_value, value));
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

    function bcs_serialize_seq_AccountOwnerBalanceInner(AccountOwnerBalanceInner[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_AccountOwnerBalanceInner(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_AccountOwnerBalanceInner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AccountOwnerBalanceInner[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        AccountOwnerBalanceInner[] memory result;
        result = new AccountOwnerBalanceInner[](len);
        AccountOwnerBalanceInner memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_AccountOwnerBalanceInner(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_AccountOwnerBalanceInner(bytes memory input)
        internal
        pure
        returns (AccountOwnerBalanceInner[] memory)
    {
        uint256 new_pos;
        AccountOwnerBalanceInner[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_AccountOwnerBalanceInner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_StreamUpdate(StreamUpdate[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_StreamUpdate(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_StreamUpdate(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, StreamUpdate[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        StreamUpdate[] memory result;
        result = new StreamUpdate[](len);
        StreamUpdate memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_StreamUpdate(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_StreamUpdate(bytes memory input)
        internal
        pure
        returns (StreamUpdate[] memory)
    {
        uint256 new_pos;
        StreamUpdate[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_StreamUpdate(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
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

    function bcs_deserialize_uint32(bytes memory input)
        internal
        pure
        returns (uint32)
    {
        uint256 new_pos;
        uint32 value;
        (new_pos, value) = bcs_deserialize_offset_uint32(0, input);
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

    function bcs_deserialize_uint64(bytes memory input)
        internal
        pure
        returns (uint64)
    {
        uint256 new_pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(0, input);
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

    function bcs_deserialize_uint8(bytes memory input)
        internal
        pure
        returns (uint8)
    {
        uint256 new_pos;
        uint8 value;
        (new_pos, value) = bcs_deserialize_offset_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

} // end of library LineraTypes
