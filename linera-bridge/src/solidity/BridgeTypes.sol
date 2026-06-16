/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;

library BridgeTypes {

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

    struct Account {
        ChainId chain_id;
        AccountOwner owner;
    }

    function bcs_serialize_Account(Account memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        return abi.encodePacked(result, bcs_serialize_AccountOwner(input.owner));
    }

    function bcs_deserialize_offset_Account(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Account memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        AccountOwner memory owner;
        (new_pos, owner) = bcs_deserialize_offset_AccountOwner(new_pos, input);
        return (new_pos, Account(chain_id, owner));
    }

    function bcs_deserialize_Account(bytes memory input)
        internal
        pure
        returns (Account memory)
    {
        uint256 new_pos;
        Account memory value;
        (new_pos, value) = bcs_deserialize_offset_Account(0, input);
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

    struct AdminOperation {
        uint8 choice;
        // choice=0 corresponds to PublishCommitteeBlob
        AdminOperation_PublishCommitteeBlob publish_committee_blob;
        // choice=1 corresponds to CreateCommittee
        AdminOperation_CreateCommittee create_committee;
        // choice=2 corresponds to RemoveCommittee
        AdminOperation_RemoveCommittee remove_committee;
    }

    function AdminOperation_case_publish_committee_blob(AdminOperation_PublishCommitteeBlob memory publish_committee_blob)
        internal
        pure
        returns (AdminOperation memory)
    {
        AdminOperation_CreateCommittee memory create_committee;
        AdminOperation_RemoveCommittee memory remove_committee;
        return AdminOperation(uint8(0), publish_committee_blob, create_committee, remove_committee);
    }

    function AdminOperation_case_create_committee(AdminOperation_CreateCommittee memory create_committee)
        internal
        pure
        returns (AdminOperation memory)
    {
        AdminOperation_PublishCommitteeBlob memory publish_committee_blob;
        AdminOperation_RemoveCommittee memory remove_committee;
        return AdminOperation(uint8(1), publish_committee_blob, create_committee, remove_committee);
    }

    function AdminOperation_case_remove_committee(AdminOperation_RemoveCommittee memory remove_committee)
        internal
        pure
        returns (AdminOperation memory)
    {
        AdminOperation_PublishCommitteeBlob memory publish_committee_blob;
        AdminOperation_CreateCommittee memory create_committee;
        return AdminOperation(uint8(2), publish_committee_blob, create_committee, remove_committee);
    }

    function bcs_serialize_AdminOperation(AdminOperation memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_AdminOperation_PublishCommitteeBlob(input.publish_committee_blob));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_AdminOperation_CreateCommittee(input.create_committee));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_AdminOperation_RemoveCommittee(input.remove_committee));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_AdminOperation(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AdminOperation memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        AdminOperation_PublishCommitteeBlob memory publish_committee_blob;
        if (choice == 0) {
            (new_pos, publish_committee_blob) = bcs_deserialize_offset_AdminOperation_PublishCommitteeBlob(new_pos, input);
        }
        AdminOperation_CreateCommittee memory create_committee;
        if (choice == 1) {
            (new_pos, create_committee) = bcs_deserialize_offset_AdminOperation_CreateCommittee(new_pos, input);
        }
        AdminOperation_RemoveCommittee memory remove_committee;
        if (choice == 2) {
            (new_pos, remove_committee) = bcs_deserialize_offset_AdminOperation_RemoveCommittee(new_pos, input);
        }
        require(choice < 3);
        return (new_pos, AdminOperation(choice, publish_committee_blob, create_committee, remove_committee));
    }

    function bcs_deserialize_AdminOperation(bytes memory input)
        internal
        pure
        returns (AdminOperation memory)
    {
        uint256 new_pos;
        AdminOperation memory value;
        (new_pos, value) = bcs_deserialize_offset_AdminOperation(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct AdminOperation_CreateCommittee {
        Epoch epoch;
        CryptoHash blob_hash;
    }

    function bcs_serialize_AdminOperation_CreateCommittee(AdminOperation_CreateCommittee memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_Epoch(input.epoch);
        return abi.encodePacked(result, bcs_serialize_CryptoHash(input.blob_hash));
    }

    function bcs_deserialize_offset_AdminOperation_CreateCommittee(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AdminOperation_CreateCommittee memory)
    {
        uint256 new_pos;
        Epoch memory epoch;
        (new_pos, epoch) = bcs_deserialize_offset_Epoch(pos, input);
        CryptoHash memory blob_hash;
        (new_pos, blob_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        return (new_pos, AdminOperation_CreateCommittee(epoch, blob_hash));
    }

    function bcs_deserialize_AdminOperation_CreateCommittee(bytes memory input)
        internal
        pure
        returns (AdminOperation_CreateCommittee memory)
    {
        uint256 new_pos;
        AdminOperation_CreateCommittee memory value;
        (new_pos, value) = bcs_deserialize_offset_AdminOperation_CreateCommittee(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct AdminOperation_PublishCommitteeBlob {
        CryptoHash blob_hash;
    }

    function bcs_serialize_AdminOperation_PublishCommitteeBlob(AdminOperation_PublishCommitteeBlob memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_CryptoHash(input.blob_hash);
    }

    function bcs_deserialize_offset_AdminOperation_PublishCommitteeBlob(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AdminOperation_PublishCommitteeBlob memory)
    {
        uint256 new_pos;
        CryptoHash memory blob_hash;
        (new_pos, blob_hash) = bcs_deserialize_offset_CryptoHash(pos, input);
        return (new_pos, AdminOperation_PublishCommitteeBlob(blob_hash));
    }

    function bcs_deserialize_AdminOperation_PublishCommitteeBlob(bytes memory input)
        internal
        pure
        returns (AdminOperation_PublishCommitteeBlob memory)
    {
        uint256 new_pos;
        AdminOperation_PublishCommitteeBlob memory value;
        (new_pos, value) = bcs_deserialize_offset_AdminOperation_PublishCommitteeBlob(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct AdminOperation_RemoveCommittee {
        Epoch epoch;
    }

    function bcs_serialize_AdminOperation_RemoveCommittee(AdminOperation_RemoveCommittee memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_Epoch(input.epoch);
    }

    function bcs_deserialize_offset_AdminOperation_RemoveCommittee(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AdminOperation_RemoveCommittee memory)
    {
        uint256 new_pos;
        Epoch memory epoch;
        (new_pos, epoch) = bcs_deserialize_offset_Epoch(pos, input);
        return (new_pos, AdminOperation_RemoveCommittee(epoch));
    }

    function bcs_deserialize_AdminOperation_RemoveCommittee(bytes memory input)
        internal
        pure
        returns (AdminOperation_RemoveCommittee memory)
    {
        uint256 new_pos;
        AdminOperation_RemoveCommittee memory value;
        (new_pos, value) = bcs_deserialize_offset_AdminOperation_RemoveCommittee(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Amount {
        uint128 value;
    }

    function bcs_serialize_Amount(Amount memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_uint128(input.value);
    }

    function bcs_deserialize_offset_Amount(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Amount memory)
    {
        uint256 new_pos;
        uint128 value;
        (new_pos, value) = bcs_deserialize_offset_uint128(pos, input);
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

    struct ApplicationPermissions {
        opt_seq_ApplicationId execute_operations;
        ApplicationId[] mandatory_applications;
        ApplicationId[] close_chain;
        ApplicationId[] change_application_permissions;
        opt_seq_ApplicationId call_service_as_oracle;
        opt_seq_ApplicationId make_http_requests;
    }

    function bcs_serialize_ApplicationPermissions(ApplicationPermissions memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_opt_seq_ApplicationId(input.execute_operations);
        result = abi.encodePacked(result, bcs_serialize_seq_ApplicationId(input.mandatory_applications));
        result = abi.encodePacked(result, bcs_serialize_seq_ApplicationId(input.close_chain));
        result = abi.encodePacked(result, bcs_serialize_seq_ApplicationId(input.change_application_permissions));
        result = abi.encodePacked(result, bcs_serialize_opt_seq_ApplicationId(input.call_service_as_oracle));
        return abi.encodePacked(result, bcs_serialize_opt_seq_ApplicationId(input.make_http_requests));
    }

    function bcs_deserialize_offset_ApplicationPermissions(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ApplicationPermissions memory)
    {
        uint256 new_pos;
        opt_seq_ApplicationId memory execute_operations;
        (new_pos, execute_operations) = bcs_deserialize_offset_opt_seq_ApplicationId(pos, input);
        ApplicationId[] memory mandatory_applications;
        (new_pos, mandatory_applications) = bcs_deserialize_offset_seq_ApplicationId(new_pos, input);
        ApplicationId[] memory close_chain;
        (new_pos, close_chain) = bcs_deserialize_offset_seq_ApplicationId(new_pos, input);
        ApplicationId[] memory change_application_permissions;
        (new_pos, change_application_permissions) = bcs_deserialize_offset_seq_ApplicationId(new_pos, input);
        opt_seq_ApplicationId memory call_service_as_oracle;
        (new_pos, call_service_as_oracle) = bcs_deserialize_offset_opt_seq_ApplicationId(new_pos, input);
        opt_seq_ApplicationId memory make_http_requests;
        (new_pos, make_http_requests) = bcs_deserialize_offset_opt_seq_ApplicationId(new_pos, input);
        return (new_pos, ApplicationPermissions(execute_operations, mandatory_applications, close_chain, change_application_permissions, call_service_as_oracle, make_http_requests));
    }

    function bcs_deserialize_ApplicationPermissions(bytes memory input)
        internal
        pure
        returns (ApplicationPermissions memory)
    {
        uint256 new_pos;
        ApplicationPermissions memory value;
        (new_pos, value) = bcs_deserialize_offset_ApplicationPermissions(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlobContent {
        BlobType blob_type;
        bytes bytes_;
    }

    function bcs_serialize_BlobContent(BlobContent memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_BlobType(input.blob_type);
        return abi.encodePacked(result, bcs_serialize_bytes(input.bytes_));
    }

    function bcs_deserialize_offset_BlobContent(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobContent memory)
    {
        uint256 new_pos;
        BlobType blob_type;
        (new_pos, blob_type) = bcs_deserialize_offset_BlobType(pos, input);
        bytes memory bytes_;
        (new_pos, bytes_) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, BlobContent(blob_type, bytes_));
    }

    function bcs_deserialize_BlobContent(bytes memory input)
        internal
        pure
        returns (BlobContent memory)
    {
        uint256 new_pos;
        BlobContent memory value;
        (new_pos, value) = bcs_deserialize_offset_BlobContent(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlobId {
        CryptoHash hash;
        BlobType blob_type;
    }

    function bcs_serialize_BlobId(BlobId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_CryptoHash(input.hash);
        return abi.encodePacked(result, bcs_serialize_BlobType(input.blob_type));
    }

    function bcs_deserialize_offset_BlobId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobId memory)
    {
        uint256 new_pos;
        CryptoHash memory hash;
        (new_pos, hash) = bcs_deserialize_offset_CryptoHash(pos, input);
        BlobType blob_type;
        (new_pos, blob_type) = bcs_deserialize_offset_BlobType(new_pos, input);
        return (new_pos, BlobId(hash, blob_type));
    }

    function bcs_deserialize_BlobId(bytes memory input)
        internal
        pure
        returns (BlobId memory)
    {
        uint256 new_pos;
        BlobId memory value;
        (new_pos, value) = bcs_deserialize_offset_BlobId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum BlobType { Data, ContractBytecode, ServiceBytecode, EvmBytecode, ApplicationDescription, Committee, ChainDescription }

    function bcs_serialize_BlobType(BlobType input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_BlobType(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobType)
    {
        uint8 choice = uint8(input[pos]);

        if (choice == 0) {
            return (pos + 1, BlobType.Data);
        }

        if (choice == 1) {
            return (pos + 1, BlobType.ContractBytecode);
        }

        if (choice == 2) {
            return (pos + 1, BlobType.ServiceBytecode);
        }

        if (choice == 3) {
            return (pos + 1, BlobType.EvmBytecode);
        }

        if (choice == 4) {
            return (pos + 1, BlobType.ApplicationDescription);
        }

        if (choice == 5) {
            return (pos + 1, BlobType.Committee);
        }

        if (choice == 6) {
            return (pos + 1, BlobType.ChainDescription);
        }

        require(choice < 7);
    }

    function bcs_deserialize_BlobType(bytes memory input)
        internal
        pure
        returns (BlobType)
    {
        uint256 new_pos;
        BlobType value;
        (new_pos, value) = bcs_deserialize_offset_BlobType(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Block {
        BlockHeader header;
        BlockBody body;
    }

    function bcs_serialize_Block(Block memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_BlockHeader(input.header);
        return abi.encodePacked(result, bcs_serialize_BlockBody(input.body));
    }

    function bcs_deserialize_offset_Block(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Block memory)
    {
        uint256 new_pos;
        BlockHeader memory header;
        (new_pos, header) = bcs_deserialize_offset_BlockHeader(pos, input);
        BlockBody memory body;
        (new_pos, body) = bcs_deserialize_offset_BlockBody(new_pos, input);
        return (new_pos, Block(header, body));
    }

    function bcs_deserialize_Block(bytes memory input)
        internal
        pure
        returns (Block memory)
    {
        uint256 new_pos;
        Block memory value;
        (new_pos, value) = bcs_deserialize_offset_Block(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlockBody {
        Transaction[] transactions;
        OutgoingMessage[][] messages;
        key_values_ChainId_tuple_CryptoHash_BlockHeight[] previous_message_blocks;
        key_values_StreamId_tuple_CryptoHash_BlockHeight[] previous_event_blocks;
        OracleResponse[][] oracle_responses;
        Event[][] events;
        BlobContent[][] blobs;
        OperationResult[] operation_results;
    }

    function bcs_serialize_BlockBody(BlockBody memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_seq_Transaction(input.transactions);
        result = abi.encodePacked(result, bcs_serialize_seq_seq_OutgoingMessage(input.messages));
        result = abi.encodePacked(result, bcs_serialize_seq_key_values_ChainId_tuple_CryptoHash_BlockHeight(input.previous_message_blocks));
        result = abi.encodePacked(result, bcs_serialize_seq_key_values_StreamId_tuple_CryptoHash_BlockHeight(input.previous_event_blocks));
        result = abi.encodePacked(result, bcs_serialize_seq_seq_OracleResponse(input.oracle_responses));
        result = abi.encodePacked(result, bcs_serialize_seq_seq_Event(input.events));
        result = abi.encodePacked(result, bcs_serialize_seq_seq_BlobContent(input.blobs));
        return abi.encodePacked(result, bcs_serialize_seq_OperationResult(input.operation_results));
    }

    function bcs_deserialize_offset_BlockBody(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlockBody memory)
    {
        uint256 new_pos;
        Transaction[] memory transactions;
        (new_pos, transactions) = bcs_deserialize_offset_seq_Transaction(pos, input);
        OutgoingMessage[][] memory messages;
        (new_pos, messages) = bcs_deserialize_offset_seq_seq_OutgoingMessage(new_pos, input);
        key_values_ChainId_tuple_CryptoHash_BlockHeight[] memory previous_message_blocks;
        (new_pos, previous_message_blocks) = bcs_deserialize_offset_seq_key_values_ChainId_tuple_CryptoHash_BlockHeight(new_pos, input);
        key_values_StreamId_tuple_CryptoHash_BlockHeight[] memory previous_event_blocks;
        (new_pos, previous_event_blocks) = bcs_deserialize_offset_seq_key_values_StreamId_tuple_CryptoHash_BlockHeight(new_pos, input);
        OracleResponse[][] memory oracle_responses;
        (new_pos, oracle_responses) = bcs_deserialize_offset_seq_seq_OracleResponse(new_pos, input);
        Event[][] memory events;
        (new_pos, events) = bcs_deserialize_offset_seq_seq_Event(new_pos, input);
        BlobContent[][] memory blobs;
        (new_pos, blobs) = bcs_deserialize_offset_seq_seq_BlobContent(new_pos, input);
        OperationResult[] memory operation_results;
        (new_pos, operation_results) = bcs_deserialize_offset_seq_OperationResult(new_pos, input);
        return (new_pos, BlockBody(transactions, messages, previous_message_blocks, previous_event_blocks, oracle_responses, events, blobs, operation_results));
    }

    function bcs_deserialize_BlockBody(bytes memory input)
        internal
        pure
        returns (BlockBody memory)
    {
        uint256 new_pos;
        BlockBody memory value;
        (new_pos, value) = bcs_deserialize_offset_BlockBody(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlockHeader {
        ChainId chain_id;
        Epoch epoch;
        BlockHeight height;
        Timestamp timestamp;
        CryptoHash state_hash;
        opt_CryptoHash previous_block_hash;
        opt_AccountOwner authenticated_signer;
    }

    function bcs_serialize_BlockHeader(BlockHeader memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_Epoch(input.epoch));
        result = abi.encodePacked(result, bcs_serialize_BlockHeight(input.height));
        result = abi.encodePacked(result, bcs_serialize_Timestamp(input.timestamp));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.state_hash));
        result = abi.encodePacked(result, bcs_serialize_opt_CryptoHash(input.previous_block_hash));
        return abi.encodePacked(result, bcs_serialize_opt_AccountOwner(input.authenticated_signer));
    }

    function bcs_deserialize_offset_BlockHeader(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlockHeader memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        Epoch memory epoch;
        (new_pos, epoch) = bcs_deserialize_offset_Epoch(new_pos, input);
        BlockHeight memory height;
        (new_pos, height) = bcs_deserialize_offset_BlockHeight(new_pos, input);
        Timestamp memory timestamp;
        (new_pos, timestamp) = bcs_deserialize_offset_Timestamp(new_pos, input);
        CryptoHash memory state_hash;
        (new_pos, state_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        opt_CryptoHash memory previous_block_hash;
        (new_pos, previous_block_hash) = bcs_deserialize_offset_opt_CryptoHash(new_pos, input);
        opt_AccountOwner memory authenticated_signer;
        (new_pos, authenticated_signer) = bcs_deserialize_offset_opt_AccountOwner(new_pos, input);
        return (new_pos, BlockHeader(chain_id, epoch, height, timestamp, state_hash, previous_block_hash, authenticated_signer));
    }

    function bcs_deserialize_BlockHeader(bytes memory input)
        internal
        pure
        returns (BlockHeader memory)
    {
        uint256 new_pos;
        BlockHeader memory value;
        (new_pos, value) = bcs_deserialize_offset_BlockHeader(0, input);
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

    enum CertificateKind { Timeout, Validated, Confirmed }

    function bcs_serialize_CertificateKind(CertificateKind input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_CertificateKind(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, CertificateKind)
    {
        uint8 choice = uint8(input[pos]);

        if (choice == 0) {
            return (pos + 1, CertificateKind.Timeout);
        }

        if (choice == 1) {
            return (pos + 1, CertificateKind.Validated);
        }

        if (choice == 2) {
            return (pos + 1, CertificateKind.Confirmed);
        }

        require(choice < 3);
    }

    function bcs_deserialize_CertificateKind(bytes memory input)
        internal
        pure
        returns (CertificateKind)
    {
        uint256 new_pos;
        CertificateKind value;
        (new_pos, value) = bcs_deserialize_offset_CertificateKind(0, input);
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

    struct ConfirmedBlockCertificate {
        Block value;
        Round round;
        tuple_Secp256k1PublicKey_Secp256k1Signature[] signatures;
    }

    function bcs_serialize_ConfirmedBlockCertificate(ConfirmedBlockCertificate memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_Block(input.value);
        result = abi.encodePacked(result, bcs_serialize_Round(input.round));
        return abi.encodePacked(result, bcs_serialize_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(input.signatures));
    }

    function bcs_deserialize_offset_ConfirmedBlockCertificate(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ConfirmedBlockCertificate memory)
    {
        uint256 new_pos;
        Block memory value;
        (new_pos, value) = bcs_deserialize_offset_Block(pos, input);
        Round memory round;
        (new_pos, round) = bcs_deserialize_offset_Round(new_pos, input);
        tuple_Secp256k1PublicKey_Secp256k1Signature[] memory signatures;
        (new_pos, signatures) = bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(new_pos, input);
        return (new_pos, ConfirmedBlockCertificate(value, round, signatures));
    }

    function bcs_deserialize_ConfirmedBlockCertificate(bytes memory input)
        internal
        pure
        returns (ConfirmedBlockCertificate memory)
    {
        uint256 new_pos;
        ConfirmedBlockCertificate memory value;
        (new_pos, value) = bcs_deserialize_offset_ConfirmedBlockCertificate(0, input);
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

    struct Epoch {
        uint32 value;
    }

    function bcs_serialize_Epoch(Epoch memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_uint32(input.value);
    }

    function bcs_deserialize_offset_Epoch(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Epoch memory)
    {
        uint256 new_pos;
        uint32 value;
        (new_pos, value) = bcs_deserialize_offset_uint32(pos, input);
        return (new_pos, Epoch(value));
    }

    function bcs_deserialize_Epoch(bytes memory input)
        internal
        pure
        returns (Epoch memory)
    {
        uint256 new_pos;
        Epoch memory value;
        (new_pos, value) = bcs_deserialize_offset_Epoch(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Event {
        StreamId stream_id;
        uint32 index;
        bytes value;
    }

    function bcs_serialize_Event(Event memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_StreamId(input.stream_id);
        result = abi.encodePacked(result, bcs_serialize_uint32(input.index));
        return abi.encodePacked(result, bcs_serialize_bytes(input.value));
    }

    function bcs_deserialize_offset_Event(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Event memory)
    {
        uint256 new_pos;
        StreamId memory stream_id;
        (new_pos, stream_id) = bcs_deserialize_offset_StreamId(pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, Event(stream_id, index, value));
    }

    function bcs_deserialize_Event(bytes memory input)
        internal
        pure
        returns (Event memory)
    {
        uint256 new_pos;
        Event memory value;
        (new_pos, value) = bcs_deserialize_offset_Event(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct EventId {
        ChainId chain_id;
        StreamId stream_id;
        uint32 index;
    }

    function bcs_serialize_EventId(EventId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_StreamId(input.stream_id));
        return abi.encodePacked(result, bcs_serialize_uint32(input.index));
    }

    function bcs_deserialize_offset_EventId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, EventId memory)
    {
        uint256 new_pos;
        ChainId memory chain_id;
        (new_pos, chain_id) = bcs_deserialize_offset_ChainId(pos, input);
        StreamId memory stream_id;
        (new_pos, stream_id) = bcs_deserialize_offset_StreamId(new_pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, EventId(chain_id, stream_id, index));
    }

    function bcs_deserialize_EventId(bytes memory input)
        internal
        pure
        returns (EventId memory)
    {
        uint256 new_pos;
        EventId memory value;
        (new_pos, value) = bcs_deserialize_offset_EventId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct EvmPublicKey {
        tuplearray33_uint8 value;
    }

    function bcs_serialize_EvmPublicKey(EvmPublicKey memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_tuplearray33_uint8(input.value);
    }

    function bcs_deserialize_offset_EvmPublicKey(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, EvmPublicKey memory)
    {
        uint256 new_pos;
        tuplearray33_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray33_uint8(pos, input);
        return (new_pos, EvmPublicKey(value));
    }

    function bcs_deserialize_EvmPublicKey(bytes memory input)
        internal
        pure
        returns (EvmPublicKey memory)
    {
        uint256 new_pos;
        EvmPublicKey memory value;
        (new_pos, value) = bcs_deserialize_offset_EvmPublicKey(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct EvmSignature {
        tuplearray65_uint8 value;
    }

    function bcs_serialize_EvmSignature(EvmSignature memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_tuplearray65_uint8(input.value);
    }

    function bcs_deserialize_offset_EvmSignature(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, EvmSignature memory)
    {
        uint256 new_pos;
        tuplearray65_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray65_uint8(pos, input);
        return (new_pos, EvmSignature(value));
    }

    function bcs_deserialize_EvmSignature(bytes memory input)
        internal
        pure
        returns (EvmSignature memory)
    {
        uint256 new_pos;
        EvmSignature memory value;
        (new_pos, value) = bcs_deserialize_offset_EvmSignature(0, input);
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

    struct Header {
        string name;
        bytes value;
    }

    function bcs_serialize_Header(Header memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_string(input.name);
        return abi.encodePacked(result, bcs_serialize_bytes(input.value));
    }

    function bcs_deserialize_offset_Header(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Header memory)
    {
        uint256 new_pos;
        string memory name;
        (new_pos, name) = bcs_deserialize_offset_string(pos, input);
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, Header(name, value));
    }

    function bcs_deserialize_Header(bytes memory input)
        internal
        pure
        returns (Header memory)
    {
        uint256 new_pos;
        Header memory value;
        (new_pos, value) = bcs_deserialize_offset_Header(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct IncomingBundle {
        ChainId origin;
        MessageBundle bundle;
        MessageAction action;
    }

    function bcs_serialize_IncomingBundle(IncomingBundle memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.origin);
        result = abi.encodePacked(result, bcs_serialize_MessageBundle(input.bundle));
        return abi.encodePacked(result, bcs_serialize_MessageAction(input.action));
    }

    function bcs_deserialize_offset_IncomingBundle(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, IncomingBundle memory)
    {
        uint256 new_pos;
        ChainId memory origin;
        (new_pos, origin) = bcs_deserialize_offset_ChainId(pos, input);
        MessageBundle memory bundle;
        (new_pos, bundle) = bcs_deserialize_offset_MessageBundle(new_pos, input);
        MessageAction action;
        (new_pos, action) = bcs_deserialize_offset_MessageAction(new_pos, input);
        return (new_pos, IncomingBundle(origin, bundle, action));
    }

    function bcs_deserialize_IncomingBundle(bytes memory input)
        internal
        pure
        returns (IncomingBundle memory)
    {
        uint256 new_pos;
        IncomingBundle memory value;
        (new_pos, value) = bcs_deserialize_offset_IncomingBundle(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message {
        uint8 choice;
        // choice=0 corresponds to System
        SystemMessage system;
        // choice=1 corresponds to User
        Message_User user;
    }

    function Message_case_system(SystemMessage memory system)
        internal
        pure
        returns (Message memory)
    {
        Message_User memory user;
        return Message(uint8(0), system, user);
    }

    function Message_case_user(Message_User memory user)
        internal
        pure
        returns (Message memory)
    {
        SystemMessage memory system;
        return Message(uint8(1), system, user);
    }

    function bcs_serialize_Message(Message memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemMessage(input.system));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_Message_User(input.user));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_Message(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        SystemMessage memory system;
        if (choice == 0) {
            (new_pos, system) = bcs_deserialize_offset_SystemMessage(new_pos, input);
        }
        Message_User memory user;
        if (choice == 1) {
            (new_pos, user) = bcs_deserialize_offset_Message_User(new_pos, input);
        }
        require(choice < 2);
        return (new_pos, Message(choice, system, user));
    }

    function bcs_deserialize_Message(bytes memory input)
        internal
        pure
        returns (Message memory)
    {
        uint256 new_pos;
        Message memory value;
        (new_pos, value) = bcs_deserialize_offset_Message(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum MessageAction { Accept, Reject }

    function bcs_serialize_MessageAction(MessageAction input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_MessageAction(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageAction)
    {
        uint8 choice = uint8(input[pos]);

        if (choice == 0) {
            return (pos + 1, MessageAction.Accept);
        }

        if (choice == 1) {
            return (pos + 1, MessageAction.Reject);
        }

        require(choice < 2);
    }

    function bcs_deserialize_MessageAction(bytes memory input)
        internal
        pure
        returns (MessageAction)
    {
        uint256 new_pos;
        MessageAction value;
        (new_pos, value) = bcs_deserialize_offset_MessageAction(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct MessageBundle {
        BlockHeight height;
        Timestamp timestamp;
        CryptoHash certificate_hash;
        uint32 transaction_index;
        PostedMessage[] messages;
    }

    function bcs_serialize_MessageBundle(MessageBundle memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_BlockHeight(input.height);
        result = abi.encodePacked(result, bcs_serialize_Timestamp(input.timestamp));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.certificate_hash));
        result = abi.encodePacked(result, bcs_serialize_uint32(input.transaction_index));
        return abi.encodePacked(result, bcs_serialize_seq_PostedMessage(input.messages));
    }

    function bcs_deserialize_offset_MessageBundle(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageBundle memory)
    {
        uint256 new_pos;
        BlockHeight memory height;
        (new_pos, height) = bcs_deserialize_offset_BlockHeight(pos, input);
        Timestamp memory timestamp;
        (new_pos, timestamp) = bcs_deserialize_offset_Timestamp(new_pos, input);
        CryptoHash memory certificate_hash;
        (new_pos, certificate_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        uint32 transaction_index;
        (new_pos, transaction_index) = bcs_deserialize_offset_uint32(new_pos, input);
        PostedMessage[] memory messages;
        (new_pos, messages) = bcs_deserialize_offset_seq_PostedMessage(new_pos, input);
        return (new_pos, MessageBundle(height, timestamp, certificate_hash, transaction_index, messages));
    }

    function bcs_deserialize_MessageBundle(bytes memory input)
        internal
        pure
        returns (MessageBundle memory)
    {
        uint256 new_pos;
        MessageBundle memory value;
        (new_pos, value) = bcs_deserialize_offset_MessageBundle(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum MessageKind { Simple, Protected, Tracked, Bouncing }

    function bcs_serialize_MessageKind(MessageKind input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_MessageKind(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, MessageKind)
    {
        uint8 choice = uint8(input[pos]);

        if (choice == 0) {
            return (pos + 1, MessageKind.Simple);
        }

        if (choice == 1) {
            return (pos + 1, MessageKind.Protected);
        }

        if (choice == 2) {
            return (pos + 1, MessageKind.Tracked);
        }

        if (choice == 3) {
            return (pos + 1, MessageKind.Bouncing);
        }

        require(choice < 4);
    }

    function bcs_deserialize_MessageKind(bytes memory input)
        internal
        pure
        returns (MessageKind)
    {
        uint256 new_pos;
        MessageKind value;
        (new_pos, value) = bcs_deserialize_offset_MessageKind(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message_User {
        ApplicationId application_id;
        bytes bytes_;
    }

    function bcs_serialize_Message_User(Message_User memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ApplicationId(input.application_id);
        return abi.encodePacked(result, bcs_serialize_bytes(input.bytes_));
    }

    function bcs_deserialize_offset_Message_User(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message_User memory)
    {
        uint256 new_pos;
        ApplicationId memory application_id;
        (new_pos, application_id) = bcs_deserialize_offset_ApplicationId(pos, input);
        bytes memory bytes_;
        (new_pos, bytes_) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, Message_User(application_id, bytes_));
    }

    function bcs_deserialize_Message_User(bytes memory input)
        internal
        pure
        returns (Message_User memory)
    {
        uint256 new_pos;
        Message_User memory value;
        (new_pos, value) = bcs_deserialize_offset_Message_User(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ModuleId {
        CryptoHash contract_blob_hash;
        CryptoHash service_blob_hash;
        VmRuntime vm_runtime;
    }

    function bcs_serialize_ModuleId(ModuleId memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_CryptoHash(input.contract_blob_hash);
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.service_blob_hash));
        return abi.encodePacked(result, bcs_serialize_VmRuntime(input.vm_runtime));
    }

    function bcs_deserialize_offset_ModuleId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ModuleId memory)
    {
        uint256 new_pos;
        CryptoHash memory contract_blob_hash;
        (new_pos, contract_blob_hash) = bcs_deserialize_offset_CryptoHash(pos, input);
        CryptoHash memory service_blob_hash;
        (new_pos, service_blob_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        VmRuntime vm_runtime;
        (new_pos, vm_runtime) = bcs_deserialize_offset_VmRuntime(new_pos, input);
        return (new_pos, ModuleId(contract_blob_hash, service_blob_hash, vm_runtime));
    }

    function bcs_deserialize_ModuleId(bytes memory input)
        internal
        pure
        returns (ModuleId memory)
    {
        uint256 new_pos;
        ModuleId memory value;
        (new_pos, value) = bcs_deserialize_offset_ModuleId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OpenChainConfig {
        ChainOwnership ownership;
        Amount balance_;
        ApplicationPermissions application_permissions;
    }

    function bcs_serialize_OpenChainConfig(OpenChainConfig memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainOwnership(input.ownership);
        result = abi.encodePacked(result, bcs_serialize_Amount(input.balance_));
        return abi.encodePacked(result, bcs_serialize_ApplicationPermissions(input.application_permissions));
    }

    function bcs_deserialize_offset_OpenChainConfig(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OpenChainConfig memory)
    {
        uint256 new_pos;
        ChainOwnership memory ownership;
        (new_pos, ownership) = bcs_deserialize_offset_ChainOwnership(pos, input);
        Amount memory balance_;
        (new_pos, balance_) = bcs_deserialize_offset_Amount(new_pos, input);
        ApplicationPermissions memory application_permissions;
        (new_pos, application_permissions) = bcs_deserialize_offset_ApplicationPermissions(new_pos, input);
        return (new_pos, OpenChainConfig(ownership, balance_, application_permissions));
    }

    function bcs_deserialize_OpenChainConfig(bytes memory input)
        internal
        pure
        returns (OpenChainConfig memory)
    {
        uint256 new_pos;
        OpenChainConfig memory value;
        (new_pos, value) = bcs_deserialize_offset_OpenChainConfig(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Operation {
        uint8 choice;
        // choice=0 corresponds to System
        SystemOperation system;
        // choice=1 corresponds to User
        Operation_User user;
    }

    function Operation_case_system(SystemOperation memory system)
        internal
        pure
        returns (Operation memory)
    {
        Operation_User memory user;
        return Operation(uint8(0), system, user);
    }

    function Operation_case_user(Operation_User memory user)
        internal
        pure
        returns (Operation memory)
    {
        SystemOperation memory system;
        return Operation(uint8(1), system, user);
    }

    function bcs_serialize_Operation(Operation memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation(input.system));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_Operation_User(input.user));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_Operation(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Operation memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        SystemOperation memory system;
        if (choice == 0) {
            (new_pos, system) = bcs_deserialize_offset_SystemOperation(new_pos, input);
        }
        Operation_User memory user;
        if (choice == 1) {
            (new_pos, user) = bcs_deserialize_offset_Operation_User(new_pos, input);
        }
        require(choice < 2);
        return (new_pos, Operation(choice, system, user));
    }

    function bcs_deserialize_Operation(bytes memory input)
        internal
        pure
        returns (Operation memory)
    {
        uint256 new_pos;
        Operation memory value;
        (new_pos, value) = bcs_deserialize_offset_Operation(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OperationResult {
        bytes value;
    }

    function bcs_serialize_OperationResult(OperationResult memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_bytes(input.value);
    }

    function bcs_deserialize_offset_OperationResult(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OperationResult memory)
    {
        uint256 new_pos;
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(pos, input);
        return (new_pos, OperationResult(value));
    }

    function bcs_deserialize_OperationResult(bytes memory input)
        internal
        pure
        returns (OperationResult memory)
    {
        uint256 new_pos;
        OperationResult memory value;
        (new_pos, value) = bcs_deserialize_offset_OperationResult(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Operation_User {
        ApplicationId application_id;
        bytes bytes_;
    }

    function bcs_serialize_Operation_User(Operation_User memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ApplicationId(input.application_id);
        return abi.encodePacked(result, bcs_serialize_bytes(input.bytes_));
    }

    function bcs_deserialize_offset_Operation_User(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Operation_User memory)
    {
        uint256 new_pos;
        ApplicationId memory application_id;
        (new_pos, application_id) = bcs_deserialize_offset_ApplicationId(pos, input);
        bytes memory bytes_;
        (new_pos, bytes_) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, Operation_User(application_id, bytes_));
    }

    function bcs_deserialize_Operation_User(bytes memory input)
        internal
        pure
        returns (Operation_User memory)
    {
        uint256 new_pos;
        Operation_User memory value;
        (new_pos, value) = bcs_deserialize_offset_Operation_User(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OracleResponse {
        uint8 choice;
        // choice=0 corresponds to Service
        bytes service;
        // choice=1 corresponds to Http
        Response http;
        // choice=2 corresponds to Blob
        BlobId blob;
        // choice=3 corresponds to Assert
        // choice=4 corresponds to Round
        opt_uint32 round;
        // choice=5 corresponds to Event
        OracleResponse_Event event_;
        // choice=6 corresponds to EventExists
        EventId event_exists;
    }

    function OracleResponse_case_service(bytes memory service)
        internal
        pure
        returns (OracleResponse memory)
    {
        Response memory http;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        return OracleResponse(uint8(0), service, http, blob, round, event_, event_exists);
    }

    function OracleResponse_case_http(Response memory http)
        internal
        pure
        returns (OracleResponse memory)
    {
        bytes memory service;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        return OracleResponse(uint8(1), service, http, blob, round, event_, event_exists);
    }

    function OracleResponse_case_blob(BlobId memory blob)
        internal
        pure
        returns (OracleResponse memory)
    {
        bytes memory service;
        Response memory http;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        return OracleResponse(uint8(2), service, http, blob, round, event_, event_exists);
    }

    function OracleResponse_case_assert()
        internal
        pure
        returns (OracleResponse memory)
    {
        bytes memory service;
        Response memory http;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        return OracleResponse(uint8(3), service, http, blob, round, event_, event_exists);
    }

    function OracleResponse_case_round(opt_uint32 memory round)
        internal
        pure
        returns (OracleResponse memory)
    {
        bytes memory service;
        Response memory http;
        BlobId memory blob;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        return OracleResponse(uint8(4), service, http, blob, round, event_, event_exists);
    }

    function OracleResponse_case_event(OracleResponse_Event memory event_)
        internal
        pure
        returns (OracleResponse memory)
    {
        bytes memory service;
        Response memory http;
        BlobId memory blob;
        opt_uint32 memory round;
        EventId memory event_exists;
        return OracleResponse(uint8(5), service, http, blob, round, event_, event_exists);
    }

    function OracleResponse_case_event_exists(EventId memory event_exists)
        internal
        pure
        returns (OracleResponse memory)
    {
        bytes memory service;
        Response memory http;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        return OracleResponse(uint8(6), service, http, blob, round, event_, event_exists);
    }

    function bcs_serialize_OracleResponse(OracleResponse memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_bytes(input.service));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_Response(input.http));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_BlobId(input.blob));
        }
        if (input.choice == 4) {
            return abi.encodePacked(input.choice, bcs_serialize_opt_uint32(input.round));
        }
        if (input.choice == 5) {
            return abi.encodePacked(input.choice, bcs_serialize_OracleResponse_Event(input.event_));
        }
        if (input.choice == 6) {
            return abi.encodePacked(input.choice, bcs_serialize_EventId(input.event_exists));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_OracleResponse(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OracleResponse memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        bytes memory service;
        if (choice == 0) {
            (new_pos, service) = bcs_deserialize_offset_bytes(new_pos, input);
        }
        Response memory http;
        if (choice == 1) {
            (new_pos, http) = bcs_deserialize_offset_Response(new_pos, input);
        }
        BlobId memory blob;
        if (choice == 2) {
            (new_pos, blob) = bcs_deserialize_offset_BlobId(new_pos, input);
        }
        opt_uint32 memory round;
        if (choice == 4) {
            (new_pos, round) = bcs_deserialize_offset_opt_uint32(new_pos, input);
        }
        OracleResponse_Event memory event_;
        if (choice == 5) {
            (new_pos, event_) = bcs_deserialize_offset_OracleResponse_Event(new_pos, input);
        }
        EventId memory event_exists;
        if (choice == 6) {
            (new_pos, event_exists) = bcs_deserialize_offset_EventId(new_pos, input);
        }
        require(choice < 7);
        return (new_pos, OracleResponse(choice, service, http, blob, round, event_, event_exists));
    }

    function bcs_deserialize_OracleResponse(bytes memory input)
        internal
        pure
        returns (OracleResponse memory)
    {
        uint256 new_pos;
        OracleResponse memory value;
        (new_pos, value) = bcs_deserialize_offset_OracleResponse(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OracleResponse_Event {
        EventId entry0;
        bytes entry1;
    }

    function bcs_serialize_OracleResponse_Event(OracleResponse_Event memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_EventId(input.entry0);
        return abi.encodePacked(result, bcs_serialize_bytes(input.entry1));
    }

    function bcs_deserialize_offset_OracleResponse_Event(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OracleResponse_Event memory)
    {
        uint256 new_pos;
        EventId memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_EventId(pos, input);
        bytes memory entry1;
        (new_pos, entry1) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, OracleResponse_Event(entry0, entry1));
    }

    function bcs_deserialize_OracleResponse_Event(bytes memory input)
        internal
        pure
        returns (OracleResponse_Event memory)
    {
        uint256 new_pos;
        OracleResponse_Event memory value;
        (new_pos, value) = bcs_deserialize_offset_OracleResponse_Event(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OutgoingMessage {
        ChainId destination;
        opt_AccountOwner authenticated_signer;
        Amount grant;
        opt_Account refund_grant_to;
        MessageKind kind;
        Message message;
    }

    function bcs_serialize_OutgoingMessage(OutgoingMessage memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.destination);
        result = abi.encodePacked(result, bcs_serialize_opt_AccountOwner(input.authenticated_signer));
        result = abi.encodePacked(result, bcs_serialize_Amount(input.grant));
        result = abi.encodePacked(result, bcs_serialize_opt_Account(input.refund_grant_to));
        result = abi.encodePacked(result, bcs_serialize_MessageKind(input.kind));
        return abi.encodePacked(result, bcs_serialize_Message(input.message));
    }

    function bcs_deserialize_offset_OutgoingMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OutgoingMessage memory)
    {
        uint256 new_pos;
        ChainId memory destination;
        (new_pos, destination) = bcs_deserialize_offset_ChainId(pos, input);
        opt_AccountOwner memory authenticated_signer;
        (new_pos, authenticated_signer) = bcs_deserialize_offset_opt_AccountOwner(new_pos, input);
        Amount memory grant;
        (new_pos, grant) = bcs_deserialize_offset_Amount(new_pos, input);
        opt_Account memory refund_grant_to;
        (new_pos, refund_grant_to) = bcs_deserialize_offset_opt_Account(new_pos, input);
        MessageKind kind;
        (new_pos, kind) = bcs_deserialize_offset_MessageKind(new_pos, input);
        Message memory message;
        (new_pos, message) = bcs_deserialize_offset_Message(new_pos, input);
        return (new_pos, OutgoingMessage(destination, authenticated_signer, grant, refund_grant_to, kind, message));
    }

    function bcs_deserialize_OutgoingMessage(bytes memory input)
        internal
        pure
        returns (OutgoingMessage memory)
    {
        uint256 new_pos;
        OutgoingMessage memory value;
        (new_pos, value) = bcs_deserialize_offset_OutgoingMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct PostedMessage {
        opt_AccountOwner authenticated_signer;
        Amount grant;
        opt_Account refund_grant_to;
        MessageKind kind;
        uint32 index;
        Message message;
    }

    function bcs_serialize_PostedMessage(PostedMessage memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_opt_AccountOwner(input.authenticated_signer);
        result = abi.encodePacked(result, bcs_serialize_Amount(input.grant));
        result = abi.encodePacked(result, bcs_serialize_opt_Account(input.refund_grant_to));
        result = abi.encodePacked(result, bcs_serialize_MessageKind(input.kind));
        result = abi.encodePacked(result, bcs_serialize_uint32(input.index));
        return abi.encodePacked(result, bcs_serialize_Message(input.message));
    }

    function bcs_deserialize_offset_PostedMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, PostedMessage memory)
    {
        uint256 new_pos;
        opt_AccountOwner memory authenticated_signer;
        (new_pos, authenticated_signer) = bcs_deserialize_offset_opt_AccountOwner(pos, input);
        Amount memory grant;
        (new_pos, grant) = bcs_deserialize_offset_Amount(new_pos, input);
        opt_Account memory refund_grant_to;
        (new_pos, refund_grant_to) = bcs_deserialize_offset_opt_Account(new_pos, input);
        MessageKind kind;
        (new_pos, kind) = bcs_deserialize_offset_MessageKind(new_pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        Message memory message;
        (new_pos, message) = bcs_deserialize_offset_Message(new_pos, input);
        return (new_pos, PostedMessage(authenticated_signer, grant, refund_grant_to, kind, index, message));
    }

    function bcs_deserialize_PostedMessage(bytes memory input)
        internal
        pure
        returns (PostedMessage memory)
    {
        uint256 new_pos;
        PostedMessage memory value;
        (new_pos, value) = bcs_deserialize_offset_PostedMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Response {
        uint16 status;
        Header[] headers;
        bytes body;
    }

    function bcs_serialize_Response(Response memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_uint16(input.status);
        result = abi.encodePacked(result, bcs_serialize_seq_Header(input.headers));
        return abi.encodePacked(result, bcs_serialize_bytes(input.body));
    }

    function bcs_deserialize_offset_Response(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Response memory)
    {
        uint256 new_pos;
        uint16 status;
        (new_pos, status) = bcs_deserialize_offset_uint16(pos, input);
        Header[] memory headers;
        (new_pos, headers) = bcs_deserialize_offset_seq_Header(new_pos, input);
        bytes memory body;
        (new_pos, body) = bcs_deserialize_offset_bytes(new_pos, input);
        return (new_pos, Response(status, headers, body));
    }

    function bcs_deserialize_Response(bytes memory input)
        internal
        pure
        returns (Response memory)
    {
        uint256 new_pos;
        Response memory value;
        (new_pos, value) = bcs_deserialize_offset_Response(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Round {
        uint8 choice;
        // choice=0 corresponds to Fast
        // choice=1 corresponds to MultiLeader
        uint32 multi_leader;
        // choice=2 corresponds to SingleLeader
        uint32 single_leader;
        // choice=3 corresponds to Validator
        uint32 validator;
    }

    function Round_case_fast()
        internal
        pure
        returns (Round memory)
    {
        uint32 multi_leader;
        uint32 single_leader;
        uint32 validator;
        return Round(uint8(0), multi_leader, single_leader, validator);
    }

    function Round_case_multi_leader(uint32 multi_leader)
        internal
        pure
        returns (Round memory)
    {
        uint32 single_leader;
        uint32 validator;
        return Round(uint8(1), multi_leader, single_leader, validator);
    }

    function Round_case_single_leader(uint32 single_leader)
        internal
        pure
        returns (Round memory)
    {
        uint32 multi_leader;
        uint32 validator;
        return Round(uint8(2), multi_leader, single_leader, validator);
    }

    function Round_case_validator(uint32 validator)
        internal
        pure
        returns (Round memory)
    {
        uint32 multi_leader;
        uint32 single_leader;
        return Round(uint8(3), multi_leader, single_leader, validator);
    }

    function bcs_serialize_Round(Round memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_uint32(input.multi_leader));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_uint32(input.single_leader));
        }
        if (input.choice == 3) {
            return abi.encodePacked(input.choice, bcs_serialize_uint32(input.validator));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_Round(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Round memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        uint32 multi_leader;
        if (choice == 1) {
            (new_pos, multi_leader) = bcs_deserialize_offset_uint32(new_pos, input);
        }
        uint32 single_leader;
        if (choice == 2) {
            (new_pos, single_leader) = bcs_deserialize_offset_uint32(new_pos, input);
        }
        uint32 validator;
        if (choice == 3) {
            (new_pos, validator) = bcs_deserialize_offset_uint32(new_pos, input);
        }
        require(choice < 4);
        return (new_pos, Round(choice, multi_leader, single_leader, validator));
    }

    function bcs_deserialize_Round(bytes memory input)
        internal
        pure
        returns (Round memory)
    {
        uint256 new_pos;
        Round memory value;
        (new_pos, value) = bcs_deserialize_offset_Round(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Secp256k1PublicKey {
        tuplearray33_uint8 value;
    }

    function bcs_serialize_Secp256k1PublicKey(Secp256k1PublicKey memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_tuplearray33_uint8(input.value);
    }

    function bcs_deserialize_offset_Secp256k1PublicKey(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Secp256k1PublicKey memory)
    {
        uint256 new_pos;
        tuplearray33_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray33_uint8(pos, input);
        return (new_pos, Secp256k1PublicKey(value));
    }

    function bcs_deserialize_Secp256k1PublicKey(bytes memory input)
        internal
        pure
        returns (Secp256k1PublicKey memory)
    {
        uint256 new_pos;
        Secp256k1PublicKey memory value;
        (new_pos, value) = bcs_deserialize_offset_Secp256k1PublicKey(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Secp256k1Signature {
        tuplearray64_uint8 value;
    }

    function bcs_serialize_Secp256k1Signature(Secp256k1Signature memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_tuplearray64_uint8(input.value);
    }

    function bcs_deserialize_offset_Secp256k1Signature(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Secp256k1Signature memory)
    {
        uint256 new_pos;
        tuplearray64_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray64_uint8(pos, input);
        return (new_pos, Secp256k1Signature(value));
    }

    function bcs_deserialize_Secp256k1Signature(bytes memory input)
        internal
        pure
        returns (Secp256k1Signature memory)
    {
        uint256 new_pos;
        Secp256k1Signature memory value;
        (new_pos, value) = bcs_deserialize_offset_Secp256k1Signature(0, input);
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

    struct SystemMessage {
        uint8 choice;
        // choice=0 corresponds to Credit
        SystemMessage_Credit credit;
        // choice=1 corresponds to Withdraw
        SystemMessage_Withdraw withdraw;
    }

    function SystemMessage_case_credit(SystemMessage_Credit memory credit)
        internal
        pure
        returns (SystemMessage memory)
    {
        SystemMessage_Withdraw memory withdraw;
        return SystemMessage(uint8(0), credit, withdraw);
    }

    function SystemMessage_case_withdraw(SystemMessage_Withdraw memory withdraw)
        internal
        pure
        returns (SystemMessage memory)
    {
        SystemMessage_Credit memory credit;
        return SystemMessage(uint8(1), credit, withdraw);
    }

    function bcs_serialize_SystemMessage(SystemMessage memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemMessage_Credit(input.credit));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemMessage_Withdraw(input.withdraw));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_SystemMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemMessage memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        SystemMessage_Credit memory credit;
        if (choice == 0) {
            (new_pos, credit) = bcs_deserialize_offset_SystemMessage_Credit(new_pos, input);
        }
        SystemMessage_Withdraw memory withdraw;
        if (choice == 1) {
            (new_pos, withdraw) = bcs_deserialize_offset_SystemMessage_Withdraw(new_pos, input);
        }
        require(choice < 2);
        return (new_pos, SystemMessage(choice, credit, withdraw));
    }

    function bcs_deserialize_SystemMessage(bytes memory input)
        internal
        pure
        returns (SystemMessage memory)
    {
        uint256 new_pos;
        SystemMessage memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemMessage_Credit {
        AccountOwner target;
        Amount amount;
        AccountOwner source;
    }

    function bcs_serialize_SystemMessage_Credit(SystemMessage_Credit memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.target);
        result = abi.encodePacked(result, bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, bcs_serialize_AccountOwner(input.source));
    }

    function bcs_deserialize_offset_SystemMessage_Credit(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemMessage_Credit memory)
    {
        uint256 new_pos;
        AccountOwner memory target;
        (new_pos, target) = bcs_deserialize_offset_AccountOwner(pos, input);
        Amount memory amount;
        (new_pos, amount) = bcs_deserialize_offset_Amount(new_pos, input);
        AccountOwner memory source;
        (new_pos, source) = bcs_deserialize_offset_AccountOwner(new_pos, input);
        return (new_pos, SystemMessage_Credit(target, amount, source));
    }

    function bcs_deserialize_SystemMessage_Credit(bytes memory input)
        internal
        pure
        returns (SystemMessage_Credit memory)
    {
        uint256 new_pos;
        SystemMessage_Credit memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemMessage_Credit(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemMessage_Withdraw {
        AccountOwner owner;
        Amount amount;
        Account recipient;
    }

    function bcs_serialize_SystemMessage_Withdraw(SystemMessage_Withdraw memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, bcs_serialize_Account(input.recipient));
    }

    function bcs_deserialize_offset_SystemMessage_Withdraw(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemMessage_Withdraw memory)
    {
        uint256 new_pos;
        AccountOwner memory owner;
        (new_pos, owner) = bcs_deserialize_offset_AccountOwner(pos, input);
        Amount memory amount;
        (new_pos, amount) = bcs_deserialize_offset_Amount(new_pos, input);
        Account memory recipient;
        (new_pos, recipient) = bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, SystemMessage_Withdraw(owner, amount, recipient));
    }

    function bcs_deserialize_SystemMessage_Withdraw(bytes memory input)
        internal
        pure
        returns (SystemMessage_Withdraw memory)
    {
        uint256 new_pos;
        SystemMessage_Withdraw memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemMessage_Withdraw(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation {
        uint8 choice;
        // choice=0 corresponds to Transfer
        SystemOperation_Transfer transfer_;
        // choice=1 corresponds to Claim
        SystemOperation_Claim claim;
        // choice=2 corresponds to OpenChain
        OpenChainConfig open_chain;
        // choice=3 corresponds to CloseChain
        // choice=4 corresponds to ChangeOwnership
        SystemOperation_ChangeOwnership change_ownership;
        // choice=5 corresponds to ChangeApplicationPermissions
        ApplicationPermissions change_application_permissions;
        // choice=6 corresponds to PublishModule
        SystemOperation_PublishModule publish_module;
        // choice=7 corresponds to PublishDataBlob
        SystemOperation_PublishDataBlob publish_data_blob;
        // choice=8 corresponds to VerifyBlob
        SystemOperation_VerifyBlob verify_blob;
        // choice=9 corresponds to CreateApplication
        SystemOperation_CreateApplication create_application;
        // choice=10 corresponds to Admin
        AdminOperation admin;
        // choice=11 corresponds to ProcessNewEpoch
        Epoch process_new_epoch;
        // choice=12 corresponds to ProcessRemovedEpoch
        Epoch process_removed_epoch;
        // choice=13 corresponds to UpdateStreams
        tuple_ChainId_StreamId_uint32[] update_streams;
    }

    function SystemOperation_case_transfer(SystemOperation_Transfer memory transfer_)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(0), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_claim(SystemOperation_Claim memory claim)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(1), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_open_chain(OpenChainConfig memory open_chain)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(2), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_close_chain()
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(3), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_change_ownership(SystemOperation_ChangeOwnership memory change_ownership)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(4), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_change_application_permissions(ApplicationPermissions memory change_application_permissions)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(5), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_publish_module(SystemOperation_PublishModule memory publish_module)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(6), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_publish_data_blob(SystemOperation_PublishDataBlob memory publish_data_blob)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(7), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_verify_blob(SystemOperation_VerifyBlob memory verify_blob)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(8), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_create_application(SystemOperation_CreateApplication memory create_application)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(9), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_admin(AdminOperation memory admin)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(10), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_process_new_epoch(Epoch memory process_new_epoch)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_removed_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(11), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_process_removed_epoch(Epoch memory process_removed_epoch)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        return SystemOperation(uint8(12), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function SystemOperation_case_update_streams(tuple_ChainId_StreamId_uint32[] memory update_streams)
        internal
        pure
        returns (SystemOperation memory)
    {
        SystemOperation_Transfer memory transfer_;
        SystemOperation_Claim memory claim;
        OpenChainConfig memory open_chain;
        SystemOperation_ChangeOwnership memory change_ownership;
        ApplicationPermissions memory change_application_permissions;
        SystemOperation_PublishModule memory publish_module;
        SystemOperation_PublishDataBlob memory publish_data_blob;
        SystemOperation_VerifyBlob memory verify_blob;
        SystemOperation_CreateApplication memory create_application;
        AdminOperation memory admin;
        Epoch memory process_new_epoch;
        Epoch memory process_removed_epoch;
        return SystemOperation(uint8(13), transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams);
    }

    function bcs_serialize_SystemOperation(SystemOperation memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_Transfer(input.transfer_));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_Claim(input.claim));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_OpenChainConfig(input.open_chain));
        }
        if (input.choice == 4) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_ChangeOwnership(input.change_ownership));
        }
        if (input.choice == 5) {
            return abi.encodePacked(input.choice, bcs_serialize_ApplicationPermissions(input.change_application_permissions));
        }
        if (input.choice == 6) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_PublishModule(input.publish_module));
        }
        if (input.choice == 7) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_PublishDataBlob(input.publish_data_blob));
        }
        if (input.choice == 8) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_VerifyBlob(input.verify_blob));
        }
        if (input.choice == 9) {
            return abi.encodePacked(input.choice, bcs_serialize_SystemOperation_CreateApplication(input.create_application));
        }
        if (input.choice == 10) {
            return abi.encodePacked(input.choice, bcs_serialize_AdminOperation(input.admin));
        }
        if (input.choice == 11) {
            return abi.encodePacked(input.choice, bcs_serialize_Epoch(input.process_new_epoch));
        }
        if (input.choice == 12) {
            return abi.encodePacked(input.choice, bcs_serialize_Epoch(input.process_removed_epoch));
        }
        if (input.choice == 13) {
            return abi.encodePacked(input.choice, bcs_serialize_seq_tuple_ChainId_StreamId_uint32(input.update_streams));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_SystemOperation(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        SystemOperation_Transfer memory transfer_;
        if (choice == 0) {
            (new_pos, transfer_) = bcs_deserialize_offset_SystemOperation_Transfer(new_pos, input);
        }
        SystemOperation_Claim memory claim;
        if (choice == 1) {
            (new_pos, claim) = bcs_deserialize_offset_SystemOperation_Claim(new_pos, input);
        }
        OpenChainConfig memory open_chain;
        if (choice == 2) {
            (new_pos, open_chain) = bcs_deserialize_offset_OpenChainConfig(new_pos, input);
        }
        SystemOperation_ChangeOwnership memory change_ownership;
        if (choice == 4) {
            (new_pos, change_ownership) = bcs_deserialize_offset_SystemOperation_ChangeOwnership(new_pos, input);
        }
        ApplicationPermissions memory change_application_permissions;
        if (choice == 5) {
            (new_pos, change_application_permissions) = bcs_deserialize_offset_ApplicationPermissions(new_pos, input);
        }
        SystemOperation_PublishModule memory publish_module;
        if (choice == 6) {
            (new_pos, publish_module) = bcs_deserialize_offset_SystemOperation_PublishModule(new_pos, input);
        }
        SystemOperation_PublishDataBlob memory publish_data_blob;
        if (choice == 7) {
            (new_pos, publish_data_blob) = bcs_deserialize_offset_SystemOperation_PublishDataBlob(new_pos, input);
        }
        SystemOperation_VerifyBlob memory verify_blob;
        if (choice == 8) {
            (new_pos, verify_blob) = bcs_deserialize_offset_SystemOperation_VerifyBlob(new_pos, input);
        }
        SystemOperation_CreateApplication memory create_application;
        if (choice == 9) {
            (new_pos, create_application) = bcs_deserialize_offset_SystemOperation_CreateApplication(new_pos, input);
        }
        AdminOperation memory admin;
        if (choice == 10) {
            (new_pos, admin) = bcs_deserialize_offset_AdminOperation(new_pos, input);
        }
        Epoch memory process_new_epoch;
        if (choice == 11) {
            (new_pos, process_new_epoch) = bcs_deserialize_offset_Epoch(new_pos, input);
        }
        Epoch memory process_removed_epoch;
        if (choice == 12) {
            (new_pos, process_removed_epoch) = bcs_deserialize_offset_Epoch(new_pos, input);
        }
        tuple_ChainId_StreamId_uint32[] memory update_streams;
        if (choice == 13) {
            (new_pos, update_streams) = bcs_deserialize_offset_seq_tuple_ChainId_StreamId_uint32(new_pos, input);
        }
        require(choice < 14);
        return (new_pos, SystemOperation(choice, transfer_, claim, open_chain, change_ownership, change_application_permissions, publish_module, publish_data_blob, verify_blob, create_application, admin, process_new_epoch, process_removed_epoch, update_streams));
    }

    function bcs_deserialize_SystemOperation(bytes memory input)
        internal
        pure
        returns (SystemOperation memory)
    {
        uint256 new_pos;
        SystemOperation memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_ChangeOwnership {
        AccountOwner[] super_owners;
        tuple_AccountOwner_uint64[] owners;
        uint32 multi_leader_rounds;
        bool open_multi_leader_rounds;
        TimeoutConfig timeout_config;
    }

    function bcs_serialize_SystemOperation_ChangeOwnership(SystemOperation_ChangeOwnership memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_seq_AccountOwner(input.super_owners);
        result = abi.encodePacked(result, bcs_serialize_seq_tuple_AccountOwner_uint64(input.owners));
        result = abi.encodePacked(result, bcs_serialize_uint32(input.multi_leader_rounds));
        result = abi.encodePacked(result, bcs_serialize_bool(input.open_multi_leader_rounds));
        return abi.encodePacked(result, bcs_serialize_TimeoutConfig(input.timeout_config));
    }

    function bcs_deserialize_offset_SystemOperation_ChangeOwnership(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_ChangeOwnership memory)
    {
        uint256 new_pos;
        AccountOwner[] memory super_owners;
        (new_pos, super_owners) = bcs_deserialize_offset_seq_AccountOwner(pos, input);
        tuple_AccountOwner_uint64[] memory owners;
        (new_pos, owners) = bcs_deserialize_offset_seq_tuple_AccountOwner_uint64(new_pos, input);
        uint32 multi_leader_rounds;
        (new_pos, multi_leader_rounds) = bcs_deserialize_offset_uint32(new_pos, input);
        bool open_multi_leader_rounds;
        (new_pos, open_multi_leader_rounds) = bcs_deserialize_offset_bool(new_pos, input);
        TimeoutConfig memory timeout_config;
        (new_pos, timeout_config) = bcs_deserialize_offset_TimeoutConfig(new_pos, input);
        return (new_pos, SystemOperation_ChangeOwnership(super_owners, owners, multi_leader_rounds, open_multi_leader_rounds, timeout_config));
    }

    function bcs_deserialize_SystemOperation_ChangeOwnership(bytes memory input)
        internal
        pure
        returns (SystemOperation_ChangeOwnership memory)
    {
        uint256 new_pos;
        SystemOperation_ChangeOwnership memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_ChangeOwnership(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_Claim {
        AccountOwner owner;
        ChainId target_id;
        Account recipient;
        Amount amount;
    }

    function bcs_serialize_SystemOperation_Claim(SystemOperation_Claim memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, bcs_serialize_ChainId(input.target_id));
        result = abi.encodePacked(result, bcs_serialize_Account(input.recipient));
        return abi.encodePacked(result, bcs_serialize_Amount(input.amount));
    }

    function bcs_deserialize_offset_SystemOperation_Claim(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_Claim memory)
    {
        uint256 new_pos;
        AccountOwner memory owner;
        (new_pos, owner) = bcs_deserialize_offset_AccountOwner(pos, input);
        ChainId memory target_id;
        (new_pos, target_id) = bcs_deserialize_offset_ChainId(new_pos, input);
        Account memory recipient;
        (new_pos, recipient) = bcs_deserialize_offset_Account(new_pos, input);
        Amount memory amount;
        (new_pos, amount) = bcs_deserialize_offset_Amount(new_pos, input);
        return (new_pos, SystemOperation_Claim(owner, target_id, recipient, amount));
    }

    function bcs_deserialize_SystemOperation_Claim(bytes memory input)
        internal
        pure
        returns (SystemOperation_Claim memory)
    {
        uint256 new_pos;
        SystemOperation_Claim memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_Claim(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_CreateApplication {
        ModuleId module_id;
        bytes parameters;
        bytes instantiation_argument;
        ApplicationId[] required_application_ids;
    }

    function bcs_serialize_SystemOperation_CreateApplication(SystemOperation_CreateApplication memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ModuleId(input.module_id);
        result = abi.encodePacked(result, bcs_serialize_bytes(input.parameters));
        result = abi.encodePacked(result, bcs_serialize_bytes(input.instantiation_argument));
        return abi.encodePacked(result, bcs_serialize_seq_ApplicationId(input.required_application_ids));
    }

    function bcs_deserialize_offset_SystemOperation_CreateApplication(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_CreateApplication memory)
    {
        uint256 new_pos;
        ModuleId memory module_id;
        (new_pos, module_id) = bcs_deserialize_offset_ModuleId(pos, input);
        bytes memory parameters;
        (new_pos, parameters) = bcs_deserialize_offset_bytes(new_pos, input);
        bytes memory instantiation_argument;
        (new_pos, instantiation_argument) = bcs_deserialize_offset_bytes(new_pos, input);
        ApplicationId[] memory required_application_ids;
        (new_pos, required_application_ids) = bcs_deserialize_offset_seq_ApplicationId(new_pos, input);
        return (new_pos, SystemOperation_CreateApplication(module_id, parameters, instantiation_argument, required_application_ids));
    }

    function bcs_deserialize_SystemOperation_CreateApplication(bytes memory input)
        internal
        pure
        returns (SystemOperation_CreateApplication memory)
    {
        uint256 new_pos;
        SystemOperation_CreateApplication memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_CreateApplication(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_PublishDataBlob {
        CryptoHash blob_hash;
    }

    function bcs_serialize_SystemOperation_PublishDataBlob(SystemOperation_PublishDataBlob memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_CryptoHash(input.blob_hash);
    }

    function bcs_deserialize_offset_SystemOperation_PublishDataBlob(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_PublishDataBlob memory)
    {
        uint256 new_pos;
        CryptoHash memory blob_hash;
        (new_pos, blob_hash) = bcs_deserialize_offset_CryptoHash(pos, input);
        return (new_pos, SystemOperation_PublishDataBlob(blob_hash));
    }

    function bcs_deserialize_SystemOperation_PublishDataBlob(bytes memory input)
        internal
        pure
        returns (SystemOperation_PublishDataBlob memory)
    {
        uint256 new_pos;
        SystemOperation_PublishDataBlob memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_PublishDataBlob(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_PublishModule {
        ModuleId module_id;
    }

    function bcs_serialize_SystemOperation_PublishModule(SystemOperation_PublishModule memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_ModuleId(input.module_id);
    }

    function bcs_deserialize_offset_SystemOperation_PublishModule(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_PublishModule memory)
    {
        uint256 new_pos;
        ModuleId memory module_id;
        (new_pos, module_id) = bcs_deserialize_offset_ModuleId(pos, input);
        return (new_pos, SystemOperation_PublishModule(module_id));
    }

    function bcs_deserialize_SystemOperation_PublishModule(bytes memory input)
        internal
        pure
        returns (SystemOperation_PublishModule memory)
    {
        uint256 new_pos;
        SystemOperation_PublishModule memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_PublishModule(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_Transfer {
        AccountOwner owner;
        Account recipient;
        Amount amount;
    }

    function bcs_serialize_SystemOperation_Transfer(SystemOperation_Transfer memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, bcs_serialize_Account(input.recipient));
        return abi.encodePacked(result, bcs_serialize_Amount(input.amount));
    }

    function bcs_deserialize_offset_SystemOperation_Transfer(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_Transfer memory)
    {
        uint256 new_pos;
        AccountOwner memory owner;
        (new_pos, owner) = bcs_deserialize_offset_AccountOwner(pos, input);
        Account memory recipient;
        (new_pos, recipient) = bcs_deserialize_offset_Account(new_pos, input);
        Amount memory amount;
        (new_pos, amount) = bcs_deserialize_offset_Amount(new_pos, input);
        return (new_pos, SystemOperation_Transfer(owner, recipient, amount));
    }

    function bcs_deserialize_SystemOperation_Transfer(bytes memory input)
        internal
        pure
        returns (SystemOperation_Transfer memory)
    {
        uint256 new_pos;
        SystemOperation_Transfer memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_Transfer(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct SystemOperation_VerifyBlob {
        BlobId blob_id;
    }

    function bcs_serialize_SystemOperation_VerifyBlob(SystemOperation_VerifyBlob memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_BlobId(input.blob_id);
    }

    function bcs_deserialize_offset_SystemOperation_VerifyBlob(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, SystemOperation_VerifyBlob memory)
    {
        uint256 new_pos;
        BlobId memory blob_id;
        (new_pos, blob_id) = bcs_deserialize_offset_BlobId(pos, input);
        return (new_pos, SystemOperation_VerifyBlob(blob_id));
    }

    function bcs_deserialize_SystemOperation_VerifyBlob(bytes memory input)
        internal
        pure
        returns (SystemOperation_VerifyBlob memory)
    {
        uint256 new_pos;
        SystemOperation_VerifyBlob memory value;
        (new_pos, value) = bcs_deserialize_offset_SystemOperation_VerifyBlob(0, input);
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

    struct Transaction {
        uint8 choice;
        // choice=0 corresponds to ReceiveMessages
        IncomingBundle receive_messages;
        // choice=1 corresponds to ExecuteOperation
        Operation execute_operation;
    }

    function Transaction_case_receive_messages(IncomingBundle memory receive_messages)
        internal
        pure
        returns (Transaction memory)
    {
        Operation memory execute_operation;
        return Transaction(uint8(0), receive_messages, execute_operation);
    }

    function Transaction_case_execute_operation(Operation memory execute_operation)
        internal
        pure
        returns (Transaction memory)
    {
        IncomingBundle memory receive_messages;
        return Transaction(uint8(1), receive_messages, execute_operation);
    }

    function bcs_serialize_Transaction(Transaction memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_IncomingBundle(input.receive_messages));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_Operation(input.execute_operation));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_Transaction(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Transaction memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        IncomingBundle memory receive_messages;
        if (choice == 0) {
            (new_pos, receive_messages) = bcs_deserialize_offset_IncomingBundle(new_pos, input);
        }
        Operation memory execute_operation;
        if (choice == 1) {
            (new_pos, execute_operation) = bcs_deserialize_offset_Operation(new_pos, input);
        }
        require(choice < 2);
        return (new_pos, Transaction(choice, receive_messages, execute_operation));
    }

    function bcs_deserialize_Transaction(bytes memory input)
        internal
        pure
        returns (Transaction memory)
    {
        uint256 new_pos;
        Transaction memory value;
        (new_pos, value) = bcs_deserialize_offset_Transaction(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum VmRuntime { Wasm, Evm }

    function bcs_serialize_VmRuntime(VmRuntime input)
        internal
        pure
        returns (bytes memory)
    {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_VmRuntime(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, VmRuntime)
    {
        uint8 choice = uint8(input[pos]);

        if (choice == 0) {
            return (pos + 1, VmRuntime.Wasm);
        }

        if (choice == 1) {
            return (pos + 1, VmRuntime.Evm);
        }

        require(choice < 2);
    }

    function bcs_deserialize_VmRuntime(bytes memory input)
        internal
        pure
        returns (VmRuntime)
    {
        uint256 new_pos;
        VmRuntime value;
        (new_pos, value) = bcs_deserialize_offset_VmRuntime(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct VoteValue {
        CryptoHash entry0;
        Round entry1;
        CertificateKind entry2;
    }

    function bcs_serialize_VoteValue(VoteValue memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_CryptoHash(input.entry0);
        result = abi.encodePacked(result, bcs_serialize_Round(input.entry1));
        return abi.encodePacked(result, bcs_serialize_CertificateKind(input.entry2));
    }

    function bcs_deserialize_offset_VoteValue(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, VoteValue memory)
    {
        uint256 new_pos;
        CryptoHash memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_CryptoHash(pos, input);
        Round memory entry1;
        (new_pos, entry1) = bcs_deserialize_offset_Round(new_pos, input);
        CertificateKind entry2;
        (new_pos, entry2) = bcs_deserialize_offset_CertificateKind(new_pos, input);
        return (new_pos, VoteValue(entry0, entry1, entry2));
    }

    function bcs_deserialize_VoteValue(bytes memory input)
        internal
        pure
        returns (VoteValue memory)
    {
        uint256 new_pos;
        VoteValue memory value;
        (new_pos, value) = bcs_deserialize_offset_VoteValue(0, input);
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

    struct key_values_ChainId_tuple_CryptoHash_BlockHeight {
        ChainId key;
        tuple_CryptoHash_BlockHeight value;
    }

    function bcs_serialize_key_values_ChainId_tuple_CryptoHash_BlockHeight(key_values_ChainId_tuple_CryptoHash_BlockHeight memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.key);
        return abi.encodePacked(result, bcs_serialize_tuple_CryptoHash_BlockHeight(input.value));
    }

    function bcs_deserialize_offset_key_values_ChainId_tuple_CryptoHash_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_ChainId_tuple_CryptoHash_BlockHeight memory)
    {
        uint256 new_pos;
        ChainId memory key;
        (new_pos, key) = bcs_deserialize_offset_ChainId(pos, input);
        tuple_CryptoHash_BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_CryptoHash_BlockHeight(new_pos, input);
        return (new_pos, key_values_ChainId_tuple_CryptoHash_BlockHeight(key, value));
    }

    function bcs_deserialize_key_values_ChainId_tuple_CryptoHash_BlockHeight(bytes memory input)
        internal
        pure
        returns (key_values_ChainId_tuple_CryptoHash_BlockHeight memory)
    {
        uint256 new_pos;
        key_values_ChainId_tuple_CryptoHash_BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_key_values_ChainId_tuple_CryptoHash_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct key_values_StreamId_tuple_CryptoHash_BlockHeight {
        StreamId key;
        tuple_CryptoHash_BlockHeight value;
    }

    function bcs_serialize_key_values_StreamId_tuple_CryptoHash_BlockHeight(key_values_StreamId_tuple_CryptoHash_BlockHeight memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_StreamId(input.key);
        return abi.encodePacked(result, bcs_serialize_tuple_CryptoHash_BlockHeight(input.value));
    }

    function bcs_deserialize_offset_key_values_StreamId_tuple_CryptoHash_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_StreamId_tuple_CryptoHash_BlockHeight memory)
    {
        uint256 new_pos;
        StreamId memory key;
        (new_pos, key) = bcs_deserialize_offset_StreamId(pos, input);
        tuple_CryptoHash_BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_CryptoHash_BlockHeight(new_pos, input);
        return (new_pos, key_values_StreamId_tuple_CryptoHash_BlockHeight(key, value));
    }

    function bcs_deserialize_key_values_StreamId_tuple_CryptoHash_BlockHeight(bytes memory input)
        internal
        pure
        returns (key_values_StreamId_tuple_CryptoHash_BlockHeight memory)
    {
        uint256 new_pos;
        key_values_StreamId_tuple_CryptoHash_BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_key_values_StreamId_tuple_CryptoHash_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct opt_Account {
        bool has_value;
        Account value;
    }

    function bcs_serialize_opt_Account(opt_Account memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_Account(input.value));
        } else {
            return abi.encodePacked(uint8(0));
        }
    }

    function bcs_deserialize_offset_opt_Account(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_Account memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        Account memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_Account(new_pos, input);
        }
        return (new_pos, opt_Account(has_value, value));
    }

    function bcs_deserialize_opt_Account(bytes memory input)
        internal
        pure
        returns (opt_Account memory)
    {
        uint256 new_pos;
        opt_Account memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_Account(0, input);
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

    struct opt_CryptoHash {
        bool has_value;
        CryptoHash value;
    }

    function bcs_serialize_opt_CryptoHash(opt_CryptoHash memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_CryptoHash(input.value));
        } else {
            return abi.encodePacked(uint8(0));
        }
    }

    function bcs_deserialize_offset_opt_CryptoHash(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_CryptoHash memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        CryptoHash memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        }
        return (new_pos, opt_CryptoHash(has_value, value));
    }

    function bcs_deserialize_opt_CryptoHash(bytes memory input)
        internal
        pure
        returns (opt_CryptoHash memory)
    {
        uint256 new_pos;
        opt_CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_CryptoHash(0, input);
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

    struct opt_seq_ApplicationId {
        bool has_value;
        ApplicationId[] value;
    }

    function bcs_serialize_opt_seq_ApplicationId(opt_seq_ApplicationId memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.has_value) {
            return abi.encodePacked(uint8(1), bcs_serialize_seq_ApplicationId(input.value));
        } else {
            return abi.encodePacked(uint8(0));
        }
    }

    function bcs_deserialize_offset_opt_seq_ApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, opt_seq_ApplicationId memory)
    {
        uint256 new_pos;
        bool has_value;
        (new_pos, has_value) = bcs_deserialize_offset_bool(pos, input);
        ApplicationId[] memory value;
        if (has_value) {
            (new_pos, value) = bcs_deserialize_offset_seq_ApplicationId(new_pos, input);
        }
        return (new_pos, opt_seq_ApplicationId(has_value, value));
    }

    function bcs_deserialize_opt_seq_ApplicationId(bytes memory input)
        internal
        pure
        returns (opt_seq_ApplicationId memory)
    {
        uint256 new_pos;
        opt_seq_ApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_seq_ApplicationId(0, input);
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

    function bcs_serialize_seq_ApplicationId(ApplicationId[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_ApplicationId(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_ApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ApplicationId[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        ApplicationId[] memory result;
        result = new ApplicationId[](len);
        ApplicationId memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_ApplicationId(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_ApplicationId(bytes memory input)
        internal
        pure
        returns (ApplicationId[] memory)
    {
        uint256 new_pos;
        ApplicationId[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_ApplicationId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_BlobContent(BlobContent[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_BlobContent(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_BlobContent(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobContent[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        BlobContent[] memory result;
        result = new BlobContent[](len);
        BlobContent memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_BlobContent(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_BlobContent(bytes memory input)
        internal
        pure
        returns (BlobContent[] memory)
    {
        uint256 new_pos;
        BlobContent[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_BlobContent(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_Event(Event[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_Event(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_Event(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Event[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        Event[] memory result;
        result = new Event[](len);
        Event memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_Event(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_Event(bytes memory input)
        internal
        pure
        returns (Event[] memory)
    {
        uint256 new_pos;
        Event[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_Event(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_Header(Header[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_Header(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_Header(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Header[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        Header[] memory result;
        result = new Header[](len);
        Header memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_Header(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_Header(bytes memory input)
        internal
        pure
        returns (Header[] memory)
    {
        uint256 new_pos;
        Header[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_Header(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_OperationResult(OperationResult[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_OperationResult(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_OperationResult(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OperationResult[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        OperationResult[] memory result;
        result = new OperationResult[](len);
        OperationResult memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_OperationResult(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_OperationResult(bytes memory input)
        internal
        pure
        returns (OperationResult[] memory)
    {
        uint256 new_pos;
        OperationResult[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_OperationResult(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_OracleResponse(OracleResponse[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_OracleResponse(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_OracleResponse(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OracleResponse[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        OracleResponse[] memory result;
        result = new OracleResponse[](len);
        OracleResponse memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_OracleResponse(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_OracleResponse(bytes memory input)
        internal
        pure
        returns (OracleResponse[] memory)
    {
        uint256 new_pos;
        OracleResponse[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_OracleResponse(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_OutgoingMessage(OutgoingMessage[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_OutgoingMessage(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_OutgoingMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OutgoingMessage[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        OutgoingMessage[] memory result;
        result = new OutgoingMessage[](len);
        OutgoingMessage memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_OutgoingMessage(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_OutgoingMessage(bytes memory input)
        internal
        pure
        returns (OutgoingMessage[] memory)
    {
        uint256 new_pos;
        OutgoingMessage[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_OutgoingMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_PostedMessage(PostedMessage[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_PostedMessage(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_PostedMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, PostedMessage[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        PostedMessage[] memory result;
        result = new PostedMessage[](len);
        PostedMessage memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_PostedMessage(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_PostedMessage(bytes memory input)
        internal
        pure
        returns (PostedMessage[] memory)
    {
        uint256 new_pos;
        PostedMessage[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_PostedMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_Transaction(Transaction[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_Transaction(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_Transaction(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Transaction[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        Transaction[] memory result;
        result = new Transaction[](len);
        Transaction memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_Transaction(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_Transaction(bytes memory input)
        internal
        pure
        returns (Transaction[] memory)
    {
        uint256 new_pos;
        Transaction[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_Transaction(0, input);
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

    function bcs_serialize_seq_key_values_ChainId_tuple_CryptoHash_BlockHeight(key_values_ChainId_tuple_CryptoHash_BlockHeight[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_key_values_ChainId_tuple_CryptoHash_BlockHeight(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_key_values_ChainId_tuple_CryptoHash_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_ChainId_tuple_CryptoHash_BlockHeight[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        key_values_ChainId_tuple_CryptoHash_BlockHeight[] memory result;
        result = new key_values_ChainId_tuple_CryptoHash_BlockHeight[](len);
        key_values_ChainId_tuple_CryptoHash_BlockHeight memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_key_values_ChainId_tuple_CryptoHash_BlockHeight(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_key_values_ChainId_tuple_CryptoHash_BlockHeight(bytes memory input)
        internal
        pure
        returns (key_values_ChainId_tuple_CryptoHash_BlockHeight[] memory)
    {
        uint256 new_pos;
        key_values_ChainId_tuple_CryptoHash_BlockHeight[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_key_values_ChainId_tuple_CryptoHash_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_key_values_StreamId_tuple_CryptoHash_BlockHeight(key_values_StreamId_tuple_CryptoHash_BlockHeight[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_key_values_StreamId_tuple_CryptoHash_BlockHeight(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_key_values_StreamId_tuple_CryptoHash_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, key_values_StreamId_tuple_CryptoHash_BlockHeight[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        key_values_StreamId_tuple_CryptoHash_BlockHeight[] memory result;
        result = new key_values_StreamId_tuple_CryptoHash_BlockHeight[](len);
        key_values_StreamId_tuple_CryptoHash_BlockHeight memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_key_values_StreamId_tuple_CryptoHash_BlockHeight(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_key_values_StreamId_tuple_CryptoHash_BlockHeight(bytes memory input)
        internal
        pure
        returns (key_values_StreamId_tuple_CryptoHash_BlockHeight[] memory)
    {
        uint256 new_pos;
        key_values_StreamId_tuple_CryptoHash_BlockHeight[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_key_values_StreamId_tuple_CryptoHash_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_seq_BlobContent(BlobContent[][] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_seq_BlobContent(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_seq_BlobContent(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobContent[][] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        BlobContent[][] memory result;
        result = new BlobContent[][](len);
        BlobContent[] memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_seq_BlobContent(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_seq_BlobContent(bytes memory input)
        internal
        pure
        returns (BlobContent[][] memory)
    {
        uint256 new_pos;
        BlobContent[][] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_seq_BlobContent(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_seq_Event(Event[][] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_seq_Event(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_seq_Event(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Event[][] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        Event[][] memory result;
        result = new Event[][](len);
        Event[] memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_seq_Event(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_seq_Event(bytes memory input)
        internal
        pure
        returns (Event[][] memory)
    {
        uint256 new_pos;
        Event[][] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_seq_Event(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_seq_OracleResponse(OracleResponse[][] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_seq_OracleResponse(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_seq_OracleResponse(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OracleResponse[][] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        OracleResponse[][] memory result;
        result = new OracleResponse[][](len);
        OracleResponse[] memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_seq_OracleResponse(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_seq_OracleResponse(bytes memory input)
        internal
        pure
        returns (OracleResponse[][] memory)
    {
        uint256 new_pos;
        OracleResponse[][] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_seq_OracleResponse(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_seq_OutgoingMessage(OutgoingMessage[][] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_seq_OutgoingMessage(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_seq_OutgoingMessage(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OutgoingMessage[][] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        OutgoingMessage[][] memory result;
        result = new OutgoingMessage[][](len);
        OutgoingMessage[] memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_seq_OutgoingMessage(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_seq_OutgoingMessage(bytes memory input)
        internal
        pure
        returns (OutgoingMessage[][] memory)
    {
        uint256 new_pos;
        OutgoingMessage[][] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_seq_OutgoingMessage(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_tuple_AccountOwner_uint64(tuple_AccountOwner_uint64[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_tuple_AccountOwner_uint64(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_tuple_AccountOwner_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_AccountOwner_uint64[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        tuple_AccountOwner_uint64[] memory result;
        result = new tuple_AccountOwner_uint64[](len);
        tuple_AccountOwner_uint64 memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_tuple_AccountOwner_uint64(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_tuple_AccountOwner_uint64(bytes memory input)
        internal
        pure
        returns (tuple_AccountOwner_uint64[] memory)
    {
        uint256 new_pos;
        tuple_AccountOwner_uint64[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_tuple_AccountOwner_uint64(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_tuple_ChainId_StreamId_uint32(tuple_ChainId_StreamId_uint32[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_tuple_ChainId_StreamId_uint32(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_tuple_ChainId_StreamId_uint32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_ChainId_StreamId_uint32[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        tuple_ChainId_StreamId_uint32[] memory result;
        result = new tuple_ChainId_StreamId_uint32[](len);
        tuple_ChainId_StreamId_uint32 memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_tuple_ChainId_StreamId_uint32(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_tuple_ChainId_StreamId_uint32(bytes memory input)
        internal
        pure
        returns (tuple_ChainId_StreamId_uint32[] memory)
    {
        uint256 new_pos;
        tuple_ChainId_StreamId_uint32[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_tuple_ChainId_StreamId_uint32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(tuple_Secp256k1PublicKey_Secp256k1Signature[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_len(len);
        for (uint256 i=0; i<len; i++) {
            result = abi.encodePacked(result, bcs_serialize_tuple_Secp256k1PublicKey_Secp256k1Signature(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_Secp256k1PublicKey_Secp256k1Signature[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        tuple_Secp256k1PublicKey_Secp256k1Signature[] memory result;
        result = new tuple_Secp256k1PublicKey_Secp256k1Signature[](len);
        tuple_Secp256k1PublicKey_Secp256k1Signature memory value;
        for (uint256 i=0; i<len; i++) {
            (new_pos, value) = bcs_deserialize_offset_tuple_Secp256k1PublicKey_Secp256k1Signature(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(bytes memory input)
        internal
        pure
        returns (tuple_Secp256k1PublicKey_Secp256k1Signature[] memory)
    {
        uint256 new_pos;
        tuple_Secp256k1PublicKey_Secp256k1Signature[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_string(string memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory input_bytes = bytes(input);
        uint256 number_bytes = input_bytes.length;
        uint256 number_char = 0;
        uint256 pos = 0;
        while (true) {
            if (uint8(input_bytes[pos]) < 128) {
                number_char += 1;
            }
            pos += 1;
            if (pos == number_bytes) {
                break;
            }
        }
        bytes memory result_len = bcs_serialize_len(number_char);
        return abi.encodePacked(result_len, input);
    }

    function bcs_deserialize_offset_string(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, string memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_len(pos, input);
        uint256 shift = 0;
        for (uint256 i=0; i<len; i++) {
            while (true) {
                bytes1 val = input[new_pos + shift];
                shift += 1;
                if (uint8(val) < 128) {
                    break;
                }
            }
        }
        bytes memory result_bytes = new bytes(shift);
        for (uint256 i=0; i<shift; i++) {
            result_bytes[i] = input[new_pos + i];
        }
        string memory result = string(result_bytes);
        return (new_pos + shift, result);
    }


    function bcs_deserialize_string(bytes memory input)
        internal
        pure
        returns (string memory)
    {
        uint256 new_pos;
        string memory value;
        (new_pos, value) = bcs_deserialize_offset_string(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuple_AccountOwner_uint64 {
        AccountOwner entry0;
        uint64 entry1;
    }

    function bcs_serialize_tuple_AccountOwner_uint64(tuple_AccountOwner_uint64 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_AccountOwner(input.entry0);
        return abi.encodePacked(result, bcs_serialize_uint64(input.entry1));
    }

    function bcs_deserialize_offset_tuple_AccountOwner_uint64(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_AccountOwner_uint64 memory)
    {
        uint256 new_pos;
        AccountOwner memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_AccountOwner(pos, input);
        uint64 entry1;
        (new_pos, entry1) = bcs_deserialize_offset_uint64(new_pos, input);
        return (new_pos, tuple_AccountOwner_uint64(entry0, entry1));
    }

    function bcs_deserialize_tuple_AccountOwner_uint64(bytes memory input)
        internal
        pure
        returns (tuple_AccountOwner_uint64 memory)
    {
        uint256 new_pos;
        tuple_AccountOwner_uint64 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_AccountOwner_uint64(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuple_ChainId_StreamId_uint32 {
        ChainId entry0;
        StreamId entry1;
        uint32 entry2;
    }

    function bcs_serialize_tuple_ChainId_StreamId_uint32(tuple_ChainId_StreamId_uint32 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.entry0);
        result = abi.encodePacked(result, bcs_serialize_StreamId(input.entry1));
        return abi.encodePacked(result, bcs_serialize_uint32(input.entry2));
    }

    function bcs_deserialize_offset_tuple_ChainId_StreamId_uint32(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_ChainId_StreamId_uint32 memory)
    {
        uint256 new_pos;
        ChainId memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_ChainId(pos, input);
        StreamId memory entry1;
        (new_pos, entry1) = bcs_deserialize_offset_StreamId(new_pos, input);
        uint32 entry2;
        (new_pos, entry2) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, tuple_ChainId_StreamId_uint32(entry0, entry1, entry2));
    }

    function bcs_deserialize_tuple_ChainId_StreamId_uint32(bytes memory input)
        internal
        pure
        returns (tuple_ChainId_StreamId_uint32 memory)
    {
        uint256 new_pos;
        tuple_ChainId_StreamId_uint32 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_ChainId_StreamId_uint32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuple_CryptoHash_BlockHeight {
        CryptoHash entry0;
        BlockHeight entry1;
    }

    function bcs_serialize_tuple_CryptoHash_BlockHeight(tuple_CryptoHash_BlockHeight memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_CryptoHash(input.entry0);
        return abi.encodePacked(result, bcs_serialize_BlockHeight(input.entry1));
    }

    function bcs_deserialize_offset_tuple_CryptoHash_BlockHeight(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_CryptoHash_BlockHeight memory)
    {
        uint256 new_pos;
        CryptoHash memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_CryptoHash(pos, input);
        BlockHeight memory entry1;
        (new_pos, entry1) = bcs_deserialize_offset_BlockHeight(new_pos, input);
        return (new_pos, tuple_CryptoHash_BlockHeight(entry0, entry1));
    }

    function bcs_deserialize_tuple_CryptoHash_BlockHeight(bytes memory input)
        internal
        pure
        returns (tuple_CryptoHash_BlockHeight memory)
    {
        uint256 new_pos;
        tuple_CryptoHash_BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_CryptoHash_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuple_Secp256k1PublicKey_Secp256k1Signature {
        Secp256k1PublicKey entry0;
        Secp256k1Signature entry1;
    }

    function bcs_serialize_tuple_Secp256k1PublicKey_Secp256k1Signature(tuple_Secp256k1PublicKey_Secp256k1Signature memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_Secp256k1PublicKey(input.entry0);
        return abi.encodePacked(result, bcs_serialize_Secp256k1Signature(input.entry1));
    }

    function bcs_deserialize_offset_tuple_Secp256k1PublicKey_Secp256k1Signature(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_Secp256k1PublicKey_Secp256k1Signature memory)
    {
        uint256 new_pos;
        Secp256k1PublicKey memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_Secp256k1PublicKey(pos, input);
        Secp256k1Signature memory entry1;
        (new_pos, entry1) = bcs_deserialize_offset_Secp256k1Signature(new_pos, input);
        return (new_pos, tuple_Secp256k1PublicKey_Secp256k1Signature(entry0, entry1));
    }

    function bcs_deserialize_tuple_Secp256k1PublicKey_Secp256k1Signature(bytes memory input)
        internal
        pure
        returns (tuple_Secp256k1PublicKey_Secp256k1Signature memory)
    {
        uint256 new_pos;
        tuple_Secp256k1PublicKey_Secp256k1Signature memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_Secp256k1PublicKey_Secp256k1Signature(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuplearray33_uint8 {
        uint8[] values;
    }

    function bcs_serialize_tuplearray33_uint8(tuplearray33_uint8 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result;
        for (uint i=0; i<33; i++) {
            result = abi.encodePacked(result, bcs_serialize_uint8(input.values[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_tuplearray33_uint8(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuplearray33_uint8 memory)
    {
        uint256 new_pos = pos;
        uint8 value;
        uint8[] memory values;
        values = new uint8[](33);
        for (uint i=0; i<33; i++) {
            (new_pos, value) = bcs_deserialize_offset_uint8(new_pos, input);
            values[i] = value;
        }
        return (new_pos, tuplearray33_uint8(values));
    }

    function bcs_deserialize_tuplearray33_uint8(bytes memory input)
        internal
        pure
        returns (tuplearray33_uint8 memory)
    {
        uint256 new_pos;
        tuplearray33_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray33_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuplearray64_uint8 {
        uint8[] values;
    }

    function bcs_serialize_tuplearray64_uint8(tuplearray64_uint8 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result;
        for (uint i=0; i<64; i++) {
            result = abi.encodePacked(result, bcs_serialize_uint8(input.values[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_tuplearray64_uint8(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuplearray64_uint8 memory)
    {
        uint256 new_pos = pos;
        uint8 value;
        uint8[] memory values;
        values = new uint8[](64);
        for (uint i=0; i<64; i++) {
            (new_pos, value) = bcs_deserialize_offset_uint8(new_pos, input);
            values[i] = value;
        }
        return (new_pos, tuplearray64_uint8(values));
    }

    function bcs_deserialize_tuplearray64_uint8(bytes memory input)
        internal
        pure
        returns (tuplearray64_uint8 memory)
    {
        uint256 new_pos;
        tuplearray64_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray64_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuplearray65_uint8 {
        uint8[] values;
    }

    function bcs_serialize_tuplearray65_uint8(tuplearray65_uint8 memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result;
        for (uint i=0; i<65; i++) {
            result = abi.encodePacked(result, bcs_serialize_uint8(input.values[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_tuplearray65_uint8(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuplearray65_uint8 memory)
    {
        uint256 new_pos = pos;
        uint8 value;
        uint8[] memory values;
        values = new uint8[](65);
        for (uint i=0; i<65; i++) {
            (new_pos, value) = bcs_deserialize_offset_uint8(new_pos, input);
            values[i] = value;
        }
        return (new_pos, tuplearray65_uint8(values));
    }

    function bcs_deserialize_tuplearray65_uint8(bytes memory input)
        internal
        pure
        returns (tuplearray65_uint8 memory)
    {
        uint256 new_pos;
        tuplearray65_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray65_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint128(uint128 input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = new bytes(16);
        uint128 value = input;
        result[0] = bytes1(uint8(value));
        for (uint i=1; i<16; i++) {
            value = value >> 8;
            result[i] = bytes1(uint8(value));
        }
        return result;
    }

    function bcs_deserialize_offset_uint128(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint128)
    {
        uint128 value = uint8(input[pos + 15]);
        for (uint256 i=0; i<15; i++) {
            value = value << 8;
            value += uint8(input[pos + 14 - i]);
        }
        return (pos + 16, value);
    }

    function bcs_deserialize_uint128(bytes memory input)
        internal
        pure
        returns (uint128)
    {
        uint256 new_pos;
        uint128 value;
        (new_pos, value) = bcs_deserialize_offset_uint128(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint16(uint16 input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = new bytes(2);
        uint16 value = input;
        result[0] = bytes1(uint8(value));
        value = value >> 8;
        result[1] = bytes1(uint8(value));
        return result;
    }

    function bcs_deserialize_offset_uint16(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint16)
    {
        uint16 value = uint8(input[pos+1]);
        value = value << 8;
        value += uint8(input[pos]);
        return (pos + 2, value);
    }

    function bcs_deserialize_uint16(bytes memory input)
        internal
        pure
        returns (uint16)
    {
        uint256 new_pos;
        uint16 value;
        (new_pos, value) = bcs_deserialize_offset_uint16(0, input);
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

} // end of library BridgeTypes
