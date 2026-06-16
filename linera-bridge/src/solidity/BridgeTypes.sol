/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;

library BridgeTypes {
    function bcs_serialize_uleb128(uint256 x) internal pure returns (bytes memory) {
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

    function bcs_deserialize_offset_uleb128(uint256 pos, bytes memory input) internal pure returns (uint256, uint256) {
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
                return (pos + idx + 1, result);
            }
            idx += 1;
        }
        require(false, "This line is unreachable");
        return (0, 0);
    }

    struct AccountOwner {
        uint64 choice;
        // choice=0 corresponds to Reserved
        uint8 reserved;
        // choice=1 corresponds to Address32
        CryptoHash address32;
        // choice=2 corresponds to Address20
        bytes20 address20;
    }

    function AccountOwner_case_reserved(uint8 reserved) internal pure returns (AccountOwner memory) {
        CryptoHash memory address32;
        bytes20 address20;
        return AccountOwner(uint64(0), reserved, address32, address20);
    }

    function AccountOwner_case_address32(CryptoHash memory address32) internal pure returns (AccountOwner memory) {
        uint8 reserved;
        bytes20 address20;
        return AccountOwner(uint64(1), reserved, address32, address20);
    }

    function AccountOwner_case_address20(bytes20 address20) internal pure returns (AccountOwner memory) {
        uint8 reserved;
        CryptoHash memory address32;
        return AccountOwner(uint64(2), reserved, address32, address20);
    }

    function bcs_serialize_AccountOwner(AccountOwner memory input) internal pure returns (bytes memory) {
        if (input.choice == 0) {
            return abi.encodePacked(hex"00", bcs_serialize_uint8(input.reserved));
        }
        if (input.choice == 1) {
            return abi.encodePacked(hex"01", bcs_serialize_CryptoHash(input.address32));
        }
        if (input.choice == 2) {
            return abi.encodePacked(hex"02", bcs_serialize_bytes20(input.address20));
        }
        revert("invalid variant index");
    }

    function bcs_deserialize_offset_AccountOwner(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, AccountOwner memory)
    {
        uint256 new_pos;
        uint256 choice_raw;
        (new_pos, choice_raw) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice_raw <= type(uint64).max, "variant index does not fit in uint64");
        uint64 choice = uint64(choice_raw);
        require(choice < 3, "invalid variant index");
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

    function bcs_deserialize_AccountOwner(bytes memory input) internal pure returns (AccountOwner memory) {
        uint256 new_pos;
        AccountOwner memory value;
        (new_pos, value) = bcs_deserialize_offset_AccountOwner(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ApplicationId {
        CryptoHash application_description_hash;
    }

    function bcs_serialize_ApplicationId(ApplicationId memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_ApplicationId(bytes memory input) internal pure returns (ApplicationId memory) {
        uint256 new_pos;
        ApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_ApplicationId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlobContent {
        BlobType blob_type;
        bytes bytes_;
    }

    function bcs_serialize_BlobContent(BlobContent memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_BlobContent(bytes memory input) internal pure returns (BlobContent memory) {
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

    function bcs_serialize_BlobId(BlobId memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_BlobId(bytes memory input) internal pure returns (BlobId memory) {
        uint256 new_pos;
        BlobId memory value;
        (new_pos, value) = bcs_deserialize_offset_BlobId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum BlobType {
        Data,
        ContractBytecode,
        ServiceBytecode,
        EvmBytecode,
        ApplicationDescription,
        Committee,
        ChainDescription,
        ApplicationFormats,
        CheckpointExecutionState
    }

    function bcs_serialize_BlobType(BlobType input) internal pure returns (bytes memory) {
        return bcs_serialize_uleb128(uint256(input));
    }

    function bcs_deserialize_offset_BlobType(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobType)
    {
        uint256 new_pos;
        uint256 choice;
        (new_pos, choice) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice < 9, "invalid variant index");
        return (new_pos, BlobType(uint8(choice)));
    }

    function bcs_deserialize_BlobType(bytes memory input) internal pure returns (BlobType) {
        uint256 new_pos;
        BlobType value;
        (new_pos, value) = bcs_deserialize_offset_BlobType(0, input);
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
        opt_AccountOwner authenticated_owner;
        CryptoHash transactions_hash;
        CryptoHash messages_hash;
        CryptoHash previous_message_blocks_hash;
        CryptoHash previous_event_blocks_hash;
        CryptoHash oracle_responses_hash;
        CryptoHash events_hash;
        CryptoHash blobs_hash;
        CryptoHash operation_results_hash;
    }

    function bcs_serialize_BlockHeader(BlockHeader memory input) internal pure returns (bytes memory) {
        bytes memory result = bcs_serialize_ChainId(input.chain_id);
        result = abi.encodePacked(result, bcs_serialize_Epoch(input.epoch));
        result = abi.encodePacked(result, bcs_serialize_BlockHeight(input.height));
        result = abi.encodePacked(result, bcs_serialize_Timestamp(input.timestamp));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.state_hash));
        result = abi.encodePacked(result, bcs_serialize_opt_CryptoHash(input.previous_block_hash));
        result = abi.encodePacked(result, bcs_serialize_opt_AccountOwner(input.authenticated_owner));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.transactions_hash));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.messages_hash));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.previous_message_blocks_hash));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.previous_event_blocks_hash));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.oracle_responses_hash));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.events_hash));
        result = abi.encodePacked(result, bcs_serialize_CryptoHash(input.blobs_hash));
        return abi.encodePacked(result, bcs_serialize_CryptoHash(input.operation_results_hash));
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
        opt_AccountOwner memory authenticated_owner;
        (new_pos, authenticated_owner) = bcs_deserialize_offset_opt_AccountOwner(new_pos, input);
        CryptoHash memory transactions_hash;
        (new_pos, transactions_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory messages_hash;
        (new_pos, messages_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory previous_message_blocks_hash;
        (new_pos, previous_message_blocks_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory previous_event_blocks_hash;
        (new_pos, previous_event_blocks_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory oracle_responses_hash;
        (new_pos, oracle_responses_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory events_hash;
        (new_pos, events_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory blobs_hash;
        (new_pos, blobs_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        CryptoHash memory operation_results_hash;
        (new_pos, operation_results_hash) = bcs_deserialize_offset_CryptoHash(new_pos, input);
        return (
            new_pos,
            BlockHeader(
                chain_id,
                epoch,
                height,
                timestamp,
                state_hash,
                previous_block_hash,
                authenticated_owner,
                transactions_hash,
                messages_hash,
                previous_message_blocks_hash,
                previous_event_blocks_hash,
                oracle_responses_hash,
                events_hash,
                blobs_hash,
                operation_results_hash
            )
        );
    }

    function bcs_deserialize_BlockHeader(bytes memory input) internal pure returns (BlockHeader memory) {
        uint256 new_pos;
        BlockHeader memory value;
        (new_pos, value) = bcs_deserialize_offset_BlockHeader(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlockHeight {
        uint64 value;
    }

    function bcs_serialize_BlockHeight(BlockHeight memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_BlockHeight(bytes memory input) internal pure returns (BlockHeight memory) {
        uint256 new_pos;
        BlockHeight memory value;
        (new_pos, value) = bcs_deserialize_offset_BlockHeight(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BlockProof {
        BlockHeader header;
        Round round;
        tuple_Secp256k1PublicKey_Secp256k1Signature[] signatures;
    }

    function bcs_serialize_BlockProof(BlockProof memory input) internal pure returns (bytes memory) {
        bytes memory result = bcs_serialize_BlockHeader(input.header);
        result = abi.encodePacked(result, bcs_serialize_Round(input.round));
        return abi.encodePacked(result, bcs_serialize_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(input.signatures));
    }

    function bcs_deserialize_offset_BlockProof(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlockProof memory)
    {
        uint256 new_pos;
        BlockHeader memory header;
        (new_pos, header) = bcs_deserialize_offset_BlockHeader(pos, input);
        Round memory round;
        (new_pos, round) = bcs_deserialize_offset_Round(new_pos, input);
        tuple_Secp256k1PublicKey_Secp256k1Signature[] memory signatures;
        (new_pos, signatures) = bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(new_pos, input);
        return (new_pos, BlockProof(header, round, signatures));
    }

    function bcs_deserialize_BlockProof(bytes memory input) internal pure returns (BlockProof memory) {
        uint256 new_pos;
        BlockProof memory value;
        (new_pos, value) = bcs_deserialize_offset_BlockProof(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    enum CertificateKind {
        Timeout,
        Validated,
        Confirmed
    }

    function bcs_serialize_CertificateKind(CertificateKind input) internal pure returns (bytes memory) {
        return bcs_serialize_uleb128(uint256(input));
    }

    function bcs_deserialize_offset_CertificateKind(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, CertificateKind)
    {
        uint256 new_pos;
        uint256 choice;
        (new_pos, choice) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice < 3, "invalid variant index");
        return (new_pos, CertificateKind(uint8(choice)));
    }

    function bcs_deserialize_CertificateKind(bytes memory input) internal pure returns (CertificateKind) {
        uint256 new_pos;
        CertificateKind value;
        (new_pos, value) = bcs_deserialize_offset_CertificateKind(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ChainId {
        CryptoHash value;
    }

    function bcs_serialize_ChainId(ChainId memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_ChainId(bytes memory input) internal pure returns (ChainId memory) {
        uint256 new_pos;
        ChainId memory value;
        (new_pos, value) = bcs_deserialize_offset_ChainId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct CryptoHash {
        bytes32 value;
    }

    function bcs_serialize_CryptoHash(CryptoHash memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_CryptoHash(bytes memory input) internal pure returns (CryptoHash memory) {
        uint256 new_pos;
        CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_CryptoHash(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Cursor {
        BlockHeight height;
        uint32 index;
    }

    function bcs_serialize_Cursor(Cursor memory input) internal pure returns (bytes memory) {
        bytes memory result = bcs_serialize_BlockHeight(input.height);
        return abi.encodePacked(result, bcs_serialize_uint32(input.index));
    }

    function bcs_deserialize_offset_Cursor(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Cursor memory)
    {
        uint256 new_pos;
        BlockHeight memory height;
        (new_pos, height) = bcs_deserialize_offset_BlockHeight(pos, input);
        uint32 index;
        (new_pos, index) = bcs_deserialize_offset_uint32(new_pos, input);
        return (new_pos, Cursor(height, index));
    }

    function bcs_deserialize_Cursor(bytes memory input) internal pure returns (Cursor memory) {
        uint256 new_pos;
        Cursor memory value;
        (new_pos, value) = bcs_deserialize_offset_Cursor(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Epoch {
        uint32 value;
    }

    function bcs_serialize_Epoch(Epoch memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_Epoch(bytes memory input) internal pure returns (Epoch memory) {
        uint256 new_pos;
        Epoch memory value;
        (new_pos, value) = bcs_deserialize_offset_Epoch(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct EpochEventData {
        CryptoHash blob_hash;
        Timestamp timestamp;
    }

    function bcs_serialize_EpochEventData(EpochEventData memory input) internal pure returns (bytes memory) {
        bytes memory result = bcs_serialize_CryptoHash(input.blob_hash);
        return abi.encodePacked(result, bcs_serialize_Timestamp(input.timestamp));
    }

    function bcs_deserialize_offset_EpochEventData(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, EpochEventData memory)
    {
        uint256 new_pos;
        CryptoHash memory blob_hash;
        (new_pos, blob_hash) = bcs_deserialize_offset_CryptoHash(pos, input);
        Timestamp memory timestamp;
        (new_pos, timestamp) = bcs_deserialize_offset_Timestamp(new_pos, input);
        return (new_pos, EpochEventData(blob_hash, timestamp));
    }

    function bcs_deserialize_EpochEventData(bytes memory input) internal pure returns (EpochEventData memory) {
        uint256 new_pos;
        EpochEventData memory value;
        (new_pos, value) = bcs_deserialize_offset_EpochEventData(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Event {
        StreamId stream_id;
        uint32 index;
        bytes value;
    }

    function bcs_serialize_Event(Event memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_Event(bytes memory input) internal pure returns (Event memory) {
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

    function bcs_serialize_EventId(EventId memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_EventId(bytes memory input) internal pure returns (EventId memory) {
        uint256 new_pos;
        EventId memory value;
        (new_pos, value) = bcs_deserialize_offset_EventId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct EvmPublicKey {
        tuplearray33_uint8 value;
    }

    function bcs_serialize_EvmPublicKey(EvmPublicKey memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_EvmPublicKey(bytes memory input) internal pure returns (EvmPublicKey memory) {
        uint256 new_pos;
        EvmPublicKey memory value;
        (new_pos, value) = bcs_deserialize_offset_EvmPublicKey(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct EvmSignature {
        tuplearray65_uint8 value;
    }

    function bcs_serialize_EvmSignature(EvmSignature memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_EvmSignature(bytes memory input) internal pure returns (EvmSignature memory) {
        uint256 new_pos;
        EvmSignature memory value;
        (new_pos, value) = bcs_deserialize_offset_EvmSignature(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct GenericApplicationId {
        uint64 choice;
        // choice=0 corresponds to System
        // choice=1 corresponds to User
        ApplicationId user;
    }

    function GenericApplicationId_case_system() internal pure returns (GenericApplicationId memory) {
        ApplicationId memory user;
        return GenericApplicationId(uint64(0), user);
    }

    function GenericApplicationId_case_user(ApplicationId memory user)
        internal
        pure
        returns (GenericApplicationId memory)
    {
        return GenericApplicationId(uint64(1), user);
    }

    function bcs_serialize_GenericApplicationId(GenericApplicationId memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return hex"00";
        }
        if (input.choice == 1) {
            return abi.encodePacked(hex"01", bcs_serialize_ApplicationId(input.user));
        }
        revert("invalid variant index");
    }

    function bcs_deserialize_offset_GenericApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, GenericApplicationId memory)
    {
        uint256 new_pos;
        uint256 choice_raw;
        (new_pos, choice_raw) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice_raw <= type(uint64).max, "variant index does not fit in uint64");
        uint64 choice = uint64(choice_raw);
        require(choice < 2, "invalid variant index");
        ApplicationId memory user;
        if (choice == 1) {
            (new_pos, user) = bcs_deserialize_offset_ApplicationId(new_pos, input);
        }
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

    function bcs_serialize_Header(Header memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_Header(bytes memory input) internal pure returns (Header memory) {
        uint256 new_pos;
        Header memory value;
        (new_pos, value) = bcs_deserialize_offset_Header(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OracleResponse {
        uint64 choice;
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
        // choice=7 corresponds to Checkpoint
        OracleResponse_Checkpoint checkpoint;
    }

    function OracleResponse_case_service(bytes memory service) internal pure returns (OracleResponse memory) {
        Response memory http;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(0), service, http, blob, round, event_, event_exists, checkpoint);
    }

    function OracleResponse_case_http(Response memory http) internal pure returns (OracleResponse memory) {
        bytes memory service;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(1), service, http, blob, round, event_, event_exists, checkpoint);
    }

    function OracleResponse_case_blob(BlobId memory blob) internal pure returns (OracleResponse memory) {
        bytes memory service;
        Response memory http;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(2), service, http, blob, round, event_, event_exists, checkpoint);
    }

    function OracleResponse_case_assert() internal pure returns (OracleResponse memory) {
        bytes memory service;
        Response memory http;
        BlobId memory blob;
        opt_uint32 memory round;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(3), service, http, blob, round, event_, event_exists, checkpoint);
    }

    function OracleResponse_case_round(opt_uint32 memory round) internal pure returns (OracleResponse memory) {
        bytes memory service;
        Response memory http;
        BlobId memory blob;
        OracleResponse_Event memory event_;
        EventId memory event_exists;
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(4), service, http, blob, round, event_, event_exists, checkpoint);
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
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(5), service, http, blob, round, event_, event_exists, checkpoint);
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
        OracleResponse_Checkpoint memory checkpoint;
        return OracleResponse(uint64(6), service, http, blob, round, event_, event_exists, checkpoint);
    }

    function OracleResponse_case_checkpoint(OracleResponse_Checkpoint memory checkpoint)
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
        return OracleResponse(uint64(7), service, http, blob, round, event_, event_exists, checkpoint);
    }

    function bcs_serialize_OracleResponse(OracleResponse memory input) internal pure returns (bytes memory) {
        if (input.choice == 0) {
            return abi.encodePacked(hex"00", bcs_serialize_bytes(input.service));
        }
        if (input.choice == 1) {
            return abi.encodePacked(hex"01", bcs_serialize_Response(input.http));
        }
        if (input.choice == 2) {
            return abi.encodePacked(hex"02", bcs_serialize_BlobId(input.blob));
        }
        if (input.choice == 3) {
            return hex"03";
        }
        if (input.choice == 4) {
            return abi.encodePacked(hex"04", bcs_serialize_opt_uint32(input.round));
        }
        if (input.choice == 5) {
            return abi.encodePacked(hex"05", bcs_serialize_OracleResponse_Event(input.event_));
        }
        if (input.choice == 6) {
            return abi.encodePacked(hex"06", bcs_serialize_EventId(input.event_exists));
        }
        if (input.choice == 7) {
            return abi.encodePacked(hex"07", bcs_serialize_OracleResponse_Checkpoint(input.checkpoint));
        }
        revert("invalid variant index");
    }

    function bcs_deserialize_offset_OracleResponse(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OracleResponse memory)
    {
        uint256 new_pos;
        uint256 choice_raw;
        (new_pos, choice_raw) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice_raw <= type(uint64).max, "variant index does not fit in uint64");
        uint64 choice = uint64(choice_raw);
        require(choice < 8, "invalid variant index");
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
        OracleResponse_Checkpoint memory checkpoint;
        if (choice == 7) {
            (new_pos, checkpoint) = bcs_deserialize_offset_OracleResponse_Checkpoint(new_pos, input);
        }
        return (new_pos, OracleResponse(choice, service, http, blob, round, event_, event_exists, checkpoint));
    }

    function bcs_deserialize_OracleResponse(bytes memory input) internal pure returns (OracleResponse memory) {
        uint256 new_pos;
        OracleResponse memory value;
        (new_pos, value) = bcs_deserialize_offset_OracleResponse(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct OracleResponse_Checkpoint {
        CryptoHash[] execution_state_blobs;
        BlobId[] used_blobs;
        CryptoHash[] outbox_block_hashes;
        tuple_ChainId_Cursor[] inbox_cursors;
    }

    function bcs_serialize_OracleResponse_Checkpoint(OracleResponse_Checkpoint memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_seq_CryptoHash(input.execution_state_blobs);
        result = abi.encodePacked(result, bcs_serialize_seq_BlobId(input.used_blobs));
        result = abi.encodePacked(result, bcs_serialize_seq_CryptoHash(input.outbox_block_hashes));
        return abi.encodePacked(result, bcs_serialize_seq_tuple_ChainId_Cursor(input.inbox_cursors));
    }

    function bcs_deserialize_offset_OracleResponse_Checkpoint(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, OracleResponse_Checkpoint memory)
    {
        uint256 new_pos;
        CryptoHash[] memory execution_state_blobs;
        (new_pos, execution_state_blobs) = bcs_deserialize_offset_seq_CryptoHash(pos, input);
        BlobId[] memory used_blobs;
        (new_pos, used_blobs) = bcs_deserialize_offset_seq_BlobId(new_pos, input);
        CryptoHash[] memory outbox_block_hashes;
        (new_pos, outbox_block_hashes) = bcs_deserialize_offset_seq_CryptoHash(new_pos, input);
        tuple_ChainId_Cursor[] memory inbox_cursors;
        (new_pos, inbox_cursors) = bcs_deserialize_offset_seq_tuple_ChainId_Cursor(new_pos, input);
        return
            (new_pos, OracleResponse_Checkpoint(execution_state_blobs, used_blobs, outbox_block_hashes, inbox_cursors));
    }

    function bcs_deserialize_OracleResponse_Checkpoint(bytes memory input)
        internal
        pure
        returns (OracleResponse_Checkpoint memory)
    {
        uint256 new_pos;
        OracleResponse_Checkpoint memory value;
        (new_pos, value) = bcs_deserialize_offset_OracleResponse_Checkpoint(0, input);
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

    struct Response {
        uint16 status;
        Header[] headers;
        bytes body;
    }

    function bcs_serialize_Response(Response memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_Response(bytes memory input) internal pure returns (Response memory) {
        uint256 new_pos;
        Response memory value;
        (new_pos, value) = bcs_deserialize_offset_Response(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Round {
        uint64 choice;
        // choice=0 corresponds to Fast
        // choice=1 corresponds to MultiLeader
        uint32 multi_leader;
        // choice=2 corresponds to SingleLeader
        uint32 single_leader;
        // choice=3 corresponds to Validator
        uint32 validator;
    }

    function Round_case_fast() internal pure returns (Round memory) {
        uint32 multi_leader;
        uint32 single_leader;
        uint32 validator;
        return Round(uint64(0), multi_leader, single_leader, validator);
    }

    function Round_case_multi_leader(uint32 multi_leader) internal pure returns (Round memory) {
        uint32 single_leader;
        uint32 validator;
        return Round(uint64(1), multi_leader, single_leader, validator);
    }

    function Round_case_single_leader(uint32 single_leader) internal pure returns (Round memory) {
        uint32 multi_leader;
        uint32 validator;
        return Round(uint64(2), multi_leader, single_leader, validator);
    }

    function Round_case_validator(uint32 validator) internal pure returns (Round memory) {
        uint32 multi_leader;
        uint32 single_leader;
        return Round(uint64(3), multi_leader, single_leader, validator);
    }

    function bcs_serialize_Round(Round memory input) internal pure returns (bytes memory) {
        if (input.choice == 0) {
            return hex"00";
        }
        if (input.choice == 1) {
            return abi.encodePacked(hex"01", bcs_serialize_uint32(input.multi_leader));
        }
        if (input.choice == 2) {
            return abi.encodePacked(hex"02", bcs_serialize_uint32(input.single_leader));
        }
        if (input.choice == 3) {
            return abi.encodePacked(hex"03", bcs_serialize_uint32(input.validator));
        }
        revert("invalid variant index");
    }

    function bcs_deserialize_offset_Round(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Round memory)
    {
        uint256 new_pos;
        uint256 choice_raw;
        (new_pos, choice_raw) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice_raw <= type(uint64).max, "variant index does not fit in uint64");
        uint64 choice = uint64(choice_raw);
        require(choice < 4, "invalid variant index");
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
        return (new_pos, Round(choice, multi_leader, single_leader, validator));
    }

    function bcs_deserialize_Round(bytes memory input) internal pure returns (Round memory) {
        uint256 new_pos;
        Round memory value;
        (new_pos, value) = bcs_deserialize_offset_Round(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Secp256k1PublicKey {
        tuplearray65_uint8 value;
    }

    function bcs_serialize_Secp256k1PublicKey(Secp256k1PublicKey memory input) internal pure returns (bytes memory) {
        return bcs_serialize_tuplearray65_uint8(input.value);
    }

    function bcs_deserialize_offset_Secp256k1PublicKey(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Secp256k1PublicKey memory)
    {
        uint256 new_pos;
        tuplearray65_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray65_uint8(pos, input);
        return (new_pos, Secp256k1PublicKey(value));
    }

    function bcs_deserialize_Secp256k1PublicKey(bytes memory input) internal pure returns (Secp256k1PublicKey memory) {
        uint256 new_pos;
        Secp256k1PublicKey memory value;
        (new_pos, value) = bcs_deserialize_offset_Secp256k1PublicKey(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Secp256k1Signature {
        tuplearray64_uint8 value;
    }

    function bcs_serialize_Secp256k1Signature(Secp256k1Signature memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_Secp256k1Signature(bytes memory input) internal pure returns (Secp256k1Signature memory) {
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

    function bcs_serialize_StreamId(StreamId memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_StreamId(bytes memory input) internal pure returns (StreamId memory) {
        uint256 new_pos;
        StreamId memory value;
        (new_pos, value) = bcs_deserialize_offset_StreamId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct StreamName {
        bytes value;
    }

    function bcs_serialize_StreamName(StreamName memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_StreamName(bytes memory input) internal pure returns (StreamName memory) {
        uint256 new_pos;
        StreamName memory value;
        (new_pos, value) = bcs_deserialize_offset_StreamName(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Timestamp {
        uint64 value;
    }

    function bcs_serialize_Timestamp(Timestamp memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_Timestamp(bytes memory input) internal pure returns (Timestamp memory) {
        uint256 new_pos;
        Timestamp memory value;
        (new_pos, value) = bcs_deserialize_offset_Timestamp(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct VoteValue {
        CryptoHash entry0;
        Round entry1;
        CertificateKind entry2;
    }

    function bcs_serialize_VoteValue(VoteValue memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_VoteValue(bytes memory input) internal pure returns (VoteValue memory) {
        uint256 new_pos;
        VoteValue memory value;
        (new_pos, value) = bcs_deserialize_offset_VoteValue(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bool(bool input) internal pure returns (bytes memory) {
        return abi.encodePacked(input);
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

    function bcs_deserialize_bool(bytes memory input) internal pure returns (bool) {
        uint256 new_pos;
        bool value;
        (new_pos, value) = bcs_deserialize_offset_bool(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bytes(bytes memory input) internal pure returns (bytes memory) {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_uleb128(len);
        return abi.encodePacked(result, input);
    }

    function bcs_deserialize_offset_bytes(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, bytes memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        bytes memory result = new bytes(len);
        for (uint256 u = 0; u < len; u++) {
            result[u] = input[new_pos + u];
        }
        return (new_pos + len, result);
    }

    function bcs_deserialize_bytes(bytes memory input) internal pure returns (bytes memory) {
        uint256 new_pos;
        bytes memory value;
        (new_pos, value) = bcs_deserialize_offset_bytes(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bytes20(bytes20 input) internal pure returns (bytes memory) {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_bytes20(uint256 pos, bytes memory input) internal pure returns (uint256, bytes20) {
        bytes20 dest;
        assembly ("memory-safe") {
            dest := mload(add(add(input, 0x20), pos))
        }
        return (pos + 20, dest);
    }

    function bcs_serialize_bytes32(bytes32 input) internal pure returns (bytes memory) {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_bytes32(uint256 pos, bytes memory input) internal pure returns (uint256, bytes32) {
        bytes32 dest;
        assembly ("memory-safe") {
            dest := mload(add(add(input, 0x20), pos))
        }
        return (pos + 32, dest);
    }

    struct opt_AccountOwner {
        bool has_value;
        AccountOwner value;
    }

    function bcs_serialize_opt_AccountOwner(opt_AccountOwner memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_opt_AccountOwner(bytes memory input) internal pure returns (opt_AccountOwner memory) {
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

    function bcs_serialize_opt_CryptoHash(opt_CryptoHash memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_opt_CryptoHash(bytes memory input) internal pure returns (opt_CryptoHash memory) {
        uint256 new_pos;
        opt_CryptoHash memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_CryptoHash(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct opt_uint32 {
        bool has_value;
        uint32 value;
    }

    function bcs_serialize_opt_uint32(opt_uint32 memory input) internal pure returns (bytes memory) {
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

    function bcs_deserialize_opt_uint32(bytes memory input) internal pure returns (opt_uint32 memory) {
        uint256 new_pos;
        opt_uint32 memory value;
        (new_pos, value) = bcs_deserialize_offset_opt_uint32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_BlobId(BlobId[] memory input) internal pure returns (bytes memory) {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_uleb128(len);
        for (uint256 i = 0; i < len; i++) {
            result = abi.encodePacked(result, bcs_serialize_BlobId(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_BlobId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BlobId[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        BlobId[] memory result;
        result = new BlobId[](len);
        BlobId memory value;
        for (uint256 i = 0; i < len; i++) {
            (new_pos, value) = bcs_deserialize_offset_BlobId(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_BlobId(bytes memory input) internal pure returns (BlobId[] memory) {
        uint256 new_pos;
        BlobId[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_BlobId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_CryptoHash(CryptoHash[] memory input) internal pure returns (bytes memory) {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_uleb128(len);
        for (uint256 i = 0; i < len; i++) {
            result = abi.encodePacked(result, bcs_serialize_CryptoHash(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_CryptoHash(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, CryptoHash[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        CryptoHash[] memory result;
        result = new CryptoHash[](len);
        CryptoHash memory value;
        for (uint256 i = 0; i < len; i++) {
            (new_pos, value) = bcs_deserialize_offset_CryptoHash(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_CryptoHash(bytes memory input) internal pure returns (CryptoHash[] memory) {
        uint256 new_pos;
        CryptoHash[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_CryptoHash(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_Header(Header[] memory input) internal pure returns (bytes memory) {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_uleb128(len);
        for (uint256 i = 0; i < len; i++) {
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
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        Header[] memory result;
        result = new Header[](len);
        Header memory value;
        for (uint256 i = 0; i < len; i++) {
            (new_pos, value) = bcs_deserialize_offset_Header(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_Header(bytes memory input) internal pure returns (Header[] memory) {
        uint256 new_pos;
        Header[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_Header(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_tuple_ChainId_Cursor(tuple_ChainId_Cursor[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_uleb128(len);
        for (uint256 i = 0; i < len; i++) {
            result = abi.encodePacked(result, bcs_serialize_tuple_ChainId_Cursor(input[i]));
        }
        return result;
    }

    function bcs_deserialize_offset_seq_tuple_ChainId_Cursor(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_ChainId_Cursor[] memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        tuple_ChainId_Cursor[] memory result;
        result = new tuple_ChainId_Cursor[](len);
        tuple_ChainId_Cursor memory value;
        for (uint256 i = 0; i < len; i++) {
            (new_pos, value) = bcs_deserialize_offset_tuple_ChainId_Cursor(new_pos, input);
            result[i] = value;
        }
        return (new_pos, result);
    }

    function bcs_deserialize_seq_tuple_ChainId_Cursor(bytes memory input)
        internal
        pure
        returns (tuple_ChainId_Cursor[] memory)
    {
        uint256 new_pos;
        tuple_ChainId_Cursor[] memory value;
        (new_pos, value) = bcs_deserialize_offset_seq_tuple_ChainId_Cursor(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(tuple_Secp256k1PublicKey_Secp256k1Signature[] memory input)
        internal
        pure
        returns (bytes memory)
    {
        uint256 len = input.length;
        bytes memory result = bcs_serialize_uleb128(len);
        for (uint256 i = 0; i < len; i++) {
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
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        tuple_Secp256k1PublicKey_Secp256k1Signature[] memory result;
        result = new tuple_Secp256k1PublicKey_Secp256k1Signature[](len);
        tuple_Secp256k1PublicKey_Secp256k1Signature memory value;
        for (uint256 i = 0; i < len; i++) {
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

    function bcs_serialize_string(string memory input) internal pure returns (bytes memory) {
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
        bytes memory result_len = bcs_serialize_uleb128(number_char);
        return abi.encodePacked(result_len, input);
    }

    function bcs_deserialize_offset_string(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, string memory)
    {
        uint256 len;
        uint256 new_pos;
        (new_pos, len) = bcs_deserialize_offset_uleb128(pos, input);
        uint256 shift = 0;
        for (uint256 i = 0; i < len; i++) {
            while (true) {
                bytes1 val = input[new_pos + shift];
                shift += 1;
                if (uint8(val) < 128) {
                    break;
                }
            }
        }
        bytes memory result_bytes = new bytes(shift);
        for (uint256 i = 0; i < shift; i++) {
            result_bytes[i] = input[new_pos + i];
        }
        string memory result = string(result_bytes);
        return (new_pos + shift, result);
    }

    function bcs_deserialize_string(bytes memory input) internal pure returns (string memory) {
        uint256 new_pos;
        string memory value;
        (new_pos, value) = bcs_deserialize_offset_string(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuple_ChainId_Cursor {
        ChainId entry0;
        Cursor entry1;
    }

    function bcs_serialize_tuple_ChainId_Cursor(tuple_ChainId_Cursor memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_ChainId(input.entry0);
        return abi.encodePacked(result, bcs_serialize_Cursor(input.entry1));
    }

    function bcs_deserialize_offset_tuple_ChainId_Cursor(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, tuple_ChainId_Cursor memory)
    {
        uint256 new_pos;
        ChainId memory entry0;
        (new_pos, entry0) = bcs_deserialize_offset_ChainId(pos, input);
        Cursor memory entry1;
        (new_pos, entry1) = bcs_deserialize_offset_Cursor(new_pos, input);
        return (new_pos, tuple_ChainId_Cursor(entry0, entry1));
    }

    function bcs_deserialize_tuple_ChainId_Cursor(bytes memory input)
        internal
        pure
        returns (tuple_ChainId_Cursor memory)
    {
        uint256 new_pos;
        tuple_ChainId_Cursor memory value;
        (new_pos, value) = bcs_deserialize_offset_tuple_ChainId_Cursor(0, input);
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

    function bcs_serialize_tuplearray33_uint8(tuplearray33_uint8 memory input) internal pure returns (bytes memory) {
        bytes memory result;
        for (uint256 i = 0; i < 33; i++) {
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
        for (uint256 i = 0; i < 33; i++) {
            (new_pos, value) = bcs_deserialize_offset_uint8(new_pos, input);
            values[i] = value;
        }
        return (new_pos, tuplearray33_uint8(values));
    }

    function bcs_deserialize_tuplearray33_uint8(bytes memory input) internal pure returns (tuplearray33_uint8 memory) {
        uint256 new_pos;
        tuplearray33_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray33_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuplearray64_uint8 {
        uint8[] values;
    }

    function bcs_serialize_tuplearray64_uint8(tuplearray64_uint8 memory input) internal pure returns (bytes memory) {
        bytes memory result;
        for (uint256 i = 0; i < 64; i++) {
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
        for (uint256 i = 0; i < 64; i++) {
            (new_pos, value) = bcs_deserialize_offset_uint8(new_pos, input);
            values[i] = value;
        }
        return (new_pos, tuplearray64_uint8(values));
    }

    function bcs_deserialize_tuplearray64_uint8(bytes memory input) internal pure returns (tuplearray64_uint8 memory) {
        uint256 new_pos;
        tuplearray64_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray64_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct tuplearray65_uint8 {
        uint8[] values;
    }

    function bcs_serialize_tuplearray65_uint8(tuplearray65_uint8 memory input) internal pure returns (bytes memory) {
        bytes memory result;
        for (uint256 i = 0; i < 65; i++) {
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
        for (uint256 i = 0; i < 65; i++) {
            (new_pos, value) = bcs_deserialize_offset_uint8(new_pos, input);
            values[i] = value;
        }
        return (new_pos, tuplearray65_uint8(values));
    }

    function bcs_deserialize_tuplearray65_uint8(bytes memory input) internal pure returns (tuplearray65_uint8 memory) {
        uint256 new_pos;
        tuplearray65_uint8 memory value;
        (new_pos, value) = bcs_deserialize_offset_tuplearray65_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint16(uint16 input) internal pure returns (bytes memory) {
        bytes memory result = new bytes(2);
        uint16 value = input;
        result[0] = bytes1(uint8(value));
        value = value >> 8;
        result[1] = bytes1(uint8(value));
        return result;
    }

    function bcs_deserialize_offset_uint16(uint256 pos, bytes memory input) internal pure returns (uint256, uint16) {
        uint16 value = uint8(input[pos + 1]);
        value = value << 8;
        value += uint8(input[pos]);
        return (pos + 2, value);
    }

    function bcs_deserialize_uint16(bytes memory input) internal pure returns (uint16) {
        uint256 new_pos;
        uint16 value;
        (new_pos, value) = bcs_deserialize_offset_uint16(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint32(uint32 input) internal pure returns (bytes memory) {
        bytes memory result = new bytes(4);
        uint32 value = input;
        result[0] = bytes1(uint8(value));
        for (uint256 i = 1; i < 4; i++) {
            value = value >> 8;
            result[i] = bytes1(uint8(value));
        }
        return result;
    }

    function bcs_deserialize_offset_uint32(uint256 pos, bytes memory input) internal pure returns (uint256, uint32) {
        uint32 value = uint8(input[pos + 3]);
        for (uint256 i = 0; i < 3; i++) {
            value = value << 8;
            value += uint8(input[pos + 2 - i]);
        }
        return (pos + 4, value);
    }

    function bcs_deserialize_uint32(bytes memory input) internal pure returns (uint32) {
        uint256 new_pos;
        uint32 value;
        (new_pos, value) = bcs_deserialize_offset_uint32(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint64(uint64 input) internal pure returns (bytes memory) {
        bytes memory result = new bytes(8);
        uint64 value = input;
        result[0] = bytes1(uint8(value));
        for (uint256 i = 1; i < 8; i++) {
            value = value >> 8;
            result[i] = bytes1(uint8(value));
        }
        return result;
    }

    function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input) internal pure returns (uint256, uint64) {
        uint64 value = uint8(input[pos + 7]);
        for (uint256 i = 0; i < 7; i++) {
            value = value << 8;
            value += uint8(input[pos + 6 - i]);
        }
        return (pos + 8, value);
    }

    function bcs_deserialize_uint64(bytes memory input) internal pure returns (uint64) {
        uint256 new_pos;
        uint64 value;
        (new_pos, value) = bcs_deserialize_offset_uint64(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint8(uint8 input) internal pure returns (bytes memory) {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input) internal pure returns (uint256, uint8) {
        uint8 value = uint8(input[pos]);
        return (pos + 1, value);
    }

    function bcs_deserialize_uint8(bytes memory input) internal pure returns (uint8) {
        uint256 new_pos;
        uint8 value;
        (new_pos, value) = bcs_deserialize_offset_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }
} // end of library BridgeTypes
