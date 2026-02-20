// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

contract LightClient {
    // Per-epoch committee storage
    struct EpochCommittee {
        mapping(address => uint64) weights;
        mapping(address => uint256) indices; // 1-indexed; 0 means not a member
        uint256 validatorCount;
        uint64 totalWeight;
        uint64 quorumThreshold;
    }
    mapping(uint32 => EpochCommittee) private committees;
    uint32 public currentEpoch;
    bytes32 public adminChainId;

    constructor(address[] memory validators, uint64[] memory weights, bytes32 _adminChainId, uint32 _epoch) {
        require(validators.length == weights.length, "length mismatch");
        _setCommittee(_epoch, validators, weights);
        adminChainId = _adminChainId;
    }

    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        bytes[] calldata validators
    ) external {
        (BridgeTypes.Block memory blockValue,) = verifyCertificate(data);

        // The block must be from the admin chain and the current epoch
        require(blockValue.header.chain_id.value.value == adminChainId, "block must be from admin chain");
        require(blockValue.header.epoch.value == currentEpoch, "block epoch must match current epoch");

        // Find CreateCommittee in block operations
        bool found = false;
        uint32 newEpoch;
        bytes32 expectedBlobHash;
        for (uint256 i = 0; i < blockValue.body.transactions.length; i++) {
            BridgeTypes.Transaction memory txn = blockValue.body.transactions[i];
            // choice=1 is ExecuteOperation
            if (txn.choice != 1) continue;
            BridgeTypes.Operation memory op = txn.execute_operation;
            // choice=0 is System
            if (op.choice != 0) continue;
            BridgeTypes.SystemOperation memory sysOp = op.system;
            // choice=10 is Admin
            if (sysOp.choice != 10) continue;
            BridgeTypes.AdminOperation memory adminOp = sysOp.admin;
            // choice=1 is CreateCommittee
            if (adminOp.choice != 1) continue;

            newEpoch = adminOp.create_committee.epoch.value;
            expectedBlobHash = adminOp.create_committee.blob_hash.value;
            found = true;
            break;
        }
        require(found, "no CreateCommittee operation found");

        // Verify committeeBlob hash matches the blob_hash from CreateCommittee
        BridgeTypes.BlobContent memory blobContent = BridgeTypes.BlobContent(
            BridgeTypes.BlobType.Committee,
            committeeBlob
        );
        bytes32 computedHash = keccak256(abi.encodePacked(
            "BlobContent::",
            BridgeTypes.bcs_serialize_BlobContent(blobContent)
        ));
        require(computedHash == expectedBlobHash, "committee blob hash mismatch");

        // Parse blob to extract addresses and weights, verified against caller's keys
        (address[] memory addrs, uint64[] memory weights) = _parseCommitteeBlob(committeeBlob, validators);

        // Store the new committee
        _setCommittee(newEpoch, addrs, weights);
    }

    function verifyBlock(bytes calldata data) external view returns (BridgeTypes.Block memory, bytes32) {
        return verifyCertificate(data);
    }

    function verifyCertificate(bytes calldata data) internal view returns (BridgeTypes.Block memory, bytes32) {
        // Copy calldata to memory for the BCS deserializer
        bytes memory mdata = data;

        // Step 1: Deserialize just the Block (value) to get its end offset
        uint256 valueEndPos;
        BridgeTypes.Block memory blockValue;
        (valueEndPos, blockValue) = BridgeTypes.bcs_deserialize_offset_Block(0, mdata);

        // Step 2: Compute value_hash = keccak256("Block::" ++ BCS(block))
        // CryptoHash::new adds a type name prefix; ConfirmedBlock is transparent over Block
        bytes32 valueHash = keccak256(abi.encodePacked("Block::", _sliceMemory(mdata, 0, valueEndPos)));

        // Step 3: Deserialize Round and signatures
        uint256 pos;
        BridgeTypes.Round memory round;
        (pos, round) = BridgeTypes.bcs_deserialize_offset_Round(valueEndPos, mdata);
        BridgeTypes.tuple_Secp256k1PublicKey_Secp256k1Signature[] memory signatures;
        (pos, signatures) = BridgeTypes.bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(pos, mdata);
        require(pos == mdata.length, "incomplete deserialization");

        // Step 4: Construct VoteValue BCS and hash with type name prefix
        // CryptoHash::new(&VoteValue(...)) = keccak256("VoteValue::" ++ BCS(VoteValue))
        BridgeTypes.VoteValue memory voteValue = BridgeTypes.VoteValue(
            BridgeTypes.CryptoHash(valueHash),
            round,
            BridgeTypes.CertificateKind.Confirmed
        );
        bytes32 signedHash = keccak256(abi.encodePacked(
            "VoteValue::",
            BridgeTypes.bcs_serialize_VoteValue(voteValue)
        ));

        // Step 5: Verify signatures against the block's epoch committee
        uint32 epoch = blockValue.header.epoch.value;
        EpochCommittee storage committee = committees[epoch];
        require(committee.totalWeight > 0, "unknown epoch");
        uint64 weight = 0;
        bool[] memory seen = new bool[](committee.validatorCount);
        for (uint256 i = 0; i < signatures.length; i++) {
            // Pack uint8[] back into contiguous bytes, then extract r and s
            uint8[] memory sigValues = signatures[i].entry1.value.values;
            bytes memory sigBytes = new bytes(64);
            for (uint256 j = 0; j < 64; j++) {
                sigBytes[j] = bytes1(sigValues[j]);
            }
            bytes32 r;
            bytes32 s;
            assembly {
                r := mload(add(sigBytes, 32))
                s := mload(add(sigBytes, 64))
            }

            // Reject zero r/s and enforce low-s canonical form (EIP-2 style)
            require(uint256(r) != 0 && uint256(s) != 0, "invalid signature component");
            require(uint256(s) <= SECP256K1_N / 2, "non-canonical high-s signature");

            // Try v=27 and v=28 since we don't have the recovery ID
            address recovered = ecrecover(signedHash, 27, r, s);
            if (recovered == address(0) || committee.weights[recovered] == 0) {
                recovered = ecrecover(signedHash, 28, r, s);
            }
            require(recovered != address(0), "signature recovery failed");
            uint64 w = committee.weights[recovered];
            require(w > 0, "unknown validator");

            // O(1) duplicate signer check via index lookup
            uint256 idx = committee.indices[recovered];
            require(!seen[idx - 1], "duplicate signer");
            seen[idx - 1] = true;

            weight += w;
        }
        require(weight >= committee.quorumThreshold, "insufficient quorum");

        return (blockValue, signedHash);
    }

    function _setCommittee(uint32 epoch, address[] memory validators, uint64[] memory weights) internal {
        require(epoch == currentEpoch + 1 || (committees[currentEpoch].totalWeight == 0 && currentEpoch == 0), "epoch must be sequential");
        require(validators.length == weights.length, "length mismatch");
        EpochCommittee storage committee = committees[epoch];
        uint64 total = 0;
        for (uint256 i = 0; i < validators.length; i++) {
            require(committee.weights[validators[i]] == 0, "duplicate validator");
            committee.weights[validators[i]] = weights[i];
            committee.indices[validators[i]] = i + 1; // 1-indexed
            total += weights[i];
        }
        committee.validatorCount = validators.length;
        committee.totalWeight = total;
        committee.quorumThreshold = 2 * total / 3 + 1;
        currentEpoch = epoch;
    }

    /// Parses a BCS-serialized CommitteeMinimal blob, verifying each compressed key
    /// against the caller-provided uncompressed keys and deriving Ethereum addresses.
    /// Returns (addresses, weights) extracted from the blob.
    function _parseCommitteeBlob(
        bytes memory blob,
        bytes[] calldata uncompressedKeys
    ) internal pure returns (address[] memory, uint64[] memory) {
        uint256 pos;
        uint256 count;
        (pos, count) = BridgeTypes.bcs_deserialize_offset_len(0, blob);
        require(count == uncompressedKeys.length, "validator count mismatch");

        address[] memory addrs = new address[](count);
        uint64[] memory weights = new uint64[](count);

        for (uint256 i = 0; i < count; i++) {
            // Read 33-byte compressed key and verify against caller's uncompressed key
            require(uncompressedKeys[i].length == 64, "uncompressed key must be 64 bytes");
            _verifyKeyCompression(uncompressedKeys[i], blob, pos);

            // Derive Ethereum address from uncompressed key
            addrs[i] = address(uint160(uint256(keccak256(uncompressedKeys[i]))));

            pos += 33; // skip compressed key

            // Skip network_address (ULEB128 length-prefixed string)
            uint256 strLen;
            (pos, strLen) = BridgeTypes.bcs_deserialize_offset_len(pos, blob);
            pos += strLen;

            // Read votes (u64 LE)
            weights[i] = _readU64LE(blob, pos);
            pos += 8;

            // Skip account_public_key (enum: 1-byte tag + payload)
            uint8 tag = uint8(blob[pos]);
            pos += 1;
            if (tag == 0) {
                pos += 32; // Ed25519: 32 bytes
            } else {
                pos += 33; // Secp256k1 or EvmSecp256k1: 33 bytes
            }
        }

        return (addrs, weights);
    }

    /// Verifies that a caller-provided 64-byte uncompressed key matches
    /// the 33-byte compressed key in the blob at the given position.
    // secp256k1 field prime
    uint256 private constant SECP256K1_P =
        0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F;
    // secp256k1 curve order
    uint256 private constant SECP256K1_N =
        0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141;

    function _verifyKeyCompression(
        bytes calldata uncompressed,
        bytes memory blob,
        uint256 keyPos
    ) internal pure {
        // Compressed key format: prefix (0x02 if y is even, 0x03 if odd) + 32-byte x
        // Uncompressed key: 32-byte x + 32-byte y (no 0x04 prefix)

        // Check x-coordinate matches (bytes 1..33 of compressed == bytes 0..32 of uncompressed)
        for (uint256 i = 0; i < 32; i++) {
            require(blob[keyPos + 1 + i] == uncompressed[i], "key x-coordinate mismatch");
        }

        // Check y-parity: last byte of y determines even/odd
        uint8 yLastByte = uint8(uncompressed[63]);
        uint8 expectedPrefix = (yLastByte % 2 == 0) ? 0x02 : 0x03;
        require(uint8(blob[keyPos]) == expectedPrefix, "key y-parity mismatch");

        // Verify (x, y) is on secp256k1: y^2 = x^3 + 7 (mod p)
        uint256 x;
        uint256 y;
        assembly {
            x := calldataload(uncompressed.offset)
            y := calldataload(add(uncompressed.offset, 32))
        }
        uint256 lhs = mulmod(y, y, SECP256K1_P);
        uint256 x2 = mulmod(x, x, SECP256K1_P);
        uint256 rhs = addmod(mulmod(x2, x, SECP256K1_P), 7, SECP256K1_P);
        require(lhs == rhs, "key not on secp256k1 curve");
    }

    /// Reads 8 bytes from data at pos as a little-endian uint64.
    function _readU64LE(bytes memory data, uint256 pos) internal pure returns (uint64) {
        uint64 result = 0;
        for (uint256 i = 0; i < 8; i++) {
            result |= uint64(uint8(data[pos + i])) << uint64(i * 8);
        }
        return result;
    }

    /// Copies a slice of memory bytes into a new bytes array.
    function _sliceMemory(bytes memory data, uint256 start, uint256 end) internal pure returns (bytes memory) {
        uint256 len = end - start;
        bytes memory result = new bytes(len);
        for (uint256 i = 0; i < len; i++) {
            result[i] = data[start + i];
        }
        return result;
    }
}
