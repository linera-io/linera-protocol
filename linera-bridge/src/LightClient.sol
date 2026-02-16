// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

contract LightClient {
    // Per-epoch committee storage
    struct EpochCommittee {
        mapping(address => uint64) weights;
        uint64 totalWeight;
        uint64 quorumThreshold;
    }
    mapping(uint32 => EpochCommittee) private committees;
    uint32 public currentEpoch;

    constructor(address[] memory validators, uint64[] memory weights) {
        _setCommittee(0, validators, weights);
        currentEpoch = 0;
    }

    function addCommittee(
        bytes calldata data,
        bytes calldata committeeBlob,
        address[] calldata validators,
        uint64[] calldata weights
    ) external {
        BridgeTypes.Block memory blockValue = verifyCertificate(data);

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

        // Store the new committee
        _setCommittee(newEpoch, validators, weights);
    }

    function addBlock(bytes calldata data) external {
        verifyCertificate(data);
        // TODO: store block data
    }

    function verifyCertificate(bytes calldata data) internal view returns (BridgeTypes.Block memory) {
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

        // Step 5: Verify signatures against current committee using ecrecover
        EpochCommittee storage committee = committees[currentEpoch];
        uint64 weight = 0;
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

            // Try v=27 and v=28 since we don't have the recovery ID
            address recovered = ecrecover(signedHash, 27, r, s);
            if (recovered == address(0) || committee.weights[recovered] == 0) {
                recovered = ecrecover(signedHash, 28, r, s);
            }
            require(recovered != address(0), "signature recovery failed");
            uint64 w = committee.weights[recovered];
            require(w > 0, "unknown validator");
            weight += w;
        }
        require(weight >= committee.quorumThreshold, "insufficient quorum");

        return blockValue;
    }

    function _setCommittee(uint32 epoch, address[] memory validators, uint64[] memory weights) internal {
        require(epoch == currentEpoch + 1 || (epoch == 0 && currentEpoch == 0), "epoch must be sequential");
        require(validators.length == weights.length, "length mismatch");
        EpochCommittee storage committee = committees[epoch];
        uint64 total = 0;
        for (uint256 i = 0; i < validators.length; i++) {
            require(committee.weights[validators[i]] == 0, "duplicate validator");
            committee.weights[validators[i]] = weights[i];
            total += weights[i];
        }
        committee.totalWeight = total;
        committee.quorumThreshold = 2 * total / 3 + 1;
        currentEpoch = epoch;
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
