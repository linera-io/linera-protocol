// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";

contract LightClient {
    // Committee: validator Ethereum address => voting weight
    mapping(address => uint64) public committeeWeights;
    address[] public committeeMembers;
    uint64 public totalWeight;
    uint64 public quorumThreshold;


    constructor(address[] memory validators, uint64[] memory weights) {
        require(validators.length == weights.length, "length mismatch");
        uint64 total = 0;
        for (uint256 i = 0; i < validators.length; i++) {
            require(committeeWeights[validators[i]] == 0, "duplicate validator");
            committeeWeights[validators[i]] = weights[i];
            committeeMembers.push(validators[i]);
            total += weights[i];
        }
        totalWeight = total;
        quorumThreshold = 2 * total / 3 + 1;
    }

    function addCommittee(bytes calldata data) external {
        verifyCertificate(data);
        // TODO: update committee from certificate content
    }

    function addBlock(bytes calldata data) external {
        verifyCertificate(data);
        // TODO: store block data
    }

    function verifyCertificate(bytes calldata data) internal view {
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

        // Step 5: Verify signatures against committee using ecrecover
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
            if (recovered == address(0) || committeeWeights[recovered] == 0) {
                recovered = ecrecover(signedHash, 28, r, s);
            }
            require(recovered != address(0), "signature recovery failed");
            uint64 w = committeeWeights[recovered];
            require(w > 0, "unknown validator");
            weight += w;
        }
        require(weight >= quorumThreshold, "insufficient quorum");
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
