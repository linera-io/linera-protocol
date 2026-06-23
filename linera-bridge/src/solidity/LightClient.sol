// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "ILightClient.sol";

contract LightClient is ILightClient {
    // Per-epoch committee storage
    struct EpochCommittee {
        mapping(address => uint64) weights;
        mapping(address => uint256) indices; // 1-indexed; 0 means not a member
        uint256 validatorCount;
        uint64 totalWeight;
        uint64 quorumThreshold;
        // Admin-chain height of the block that created this committee.
        uint64 createdAtHeight;
    }
    mapping(uint32 => EpochCommittee) private committees;
    uint32 public currentEpoch;
    // Certificates whose epoch is below this are rejected. Defaults to 0 (no
    // epoch retired). Raised monotonically via `expireEpochsBelow` to drop
    // retired committees from the set of valid signing roots — a
    // weak-subjectivity floor: a quorum of a long-retired committee's keys can
    // no longer forge a certificate once its epoch is expired.
    uint32 public minAcceptedEpoch;
    bytes32 public override adminChainId;

    // Governance. Both immutable — rotation is a redeployment, never an on-chain
    // setter (avoids a recursive "who controls the rotation?" trust problem).
    // `pauseGuardian` can pause `registerBlock`; `proposer` gates the
    // `expireEpochsBelow` weak-subjectivity floor.
    address public immutable pauseGuardian;
    address public immutable proposer;

    // Emergency pause. `registerBlock` is rejected while `block.timestamp <
    // pausedUntil`. Auto-expires so the guardian can never freeze the client
    // indefinitely. NOTE: this client is shared by every consumer bridge on the
    // network, so a pause here halts inbound block registration for all of them.
    uint256 public constant PAUSE_MAX_DURATION = 14 days;
    uint256 public pausedUntil;

    modifier onlyProposer() {
        require(msg.sender == proposer, "only proposer");
        _;
    }

    modifier onlyPauseGuardian() {
        require(msg.sender == pauseGuardian, "only pause guardian");
        _;
    }

    modifier whenNotEmergencyPaused() {
        require(block.timestamp >= pausedUntil, "emergency paused");
        _;
    }

    constructor(
        address[] memory validators,
        uint64[] memory weights,
        bytes32 _adminChainId,
        uint32 _epoch,
        address _pauseGuardian,
        address _proposer
    ) {
        require(validators.length == weights.length, "length mismatch");
        require(_pauseGuardian != address(0), "zero pauseGuardian");
        require(_proposer != address(0), "zero proposer");
        // Genesis committee has no backing admin block, so its height is 0.
        _setCommittee(_epoch, validators, weights, 0);
        adminChainId = _adminChainId;
        pauseGuardian = _pauseGuardian;
        proposer = _proposer;
    }

    event EmergencyPaused(uint256 until);
    event EmergencyUnpaused();

    /// Pauses `registerBlock` for `duration` (auto-expiring, capped at
    /// `PAUSE_MAX_DURATION`). Guardian-only. Re-pausing before expiry restarts
    /// the clock.
    function emergencyPause(uint256 duration) external onlyPauseGuardian {
        require(duration > 0 && duration <= PAUSE_MAX_DURATION, "invalid duration");
        pausedUntil = block.timestamp + duration;
        emit EmergencyPaused(pausedUntil);
    }

    /// Lifts an active pause early. Guardian-only.
    function emergencyUnpause() external onlyPauseGuardian {
        require(pausedUntil > block.timestamp, "not paused");
        pausedUntil = 0;
        emit EmergencyUnpaused();
    }

    function addCommittee(bytes calldata data, bytes calldata committeeBlob, bytes[] calldata validators)
        external
        whenNotEmergencyPaused
    {
        (BridgeTypes.Block memory blockValue,) = verifyCertificate(data);

        // The block must be from the admin chain and the current epoch
        require(blockValue.header.chain_id.value.value == adminChainId, "block must be from admin chain");
        require(blockValue.header.epoch.value == currentEpoch, "block epoch must match current epoch");

        // Find the CreateCommittee in the block operations. Linera emits at most
        // one CreateCommittee per admin block; together with the
        // `block.epoch == currentEpoch` check above, each admin block drives
        // exactly one epoch transition, so taking the first match is sufficient.
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
        BridgeTypes.BlobContent memory blobContent =
            BridgeTypes.BlobContent(BridgeTypes.BlobType.Committee, committeeBlob);
        bytes32 computedHash =
            keccak256(abi.encodePacked("BlobContent::", BridgeTypes.bcs_serialize_BlobContent(blobContent)));
        require(computedHash == expectedBlobHash, "committee blob hash mismatch");

        // Parse blob to extract addresses and weights, verified against caller's keys
        (address[] memory addrs, uint64[] memory weights) = _parseCommitteeBlob(committeeBlob, validators);

        // Store the new committee, recording the admin-chain height of the
        // block that created it so the relayer can resume scanning from here.
        _setCommittee(newEpoch, addrs, weights, blockValue.header.height.value);
    }

    /// Raises `minAcceptedEpoch`, permanently retiring every committee with an
    /// epoch below `newMinEpoch` from certificate verification and deleting its
    /// stored scalar fields (`totalWeight`, `validatorCount`, `quorumThreshold`).
    /// The per-validator `weights`/`indices` mapping entries cannot be cleared
    /// (Solidity cannot enumerate mapping keys), but they become permanently
    /// unreachable: a zeroed `totalWeight` fails the `verifyCertificate` quorum
    /// lookup, and epoch numbers are never reused (epochs are monotonic).
    ///
    /// Monotonic (`newMinEpoch` must strictly increase) and capped at
    /// `currentEpoch`, so the current committee is never retired: at
    /// `newMinEpoch == currentEpoch` the floor still admits `epoch == currentEpoch`
    /// (the verification check is `epoch >= minAcceptedEpoch`) and the delete
    /// loop stops before `currentEpoch`. Only `newMinEpoch > currentEpoch` could
    /// retire the live committee, which is rejected. The caller may keep older
    /// epochs valid for certificate-lag tolerance by choosing a lower
    /// `newMinEpoch`. Retiring a wide range in one call costs O(range) gas;
    /// expire incrementally if the gap is large.
    ///
    /// Gated to `proposer`: an unauthenticated caller could otherwise raise the
    /// floor up to `currentEpoch` and reject legitimate lagging certificates (a
    /// liveness DoS). Immediate-effect (no timelock): it cannot move funds, is
    /// monotonic, and is capped at `currentEpoch` so it can never retire the live
    /// committee — and an operator may need to retire a compromised retired
    /// committee promptly during an incident.
    function expireEpochsBelow(uint32 newMinEpoch) external onlyProposer {
        require(newMinEpoch > minAcceptedEpoch, "minAcceptedEpoch must increase");
        require(newMinEpoch <= currentEpoch, "cannot expire current epoch");
        for (uint32 epoch = minAcceptedEpoch; epoch < newMinEpoch; epoch++) {
            delete committees[epoch];
        }
        minAcceptedEpoch = newMinEpoch;
    }

    /// Total voting weight of the committee stored at `epoch`, or 0 if no such
    /// committee exists (never set, or retired via `expireEpochsBelow`).
    function committeeTotalWeight(uint32 epoch) external view returns (uint64) {
        return committees[epoch].totalWeight;
    }

    /// Admin-chain height of the block that created the committee at `epoch`, or
    /// 0 if no such committee exists or it was the genesis committee. The relayer
    /// uses `committeeHeight(currentEpoch)` to resume committee reconciliation
    /// from that height instead of re-scanning the admin chain from 0.
    function committeeHeight(uint32 epoch) external view returns (uint64) {
        return committees[epoch].createdAtHeight;
    }

    function verifyBlock(bytes calldata data) external view override returns (BridgeTypes.Block memory, bytes32) {
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
        (pos, signatures) =
            BridgeTypes.bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(pos, mdata);
        require(pos == mdata.length, "incomplete deserialization");

        // Step 4: Construct VoteValue BCS and hash with type name prefix
        // CryptoHash::new(&VoteValue(...)) = keccak256("VoteValue::" ++ BCS(VoteValue))
        BridgeTypes.VoteValue memory voteValue =
            BridgeTypes.VoteValue(BridgeTypes.CryptoHash(valueHash), round, BridgeTypes.CertificateKind.Confirmed);
        bytes32 signedHash = keccak256(abi.encodePacked("VoteValue::", BridgeTypes.bcs_serialize_VoteValue(voteValue)));

        // Step 5: Verify signatures against the block's epoch committee
        uint32 epoch = blockValue.header.epoch.value;
        require(epoch >= minAcceptedEpoch, "epoch expired");
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

    function _setCommittee(uint32 epoch, address[] memory validators, uint64[] memory weights, uint64 createdAtHeight)
        internal
    {
        require(
            epoch == currentEpoch + 1 || (committees[currentEpoch].totalWeight == 0 && currentEpoch == 0),
            "epoch must be sequential"
        );
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
        committee.createdAtHeight = createdAtHeight;
        currentEpoch = epoch;
    }

    /// Parses a BCS-serialized CommitteeMinimal blob and derives Ethereum addresses.
    /// The caller must provide `uncompressedKeys` in the same order as the blob's
    /// compressed keys (BCS canonical map order, i.e. sorted by serialized key bytes).
    /// Returns (addresses, weights) extracted from the blob.
    function _parseCommitteeBlob(bytes memory blob, bytes[] calldata uncompressedKeys)
        internal
        pure
        returns (address[] memory, uint64[] memory)
    {
        uint256 pos;
        uint256 count;
        (pos, count) = BridgeTypes.bcs_deserialize_offset_len(0, blob);
        require(count == uncompressedKeys.length, "validator count mismatch");

        address[] memory addrs = new address[](count);
        uint64[] memory weights = new uint64[](count);

        for (uint256 i = 0; i < count; i++) {
            // _verifyKeyCompression checks the x-coordinate against blob[pos+1..pos+33],
            // so a wrong-order or wrong-key entry reverts there.
            require(uncompressedKeys[i].length == 64, "uncompressed key must be 64 bytes");
            _verifyKeyCompression(uncompressedKeys[i], blob, pos);

            // Derive Ethereum address from the verified uncompressed key
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
    uint256 private constant SECP256K1_P = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F;
    // secp256k1 curve order
    uint256 private constant SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141;

    function _verifyKeyCompression(bytes calldata uncompressed, bytes memory blob, uint256 keyPos) internal pure {
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
            result |= uint64(uint8(data[pos + i])) << (i * 8);
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
