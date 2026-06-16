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
    bytes32 public adminChainId;

    /// Metadata recorded for a block whose quorum has been verified via `registerBlock`. Stored so
    /// individual events can later be proven against it (`verifyEventInclusion`) and settled
    /// (`processBurns`) without re-checking the certificate or re-parsing the header per chunk.
    /// `eventsHash` is never zero for a valid header, so a zero `eventsHash` means "unregistered".
    struct RegisteredBlock {
        bytes32 eventsHash;
        uint64 height;
        bytes32 chainId;
    }

    /// Maps a registered block's hash (`keccak256("BlockHeader::" ++ BCS(header))`) to its metadata.
    mapping(bytes32 => RegisteredBlock) public registeredBlocks;

    constructor(address[] memory validators, uint64[] memory weights, bytes32 _adminChainId, uint32 _epoch) {
        require(validators.length == weights.length, "length mismatch");
        // Genesis committee has no backing admin block, so its height is 0.
        _setCommittee(_epoch, validators, weights, 0);
        adminChainId = _adminChainId;
    }

    function addCommittee(bytes calldata blockProof, bytes[] calldata transactionBcs, bytes calldata committeeBlob)
        external
    {
        uint64 height = _verifyAdminTransactions(blockProof, transactionBcs);

        (bool found, uint32 newEpoch, bytes32 expectedBlobHash) = _findCreateCommittee(transactionBcs);
        require(found, "no CreateCommittee operation found");

        // Verify committeeBlob hash matches the blob_hash from CreateCommittee
        BridgeTypes.BlobContent memory blobContent =
            BridgeTypes.BlobContent(BridgeTypes.BlobType.Committee, committeeBlob);
        bytes32 computedHash =
            keccak256(abi.encodePacked("BlobContent::", BridgeTypes.bcs_serialize_BlobContent(blobContent)));
        require(computedHash == expectedBlobHash, "committee blob hash mismatch");

        // Parse blob to extract addresses and weights
        (address[] memory addrs, uint64[] memory weights) = _parseCommitteeBlob(committeeBlob);

        // Store the new committee, recording the admin-chain height of the
        // block that created it so the relayer can resume scanning from here.
        _setCommittee(newEpoch, addrs, weights, height);
    }

    /// Raises `minAcceptedEpoch`, permanently retiring every committee with an
    /// epoch below `newMinEpoch` from certificate verification and deleting its
    /// stored scalar fields (`totalWeight`, `validatorCount`, `quorumThreshold`).
    /// The per-validator `weights`/`indices` mapping entries cannot be cleared
    /// (Solidity cannot enumerate mapping keys), but they become permanently
    /// unreachable: a zeroed `totalWeight` fails the `_verifyQuorum` committee
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
    /// TODO(security): access control is intentionally deferred. This function
    /// is currently UNAUTHENTICATED and MUST be gated (owner / governance)
    /// before any production deployment — an unauthenticated caller can
    /// otherwise raise the floor up to `currentEpoch` and reject legitimate
    /// lagging certificates (a liveness DoS).
    function expireEpochsBelow(uint32 newMinEpoch) external {
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

    /// Verifies the header proof's quorum, that the block is from the admin chain at the current
    /// epoch, and that `transactionBcs` (the per-transaction BCS encodings) are the transactions the
    /// header commits to. Returns the header's height, so the committee created by this admin
    /// block can record it.
    function _verifyAdminTransactions(bytes calldata blockProof, bytes[] calldata transactionBcs)
        internal
        view
        returns (uint64 height)
    {
        (BridgeTypes.BlockProof memory proof, bytes32 blockHash) = _deserializeAndHash(blockProof);
        _verifyQuorum(blockHash, proof.header.epoch.value, proof.round, proof.signatures);

        require(proof.header.chain_id.value.value == adminChainId, "block must be from admin chain");
        require(proof.header.epoch.value == currentEpoch, "block epoch must match current epoch");
        require(
            _hashTransactionsFromBcs(transactionBcs) == proof.header.transactions_hash.value,
            "transactions do not match header"
        );
        height = proof.header.height.value;
    }

    /// Scans the per-transaction BCS encodings for a `CreateCommittee` admin operation, returning
    /// its epoch and blob hash if found.
    function _findCreateCommittee(bytes[] calldata transactionBcs)
        internal
        pure
        returns (bool found, uint32 newEpoch, bytes32 expectedBlobHash)
    {
        for (uint256 i = 0; i < transactionBcs.length; i++) {
            BridgeTypes.Transaction memory txn = BridgeTypes.bcs_deserialize_Transaction(transactionBcs[i]);
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
    }

    /// Recomputes `transactions_hash` (`hash_vec`) from the per-transaction BCS encodings: each
    /// hashes to `keccak256("Transaction::" ++ transactionBcs[i])`, then the leaves fold once.
    function _hashTransactionsFromBcs(bytes[] calldata transactionBcs) internal pure returns (bytes32) {
        BridgeTypes.CryptoHash[] memory leaves = new BridgeTypes.CryptoHash[](transactionBcs.length);
        for (uint256 i = 0; i < transactionBcs.length; i++) {
            leaves[i] = BridgeTypes.CryptoHash(keccak256(abi.encodePacked("Transaction::", transactionBcs[i])));
        }
        return _hashCryptoHashVec(leaves);
    }

    /// Verifies a block's signatures from its header and records its `events_hash`, so that
    /// individual events can later be proven against it (via `verifyEventInclusion`) without
    /// re-checking the whole certificate. Returns the block hash
    /// (`keccak256("BlockHeader::" ++ BCS(header))`).
    function registerBlock(bytes calldata blockProof) external returns (bytes32) {
        (BridgeTypes.BlockProof memory proof, bytes32 blockHash) = _deserializeAndHash(blockProof);
        _verifyQuorum(blockHash, proof.header.epoch.value, proof.round, proof.signatures);
        registeredBlocks[blockHash] = RegisteredBlock(
            proof.header.events_hash.value, proof.header.height.value, proof.header.chain_id.value.value
        );
        return blockHash;
    }

    /// Proves that the events whose canonical BCS encodings are `eventBcs` sit at `positions`
    /// (ascending) within transaction `txIndex` of the block registered under `blockHash`. Reverts
    /// unless they fold — with the supplied sibling hashes — to the block's registered
    /// `events_hash`. `numTxs`/`numEventsInTx` are the outer/inner vector lengths; `innerSiblings`
    /// are the leaf hashes of the unproven events in `txIndex` (position order) and `outerSiblings`
    /// the per-transaction hashes of the other transactions (transaction order). This lets a caller
    /// settle a subset of a block's events without re-hashing the whole block.
    function verifyEventInclusion(
        bytes32 blockHash,
        bytes[] calldata eventBcs,
        uint32 txIndex,
        uint32 numTxs,
        uint32 numEventsInTx,
        uint32[] calldata positions,
        bytes32[] calldata innerSiblings,
        bytes32[] calldata outerSiblings
    ) external view {
        bytes32 eventsHash = registeredBlocks[blockHash].eventsHash;
        require(eventsHash != 0, "block not registered");
        require(txIndex < numTxs, "txIndex out of range");
        require(eventBcs.length == positions.length, "events/positions length mismatch");
        require(positions.length + innerSiblings.length == numEventsInTx, "inner sibling count mismatch");
        require(outerSiblings.length + 1 == numTxs, "outer sibling count mismatch");

        bytes32[] memory provenLeaves = _eventLeaves(eventBcs);
        bytes32 txHash = _foldTransactionEvents(provenLeaves, positions, innerSiblings, numEventsInTx);
        bytes32 computed = _foldEventsHash(txHash, txIndex, numTxs, outerSiblings);
        require(computed == eventsHash, "event inclusion proof failed");
    }

    /// Leaf hash of each proven event: `keccak256("Event::" ++ BCS(event))`.
    function _eventLeaves(bytes[] calldata eventBcs) internal pure returns (bytes32[] memory leaves) {
        leaves = new bytes32[](eventBcs.length);
        for (uint256 i = 0; i < eventBcs.length; i++) {
            leaves[i] = keccak256(abi.encodePacked("Event::", eventBcs[i]));
        }
    }

    /// Reconstructs transaction `txIndex`'s event hash (`hash_vec` over its event leaves): proven
    /// positions take their leaf from `provenLeaves`, the rest from `innerSiblings`. The cursor walk
    /// enforces ascending, in-range `positions` (every position must be consumed in order).
    function _foldTransactionEvents(
        bytes32[] memory provenLeaves,
        uint32[] calldata positions,
        bytes32[] calldata innerSiblings,
        uint32 numEventsInTx
    ) internal pure returns (bytes32) {
        BridgeTypes.CryptoHash[] memory innerLeaves = new BridgeTypes.CryptoHash[](numEventsInTx);
        uint256 provenCursor = 0;
        uint256 innerCursor = 0;
        for (uint32 p = 0; p < numEventsInTx; p++) {
            if (provenCursor < positions.length && positions[provenCursor] == p) {
                innerLeaves[p] = BridgeTypes.CryptoHash(provenLeaves[provenCursor]);
                provenCursor++;
            } else {
                innerLeaves[p] = BridgeTypes.CryptoHash(innerSiblings[innerCursor]);
                innerCursor++;
            }
        }
        require(provenCursor == positions.length, "position out of range or unsorted");
        return _hashCryptoHashVec(innerLeaves);
    }

    /// Reconstructs the block's `events_hash` (`hash_vec_vec`) from transaction `txIndex`'s
    /// recomputed event hash and the per-transaction `outerSiblings` for the other transactions.
    function _foldEventsHash(bytes32 txHash, uint32 txIndex, uint32 numTxs, bytes32[] calldata outerSiblings)
        internal
        pure
        returns (bytes32)
    {
        BridgeTypes.CryptoHash[] memory outer = new BridgeTypes.CryptoHash[](numTxs);
        uint256 outerCursor = 0;
        for (uint32 j = 0; j < numTxs; j++) {
            if (j == txIndex) {
                outer[j] = BridgeTypes.CryptoHash(txHash);
            } else {
                outer[j] = BridgeTypes.CryptoHash(outerSiblings[outerCursor]);
                outerCursor++;
            }
        }
        return _hashCryptoHashVec(outer);
    }

    /// Deserializes a `BlockProof` and computes its block hash. The block hash is
    /// `keccak256("BlockHeader::" ++ BCS(header))`; since the header is the first field, its BCS
    /// bytes are already in calldata, so we hash that slice directly rather than re-serializing
    /// the just-deserialized header.
    function _deserializeAndHash(bytes calldata blockProof)
        internal
        pure
        returns (BridgeTypes.BlockProof memory proof, bytes32 blockHash)
    {
        bytes memory mdata = blockProof;
        uint256 pos;
        BridgeTypes.BlockHeader memory header;
        (pos, header) = BridgeTypes.bcs_deserialize_offset_BlockHeader(0, mdata);
        blockHash = keccak256(abi.encodePacked("BlockHeader::", blockProof[0:pos]));

        BridgeTypes.Round memory round;
        (pos, round) = BridgeTypes.bcs_deserialize_offset_Round(pos, mdata);
        BridgeTypes.tuple_Secp256k1PublicKey_Secp256k1Signature[] memory signatures;
        (pos, signatures) =
            BridgeTypes.bcs_deserialize_offset_seq_tuple_Secp256k1PublicKey_Secp256k1Signature(pos, mdata);
        require(pos == mdata.length, "incomplete deserialization");

        proof = BridgeTypes.BlockProof(header, round, signatures);
    }

    /// Verifies that `signatures` form a quorum of the `epoch` committee over the block whose hash
    /// is `blockHash`.
    function _verifyQuorum(
        bytes32 blockHash,
        uint32 epoch,
        BridgeTypes.Round memory round,
        BridgeTypes.tuple_Secp256k1PublicKey_Secp256k1Signature[] memory signatures
    ) internal view {
        // Construct VoteValue BCS and hash with type name prefix
        // CryptoHash::new(&VoteValue(...)) = keccak256("VoteValue::" ++ BCS(VoteValue))
        BridgeTypes.VoteValue memory voteValue =
            BridgeTypes.VoteValue(BridgeTypes.CryptoHash(blockHash), round, BridgeTypes.CertificateKind.Confirmed);
        bytes32 signedHash = keccak256(abi.encodePacked("VoteValue::", BridgeTypes.bcs_serialize_VoteValue(voteValue)));

        // Verify signatures against the block's epoch committee
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
            assembly ("memory-safe") {
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
    }

    /// keccak256("CryptoHashVec::" ++ BCS(Vec<CryptoHash>)).
    function _hashCryptoHashVec(BridgeTypes.CryptoHash[] memory hashes) internal pure returns (bytes32) {
        return keccak256(abi.encodePacked("CryptoHashVec::", BridgeTypes.bcs_serialize_seq_CryptoHash(hashes)));
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

    // secp256k1 curve order
    uint256 private constant SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141;

    /// Parses a BCS-serialized committee blob and derives Ethereum addresses
    /// directly from the uncompressed validator keys it contains.
    /// Returns (addresses, weights) extracted from the blob.
    function _parseCommitteeBlob(bytes memory blob) internal pure returns (address[] memory, uint64[] memory) {
        uint256 pos;
        uint256 count;
        (pos, count) = BridgeTypes.bcs_deserialize_offset_uleb128(0, blob);

        address[] memory addrs = new address[](count);
        uint64[] memory weights = new uint64[](count);

        for (uint256 i = 0; i < count; i++) {
            // Validator key: 65-byte uncompressed SEC1 encoding (0x04 ++ x ++ y)
            require(pos + 65 <= blob.length, "truncated validator key");
            require(uint8(blob[pos]) == 0x04, "invalid uncompressed key prefix");

            // Derive the Ethereum address: keccak256 of the 64-byte x ++ y
            bytes32 keyHash;
            assembly ("memory-safe") {
                keyHash := keccak256(add(add(blob, 33), pos), 64)
            }
            addrs[i] = address(uint160(uint256(keyHash)));
            pos += 65;

            // Skip network_address (ULEB128 length-prefixed string)
            uint256 strLen;
            (pos, strLen) = BridgeTypes.bcs_deserialize_offset_uleb128(pos, blob);
            pos += strLen;

            // Read votes (u64 LE)
            weights[i] = _readU64LE(blob, pos);
            pos += 8;

            // Skip account_public_key (enum: 1-byte tag + payload)
            uint8 tag = uint8(blob[pos]);
            pos += 1;
            if (tag == 0) {
                pos += 32; // Ed25519: 32 bytes
            } else if (tag == 1) {
                pos += 65; // Secp256k1: 65 bytes (uncompressed)
            } else {
                pos += 33; // EvmSecp256k1: 33 bytes (compressed)
            }
        }

        return (addrs, weights);
    }

    /// Reads 8 bytes from data at pos as a little-endian uint64.
    function _readU64LE(bytes memory data, uint256 pos) internal pure returns (uint64) {
        uint64 result = 0;
        for (uint256 i = 0; i < 8; i++) {
            result |= uint64(uint8(data[pos + i])) << (i * 8);
        }
        return result;
    }
}
