// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "Microchain.sol";
import "IBurnEventDecoder.sol";

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

/// Bridges ERC20 tokens between Linera and EVM.
/// Linera→EVM: processes Burn operations from Linera blocks and releases ERC-20 tokens.
/// EVM→Linera: accepts deposits via deposit() and emits DepositInitiated events.
contract FungibleBridge is Microchain {
    /// Emitted when a user deposits ERC-20 tokens for bridging to Linera.
    /// The off-chain relayer uses this event (plus an MPT receipt proof) to
    /// mint the corresponding tokens on the target Linera chain.
    event DepositInitiated(
        address indexed depositor,
        uint256 source_chain_id,
        bytes32 target_chain_id,
        bytes32 target_application_id,
        bytes32 target_account_owner,
        address token,
        uint256 amount,
        uint256 nonce
    );

    /// Emitted when a Linera burn is released as an ERC-20 transfer to
    /// `target`. `height` and `eventIndex` together identify the burn in
    /// the source Linera block (matches the on-chain dedup key).
    event BurnReleased(uint64 indexed height, uint32 indexed eventIndex, address indexed target, uint256 amount);

    // WrappedFungible application ID on Linera. Required deposit target
    // (see `deposit`), on whose behalf the released ERC-20 is bridged.
    bytes32 public immutable fungibleApplicationId;
    // EVM bridge application ID on Linera, whose "burns" stream this contract
    // matches Burn events against when releasing ERC-20 tokens.
    bytes32 public immutable bridgeApplicationId;
    // The ERC-20 token being bridged.
    IERC20 public immutable token;
    uint256 public depositNonce;

    // Decodes a burn event's payload (BridgeTypes.Event.value) into
    // (recipient, amount). Swappable via the timelocked decoder-update flow
    // below so a fungible-app BurnEvent schema change (same applicationId) needs
    // no TVL migration, using the proposer/canceller/timelockDelay governance
    // inherited from Microchain. (The light client itself is immutable on this
    // network — see Microchain.)
    IBurnEventDecoder public decoder;
    IBurnEventDecoder public pendingDecoder;
    uint256 public pendingDecoderReadyAt;

    /// Per-burn dedup keyed by
    /// `keccak256(abi.encode(bridgeApplicationId, height, eventIndex))`
    /// where `eventIndex` is the underlying Linera `Event.index` — the
    /// position of the burn event within its stream. Set inside
    /// `_onBlock` after the burn's `token.transfer` succeeds.
    mapping(bytes32 => bool) internal processedBurns;

    constructor(
        address _lightClient,
        bytes32 _chainId,
        address _token,
        bytes32 _fungibleApplicationId,
        bytes32 _bridgeApplicationId,
        address _initialDecoder,
        address _pauseGuardian,
        address _proposer,
        address _canceller,
        uint256 _timelockDelay
    ) Microchain(_lightClient, _chainId, _pauseGuardian, _proposer, _canceller, _timelockDelay) {
        require(_fungibleApplicationId != bytes32(0), "fungibleApplicationId must be non-zero");
        require(_bridgeApplicationId != bytes32(0), "bridgeApplicationId must be non-zero");
        require(_initialDecoder != address(0), "zero decoder");
        token = IERC20(_token);
        fungibleApplicationId = _fungibleApplicationId;
        bridgeApplicationId = _bridgeApplicationId;
        decoder = IBurnEventDecoder(_initialDecoder);
    }

    /// Returns whether the burn at `(height, eventIndex)` has already been
    /// released by a prior `addBlock` call. `eventIndex` matches
    /// `Event.index` from the Linera block body — the same value the
    /// off-chain relayer pulls from the certificate.
    function isBurnProcessed(uint64 height, uint32 eventIndex) external view returns (bool) {
        return processedBurns[_burnKey(height, eventIndex)];
    }

    /// Locks ERC-20 tokens in the bridge and emits a DepositInitiated event.
    /// Caller must first approve this contract to spend `amount` tokens.
    /// The emitted event is consumed by the off-chain relayer to produce an
    /// MPT proof that the Linera bridge app verifies before minting.
    function deposit(
        bytes32 target_chain_id,
        bytes32 target_application_id,
        bytes32 target_account_owner,
        uint256 amount
    ) external whenNotEmergencyPaused {
        require(amount > 0, "amount=0");
        require(target_application_id == fungibleApplicationId, "target application mismatch");
        // The Linera side holds amounts as U128 and mints exactly `amount`, so a
        // deposit above u128::MAX could never be minted — reject it at lock time
        // instead of locking ERC-20 that can never be bridged or refunded.
        require(amount <= type(uint128).max, "amount exceeds u128");

        uint256 before = token.balanceOf(address(this));
        _safeTransferFrom(msg.sender, address(this), amount);
        uint256 received = token.balanceOf(address(this)) - before;
        require(received == amount, "fee-on-transfer tokens unsupported");

        uint256 currentNonce = depositNonce++;

        emit DepositInitiated(
            msg.sender,
            block.chainid,
            target_chain_id,
            target_application_id,
            target_account_owner,
            address(token),
            amount,
            currentNonce
        );
    }

<<<<<<< HEAD
    /// Processes a Linera block and releases ERC-20 tokens for any BurnEvent
    /// events on the "burns" stream from the bridge application.
    /// Idempotent: each burn's release is gated on
    /// `processedBurns[keccak256(abi.encode(bridgeApplicationId, height, evt.index))]`, so
    /// re-submitting the same cert is a no-op for burns already released
    /// by a prior call.
    function _onBlock(BridgeTypes.Block memory blockValue) internal override {
        bytes32 burnsHash = keccak256("burns");
        uint64 height = blockValue.header.height.value;
        for (uint256 i = 0; i < blockValue.body.events.length; i++) {
            BridgeTypes.Event[] memory txEvents = blockValue.body.events[i];
            for (uint256 j = 0; j < txEvents.length; j++) {
                BridgeTypes.Event memory evt = txEvents[j];
                if (!_isMatchingBurn(evt, burnsHash)) continue;

                bytes32 key = _burnKey(height, evt.index);
                if (processedBurns[key]) continue;

                _releaseBurn(evt, key, height);
            }
        }
=======
    /// Releases the burns whose canonical BCS encodings are `eventBcs`, after proving they sit at
    /// `positions` within transaction `txIndex` of the block registered under `blockHash` (see
    /// `LightClient.proveEventsCommitted`). The off-chain relayer registers the block once, then
    /// settles burns in chunks, each proving only its own events instead of re-verifying the whole
    /// certificate.
    ///
    /// Idempotent: burns already in `processedBurns` are skipped silently rather than reverted, so
    /// the relayer can recover from overlap with a racing/retrying `processBurns`.
    ///
    /// Reverts (atomically — no `processedBurns` flag is set if the call reverts) on:
    /// - empty `positions` (`"empty positions"`)
    /// - a block that was never registered (`"block not registered"`)
    /// - a block registered for a different chain (`"chain id mismatch"`)
    /// - a failed inclusion proof: events/siblings do not fold to the block's `events_hash`
    ///   (`"event inclusion proof failed"`)
    /// - any event that is not a matching burn for this app (`"not a matching burn"`)
    /// - any failed `token.transfer` (`"safeTransfer failed"`)
    function processBurns(
        bytes32 blockHash,
        bytes[] calldata eventBcs,
        uint32 txIndex,
        uint32 numTxs,
        uint32 numEventsInTx,
        uint32[] calldata positions,
        bytes32[] calldata siblings
    ) external {
        require(positions.length > 0, "empty positions");

        // The block must have been registered (its signatures were checked once, then); fetch its
        // committed events hash and metadata, then prove these events are part of it.
        (bytes32 eventsHash, uint64 height, bytes32 blockChainId,) = lightClient.registeredBlocks(blockHash);
        require(eventsHash != 0, "block not registered");
        require(blockChainId == chainId, "chain id mismatch");
        lightClient.proveEventsCommitted(eventsHash, eventBcs, txIndex, numTxs, numEventsInTx, positions, siblings);

        _releaseBurns(eventBcs, height);
>>>>>>> 22c1ee41d1 (Extract new committee rotation from an event, not operation (#6482))
    }

    /// Processes burns at the requested `eventPositionsInTx` positions
    /// within transaction `txIndex` of `cert`. Verifies the cert once
    /// and uses direct array access (`body.events[txIndex][pos]`) for
    /// every burn — no nested-loop scan. The off-chain relayer uses
    /// this when `addBlock(cert)` would not fit in a single EVM tx,
    /// chunking burns per-tx-then-by-gas.
    ///
    /// Idempotent like `_onBlock`: positions already in `processedBurns` are
    /// skipped silently rather than reverted. Lets the relayer recover from
    /// overlap with a prior `addBlock` (or a racing/retrying `processBurns`)
    /// instead of losing the whole chunk to a single duplicate.
    ///
    /// Reverts (atomically — no `processedBurns` flag is set if the call
    /// reverts) on:
    /// - empty `eventPositionsInTx` (`"empty positions"`)
    /// - `txIndex` out of range (`"txIndex out of range"`)
    /// - any position out of range (`"eventPos out of range"`)
    /// - any position whose event is not a matching burn for this app
    ///   (`"not a matching burn"`)
    /// - any failed `token.transfer` (`"safeTransfer failed"`)
    function processBurns(bytes calldata data, uint32 txIndex, uint32[] calldata eventPositionsInTx)
        external
        whenNotEmergencyPaused
    {
        require(eventPositionsInTx.length > 0, "empty positions");
        (BridgeTypes.Block memory blockValue,) = lightClient.verifyBlock(data);
        require(blockValue.header.chain_id.value.value == chainId, "chain id mismatch");
        require(txIndex < blockValue.body.events.length, "txIndex out of range");

        uint64 height = blockValue.header.height.value;
        bytes32 burnsHash = keccak256("burns");
        BridgeTypes.Event[] memory txEvents = blockValue.body.events[txIndex];

        for (uint256 k = 0; k < eventPositionsInTx.length; k++) {
            uint32 pos = eventPositionsInTx[k];
            require(pos < txEvents.length, "eventPos out of range");
            BridgeTypes.Event memory evt = txEvents[pos];
            require(_isMatchingBurn(evt, burnsHash), "not a matching burn");

            bytes32 key = _burnKey(height, evt.index);
            if (processedBurns[key]) continue;

            _releaseBurn(evt, key, height);
        }
    }

    /// Dedup key for a burn at `(height, eventIndex)`. `eventIndex` is the
    /// underlying Linera `Event.index`. `bridgeApplicationId` is folded in so
    /// the key is self-describing: dedup correctness no longer rests on the
    /// implicit invariant that this contract only ever consumes the bridge
    /// app's "burns" stream — if a future change let it match more than one
    /// stream, keys from different apps could not collide.
    function _burnKey(uint64 height, uint32 eventIndex) private view returns (bytes32) {
        return keccak256(abi.encode(bridgeApplicationId, height, eventIndex));
    }

    /// Returns true if `evt` belongs to the configured bridge application's
    /// "burns" stream.
    function _isMatchingBurn(BridgeTypes.Event memory evt, bytes32 burnsHash) private view returns (bool) {
        // choice == 1 is User application
        if (evt.stream_id.application_id.choice != 1) return false;
        if (evt.stream_id.application_id.user.application_description_hash.value != bridgeApplicationId) {
            return false;
        }
        if (keccak256(evt.stream_id.stream_name.value) != burnsHash) return false;
        return true;
    }

    /// Releases the ERC-20 tokens for the burn described by `evt`, decoding its
    /// payload via the swappable `decoder`. Sets the dedup flag BEFORE the
    /// external `token.transfer` call (checks-effects-interactions) so a
    /// malicious token that re-enters `addBlock` / `processBurns` cannot trigger
    /// a second release. The decoder is `pure`, so the only way execution escapes
    /// here is the `token.transfer` — no reentrancy via the decoder.
    function _releaseBurn(BridgeTypes.Event memory evt, bytes32 key, uint64 height) private {
        (address target, uint256 amount) = decoder.decodeBurnEvent(evt.value);
        processedBurns[key] = true;
        _safeTransfer(target, amount);
        emit BurnReleased(height, evt.index, target, amount);
    }

    /// @dev Calls transfer and handles tokens that don't return a boolean.
    function _safeTransfer(address to, uint256 amount_) internal {
        (bool success, bytes memory data) =
            address(token).call(abi.encodeWithSelector(token.transfer.selector, to, amount_));
        require(success && (data.length == 0 || abi.decode(data, (bool))), "safeTransfer failed");
    }

    /// @dev Calls transferFrom and handles tokens that don't return a boolean.
    function _safeTransferFrom(address from, address to, uint256 amount_) internal {
        (bool success, bytes memory data) =
            address(token).call(abi.encodeWithSelector(token.transferFrom.selector, from, to, amount_));
        require(success && (data.length == 0 || abi.decode(data, (bool))), "safeTransferFrom failed");
    }

    // --- Decoder update (timelocked) ---
    //
    // Proposer proposes a new decoder; anyone may execute it once timelockDelay
    // elapses; canceller (or proposer) may cancel a pending update. Swaps the
    // BurnEvent-payload schema without migrating TVL.

    event DecoderUpdateProposed(address indexed newDecoder, uint256 readyAt);
    event DecoderUpdateExecuted(address indexed oldDecoder, address indexed newDecoder);
    event DecoderUpdateCancelled(address indexed proposed);

    function proposeDecoderUpdate(address newDecoder) external onlyProposer {
        require(newDecoder != address(0), "zero address");
        require(newDecoder != address(decoder), "no-op update");
        require(address(pendingDecoder) == address(0), "update already pending");

        pendingDecoder = IBurnEventDecoder(newDecoder);
        pendingDecoderReadyAt = block.timestamp + timelockDelay;
        emit DecoderUpdateProposed(newDecoder, pendingDecoderReadyAt);
    }

    function executeDecoderUpdate() external {
        require(address(pendingDecoder) != address(0), "no pending update");
        require(block.timestamp >= pendingDecoderReadyAt, "delay not elapsed");
        address old = address(decoder);
        decoder = pendingDecoder;
        emit DecoderUpdateExecuted(old, address(pendingDecoder));
        pendingDecoder = IBurnEventDecoder(address(0));
        pendingDecoderReadyAt = 0;
    }

    function cancelDecoderUpdate() external {
        require(msg.sender == canceller || msg.sender == proposer, "not authorized");
        require(address(pendingDecoder) != address(0), "no pending update");
        address proposed = address(pendingDecoder);
        pendingDecoder = IBurnEventDecoder(address(0));
        pendingDecoderReadyAt = 0;
        emit DecoderUpdateCancelled(proposed);
    }
}
