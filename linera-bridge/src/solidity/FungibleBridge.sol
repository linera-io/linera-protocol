// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "BridgeTypes.sol";
import "WrappedFungibleTypes.sol";
import "Microchain.sol";

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

    // WrappedFungible application ID on Linera,
    // used to identify Burn events in the block stream.
    bytes32 public immutable fungibleApplicationId;
    // The ERC-20 token being bridged.
    IERC20 public immutable token;
    uint256 public depositNonce;

    /// Per-burn dedup keyed by `keccak256(abi.encode(height, eventIndex))`
    /// where `eventIndex` is the underlying Linera `Event.index` — the
    /// position of the burn event within its stream. Set inside
    /// `_onBlock` after the burn's `token.transfer` succeeds.
    mapping(bytes32 => bool) internal processedBurns;

    constructor(address _lightClient, bytes32 _chainId, address _token, bytes32 _fungibleApplicationId)
        Microchain(_lightClient, _chainId)
    {
        require(_fungibleApplicationId != bytes32(0), "fungibleApplicationId must be non-zero");
        token = IERC20(_token);
        fungibleApplicationId = _fungibleApplicationId;
    }

    /// Returns whether the burn at `(height, eventIndex)` has already been
    /// released by a prior `addBlock` call. `eventIndex` matches
    /// `Event.index` from the Linera block body — the same value the
    /// off-chain relayer pulls from the certificate.
    function isBurnProcessed(uint64 height, uint32 eventIndex) external view returns (bool) {
        return processedBurns[keccak256(abi.encode(height, eventIndex))];
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
    ) external {
        require(amount > 0, "amount=0");
        require(target_application_id == fungibleApplicationId, "target application mismatch");

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

    /// Processes a Linera block and releases ERC-20 tokens for any BurnEvent
    /// events on the "burns" stream from the wrapped-fungible application.
    /// Idempotent: each burn's release is gated on
    /// `processedBurns[keccak256(abi.encode(height, evt.index))]`, so
    /// re-submitting the same cert is a no-op for burns already released
    /// by a prior call.
    function _onBlock(BridgeTypes.Block memory blockValue) internal override {
        bytes32 burnsHash = keccak256("burns");
        uint64 height = blockValue.header.height.value;
        for (uint256 i = 0; i < blockValue.body.events.length; i++) {
            BridgeTypes.Event[] memory txEvents = blockValue.body.events[i];
            for (uint256 j = 0; j < txEvents.length; j++) {
                BridgeTypes.Event memory evt = txEvents[j];

                // choice==1 is User application
                if (evt.stream_id.application_id.choice != 1) continue;
                if (evt.stream_id.application_id.user.application_description_hash.value != fungibleApplicationId) {
                    continue;
                }

                // Check stream name is "burns"
                if (keccak256(evt.stream_id.stream_name.value) != burnsHash) continue;

                bytes32 key = keccak256(abi.encode(height, evt.index));
                if (processedBurns[key]) continue;

                WrappedFungibleTypes.BurnEvent memory burnEvt =
                    WrappedFungibleTypes.bcs_deserialize_BurnEvent(evt.value);
                address target = address(burnEvt.target);
                require(token.transfer(target, burnEvt.amount.value), "token transfer failed");
                processedBurns[key] = true;
            }
        }
    }

    /// Processes burns at the requested `eventPositionsInTx` positions
    /// within transaction `txIndex` of `cert`. Verifies the cert once
    /// and uses direct array access (`body.events[txIndex][pos]`) for
    /// every burn — no nested-loop scan. The off-chain relayer uses
    /// this when `addBlock(cert)` would not fit in a single EVM tx,
    /// chunking burns per-tx-then-by-gas.
    ///
    /// Reverts (atomically — no `processedBurns` flag is set if the call
    /// reverts) on:
    /// - `txIndex` out of range (`"txIndex out of range"`)
    /// - any position out of range (`"eventPos out of range"`)
    /// - any position whose event is not a matching burn for this app
    ///   (`"not a matching burn"`)
    /// - any burn already processed (`"burn already processed"`)
    /// - any failed `token.transfer` (`"token transfer failed"`)
    function processBurns(
        bytes calldata data,
        uint32 txIndex,
        uint32[] calldata eventPositionsInTx
    ) external {
        (BridgeTypes.Block memory blockValue, ) = lightClient.verifyBlock(data);
        require(blockValue.header.chain_id.value.value == chainId, "chain id mismatch");
        require(txIndex < blockValue.body.events.length, "txIndex out of range");

        uint64 height = blockValue.header.height.value;
        bytes32 burnsHash = keccak256("burns");
        BridgeTypes.Event[] memory txEvents = blockValue.body.events[txIndex];

        for (uint256 k = 0; k < eventPositionsInTx.length; k++) {
            uint32 pos = eventPositionsInTx[k];
            require(pos < txEvents.length, "eventPos out of range");
            BridgeTypes.Event memory evt = txEvents[pos];

            if (evt.stream_id.application_id.choice != 1) {
                revert("not a matching burn");
            }
            if (evt.stream_id.application_id.user.application_description_hash.value
                    != fungibleApplicationId) {
                revert("not a matching burn");
            }
            if (keccak256(evt.stream_id.stream_name.value) != burnsHash) {
                revert("not a matching burn");
            }

            bytes32 key = keccak256(abi.encode(height, evt.index));
            require(!processedBurns[key], "burn already processed");

            WrappedFungibleTypes.BurnEvent memory burnEvt =
                WrappedFungibleTypes.bcs_deserialize_BurnEvent(evt.value);
            require(
                token.transfer(address(burnEvt.target), burnEvt.amount.value),
                "token transfer failed"
            );
            processedBurns[key] = true;
        }
    }

    /// @dev Calls transferFrom and handles tokens that don't return a boolean.
    function _safeTransferFrom(address from, address to, uint256 amount_) internal {
        (bool success, bytes memory data) =
            address(token).call(abi.encodeWithSelector(token.transferFrom.selector, from, to, amount_));
        require(success && (data.length == 0 || abi.decode(data, (bool))), "safeTransferFrom failed");
    }
}
