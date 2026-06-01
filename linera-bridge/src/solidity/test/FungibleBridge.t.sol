// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test, Vm} from "forge-std/Test.sol";
import {FungibleBridge} from "../FungibleBridge.sol";
import {BridgeTypes} from "../BridgeTypes.sol";
import {WrappedFungibleTypes} from "../WrappedFungibleTypes.sol";
import {LineraToken} from "../LineraToken.sol";

// ------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------

bytes32 constant CHAIN_ID = bytes32(uint256(0xC1));
uint64 constant HEIGHT = 42;
uint32 constant TX = 0;
uint128 constant AMOUNT = 1_000_000_000_000_000_000; // 1e18
address constant RECIP_0 = address(0xA0);
address constant RECIP_1 = address(0xA1);
address constant RECIP_2 = address(0xA2);
bytes32 constant APP_ID = bytes32(uint256(0xF00D));

// Stable test source — chain id and Address32 owner — used by the mocks
// for the burn event's `source: Account` field. Tests that assert on the
// emitted BurnBlocked payload compare against these.
bytes32 constant SOURCE_CHAIN_ID = bytes32(uint256(0xC2));
bytes32 constant SOURCE_OWNER_ADDR32 = bytes32(uint256(0xBA5EBA11));

// ------------------------------------------------------------------
// MockLightClientForBurns
//
// Returns a Block that has `numBurns` matching burn events at
// tx-slot `txIndexUsed` (preceding tx-slots are empty).
// Stream indices for the burns are 5, 6, ..., 4+numBurns.
// ------------------------------------------------------------------
contract MockLightClientForBurns {
    bytes32 public immutable chainIdRet;
    uint64 public immutable heightRet;
    uint32 public immutable txIndexUsed;
    bytes32 public immutable fungibleAppIdRet;
    uint32 public immutable numBurns;
    uint128 public immutable amountPerBurn;
    address public immutable recipBase;

    constructor(
        bytes32 _chainId,
        uint64 _height,
        uint32 _txIndex,
        bytes32 _fungibleAppId,
        uint32 _numBurns,
        uint128 _amountPerBurn,
        address _recipBase
    ) {
        chainIdRet = _chainId;
        heightRet = _height;
        txIndexUsed = _txIndex;
        fungibleAppIdRet = _fungibleAppId;
        numBurns = _numBurns;
        amountPerBurn = _amountPerBurn;
        recipBase = _recipBase;
    }

    function verifyBlock(bytes calldata) external view returns (BridgeTypes.Block memory b, bytes32 sigHash) {
        b.header.chain_id.value.value = chainIdRet;
        b.header.height.value = heightRet;

        // Allocate txIndexUsed + 1 tx-slots; all before txIndexUsed are empty.
        b.body.events = new BridgeTypes.Event[][](uint256(txIndexUsed) + 1);
        b.body.events[txIndexUsed] = new BridgeTypes.Event[](numBurns);

        for (uint32 i = 0; i < numBurns; i++) {
            BridgeTypes.Event memory evt;
            evt.stream_id.application_id.choice = 1; // User
            evt.stream_id.application_id.user.application_description_hash.value = fungibleAppIdRet;
            evt.stream_id.stream_name.value = bytes("burns");
            evt.index = 5 + i; // stream index differs from positional index
            evt.value = _encodeBurn(address(uint160(recipBase) + i), amountPerBurn);
            b.body.events[txIndexUsed][i] = evt;
        }

        sigHash = bytes32(uint256(0x1234));
    }

    function _encodeBurn(address target, uint128 amount) private pure returns (bytes memory) {
        WrappedFungibleTypes.BurnEvent memory burnEvt;
        burnEvt.source.chain_id.value.value = SOURCE_CHAIN_ID;
        // Address32 variant of AccountOwner (choice index 1 per BridgeTypes).
        burnEvt.source.owner.choice = 1;
        burnEvt.source.owner.address32.value = SOURCE_OWNER_ADDR32;
        burnEvt.target = bytes20(target);
        burnEvt.amount = amount;
        return WrappedFungibleTypes.bcs_serialize_BurnEvent(burnEvt);
    }
}

// ------------------------------------------------------------------
// MockLightClientForNonBurn
//
// Returns a Block whose single event has stream_name == "deposits"
// (not "burns"), so FungibleBridge.processBurns must reject it.
// ------------------------------------------------------------------
contract MockLightClientForNonBurn {
    bytes32 public immutable chainIdRet;
    uint64 public immutable heightRet;
    bytes32 public immutable fungibleAppIdRet;
    uint128 public immutable amountPerBurn;
    address public immutable recipBase;

    constructor(bytes32 _chainId, uint64 _height, bytes32 _fungibleAppId, uint128 _amountPerBurn, address _recipBase) {
        chainIdRet = _chainId;
        heightRet = _height;
        fungibleAppIdRet = _fungibleAppId;
        amountPerBurn = _amountPerBurn;
        recipBase = _recipBase;
    }

    function verifyBlock(bytes calldata) external view returns (BridgeTypes.Block memory b, bytes32 sigHash) {
        b.header.chain_id.value.value = chainIdRet;
        b.header.height.value = heightRet;

        b.body.events = new BridgeTypes.Event[][](1);
        b.body.events[0] = new BridgeTypes.Event[](1);

        BridgeTypes.Event memory evt;
        evt.stream_id.application_id.choice = 1;
        evt.stream_id.application_id.user.application_description_hash.value = fungibleAppIdRet;
        // Wrong stream name — should cause "not a matching burn"
        evt.stream_id.stream_name.value = bytes("deposits");
        evt.index = 5;
        WrappedFungibleTypes.BurnEvent memory burnEvt;
        burnEvt.source.chain_id.value.value = SOURCE_CHAIN_ID;
        burnEvt.source.owner.choice = 1;
        burnEvt.source.owner.address32.value = SOURCE_OWNER_ADDR32;
        burnEvt.target = bytes20(recipBase);
        burnEvt.amount = amountPerBurn;
        evt.value = WrappedFungibleTypes.bcs_serialize_BurnEvent(burnEvt);
        b.body.events[0][0] = evt;

        sigHash = bytes32(uint256(0x1234));
    }
}

// ------------------------------------------------------------------
// MockLightClientWithOwner
//
// Like MockLightClientForBurns but with a fully parametric AccountOwner
// so each variant can be exercised independently.
// ------------------------------------------------------------------
contract MockLightClientWithOwner {
    bytes32 public immutable chainIdRet;
    uint64 public immutable heightRet;
    uint32 public immutable txIndexUsed;
    bytes32 public immutable fungibleAppIdRet;
    uint128 public immutable amountPerBurn;
    address public immutable recipBase;
    uint8 public immutable ownerChoice;
    uint8 public immutable ownerReserved;
    bytes32 public immutable ownerAddr32;
    bytes20 public immutable ownerAddr20;

    constructor(
        bytes32 _chainId,
        uint64 _height,
        uint32 _txIndex,
        bytes32 _fungibleAppId,
        uint128 _amountPerBurn,
        address _recipBase,
        uint8 _ownerChoice,
        uint8 _ownerReserved,
        bytes32 _ownerAddr32,
        bytes20 _ownerAddr20
    ) {
        chainIdRet = _chainId;
        heightRet = _height;
        txIndexUsed = _txIndex;
        fungibleAppIdRet = _fungibleAppId;
        amountPerBurn = _amountPerBurn;
        recipBase = _recipBase;
        ownerChoice = _ownerChoice;
        ownerReserved = _ownerReserved;
        ownerAddr32 = _ownerAddr32;
        ownerAddr20 = _ownerAddr20;
    }

    function verifyBlock(bytes calldata) external view returns (BridgeTypes.Block memory b, bytes32 sigHash) {
        b.header.chain_id.value.value = chainIdRet;
        b.header.height.value = heightRet;

        b.body.events = new BridgeTypes.Event[][](uint256(txIndexUsed) + 1);
        b.body.events[txIndexUsed] = new BridgeTypes.Event[](1);

        BridgeTypes.Event memory evt;
        evt.stream_id.application_id.choice = 1;
        evt.stream_id.application_id.user.application_description_hash.value = fungibleAppIdRet;
        evt.stream_id.stream_name.value = bytes("burns");
        evt.index = 5;
        evt.value = _encodeBurn(recipBase, amountPerBurn);
        b.body.events[txIndexUsed][0] = evt;

        sigHash = bytes32(uint256(0x1234));
    }

    function _encodeBurn(address target, uint128 amount) private view returns (bytes memory) {
        WrappedFungibleTypes.BurnEvent memory burnEvt;
        burnEvt.source.chain_id.value.value = SOURCE_CHAIN_ID;
        burnEvt.source.owner.choice = ownerChoice;
        burnEvt.source.owner.reserved = ownerReserved;
        burnEvt.source.owner.address32.value = ownerAddr32;
        burnEvt.source.owner.address20 = ownerAddr20;
        burnEvt.target = bytes20(target);
        burnEvt.amount = amount;
        return WrappedFungibleTypes.bcs_serialize_BurnEvent(burnEvt);
    }
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

function _u32s(uint32 a, uint32 b) pure returns (uint32[] memory) {
    uint32[] memory arr = new uint32[](2);
    arr[0] = a;
    arr[1] = b;
    return arr;
}

function _u32s_single(uint32 a) pure returns (uint32[] memory) {
    uint32[] memory arr = new uint32[](1);
    arr[0] = a;
    return arr;
}

/// Mirrors the `AccountOwner` value the mocks bake into every BurnEvent,
/// then runs it through the same codegen serializer that `blockBurn`
/// uses when emitting `BurnBlocked.source_owner_bcs`.
function _expectedOwnerBcs() pure returns (bytes memory) {
    BridgeTypes.CryptoHash memory address32;
    address32.value = SOURCE_OWNER_ADDR32;
    BridgeTypes.AccountOwner memory owner = BridgeTypes.AccountOwner_case_address32(address32);
    return BridgeTypes.bcs_serialize_AccountOwner(owner);
}

// ------------------------------------------------------------------
// Test contract
// ------------------------------------------------------------------

contract FungibleBridgeProcessBurnsTest is Test {
    event BurnReleased(uint64 indexed height, uint32 indexed eventIndex, address indexed target, uint256 amount);
    event BurnBlocked(
        uint64 indexed height,
        uint32 indexed eventIndex,
        address indexed blocked_by,
        bytes32 source_chain_id,
        bytes source_owner_bcs,
        uint128 amount
    );

    // Deploy a bridge backed by `lc`, with a LineraToken that has
    // `supply` tokens pre-minted to the bridge.
    function _deployBridge(address lc, uint256 supply) internal returns (FungibleBridge bridge, LineraToken tok) {
        tok = new LineraToken("Test", "TST", 18, supply);
        bridge = new FungibleBridge(lc, CHAIN_ID, address(tok), APP_ID);
        // Send all tokens to the bridge so transfer() calls succeed.
        tok.transfer(address(bridge), supply);
    }

    // ------------------------------------------------------------------

    function test_processBurns_single_position_marks_processed() public {
        // 2 burns in tx TX at positions 0 and 1 with stream indices 5 and 6.
        // Settle only position 0; assert (HEIGHT, 5) is flipped, (HEIGHT, 6) stays false.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.processBurns(hex"deadbeef", TX, _u32s_single(0));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "stream index 5 should be processed");
        assertFalse(bridge.isBurnProcessed(HEIGHT, 6), "stream index 6 should not be processed yet");
    }

    function test_processBurns_multi_position_marks_both_processed() public {
        // 2 burns; settle both positions; both flags true.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.processBurns(hex"deadbeef", TX, _u32s(0, 1));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "stream index 5 should be processed");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "stream index 6 should be processed");
    }

    function test_processBurns_already_processed_skips() public {
        // Idempotent like `_onBlock`: re-processing the same burn must be a
        // no-op, not a revert. Keeps the relayer robust to overlap between
        // an addBlock-path settlement and a racing/retrying processBurns
        // call covering the same (height, tx, pos).
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge, LineraToken tok) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.processBurns(hex"deadbeef", TX, _u32s_single(0));
        uint256 firstBal = tok.balanceOf(RECIP_0);
        assertEq(firstBal, AMOUNT, "first call should have released to recipient");

        // Second call: must not revert and must not double-release.
        bridge.processBurns(hex"deadbeef", TX, _u32s_single(0));
        assertEq(tok.balanceOf(RECIP_0), firstBal, "second call must not double-release");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "burn stays marked processed");
    }

    function test_processBurns_tx_index_out_of_range_reverts() public {
        // Block has 1 tx; processBurns with txIndex=99 → revert "txIndex out of range".
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("txIndex out of range"));
        bridge.processBurns(hex"deadbeef", 99, _u32s_single(0));
    }

    function test_processBurns_event_pos_out_of_range_reverts() public {
        // 2 burns at positions 0,1; processBurns with position=99 → revert "eventPos out of range".
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("eventPos out of range"));
        bridge.processBurns(hex"deadbeef", TX, _u32s_single(99));
    }

    function test_processBurns_non_burn_event_reverts() public {
        // MockLightClient returns a Block whose only event has the wrong
        // stream_name ("deposits") → processBurns(tx=0, [0]) → revert "not a matching burn".
        MockLightClientForNonBurn lc = new MockLightClientForNonBurn(CHAIN_ID, HEIGHT, APP_ID, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("not a matching burn"));
        bridge.processBurns(hex"deadbeef", 0, _u32s_single(0));
    }

    function test_processBurns_empty_positions_reverts() public {
        // An empty positions array would silently pay for cert verification
        // with no work to do. Reject it eagerly so caller bugs surface.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        uint32[] memory empty = new uint32[](0);
        vm.expectRevert(bytes("empty positions"));
        bridge.processBurns(hex"deadbeef", TX, empty);
    }

    function test_processBurns_emits_BurnReleased() public {
        // Settle two burns; each release emits BurnReleased with
        // (height, evt.index, target, amount). Recipients are
        // RECIP_0 and RECIP_0+1; stream indices are 5 and 6.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        address recip1 = address(uint160(RECIP_0) + 1);

        vm.expectEmit(true, true, true, true, address(bridge));
        emit BurnReleased(HEIGHT, 5, RECIP_0, AMOUNT);
        vm.expectEmit(true, true, true, true, address(bridge));
        emit BurnReleased(HEIGHT, 6, recip1, AMOUNT);

        bridge.processBurns(hex"deadbeef", TX, _u32s(0, 1));
    }

    function test_processBurns_partial_overlap_releases_remaining() public {
        // 2 burns at positions 0,1. Settle pos 1 first; then call
        // processBurns([0, 1]). Under skip-on-duplicate semantics pos 0 must
        // be released and pos 1 silently skipped — no revert, no double-release.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge, LineraToken tok) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.processBurns(hex"deadbeef", TX, _u32s_single(1));
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "pos 1 should now be processed");
        address recip1 = address(uint160(RECIP_0) + 1);
        assertEq(tok.balanceOf(recip1), AMOUNT, "pos 1 recipient should hold released amount");

        // Overlapping call — pos 0 settles, pos 1 silently skipped.
        bridge.processBurns(hex"deadbeef", TX, _u32s(0, 1));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "pos 0 should now be processed");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "pos 1 stays processed");
        assertEq(tok.balanceOf(RECIP_0), AMOUNT, "pos 0 released once to its recipient");
        assertEq(tok.balanceOf(recip1), AMOUNT, "pos 1 not double-released");
    }

    function test_blockBurn_prevents_processBurns_release() public {
        // 2 burns. Block (HEIGHT, 5) at position 0; then processBurns([0, 1]) —
        // pos 0 (stream index 5) silently skipped, pos 1 still released.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge, LineraToken tok) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.blockBurn(hex"deadbeef", TX, 0);
        assertTrue(bridge.isBurnBlocked(HEIGHT, 5), "burn at index 5 should be blocked");

        bridge.processBurns(hex"deadbeef", TX, _u32s(0, 1));

        assertFalse(bridge.isBurnProcessed(HEIGHT, 5), "blocked burn must not be marked processed");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "unblocked burn settles");
        assertEq(tok.balanceOf(RECIP_0), 0, "blocked recipient holds no released tokens");
        address recip1 = address(uint160(RECIP_0) + 1);
        assertEq(tok.balanceOf(recip1), AMOUNT, "unblocked recipient receives release");
    }

    function test_blockBurn_prevents_addBlock_release() public {
        // Same as above but through the addBlock path: block (HEIGHT, 5)
        // via position 0, then addBlock(cert) — pos 0 skipped, pos 1 released.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge, LineraToken tok) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.blockBurn(hex"deadbeef", TX, 0);
        bridge.addBlock(hex"deadbeef");

        assertFalse(bridge.isBurnProcessed(HEIGHT, 5), "blocked burn must not be marked processed");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "unblocked burn settles");
        assertEq(tok.balanceOf(RECIP_0), 0, "blocked recipient holds no released tokens");
    }

    function test_blockBurn_idempotent_emits_once() public {
        // First blockBurn sets the flag and emits with the decoded
        // source + amount. Second is a no-op (no second event, flag stays set).
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectEmit(true, true, true, true, address(bridge));
        emit BurnBlocked(HEIGHT, 5, address(this), SOURCE_CHAIN_ID, _expectedOwnerBcs(), AMOUNT);
        bridge.blockBurn(hex"deadbeef", TX, 0);
        assertTrue(bridge.isBurnBlocked(HEIGHT, 5));

        vm.recordLogs();
        bridge.blockBurn(hex"deadbeef", TX, 0);
        Vm.Log[] memory logs = vm.getRecordedLogs();
        assertEq(logs.length, 0, "second blockBurn must be silent");
    }

    function test_blockBurn_chain_id_mismatch_reverts() public {
        MockLightClientForBurns lc =
            new MockLightClientForBurns(bytes32(uint256(0xDEAD)), HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
        vm.expectRevert(bytes("chain id mismatch"));
        bridge.blockBurn(hex"deadbeef", TX, 0);
    }

    function test_blockBurn_tx_index_out_of_range_reverts() public {
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
        vm.expectRevert(bytes("txIndex out of range"));
        bridge.blockBurn(hex"deadbeef", 99, 0);
    }

    function test_blockBurn_event_pos_out_of_range_reverts() public {
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
        vm.expectRevert(bytes("eventPos out of range"));
        bridge.blockBurn(hex"deadbeef", TX, 99);
    }

    function test_blockBurn_non_burn_event_reverts() public {
        MockLightClientForNonBurn lc = new MockLightClientForNonBurn(CHAIN_ID, HEIGHT, APP_ID, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
        vm.expectRevert(bytes("not a matching burn"));
        bridge.blockBurn(hex"deadbeef", 0, 0);
    }

    function test_blockBurn_already_processed_is_silent_noop() public {
        // After processBurns has settled (HEIGHT, 5), blockBurn for the same
        // position must be a silent no-op: no BurnBlocked event, no flip of
        // the blocked flag. Matches the early `if (processedBurns[key]) return;`
        // in FungibleBridge.blockBurn.
        MockLightClientForBurns lc = new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
        bridge.processBurns(hex"deadbeef", TX, _u32s_single(0));
        assertTrue(bridge.isBurnProcessed(HEIGHT, 5));
        vm.recordLogs();
        bridge.blockBurn(hex"deadbeef", TX, 0);
        Vm.Log[] memory logs = vm.getRecordedLogs();
        assertEq(logs.length, 0, "no event when blocking a settled burn");
        assertFalse(bridge.isBurnBlocked(HEIGHT, 5));
    }

    /// blockBurn must emit a BurnBlocked event whose `source_owner_bcs` field
    /// round-trips correctly for every AccountOwner variant.
    ///
    /// Variant 0 — Reserved/CHAIN: choice byte 0x00 + one reserved byte 0x00 (2 bytes total).
    /// Variant 1 — Address32: choice byte 0x01 + 32-byte CryptoHash (33 bytes total).
    /// Variant 2 — Address20: choice byte 0x02 + 20-byte EVM address (21 bytes total).
    function test_blockBurn_emits_BurnBlocked_with_decoded_fields() public {
        // --- Variant 0: Reserved/CHAIN (choice=0, reserved=0) ---
        {
            MockLightClientWithOwner lc = new MockLightClientWithOwner(
                CHAIN_ID, HEIGHT, TX, APP_ID, AMOUNT, RECIP_0, 0, 0, bytes32(0), bytes20(0)
            );
            (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
            bytes memory expectedBcs = BridgeTypes.bcs_serialize_AccountOwner(BridgeTypes.AccountOwner_case_reserved(0));
            vm.expectEmit(true, true, true, true, address(bridge));
            emit BurnBlocked(HEIGHT, 5, address(this), SOURCE_CHAIN_ID, expectedBcs, AMOUNT);
            bridge.blockBurn(hex"deadbeef", TX, 0);
            assertTrue(bridge.isBurnBlocked(HEIGHT, 5), "Reserved/CHAIN variant must be blocked");
        }

        // --- Variant 1: Address32 (choice=1) ---
        {
            bytes32 addr32 = bytes32(uint256(0xBEEF1234CAFE5678));
            MockLightClientWithOwner lc =
                new MockLightClientWithOwner(CHAIN_ID, HEIGHT, TX, APP_ID, AMOUNT, RECIP_0, 1, 0, addr32, bytes20(0));
            (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
            BridgeTypes.CryptoHash memory ch;
            ch.value = addr32;
            bytes memory expectedBcs =
                BridgeTypes.bcs_serialize_AccountOwner(BridgeTypes.AccountOwner_case_address32(ch));
            vm.expectEmit(true, true, true, true, address(bridge));
            emit BurnBlocked(HEIGHT, 5, address(this), SOURCE_CHAIN_ID, expectedBcs, AMOUNT);
            bridge.blockBurn(hex"deadbeef", TX, 0);
            assertTrue(bridge.isBurnBlocked(HEIGHT, 5), "Address32 variant must be blocked");
        }

        // --- Variant 2: Address20 (choice=2) ---
        {
            bytes20 addr20 = bytes20(address(0xAAAA0000BBBBCCCCDDDD));
            MockLightClientWithOwner lc =
                new MockLightClientWithOwner(CHAIN_ID, HEIGHT, TX, APP_ID, AMOUNT, RECIP_0, 2, 0, bytes32(0), addr20);
            (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);
            bytes memory expectedBcs =
                BridgeTypes.bcs_serialize_AccountOwner(BridgeTypes.AccountOwner_case_address20(addr20));
            vm.expectEmit(true, true, true, true, address(bridge));
            emit BurnBlocked(HEIGHT, 5, address(this), SOURCE_CHAIN_ID, expectedBcs, AMOUNT);
            bridge.blockBurn(hex"deadbeef", TX, 0);
            assertTrue(bridge.isBurnBlocked(HEIGHT, 5), "Address20 variant must be blocked");
        }
    }
}
