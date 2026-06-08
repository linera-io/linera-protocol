// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
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
// The Linera bridge application ID the mock emits its "burns" stream under;
// the FungibleBridge matches burns against this id.
bytes32 constant BRIDGE_APP_ID = bytes32(uint256(0xF00D));
// A distinct wrapped-fungible (deposit/mint target) application ID, used to
// confirm that burn-matching keys on the bridge id and not on this one.
bytes32 constant FUNGIBLE_APP_ID = bytes32(uint256(0xBEEF));

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
        burnEvt.target = bytes20(recipBase);
        burnEvt.amount = amountPerBurn;
        evt.value = WrappedFungibleTypes.bcs_serialize_BurnEvent(burnEvt);
        b.body.events[0][0] = evt;

        sigHash = bytes32(uint256(0x1234));
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

// ------------------------------------------------------------------
// Test contract
// ------------------------------------------------------------------

contract FungibleBridgeProcessBurnsTest is Test {
    event BurnReleased(uint64 indexed height, uint32 indexed eventIndex, address indexed target, uint256 amount);

    // Deploy a bridge backed by `lc`, with a LineraToken that has
    // `supply` tokens pre-minted to the bridge.
    function _deployBridge(address lc, uint256 supply) internal returns (FungibleBridge bridge, LineraToken tok) {
        tok = new LineraToken("Test", "TST", 18, supply);
        bridge = new FungibleBridge(lc, CHAIN_ID, address(tok), FUNGIBLE_APP_ID, BRIDGE_APP_ID);
        // Send all tokens to the bridge so transfer() calls succeed.
        tok.transfer(address(bridge), supply);
    }

    // ------------------------------------------------------------------

    function test_processBurns_single_position_marks_processed() public {
        // 2 burns in tx TX at positions 0 and 1 with stream indices 5 and 6.
        // Settle only position 0; assert (HEIGHT, 5) is flipped, (HEIGHT, 6) stays false.
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        bridge.processBurns(hex"deadbeef", TX, _u32s_single(0));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "stream index 5 should be processed");
        assertFalse(bridge.isBurnProcessed(HEIGHT, 6), "stream index 6 should not be processed yet");
    }

    function test_processBurns_multi_position_marks_both_processed() public {
        // 2 burns; settle both positions; both flags true.
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 2, AMOUNT, RECIP_0);
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
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 1, AMOUNT, RECIP_0);
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
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("txIndex out of range"));
        bridge.processBurns(hex"deadbeef", 99, _u32s_single(0));
    }

    function test_processBurns_event_pos_out_of_range_reverts() public {
        // 2 burns at positions 0,1; processBurns with position=99 → revert "eventPos out of range".
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 2, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("eventPos out of range"));
        bridge.processBurns(hex"deadbeef", TX, _u32s_single(99));
    }

    function test_processBurns_non_burn_event_reverts() public {
        // MockLightClient returns a Block whose only event has the wrong
        // stream_name ("deposits") → processBurns(tx=0, [0]) → revert "not a matching burn".
        MockLightClientForNonBurn lc = new MockLightClientForNonBurn(CHAIN_ID, HEIGHT, BRIDGE_APP_ID, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("not a matching burn"));
        bridge.processBurns(hex"deadbeef", 0, _u32s_single(0));
    }

    function test_processBurns_empty_positions_reverts() public {
        // An empty positions array would silently pay for cert verification
        // with no work to do. Reject it eagerly so caller bugs surface.
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 1, AMOUNT, RECIP_0);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        uint32[] memory empty = new uint32[](0);
        vm.expectRevert(bytes("empty positions"));
        bridge.processBurns(hex"deadbeef", TX, empty);
    }

    function test_processBurns_emits_BurnReleased() public {
        // Settle two burns; each release emits BurnReleased with
        // (height, evt.index, target, amount). Recipients are
        // RECIP_0 and RECIP_0+1; stream indices are 5 and 6.
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 2, AMOUNT, RECIP_0);
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
        MockLightClientForBurns lc =
            new MockLightClientForBurns(CHAIN_ID, HEIGHT, TX, BRIDGE_APP_ID, 2, AMOUNT, RECIP_0);
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
}
