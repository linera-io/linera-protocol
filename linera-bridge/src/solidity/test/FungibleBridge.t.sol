// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {FungibleBridge} from "../FungibleBridge.sol";
import {BridgeTypes} from "../BridgeTypes.sol";
import {WrappedFungibleTypesV1} from "../WrappedFungibleTypesV1.sol";
import {LineraToken} from "../LineraToken.sol";
import {IBurnEventDecoder} from "../IBurnEventDecoder.sol";
import {FungibleBurnEventDecoderV1} from "../FungibleBurnEventDecoderV1.sol";

// ------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------

bytes32 constant CHAIN_ID = bytes32(uint256(0xC1));
uint64 constant HEIGHT = 42;
uint32 constant TX = 0;
bytes32 constant BLOCK_HASH = bytes32(uint256(0xB10C));
uint128 constant AMOUNT = 1_000_000_000_000_000_000; // 1e18
address constant RECIP_0 = address(0xA0);
// The Linera bridge application ID the mock emits its "burns" stream under;
// the FungibleBridge matches burns against this id.
bytes32 constant BRIDGE_APP_ID = bytes32(uint256(0xF00D));
// A distinct wrapped-fungible (deposit/mint target) application ID, used to
// confirm that burn-matching keys on the bridge id and not on this one.
bytes32 constant FUNGIBLE_APP_ID = bytes32(uint256(0xBEEF));
// Governance constructor args for the bridges built in this file's
// process-burns / deposit / pause tests. Those suites don't drive governance
// (these are just valid non-zero construction args) except FungibleBridgePauseTest,
// which pranks as PAUSE_GUARDIAN. The timelocked flows are covered by
// FungibleBridgeDecoderTest (setDecoder) and FungibleBridgeLightClientUpdateTest
// (setLightClient) — both use their own role addresses — and the full role/validation
// matrix lives in MicrochainGovernance.t.sol and LightClientGovernance.t.sol.
address constant PAUSE_GUARDIAN = address(0xDA);
address constant PROPOSER = address(0xBB);
address constant CANCELLER = address(0xCC);
uint256 constant TIMELOCK_DELAY = 1 days;

// ------------------------------------------------------------------
// MockLightClient
//
// Stands in for the real LightClient. `assertEventsCommitted` is a no-op
// (or reverts when armed) — the fold itself is covered by LightClient's
// own tests. `registeredBlocks` returns a nonzero events hash plus the
// configured chain id and height, so `FungibleBridge.processBurns` can
// read them and release the burns it is handed.
// ------------------------------------------------------------------
contract MockLightClient {
    bytes32 public chainIdRet;
    uint64 public heightRet;
    bool public inclusionReverts;
    // The Linera network this client tracks; only read by the setLightClient
    // "same network" check, so it defaults to zero for tests that ignore it.
    bytes32 public adminChainIdRet;

    constructor(bytes32 _chainId, uint64 _height) {
        chainIdRet = _chainId;
        heightRet = _height;
    }

    function setInclusionReverts(bool value) external {
        inclusionReverts = value;
    }

    function setAdminChainId(bytes32 value) external {
        adminChainIdRet = value;
    }

    function adminChainId() external view returns (bytes32) {
        return adminChainIdRet;
    }

    function assertEventsCommitted(
        bytes32,
        bytes[] calldata,
        uint32,
        uint32,
        uint32,
        uint32[] calldata,
        bytes32[] calldata
    ) external view {
        require(!inclusionReverts, "event inclusion proof failed");
    }

    // Mirrors the public getter of LightClient's `registeredBlocks` mapping. A nonzero
    // `eventsHash` marks the block as registered.
    function registeredBlocks(bytes32)
        external
        view
        returns (bytes32 eventsHash, uint64 height, bytes32 chainId, uint32 epoch)
    {
        return (bytes32(uint256(1)), heightRet, chainIdRet, 0);
    }
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

/// BCS encoding of a wrapped-fungible burn event for `target`/`amount` at Linera stream `index`,
/// on the application this bridge matches burns against (`BRIDGE_APP_ID`). `streamName` selects the stream — "burns"
/// is the one the bridge releases.
function _eventBcs(uint32 index, address target, uint128 amount, bytes memory streamName) pure returns (bytes memory) {
    BridgeTypes.Event memory evt;
    evt.stream_id.application_id.choice = 1; // User
    evt.stream_id.application_id.user.application_description_hash.value = BRIDGE_APP_ID;
    evt.stream_id.stream_name.value = streamName;
    evt.index = index;
    WrappedFungibleTypesV1.BurnEvent memory burnEvt;
    burnEvt.target = bytes20(target);
    burnEvt.amount = amount;
    evt.value = WrappedFungibleTypesV1.bcs_serialize_BurnEvent(burnEvt);
    return BridgeTypes.bcs_serialize_Event(evt);
}

function _burnBcs(uint32 index, address target, uint128 amount) pure returns (bytes memory) {
    return _eventBcs(index, target, amount, bytes("burns"));
}

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

function _deployDecoderV1() returns (address) {
    return address(new FungibleBurnEventDecoderV1());
}

// A decoder that ignores its input and always returns a fixed (recipient,
// amount). Lets a test prove that `setDecoder` actually reroutes payload
// decoding through the new contract. `pure` per the interface, so the fixed
// values are compile-time constants.
address constant MOCK_DECODER_RECIPIENT = address(0x0FEE);
uint256 constant MOCK_DECODER_AMOUNT = 12_345;

contract MockDecoder is IBurnEventDecoder {
    function decodeBurnEvent(bytes calldata) external pure override returns (address, uint256) {
        return (MOCK_DECODER_RECIPIENT, MOCK_DECODER_AMOUNT);
    }
}

// ------------------------------------------------------------------
// Test contract
// ------------------------------------------------------------------

contract FungibleBridgeProcessBurnsTest is Test {
    event BurnReleased(uint64 indexed height, uint32 indexed eventIndex, address indexed target, uint256 amount);

    bytes32[] internal noSiblings;

    // Deploy a bridge backed by `lc`, with a LineraToken that has `supply` tokens pre-minted to
    // the bridge.
    function _deployBridge(address lc, uint256 supply) internal returns (FungibleBridge bridge, LineraToken tok) {
        tok = new LineraToken("Test", "TST", 18, supply);
        bridge = new FungibleBridge(
            lc,
            CHAIN_ID,
            address(tok),
            FUNGIBLE_APP_ID,
            BRIDGE_APP_ID,
            _deployDecoderV1(),
            PAUSE_GUARDIAN,
            PROPOSER,
            CANCELLER,
            TIMELOCK_DELAY
        );
        // Send all tokens to the bridge so transfer() calls succeed.
        tok.transfer(address(bridge), supply);
    }

    // Settles the chunk `eventBcs` at `positions`. The inclusion-proof structure (sibling counts,
    // tx/event sizes) is irrelevant here because `MockLightClient.assertEventsCommitted` is a no-op;
    // these tests exercise FungibleBridge's release logic, not the fold.
    function _settle(FungibleBridge bridge, bytes[] memory eventBcs, uint32[] memory positions) internal {
        bridge.processBurns(BLOCK_HASH, eventBcs, TX, 1, uint32(eventBcs.length), positions, noSiblings);
    }

    function _bytesArray(bytes memory a) internal pure returns (bytes[] memory) {
        bytes[] memory arr = new bytes[](1);
        arr[0] = a;
        return arr;
    }

    function _bytesArray(bytes memory a, bytes memory b) internal pure returns (bytes[] memory) {
        bytes[] memory arr = new bytes[](2);
        arr[0] = a;
        arr[1] = b;
        return arr;
    }

    // ------------------------------------------------------------------

    function test_processBurns_single_position_marks_processed() public {
        // Hand the bridge only the first of two burns (stream indices 5 and 6); assert (HEIGHT, 5)
        // is flipped and the untouched (HEIGHT, 6) stays false.
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT)), _u32s_single(0));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "stream index 5 should be processed");
        assertFalse(bridge.isBurnProcessed(HEIGHT, 6), "stream index 6 should not be processed yet");
    }

    function test_processBurns_multi_position_marks_both_processed() public {
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        address recip1 = address(uint160(RECIP_0) + 1);
        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT), _burnBcs(6, recip1, AMOUNT)), _u32s(0, 1));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "stream index 5 should be processed");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "stream index 6 should be processed");
    }

    function test_processBurns_already_processed_skips() public {
        // Idempotent: re-processing the same burn must be a no-op, not a revert.
        // Keeps the relayer robust to overlap between a racing/retrying processBurns call
        // covering the same (height, eventIndex).
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge, LineraToken tok) = _deployBridge(address(lc), AMOUNT * 10);

        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT)), _u32s_single(0));
        uint256 firstBal = tok.balanceOf(RECIP_0);
        assertEq(firstBal, AMOUNT, "first call should have released to recipient");

        // Second call: must not revert and must not double-release.
        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT)), _u32s_single(0));
        assertEq(tok.balanceOf(RECIP_0), firstBal, "second call must not double-release");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "burn stays marked processed");
    }

    function test_processBurns_non_burn_event_reverts() public {
        // An event on the "deposits" stream is not a burn the bridge releases → "not a matching burn".
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        bytes[] memory chunk = _bytesArray(_eventBcs(5, RECIP_0, AMOUNT, bytes("deposits")));
        vm.expectRevert(bytes("not a matching burn"));
        _settle(bridge, chunk, _u32s_single(0));
    }

    function test_processBurns_empty_positions_reverts() public {
        // An empty positions array would pay for inclusion verification with no work to do. Reject
        // it eagerly so caller bugs surface.
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        bytes[] memory empty = new bytes[](0);
        uint32[] memory emptyPositions = new uint32[](0);
        vm.expectRevert(bytes("empty positions"));
        _settle(bridge, empty, emptyPositions);
    }

    function test_processBurns_chain_id_mismatch_reverts() public {
        // The block is registered for a different chain than the one this bridge settles.
        MockLightClient lc = new MockLightClient(bytes32(uint256(0xBAD)), HEIGHT);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("chain id mismatch"));
        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT)), _u32s_single(0));
    }

    function test_processBurns_failed_inclusion_proof_reverts() public {
        // A failing inclusion proof in the light client must abort the whole settlement.
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        lc.setInclusionReverts(true);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        vm.expectRevert(bytes("event inclusion proof failed"));
        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT)), _u32s_single(0));
    }

    function test_processBurns_emits_BurnReleased() public {
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge,) = _deployBridge(address(lc), AMOUNT * 10);

        address recip1 = address(uint160(RECIP_0) + 1);

        vm.expectEmit(true, true, true, true, address(bridge));
        emit BurnReleased(HEIGHT, 5, RECIP_0, AMOUNT);
        vm.expectEmit(true, true, true, true, address(bridge));
        emit BurnReleased(HEIGHT, 6, recip1, AMOUNT);

        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT), _burnBcs(6, recip1, AMOUNT)), _u32s(0, 1));
    }

    function test_processBurns_partial_overlap_releases_remaining() public {
        // Settle burn 6 first, then a chunk covering burns 5 and 6. Under skip-on-duplicate
        // semantics burn 5 must be released and burn 6 silently skipped — no revert, no
        // double-release.
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        (FungibleBridge bridge, LineraToken tok) = _deployBridge(address(lc), AMOUNT * 10);
        address recip1 = address(uint160(RECIP_0) + 1);

        _settle(bridge, _bytesArray(_burnBcs(6, recip1, AMOUNT)), _u32s_single(0));
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "burn 6 should now be processed");
        assertEq(tok.balanceOf(recip1), AMOUNT, "burn 6 recipient should hold released amount");

        // Overlapping chunk — burn 5 settles, burn 6 silently skipped.
        _settle(bridge, _bytesArray(_burnBcs(5, RECIP_0, AMOUNT), _burnBcs(6, recip1, AMOUNT)), _u32s(0, 1));

        assertTrue(bridge.isBurnProcessed(HEIGHT, 5), "burn 5 should now be processed");
        assertTrue(bridge.isBurnProcessed(HEIGHT, 6), "burn 6 stays processed");
        assertEq(tok.balanceOf(RECIP_0), AMOUNT, "burn 5 released once to its recipient");
        assertEq(tok.balanceOf(recip1), AMOUNT, "burn 6 not double-released");
    }
}

contract FungibleBridgeDepositTest is Test {
    // A deposit above u128::MAX could never be minted on Linera (which holds
    // U128), so deposit() must reject it at lock time rather than locking ERC-20
    // that can never be bridged or refunded. The guard reverts before any token
    // transfer, so the depositor needn't hold the amount and a minimal token
    // suffices; deposit() also never calls the light client, so a dummy address
    // works.
    function test_deposit_reverts_amount_exceeds_u128() public {
        uint256 oversized = uint256(type(uint128).max) + 1;
        LineraToken tok = new LineraToken("Test", "TST", 18, 1);
        FungibleBridge bridge = new FungibleBridge(
            address(0xdead),
            CHAIN_ID,
            address(tok),
            FUNGIBLE_APP_ID,
            BRIDGE_APP_ID,
            _deployDecoderV1(),
            PAUSE_GUARDIAN,
            PROPOSER,
            CANCELLER,
            TIMELOCK_DELAY
        );

        vm.expectRevert(bytes("amount exceeds u128"));
        bridge.deposit(CHAIN_ID, FUNGIBLE_APP_ID, bytes32(uint256(0xBEEF)), oversized);
    }
}

// Integration: the emergency pause (inherited from Microchain) gates both
// value-flow entry points on the bridge.
contract FungibleBridgePauseTest is Test {
    bytes32[] internal noSiblings;

    function _deploy() internal returns (FungibleBridge bridge, LineraToken tok, MockLightClient lc) {
        lc = new MockLightClient(CHAIN_ID, HEIGHT);
        tok = new LineraToken("Test", "TST", 18, AMOUNT * 10);
        bridge = new FungibleBridge(
            address(lc),
            CHAIN_ID,
            address(tok),
            FUNGIBLE_APP_ID,
            BRIDGE_APP_ID,
            _deployDecoderV1(),
            PAUSE_GUARDIAN,
            PROPOSER,
            CANCELLER,
            TIMELOCK_DELAY
        );
        tok.transfer(address(bridge), AMOUNT * 10);
    }

    function test_deposit_reverts_when_paused() public {
        // The pause modifier fires before any token movement, so no funding or
        // approval is needed to observe it.
        (FungibleBridge bridge,,) = _deploy();

        vm.prank(PAUSE_GUARDIAN);
        bridge.emergencyPause(1 days);

        vm.expectRevert(bytes("emergency paused"));
        bridge.deposit(CHAIN_ID, FUNGIBLE_APP_ID, bytes32(uint256(0xBEEF)), AMOUNT);
    }

    function test_processBurns_reverts_when_paused() public {
        (FungibleBridge bridge,,) = _deploy();

        vm.prank(PAUSE_GUARDIAN);
        bridge.emergencyPause(1 days);

        bytes[] memory chunk = _bytesArray(_burnBcs(5, RECIP_0, AMOUNT));
        uint32[] memory positions = _u32s_single(0);
        vm.expectRevert(bytes("emergency paused"));
        bridge.processBurns(BLOCK_HASH, chunk, TX, 1, 1, positions, noSiblings);
    }

    function test_processBurns_works_after_pause_expiry() public {
        (FungibleBridge bridge, LineraToken tok,) = _deploy();

        vm.prank(PAUSE_GUARDIAN);
        bridge.emergencyPause(1 days);
        vm.warp(block.timestamp + 1 days);

        bytes[] memory chunk = _bytesArray(_burnBcs(5, RECIP_0, AMOUNT));
        uint32[] memory positions = _u32s_single(0);
        bridge.processBurns(BLOCK_HASH, chunk, TX, 1, 1, positions, noSiblings);
        assertEq(tok.balanceOf(RECIP_0), AMOUNT, "burn released after pause expiry");
    }

    function _bytesArray(bytes memory a) internal pure returns (bytes[] memory) {
        bytes[] memory arr = new bytes[](1);
        arr[0] = a;
        return arr;
    }
}

// The swappable decoder: setDecoder governance flow (mirrors setLightClient on
// Microchain) plus proof that a swapped decoder actually reroutes payload
// decoding.
contract FungibleBridgeDecoderTest is Test {
    event BurnReleased(uint64 indexed height, uint32 indexed eventIndex, address indexed target, uint256 amount);

    bytes32[] internal noSiblings;
    address internal proposer = makeAddr("proposer");
    address internal canceller = makeAddr("canceller");
    address internal guardian = makeAddr("guardian");
    address internal stranger = makeAddr("stranger");
    uint256 constant TIMELOCK = 1 days;

    function _deploy() internal returns (FungibleBridge bridge, LineraToken tok, address v1) {
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        tok = new LineraToken("Test", "TST", 18, AMOUNT * 10);
        v1 = _deployDecoderV1();
        bridge = new FungibleBridge(
            address(lc),
            CHAIN_ID,
            address(tok),
            FUNGIBLE_APP_ID,
            BRIDGE_APP_ID,
            v1,
            guardian,
            proposer,
            canceller,
            TIMELOCK
        );
        tok.transfer(address(bridge), AMOUNT * 10);
    }

    function _settle(FungibleBridge bridge, uint32 index, address target, uint128 amount) internal {
        bytes[] memory chunk = new bytes[](1);
        chunk[0] = _burnBcs(index, target, amount);
        uint32[] memory positions = _u32s_single(0);
        bridge.processBurns(BLOCK_HASH, chunk, TX, 1, 1, positions, noSiblings);
    }

    // --- constructor ---

    function test_constructor_rejects_zero_decoder() public {
        MockLightClient lc = new MockLightClient(CHAIN_ID, HEIGHT);
        LineraToken tok = new LineraToken("Test", "TST", 18, 1);
        vm.expectRevert(bytes("zero decoder"));
        new FungibleBridge(
            address(lc),
            CHAIN_ID,
            address(tok),
            FUNGIBLE_APP_ID,
            BRIDGE_APP_ID,
            address(0),
            guardian,
            proposer,
            canceller,
            TIMELOCK
        );
    }

    // --- setDecoder flow ---

    function test_propose_execute_updates_decoder() public {
        (FungibleBridge bridge,,) = _deploy();
        MockDecoder next = new MockDecoder();

        vm.prank(proposer);
        bridge.proposeDecoderUpdate(address(next));
        assertEq(address(bridge.pendingDecoder()), address(next), "pending set");

        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(stranger);
        bridge.executeDecoderUpdate();
        assertEq(address(bridge.decoder()), address(next), "decoder updated");
    }

    function test_non_proposer_cannot_propose() public {
        (FungibleBridge bridge,,) = _deploy();
        MockDecoder next = new MockDecoder();
        vm.prank(stranger);
        vm.expectRevert(bytes("only proposer"));
        bridge.proposeDecoderUpdate(address(next));
    }

    function test_propose_zero_reverts() public {
        (FungibleBridge bridge,,) = _deploy();
        vm.prank(proposer);
        vm.expectRevert(bytes("zero address"));
        bridge.proposeDecoderUpdate(address(0));
    }

    function test_propose_noop_reverts() public {
        (FungibleBridge bridge,, address v1) = _deploy();
        vm.prank(proposer);
        vm.expectRevert(bytes("no-op update"));
        bridge.proposeDecoderUpdate(v1);
    }

    function test_execute_before_delay_reverts() public {
        (FungibleBridge bridge,,) = _deploy();
        MockDecoder next = new MockDecoder();
        vm.prank(proposer);
        bridge.proposeDecoderUpdate(address(next));
        vm.expectRevert(bytes("delay not elapsed"));
        bridge.executeDecoderUpdate();
    }

    function test_canceller_can_cancel() public {
        (FungibleBridge bridge,,) = _deploy();
        MockDecoder next = new MockDecoder();
        vm.prank(proposer);
        bridge.proposeDecoderUpdate(address(next));
        vm.prank(canceller);
        bridge.cancelDecoderUpdate();
        assertEq(address(bridge.pendingDecoder()), address(0), "pending cleared");
    }

    // --- behavior ---

    function test_v1_decoder_releases_to_event_target() public {
        (FungibleBridge bridge, LineraToken tok,) = _deploy();
        _settle(bridge, 5, RECIP_0, AMOUNT);
        assertEq(tok.balanceOf(RECIP_0), AMOUNT, "V1 decodes recipient/amount from the event payload");
    }

    function test_swapped_decoder_is_used() public {
        (FungibleBridge bridge, LineraToken tok,) = _deploy();
        MockDecoder mock = new MockDecoder();

        vm.prank(proposer);
        bridge.proposeDecoderUpdate(address(mock));
        vm.warp(block.timestamp + TIMELOCK);
        bridge.executeDecoderUpdate();

        // The event still encodes (RECIP_0, AMOUNT), but the mock decoder
        // overrides the payout to its fixed (recipient, amount).
        vm.expectEmit(true, true, true, true, address(bridge));
        emit BurnReleased(HEIGHT, 7, MOCK_DECODER_RECIPIENT, MOCK_DECODER_AMOUNT);
        _settle(bridge, 7, RECIP_0, AMOUNT);

        assertEq(tok.balanceOf(MOCK_DECODER_RECIPIENT), MOCK_DECODER_AMOUNT, "new decoder routed the payout");
        assertEq(tok.balanceOf(RECIP_0), 0, "old decoder path no longer used");
    }
}

// End-to-end setLightClient flow on a real FungibleBridge (not the abstract
// Microchain harness): the timelocked swap repoints the bridge and settlement
// keeps working through the new light client — i.e. TVL is preserved across the
// swap.
contract FungibleBridgeLightClientUpdateTest is Test {
    bytes32 constant ADMIN = bytes32(uint256(0xAD));
    uint256 constant TIMELOCK = 1 days;

    address internal proposer = makeAddr("proposer");
    address internal canceller = makeAddr("canceller");
    address internal guardian = makeAddr("guardian");

    bytes32[] internal noSiblings;

    function _bridgeWith(MockLightClient lc) internal returns (FungibleBridge bridge, LineraToken tok) {
        tok = new LineraToken("Test", "TST", 18, AMOUNT * 10);
        bridge = new FungibleBridge(
            address(lc),
            CHAIN_ID,
            address(tok),
            FUNGIBLE_APP_ID,
            BRIDGE_APP_ID,
            _deployDecoderV1(),
            guardian,
            proposer,
            canceller,
            TIMELOCK
        );
        tok.transfer(address(bridge), AMOUNT * 10);
    }

    function test_setLightClient_swaps_and_keeps_settling() public {
        MockLightClient lc1 = new MockLightClient(CHAIN_ID, HEIGHT);
        lc1.setAdminChainId(ADMIN);
        (FungibleBridge bridge, LineraToken tok) = _bridgeWith(lc1);

        MockLightClient lc2 = new MockLightClient(CHAIN_ID, HEIGHT);
        lc2.setAdminChainId(ADMIN);

        vm.prank(proposer);
        bridge.proposeLightClientUpdate(address(lc2));
        vm.warp(block.timestamp + TIMELOCK);
        // Permissionless execution.
        bridge.executeLightClientUpdate();
        assertEq(address(bridge.lightClient()), address(lc2), "bridge repointed to new light client");

        // A burn settles through the swapped-in light client and releases tokens.
        bytes[] memory chunk = new bytes[](1);
        chunk[0] = _burnBcs(5, RECIP_0, AMOUNT);
        uint32[] memory positions = _u32s_single(0);
        bridge.processBurns(BLOCK_HASH, chunk, TX, 1, 1, positions, noSiblings);
        assertEq(tok.balanceOf(RECIP_0), AMOUNT, "burn released via swapped light client");
    }

    function test_propose_different_network_reverts() public {
        MockLightClient lc1 = new MockLightClient(CHAIN_ID, HEIGHT);
        lc1.setAdminChainId(ADMIN);
        (FungibleBridge bridge,) = _bridgeWith(lc1);

        MockLightClient wrong = new MockLightClient(CHAIN_ID, HEIGHT);
        wrong.setAdminChainId(bytes32(uint256(0xBAD)));

        vm.prank(proposer);
        vm.expectRevert(bytes("different network"));
        bridge.proposeLightClientUpdate(address(wrong));
    }
}
