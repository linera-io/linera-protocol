// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {Microchain} from "../Microchain.sol";
import {ILightClient} from "../ILightClient.sol";

// Minimal ILightClient stand-in. Only adminChainId() matters for the
// setLightClient flow; the proof methods are inert so the harness can be
// constructed and pointed at.
contract MockLC is ILightClient {
    bytes32 internal admin;

    constructor(bytes32 _admin) {
        admin = _admin;
    }

    function adminChainId() external view override returns (bytes32) {
        return admin;
    }

    function registeredBlocks(bytes32) external pure override returns (bytes32, uint64, bytes32, uint32) {
        return (bytes32(0), 0, bytes32(0), 0);
    }

    function proveEventsCommitted(
        bytes32,
        bytes[] calldata,
        uint32,
        uint32,
        uint32,
        uint32[] calldata,
        bytes32[] calldata
    ) external pure override {}
}

// Concrete Microchain so the abstract base's governance can be exercised in
// isolation. `gated` exposes the pause modifier.
contract TestMicrochain is Microchain {
    constructor(
        address _lightClient,
        bytes32 _chainId,
        address _pauseGuardian,
        address _proposer,
        address _canceller,
        uint256 _timelockDelay
    ) Microchain(_lightClient, _chainId, _pauseGuardian, _proposer, _canceller, _timelockDelay) {}

    function gated() external whenNotEmergencyPaused returns (bool) {
        return true;
    }
}

contract MicrochainGovernanceTest is Test {
    bytes32 constant ADMIN_CHAIN = bytes32(uint256(0xAD));
    bytes32 constant CHAIN_ID = bytes32(uint256(0xC1));
    uint256 constant TIMELOCK = 7 days;

    address internal guardian = makeAddr("guardian");
    address internal proposer = makeAddr("proposer");
    address internal canceller = makeAddr("canceller");
    address internal stranger = makeAddr("stranger");

    MockLC internal lc;
    TestMicrochain internal mc;

    function setUp() public {
        lc = new MockLC(ADMIN_CHAIN);
        mc = new TestMicrochain(address(lc), CHAIN_ID, guardian, proposer, canceller, TIMELOCK);
    }

    // --- constructor validation ---

    function test_constructor_rejects_zero_lightClient() public {
        vm.expectRevert(bytes("zero lightClient"));
        new TestMicrochain(address(0), CHAIN_ID, guardian, proposer, canceller, TIMELOCK);
    }

    function test_constructor_rejects_zero_guardian() public {
        vm.expectRevert(bytes("zero pauseGuardian"));
        new TestMicrochain(address(lc), CHAIN_ID, address(0), proposer, canceller, TIMELOCK);
    }

    function test_constructor_rejects_zero_proposer() public {
        vm.expectRevert(bytes("zero proposer"));
        new TestMicrochain(address(lc), CHAIN_ID, guardian, address(0), canceller, TIMELOCK);
    }

    function test_constructor_rejects_zero_canceller() public {
        vm.expectRevert(bytes("zero canceller"));
        new TestMicrochain(address(lc), CHAIN_ID, guardian, proposer, address(0), TIMELOCK);
    }

    function test_constructor_rejects_proposer_equals_canceller() public {
        vm.expectRevert(bytes("proposer == canceller"));
        new TestMicrochain(address(lc), CHAIN_ID, guardian, proposer, proposer, TIMELOCK);
    }

    function test_constructor_rejects_timelock_too_short() public {
        vm.expectRevert(bytes("timelock too short"));
        new TestMicrochain(address(lc), CHAIN_ID, guardian, proposer, canceller, 1 days - 1);
    }

    function test_constructor_rejects_timelock_too_long() public {
        vm.expectRevert(bytes("timelock too long"));
        new TestMicrochain(address(lc), CHAIN_ID, guardian, proposer, canceller, 90 days + 1);
    }

    // --- setLightClient flow ---

    function test_propose_execute_updates_lightClient() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        vm.prank(proposer);
        mc.proposeLightClientUpdate(address(next));
        assertEq(address(mc.pendingLightClient()), address(next), "pending set");

        vm.warp(block.timestamp + TIMELOCK);
        // Permissionless execution.
        vm.prank(stranger);
        mc.executeLightClientUpdate();
        assertEq(address(mc.lightClient()), address(next), "lightClient updated");
        assertEq(address(mc.pendingLightClient()), address(0), "pending cleared");
    }

    function test_non_proposer_cannot_propose() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        vm.prank(stranger);
        vm.expectRevert(bytes("only proposer"));
        mc.proposeLightClientUpdate(address(next));
    }

    function test_propose_zero_reverts() public {
        vm.prank(proposer);
        vm.expectRevert(bytes("zero address"));
        mc.proposeLightClientUpdate(address(0));
    }

    function test_propose_noop_reverts() public {
        vm.prank(proposer);
        vm.expectRevert(bytes("no-op update"));
        mc.proposeLightClientUpdate(address(lc));
    }

    function test_propose_when_pending_reverts() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        MockLC other = new MockLC(ADMIN_CHAIN);
        vm.prank(proposer);
        mc.proposeLightClientUpdate(address(next));
        vm.prank(proposer);
        vm.expectRevert(bytes("update already pending"));
        mc.proposeLightClientUpdate(address(other));
    }

    function test_propose_different_network_reverts() public {
        MockLC wrong = new MockLC(bytes32(uint256(0xBAD)));
        vm.prank(proposer);
        vm.expectRevert(bytes("different network"));
        mc.proposeLightClientUpdate(address(wrong));
    }

    function test_execute_before_delay_reverts() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        vm.prank(proposer);
        mc.proposeLightClientUpdate(address(next));
        vm.expectRevert(bytes("delay not elapsed"));
        mc.executeLightClientUpdate();
    }

    function test_execute_no_pending_reverts() public {
        vm.expectRevert(bytes("no pending update"));
        mc.executeLightClientUpdate();
    }

    function test_canceller_can_cancel() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        vm.prank(proposer);
        mc.proposeLightClientUpdate(address(next));
        vm.prank(canceller);
        mc.cancelLightClientUpdate();
        assertEq(address(mc.pendingLightClient()), address(0), "pending cleared");

        vm.warp(block.timestamp + TIMELOCK);
        vm.expectRevert(bytes("no pending update"));
        mc.executeLightClientUpdate();
    }

    function test_proposer_can_cancel_own() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        vm.prank(proposer);
        mc.proposeLightClientUpdate(address(next));
        vm.prank(proposer);
        mc.cancelLightClientUpdate();
        assertEq(address(mc.pendingLightClient()), address(0), "pending cleared");
    }

    function test_stranger_cannot_cancel() public {
        MockLC next = new MockLC(ADMIN_CHAIN);
        vm.prank(proposer);
        mc.proposeLightClientUpdate(address(next));
        vm.prank(stranger);
        vm.expectRevert(bytes("not authorized"));
        mc.cancelLightClientUpdate();
    }

    // --- emergency pause ---

    function test_pause_blocks_gated() public {
        vm.prank(guardian);
        mc.emergencyPause(1 days);
        vm.expectRevert(bytes("emergency paused"));
        mc.gated();
    }

    function test_pause_auto_expires() public {
        vm.prank(guardian);
        mc.emergencyPause(1 days);
        vm.warp(block.timestamp + 1 days);
        assertTrue(mc.gated(), "gated works after auto-expiry");
    }

    function test_guardian_unpause_early() public {
        vm.prank(guardian);
        mc.emergencyPause(7 days);
        vm.prank(guardian);
        mc.emergencyUnpause();
        assertTrue(mc.gated(), "gated works after early unpause");
    }

    function test_non_guardian_cannot_pause() public {
        vm.prank(stranger);
        vm.expectRevert(bytes("only pause guardian"));
        mc.emergencyPause(1 days);
    }

    function test_pause_duration_bounds() public {
        vm.prank(guardian);
        vm.expectRevert(bytes("invalid duration"));
        mc.emergencyPause(0);
        vm.prank(guardian);
        vm.expectRevert(bytes("invalid duration"));
        mc.emergencyPause(14 days + 1);
    }

    function test_unpause_when_not_paused_reverts() public {
        vm.prank(guardian);
        vm.expectRevert(bytes("not paused"));
        mc.emergencyUnpause();
    }
}
