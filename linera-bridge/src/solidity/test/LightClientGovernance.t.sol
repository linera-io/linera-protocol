// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.30;

import {Test} from "forge-std/Test.sol";
import {LightClient} from "../LightClient.sol";

contract LightClientGovernanceTest is Test {
    bytes32 constant ADMIN_CHAIN = bytes32(uint256(0xAD));
    address internal guardian = makeAddr("guardian");
    address internal proposer = makeAddr("proposer");
    address internal stranger = makeAddr("stranger");

    function _deploy() internal returns (LightClient) {
        address[] memory validators = new address[](1);
        validators[0] = vm.addr(1);
        uint64[] memory weights = new uint64[](1);
        weights[0] = 1;
        return new LightClient(validators, weights, ADMIN_CHAIN, 0, guardian, proposer);
    }

    /// Captures the revert reason of an `addCommittee` call (string requires only;
    /// non-string panics return a sentinel). `addCommittee` is the pause-gated,
    /// state-changing entry point on this network's LightClient (there is no
    /// `registerBlock`); we use it to assert the pause gate fires before the
    /// (heavy) certificate verification that the block-proof tests already cover.
    function _addCommitteeReason(LightClient lc, bytes memory data) internal returns (string memory) {
        try lc.addCommittee(data, hex"", new bytes[](0)) {
            return "<no-revert>";
        } catch Error(string memory reason) {
            return reason;
        } catch {
            return "<non-string-revert>";
        }
    }

    // --- constructor validation ---

    function test_constructor_rejects_zero_pause_guardian() public {
        address[] memory validators = new address[](1);
        validators[0] = vm.addr(1);
        uint64[] memory weights = new uint64[](1);
        weights[0] = 1;
        vm.expectRevert(bytes("zero pauseGuardian"));
        new LightClient(validators, weights, ADMIN_CHAIN, 0, address(0), proposer);
    }

    function test_constructor_rejects_zero_proposer() public {
        address[] memory validators = new address[](1);
        validators[0] = vm.addr(1);
        uint64[] memory weights = new uint64[](1);
        weights[0] = 1;
        vm.expectRevert(bytes("zero proposer"));
        new LightClient(validators, weights, ADMIN_CHAIN, 0, guardian, address(0));
    }

    // --- emergency pause on registerBlock ---

    function test_registerBlock_reverts_when_paused() public {
        LightClient lc = _deploy();
        vm.prank(guardian);
        lc.emergencyPause(1 days);
        assertEq(
            _addCommitteeReason(lc, hex""), "emergency paused", "paused registerBlock must revert with pause reason"
        );
    }

    function test_pause_auto_expires() public {
        LightClient lc = _deploy();
        vm.prank(guardian);
        lc.emergencyPause(1 days);
        vm.warp(block.timestamp + 1 days);
        // Gate lifted: registerBlock now fails on the garbage proof, not on the pause.
        assertEq(_addCommitteeReason(lc, hex""), "<non-string-revert>", "after expiry the pause gate must be lifted");
    }

    function test_guardian_can_unpause_early() public {
        LightClient lc = _deploy();
        vm.prank(guardian);
        lc.emergencyPause(7 days);
        vm.prank(guardian);
        lc.emergencyUnpause();
        assertEq(_addCommitteeReason(lc, hex""), "<non-string-revert>", "after unpause the gate must be lifted");
    }

    function test_unpause_when_not_paused_reverts() public {
        LightClient lc = _deploy();
        vm.prank(guardian);
        vm.expectRevert(bytes("not paused"));
        lc.emergencyUnpause();
    }

    function test_non_guardian_cannot_pause() public {
        LightClient lc = _deploy();
        vm.prank(stranger);
        vm.expectRevert(bytes("only pause guardian"));
        lc.emergencyPause(1 days);
    }

    function test_pause_duration_zero_reverts() public {
        LightClient lc = _deploy();
        vm.prank(guardian);
        vm.expectRevert(bytes("invalid duration"));
        lc.emergencyPause(0);
    }

    function test_pause_duration_too_long_reverts() public {
        LightClient lc = _deploy();
        vm.prank(guardian);
        vm.expectRevert(bytes("invalid duration"));
        lc.emergencyPause(14 days + 1);
    }

    // --- expireEpochsBelow access control ---

    function test_expireEpochsBelow_only_proposer() public {
        LightClient lc = _deploy();
        vm.prank(stranger);
        vm.expectRevert(bytes("only proposer"));
        lc.expireEpochsBelow(1);
    }

    function test_expireEpochsBelow_proposer_passes_auth() public {
        LightClient lc = _deploy();
        // Proposer passes the auth gate and reaches the body, which rejects a
        // non-increasing floor — proving auth succeeded.
        vm.prank(proposer);
        vm.expectRevert(bytes("minAcceptedEpoch must increase"));
        lc.expireEpochsBelow(0);
    }
}
