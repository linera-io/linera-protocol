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

    // --- expireEpochsBelow access control ---
    //
    // The light client is NOT self-pausing (it is at the EVM bytecode-size
    // limit; fund-moving paths are pause-gated at the bridge/Microchain level).
    // The one governance surface here is the authenticated weak-subjectivity
    // floor, which must be proposer-gated.

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
